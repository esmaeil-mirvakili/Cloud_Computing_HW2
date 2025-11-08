import rpyc
import string
import collections
import itertools
import time
import operator
import glob
import os
import sys
import math
import threading
import requests
from pathlib import Path
from zipfile import ZipFile
import json
import shutil


NUM_WORKERS = int(os.getenv("NUM_WORKERS", "3"))
PROJ_NAME = os.getenv("PROJ_NAME", "cloud_computing_hw2")
OVER_PARTITION = int(os.getenv("OVER_PARTITION", "4"))
TASK_TIMEOUT = int(os.getenv("TASK_TIMEOUT", "20"))
SHARED_PATH = os.getenv("SHARED_PATH", "/shared")


WORKERS = [
    (f"{PROJ_NAME}-worker-{i + 1}", 18861)
    for i in range(NUM_WORKERS)
]

task_lock = threading.Lock()


class TaskState:
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"


class Task:
    def __init__(self, task_id, kind, payload):
        self.id = task_id        # integer id
        self.kind = kind         # "map" or "reduce"
        self.state = TaskState.PENDING
        self.payload = payload
        self.worker = None       # (host, port)
        self.start_time = None   # float timestamp
        self.result = None       # dict of results


def connect_to_worker(worker):
    """Return an RPyC connection to the given worker (host, port)."""
    host, port = worker
    return rpyc.connect(host, port)


def get_available_worker(tasks):
    """
    Return a worker (host, port) that is not currently running any task
    from the given task list. If none free, return None.
    """
    with task_lock:
        busy_workers = {
            t.worker
            for t in tasks
            if t.state == TaskState.RUNNING and t.worker is not None
        }

    for w in WORKERS:
        if w not in busy_workers:
            return w
    return None


def run_map_task(task, worker):
    """Run a single map task on the given worker."""
    with task_lock:
        task.state = TaskState.RUNNING
        task.worker = worker
        task.start_time = time.time()

    try:
        conn = connect_to_worker(worker)
        result = conn.root.map(task.payload)
    except Exception as e:
        print(f"Error while sending a map task: {e}")
        # On error, mark it pending again to be retried
        with task_lock:
            task.state = TaskState.PENDING
            task.worker = None
            task.start_time = None
        return

    # Success
    with task_lock:
        task.state = TaskState.DONE
        task.result = result


def run_map_phase(map_tasks):
    """
    Schedule and monitor all map tasks.
    - Assigns pending tasks to workers.
    - Retries tasks if they exceed TASK_TIMEOUT.
    - Returns only when all map tasks are DONE.
    """

    def scheduler():
        while True:
            with task_lock:
                pending = [t for t in map_tasks if t.state == TaskState.PENDING]
            if not pending:
                # No pending tasks right now, just wait
                time.sleep(0.1)
                continue

            worker = get_available_worker(map_tasks)
            if worker is None:
                # all workers busy, wait
                time.sleep(0.1)
                continue

            task = pending[0]
            
            threading.Thread(
                target=run_map_task,
                args=(task, worker),
                daemon=True,
            ).start()

    def monitor():
        while True:
            all_done = True
            now = time.time()
            with task_lock:
                for t in map_tasks:
                    if t.state != TaskState.DONE:
                        all_done = False
                    if (
                        t.state == TaskState.RUNNING
                        and now - t.start_time > TASK_TIMEOUT
                    ):
                        # Timeout: reschedule
                        t.state = TaskState.PENDING
                        t.worker = None
                        t.start_time = None
            if all_done:
                return
            time.sleep(1)

    # Start scheduler in background and block on monitor until all tasks finish
    threading.Thread(target=scheduler, daemon=True).start()
    monitor()


def run_reduce_task(task, worker):
    """Run a single reduce task on the given worker."""
    with task_lock:
        task.state = TaskState.RUNNING
        task.worker = worker
        task.start_time = time.time()

    try:
        conn = connect_to_worker(worker)
        result = conn.root.reduce(
            task.payload
        )
    except Exception as e:
        print(f"Error while sending a reduce task: {e}")
        # On error, mark pending for retry
        with task_lock:
            task.state = TaskState.PENDING
            task.worker = None
            task.start_time = None
        return

    # Success
    with task_lock:
        task.state = TaskState.DONE
        task.result = result


def run_reduce_phase(reduce_tasks):
    """
    Schedule and monitor all reduce tasks.
    - Assigns pending tasks to workers.
    - Retries tasks if they exceed TASK_TIMEOUT.
    - Returns only when all reduce tasks are DONE.
    """

    def scheduler():
        while True:
            with task_lock:
                pending = [t for t in reduce_tasks if t.state == TaskState.PENDING]
            if not pending:
                time.sleep(0.1)
                continue

            worker = get_available_worker(reduce_tasks)
            if worker is None:
                # all workers busy, wait
                time.sleep(0.1)
                continue

            task = pending[0]

            threading.Thread(
                target=run_reduce_task,
                args=(task, worker),
                daemon=True,
            ).start()

    def monitor():
        while True:
            all_done = True
            now = time.time()
            with task_lock:
                for t in reduce_tasks:
                    if t.state != TaskState.DONE:
                        all_done = False
                    if (
                        t.state == TaskState.RUNNING
                        and now - t.start_time > TASK_TIMEOUT
                    ):
                        # Timeout: reschedule
                        t.state = TaskState.PENDING
                        t.worker = None
                        t.start_time = None
            if all_done:
                return
            time.sleep(1)

    threading.Thread(target=scheduler, daemon=True).start()
    monitor()


def mapreduce_wordcount(file_paths):
    # Split text into chunks
    num_chunks = NUM_WORKERS * OVER_PARTITION
    print(f"Splitting {file_paths} to {num_chunks} parts...")
    chunks = split_text(file_paths, num_chunks)

    # MAP PHASE: Send chunks and get intermediate pairs
    map_tasks = []
    for i, (path, start, end) in enumerate(chunks):
        payload = {
            "index": i,
            "path": path,
            "start": start,
            "end": end
        }
        map_tasks.append(Task(i, "map", payload))

    print(f"Run {len(map_tasks)} map tasks")
    run_map_phase(map_tasks)

    # SHUFFLE PHASE: Group intermediate pairs by key
    print("Shuffle phase...")
    grouped = collections.defaultdict(list)
    for t in map_tasks:
        path = t.result["output"]
        with open(path, "r", encoding="utf-8") as f:
            counts = json.load(f)
        for w, c in counts.items():
            grouped[w].append(c)

    num_parts = NUM_WORKERS * OVER_PARTITION
    buckets = partition_dict(grouped, num_parts)

    # REDUCE PHASE: Send grouped data to reducers
    reduce_tasks = []
    base_id = len(map_tasks)
    for index, bucket in enumerate(buckets):
        name = f"reduce_{index}"
        path = os.path.join(f"{SHARED_PATH}", f"{name}.json")
        reduce_tasks.append(Task(base_id + index, "reduce", path))
        with open(path, "w", encoding="utf-8") as f:
            json.dump(bucket, f)

    print(f"Run {len(reduce_tasks)} reduce tasks")
    run_reduce_phase(reduce_tasks)

    # FINAL AGGREGATION
    print("Final aggregation...")
    final_counts = collections.defaultdict(int)
    for t in reduce_tasks:
        path = t.result["output"]
        with open(path, "r", encoding="utf-8") as f:
            parts = json.load(f)
        for w, c in parts.items():
            final_counts[w] += c

    # Return sorted list of (word, count) descending
    total_counts = sorted(
        final_counts.items(),
        key=operator.itemgetter(1),
        reverse=True,
    )
    return total_counts


def split_text(file_paths, n):
    files = []
    total = 0
    for path in file_paths:
        result_path = shutil.copy2(path, SHARED_PATH)
        try:
            sz = Path(path).stat().st_size
        except FileNotFoundError:
            continue
        if sz > 0:
            files.append((result_path, sz))
            total += sz
    if not files:
        return []

    chunk_bytes = int(math.ceil(total / max(1, n)))

    chunks = []
    for path, sz in files:
        if sz <= chunk_bytes:
            chunks.append((path, 0, sz))
            continue
        n = max(1, math.ceil(sz / chunk_bytes))
        step = math.ceil(sz / n)
        s = 0
        while s < sz:
            e = min(s + step, sz)
            chunks.append((path, s, e))
            s = e
    return chunks


def partition_dict(d, n):
    if n <= 0:
        return []

    buckets = [[] for _ in range(n)]
    for word, counts_list in d.items():
        idx = hash(word) % n    # round robin distribution
        buckets[idx].append((word, counts_list))
    return buckets


def download(urls="https://mattmahoney.net/dc/enwik9.zip"):
    """Downloads and unzips a wikipedia dataset in txt/."""
    if isinstance(urls, str):
        urls = [urls]

    # cleaning the txt/
    for path in glob.glob("txt/*"):
        file = Path(path)
        file.unlink(missing_ok=True)

    for url in urls:
        dest_dir = Path("txt")
        dest_dir.mkdir(parents=True, exist_ok=True)

        zip_path = dest_dir / "data.zip"

        print(f"Downloading {url} to {zip_path}...")
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                    if chunk:  # filter out keep-alive chunks
                        f.write(chunk)

        print("Unzipping the file...")
        with ZipFile(zip_path) as zf:
            zf.extractall(dest_dir)

        zip_path.unlink(missing_ok=True)

        print("File unzipped...")


if __name__ == "__main__":
    # DOWNLOAD AND UNZIP DATASET
    download(sys.argv[1:])
    start_time = time.time()
    input_files = glob.glob('txt/*')
    word_counts = mapreduce_wordcount(input_files)
    print('\nTOP 20 WORDS BY FREQUENCY\n')
    top20 = word_counts[0:20]
    longest = max(len(word) for word, count in top20)
    i = 1
    for word, count in top20:
        print('%s.\t%-*s: %5s' % (i, longest+1, word, count))
        i = i + 1
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Elapsed Time: {} seconds".format(elapsed_time))
