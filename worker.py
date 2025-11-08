import rpyc
from collections import defaultdict
import collections
import string
import time
import json
import os

STOP_WORDS = set(
    [
        "a",
        "an",
        "and",
        "are",
        "as",
        "be",
        "by",
        "for",
        "if",
        "in",
        "is",
        "it",
        "of",
        "or",
        "py",
        "rst",
        "that",
        "the",
        "to",
        "with",
    ]
)
TR = "".maketrans(string.punctuation, " " * len(string.punctuation))


class MapReduceService(rpyc.Service):
    def exposed_map(self, map_task):
        """Map step: tokenize and count words in text chunk."""
        counts = collections.Counter()

        if not map_task:
            return {}

        index = map_task["index"]
        chunk_path = map_task["path"]
        start = map_task["start"]
        end = map_task["end"]

        # Process line by line, like file_to_words
        with open(chunk_path, "r", encoding="utf-8") as f:
            f.seek(start)
            if start != 0:
                f.readline()
            while True:
                pos = f.tell()
                if pos > end:
                    break

                line = f.readline()
                if not line:
                    break  # EOF

                # Same logic as your file_to_words
                if line.lstrip().startswith(".."):
                    continue

                line = line.translate(TR)
                for word in line.split():
                    word = word.lower()
                    if word.isalpha() and word not in STOP_WORDS:
                        counts[word] += 1
        output_path = f"{os.path.basename(chunk_path)}_{index}_map_result.json"
        output_path = os.path.join(os.path.dirname(chunk_path), output_path)
        with open(output_path, "w", encoding="utf-8") as out:
            json.dump(counts, out)
        return {"output": output_path}

    def exposed_reduce(self, items_path):
        """Reduce step: sum counts for a subset of words."""
        result = {}

        if not items_path:
            return result

        with open(items_path, "r", encoding="utf-8") as f:
            grouped_items = json.load(f)

        for word, counts_list in grouped_items:
            if isinstance(counts_list, int):
                total = counts_list
            else:
                total = sum(counts_list)
            result[word] = result.get(word, 0) + total
        output_path = f"{os.path.basename(items_path)}_reduce_result.json"
        output_path = os.path.join(os.path.dirname(items_path), output_path)
        with open(output_path, "w", encoding="utf-8") as out:
            json.dump(result, out)
        return {"output": output_path}


if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer

    t = ThreadedServer(MapReduceService, port=18861)
    t.start()
