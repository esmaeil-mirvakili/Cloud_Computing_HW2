# Cloud_Computing_HW2
## How to Build & Run

### 1. Prerequisites

- Docker
- Docker Compose
- Bash (for `run.sh`)

Clone this repo and `cd` into it:

```bash
git clone https://github.com/esmaeil-mirvakili/Cloud_Computing_HW2.git
cd Cloud_Computing_HW2
```

### 2. Run the stack
Run the program by:
```bash
NUM_WORKERS=3 DATA_URLS="https://mattmahoney.net/dc/enwik9.zip" ./run.sh
```