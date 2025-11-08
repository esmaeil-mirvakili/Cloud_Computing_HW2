#!/usr/bin/env bash

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 NUM_WORKERS [url1 [url2 ...]]"
    echo "Example: $0 3 https://mattmahoney.net/dc/enwik9.zip"
    exit 1
fi

NUM_WORKERS="$1"
shift

# Join remaining args as a single string of URLs
if [ "$#" -eq 0 ]; then
    DATA_URLS="https://mattmahoney.net/dc/enwik9.zip"  # coordinator will use its default
else
    DATA_URLS="$*"
fi

mkdir txt
rm -rf txt/*
mkdir shared
rm -rf shared/*

echo "Running with:"
echo "  NUM_WORKERS = ${NUM_WORKERS}"
echo "  DATA_URLS   = ${DATA_URLS}"

PROJ_NAME="${PROJ_NAME:-$(basename "$PWD")}"
SHARED_PATH=/shared

# Export env vars for docker compose
export NUM_WORKERS
export DATA_URLS
export PROJ_NAME
export SHARED_PATH

# Bring up the stack, scaling worker service
docker compose -p "${PROJ_NAME}" up --scale worker="${NUM_WORKERS}" \
    --build --remove-orphans --abort-on-container-exit\
    2> >(grep -v 'ERRO\[.*\] 0' >&2)

rm -rf txt
rm -rf shared