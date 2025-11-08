#!/usr/bin/env bash

NUM_WORKERS="${NUM_WORKERS:-8}"
DATA_URLS="${DATA_URLS:-https://mattmahoney.net/dc/enwik9.zip}"
PROJ_NAME="${PROJ_NAME:-$(basename "$PWD")}"
PROJ_NAME="${PROJ_NAME,,}"
SHARED_PATH=/shared

mkdir txt
rm -rf txt/*
mkdir shared
rm -rf shared/*

echo "Running with:"
echo "  NUM_WORKERS = ${NUM_WORKERS}"
echo "  DATA_URLS   = ${DATA_URLS}"

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