#!/usr/bin/env bash

uv run python3 ingestion/google_sheets.py

for arg in "$@"; do
    uv run python3 ingestion/zrapp.py $arg
    sleep 62
    done
    
uv run dbt build
