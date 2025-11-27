#!/usr/bin/env bash

uv run python3 ingestion/google_sheets.py

for arg in "$@"; do
    uv run python3 ingestion/zrapp.py $arg
    uv run python3 ingestion/zpdf.py $arg
    sleep 61
    done
    
uv run dbt build
