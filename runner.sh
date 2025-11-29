#!/usr/bin/env bash

uv run python3 ingestion/google_sheets.py

for arg in "$@"; do
    sleep 61
    uv run python3 ingestion/zrapp.py $arg
    uv run python3 ingestion/zpdf.py $arg
    done
    
uv run dbt build
#uv run modal deploy modal/web_app.py
#uv run fly deploy