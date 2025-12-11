#!/bin/bash

# This script runs the ingestion pipeline; it uses the data in seeds/events.csv
# uv run bash runner.sh # to ingest all events
# uv run bash runner.sh -r <round_id> # to ingest a round
# uv run bash runner.sh 12345 67890 # to ingest specific events

# Parse flags
FILTER_VALUE=""
while getopts "r:" opt; do
    case $opt in
        r)
            FILTER_VALUE="$OPTARG"
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            exit 1
            ;;
    esac
done

# Shift past the parsed flags to get positional args
shift $((OPTIND-1))

# Use CLI args if provided, otherwise parse from CSV
if [ $# -gt 0 ]; then
    # Positional arguments provided
    items=("$@")
elif [ -n "$FILTER_VALUE" ]; then
    # Filter by column 2 value AND past timestamps
    MY_LIST=$(tail -n +2 seeds/events.csv | awk -F',' -v val="$FILTER_VALUE" -v now="$(date +%s)" '
        $2 == val {
            cmd = "date -d \"" $3 "\" +%s 2>/dev/null"
            cmd | getline ts
            close(cmd)
            if (ts <= now) print $1
        }
    ' | tr '\n' ' ')
    items=($MY_LIST)
else
    # Parse all from CSV, only past timestamps
    MY_LIST=$(tail -n +2 seeds/events.csv | awk -F',' -v now="$(date +%s)" '
        {
            cmd = "date -d \"" $3 "\" +%s 2>/dev/null"
            cmd | getline ts
            close(cmd)
            if (ts <= now) print $1
        }
    ' | tr '\n' ' ')
    items=($MY_LIST)
fi

# Iterate with index to detect last item
for i in "${!items[@]}"; do
    arg="${items[$i]}"
    
    # Track start time
    start_time=$(date +%s)
    
    uv run python3 ingestion/zrapp.py "$arg"
    uv run python3 ingestion/zpdf.py "$arg"
    
    # Calculate elapsed time and sleep remainder
    if [ $i -lt $((${#items[@]} - 1)) ]; then
        end_time=$(date +%s)
        elapsed=$((end_time - start_time))
        sleep_time=$((70 - elapsed))
        
        if [ $sleep_time -gt 0 ]; then
            echo "Sleeping for $sleep_time seconds..."
            sleep $sleep_time
        else
            echo "Execution took $elapsed seconds (>70), no sleep needed"
        fi
    fi
done

uv run dbt build