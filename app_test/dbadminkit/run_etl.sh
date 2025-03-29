#!/bin/bash
# run_etl.sh

# Loop 6 times (60 seconds / 10 seconds = 6 runs per minute)
for i in {1..6}; do
    /usr/bin/env python3 /home/user/etl_script.py >> /home/user/etl_log.log 2>&1
    sleep 10  # Wait 10 seconds between runs
done