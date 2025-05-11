#!/bin/bash

fails=0
logfile="project2b_runs.log"
> "$logfile"  # 清空日志文件

for i in {1..20}; do
  echo "=== Run $i ===" | tee -a "$logfile"
  
  if ! make project2b 2>&1 | tee -a "$logfile"; then
    echo "❌ Failed on run $i" | tee -a "$logfile"
    ((fails++))
  fi
done

echo "=== Total failures: $fails ===" | tee -a "$logfile"
