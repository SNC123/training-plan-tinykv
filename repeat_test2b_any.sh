#!/bin/bash

set -e

TEST_NAME=$1         # 传入的测试名，例如 TestBasic2B
REPEAT=${2:-10}      # 重复次数，默认10次
fails=0
if [ -z "$TEST_NAME" ]; then
  echo "Usage: $0 <TestName> [RepeatCount]"
  exit 1
fi

echo "=== Repeating test $TEST_NAME $REPEAT times ==="

for i in $(seq 1 $REPEAT); do
  echo "[$TEST_NAME] Run $i/$REPEAT"
  go test -count=1 ./kv/test_raftstore -run "^$TEST_NAME$"  || {
    echo "❌ $TEST_NAME failed on run $i"
    # exit 1
    ((fails++))
  }
done

echo "=== Total failures: $fails ===" 