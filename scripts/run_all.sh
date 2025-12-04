#!/usr/bin/env bash
set -euo pipefail

# Basic launcher for one master and N workers on the same machine.
# Usage: scripts/run_all.sh [num_workers]

NUM_WORKERS=${1:-2}
CARGO_BIN="cargo run --"

echo "Starting master..."
${CARGO_BIN} master &> /tmp/dpe-master.log &
MASTER_PID=$!
echo "Master pid: ${MASTER_PID}"

for i in $(seq 0 $((NUM_WORKERS - 1))); do
  PORT=$((9100 + i))
  echo "Starting worker ${i} on port ${PORT}"
  ${CARGO_BIN} worker --port ${PORT} &> "/tmp/dpe-worker-${i}.log" &
done

echo "Processes launched. Check /tmp/dpe-*.log for output."
