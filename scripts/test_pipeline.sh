#!/usr/bin/env bash
set -euo pipefail

# Simple smoke test that submits a synthetic wordcount DAG.

MASTER_URL="${MASTER_URL:-http://127.0.0.1:8080}"
PARTITIONS="${PARTITIONS:-2}"
INPUT_FILE="${INPUT_FILE:-/tmp/dpe-wordcount.txt}"

echo "preparing input at ${INPUT_FILE}"
echo -e "hello world\nhello rust\nrust spark" > "${INPUT_FILE}"

echo "submitting wordcount"
cargo run -- client example-wordcount --input "${INPUT_FILE}" --partitions "${PARTITIONS}"

echo "query status (manual follow-up recommended)"
echo "curl ${MASTER_URL}/api/v1/jobs/<job_id>"
