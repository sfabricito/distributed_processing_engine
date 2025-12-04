#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

COMPOSE_BIN="${COMPOSE:-$(if command -v docker-compose >/dev/null 2>&1; then echo docker-compose; else echo "docker compose"; fi)}"

echo "Bringing up master + worker..."
$COMPOSE_BIN up --build -d

echo "Waiting for master health..."
for i in {1..30}; do
  if $COMPOSE_BIN exec -T master curl -fs http://localhost:8080/api/v1/jobs >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

JOB_FILE="$ROOT_DIR/cli_test_job.json"
cat > "$JOB_FILE" <<'EOF'
{
  "partitions": 1,
  "nodes": [
    { "id": "read", "operator": { "Read": { "uri": "./data/input.json", "format": "json" } } },
    { "id": "map", "operator": { "Map": { "script": "identity" } } }
  ],
  "edges": [
    { "from": "read", "to": "map" }
  ]
}
EOF

echo "Submitting job via CLI container..."
JOB_SUBMIT=$($COMPOSE_BIN run --rm --entrypoint mini-spark-cli cli --master http://master:8080 submit "/app/$(basename "$JOB_FILE")")
echo "$JOB_SUBMIT"
JOB_ID=$(echo "$JOB_SUBMIT" | awk '{print $3}')

echo "Polling job status..."
for i in {1..30}; do
  STATUS=$($COMPOSE_BIN exec -T master curl -s http://localhost:8080/api/v1/jobs/$JOB_ID | jq -r .status)
  echo "Status: $STATUS"
  if [[ "$STATUS" == "Succeeded" || "$STATUS" == "Completed" || "$STATUS" == "SUCCEEDED" ]]; then
    echo "CLI docker test succeeded (job $JOB_ID)."
    $COMPOSE_BIN down -v --remove-orphans >/dev/null 2>&1 || true
    exit 0
  fi
  if [[ "$STATUS" == "Failed" || "$STATUS" == "FAILED" ]]; then
    echo "CLI docker test failed (job $JOB_ID)."
    $COMPOSE_BIN down -v --remove-orphans >/dev/null 2>&1 || true
    exit 1
  fi
  sleep 2
done

echo "Timed out waiting for job completion."
$COMPOSE_BIN down -v --remove-orphans >/dev/null 2>&1 || true
exit 1
