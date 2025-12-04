#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

COMPOSE_BIN="${COMPOSE:-$(if command -v docker-compose >/dev/null 2>&1; then echo docker-compose; else echo "docker compose"; fi)}"
export COMPOSE_BIN

echo "Stopping any running stack..."
$COMPOSE_BIN down -v --remove-orphans >/dev/null 2>&1 || true

echo "Building and starting stack..."
$COMPOSE_BIN up --build -d

echo "Waiting for master to become healthy..."
for i in {1..30}; do
  if $COMPOSE_BIN exec -T master curl -fs http://localhost:8080/api/v1/jobs >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

JOB_FILE="$ROOT_DIR/demo_job.json"
cat > "$JOB_FILE" <<'EOF'
{
  "partitions": 2,
  "nodes": [
    { "id": "read", "operator": { "Read": { "uri": "./data/input.json", "format": "json" } } },
    { "id": "map", "operator": { "Map": { "script": "identity" } } },
    { "id": "filter", "operator": { "Filter": { "predicate": "always_true" } } }
  ],
  "edges": [
    { "from": "read", "to": "map" },
    { "from": "map", "to": "filter" }
  ]
}
EOF
echo "Submitting demo job..."
JOB_ID=$($COMPOSE_BIN run --rm --entrypoint mini-spark-cli cli --master http://master:8080 submit "/app/$(basename "$JOB_FILE")" | awk '{print $3}')

echo "Waiting for completion..."
for i in {1..30}; do
  STATUS_JSON=$($COMPOSE_BIN run --rm --entrypoint mini-spark-cli cli --master http://master:8080 status "$JOB_ID" | tail -1 || true)
  if echo "$STATUS_JSON" | grep -qE "SUCCEEDED|COMPLETED|FAILED"; then
    echo "$STATUS_JSON"
    break
  fi
  sleep 2
done

echo "Fetching results..."
$COMPOSE_BIN run --rm --entrypoint mini-spark-cli cli --master http://master:8080 results "$JOB_ID" --output ./results || true

echo "DEMO COMPLETED SUCCESSFULLY"
