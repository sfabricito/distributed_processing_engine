#!/usr/bin/env bash
set -euo pipefail

# Assumes docker compose stack is already up (master + at least one worker).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

COMPOSE_BIN="${COMPOSE:-$(if command -v docker-compose >/dev/null 2>&1; then echo docker-compose; else echo "docker compose"; fi)}"
MASTER_URL="${MASTER_URL:-http://master:8080}"

JOB_FILE="$ROOT_DIR/cli_live_job.json"
cat > "$JOB_FILE" <<'EOF'
{
    "nodes": [
      { "id": "read",   "operator": { "Read": { "uri": "./data/input.csv", "format": "csv" } } },
      { "id": "filter", "operator": { "Filter": { "predicate": "equals:Product=Monitor" } } },
      { "id": "map",    "operator": { "Map": { "script": "uppercase" } } }
    ],
    "edges": [
      { "from": "read",   "to": "filter" },
      { "from": "filter", "to": "map" }
    ],
    "partitions": 1
  }
EOF

echo "Submitting job via CLI container to ${MASTER_URL}..."
JOB_SUBMIT=$($COMPOSE_BIN run --rm --entrypoint mini-spark-cli cli --master "$MASTER_URL" submit "/app/$(basename "$JOB_FILE")")
echo "$JOB_SUBMIT"
JOB_ID=$(echo "$JOB_SUBMIT" | awk '{print $3}')

if [[ -z "$JOB_ID" ]]; then
  echo "Failed to parse job id from submit output" >&2
  exit 1
fi

echo "Polling job status for $JOB_ID..."
for i in {1..30}; do
  STATUS=$($COMPOSE_BIN exec -T master curl -s http://localhost:8080/api/v1/jobs/$JOB_ID | jq -r '.status // "unknown"')
  echo "Status: ${STATUS}"
  case "$STATUS" in
    Succeeded|Completed|SUCCEEDED|COMPLETED)
      echo "CLI docker test succeeded (job $JOB_ID)."
      exit 0
      ;;
    Failed|FAILED)
      echo "CLI docker test failed (job $JOB_ID)." >&2
      exit 1
      ;;
  esac
  sleep 2
done

echo "Timed out waiting for job completion." >&2
exit 1
