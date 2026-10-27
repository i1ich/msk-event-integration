#!/usr/bin/env bash
# Idempotently create/update the Debezium orders-source connector (PUT /connectors/<name>/config).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=wait-for.sh
. "$ROOT/scripts/wait-for.sh"

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
NAME="orders-source"
FILE="$ROOT/infra/connect/orders-source.local.json"

wait_for_url "$CONNECT_URL/connectors" 180

# Extract the "config" object (everything between '"config": {' and its closing 2-space-indented '}') without jq.
BODY="{
$(awk '/^  "config": *\{/{f=1; next} f && /^  \}/{f=0} f' "$FILE" | tr -d '\r')
}"

echo "Registering connector $NAME"
curl -sS -f -X PUT -H 'Content-Type: application/json' --data "$BODY" \
  "$CONNECT_URL/connectors/$NAME/config" >/dev/null
echo "Registered."

# Give the task a moment to start, then print status.
for _ in 1 2 3 4 5 6 7 8 9 10; do
  status=$(curl -s "$CONNECT_URL/connectors/$NAME/status" || true)
  case "$status" in *'"state":"RUNNING"'*'"tasks":[{'*) break ;; esac
  sleep 2
done
echo "$status"
echo
