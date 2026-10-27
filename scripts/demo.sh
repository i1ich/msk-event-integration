#!/usr/bin/env bash
# M1 demo: post sample events to ingress, change a Postgres row (CDC), show Kafka topics and subjects.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=wait-for.sh
. "$ROOT/scripts/wait-for.sh"

INGRESS_URL="${INGRESS_URL:-http://localhost:${INGRESS_HOST_PORT:-8088}}"
SR="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
N="${DEMO_MAX_MESSAGES:-10}"

wait_for_url "$INGRESS_URL/health" 120

echo
echo "=== 1. POST samples to $INGRESS_URL/events ==="
shopt -s nullglob
for f in "$ROOT"/samples/*.json; do
  hdrs=(-H 'Content-Type: application/json')
  if [ -n "${INGRESS_HMAC_SECRET:-}" ]; then
    sig=$(openssl dgst -sha256 -hmac "$INGRESS_HMAC_SECRET" -hex < "$f" | sed 's/^.*= *//')
    hdrs+=(-H "X-Signature: sha256=$sig")
  fi
  if [ -n "${INGRESS_API_KEY:-}" ]; then
    hdrs+=(-H "x-api-key: $INGRESS_API_KEY")
  fi
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "${hdrs[@]}" --data-binary "@$f" "$INGRESS_URL/events" || true)
  printf '  %-40s HTTP %s\n' "$(basename "$f")" "$code"
done

echo
echo "=== 2. Postgres changes (INSERT, UPDATE, DELETE) ==="
PSQL=(docker compose exec -T postgres psql -U erp -d erp -v ON_ERROR_STOP=1 -At)
"${PSQL[@]}" -c "INSERT INTO orders (account_id, status, amount, currency) VALUES ('001000000000009AAA','NEW',42.00,'EUR') RETURNING id;"
ID=$("${PSQL[@]}" -c "SELECT max(id) FROM orders WHERE account_id='001000000000009AAA';" | tr -d '\r')
"${PSQL[@]}" -c "UPDATE orders SET status='PAID', amount=45.50, updated_at=now() WHERE id=$ID;"
"${PSQL[@]}" -c "DELETE FROM orders WHERE id=$ID;"
echo "  order id=$ID inserted, updated, deleted"
sleep 5

echo
echo "=== 3. Messages of sf.change-events.raw (headers, key, value) ==="
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic sf.change-events.raw --from-beginning --max-messages "$N" --timeout-ms 10000 \
  --property print.key=true --property print.headers=true 2>/dev/null || true

echo
echo "=== 4. Messages of erp.public.orders (Avro) ==="
# The Confluent tools log at INFO to stdout; point log4j at a WARN-only stderr config to keep the output readable.
# MSYS_NO_PATHCONV: stop Git Bash on Windows from rewriting /tmp/... inside the script into a Windows path.
MSYS_NO_PATHCONV=1 docker compose exec -T schema-registry sh -c '
  printf "log4j.rootLogger=WARN, err
log4j.appender.err=org.apache.log4j.ConsoleAppender
log4j.appender.err.Target=System.err
log4j.appender.err.layout=org.apache.log4j.PatternLayout
" > /tmp/quiet-log4j.properties
  SCHEMA_REGISTRY_LOG4J_OPTS=-Dlog4j.configuration=file:/tmp/quiet-log4j.properties   kafka-avro-console-consumer --bootstrap-server kafka:29092     --topic erp.public.orders --from-beginning --max-messages "$1" --timeout-ms 10000     --property schema.registry.url=http://localhost:8081 --property print.key=true
' sh "$N" 2>/dev/null || true

echo
echo "=== 5. Schema Registry subjects ==="
curl -sS "$SR/subjects"
echo
