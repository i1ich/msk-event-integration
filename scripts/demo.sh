#!/usr/bin/env bash
# Demo (M1 + M2): post sample events, change Postgres rows (CDC), then show raw, canonical and DLQ topics,
# what the downstream received, and a replay dry run.
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

# Plain-text consumer (raw JSON and DLQ). Prints headers, key and value.
show_text() {
  docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 \
    --topic "$1" --from-beginning --max-messages "$N" --timeout-ms 10000 \
    --property print.key=true --property print.headers=true 2>/dev/null || true
}

# Avro consumer: show_avro <topic> [avro|string] (key format). Canonical topics have plain String keys,
# Debezium keys are Avro. The Confluent tools log at INFO to stdout, so log4j is pointed at a WARN-only stderr config.
# MSYS_NO_PATHCONV stops Git Bash on Windows from rewriting /tmp/... in the script into a Windows path.
show_avro() {
  MSYS_NO_PATHCONV=1 docker compose exec -T schema-registry sh -c '
    printf "log4j.rootLogger=WARN, err\nlog4j.appender.err=org.apache.log4j.ConsoleAppender\nlog4j.appender.err.Target=System.err\nlog4j.appender.err.layout=org.apache.log4j.PatternLayout\n" > /tmp/quiet-log4j.properties
    SCHEMA_REGISTRY_LOG4J_OPTS=-Dlog4j.configuration=file:/tmp/quiet-log4j.properties \
    kafka-avro-console-consumer --bootstrap-server kafka:29092 \
      --topic "$1" --from-beginning --max-messages "$2" --timeout-ms 10000 \
      --property schema.registry.url=http://localhost:8081 --property print.key=true $3
  ' sh "$1" "$N" "$( [ "${2:-avro}" = string ] && echo --key-deserializer=org.apache.kafka.common.serialization.StringDeserializer )" 2>/dev/null || true
}

echo
echo "=== 3. Raw events: sf.change-events.raw (headers, key, value) ==="
show_text sf.change-events.raw

echo
echo "=== 4. CDC events: erp.public.orders (Debezium envelope, Avro) ==="
show_avro erp.public.orders

WAIT="${DEMO_RETRY_WAIT_SECONDS:-25}"
echo
echo "=== 5. Waiting ${WAIT}s for stream processing and delivery retries (local retry delays: 5s / 15s) ==="
sleep "$WAIT"

echo
echo "=== 6. Canonical accounts: integration.account.v1 (Avro) ==="
show_avro integration.account.v1 string

echo
echo "=== 7. Canonical orders: integration.order.v1 (Avro) ==="
show_avro integration.order.v1 string

echo
echo "=== 8. Dead letters: integration.dlq (error headers, original bytes) ==="
show_text integration.dlq

echo
echo "=== 9. What the downstream received (WireMock journal: status, path, Idempotency-Key) ==="
# The journal is pretty-printed JSON, newest first. Each entry has the request ("absoluteUrl" appears only there),
# its Idempotency-Key header, then the response "status" (the first status after it). awk keeps this
# dependency-free (no jq/python); the matched stub mapping also has "url"/"status", which is why absoluteUrl is used.
curl -sS "${WIREMOCK_URL:-http://localhost:8089}/__admin/requests" | tr -d '\r' | awk '
  /"absoluteUrl" *:/     { gsub(/[",]/, "", $3); url = $3; sub(/^https?:\/\/[^\/]+/, "", url); key = "-"; status = "" }
  /"Idempotency-Key" *:/ { gsub(/[",]/, "", $3); key = $3 }
  /"status" *: *[0-9]+/  { if (url != "" && status == "") { gsub(/,/, "", $3); status = $3;
                           lines[n++] = sprintf("  HTTP %s  %-22s Idempotency-Key=%s", status, url, key); url = "" } }
  END { for (i = n - 1; i >= 0; i--) print lines[i] }' || echo "  (could not read the WireMock journal)"

echo
echo "=== 10. Replay dry run (nothing is republished) ==="
JAR="$ROOT/replay-cli/target/replay-cli.jar"
if [ -f "$JAR" ]; then
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092 java -jar "$JAR" --dry-run || true
else
  echo "  $JAR not found; build it with: mvn -B -pl replay-cli -am package -DskipTests"
fi

echo
echo "=== 11. Schema Registry subjects ==="
curl -sS "$SR/subjects"
echo
