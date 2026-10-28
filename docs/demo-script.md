# 5-minute live demo

Everything runs locally (Docker Compose). Nothing here needs AWS. All commands were run end to end against a fresh stack before this script was written.

## Before the call

Prerequisites: Docker Desktop running, Java 21, Maven 3.9+, Git Bash (or any bash), a terminal in the repository root, a browser tab for the Kafka UI.

Start from a fresh stack. Do this at least 10 minutes before the call, the first build takes a few minutes:

```bash
bash scripts/down.sh                                    # removes old volumes and topics
bash scripts/up.sh                                      # build and start everything, register connector and schemas
mvn -B -q -pl replay-cli -am package -DskipTests        # builds replay-cli/target/replay-cli.jar
docker compose --profile ui up -d kafka-ui              # http://localhost:8090
```

Why fresh: the console consumers below read topics from the beginning, so old data would show up first, and the flaky downstream account answers `503` only once per WireMock lifetime (after that it always answers `200`). To reset only the downstream without rebuilding: `docker compose restart wiremock`.

Paste this helper once into the demo terminal. It reads Avro topics through the Schema Registry container (quiet logging, Windows path conversion off):

```bash
avro() {  # avro <topic> [max-messages] [string]   (third argument: plain String keys, for the canonical topics)
  MSYS_NO_PATHCONV=1 docker compose exec -T schema-registry sh -c '
    printf "log4j.rootLogger=WARN, err\nlog4j.appender.err=org.apache.log4j.ConsoleAppender\nlog4j.appender.err.Target=System.err\nlog4j.appender.err.layout=org.apache.log4j.PatternLayout\n" > /tmp/q.properties
    SCHEMA_REGISTRY_LOG4J_OPTS=-Dlog4j.configuration=file:/tmp/q.properties \
    kafka-avro-console-consumer --bootstrap-server kafka:29092 --topic "$1" --from-beginning --max-messages "$2" \
      --timeout-ms 10000 --property schema.registry.url=http://localhost:8081 --property print.key=true $3
  ' sh "$1" "${2:-10}" "$([ "${3:-}" = string ] && echo --key-deserializer=org.apache.kafka.common.serialization.StringDeserializer)" 2>/dev/null
}
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

Fallback if anything goes wrong live: `bash scripts/demo.sh` runs the whole flow in about a minute and prints every stage (it uses the same samples and topics).

## Timeline

### 0:00 The story in one sentence (20 s)

Salesforce-style change events arrive over HTTP, order rows arrive through Debezium CDC. Kafka Streams turns both into canonical Avro topics, a delivery service pushes them to a downstream HTTP API with retries and a DLQ, and a CLI replays failures. Show the diagram in the [README](../README.md#architecture).

### 0:20 Ingress: 202 and 400 (40 s)

```bash
curl -si -X POST http://localhost:8088/events -H 'Content-Type: application/json' --data-binary @samples/account-create.json
curl -si -X POST http://localhost:8088/events -H 'Content-Type: application/json' --data-binary @samples/invalid-missing-header.json
```

Point at:
- `202 Accepted` with `{"eventId", "topic", "partition", "offset"}`: the answer is sent only after the broker acknowledged the write.
- `400` with `{"error":"validation_failed","details":[...]}`: bad input never reaches Kafka.

Then send the rest of the samples (all `202`):

```bash
for f in account-create-with-industry account-update-flaky-downstream account-update-rejected-downstream account-unknown-entity; do
  curl -s -o /dev/null -w "$f %{http_code}\n" -X POST http://localhost:8088/events -H 'Content-Type: application/json' --data-binary @samples/$f.json
done
```

### 1:00 Raw topic and its headers (30 s)

```bash
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic sf.change-events.raw \
  --from-beginning --max-messages 2 --timeout-ms 10000 --property print.key=true --property print.headers=true 2>/dev/null
```

Point at: headers `event-id`, `received-at`, `source`, `transaction-key`; the key is the Salesforce record id (per-record ordering); the body is the JSON exactly as received.

### 1:30 CDC: change a database row (40 s)

```bash
P="docker compose exec -T postgres psql -U erp -d erp -At"
$P -c "INSERT INTO orders (account_id, status, amount, currency) VALUES ('001000000000009AAA','NEW',42.00,'EUR') RETURNING id;"
$P -c "UPDATE orders SET status='PAID', amount=45.50, updated_at=now() WHERE id=(SELECT max(id) FROM orders);"
sleep 5
avro erp.public.orders 8 | tail -2
```

Point at: the Debezium envelope with `before` / `after` and `source` (connector `postgresql`, `2.5.4.Final`); amounts are strings (exact decimals); `avro erp.public.orders 8` without `tail` would also show the initial snapshot of the three seed rows.

### 2:10 Canonical Avro events (50 s)

```bash
avro integration.account.v1 10 string | grep -i industry
avro integration.order.v1 10 string | tail -2
```

Point at:
- Account events with a stable `eventId` (it becomes the downstream `Idempotency-Key`), `changeType`, optional fields as unions (`{"string": ...}` or `null`).
- `"industry":{"string":"Energy"}` on the Globex event and `"industry":null` on the others: `AccountEvent` v2 added the field as optional with a default, a `BACKWARD`-compatible change that the registry accepted ([schema evolution runbook](runbooks/schema-evolution.md)).
- Order events have an `op` (`CREATE`, `UPDATE`, ...) and a deterministic `eventId` derived from the source position, so a replay produces the same idempotency key.

### 3:00 Dead letters with error headers (40 s)

```bash
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic integration.dlq \
  --from-beginning --max-messages 2 --timeout-ms 10000 --property print.headers=true 2>/dev/null
```

Point at the two entries and their headers:
- `UnmappedEntityException`, `No mapping for entity 'Opportunity'`, `source-topic:sf.change-events.raw`: written by the stream-processor.
- `DownstreamException`, `Downstream returned HTTP 400`, `source-topic:integration.account.v1`: written by the delivery-service (permanent error, no retry).
- `source-partition`, `source-offset`, `failed-at`, `attempts`: enough to find the original and to filter a replay. The body is the original bytes.

### 3:40 Retry: same Idempotency-Key, 503 then 200 (35 s)

```bash
curl -sS http://localhost:8089/__admin/requests | tr -d '\r' | awk '
  /"absoluteUrl" *:/     { gsub(/[",]/, "", $3); url = $3; sub(/^https?:\/\/[^\/]+/, "", url); key = "-"; status = "" }
  /"Idempotency-Key" *:/ { gsub(/[",]/, "", $3); key = $3 }
  /"status" *: *[0-9]+/  { if (url != "" && status == "") { gsub(/,/, "", $3); status = $3; lines[n++] = sprintf("  HTTP %s  %-22s Idempotency-Key=%s", status, url, key); url = "" } }
  END { for (i = n - 1; i >= 0; i--) print lines[i] }'
```

(The same listing is section 9 of `scripts/demo.sh`.) The journal is oldest first. Point at the flaky account (`001xx000000FAIL1AAA`): one `503`, then after the retry hop (5 s locally, 1 minute in production) a `200`, both with the same `Idempotency-Key`. The rejected account shows a single `400` and no retry. Each event is delivered at least once; the downstream deduplicates by key.

### 4:15 Replay (35 s)

```bash
java -jar replay-cli/target/replay-cli.jar --dry-run
```

Point at: one line per DLQ record (`partition`, `offset`, `source-topic`, `error-class`, `event-id`, `key`) and the summary `scanned=2 matched=2 replayed=0`. Nothing is published, and the tool uses no consumer group.

Optional real replay of one record:

```bash
java -jar replay-cli/target/replay-cli.jar --error-class UnmappedEntityException --max 1 --rate 2
```

Expected: `Summary: scanned=1 matched=1 replayed=1 ... failed=0`. The record goes back to `sf.change-events.raw` with a `replayed-at` header and the same `event-id`. Because `Opportunity` is still unmapped it fails again and a new DLQ entry appears (a second dry run shows `matched=2`). Say it out loud: replay is for after the root cause is fixed ([runbook](runbooks/dlq-replay.md)).

### 4:50 Kafka UI (10 s)

Open http://localhost:8090: topics, messages, the Schema Registry subjects, the Connect connector `orders-source`. Close with the point that the AWS version of this (MSK, Glue, MSK Connect, Fargate) is written as CloudFormation and lint-clean, but not deployed yet: the first run stopped because the account is on the AWS Free plan, which has no MSK (see [cost.md](cost.md)).

## Likely questions

Longer answers are in [talking-points.md](talking-points.md).

- Why Kafka Streams and not Lambda for processing? Lambda is right for request-driven ingress; processing is a continuous consumer that needs offsets, ordering per key and exactly-once read-process-write. [ADR 0001](adr/0001-kafka-streams-vs-lambda.md).
- Is it exactly-once end to end? No. `exactly_once_v2` covers consume, process, produce and the DLQ writes inside Kafka. The HTTP delivery is at-least-once and relies on the `Idempotency-Key`. [architecture](architecture.md).
- What about ordering? Per key only. After a retry an event can arrive after a newer one for the same key, so the downstream must tolerate out-of-order events. [ADR 0003](adr/0003-retry-topics.md).
- What happens when the downstream is down for an hour? Each event walks `retry.1m`, `retry.10m` and lands in the DLQ after about 11 minutes; replay after recovery, rate limited.
- How do you change a schema safely? Optional field with a default, check compatibility against the registry, register, upgrade consumers first, then producers. [runbook](runbooks/schema-evolution.md).
- Why Glue on AWS and Confluent locally? Managed and IAM-secured on AWS, no Glue emulator locally; wire formats differ, so the registry is chosen per deployment. [ADR 0002](adr/0002-glue-vs-confluent-sr.md).
- What if the Debezium connector stops? The replication slot keeps WAL and can fill the disk; check slot lag, restart, drop the slot only as a last resort. [runbook](runbooks/connector-restart-and-slot-lag.md).
- What does the AWS version cost, and has it run? About 0.55 USD per hour, about 1.7 USD per demo run, guarded by budgets and a same-day teardown. Not deployed yet; the first run was blocked by the Free plan. [cost.md](cost.md).
- How is IAM authentication handled? One switch, `KAFKA_SECURITY=iam`, with per-component least-privilege policies; plaintext locally. Not yet exercised on AWS. [ADR 0004](adr/0004-iam-auth.md).

## After the call

Leave the stack running or stop it with `bash scripts/down.sh` (removes volumes).
