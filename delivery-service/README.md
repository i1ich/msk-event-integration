# delivery-service

Plain Java 21 Kafka consumer (no Spring). Reads canonical events from Kafka and delivers them to a downstream
HTTP API, with a retry ladder and a dead-letter queue (DLQ).

## How it works

Two consumers run in one process (two threads, each with its own `KafkaConsumer`):

| Consumer | Group                    | Topics                                      |
|----------|--------------------------|---------------------------------------------|
| main     | `delivery-service`       | `integration.account.v1`, `integration.order.v1` |
| retry    | `delivery-service-retry` | `integration.retry.1m`, `integration.retry.10m`  |

For every record:

1. The value is read as raw `byte[]`, so retry and DLQ records carry the original bytes untouched.
2. It is decoded with `KafkaAvroDeserializer` and turned into JSON (`GenericRecord.toString()`).
3. `POST {DOWNSTREAM_URL}/accounts` (account events) or `/orders` (order events). The type comes from the
   ORIGINAL topic: the `source-topic` header on retry topics, the record topic otherwise.
   Headers: `Content-Type: application/json`, `Idempotency-Key: <eventId>`.
4. The result is classified (`Classifier`, a pure function):
   - 2xx: success, nothing else to do.
   - 429, 5xx, timeout, IOException: retryable.
   - any other 4xx: permanent, straight to the DLQ.
   - a record that cannot be decoded (poison pill) or has an unknown topic: permanent.

### Retry ladder (3 attempts in total)

```
integration.account.v1 / order.v1 --attempt 1 fails--> integration.retry.1m
integration.retry.1m              --attempt 2 fails--> integration.retry.10m
integration.retry.10m             --attempt 3 fails--> integration.dlq
any permanent failure                                -> integration.dlq
```

A retry record has the same key and value bytes, the original headers, plus `source-topic` (canonical topic),
`attempts` (failed attempts so far) and `not-before` (ISO-8601 instant, now + delay).

The topic names are fixed. Only the delay is configurable (`RETRY_DELAY_1M`, `RETRY_DELAY_10M`). Locally,
docker compose sets short delays (for example `PT5S` and `PT15S`) so the demo takes seconds; the topic called
`retry.1m` then does NOT wait one minute locally. In production leave the defaults.

DLQ records are built only by `DeadLetters.record(...)` from `common`: original key/value/headers plus
`error-class`, `error-message`, `source-topic` (the canonical topic, so a replay knows where to publish),
`source-partition`/`source-offset` (position in the topic where the last attempt failed), `failed-at`, `attempts`.

### The not-before gate (retry consumer)

If the first record of a partition has a `not-before` in the future, the consumer `seek`s back to that offset,
`pause`s the partition and remembers when to `resume` it. It keeps calling `poll()` so it stays in the group.
Blocking the head is correct: all records in one retry topic have the same delay, so they are ordered by due
time and nothing behind the head can be due earlier.

### Offsets

`enable.auto.commit=false`. Offsets are committed synchronously after every record of the poll batch is handled:
delivered, or its retry/DLQ write is acknowledged by the broker (the producer is idempotent, `acks=all`; the
loop waits on the send futures).

This is at-least-once. A crash between the POST and the commit means the batch is read again and the POST is
repeated. The downstream must deduplicate by `Idempotency-Key` (the event id).

### Shutdown

A shutdown hook calls `consumer.wakeup()` on both consumers; the loops end, close their consumers, then the
producer is closed. If a loop dies unexpectedly (for example a retry write failed) the process exits with
code 1 and is restarted by the orchestrator; uncommitted records are processed again.

## What can go wrong

- **Duplicates.** Crash after POST, before commit; or a rebalance mid-batch. Mitigation: `Idempotency-Key`
  on the downstream side. Also, a retry after a timeout may duplicate a request the downstream actually processed.
- **Head-of-line blocking in retry topics.** A not-due head record pauses the whole partition. This is intended
  (equal delay, FIFO), but it means a retry can be up to one poll interval (500 ms) late, and a partition is
  idle while waiting. Ordering across the main topic and retry topics is NOT preserved: a newer event for the
  same account may be delivered before an older one that is waiting for retry.
- **Downstream outage.** Every event walks the ladder and, after about 11 minutes with default delays, lands
  in the DLQ: a DLQ flood. After the downstream is healthy, replay the DLQ with `replay-cli`. The Idempotency-Key
  makes a replay of already-delivered events safe.
- **Rebalances during a pause.** Pause state is lost for partitions that move. The new owner reads the head
  record again and pauses again, so nothing is delivered early. Offsets of paused records were never committed.
- **Slow batches.** A poll batch is up to 50 records, each with up to 5 s HTTP timeout, so a batch can take
  minutes during a brownout. It must stay below `max.poll.interval.ms` (5 min default), otherwise the consumer
  is kicked out of the group and the batch is redelivered.
- **Poison pills.** Bytes that are not valid Avro, an unknown schema id, or a missing `eventId` go to the DLQ
  with attempts=1 and the exception in `error-message`; they never block the partition.
- **Retry write failure.** If Kafka cannot take the retry/DLQ write, the offset is not committed and the
  process exits; the record is processed again after restart.

## Environment variables

| Variable                  | Default     | Meaning |
|---------------------------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | required    | Kafka bootstrap servers |
| `KAFKA_SECURITY`          | `plaintext` | `plaintext` or `iam` (Amazon MSK) |
| `SCHEMA_REGISTRY_TYPE` | `confluent` | `confluent` or `glue` (AWS Glue Schema Registry). Chosen per deployment: the wire formats are incompatible, so a topic written with one registry cannot be read with the other |
| `SCHEMA_REGISTRY_URL` | required for `confluent` | Confluent Schema Registry |
| `GLUE_REGISTRY_NAME` | required for `glue` | Glue registry; schema name = topic name (default naming strategy), compatibility `BACKWARD`, format `AVRO` |
| `AWS_REGION` | required for `glue` | region of the Glue registry; credentials from the default AWS provider chain (task role) |
| `DOWNSTREAM_URL`          | required    | Base URL, e.g. `http://wiremock:8080/downstream` |
| `RETRY_DELAY_1M`          | `PT1M`      | ISO-8601 delay stored in `not-before` for `retry.1m` |
| `RETRY_DELAY_10M`         | `PT10M`     | ISO-8601 delay stored in `not-before` for `retry.10m` |
| `HTTP_TIMEOUT`            | `PT5S`      | HTTP request timeout (ISO-8601) |
| `HTTP_CONNECT_TIMEOUT`    | `PT2S`      | HTTP connect timeout (ISO-8601) |

## Build and run

```
mvn -pl delivery-service -am verify          # tests + target/delivery-service.jar
docker build -f delivery-service/Dockerfile . # build context = repo root
```

Local demo: `infra/wiremock` has two magic account ids, `001xx000000FAIL1AAA` (503 once, then 200) and
`001xx000000REJECTAAA` (always 400).
