# Architecture

Status: M1 in progress. Sections marked `TODO (M2)` describe planned behavior that is not yet implemented.

## Components

### ingress-lambda

**How it works.** Exposed through API Gateway REST `POST /events`. Validates the payload, optionally verifies an HMAC signature (`INGRESS_HMAC_SECRET`), and produces to `sf.change-events.raw` with an idempotent Kafka producer. Returns `202` only after the broker acknowledges the write, and `503` if the broker call fails.

**What can go wrong.**
- Broker unavailable or slow: the caller gets `503` and must retry. Nothing is acknowledged that was not written.
- Client retries after a timeout: the same event can be sent twice. The idempotent producer prevents duplicates from producer retries within one session, but not from client-level retries. Downstream deduplication is by event id.
- HMAC secret misconfigured or absent: requests are rejected (if HMAC is enabled) or accepted without authentication (if it is not). Enable it in any shared environment.

### PostgreSQL and Debezium

**How it works.** The `orders` table is read through logical replication (`pgoutput`) by the Debezium PostgreSQL connector (`orders-source`), running on Kafka Connect locally and on MSK Connect in AWS. Changes are published to `erp.public.orders` as Avro.

**What can go wrong.**
- Connector stops: the replication slot keeps retaining WAL and disk on the database grows. See [connector restart and slot lag](runbooks/connector-restart-and-slot-lag.md).
- Slot dropped: changes not yet consumed are lost from the CDC stream, and the connector must re-snapshot.
- Schema change on `orders` without a matching schema change in `schemas/order/`: events fail to serialize or consumers fail to read. See [schema evolution](runbooks/schema-evolution.md).

### stream-processor (Kafka Streams)

**How it works.** Runs with `processing.guarantee=exactly_once_v2`. Two topologies:
- Topology A: `sf.change-events.raw` to `integration.account.v1` (Avro `AccountEvent`).
- Topology B: `erp.public.orders` to `integration.order.v1` (Avro `OrderEvent`).

Records that cannot be transformed are routed to `integration.dlq` with the error headers described below.

**What can go wrong.**
- Poison message: a record that always fails is routed to the DLQ, not retried forever. Verify that deserialization errors are caught per record. `TODO (M2)`: confirm the error-handling wiring.
- Rebalance or restart: processing resumes from the last committed transaction. Records may be reprocessed, but transactional output is not duplicated within Kafka.
- Schema mismatch with the registry: serialization fails. Check compatibility before deploying a producer change.
- Scaling beyond partition count: extra tasks sit idle. Partition count is set in the topics stack.

### delivery-service

**How it works.** Consumes `integration.account.v1` and `integration.order.v1` with manual commits (at-least-once). Each event is POSTed to the downstream HTTP service with `Idempotency-Key` set to the event id.
- `4xx` (except `429`): sent to `integration.dlq`.
- `5xx`, `429`, or timeout: sent to `integration.retry.1m`, then `integration.retry.10m`, then `integration.dlq`.

**What can go wrong.**
- Crash after the HTTP call but before the commit: the event is delivered again. The downstream service must honor `Idempotency-Key`.
- Retry topics reorder events: a retried event for a key can be processed after later events for the same key. See [ordering](#ordering-per-key-and-partitions).
- Downstream outage: events move through retry hops and can reach the DLQ. Replay them after recovery with replay-cli.
- Downstream ignores `Idempotency-Key`: duplicate side effects are possible. This is a contract with the downstream service, not something this platform can enforce.

### replay-cli

**How it works.** Reads messages from `integration.dlq` and republishes them to the source topic (from the `source-topic` header). Supports rate limiting, filters, and `--dry-run`. Adds a `replayed-at` header to each republished record.

**What can go wrong.**
- Replaying without fixing the cause: the message fails again and returns to the DLQ.
- Replaying into a stream that has moved on: the event may now be stale. Filter by `failed-at` where needed.
- Without `--dry-run`, a wrong filter republishes a large set. Always dry-run first.
- `TODO (M2)`: confirm how replayed messages keep their original key and partition routing.

### observability

`TODO (M4)`: metrics, dashboards, and alarms (consumer lag, DLQ depth, connector status, replication slot lag). Local equivalent: Kafka UI on port `8090`.

## Ordering per key and partitions

- Kafka guarantees order only within a partition. Records with the same key go to the same partition, so they are ordered relative to each other on the source topic.
- Keys: the raw topic uses the first Salesforce record id from `ChangeEventHeader.recordIds`. CDC uses the `orders` primary key. Canonical topics use the account id or order id.
- Topology A and B preserve per-key order as long as they do not reroute records.
- Delivery retries break per-key order. A retried event is republished to a retry topic and processed after events that arrived later. Downstream must tolerate out-of-order arrival (for example, by comparing a version or timestamp).
- The DLQ is not ordered with respect to the main flow, and replay does not restore original order.

## Delivery semantics

| Stage | Guarantee | Notes |
|---|---|---|
| ingress to raw topic | At least once from the client's view; no duplicates from producer retries within a session (idempotent producer) | Client retries can still duplicate. Deduplicate downstream by event id. |
| Kafka Streams read-process-write | Exactly once (`exactly_once_v2`) | Scope is Kafka only: reads, state, and writes to Kafka topics within one transaction. |
| stream-processor to canonical topics | Exactly once within Kafka | Does not cover the downstream HTTP call. |
| delivery-service to downstream HTTP | At least once | Manual commit after the HTTP outcome. Duplicates possible. |

**`exactly_once_v2` does not cover external HTTP calls.** A delivery is an external side effect. If the process crashes after the HTTP call succeeds and before the offset is committed, the event is sent again. The `Idempotency-Key` (event id) header is the mechanism that lets the downstream service suppress the duplicate.

## DLQ header contract

Every record written to `integration.dlq` carries these headers:

| Header | Meaning |
|---|---|
| `error-class` | Exception class name that caused the failure |
| `error-message` | Exception message (truncated; `TODO (M2)`: confirm limit) |
| `source-topic` | Topic the record was read from; replay returns it here |
| `source-partition` | Partition of the source record |
| `source-offset` | Offset of the source record |
| `failed-at` | ISO-8601 UTC timestamp of the failure |
| `attempts` | Number of delivery or processing attempts made |

Added by replay-cli on republish:

| Header | Meaning |
|---|---|
| `replayed-at` | ISO-8601 UTC timestamp of the replay |

`TODO (M2)`: confirm the value formats and whether `attempts` is carried through the retry topics.
