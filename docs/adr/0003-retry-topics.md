# ADR 0003: Retry topics instead of in-process or single-topic retry

- Status: Accepted (final state, M5)
- Date: M1

## Context

`delivery-service` calls a downstream HTTP service. Some failures are transient (`5xx`, `429`, timeouts, I/O errors) and some are permanent (other `4xx`, undecodable records). Transient failures need a delay before the next attempt. Options considered:

1. Retry in process with sleeps: blocks the partition, delays all keys on it, and loses the retry state on a crash.
2. Retry on the same topic: the record is re-consumed after a fixed backoff, but it blocks the partition during that time.
3. Retry topics with fixed delays: each hop is a separate topic. A consumer reads a hop only after its delay has elapsed, and forwards the record to the next hop or the DLQ on failure.

## Decision

Use retry topics, with 3 delivery attempts in total (`RetryRouter.MAX_ATTEMPTS = 3`):

```text
integration.account.v1 / order.v1 --attempt 1 fails--> integration.retry.1m
integration.retry.1m              --attempt 2 fails--> integration.retry.10m
integration.retry.10m             --attempt 3 fails--> integration.dlq
any permanent failure                                -> integration.dlq
```

- A retry record keeps key, value bytes and original headers, and adds `source-topic`, `attempts` and `not-before` (now + delay).
- The retry consumer (group `delivery-service-retry`) pauses a partition while its head record is not yet due, but keeps polling, so it stays in the group. All records of one retry topic have the same delay, so blocking the head is correct.
- Delays are configurable (`RETRY_DELAY_1M`, `RETRY_DELAY_10M`; defaults `PT1M` and `PT10M`). The topic names are fixed. `docker-compose.yml` sets `PT5S` / `PT15S` so the local demo takes seconds; the integration tests use `PT2S` / `PT4S`.
- Offsets are committed manually, only after the outcome (delivered, retry hop, or DLQ) is durably written. The producer is idempotent with `acks=all`.
- DLQ records carry `error-class`, `error-message`, `source-topic`, `source-partition`, `source-offset`, `failed-at`, `attempts`, which `replay-cli` uses to re-drive them.

## Consequences

Positive:
- A failing downstream does not block the main partition for long; the main flow keeps moving.
- Retry state is in Kafka, so a crash does not lose it.
- Terminal failures are explicit in `integration.dlq` with headers that support replay.

Negative:
- Ordering per key is not preserved across retries. A retried event can arrive after newer events for the same key. Downstream must handle out-of-order arrival.
- Delivery is at-least-once: a crash between the POST and the commit repeats the POST. The downstream must deduplicate by `Idempotency-Key` (the event id).
- Delays are approximate: a hop delivers after at least its nominal delay, and later under consumer lag.
- A full downstream outage sends every event down the ladder and floods the DLQ after about 11 minutes with default delays. Recovery is a replay (see the [DLQ replay runbook](../runbooks/dlq-replay.md)).
- More topics to operate, monitor, and size. `scripts/topics.txt` lists them (3 partitions each; DLQ retention 14 days, the others 7 days) and the `topics` stack (custom resource) creates them on AWS.
- Retry count and total window are fixed by the code: two hops, about 11 minutes in total. Changing them is a code change.
