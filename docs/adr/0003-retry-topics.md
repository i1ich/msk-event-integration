# ADR 0003: Retry topics instead of in-process or single-topic retry

- Status: Accepted
- Date: M1

## Context

`delivery-service` calls a downstream HTTP service. Some failures are transient (`5xx`, `429`, timeouts) and some are permanent (`4xx` other than `429`). Transient failures need a delay before the next attempt. Options considered:

1. Retry in process with sleeps: blocks the partition, delays all keys on it, and loses the retry state on a crash.
2. Retry on the same topic: the record is re-consumed after a fixed backoff, but it blocks the partition during that time.
3. Retry topics with fixed delays: each hop is a separate topic. A consumer reads a hop only after its delay has elapsed, and forwards the record to the next hop or the DLQ on failure.

## Decision

Use retry topics: `integration.retry.1m`, then `integration.retry.10m`, then `integration.dlq`. Permanent failures go directly to `integration.dlq`. Retries use manual commits, so an event is committed only after its outcome (success, retry hop, or DLQ) is durably written.

## Consequences

Positive:
- A failing downstream does not block the main partition for long; the main flow keeps moving.
- Retry state is in Kafka, so a crash does not lose it.
- Terminal failures are explicit in `integration.dlq` with headers that support replay.

Negative:
- Ordering per key is not preserved across retries. A retried event can arrive after newer events for the same key. Downstream must handle out-of-order arrival.
- Delays are approximate: a hop delivers after at least its nominal delay, and time can be longer under consumer lag.
- More topics to operate, monitor, and size. The topics stack (`infra/topics`) creates them.
- Retry count and total retry window are fixed by topology: two hops, about 11 minutes in total. Changing them is a code and infra change.
