# Runbook: DLQ replay

Status: outline. `TODO (M2)`: finalize once replay-cli is implemented.

## When to use

- `integration.dlq` has records that failed for a reason that is now fixed (downstream outage over, bad config corrected, consumer bug patched).
- Do not replay records whose cause is still present. They will return to the DLQ.

## Before replaying

1. Group DLQ records by `error-class` and `error-message`. Confirm one root cause.
2. Check `failed-at` and `source-topic` to find the time window and the topic.
3. Confirm the fix is deployed.
4. Run with `--dry-run` and check the count and sample.

## Steps

`TODO (M2)`: confirm flag names and defaults. Planned interface:

```bash
replay-cli --dry-run --source-topic <dlq-filter> --rate <msgs-per-sec> [filters]
replay-cli --source-topic <dlq-filter> --rate <msgs-per-sec> [filters]
```

- Filters: `error-class`, `failed-at` range, `source-topic`. `TODO (M2)`: list all supported filters.
- Rate limit: keep replay well below downstream capacity.
- Each republished record gets a `replayed-at` header.

## Verify

- Watch consumer lag on the canonical topics and the delivery-service logs.
- Confirm the downstream service received the events (WireMock locally).
- Check that records did not return to `integration.dlq`. If they did, stop and investigate.

## Stop conditions

- Downstream returns sustained `5xx` or `429`: stop, let the retry path handle it, and resume later.
- Replayed records fail with a new error class: stop and treat as a new incident.

## Notes

- Replay can duplicate side effects if the downstream does not honor `Idempotency-Key`. Use the event id from the original record.
- Replay does not restore original ordering. See [architecture ordering](../architecture.md#ordering-per-key-and-partitions).
