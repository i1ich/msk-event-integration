# replay-cli

Operator tool that republishes records from `integration.dlq` back to the topic they failed on, at a limited rate.
Runbook: [docs/runbooks/dlq-replay.md](../docs/runbooks/dlq-replay.md).

## Build and run

```bash
mvn -q -B -pl replay-cli -am package          # -> replay-cli/target/replay-cli.jar
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 java -jar replay-cli/target/replay-cli.jar --dry-run
```

Connection comes from env (`KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SECURITY=plaintext|iam`, same as the services).
`--bootstrap-servers` overrides the first one.

## Options

| Option | Meaning |
|---|---|
| `--source-topic <t>` | Only records whose `source-topic` header equals `t` (exact). |
| `--error-class <c>` | Only records whose `error-class` equals `c`, full name or simple name (`UnmappedEntityException`). |
| `--from <instant>` | `failed-at` >= instant (ISO-8601, e.g. `2026-01-31T10:00:00Z`). |
| `--to <instant>` | `failed-at` < instant (exclusive). |
| `--rate <n>` | Messages per second, must be > 0. Default 10. |
| `--max <n>` | Stop after `n` matching records. |
| `--dry-run` | Print what would be replayed (partition, offset, source-topic, error-class, event-id, key); publish nothing. |
| `--bootstrap-servers <h:p>` | Overrides `KAFKA_BOOTSTRAP_SERVERS`. |

Exit codes: `0` ok, `1` runtime error or at least one failed send, `2` invalid arguments.
The last line is a summary: `scanned`, `matched`, `replayed`, `skipped(no source-topic)`, `failed`.

## How it works

1. Reads the whole DLQ with `assign()` on all partitions and `seekToBeginning`. There is **no consumer group and no commit**:
   replay is a one-off operator action and must not create or move the offsets of any group.
2. Captures the end offsets at start and stops when every partition reaches them. Records that arrive in the DLQ
   meanwhile are not read, so a record that fails again is not replayed in a loop.
3. Each record is checked against the filters. Records that pass the filters but have no `source-topic` header are
   skipped and counted.
4. A matching record is produced to its `source-topic` with the same key and value bytes. Headers are copied except
   `error-class`, `error-message`, `source-topic`, `source-partition`, `source-offset`, `failed-at`, `attempts`,
   `not-before`; `replayed-at` (ISO-8601) is added. `event-id` stays, so consumers can deduplicate.
5. `RateLimiter` waits `1s / rate` between sends (no catch-up bursts).
6. The producer is idempotent with `acks=all`. `flush()` waits for all acks before the summary is printed.

## What can go wrong

- **Duplicates.** The DLQ is not modified (retention 14 days). Running the same replay twice sends the records twice.
  Consumers are idempotent by `event-id` / `Idempotency-Key`, but record what you replayed anyway.
- **Root cause not fixed.** Records fail again and return to the DLQ (a new entry with a fresh `failed-at`).
- **Ordering.** Replayed records are appended at the end of the partition; the original order per key is not restored.
- **Partial failure.** If sends fail (broker down), `failed` is non-zero and the exit code is 1. Already sent records
  are not rolled back; re-run with narrower filters (`--from`) instead of repeating the whole run.
- **Records without `source-topic`.** Skipped; look at them by hand.
- **Records without a parsable `failed-at`** never match when `--from`/`--to` is used.
- **Wrong `--rate`.** Keep it well below what the downstream handles; start low (e.g. 5) and use `--max` for a first batch.

## Examples

```bash
# 1. What is in there? Nothing is published.
java -jar replay-cli/target/replay-cli.jar --dry-run

# 2. Only one failure type on one topic, in a time window
java -jar replay-cli/target/replay-cli.jar --dry-run \
  --source-topic integration.account.v1 --error-class UnmappedEntityException \
  --from 2026-01-31T10:00:00Z --to 2026-01-31T12:00:00Z

# 3. First 20 records at 5 msg/s
java -jar replay-cli/target/replay-cli.jar --source-topic integration.account.v1 --rate 5 --max 20
```
