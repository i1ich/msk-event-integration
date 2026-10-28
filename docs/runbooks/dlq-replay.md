# Runbook: DLQ replay

Tool: [replay-cli](../../replay-cli/README.md). Build once: `mvn -q -B -pl replay-cli -am package -DskipTests` (produces `replay-cli/target/replay-cli.jar`).

## When to use

- `integration.dlq` has records that failed for a reason that is now fixed (downstream outage over, bad config corrected, consumer bug patched).
- Do not replay records whose cause is still present. They will return to the DLQ.

## 1. Inspect the DLQ

Local stack (headers show why and where each record failed):

```bash
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic integration.dlq --from-beginning --property print.headers=true --timeout-ms 10000
```

Look at the headers: `error-class`, `error-message`, `source-topic`, `source-partition`, `source-offset`, `failed-at`, `attempts`, plus the original `event-id`.

Quicker overview without a console consumer (works on AWS too, see "AWS variant"; set `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` locally): `java -jar replay-cli/target/replay-cli.jar --dry-run` prints one line per record (partition, offset, source-topic, error-class, event-id, key) and a summary. Group the output by error class.

For the local `docker compose` stack the Kafka UI also works: `docker compose --profile ui up -d kafka-ui`, then http://localhost:8090 (topic `integration.dlq`).

## 2. Find the root cause

1. Group the records by `error-class` and `error-message`. Confirm there is one root cause for what you want to replay.
2. Note the time window (`failed-at`) and the `source-topic`.
3. Look at the logs of the service that wrote them (`stream-processor` or `delivery-service`) around `failed-at`.

## 3. Fix

Deploy the fix (or wait until the downstream is healthy again). Confirm it works with a fresh event before touching the DLQ.

## 4. Dry run (always)

Nothing is published; it prints partition, offset, source-topic, error-class, event-id and key of each match.

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
java -jar replay-cli/target/replay-cli.jar --dry-run \
  --source-topic integration.account.v1 --error-class UnmappedEntityException \
  --from 2026-01-31T10:00:00Z --to 2026-01-31T12:00:00Z
```

Check the `matched` count and a few samples. Prefer narrow filters (topic + error class + time window).
Records without a `source-topic` header are skipped and counted; handle them by hand.

## 5. Replay with a rate limit

Start small, then the rest:

```bash
# first batch
java -jar replay-cli/target/replay-cli.jar --source-topic integration.account.v1 \
  --error-class UnmappedEntityException --from 2026-01-31T10:00:00Z --to 2026-01-31T12:00:00Z \
  --rate 5 --max 20

# if it looks good: the whole window (the first 20 will be sent a second time, see Notes; narrow --from instead)
java -jar replay-cli/target/replay-cli.jar --source-topic integration.account.v1 \
  --error-class UnmappedEntityException --from 2026-01-31T10:00:00Z --to 2026-01-31T12:00:00Z \
  --rate 10
```

Keep `--rate` well below downstream capacity.
Exit code 0 and `failed=0` in the summary mean every record was acknowledged by the broker.

## 6. Verify

- Watch consumer lag on the canonical topics and the delivery-service logs.
- Confirm the downstream service received the events (WireMock locally).
- Check that records did not return to `integration.dlq` (re-run the console consumer from step 1 and compare). If they did, stop and investigate.

## 7. Record what you replayed

The DLQ itself is not modified (retention 14 days), so nothing marks a record as replayed. Write down in the incident ticket:
the exact command, time, the summary line, and the `--from`/`--to` window. Replayed records carry a `replayed-at` header on the target topic.

## Stop conditions

- Downstream returns sustained `5xx` or `429`: stop (Ctrl+C), let the retry path handle it, and resume later with `--from` set after the last replayed `failed-at`.
- Replayed records fail with a new error class: stop and treat as a new incident.

## Notes

- Replays can create duplicates. Consumers are idempotent by `event-id` / `Idempotency-Key`, so duplicates are harmless as long as the downstream honors the key.
- Replaying twice with the same filters sends the records twice, because the DLQ is not changed.
- Replay only after the root cause is fixed; always `--dry-run` first; prefer narrow filters.
- Replay does not restore original ordering. See [architecture ordering](../architecture.md#ordering-per-key-and-partitions).
- The tool never commits offsets and uses no consumer group, so it does not affect any running consumer.

## AWS variant

The procedure is the same; only the connection differs.

- `replay-cli` reads and writes record bytes without decoding them, so it needs no schema registry settings, on Confluent or Glue alike.
- Set `KAFKA_SECURITY=iam` and `KAFKA_BOOTSTRAP_SERVERS` to the MSK IAM bootstrap string (`BootstrapBrokerStringSaslIam` from `aws kafka get-bootstrap-brokers --cluster-arn <arn>`). The caller needs AWS credentials whose IAM policy allows reading `integration.dlq` and writing the target topics (the stack roles do not include an operator role; add one before using it), and a network path into the VPC (the brokers are private).
- `--bootstrap-servers <host:port,...>` can be used instead of the environment variable.
- Console consumers cannot decode Glue-encoded Avro, but the DLQ headers are plain strings, so `--dry-run` shows everything needed to decide.
- Verify with the CloudWatch dashboard (DLQ `BytesInPerSec`, consumer lag) and the `delivery-service` log group.

Not yet exercised on AWS (deploy pending Paid plan).
