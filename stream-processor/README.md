# stream-processor

Kafka Streams app (Java 21, no Spring). One application, two topologies, one dead-letter topic.

| Topology | Input | Output | Mapping |
|---|---|---|---|
| A | `sf.change-events.raw` (String key, raw JSON bytes) | `integration.account.v1` (`AccountEvent`, key = account id) | Salesforce Change Event JSON; only entity `Account` |
| B | `erp.public.orders` (raw Avro bytes) | `integration.order.v1` (`OrderEvent`, key = order id) | Debezium envelope (`op` c/u/r/d) |
| both | any failed record | `integration.dlq` | original key/value bytes + original and error headers |

## How it works

1. Both sources are read as **raw bytes**. That way a record we cannot parse is still available, untouched, for the DLQ.
2. `SafeMapProcessor` (Processor API) calls a mapper (`AccountMapper` / `OrderMapper`) inside a try/catch and forwards a
   `Result`: either the mapped event or a ready-made dead-letter record (`DeadLetters.record(...)` from `common`).
3. `split()` sends failed results to `integration.dlq` and good results to the output topic (Avro, `SpecificAvroSerde`).
   The DLQ is written from inside the topology, so it is part of the same transaction as the normal output.
4. Output headers are the input headers (`event-id`, ...). DLQ headers are the input headers plus
   `error-class`, `error-message`, `source-topic/partition/offset`, `failed-at`, `attempts=1`.

Mapping details worth knowing:

- **Account**: `eventId` = header `event-id` (fallback `<topic>-<partition>-<offset>`). A Salesforce event with several
  `recordIds` produces one `AccountEvent` per id; the `eventId` then gets a `-<index>` suffix so ids stay unique.
- **Order**: `eventId` = `erp.public.orders-<orderId>-<lsn>-<op>`, built from the change's position in the PostgreSQL
  WAL (`source.lsn`). It is the same however often the change is re-read, including a DLQ replay (which republishes at
  a new offset), so the downstream Idempotency-Key catches the duplicate. Without an LSN it falls back to
  `erp.public.orders-<partition>-<offset>` (stable for reprocessing only). `amount` is a string (`decimal.handling.mode=string`); `updated_at` is an ISO-8601
  string (also accepted: epoch millis). For deletes the fields come from `before`.
- **Tombstones** (null value after a delete) are skipped on purpose: the DELETE event already carries the information;
  tombstones only exist for log compaction.

## What can go wrong

- **Poison pills** (bad JSON, unknown entity/op, garbage bytes): go to the DLQ, the app keeps running. Look at the
  `error-class` / `error-message` headers; replay with `replay-cli` once fixed.
- **Schema mismatch** (e.g. Debezium envelope without `op`): shows up as a DLQ record with an Avro error. If *output*
  registration is denied (`SCHEMA_AUTO_REGISTER=false` and schema not registered), the stream thread fails and is
  replaced, then fails again: that is an ops problem, register the schemas.
- **Rebalances**: with `exactly_once_v2` an in-flight transaction is aborted and redone by the new owner. No duplicates
  for `read_committed` consumers, only a short pause.
- **Exactly-once scope**: covers consume, process, produce and the DLQ writes done by the topology (one atomic
  transaction). It does NOT cover the `DlqDeserializationExceptionHandler` (own non-transactional producer; a DLQ
  duplicate is possible after a crash) nor external side effects such as the HTTP calls in `delivery-service`, which
  are at-least-once and rely on the `eventId` Idempotency-Key.
- **Ordering**: guaranteed per key only (account id / order id keep one partition). Never across keys. Fan-out of a
  multi-id Salesforce event goes to different partitions.
- **Unexpected exceptions** in a stream thread: the thread is replaced (`REPLACE_THREAD`); the record is retried. Mapping
  errors are caught earlier and never get here.
- Consumers of the output topics must use `isolation.level=read_committed`.

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | required | e.g. `kafka:9092` |
| `KAFKA_SECURITY` | `plaintext` | `plaintext` or `iam` (Amazon MSK) |
| `SCHEMA_REGISTRY_TYPE` | `confluent` | `confluent` or `glue` (AWS Glue Schema Registry). Chosen per deployment: the wire formats are incompatible, so a topic written with one registry cannot be read with the other |
| `SCHEMA_REGISTRY_URL` | required for `confluent` | Confluent Schema Registry |
| `GLUE_REGISTRY_NAME` | required for `glue` | Glue registry; schema name = topic name (default naming strategy), compatibility `BACKWARD`, format `AVRO` |
| `AWS_REGION` | required for `glue` | region of the Glue registry; credentials from the default AWS provider chain (task role) |
| `SCHEMA_AUTO_REGISTER` | `true` | set `false` where CI registers schemas |
| `STREAMS_APPLICATION_ID` | `stream-processor` | consumer group / internal topic prefix |
| `STREAMS_REPLICATION_FACTOR` | unset (Kafka Streams default `-1`: the broker default) | AWS sets `-1` explicitly; the 2-broker MSK configuration uses `default.replication.factor=2` |

## Build and run

```
mvn -B -pl stream-processor -am verify      # tests + target/stream-processor.jar
docker build -f stream-processor/Dockerfile -t stream-processor .   # build context = repo root
```

Tests use `TopologyTestDriver` with an in-memory Schema Registry (`mock://stream-processor-test`); no Kafka needed.
