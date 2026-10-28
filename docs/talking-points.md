# Talking points

What this project does for each Kafka and AWS topic that tends to come up in a technical discussion, and why, with the code as evidence. Each point carries a status tag:

- **[local]**: implemented and tested on the local stack (unit, topology and Testcontainers end-to-end tests).
- **[AWS design only]**: templates and scripts exist and are `cfn-lint` clean, but nothing has run on AWS. The first deployment stopped at the MSK stack because the account is on the Free plan, which does not include MSK ([cost.md](cost.md#session-log)). Pending: AWS run (needs Paid plan).

Background: [architecture](architecture.md), [ADRs](adr/).

## 1. Partitions and ordering per key [local]

- Every topic has 3 partitions ([`scripts/topics.txt`](../scripts/topics.txt)). Kafka orders only within a partition, so ordering is a key design decision, not a setting.
- The raw topic is keyed by the first Salesforce record id ([`EventIngestService`](../ingress-lambda/src/main/java/io/github/i1ich/mskei/ingress/EventIngestService.java)); CDC is keyed by the `orders` primary key; the canonical topics are keyed by account id and order id ([`ProcessorTopology`](../stream-processor/src/main/java/io/github/i1ich/mskei/processor/ProcessorTopology.java)). One entity therefore stays on one partition from source to the delivery service.
- A Salesforce event that batches several record ids is split into one `AccountEvent` per id ([`AccountMapper`](../stream-processor/src/main/java/io/github/i1ich/mskei/processor/AccountMapper.java)); those events go to the partitions of their own keys. Ordering between different ids is not promised.
- `CdcOrdersIT` asserts that insert, update and delete of one order land on the same partition in commit order ([`CdcOrdersIT`](../integration-tests/src/test/java/io/github/i1ich/mskei/it/CdcOrdersIT.java)).
- Retries deliberately break per-key order (see point 5); the downstream has to tolerate that.
- Production, differently: choose the partition count from the target throughput and consumer parallelism (3 is a demo size), and carry a per-entity version so the downstream can drop stale events instead of trusting arrival order.

## 2. Consumer groups and rebalancing [local]

- Three groups: `stream-processor` (Kafka Streams application id), `delivery-service` (canonical topics) and `delivery-service-retry` (retry topics). Splitting the retry topics into their own group lets the retry consumer pause partitions without stalling the main flow ([`DeliveryServiceApp`](../delivery-service/src/main/java/io/github/i1ich/mskei/delivery/DeliveryServiceApp.java)).
- The retry consumer pauses a partition while the head record is not due, but keeps calling `poll()`, so it stays in the group and `max.poll.interval.ms` does not expire during a 10-minute wait. Paused state is dropped in `onPartitionsRevoked`; the new owner re-reads the head record and pauses again ([`ConsumerLoop`](../delivery-service/src/main/java/io/github/i1ich/mskei/delivery/ConsumerLoop.java)).
- `max.poll.records=50` bounds a batch so a slow downstream (5 s timeout per call) does not push a poll cycle past the poll interval. The README of delivery-service lists the brownout case where it still can.
- Parallelism is capped by partitions: 3 per topic, one task each in Kafka Streams. More instances than partitions sit idle.
- In Kafka Streams a rebalance aborts the in-flight transaction and the new owner redoes it ([`StreamProcessorApp`](../stream-processor/src/main/java/io/github/i1ich/mskei/processor/StreamProcessorApp.java)). A dead stream thread is replaced (`REPLACE_THREAD`) rather than killing the app.
- Production, differently: tune assignment (static membership or cooperative rebalancing) and alert on rebalance rate; this project uses client defaults.

## 3. Offsets and at-least-once versus exactly-once [local]

- The delivery service commits offsets manually and only after the outcome is durable: delivered, or its retry/DLQ write acknowledged ([`ConsumerLoop.pollOnce`](../delivery-service/src/main/java/io/github/i1ich/mskei/delivery/ConsumerLoop.java)). A crash before the commit means the batch is processed again: at-least-once.
- Kafka Streams runs with `exactly_once_v2`: consume, map, produce and DLQ writes are one transaction with the input offsets ([`StreamProcessorApp`](../stream-processor/src/main/java/io/github/i1ich/mskei/processor/StreamProcessorApp.java)). The DLQ is written from inside the topology on purpose ([`SafeMapProcessor`](../stream-processor/src/main/java/io/github/i1ich/mskei/processor/SafeMapProcessor.java)), so a bad record is committed and dead-lettered atomically.
- What exactly-once does not cover: the HTTP call to the downstream, and the separate producer in [`DlqDeserializationExceptionHandler`](../stream-processor/src/main/java/io/github/i1ich/mskei/processor/DlqDeserializationExceptionHandler.java) (a DLQ duplicate is possible after a crash).
- End to end this is effectively-once only through idempotency: the `eventId` is sent as `Idempotency-Key` ([`HttpDownstreamSender`](../delivery-service/src/main/java/io/github/i1ich/mskei/delivery/HttpDownstreamSender.java)). The CDC `eventId` is `erp.public.orders-<orderId>-<lsn>-<op>` from the WAL position, stable across reprocessing and DLQ replay ([`OrderMapper`](../stream-processor/src/main/java/io/github/i1ich/mskei/processor/OrderMapper.java)).
- Consumers of transactional output must say so: delivery-service and replay-cli use `isolation.level=read_committed`, otherwise records of an aborted stream-processor transaction would be delivered or replayed ([`DeliveryServiceApp`](../delivery-service/src/main/java/io/github/i1ich/mskei/delivery/DeliveryServiceApp.java)). This was a real gap found in review and fixed.

## 4. Idempotent producer [local]

- Ingress, delivery-service, replay-cli and the DLQ handler use `enable.idempotence=true` with `acks=all` ([`ProducerFactory`](../ingress-lambda/src/main/java/io/github/i1ich/mskei/ingress/ProducerFactory.java), [`DeliveryServiceApp`](../delivery-service/src/main/java/io/github/i1ich/mskei/delivery/DeliveryServiceApp.java), [`ReplayCli`](../replay-cli/src/main/java/io/github/i1ich/mskei/replay/ReplayCli.java)). It removes duplicates caused by the producer's own retries, within one producer session.
- It does not remove client-level duplicates: a client that retries a `POST /events` after a lost 202 produces two records. Ingress uses `ChangeEventHeader.eventId` as `event-id` when present and a random UUID otherwise, so deduplication downstream works only for clients that send a stable id. State this limit when asked.
- Ingress answers 202 only after the broker ack (10 s bound) and 503 otherwise, so a 202 means the record is stored. Timeouts are set below API Gateway's 29 s limit.
- On the 2-broker AWS cluster `min.insync.replicas=1`, so `acks=all` can be satisfied by one replica ([ADR 0006](adr/0006-msk-provisioned-in-sa-east-1.md)). Production: 3 brokers, RF 3, min ISR 2. [AWS design only]

## 5. Retry topics versus blocking retries [local]

- Options and the decision are in [ADR 0003](adr/0003-retry-topics.md). Blocking retries (sleep in the consumer, or re-consume the same topic) hold a partition and every key on it; retry topics move the failed record aside and let the main flow continue.
- Ladder: attempt 1 on the canonical topic, then `integration.retry.1m`, then `integration.retry.10m`, then `integration.dlq`; 429, 5xx and IO errors are retryable, other 4xx are permanent and skip the ladder ([`Classifier`](../delivery-service/src/main/java/io/github/i1ich/mskei/delivery/Classifier.java), [`RetryRouter`](../delivery-service/src/main/java/io/github/i1ich/mskei/delivery/RetryRouter.java)).
- The delay is a `not-before` header, not a broker feature. Within one retry topic every record has the same delay, so the head of a partition is always the next one due; blocking on it is correct ([`NotBefore`](../delivery-service/src/main/java/io/github/i1ich/mskei/delivery/NotBefore.java)).
- Retry state lives in Kafka and survives crashes. The cost is lost ordering across the main and retry topics, fixed delays, and more topics to operate.
- `DeliveryRetryIT` proves 503 then 200 passes through `retry.1m` with the same `Idempotency-Key`, and a 400 goes straight to the DLQ ([`DeliveryRetryIT`](../integration-tests/src/test/java/io/github/i1ich/mskei/it/DeliveryRetryIT.java)).
- Production, differently: exponential backoff with jitter, a circuit breaker so an outage does not push every event into the DLQ after about 11 minutes, and a per-downstream rate limit.

## 6. DLQ design and replay safety [local]

- A DLQ record keeps the **original key and value bytes** and the original headers, and adds the error headers; built in one place, [`DeadLetters`](../common/src/main/java/io/github/i1ich/mskei/common/DeadLetters.java), with the message truncated to 1000 characters and only the latest failure's headers kept. Keeping raw bytes is why both topologies read raw bytes: a record that cannot be parsed is still replayable.
- `source-topic` is the topic to republish to (the canonical topic for delivery failures, even after retries), so replay needs no knowledge of the retry ladder. `attempts` has two meanings (failed so far on retry records, failed attempt number on DLQ records); see the [headers contract](architecture.md#headers-contract).
- Replay ([`DlqReplayer`](../replay-cli/src/main/java/io/github/i1ich/mskei/replay/DlqReplayer.java)): no consumer group and no commits, a snapshot of the end offsets so a record that fails again is not replayed in a loop, filters, a rate limit without catch-up bursts, `--dry-run`, an idempotent producer, and `event-id` preserved. Tested by `ReplayIT`.
- Honest limits: the DLQ is not modified by replay (a second run replays again); a replayed CDC record gets a new offset and therefore a new `eventId`; replay appends at the end of the partition.
- Production, differently: track replay status per record (a replay ledger or a compacted status topic), and keep a replay audit trail.

## 7. Debezium on Kafka Connect, replication slots and WAL growth [local]

- The connector `orders-source` uses `pgoutput`, a pre-created publication, and a fixed slot name `dbz_orders_slot` so a restart resumes from the same position ([`orders-source.local.json`](../infra/connect/orders-source.local.json), [`01-schema.sql`](../infra/postgres/init/01-schema.sql)). `REPLICA IDENTITY FULL` makes delete events carry the full old row, which `OrderMapper` uses.
- A slot retains WAL until the connector confirms it. A stopped connector therefore grows database storage; this is the main operational risk of CDC, with its own runbook ([connector restart and slot lag](runbooks/connector-restart-and-slot-lag.md)). No alarm covers it yet.
- Tombstones are enabled for log compaction; the mapper skips them because the delete event already carries the data.
- `CdcOrdersIT` runs the real Debezium connector against PostgreSQL in Testcontainers. On AWS the same connector runs on MSK Connect with a JSON key converter and the Glue value converter; the config differs from local by design ([infra/connect/README.md](../infra/connect/README.md)). [AWS design only]
- Production, differently: monitor slot lag and `pg_replication_slots` retention with alarms, set a WAL size limit on the slot, use the MSK config provider for the database password (the CloudFormation dynamic reference makes it visible to anyone who can describe the connector), and rehearse the re-snapshot after a lost slot.

## 8. Schema evolution and compatibility modes [local, Glue path AWS design only]

- Avro with `BACKWARD` compatibility in both registries: consumers on the new schema read old data, so consumers are upgraded before producers. `AccountEvent` has v1 and v2 (adds optional `industry`); [`SchemaEvolutionTest`](../common/src/test/java/io/github/i1ich/mskei/common/SchemaEvolutionTest.java) shows old and new readers, and that a required field without a default is rejected.
- `SchemaRegistryCompatibilityIT` shows the registry rejecting an incompatible change with HTTP 409; `scripts/check-schema-compat.sh` runs the same check in CI ([`schemas/test-fixtures`](../schemas/test-fixtures/AccountEvent.incompatible.avsc)).
- Services run with auto-registration off, as in production. That exposed a real problem: generated classes carry `avro.java.string`, so their schema text does not equal the `.avsc`. Confluent is handled by `use.latest.version=true`; Glue has no such option, so [`GeneratedSchemas`](../common/src/main/java/io/github/i1ich/mskei/common/GeneratedSchemas.java) prints the generated schema text and [`register-schemas-glue.sh`](../scripts/register-schemas-glue.sh) registers it as the newest version ([`SchemaRegistryConfig`](../common/src/main/java/io/github/i1ich/mskei/common/SchemaRegistryConfig.java)). The Glue half is [AWS design only].
- Confluent and Glue wire formats are incompatible, so the registry is a per-deployment choice ([ADR 0002](adr/0002-glue-vs-confluent-sr.md)). Glue was chosen on AWS for IAM-only access and no self-operated service.
- Production, differently: run the compatibility check against the real Glue registry in CI (today it only runs against the local one), and consider `FULL_TRANSITIVE` for topics with long retention.

## 9. MSK IAM auth, Serverless versus provisioned [AWS design only]

- IAM authentication everywhere on AWS (`KAFKA_SECURITY=iam`): no passwords or certificates to rotate, access scoped per topic, consumer group and transactional id ([ADR 0004](adr/0004-iam-auth.md), [`KafkaClientConfig`](../common/src/main/java/io/github/i1ich/mskei/common/KafkaClientConfig.java), and the least-privilege table in [CONTRACT.md](../infra/cloudformation/CONTRACT.md#iam-for-kafka-least-privilege)). The client config is unit-tested; a real MSK IAM handshake has not been exercised.
- Serverless was not an option: MSK Serverless is not offered in sa-east-1 (checked against AWS docs on 2026-10-27, [ADR 0006](adr/0006-msk-provisioned-in-sa-east-1.md)). The template keeps `ClusterType` as a parameter and `deploy.sh` refuses serverless where unsupported.
- Provisioned trade-offs: fixed hourly cost whether or not there is traffic (about 0.55 USD per hour for the whole stack, [cost.md](cost.md)), we own sizing and upgrades, small instance limits. In return it works in the target region.
- A 2-broker cluster needs custom configuration: the default transaction state log replication factor of 3 cannot be met, and `exactly_once_v2` needs that log ([`msk.yaml`](../infra/cloudformation/msk.yaml)). Demo trade-off: `min.insync.replicas=1`. Production: 3 brokers, RF 3, min ISR 2.
- Authorization errors on IAM look like connection errors; check the policy first.

## 10. Lambda in a VPC talking to MSK [AWS design only]

- MSK has no public endpoint here, so the ingress Lambda runs in the private subnets, with a security group allowed on port 9098 ([`ingress.yaml`](../infra/cloudformation/ingress.yaml), [`network.yaml`](../infra/cloudformation/network.yaml)). API Gateway stays outside the VPC and invokes the function through the Lambda service.
- Connection reuse: the Kafka producer and the HMAC secret are built once per container in a static holder ([`IngressHandler`](../ingress-lambda/src/main/java/io/github/i1ich/mskei/ingress/IngressHandler.java), [`IngressConfig`](../ingress-lambda/src/main/java/io/github/i1ich/mskei/ingress/IngressConfig.java)), never per request. Producer timeouts are bounded below the Lambda timeout (15 s) and API Gateway's 29 s.
- Cold start is the cost: JVM start, producer creation, SASL/IAM handshake. SnapStart is a template parameter, default off. No cold-start numbers exist yet; they are to be measured during the first AWS run.
- A VPC function needs a route for any other AWS call (Secrets Manager, the CloudFormation response URL for custom resources); the project uses one NAT gateway instead of interface endpoints ([ADR 0005](adr/0005-nat-vs-vpc-endpoints.md)), cheaper for a short-lived demo but a single-AZ dependency.
- The same code runs locally as [`LocalServer`](../ingress-lambda/src/main/java/io/github/i1ich/mskei/ingress/local/LocalServer.java) around the same `EventIngestService`, which is tested without Lambda or HTTP types. [local]
- Production, differently: interface VPC endpoints instead of NAT, provisioned concurrency or SnapStart after measuring, and a connection health check strategy for long-idle containers.

## 11. CloudFormation stack design and teardown [AWS design only]

- Eleven stacks plus account-level budget and optional OIDC, each with one purpose and exports named `<env>-msk-ei-<Name>`; deploy order follows dependencies (network, MSK, topics, registry, database, images, connector, observability, ingress, processing). One contract file is the source of truth ([CONTRACT.md](../infra/cloudformation/CONTRACT.md)).
- Custom resources cover what CloudFormation cannot: Kafka topics and the database schema, via Lambdas in [`infra-bootstrap`](../infra-bootstrap/README.md). The handler always answers CloudFormation, even on failure or near timeout, because a missing response hangs a stack for an hour ([`CustomResource`](../infra-bootstrap/src/main/java/io/github/i1ich/mskei/bootstrap/CustomResource.java)). Unit-tested; never run in a stack.
- Everything must be deletable: no `Retain`, explicit log groups, ECR `EmptyOnDelete`, RDS without final snapshot, tags `Project` and `Env` on everything. Teardown runs in reverse order, empties the artifacts bucket, deletes Glue schemas before their stack, then `check-leftovers.sh` looks for NAT gateways, Elastic IPs, RDS, MSK and the rest ([`teardown.sh`](../scripts/teardown.sh), [`check-leftovers.sh`](../scripts/check-leftovers.sh), [runbook](runbooks/teardown.md)).
- Cost guardrails come first: budgets (monthly and daily) are a precondition for `deploy.sh`, and the script stops when the account is on the Free plan, a check added after the first run showed that MSK is not available there. That first run created only `artifacts` and `network`, was torn down within minutes and cost about 0.02 USD ([cost.md](cost.md#session-log)). The experience is itself a talking point: fail before billable resources exist.
- Known rough edges: the OIDC deploy role is broad; the VPC stack can take 20 to 40 minutes to delete because Lambda network interfaces are released late; the template lint is offline only (`cfn-lint`), no stack has gone through a full create-and-delete cycle yet.
- Production, differently: per-account separation of environments, scoped deploy roles with permission boundaries, change sets reviewed before apply, drift detection, and `Retain` for stateful resources.
