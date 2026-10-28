# ADR 0001: Kafka Streams instead of Lambda for stream processing

- Status: Accepted (final state, M5)
- Date: M1

## Context

The platform must normalize two streams (raw change events and CDC `orders` rows) into canonical topics, keep per-key order, and route failures to a DLQ. Processing needs state-free transforms today, but later milestones may add joins or aggregations.

Lambda is used for ingress because ingress is request-driven: an HTTP call arrives, we validate it, and we return after a broker ack. Stream processing is different: it is a continuous consumer of Kafka partitions with offsets, rebalances, and optional state stores.

## Decision

- `ingress-lambda` runs on AWS Lambda behind API Gateway (`infra/cloudformation/ingress.yaml`).
- `stream-processor` is a Kafka Streams application (plain Java 21, no Spring). On AWS it runs as an ECS Fargate service (`infra/cloudformation/processing.yaml`, 0.5 vCPU / 1 GB, desired count 1). Locally it is the `stream-processor` container in `docker-compose.yml`.
- Kafka Streams is used with `processing.guarantee=exactly_once_v2`. One application holds both topologies (A: account, B: order) and writes failures to `integration.dlq` from inside the topology, so the DLQ write is part of the same transaction as the normal output.
- `delivery-service` is also a long-running consumer (plain Java 21, manual commits) and runs next to it on Fargate.

Status of the AWS part: the Fargate services are defined and lint-clean, but not deployed yet (the first AWS run stopped at MSK, see [cost.md](../cost.md)). The processor itself is tested locally and in the Testcontainers end-to-end tests.

## Consequences

Positive:
- Exactly-once processing within Kafka comes from the library and needs no custom offset bookkeeping.
- Per-partition ordering and offset management are handled by the framework.
- The same code and the same jar run locally and on ECS.

Negative:
- Kafka Streams needs a long-running process, which adds a fixed cost even when traffic is low (Fargate, about 0.04 USD/h per task; see [cost.md](../cost.md)).
- Lambda with a Kafka event source mapping would have been cheaper at low volume, but it gives weaker control over ordering and has no built-in exactly-once read-process-write. We accept the cost for this design.
- Operators must understand rebalances and state-store recovery. See [knowledge transfer](../knowledge-transfer.md).
- The exactly-once guarantee covers Kafka only. It does not cover the `DlqDeserializationExceptionHandler` (own non-transactional producer) or the HTTP delivery step, which is at-least-once and relies on the `Idempotency-Key`. See [architecture](../architecture.md).
- Consumers of the output topics must use `isolation.level=read_committed`.
