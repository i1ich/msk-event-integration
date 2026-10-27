# ADR 0001: Kafka Streams instead of Lambda for stream processing

- Status: Accepted
- Date: M1

## Context

The platform must normalize two streams (raw change events and CDC `orders` rows) into canonical topics, keep per-key order, and route failures to a DLQ. Processing needs state-free transforms today, but later milestones may add joins or aggregations.

Lambda is used for ingress because ingress is request-driven: an HTTP call arrives, we validate it, and we return after a broker ack. Stream processing is different: it is a continuous consumer of Kafka partitions with offsets, rebalances, and optional state stores.

## Decision

- `ingress-lambda` runs on AWS Lambda behind API Gateway.
- `stream-processor` runs as a Kafka Streams application on ECS Fargate (AWS, M4). Locally it runs as a plain JVM process.
- Kafka Streams is used with `processing.guarantee=exactly_once_v2`.

## Consequences

Positive:
- Exactly-once processing within Kafka comes from the library and needs no custom offset bookkeeping.
- Per-partition ordering and offset management are handled by the framework.
- The same code runs locally and on ECS.

Negative:
- Kafka Streams needs a long-running process, which adds a fixed cost (ECS tasks) even when traffic is low. This is recorded in [cost.md](../cost.md) (M4).
- Lambda with a Kafka event source mapping would have been cheaper at low volume, but it gives weaker control over ordering and has no built-in exactly-once read-process-write. We accept the cost for this design.
- Operators must understand rebalances and state-store recovery. See [knowledge transfer](../knowledge-transfer.md).
- The exactly-once guarantee covers Kafka only, not the HTTP delivery step. See [architecture](../architecture.md#delivery-semantics).
