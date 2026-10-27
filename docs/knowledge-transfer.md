# Knowledge transfer

Audience: an operations team taking over this platform. Outline only; M5 fills in the details.

## 1. What runs where

| Part | Runs on (AWS) | Runs on (local) | Owner doc |
|---|---|---|---|
| Ingress (`POST /events`) | API Gateway + ingress-lambda | Container, host port `8088` | [architecture](architecture.md#ingress-lambda) |
| Raw topic and CDC topic | MSK | Kafka `localhost:9092` | [architecture](architecture.md#ordering-per-key-and-partitions) |
| CDC from `orders` | MSK Connect (Debezium) + RDS PostgreSQL | Kafka Connect `8083` + `postgres:16` | [slot lag runbook](runbooks/connector-restart-and-slot-lag.md) |
| Stream processing | ECS Fargate (stream-processor) | JVM process | [ADR 0001](adr/0001-kafka-streams-vs-lambda.md) |
| Delivery and retries | ECS Fargate (delivery-service) | JVM process | [ADR 0003](adr/0003-retry-topics.md) |
| Schemas | Glue Schema Registry | Confluent Schema Registry `8081` | [ADR 0002](adr/0002-glue-vs-confluent-sr.md) |
| Downstream | Customer HTTP service | WireMock `8089` | [architecture](architecture.md#delivery-service) |
| Replay | Operator runs replay-cli | Same | [DLQ replay runbook](runbooks/dlq-replay.md) |
| Monitoring | CloudWatch | Kafka UI `8090` | `TODO (M4)` |
| Infrastructure | CloudFormation (`infra/`) | docker compose (`scripts/`) | [deploy runbook](runbooks/deploy.md) |

## 2. How to redeploy

- Local: `bash scripts/down.sh` then `bash scripts/up.sh`. Note that `down` removes volumes unless told otherwise. `TODO (M1)`: confirm.
- AWS: follow [deploy.md](runbooks/deploy.md). Stack order matters. `TODO (M4)`: add the exact commands.
- Teardown: follow [teardown.md](runbooks/teardown.md). It is required after every session.

## 3. How to operate

### DLQ

- Look at `integration.dlq` by error class and `failed-at` before doing anything.
- Replay only after the cause is fixed. See [DLQ replay](runbooks/dlq-replay.md).

### Consumer lag

- Check lag per consumer group on the canonical topics (Kafka UI locally, MSK metrics on AWS).
- Rising lag with healthy downstream: check stream-processor and delivery-service task health and scaling.
- Rising lag with downstream errors: the retry path is in use. Expect retry topics to grow.

### Connector and replication slot

- Check connector status and slot lag. Restart before anything else. See [connector restart and slot lag](runbooks/connector-restart-and-slot-lag.md).
- Never drop a replication slot without approval.

### Schemas

- Schema changes follow [schema evolution](runbooks/schema-evolution.md). Consumers before producers.

## 4. Incident triage: what to check first

1. **Is ingress returning 202?** If not, check API Gateway and ingress-lambda logs, then MSK connectivity and IAM.
2. **Is the CDC connector running and is the slot advancing?** A stalled connector grows WAL on the database. Check this early.
3. **Is the stream-processor running?** Check task health and consumer lag on the raw and CDC topics.
4. **Is the DLQ growing?** Group by error class to find the cause.
5. **Is the downstream healthy?** Retry topics growing means downstream errors or timeouts.
6. **Are IAM permissions intact?** Authorization errors often appear as connection failures on AWS.

## 5. Reference

- [Architecture and delivery semantics](architecture.md)
- [ADRs](adr/)
- [Runbooks](runbooks/)
- [Cost guardrails](cost.md)

`TODO (M5)`: add contacts and on-call ownership. Do not add personal names or account identifiers to this file.
