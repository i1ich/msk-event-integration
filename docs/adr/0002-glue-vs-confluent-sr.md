# ADR 0002: AWS Glue Schema Registry on AWS, Confluent Schema Registry locally

- Status: Accepted
- Date: 2026-10-27

## Context

Avro schemas for the canonical events and the CDC payload need a registry for id-based serialization and compatibility checks. The local stack runs Confluent Schema Registry in docker compose. On AWS, the deployment needs a managed registry that CloudFormation can provision and that IAM can secure.

Two registries with different serializers and wire formats create a risk: a record written with one cannot be read with the other. Glue's wire format (header byte, compression byte, 16-byte schema version UUID) is not the Confluent one.

## Decision

- **On AWS: AWS Glue Schema Registry everywhere.** Registry `${EnvName}-msk-ei`. Schemas are created and registered by `scripts/register-schemas-glue.sh`. Schema source of truth stays in `schemas/`.
- **Locally: Confluent Schema Registry** on port 8081, using the Confluent Avro serializers. There is no Glue emulator.
- **A `SCHEMA_REGISTRY_TYPE` switch in `common`** (`confluent` local default, `glue` on AWS) selects the serializer and deserializer.
- **The registry is chosen per deployment, never per topic.** The wire formats are incompatible, so one deployment uses one registry for all topics.
- Compatibility mode `BACKWARD`, `TopicNameStrategy`, in both environments.

## Consequences

Positive:
- The AWS path has no self-hosted registry to run, patch or back up.
- Glue Schema Registry usage is free (AWS Glue pricing page, checked 2026-10-27), so the registry adds no hourly cost.
- Access is IAM-only, so it uses the same roles and audit trail as MSK (ADR 0004).

Negative:
- The Glue path is verified only by the AWS smoke test (`scripts/smoke-test.sh`). Local tests cover the Confluent path only.
- Confluent console tools and Kafka UI cannot decode Glue-encoded records. Debugging on AWS needs the Glue SDK or the AWS console.
- Debezium on MSK Connect uses the AWS Glue Kafka Connect converter (`AWSKafkaAvroConverter`, pinned in `infra/connect/plugin/pom.xml`), not the Confluent converter. The local Connect worker uses the Confluent converter, so the two connector configurations differ.
- Compatibility checks differ: Glue has its own API, not the Confluent compatibility endpoint. The M3 schema-compatibility CI check runs against the local registry only.
- Schema ids are not portable. Data written locally does not carry over to AWS.

## Alternatives considered

- **Confluent Schema Registry on Fargate, on AWS as well.** One code path and one wire format everywhere, and the CI check covers AWS too. Rejected: the registry becomes a self-operated service with its own task, storage and upgrades. Access is by network position only (no IAM), so it needs its own authentication layer.
- **Hybrid: Glue for one topology only.** Rejected: the system would run two registries and two wire formats at once, and consumers would need to know which one produced a given topic.
