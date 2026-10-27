# ADR 0002: AWS Glue Schema Registry on AWS, Confluent Schema Registry locally

- Status: Accepted
- Date: M1

## Context

Avro schemas for the canonical events and the CDC payload need a registry for id-based serialization and compatibility checks. Locally, the Confluent stack (`cp-schema-registry` 7.9.0) is available in docker compose and is the common reference. On AWS, the managed option that fits the deployment is AWS Glue Schema Registry, which is provisioned by CloudFormation (`infra/schema-registry`).

Using two registries creates a risk: serializer configuration and wire format may differ.

## Decision

- Locally: Confluent Schema Registry on port `8081`, using the Confluent Avro serializers (7.9.0).
- On AWS: AWS Glue Schema Registry.
- Subject naming: `TopicNameStrategy`, so each topic's value schema is `<topic>-value`.
- Compatibility mode: `BACKWARD` in both environments.
- Schema source of truth: `schemas/` in this repository.

## Consequences

Positive:
- Local development matches the schema rules (`BACKWARD`, subject naming) that AWS enforces.
- Glue removes a self-hosted registry from the AWS deployment.
- Compatibility can be tested in CI against the local registry before deploying.

Negative:
- The two registries use different serializers and wire formats. Glue's serde library is not in the pinned dependency list (the pinned list has Confluent serdes 7.9.0 only). `TODO (M4)`: choose and pin the Glue serde library, or confirm that a single serializer path works for both. Record the decision in a follow-up ADR.
- Compatibility checks differ: the Confluent compatibility endpoint is not the same API as Glue's. The schema-compatibility CI check (M3) must work against the local registry, and the Glue behavior is verified in M4.
- Schema ids are not portable between registries. Data written locally does not carry over to AWS.
