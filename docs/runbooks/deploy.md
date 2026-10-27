# Runbook: deploy

Status: outline. `TODO (M4)`: fill in once the CloudFormation stacks are deployed and verified.

## Prerequisites

- AWS account and region chosen; `AWS_REGION` and `ENV_NAME` set. No account IDs or ARNs are committed to the repository.
- A budget alert created before any resource (see [cost guardrails](../cost.md)).
- Java 21 and Maven build passing: `mvn -B verify`.
- cfn-lint passing (`TODO (M3)`).

## Stack order

Deploy in dependency order:

1. `network`: VPC, subnets, NAT gateway or endpoints.
2. `msk`: serverless by default, or provisioned (2 x `kafka.t3.small`).
3. `topics`: created through a custom resource or an admin script. CloudFormation has no native resource for MSK topics.
4. `schema-registry`: AWS Glue Schema Registry.
5. `msk-connect`: Debezium connector, with RDS PostgreSQL (`db.t4g.micro`).
6. `ingress`: API Gateway and ingress-lambda.
7. `processing`: ECS Fargate for stream-processor and delivery-service.
8. `observability`: dashboards and alarms.

## Steps

`TODO (M4)`: exact commands and parameter names.

1. Build artifacts.
2. Deploy each stack in the order above.
3. Create topics and register schemas.
4. Run the smoke test: send one event through ingress and confirm it reaches the canonical topic.

## Verify

`TODO (M4)`: list of checks and expected outputs.

## Rollback

`TODO (M4)`: rollback approach per stack. Prefer stack deletion over partial manual changes.
