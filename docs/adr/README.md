# Architecture decision records

| # | Title | Status | Decision in one line |
|---|---|---|---|
| [0001](0001-kafka-streams-vs-lambda.md) | Kafka Streams instead of Lambda for stream processing | Accepted | Lambda only for request-driven ingress; processing is a Kafka Streams app (`exactly_once_v2`) on ECS Fargate, a container locally. |
| [0002](0002-glue-vs-confluent-sr.md) | AWS Glue Schema Registry on AWS, Confluent Schema Registry locally | Accepted | One registry per deployment, switched by `SCHEMA_REGISTRY_TYPE`; Avro schemas stay in `schemas/`, compatibility `BACKWARD`. |
| [0003](0003-retry-topics.md) | Retry topics instead of in-process or single-topic retry | Accepted | `retry.1m` then `retry.10m` then `dlq`, 3 attempts, manual commits, permanent errors go straight to the DLQ. |
| [0004](0004-iam-auth.md) | IAM authentication to MSK | Accepted (not yet exercised on AWS) | IAM only on MSK via `aws-msk-iam-auth`, switched by `KAFKA_SECURITY=iam`; plaintext locally. |
| [0005](0005-nat-vs-vpc-endpoints.md) | One NAT gateway instead of interface VPC endpoints | Accepted | A single NAT gateway is cheaper than five interface endpoints per AZ for this demo; S3 uses the free gateway endpoint. |
| [0006](0006-msk-provisioned-in-sa-east-1.md) | MSK provisioned cluster in sa-east-1 | Accepted | MSK Serverless is not offered in sa-east-1, so the default is provisioned (2 x `kafka.t3.small`). |

Other documents: [architecture](../architecture.md), [cost](../cost.md), [runbooks](../runbooks/README.md).
