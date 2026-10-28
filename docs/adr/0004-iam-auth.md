# ADR 0004: IAM authentication to MSK

- Status: Accepted. Implemented in code and templates; **not yet exercised on AWS** (the first AWS run stopped before MSK, see [cost.md](../cost.md); deploy pending Paid plan).
- Date: M1 (updated M5)

## Context

Amazon MSK clusters can be reached with several authentication methods: IAM (SASL with `aws-msk-iam-auth`), SASL/SCRAM with secrets, or mutual TLS. The platform runs on ECS Fargate, Lambda, and MSK Connect, which all have IAM roles. Locally, the Kafka broker is a single-node KRaft container without TLS or SASL.

Secrets (SCRAM passwords, certificates) would need rotation and storage. IAM roles avoid long-lived credentials.

## Decision

- On AWS, the MSK cluster accepts IAM authentication only (port 9098, TLS). All clients (ingress-lambda, stream-processor, delivery-service, replay-cli, the topics custom resource, MSK Connect) authenticate with the role of the task or function, through `aws-msk-iam-auth` 2.2.0.
- Clients choose the mode with one variable, `KAFKA_SECURITY`: `plaintext` (default, local) or `iam`. In `KafkaClientConfig` (`common`), `iam` sets `security.protocol=SASL_SSL`, `sasl.mechanism=AWS_MSK_IAM`, `IAMLoginModule` and `IAMClientCallbackHandler`; credentials come from the default AWS provider chain.
- Authorization is least privilege per component and is defined in the CloudFormation templates (`ingress.yaml`, `processing.yaml`, `msk-connect.yaml`, `topics.yaml`). The matrix is in [`CONTRACT.md`](../../infra/cloudformation/CONTRACT.md) ("IAM for Kafka"): for example the ingress role may only write `sf.change-events.raw`, and the delivery-service may only read the canonical and retry topics and write the retry topics and the DLQ.
- Locally, the broker is reached on `localhost:9092` with no authentication (`KAFKA_SECURITY=plaintext` in `docker-compose.yml`). This is a development-only setting.
- HTTP ingress has its own optional HMAC check (`INGRESS_HMAC_SECRET`) and, on AWS, an API key. Both are independent of Kafka authentication. On AWS the HMAC secret lives in Secrets Manager (`INGRESS_HMAC_SECRET_ARN`).

## Consequences

Positive:
- No Kafka passwords or certificates to store or rotate.
- Access is auditable through IAM and CloudTrail.
- Clients need only the library and the correct role.

Negative:
- Clients must run with an IAM role; running `replay-cli` from a laptop against MSK needs temporary credentials and a network path into the VPC.
- Local and AWS configurations differ (plaintext vs IAM SASL). The switch is `KAFKA_SECURITY`, so mistakes show up only at deploy time. The check is `scripts/smoke-test.sh` after the first successful deploy. The IAM policies in the templates are lint-clean but not validated against a live cluster.
- IAM policy mistakes cause authorization errors that look like connection errors. Check IAM first when clients fail to connect on AWS.
- The HMAC secret for HTTP ingress is still a shared secret and needs its own handling.
