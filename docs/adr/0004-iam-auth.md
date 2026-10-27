# ADR 0004: IAM authentication to MSK

- Status: Accepted
- Date: M1

## Context

Amazon MSK clusters can be reached with several authentication methods: IAM (SASL/OAUTHBEARER via `aws-msk-iam-auth`), SASL/SCRAM with secrets, or mutual TLS. The platform runs on ECS Fargate, Lambda, and MSK Connect, which all have IAM roles. Locally, the Kafka broker is a single-node KRaft container without TLS or SASL.

Secrets (SCRAM passwords, certificates) would need rotation and storage. IAM roles avoid long-lived credentials.

## Decision

- On AWS, all clients (ingress-lambda, stream-processor, delivery-service, replay-cli, MSK Connect) authenticate to MSK with IAM using `aws-msk-iam-auth` 2.2.0 and the task or function execution role.
- Authorization is scoped to the topics and consumer groups each component needs. `TODO (M4)`: define the IAM policies in `infra/msk` and `infra/processing`.
- Locally, the broker is reached on `localhost:9092` with no authentication. This is a development-only setting and is not used on AWS.
- HTTP ingress has its own optional HMAC check (`INGRESS_HMAC_SECRET`). It is independent of Kafka authentication.

## Consequences

Positive:
- No Kafka passwords or certificates to store or rotate.
- Access is auditable through IAM and CloudTrail.
- Clients need only the library and the correct role.

Negative:
- Clients must run with the IAM role; running from a laptop against MSK needs temporary credentials.
- Local and AWS configurations differ (plaintext vs IAM SASL). Config must be switched by environment (`KAFKA_SECURITY` in `.env`), and mistakes show up only at deploy time. Covered by the M4 deploy check.
- IAM policy mistakes cause authorization errors that look like connection errors. Check IAM first when clients fail to connect on AWS.
- The HMAC secret for HTTP ingress is still a shared secret and needs its own handling (`TODO (M4)`: store in Secrets Manager, not in `.env`).
