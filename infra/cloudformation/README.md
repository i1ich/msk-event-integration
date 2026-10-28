# CloudFormation templates

Binding names, exports and IAM rules are in [CONTRACT.md](CONTRACT.md). Templates are plain YAML (no CDK) and are
linted with `cfn-lint==1.20.2` (same as CI). Stacks are named `${EnvName}-msk-ei-<stack>`; exports
`${EnvName}-msk-ei-<Name>`. Nothing uses `Retain`; log groups are explicit with 7-day retention, so deleting a stack
leaves nothing behind (apart from the items under "Deletion behaviour").

## Stacks (core set)

| Template | Creates | Key parameters | Outputs / exports |
|---|---|---|---|
| `budget.yaml` (account-level) | `AWS::Budgets::Budget` monthly COST; e-mail at ACTUAL > 80% and FORECASTED > 100% | `MonthlyLimitUsd` (20), `AlertEmail` | - |
| `github-oidc.yaml` (account-level, optional) | GitHub OIDC provider (or reuse one) and role `msk-ei-github-deploy` | `GitHubOwner`, `GitHubRepo`, `GitHubEnvironment` (aws), `ExistingOidcProviderArn` | Output `DeployRoleArn` (repo variable `AWS_DEPLOY_ROLE_ARN`) |
| `artifacts.yaml` | S3 bucket (generated name), SSE-S3, public access blocked, TLS-only, no versioning | `EnvName` | `ArtifactsBucket` |
| `network.yaml` | VPC 10.40.0.0/16, 2 private subnets (AZ 0/1), 1 public subnet, IGW, one NAT gateway + EIP, route tables, free S3 gateway endpoint, security groups client / MSK (9098 from client) / DB (5432 from client) | `EnvName`, `VpcCidr` | `VpcId`, `PrivateSubnet1`, `PrivateSubnet2`, `ClientSecurityGroup`, `MskSecurityGroup`, `DbSecurityGroup` |
| `msk.yaml` | Provisioned: `AWS::MSK::Configuration` (2-broker server properties), 2 brokers, EBS 10 GiB, IAM only, TLS, broker logs to `/aws/msk/<env>-msk-ei`. Serverless: `AWS::MSK::ServerlessCluster` with IAM | `ClusterType` (provisioned), `BrokerInstanceType` (kafka.t3.small), `KafkaVersion` (3.9.x), `BrokerVolumeSizeGiB` | `ClusterArn` |
| `topics.yaml` | Lambda (Java 21, `TopicsHandler`) in the private subnets + role limited to the seven topics + `Custom::KafkaTopics` | `BootstrapServers`, `BootstrapArtifactKey`, `TopicPartitions` (3), `ReplicationFactor` (-1), `TopicsVersion` | Output `TopicsFunctionName` |
| `schema-registry.yaml` | `AWS::Glue::Registry` `<env>-msk-ei` | `EnvName` | `RegistryName`, `RegistryArn` |
| `database.yaml` | Generated secret, subnet group, parameter group (`rds.logical_replication=1`), RDS PostgreSQL 16 `db.t4g.micro` 20 GiB gp3, secret attachment, `DbInit` Lambda + `Custom::DbInit` | `EngineVersion`, `BootstrapArtifactKey`, `SchemaVersion` | `DbEndpoint`, `DbSecretArn`, `DbInitFunctionName` |

## Decisions worth knowing

- **Kafka version**: default `3.9.x`, the version the AWS docs mark "Recommended", and the last one supporting both
  metadata modes. `3.9.x` is the **ZooKeeper** mode; `3.9.x.kraft` selects **KRaft**. Versions 4.0/4.1 are KRaft-only,
  4.2 is Express-only (docs checked 2026-10-27, supported-kafka-versions). The AWS docs list no kafka.t3.small restriction
  for these versions or for IAM, but I could not find a page that states t3.small support per version explicitly; the first
  deploy confirms it. ZooKeeper vs KRaft is invisible to clients (same bootstrap and IAM endpoints).
- **Custom MSK configuration**: defaults assume 3+ brokers; with 2 brokers the internal topics and the
  transaction log would be under-replicated, so `exactly_once_v2` would fail. `min.insync.replicas=1` is a demo trade-off.
- **NAT gateway** instead of interface endpoints: cheaper for this demo (ADR 0005). Single NAT = no AZ redundancy for
  outbound traffic.
- **Custom resources** (topics, DbInit): neither Kafka topics nor SQL have CloudFormation types and both targets are
  private; Lambdas in the VPC with the client security group are the way in.
- **RDS backups**: `BackupRetentionPeriod: 1` with `DeleteAutomatedBackups: true`. Retention 0 disables automated backups
  and I could not confirm offline that logical replication slots behave well with it; 1 day costs nothing at this size
  and is removed with the instance. `EngineVersion` default `16.13` must exist in the target region; if it does not, pass
  another 16.x (`aws rds describe-db-engine-versions --engine postgres`).
- **Secret has no fixed name**: a deleted secret with a fixed name stays pending deletion and blocks re-creation.

## Deletion behaviour

- All stacks delete cleanly. No `Retain`, no termination protection, RDS without final snapshot and without kept backups.
- The artifacts bucket must be empty before its stack is deleted; `teardown` empties it first.
- Glue schemas registered by `register-schemas-glue.sh` must be deleted before the registry stack (teardown does it).
- Budget and OIDC stacks are account-level and kept by teardown unless `--all`.
- Export dependencies enforce reverse-order deletion.

## GitHub deploy role: scope, honestly

The role `msk-ei-github-deploy` is assumable only through OIDC by `repo:<owner>/<repo>:environment:<env>` (default
`aws`), so protect that GitHub environment with required reviewers. Its permissions are **broad because it deploys
everything**: full actions on EC2/VPC, MSK, Kafka Connect, RDS, ECS, ECR, Lambda, API Gateway, Glue, Logs, CloudWatch and
SNS on `*`; CloudFormation only on stacks `*-msk-ei-*`; Secrets Manager only on secrets and S3 only on buckets with
`-msk-ei-` in the name; IAM role management and `iam:PassRole` only on roles `*-msk-ei-*`. Known gap: it can attach any
managed policy to such roles (including admin), so anyone able to run workflows in that environment can effectively
escalate to admin inside the account. Use a dedicated sandbox account and add a permissions boundary for anything more
serious.
