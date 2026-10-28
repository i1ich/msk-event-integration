# AWS deployment contract (M4)

Single source of truth for everyone writing templates, scripts or AWS-facing code. If a template, a script and
this file disagree, this file wins; change it first, then the code.

## Region and cluster type

- Default region: **sa-east-1** (São Paulo). Everything is a parameter; nothing is hard-coded to a region.
- **MSK Serverless is not available in sa-east-1** (checked against the AWS docs on 2026-10-27). The `ClusterType`
  parameter keeps both options, but the default is `provisioned` (2 × `kafka.t3.small`, one per AZ).
  `deploy.sh` refuses `serverless` in a region that does not support it.
- Schema registry on AWS: **AWS Glue Schema Registry everywhere** (ingress publishes raw JSON, no registry).

## Naming

- `EnvName` parameter (default `dev`, pattern `[a-z][a-z0-9-]{1,15}`).
- Stack names: `${EnvName}-msk-ei-<stack>` (e.g. `dev-msk-ei-network`).
- Resource name prefix: `${EnvName}-msk-ei-...` wherever a name is needed.
- Exports: `${EnvName}-msk-ei-<Name>` (e.g. `dev-msk-ei-VpcId`). Import with `!ImportValue !Sub '${EnvName}-msk-ei-VpcId'`.
- Tags on every taggable resource: `Project=msk-event-integration`, `Env=${EnvName}`.
- No account IDs, ARNs or emails in the repo. Use pseudo parameters (`AWS::AccountId`, `AWS::Region`,
  `AWS::Partition`) and parameters.

## Stacks, in deploy order

| # | Template | Stack | Purpose | Exports (all prefixed `${EnvName}-msk-ei-`) |
|---|---|---|---|---|
| 0 | `budget.yaml` | `msk-ei-budget` (account-level, no EnvName) | AWS Budget, monthly cost limit (default 20 USD), email alerts at 80% actual / 100% forecast. Deployed FIRST, once. | – |
| 0 | `github-oidc.yaml` | `msk-ei-github-oidc` (once, optional) | GitHub OIDC provider + deploy role trusted only for `repo:<owner>/<repo>:environment:aws` (protect the `aws` environment in GitHub: required reviewer, branch `main`) | – (role ARN is an Output, stored as repo variable `AWS_DEPLOY_ROLE_ARN`) |
| 1 | `artifacts.yaml` | `artifacts` | S3 bucket for Lambda jars and the Connect plugin zip. Versioning off, public access blocked, SSE-S3. Emptied by teardown before delete. | `ArtifactsBucket` |
| 2 | `network.yaml` | `network` | VPC 10.40.0.0/16, 2 private subnets (2 AZs), 1 public subnet + IGW + **one NAT gateway**, S3 gateway endpoint (free). SGs below. | `VpcId`, `PrivateSubnet1`, `PrivateSubnet2`, `ClientSecurityGroup`, `MskSecurityGroup`, `DbSecurityGroup` |
| 3 | `msk.yaml` | `msk` | MSK cluster (`ClusterType` provisioned\|serverless), IAM auth only, TLS in transit. For provisioned: `AWS::MSK::Configuration` (see below), Kafka version param, EBS 10 GiB, broker logs to CloudWatch (7 days). | `ClusterArn` |
| 4 | `topics.yaml` | `topics` | Custom resource (infra-bootstrap `TopicsHandler`) creating topics from `scripts/topics.txt` (bundled in the jar). | – |
| 5 | `schema-registry.yaml` | `schema-registry` | `AWS::Glue::Registry` `${EnvName}-msk-ei`. Schemas are registered from `schemas/` by `scripts/register-schemas-glue.sh` (source of truth stays in the repo). | `RegistryName`, `RegistryArn` |
| 6 | `database.yaml` | `database` | RDS PostgreSQL 16 `db.t4g.micro`, single-AZ, 20 GiB gp3, parameter group `rds.logical_replication=1`, own `AWS::SecretsManager::Secret` (generated password, user `erp`), not public, deletion protection off, `DeleteAutomatedBackups: true`, no final snapshot (`DeletionPolicy: Delete`). Custom resource (infra-bootstrap `DbInitHandler`) runs the SQL from `infra/postgres/init/01-schema.sql` (bundled). | `DbEndpoint`, `DbSecretArn` |
| 7 | `ecr.yaml` | `ecr` | ECR repos `${EnvName}-msk-ei/stream-processor`, `/delivery-service`, `/downstream-mock`, `EmptyOnDelete: true`, scan on push, lifecycle keep last 5. | `RepoStreamProcessor`, `RepoDeliveryService`, `RepoDownstreamMock` (repository URIs) |
| 8 | `msk-connect.yaml` | `msk-connect` | `AWS::KafkaConnect::CustomPlugin` (zip in artifacts bucket), `WorkerConfiguration`, `Connector` (Kafka Connect 3.7.x, provisioned 1 MCU × 1 worker), service execution role, worker logs to CloudWatch (7 days). | – |
| 9 | `ingress.yaml` | `ingress` | API Gateway REST API `POST /events` (Lambda proxy, API key required, usage plan 10 rps / burst 20 / 10 000 per day), Lambda Java 21 in the private subnets, HMAC secret (`AWS::SecretsManager::Secret`, generated), explicit log groups (7 days), alarms: API 5XX, Lambda errors, Lambda throttles. | `ApiUrl` (Output), `ApiKeyId` (Output). API Gateway's CloudWatch Logs role is one setting per account and region: `ConfigureApiGatewayAccountRole` defaults to `false`; `deploy.sh` sets it to `true` only when the account has no role yet, so other projects' APIs are never affected. |
| 10 | `processing.yaml` | `processing` | ECS cluster, Fargate services `stream-processor` and `delivery-service` (+ `downstream-mock` sidecar container in the delivery task), desired count 1, private subnets, no public IP, task roles, log groups (7 days). | `EcsClusterName` |
| 11 | `observability.yaml` | `observability` | CloudWatch dashboard (ingress rate/5XX, Lambda errors/duration, consumer lag, bytes in per topic incl. DLQ), alarms DLQ `BytesInPerSec` > 0 and consumer lag > threshold. All used MSK metrics are DEFAULT level (free): per-topic `BytesInPerSec` (Cluster Name, Broker ID, Topic) and `SumOffsetLag`/`MaxOffsetLag` (Cluster Name, Consumer Group, Topic); the cluster keeps `EnhancedMonitoring: DEFAULT`, SNS topic with email subscription (parameter). | – |

Deploy order in `deploy.sh` puts **observability (11) before ingress (9)**: the ingress alarms publish to the observability SNS topic (passed as `AlarmTopicArn`). Teardown deletes processing, ingress, observability, then 8 → 1, empties the artifacts bucket and deletes the Glue schemas registered by the
script before their stacks. The budget and OIDC stacks are kept (free) unless `--all` is given.

### Why NAT and not VPC endpoints (ADR 0005)

Fargate (ECR pull, CloudWatch Logs), the services (Glue Schema Registry API), the Lambdas (Secrets Manager,
CloudFormation response URL) need AWS APIs. Interface endpoints would be needed for ecr.api, ecr.dkr, logs, glue,
secretsmanager (5 × per AZ, billed per hour each); one NAT gateway is cheaper for this demo and simpler. S3 uses the free
gateway endpoint. Exact prices are in `docs/cost.md`.

## Security groups

- `ClientSecurityGroup`: attached to Lambdas, ECS tasks, MSK Connect workers, bootstrap Lambdas. Egress all.
- `MskSecurityGroup`: ingress TCP 9098 (IAM, provisioned) and 9098 for serverless, from `ClientSecurityGroup` only.
- `DbSecurityGroup`: ingress TCP 5432 from `ClientSecurityGroup` only.

## MSK provisioned configuration (2 brokers)

Defaults assume 3+ brokers, which breaks a 2-broker cluster (exactly_once_v2 needs the transaction state log).
`AWS::MSK::Configuration` server properties:

```
auto.create.topics.enable=false
default.replication.factor=2
min.insync.replicas=1
offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
transaction.state.log.min.isr=1
num.partitions=3
```

`min.insync.replicas=1` with `acks=all` means one broker may be down for maintenance without stopping producers;
documented as a demo trade-off (production: 3 brokers, RF 3, min ISR 2).

## Bootstrap servers

Neither `AWS::MSK::Cluster` nor `AWS::MSK::ServerlessCluster` returns bootstrap brokers via `GetAtt`. `deploy.sh`
reads them after stack 3 with `aws kafka get-bootstrap-brokers --cluster-arn ...` (`BootstrapBrokerStringSaslIam`)
and passes them as the `BootstrapServers` parameter to stacks 4, 8, 9, 10.

## IAM for Kafka (least privilege)

Cluster ARN: `arn:${AWS::Partition}:kafka:${AWS::Region}:${AWS::AccountId}:cluster/<ClusterName>/<ClusterUuid>`.
Derive name and UUID with `!Select [1, !Split ['/', ClusterArn]]` and `!Select [2, ...]`. Then:

- topic: `arn:...:topic/<ClusterName>/<ClusterUuid>/<topic>`
- group: `arn:...:group/<ClusterName>/<ClusterUuid>/<group>`
- transactional id: `arn:...:transactional-id/<ClusterName>/<ClusterUuid>/<id>`

| Principal | Cluster | Topics | Groups / txn ids |
|---|---|---|---|
| ingress Lambda | Connect, DescribeCluster | `sf.change-events.raw`: WriteData, DescribeTopic | `kafka-cluster:WriteDataIdempotently` on the cluster |
| stream-processor task | Connect, DescribeCluster, WriteDataIdempotently | read `sf.change-events.raw`, `erp.public.orders`; write `integration.account.v1`, `integration.order.v1`, `integration.dlq`; `stream-processor-*` internal topics: Create/Describe/Read/Write/Delete | group `stream-processor`; transactional-id `stream-processor-*` (Describe, Alter) |
| delivery-service task | Connect, DescribeCluster, WriteDataIdempotently | read `integration.account.v1`, `integration.order.v1`, `integration.retry.*`; write `integration.retry.*`, `integration.dlq` | groups `delivery-service`, `delivery-service-retry` |
| MSK Connect | Connect, DescribeCluster, WriteDataIdempotently | write `erp.public.orders`; `__amazon_msk_connect_*` and `__consumer_offsets`-style internal topics of the connector: Create/Describe/Read/Write | group `__amazon_msk_connect_*`, connector groups `connect-*` |
| TopicsHandler Lambda | Connect, DescribeCluster | all topics of `topics.txt`: CreateTopic, DescribeTopic, AlterTopic | – |

Glue permissions: services `glue:GetRegistry`, `glue:GetSchema`, `glue:GetSchemaVersion`,
`glue:GetSchemaByDefinition`, `glue:QuerySchemaVersionMetadata` on the registry and its schemas. MSK Connect
additionally `glue:CreateSchema`, `glue:RegisterSchemaVersion`, `glue:PutSchemaVersionMetadata` on schema
`erp.public.orders*` only (Debezium registers its own envelope schema).

## Glue Schema Registry conventions

- Registry name: `${EnvName}-msk-ei` (exported `RegistryName`).
- Schema name = topic name: `integration.account.v1`, `integration.order.v1`, `erp.public.orders` (value).
  Compatibility `BACKWARD`, data format `AVRO`.
- `scripts/register-schemas-glue.sh <env>` creates missing schemas (`aws glue create-schema` with the v1 file) and
  registers later versions (`aws glue register-schema-version`) in file order, like `register-schemas.sh` does locally.
- Glue wire format is NOT the Confluent one (Glue: header byte + compression byte + 16-byte schema version UUID).
  A record written with one registry cannot be read with the other's deserializer; the registry is chosen for the
  whole deployment, never per topic.

## Service configuration on AWS (environment variables)

Common to stream-processor, delivery-service, Connect-related code:

| Variable | Value on AWS | Local default |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | from `BootstrapServers` parameter | `localhost:9092` |
| `KAFKA_SECURITY` | `iam` | `plaintext` |
| `SCHEMA_REGISTRY_TYPE` | `glue` | `confluent` |
| `GLUE_REGISTRY_NAME` | `${EnvName}-msk-ei` | – |
| `AWS_REGION` | `${AWS::Region}` (Lambda sets it automatically) | – |
| `SCHEMA_AUTO_REGISTER` | `false` | `true` |
| `SCHEMA_REGISTRY_URL` | unset | `http://localhost:8081` |

Ingress Lambda: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SECURITY=iam`, `INGRESS_HMAC_SECRET_ARN` (fetched once per cold start
from Secrets Manager; `INGRESS_HMAC_SECRET` stays for local use), handler
`io.github.i1ich.mskei.ingress.IngressHandler::handleRequest`, memory 1024 MB, timeout 15 s, SnapStart off by default
(parameter; cold-start numbers measured during the M4 run go to `docs/cost.md` / README).

Delivery service: `DOWNSTREAM_URL=http://localhost:8080/downstream` (WireMock sidecar in the same task, image built from
`infra/wiremock/Dockerfile` with the same mappings as local), retry delays default PT1M / PT10M.

Stream processor: `STREAMS_APPLICATION_ID=stream-processor`, `STREAMS_REPLICATION_FACTOR=-1` (broker default).

Task sizes: stream-processor 0.5 vCPU / 1 GB, delivery-service 0.5 vCPU / 1 GB (incl. sidecar 0.25 GB).

## Artifacts

- Lambda jars in the artifacts bucket: `lambda/ingress-lambda-<gitsha>.jar`, `lambda/infra-bootstrap-<gitsha>.jar`
  (the fat jars `*/target/<module>.jar`). Templates take `ArtifactsBucket` (import) and `<X>ArtifactKey` parameters.
- Connect plugin: `connect/debezium-postgres-glue-<gitsha>.zip`, built by `scripts/build-connect-plugin.sh`:
  Debezium PostgreSQL 2.5.4.Final + AWS Glue Schema Registry Kafka Connect converter 1.1.27 (+ its dependencies),
  resolved with Maven from `infra/connect/plugin/pom.xml` (pinned, reproducible, no ad-hoc downloads).
- Images: `<repo-uri>:<gitsha>`; the `processing.yaml` parameters take the full image URIs.

## Debezium on MSK Connect

Same connector settings as `infra/connect/orders-source.local.json`, plus:

- `database.hostname` = `DbEndpoint`, `database.user=erp`, `database.password` via dynamic reference to `DbSecretArn`
  (`{{resolve:secretsmanager:...:SecretString:password}}`). Trade-off documented: the resolved value is visible to whoever
  can describe the connector; production would use the MSK config provider for Secrets Manager.
- `key.converter=org.apache.kafka.connect.json.JsonConverter`, `key.converter.schemas.enable=false` (the stream
  processor takes the order id from the value; only the DLQ keeps key bytes).
- `value.converter=com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter`,
  `value.converter.region`, `value.converter.registry.name`, `value.converter.schemaAutoRegistrationEnabled=true`,
  `value.converter.avroRecordType=GENERIC_RECORD`, `value.converter.compatibility=BACKWARD`.

## Smoke test (`scripts/smoke-test.sh <env>`)

1. `POST {ApiUrl}/events` with `x-api-key` (from `aws apigateway get-api-key --include-value`) and `X-Signature`
   (HMAC from the secret) using `samples/account-create-with-industry.json`; expect 202 and an `eventId`.
2. Within 120 s, the delivery-service log group contains a log line with that `eventId` and HTTP status 200
   (`aws logs filter-log-events`). This proves ingress → MSK → stream-processor → canonical topic → delivery.
3. CDC: invoke the `DbInitHandler` Lambda with `{"action":"smoke-order"}` (inserts, updates and deletes one order) and
   expect three delivery log lines with path `/orders` within 180 s (delivery logs the path relative to `DOWNSTREAM_URL`).
Exit non-zero on any failure; print what was checked.

## Hard rules

- No AWS command that creates billable resources runs without the owner's explicit go-ahead in the session.
  Writing templates and scripts is fine; executing `deploy.sh` is not. `cfn-lint`, `aws cloudformation validate-template`
  are fine only offline (cfn-lint); do not call AWS APIs at all while building M4.
- `cfn-lint` clean (version 1.20.2, same as CI). Run it with Docker:
  `docker run --rm -v "$(pwd -W):/repo" -w /repo python:3.12-slim sh -c "pip install -q cfn-lint==1.20.2 && cfn-lint infra/cloudformation/*.yaml"`
  (prefix with `MSYS_NO_PATHCONV=1` in Git Bash).
- Everything deletable by teardown: no `Retain`, no termination protection, explicit log groups (so they are deleted
  with the stack), ECR `EmptyOnDelete`, RDS without final snapshot.
