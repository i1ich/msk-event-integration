# Knowledge transfer

Audience: an operations team taking over this platform. Read [architecture](architecture.md) first for how the parts fit and what each guarantees.

Status: the local stack is complete and tested (`mvn -B verify`, `-Pit`). The AWS templates and scripts are written and lint-clean but have never completed a deployment: the first run stopped at the `msk` stack because the account is on the AWS Free plan, which does not include Amazon MSK ([cost.md session log](cost.md#session-log)). Everything about AWS operation below is design intent. Pending: AWS run (needs Paid plan).

## 1. What runs where

| Part | AWS | Local |
|---|---|---|
| Ingress `POST /events` | API Gateway + ingress-lambda (Java 21, private subnets) | `LocalServer` container, host port `8088` |
| Kafka | MSK provisioned, 2 brokers, IAM auth | 1 broker KRaft, plaintext, `localhost:9092` |
| CDC from `orders` | MSK Connect (Debezium) + RDS PostgreSQL 16 | Kafka Connect `8083` + `postgres:16` (host port `5433`) |
| Stream processing | ECS Fargate `stream-processor` | container |
| Delivery and retries | ECS Fargate `delivery-service` + WireMock sidecar as downstream | container + WireMock `8089` |
| Schemas | Glue Schema Registry `<env>-msk-ei` | Confluent Schema Registry `8081` |
| Replay | `replay-cli` run by an operator (see limitations) | `java -jar replay-cli/target/replay-cli.jar` |
| Monitoring | CloudWatch dashboard `<env>-msk-ei`, alarms, SNS email | Kafka UI `8090` (profile `ui`), `docker compose logs` |
| Infrastructure | CloudFormation (`infra/cloudformation`), `scripts/deploy.sh` | `docker-compose.yml`, `scripts/up.sh` |

More detail: [architecture](architecture.md), [ADRs](adr/), [CONTRACT.md](../infra/cloudformation/CONTRACT.md).

## 2. Build and test

Prerequisites: Java 21, Maven 3.9+, bash. Docker for the local stack and the end-to-end tests.

```bash
mvn -B verify          # build all modules, unit and topology tests, JaCoCo gate (80% lines per module)
mvn -B verify -Pit     # also the Testcontainers end-to-end tests (needs Docker, several minutes)
```

Fat jars land in `<module>/target/<module>.jar`. Test layers and what each end-to-end test proves: [integration-tests/README.md](../integration-tests/README.md). CI (`.github/workflows/ci.yml`) runs unit tests with the coverage gate, the end-to-end tests, `cfn-lint` 1.20.2, the schema compatibility test and a Trivy scan.

## 3. Run locally

```bash
bash scripts/up.sh      # docker compose up --build, then registers the connector and the schemas
bash scripts/demo.sh    # posts samples/, changes Postgres rows, shows raw/canonical/DLQ topics, WireMock journal, replay dry run
bash scripts/down.sh    # docker compose down -v: removes containers AND volumes (all local data)
```

`make up`, `make demo`, `make down` call the same scripts. For a UI: `docker compose --profile ui up -d kafka-ui`.

| Service | Host port | Notes |
|---|---|---|
| Kafka | 9092 | plaintext |
| Schema Registry | 8081 | |
| Kafka Connect | 8083 | connector `orders-source` |
| PostgreSQL | 5433 | database `erp`, user `erp`; override with `POSTGRES_HOST_PORT` |
| Ingress | 8088 | override with `INGRESS_HOST_PORT`; `/health`, `POST /events` |
| WireMock (downstream) | 8089 | `__admin/requests` shows what was received |
| Kafka UI | 8090 | only with `--profile ui` |

Local retry delays are 5 s and 15 s, so the demo takes about a minute. Demo accounts: `001xx000000FAIL1AAA` (downstream answers 503 once, then 200) and `001xx000000REJECTAAA` (always 400); see [samples](../samples/README.md) and [WireMock](../infra/wiremock/README.md).

## 4. Deploy and tear down on AWS

Full procedures: [deploy runbook](runbooks/deploy.md), [teardown runbook](runbooks/teardown.md). Cost model and guardrails: [cost.md](cost.md).

Prerequisites:
- AWS account on the **Paid plan**. On the Free plan the `msk` stack fails with `SubscriptionRequiredException`; `deploy.sh` checks the plan and stops before creating billable resources.
- AWS CLI v2 **>= 2.13** (the scripts check it), Docker with a running daemon, Java 21, Maven, `openssl`, `curl`, Git Bash on Windows.
- Credentials via SSO or a role; no long-lived keys in the repo. Region `sa-east-1` by default.
- Budgets created first, once per account: `scripts/create-budget.sh <email> [monthly] [daily]`. `deploy.sh` refuses to run without the budget stack.

```bash
scripts/deploy.sh dev        # builds, deploys 11 stacks in order, runs scripts/smoke-test.sh
scripts/teardown.sh dev      # reverse order, then scripts/check-leftovers.sh
```

Rules: deploy, demo and tear down on the same day. The whole stack costs about 0.55 USD per hour; a forgotten teardown costs about 13 USD per day ([cost.md](cost.md)). After every teardown `check-leftovers.sh` must report clean. The workflow `.github/workflows/deploy.yml` (manual) can do the same through OIDC.

## 5. Day-2 operations

### DLQ triage and replay

1. Look at `integration.dlq` and group by `error-class` and `failed-at`. Locally:
   ```bash
   docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 \
     --topic integration.dlq --from-beginning --timeout-ms 10000 \
     --property print.key=true --property print.headers=true
   ```
2. Decide from `source-topic` where it failed: `sf.change-events.raw` or `erp.public.orders` means stream-processor (mapping/data problem, `attempts=1`); `integration.account.v1` or `integration.order.v1` means delivery (permanent downstream error, or retries exhausted, `attempts` 1 to 3). Header meanings: [headers contract](architecture.md#headers-contract).
3. Fix the cause first. Then `replay-cli --dry-run`, then replay in a narrow window at a low rate. Procedure and stop conditions: [DLQ replay runbook](runbooks/dlq-replay.md).
4. Things to know: replay does not delete from the DLQ (running it twice sends twice); a replayed CDC record keeps its `eventId` (built from the WAL position, not the offset); a replayed delivery failure keeps its `eventId`, so the `Idempotency-Key` is unchanged ([why](architecture.md#replay-cli)).

### Connector restart and replication slot lag

The slot `dbz_orders_slot` keeps WAL until the connector confirms it. A stopped connector makes database storage grow. Check connector status and slot lag, restart before anything else, and do not drop the slot without approval: [connector restart and slot lag](runbooks/connector-restart-and-slot-lag.md). No alarm covers this today; check it by hand (local: Kafka Connect on `8083`; AWS: MSK Connect log group and RDS storage).

### Schema evolution

Changes must be `BACKWARD` compatible; consumers are upgraded before producers. Add `.avsc` files under `schemas/` in version order, run `scripts/check-schema-compat.sh` against the local registry, and register with `scripts/register-schemas.sh` (local) or `scripts/register-schemas-glue.sh <env>` (AWS; it also registers the generated-class text, see [architecture](architecture.md#schema-registry-switch-confluent--glue)). If you change an Avro schema, the generated class changes too: rebuild and redeploy the services. Procedure: [schema evolution runbook](runbooks/schema-evolution.md).

### Consumer lag

Groups: `stream-processor` (inputs: raw and CDC topics), `delivery-service` (canonical topics), `delivery-service-retry` (retry topics). Locally:

```bash
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group delivery-service
```

On AWS the dashboard and alarms cover `stream-processor` and `delivery-service`; `delivery-service-retry` lag is not alarmed. Retry consumers wait for `not-before`, so some lag on `integration.retry.*` is normal. Rising lag with a healthy downstream: check task health and logs of the two services. Rising lag with downstream errors: the retry path is in use and the DLQ will fill after about 11 minutes.

## 6. Incident first steps

| Symptom | Where to look first | Next |
|---|---|---|
| `POST /events` returns 5xx or 503 | Ingress logs (JSON, field `eventId`); 503 means the broker did not acknowledge within 10 s. AWS: Lambda errors alarm, API Gateway 5XX alarm | MSK reachability, IAM, security group 9098; [deploy runbook](runbooks/deploy.md) |
| `POST /events` returns 401 / 413 / 400 | Response body (`invalid_signature`, `payload_too_large`, `malformed_json`, `validation_failed`) | Client signature or payload, [samples](../samples/README.md) |
| 202 but nothing delivered | `stream-processor` lag and logs, then `integration.account.v1`, then `delivery-service` logs (`Delivered` lines carry `eventId`) | DLQ for the `eventId` |
| DLQ growing | Group by `error-class` | [DLQ replay](runbooks/dlq-replay.md) |
| Orders missing downstream | Kafka Connect / MSK Connect connector state, topic `erp.public.orders` | [connector runbook](runbooks/connector-restart-and-slot-lag.md) |
| Database disk growing | Replication slot lag | [connector runbook](runbooks/connector-restart-and-slot-lag.md) |
| Retry topics growing | Downstream status and latency | Wait for recovery, replay the DLQ afterwards |
| Clients cannot connect on AWS | IAM policies first (authorization errors look like connection errors), then security groups and NAT route | [CONTRACT.md](../infra/cloudformation/CONTRACT.md#iam-for-kafka-least-privilege) |
| Serialization errors after a schema change | Registry contents, compatibility result | [schema evolution](runbooks/schema-evolution.md) |
| Unexpected AWS cost | Cost Explorer, then `scripts/check-leftovers.sh <env>` | [teardown](runbooks/teardown.md) |

## 7. Configuration reference

Environment variables per service. The AWS values are in [CONTRACT.md](../infra/cloudformation/CONTRACT.md#service-configuration-on-aws-environment-variables).

| Variable | Used by | Default | Meaning |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | all services, replay-cli, TopicsHandler | required | Brokers |
| `KAFKA_SECURITY` | same | `plaintext` | `plaintext` or `iam` |
| `SCHEMA_REGISTRY_TYPE` | stream-processor, delivery-service | `confluent` | `confluent` or `glue` |
| `SCHEMA_REGISTRY_URL` | same | required for `confluent` | Confluent registry |
| `GLUE_REGISTRY_NAME`, `AWS_REGION` | same | required for `glue` | Glue registry and region |
| `SCHEMA_AUTO_REGISTER` | same | `true` | `false` on AWS and in the end-to-end tests |
| `STREAMS_APPLICATION_ID` | stream-processor | `stream-processor` | Group id and internal topic prefix |
| `STREAMS_REPLICATION_FACTOR` | stream-processor | unset | `-1` on AWS (broker default) |
| `DOWNSTREAM_URL` | delivery-service | required | Base URL; paths `/accounts`, `/orders` |
| `RETRY_DELAY_1M`, `RETRY_DELAY_10M` | delivery-service | `PT1M`, `PT10M` | ISO-8601; only changes `not-before` |
| `HTTP_TIMEOUT`, `HTTP_CONNECT_TIMEOUT` | delivery-service | `PT5S`, `PT2S` | Downstream HTTP |
| `INGRESS_PORT`, `INGRESS_API_KEY` | ingress `LocalServer` | `8080`, unset | Local only; the API key mimics API Gateway |
| `INGRESS_HMAC_SECRET` / `INGRESS_HMAC_SECRET_ARN` | ingress | unset | Signature check; the ARN (Secrets Manager) wins |
| `INGRESS_MAX_BODY_BYTES` | ingress | `262144` | Body limit |
| `DB_HOST`, `DB_NAME`, `DB_SECRET_ARN` | DbInitHandler | required | RDS init |
| `POSTGRES_HOST_PORT`, `INGRESS_HOST_PORT` | docker compose | `5433`, `8088` | Host ports |
| `ENV_NAME`, `AWS_REGION`, `CLUSTER_TYPE`, `ALERT_EMAIL`, `TOPIC_PARTITIONS` | deploy scripts | `dev`, `sa-east-1`, `provisioned`, - , - | See [deploy runbook](runbooks/deploy.md) |

Module READMEs have the details: [ingress-lambda](../ingress-lambda/README.md), [stream-processor](../stream-processor/README.md), [delivery-service](../delivery-service/README.md), [replay-cli](../replay-cli/README.md), [infra-bootstrap](../infra-bootstrap/README.md), [infra/connect](../infra/connect/README.md).

## 8. Known limitations

- **Glue path verified only on AWS, and AWS has not run yet.** Unit tests cover Glue configuration; serialization against the real Glue registry, the registration script and the MSK Connect Glue converter have never executed.
- **MSK deployment is pending the Paid plan.** None of the AWS stacks beyond `artifacts` and `network` has been created. The IAM auth path, Lambda in a VPC reaching MSK, MSK Connect and the dashboards are unverified. Pending: AWS run (needs Paid plan).
- **Local Kafka is a single broker** (RF 1, no failure testing possible). Local retry delays are shortened.
- **Demo-grade AWS settings:** 2 brokers with `min.insync.replicas=1` and `acks=all` (a write acknowledged by one replica can be lost if that broker fails; production: 3 brokers, RF 3, min ISR 2); one NAT gateway (single AZ dependency); single-AZ RDS, no final snapshot; deletion protection off.
- **Connector password visible:** it is resolved from Secrets Manager at deploy time into the connector configuration, so anyone who can describe the connector sees it. Production would use the MSK config provider.
- **Broad OIDC deploy role:** `github-oidc.yaml` grants service-wide permissions (for example `ec2:*`, `kafka:*`, `rds:*`) to deploy and tear down everything. It is trusted only for the `aws` GitHub environment on `main`.
- **No operator access path for replay-cli on AWS:** MSK is in private subnets and the stacks define no operator host or IAM role for it. Running replay-cli there needs a host in the VPC with a role permitted on `integration.dlq` and the source topics. Pending: AWS run.
- **Monitoring gaps:** no alarm on `delivery-service-retry` lag, connector state, replication slot lag or RDS storage.
- **Retry window is fixed:** two hops, about 11 minutes with default delays. Changing it is a code and topic change ([ADR 0003](adr/0003-retry-topics.md)).
- **Ordering after retries is not preserved**; the downstream must tolerate it.

## 9. Glossary

- **CDC**: change data capture; here Debezium reading row changes of `orders` from PostgreSQL.
- **Canonical topic**: `integration.account.v1` and `integration.order.v1`, the normalized Avro output.
- **DLQ**: dead-letter queue, `integration.dlq`; holds original bytes plus error headers.
- **Retry ladder**: `integration.retry.1m`, then `integration.retry.10m`, then the DLQ; three delivery attempts in total.
- **`not-before`**: header on retry records; the retry consumer does not deliver before this instant.
- **`eventId` / `Idempotency-Key`**: identifier carried in the Avro payload and sent to the downstream as the `Idempotency-Key` header.
- **`exactly_once_v2`**: Kafka Streams processing guarantee; transactional consume-process-produce inside Kafka only.
- **Replication slot**: PostgreSQL's marker of how far the CDC reader has consumed; WAL is kept until it advances.
- **Glue Schema Registry**: AWS-managed Avro schema registry; its wire format is not the Confluent one.
- **BACKWARD compatibility**: consumers using the new schema can read data written with the previous schema.
- **Salesforce-style change events**: JSON events with a `ChangeEventHeader` (`entityName`, `changeType`, `recordIds`, `commitTimestamp`) posted to the ingress.

Contacts and on-call ownership are not recorded in this repository. Do not add personal names or account identifiers to this file.
