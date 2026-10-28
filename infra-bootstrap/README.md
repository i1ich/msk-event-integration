# infra-bootstrap

Java 21 Lambdas used as CloudFormation custom resources, for what CloudFormation cannot do natively.

| Handler | Does | Env |
|---|---|---|
| `TopicsHandler` | Creates the Kafka topics of `scripts/topics.txt` (bundled in the jar). Properties: `Partitions` (override), `ReplicationFactor` (`-1` = broker default, needed for MSK Serverless). | `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SECURITY=iam` |
| `DbInitHandler` | Applies `infra/postgres/init/01-schema.sql` (bundled) to RDS. Direct invocation `{"action":"smoke-order"}` inserts, updates and deletes one order and returns its id (smoke test). | `DB_HOST`, `DB_NAME` (`erp`), `DB_SECRET_ARN` |

Handlers: `io.github.i1ich.mskei.bootstrap.TopicsHandler::handleRequest`, `...DbInitHandler::handleRequest`.
Build: `mvn -pl infra-bootstrap -am package` produces `infra-bootstrap/target/infra-bootstrap.jar`.

## Why custom resources

- MSK has no native topic resource (neither `AWS::MSK::Cluster` nor Serverless creates topics).
- RDS creates an empty database; the table, the publication Debezium needs and the seed rows must be loaded by something
  inside the VPC.

## How responses work

CloudFormation sends an event with a presigned S3 `ResponseURL`. The Lambda must HTTP PUT a JSON body
(`Status` SUCCESS/FAILED, `Reason`, `PhysicalResourceId`, `StackId`, `RequestId`, `LogicalResourceId`) with an
**empty Content-Type**. `CustomResource.handle` always does this: the work runs in a worker thread, exceptions become
FAILED, and if the work is still running 10 s before the Lambda deadline it answers FAILED.

## What can go wrong

- **No response = the stack hangs for an hour.** Any path that skips the PUT (crash, timeout, unreachable URL) causes it.
  Lambda timeout should be well above the work time (e.g. 120 s) so the guard can answer.
- **Networking.** The Lambdas run in the private subnets (to reach MSK and RDS). The response URL (S3) and Secrets Manager
  are then reached through the NAT gateway (the S3 gateway endpoint does not cover the presigned URL's public host
  reliably). Without NAT the PUT fails and the stack hangs.
- **Idempotency on Update.** Update re-runs the same code: existing topics are kept (never altered, e.g. a changed
  `Partitions` does not repartition), the schema script skips existing table/publication and does not re-seed.
- **Delete does nothing**, on purpose: topics and database die with their clusters.
- RDS requires TLS; the JDBC connection uses `sslmode=require` (encrypted, server certificate not verified).
