# Runbook: deploy to AWS

Deploy, demo and tear down the same day. The stack bills by the hour (MSK brokers, NAT gateway, RDS, Fargate, MSK Connect).
The binding contract (stack names, parameters, exports) is `infra/cloudformation/CONTRACT.md`.

## Prerequisites

- AWS CLI v2, Docker (daemon running), Java 21, Maven, Git, `openssl`, `curl`. Use Git Bash on Windows.
- Credentials for the target account (SSO or a role; no long-lived keys in the repo). Check with `aws sts get-caller-identity`.
- Region: `sa-east-1` by default (`export AWS_REGION=...` to change). MSK Serverless is not available in `sa-east-1`;
  the default `ClusterType` is `provisioned` (2 x `kafka.t3.small`). `deploy.sh` refuses `serverless` in unsupported regions.
- **Budget first**, once per account: `scripts/create-budget.sh you@example.com 20`. `deploy.sh` refuses to run without
  the `msk-ei-budget` stack. Confirm the email subscription.
- Optional, once: the GitHub OIDC stack (`github-oidc.yaml`) if you deploy from GitHub Actions; store the role ARN as repo
  variable `AWS_DEPLOY_ROLE_ARN` and create a GitHub environment named `aws`.
- Build passes locally: `mvn -B verify`.

## Deploy

```bash
scripts/deploy.sh dev                 # asks for confirmation after the cost warning
scripts/deploy.sh dev --yes           # no prompt (CI)
scripts/deploy.sh dev --skip-build    # reuse target/*.jar and the Connect plugin zip
# Environment: AWS_REGION, CLUSTER_TYPE (provisioned|serverless), ALERT_EMAIL (alarm email), TOPIC_PARTITIONS
```

What it does, in order (parameters are in one function per stack at the top of `scripts/deploy.sh`):

| Step | Stack / action | Typical duration |
|---|---|---|
| build | `mvn package`, `scripts/build-connect-plugin.sh` | 2-5 min |
| 1 | `artifacts` (bucket), then upload jars and plugin zip with git-sha keys | 1 min |
| 2 | `network` (VPC, one NAT gateway) | 2-4 min |
| 3 | `msk` (provisioned) | typically 20-30 min |
| - | read bootstrap servers (`aws kafka get-bootstrap-brokers`) | seconds |
| 4 | `topics` (custom resource Lambda) | 2-3 min |
| 5 | `schema-registry`, then `scripts/register-schemas-glue.sh` | 1 min |
| 6 | `database` (RDS PostgreSQL + schema init) | typically ~10 min |
| 7 | `ecr`, then docker build/push of 3 images (linux/amd64) | 5-10 min |
| 8 | `msk-connect` (plugin, connector) | typically ~10 min |
| 9-11 | `ingress`, `processing`, `observability` | 3-6 min each |
| end | `scripts/smoke-test.sh` | 1-5 min |

Durations are typical AWS figures; the first full M4 run records the actual numbers here. Elapsed time per stack is printed
by the script. Expect roughly 60-90 minutes end to end.

From GitHub Actions: workflow **Deploy to AWS** (manual), action `deploy`, uses OIDC and the `aws` environment.

## Verify

`scripts/smoke-test.sh dev` prints PASS/FAIL per check and exits non-zero on failure:

1. Signed `POST /events` returns 202 with an `eventId`.
2. The delivery-service log contains `Delivered` for that `eventId` within 120 s (ingress, MSK, stream-processor, delivery).
3. The DbInit Lambda `{"action":"smoke-order"}` produces three deliveries to `/downstream/orders` within 180 s (Debezium CDC).

The final summary of `deploy.sh` prints the API URL and the dashboard name (CloudWatch, Dashboards).

## What can go wrong

- **Stack ends in `ROLLBACK_COMPLETE` / `CREATE_FAILED`.** The script prints the failed events. Fix the cause, delete the failed
  stack (`aws cloudformation delete-stack --stack-name dev-msk-ei-<name>`; a stack in `ROLLBACK_COMPLETE` cannot be updated),
  and re-run `deploy.sh --skip-build`. Finished stacks are no-ops.
- **Custom resource stuck (topics, database init) for up to an hour.** The Lambda failed to send its response. Read its log
  group `/aws/lambda/dev-msk-ei-topics` or `-dbinit`. Typical causes: no route to AWS APIs (NAT), security group, wrong
  bootstrap servers. Wait for the timeout or cancel the update, then delete the stack and retry.
- **MSK Connect connector `FAILED` or stuck creating.** Open the connector log group in CloudWatch. Common causes: database not
  reachable from the workers (security group), missing Glue permissions, plugin zip problem. Delete `msk-connect` and
  redeploy; see also `connector-restart-and-slot-lag.md`.
- **ECS service never becomes stable.** Check the service events and task stopped reason: image pull errors (NAT, wrong tag),
  Kafka IAM authorization errors in the log group, out of memory. Redeploy `processing` after fixing.
- **`docker push` denied.** The ECR login lasts 12 hours; re-run `deploy.sh --skip-build`.
- **Budget stack missing.** Run `scripts/create-budget.sh`, or `--no-budget-check` for a throwaway experiment (not in CI).
- **Smoke test fails at step 2 or 3 only.** The infrastructure is up; look at the stream-processor and delivery-service logs
  and the DLQ before redeploying anything.

## Rollback

Prefer deleting and recreating a stack over manual changes. To go back to nothing, use
[teardown](teardown.md). To go back to a previous build, check out the commit and run `deploy.sh` again (artifacts and
images are keyed by git sha).
