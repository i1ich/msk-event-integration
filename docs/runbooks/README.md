# Runbooks

| Runbook | Use it when |
|---|---|
| [dlq-replay.md](dlq-replay.md) | `integration.dlq` holds records whose cause is fixed (downstream outage over, mapping bug patched) and they must be re-driven with `replay-cli`. |
| [connector-restart-and-slot-lag.md](connector-restart-and-slot-lag.md) | No new records on `erp.public.orders`, the Debezium connector is `FAILED` or `PAUSED`, or database disk grows because the replication slot retains WAL. |
| [schema-evolution.md](schema-evolution.md) | You change an Avro schema in `schemas/` (add a field, new version) and need the safe rollout order and compatibility checks, locally or in Glue. |
| [deploy.md](deploy.md) | Deploying the AWS stacks (`scripts/deploy.sh`). Costs money while it runs. |
| [teardown.md](teardown.md) | Deleting everything after a session (`scripts/teardown.sh`, `scripts/check-leftovers.sh`). Always run it the same day. |

Related: [architecture](../architecture.md), [knowledge transfer](../knowledge-transfer.md), [cost guardrails](../cost.md), [ADR index](../adr/README.md).

AWS variants of the three operational runbooks are written from the AWS documentation and the templates; they are not yet exercised on AWS (deploy pending Paid plan).
