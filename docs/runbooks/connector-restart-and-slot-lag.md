# Runbook: connector restart and replication slot lag

Applies to the Debezium PostgreSQL connector `orders-source`, replication slot `dbz_orders_slot`, publication `dbz_orders_pub` (see [infra/connect/README.md](../../infra/connect/README.md)).

- Local: Kafka Connect REST on `http://localhost:8083`, PostgreSQL in the `postgres` compose service.
- AWS: MSK Connect connector (defined in `infra/cloudformation/msk-connect.yaml`) and RDS PostgreSQL. Not yet exercised on AWS (deploy pending Paid plan).

## Symptoms

- No new records on `erp.public.orders` after changes to `orders`.
- Connector or task state is `FAILED` or `PAUSED`.
- Disk usage on the PostgreSQL instance grows without a matching increase in data.

## Why this matters

PostgreSQL keeps WAL until the replication slot's consumer confirms it. If the connector stops, the slot pins WAL, and the database's disk fills. This is the main operational risk of CDC.

## Check slot lag

Run on the PostgreSQL primary. Local:

```bash
docker compose exec -T postgres psql -U erp -d erp -c "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS retained_wal FROM pg_replication_slots;"
```

Expected on a healthy local stack: one row, `dbz_orders_slot`, `active = t`, a few kB retained.

- `active = f` with growing `retained_wal`: the connector is not consuming. Act now.
- `active = t` with growing lag: the connector runs but is behind. Check Kafka availability and connector logs (`docker compose logs connect`).

On AWS run the same SQL from a host inside the VPC (the database is private), with the user and password from the Secrets Manager secret (`DbSecretArn` output of the `database` stack). RDS needs `rds.logical_replication=1`, which the template sets.

## Check the connector

Local:

```bash
curl -s http://localhost:8083/connectors/orders-source/status
```

A healthy answer has `"state":"RUNNING"` for the connector and for task 0. A failed task carries a `trace` with the cause.

AWS: look at the connector state and its log group in the console (MSK > MSK Connect > Connectors), or `aws kafkaconnect describe-connector --connector-arn <arn>` (field `connectorState`). The Kafka Connect REST API is not exposed by MSK Connect.

## Restart the connector

Local, connector and all tasks:

```bash
curl -X POST "http://localhost:8083/connectors/orders-source/restart?includeTasks=true&onlyFailed=false"
```

The call answers `202`; the status shows `RESTARTING` for a few seconds, then `RUNNING` (verified locally). To restart only failed tasks, use `onlyFailed=true`. A single task: `curl -X POST http://localhost:8083/connectors/orders-source/tasks/0/restart`. Then re-check the status and that `retained_wal` begins to shrink.

AWS: there is no Kafka Connect REST access. Documented options, checked against the AWS documentation on 2026-10-27 (not run, no AWS commands were executed for this runbook):

- `RestartConnector` API (`aws kafkaconnect restart-connector --connector-arn <arn>`, optional "only failed tasks" flag; asynchronous, returns a `connectorOperationArn` to follow with `aws kafkaconnect describe-connector-operation`). The API reference documents it; the AWS pause/resume page says restart is not available for connectors created before the operation was supported. Confirm the exact flag with `aws kafkaconnect restart-connector help`.
- Pause and resume: `aws kafkaconnect pause-connector` / `resume-connector --connector-arn <arn>`. A paused connector still bills at full capacity.
- Configuration change: update the `msk-connect` stack (`scripts/deploy.sh` re-applies it); `UpdateConnector` cannot change `connector.class`.
- Last resort: delete the `dev-msk-ei-msk-connect` stack and recreate it. The replication slot and the stored connector offsets are not part of the stack; read the next section before doing this.

## When to drop a slot

Drop the slot only if the connector cannot recover and the database disk is at risk. Dropping a slot releases the retained WAL but has consequences:

- Changes not yet read from the slot are lost from the CDC stream.
- The connector cannot resume from its offset. It must take a new snapshot of `orders`.
- Downstream consumers see a gap. Reconcile `orders` against `integration.order.v1` before declaring recovery.

Steps (only after approval):
1. Stop the connector. Local: `curl -X PUT http://localhost:8083/connectors/orders-source/stop` (Kafka Connect 3.5+). AWS: delete the connector (or the `msk-connect` stack).
2. Drop the slot: `SELECT pg_drop_replication_slot('dbz_orders_slot');`
3. Make the connector snapshot again by deleting its stored offsets. Local (Kafka Connect 3.6+, connector must be stopped): `curl -X DELETE http://localhost:8083/connectors/orders-source/offsets`. AWS: a recreated connector has new offsets, so it snapshots (`snapshot.mode=initial`). The local offsets commands are not exercised by this repo's tests; check the response status before continuing.
4. Start the connector. Local: `curl -X PUT http://localhost:8083/connectors/orders-source/resume`, or `bash scripts/register-connector.sh` to re-apply the configuration. Watch the snapshot complete (`erp.public.orders` receives `op=r` rows, which the processor maps to `SNAPSHOT` order events). The publication `dbz_orders_pub` stays; the connector recreates the slot.
5. Record the time window of the gap and tell the owners of downstream consumers. Snapshot events have new event ids derived from partition and offset, so the downstream sees them as new events.

## After recovery

- Confirm slot lag returns to near zero and `retained_wal` stays flat.
- Record the cause in the incident notes.
- Pending: a CloudWatch alarm on slot lag and connector status is not defined yet.
