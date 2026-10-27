# Runbook: connector restart and replication slot lag

Applies to the Debezium PostgreSQL connector `orders-source` (local Kafka Connect on `localhost:8083`; MSK Connect on AWS, `TODO (M4)`: confirm the endpoint).

## Symptoms

- No new records on `erp.public.orders` after changes to `orders`.
- Connector or task state is `FAILED` or `PAUSED` in the Connect REST API.
- Disk usage on the PostgreSQL instance grows without a matching increase in data.

## Why this matters

PostgreSQL keeps WAL until the replication slot's consumer confirms it. If the connector stops, the slot pins WAL, and the database's disk fills. This is the main operational risk of CDC.

## Check slot lag

Run on the PostgreSQL primary (`psql` or any SQL client):

```sql
SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS retained_wal
FROM pg_replication_slots;
```

- `active = false` with growing `retained_wal`: the connector is not consuming. Act now.
- `active = true` with growing lag: the connector runs but is behind. Check consumer throughput and Kafka availability.

Check connector status:

```bash
curl -s http://localhost:8083/connectors/orders-source/status
```

## Restart the connector

Restart the connector and all its tasks:

```bash
curl -X POST "http://localhost:8083/connectors/orders-source/restart?includeTasks=true"
```

Then confirm the status returns to `RUNNING` and that `retained_wal` begins to shrink.

## When to drop a slot

Drop the slot only if the connector cannot recover and the database disk is at risk. Dropping a slot releases the retained WAL but has consequences:

- Changes not yet read from the slot are lost from the CDC stream.
- The connector cannot resume from its offset. It must take a new snapshot of `orders`.
- Downstream consumers see a gap. Reconcile `orders` against `integration.order.v1` before declaring recovery.

Steps (only after approval):
1. Stop the connector.
2. Drop the slot: `SELECT pg_drop_replication_slot('<slot_name>');`
3. Delete the connector's stored offsets, so it snapshots again.
4. Start the connector and watch the snapshot complete.
5. Record the time window of the gap and tell the owners of downstream consumers.

## After recovery

- Confirm slot lag returns to near zero and `retained_wal` stays flat.
- Record the cause in the incident notes.
- `TODO (M4)`: add a CloudWatch alarm on slot lag and connector status.
