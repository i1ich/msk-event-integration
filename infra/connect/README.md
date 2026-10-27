# Debezium PostgreSQL source connector

`orders-source.local.json` is applied by `scripts/register-connector.sh` (PUT, idempotent).

| Setting | Why |
|---|---|
| `plugin.name=pgoutput` | Built-in logical decoding plugin, no extension needed. |
| `publication.name=dbz_orders_pub`, `publication.autocreate.mode=disabled` | Publication is created by `infra/postgres/init/01-schema.sql`; the connector user needs no DDL rights. |
| `slot.name=dbz_orders_slot` | Fixed replication slot name so restarts resume from the same position. |
| `topic.prefix=erp` | Topic = `<prefix>.<schema>.<table>` = `erp.public.orders`. |
| `table.include.list=public.orders` | Capture only this table. |
| `decimal.handling.mode=string` | `numeric(12,2)` as an exact string, avoids lossy doubles and opaque bytes. |
| `time.precision.mode=connect` | Kafka Connect logical time types (millisecond precision). |
| `tombstones.on.delete=true` | Emit a tombstone after each delete event (log compaction). |
| `snapshot.mode=initial` | Snapshot existing rows once, then stream changes. |
| Avro converters | Key/value serialized as Avro, schemas stored in Schema Registry. |

`orders-source.msk-connect.json` is a template with `${...}` placeholders for MSK Connect; it is not used locally.
