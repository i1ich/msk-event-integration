# Runbook: schema evolution

Schemas live in `schemas/account/` and `schemas/order/`. Registry: Confluent Schema Registry locally, AWS Glue Schema Registry on AWS. Compatibility mode: `BACKWARD`. Subjects follow `TopicNameStrategy`, for example `integration.account.v1-value`.

## What BACKWARD means

`BACKWARD` means a schema can read data written with the previous version. In practice:
- A **new** consumer schema can read records written with the **old** producer schema.
- Adding a field is allowed only if it has a default (or is nullable with a default).
- Removing a field or changing its type is not allowed in place.

Because of this, consumers must be upgraded before producers start writing the new field.

## Safe sequence

1. **Change the schema** in `schemas/` as a new version. Add the new field as optional, with `"default": null` in a union with `null`. Example for `AccountEvent` v2 (`industry`):
   ```json
   {"name": "industry", "type": ["null", "string"], "default": null}
   ```
2. **Test compatibility** against the registry before registering (see below).
3. **Register** the new version under the subject.
4. **Deploy consumers** (stream-processor, delivery-service) that can read the new version.
5. **Deploy producers** (topology output, CDC source changes) that write the new field.
6. **Backfill** existing records if downstream needs the field for old data. Backfill is optional for nullable fields.

Never skip step 4. A producer that writes the new field before consumers can read it breaks them under `BACKWARD`.

## Test compatibility

Use the registry's compatibility endpoint against the latest version of a subject. Locally:

```bash
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @payload.json \
  http://localhost:8081/compatibility/subjects/integration.account.v1-value/versions/latest
```

`payload.json` contains the candidate schema as an escaped string: `{"schema": "<schema JSON as a string>"}`. A response of `{"is_compatible": true}` means the change passes. `TODO (M3)`: add this check to CI.

On AWS, Glue Schema Registry has its own compatibility check (console or API). `TODO (M4)`: document the exact command after the registry is provisioned.

## Breaking change

If a change is not `BACKWARD`-compatible (for example, removing a field, or changing a type):
- Do not change the existing subject. Create a new versioned topic and schema (for example `integration.account.v2`).
- Run both topics in parallel during migration, and move consumers over before retiring the old topic.

## Rollback

- Registering a new version does not delete old ones. Roll back consumers and producers to the previous schema version.
- Records already written with the new version remain readable by consumers that support it.
