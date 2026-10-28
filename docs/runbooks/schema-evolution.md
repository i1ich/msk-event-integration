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
3. **Register** the new version under the subject. Local: `bash scripts/register-schemas.sh` (idempotent, registers every `.avsc` of a directory in file-name order). AWS: `bash scripts/register-schemas-glue.sh <env>` (see "AWS (Glue)"). The services run with `SCHEMA_AUTO_REGISTER=false`, so nothing is registered implicitly.
4. **Deploy consumers** (stream-processor, delivery-service) that can read the new version.
5. **Deploy producers** (topology output, CDC source changes) that write the new field.
6. **Backfill** existing records if downstream needs the field for old data. Backfill is optional for nullable fields.

Never skip step 4. A producer that writes the new field before consumers can read it breaks them under `BACKWARD`.

## Why an added required field breaks BACKWARD

In Avro terms a BACKWARD check makes the **new** schema the reader and the **previous** one the writer. A reader field that the writer never wrote needs a default, otherwise the reader has no value to fill in. So adding a field without a default is the canonical BACKWARD break, and it is what `schemas/test-fixtures/AccountEvent.incompatible.avsc` does (v2 plus a required `region` string with no default).

"Removing a required field without a default" is the same rule seen from the other side: it breaks old readers of new data (FORWARD direction), because the old reader still expects the field. In Avro, removing a field is itself BACKWARD compatible: the new reader just skips the data. Do not rely on it, since old consumers still running during a rollout would fail.

## Test compatibility

Three layers, from fastest to most realistic:

1. **Unit test, no registry** (Avro `SchemaCompatibility`): v2 reads v1 data, v1 reads v2 data (the extra `industry` field is ignored), and the fixture is reported incompatible.
   ```bash
   mvn -B verify -pl common -am
   # or only this test: mvn -B test -pl common -Dtest=SchemaEvolutionTest
   ```
2. **Registry test** (Testcontainers: Kafka + Schema Registry): sets BACKWARD, registers v1, accepts v2, answers `is_compatible: false` for the fixture and rejects its registration with HTTP 409. Needs Docker.
   ```bash
   mvn -B verify -Pit -pl integration-tests -am -Dit.test=SchemaRegistryCompatibilityIT -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false -Djacoco.skip=true
   ```
   CI runs this as the `schema-compat` job.
3. **Script against a running registry**: checks the latest `.avsc` in `schemas/account` and `schemas/order` against the latest registered version of the subject. Exit code is non-zero if any schema is incompatible. Subjects not registered yet (404) are skipped with a message.
   ```bash
   SCHEMA_REGISTRY_URL=http://localhost:8081 scripts/check-schema-compat.sh
   ```

Manual check of a candidate schema (the script does the escaping for you; this is the raw call):

```bash
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data @payload.json http://localhost:8081/compatibility/subjects/integration.account.v1-value/versions/latest
```

`payload.json` contains the candidate schema as an escaped string: `{"schema": "<schema JSON as a string>"}`. A response of `{"is_compatible": true}` means the change passes.


## AWS (Glue)

On AWS the registry is AWS Glue Schema Registry `<env>-msk-ei` (ADR 0002), compatibility `BACKWARD`, schema name = topic name (`integration.account.v1`, `integration.order.v1`). The `.avsc` files in `schemas/` stay the source of truth.

- Run the compatibility test layers 1 and 2 above before touching AWS; they use the same Avro rules.
- Register: `bash scripts/register-schemas-glue.sh <env>` (needs the AWS CLI, credentials, and `stream-processor/target/stream-processor.jar`, built by `mvn -B -q package -DskipTests`). For each schema it creates the schema from the first `.avsc` if missing, then registers every file in order with `aws glue register-schema-version`. Glue checks `BACKWARD` compatibility at registration: an incompatible version ends in status `FAILURE` and the script stops with an error. That is the AWS-side compatibility check; a failed version is not used.
- The `GeneratedSchemas` note: the Avro-generated classes add `"avro.java.string":"String"` to the schema text, and the Glue serializer finds a schema only by its exact text (there is no "use latest version"; the services run with auto-registration off). So after the `.avsc` files, the script registers one more version: the text printed by `io.github.i1ich.mskei.common.GeneratedSchemas <topic>` from the jar. It is the same canonical schema as the latest `.avsc` (checked by `GeneratedSchemasTest`). The latest Glue version is therefore the generated text, not the file text. Regenerate the jar before registering a new version, otherwise the services cannot find their schema.
- `erp.public.orders` is not registered by the script; the Debezium converter registers its own envelope schema in Glue (the MSK Connect role may create schemas named `erp.public.orders*` only).
- Glue-encoded records cannot be read with the Confluent console tools or Kafka UI.

Not yet exercised on AWS (deploy pending Paid plan).

## Breaking change

If a change is not `BACKWARD`-compatible (for example, removing a field, or changing a type):
- Do not change the existing subject. Create a new versioned topic and schema (for example `integration.account.v2`): add the topic to `scripts/topics.txt` (read by docker compose and by the `topics` custom resource) and register its schema.
- Run both topics in parallel during migration, and move consumers over before retiring the old topic.

## Rollback

- Registering a new version does not delete old ones. Roll back consumers and producers to the previous schema version.
- Records already written with the new version remain readable by consumers that support it.
