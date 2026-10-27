# integration-tests

End-to-end tests with Testcontainers. They run the real services against real infrastructure:

| Where | What |
|-------|------|
| Docker containers (one network) | Kafka (cp-kafka 7.9.0, KRaft), Schema Registry 7.9.0, PostgreSQL 16 (`wal_level=logical`), Kafka Connect + Debezium 2.5.4 (built from `infra/connect/Dockerfile`) |
| Inside the test JVM | `stream-processor` (`StreamProcessorApp.start`), `delivery-service` (`DeliveryServiceApp.start`), ingress (`EventIngestService` + a real producer), `replay-cli` (`ReplayCli` via picocli), WireMock as the downstream system |

The environment mirrors `docker-compose.yml`: the same topics (`scripts/topics.txt`), the same schemas (`schemas/`,
registered v1 then v2 for accounts, v1 for orders), the same connector config
(`infra/connect/orders-source.local.json`), the same WireMock mappings (`infra/wiremock`) and the same Postgres init
script (table, `REPLICA IDENTITY FULL`, publication). Retry delays are short (`PT2S` / `PT4S`) so the retry ladder
runs in seconds; the topics are still `retry.1m` / `retry.10m`.

It is started **once per test run** (singleton pattern, `support/ItEnvironment`) and shared by all test classes.
Tests use unique record ids, so they do not depend on each other or on their order, and they use Awaitility
instead of sleeps.

## What each test proves

| Test | Proves |
|------|--------|
| `IngressToDownstreamIT` | A valid Account event is accepted (202), lands in `sf.change-events.raw` with `event-id`, `source`, `received-at`, `transaction-key` headers and the original bytes; appears in `integration.account.v1` as `AccountEvent` (Avro, schema registry, specific reader) with all mapped fields incl. `industry` (schema v2); WireMock receives `POST /downstream/accounts` with `Idempotency-Key` = event id. |
| `CdcOrdersIT` | INSERT / UPDATE / DELETE of a row in PostgreSQL become CREATE / UPDATE / DELETE events on `integration.order.v1` for the same key (same partition, in order); DELETE carries the `before` values; the tombstone is skipped; the three calls reach `/downstream/orders`. |
| `PoisonMessagesIT` | An unknown entity (`Opportunity`) goes to `integration.dlq` with `error-class` `...UnmappedEntityException`, `source-topic`, `attempts=1` and the original bytes. Garbage bytes written to `erp.public.orders` go to the DLQ too, and the processor keeps working (a following valid event is processed). |
| `DeliveryRetryIT` | Account `001xx000000FAIL1AAA`: downstream answers 503 then 200, both calls carry the same `Idempotency-Key`, the record passed through `integration.retry.1m` (with `attempts` and `not-before`), nothing reaches the DLQ. Account `001xx000000REJECTAAA`: 400 goes straight to the DLQ (`...DownstreamException`), no retry record, exactly one call. |
| `ReplayIT` | `ReplayCli --dry-run` publishes nothing; the real run republishes exactly this test's DLQ record to `sf.change-events.raw` with `replayed-at` and without any error header; it is processed again (and fails again, being still an unknown entity). |
| `schema/*` | Schema Registry compatibility tests (separate package, not described here). |

## How to run

```bash
# from the repository root (needs Docker; unit tests run as well)
mvn -B verify -Pit

# only the end-to-end tests of this module, skipping the unit tests of the other modules
mvn -B verify -Pit -pl integration-tests -am -Dtest=NoSuch -Dsurefire.failIfNoSpecifiedTests=false
# one class
mvn -B verify -Pit -pl integration-tests -am -Dtest=NoSuch -Dsurefire.failIfNoSpecifiedTests=false -Dit.test=CdcOrdersIT
```

Plain `mvn -B verify` (without `-Pit`) does not build this module and does not need Docker.

## Expected duration

* First run: several minutes extra. Docker pulls the images and builds the Connect image (confluent-hub download).
* Later runs (images and the Connect image cached): about 2 to 3 minutes of start-up (Connect is the slow one: 60 to 120 s
  until `/connectors` answers), then the five test classes take about 30 seconds (the retry tests wait for real delays).

## What can go wrong

* **Docker is not running.** Testcontainers reports "Could not find a valid Docker environment". Start Docker Desktop.
* **Docker Engine 29+ and "Status 400" on `/info`.** The docker-java client in Testcontainers 1.20.4 speaks API 1.32
  by default, which new engines reject. `src/test/resources/docker-java.properties` pins `api.version=1.44`
  (needs Docker Engine 25+). On an older engine delete that file.
* **Kafka does not start with "advertised.listeners cannot use the nonroutable meta-address 0.0.0.0".**
  Testcontainers 1.20.4 plus cp-kafka 7.9 (Kafka 3.9) do not work together out of the box; `support/KafkaBroker`
  replaces the start script (controller listener on `localhost:9094`, extra in-network listener `kafka:19092`).
* **Slow Connect start / timeouts.** The wait strategy allows 6 minutes; Awaitility waits 90 s per assertion.
  On a busy laptop (the full docker-compose stack running next to the tests) raise them in `ItEnvironment`.
* **Ryuk / networking on Windows.** Ryuk (the container reaper) needs to reach the Docker socket. If it fails you can
  run locally with `TESTCONTAINERS_RYUK_DISABLED=true`; the environment then stops its containers in a shutdown hook,
  but a killed JVM leaves them behind (`docker ps`). Do not commit that setting.
* **Port clashes.** Everything uses random host ports, so the docker-compose stack can run at the same time.
* **Leftover state.** Containers are new for every run. The Kafka Streams state directory (`/tmp/kafka-streams`)
  holds no state for this topology.

## Schema registration: production setting

The services run with `SCHEMA_AUTO_REGISTER=false`, as in production: schemas are pre-registered from `schemas/`
(v1 then v2 for accounts, v1 for orders) and the services never register anything.

This needed a fix found by these tests. The classes generated from `schemas/*.avsc` (with `stringType=String`)
contain `"avro.java.string":"String"`, so a lookup by schema text did not match the registered `.avsc` and the
registry answered 40403 "Schema not found". `SchemaRegistryConfig` now sets `use.latest.version=true` when
auto-registration is off: the serializer writes with the latest registered version and still checks that it is
compatible with the record's own schema.
