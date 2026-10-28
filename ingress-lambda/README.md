# ingress-lambda

HTTP entry point for Salesforce change events: `POST /events` (API Gateway + Lambda) validates the event and
publishes it to the Kafka topic `sf.change-events.raw`.

## How it works

```
client -> API Gateway -> IngressHandler (Lambda) -> EventIngestService -> Kafka (MSK)
                          LocalServer (local run) ->        ^
```

`EventIngestService` holds all the logic; `IngressHandler` (Lambda) and `LocalServer` (local JDK HTTP server)
are thin adapters around it. Steps, in order; the first failure stops the request and nothing is published:

1. Body larger than `INGRESS_MAX_BODY_BYTES` (default 262144) -> **413**.
2. If `INGRESS_HMAC_SECRET` is set: header `X-Signature: sha256=<hex HMAC-SHA256 of the raw body>` is required
   (header name case-insensitive, constant-time compare) -> otherwise **401**.
3. Body must be JSON -> otherwise **400** `{"error":"malformed_json"}`.
4. `ChangeEventHeader` must have `entityName` (string), `changeType` (CREATE/UPDATE/DELETE/UNDELETE),
   `recordIds` (non-empty array of strings), `commitTimestamp` (number). All problems are reported at once:
   **400** `{"error":"validation_failed","details":[...]}`.
5. Publish with key = first `recordIds` entry, value = the raw body, headers `event-id`, `received-at`,
   `source=salesforce` and `transaction-key` (if present). `event-id` is `ChangeEventHeader.eventId` or a new UUID.
6. Wait for the broker ack -> **202** `{"eventId","topic","partition","offset"}`.
   Broker error or timeout -> **503** `{"error":"broker_unavailable"}`.

Producer settings: `enable.idempotence=true`, `acks=all`, `delivery.timeout.ms=10000`,
`request.timeout.ms=5000`, `max.block.ms=5000`, `linger.ms=0`. Connection settings come from
`KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_SECURITY` (`plaintext` or `iam`), see `common`.

Logs are JSON (one object per line) and carry `eventId`. The HMAC secret is never logged.
On AWS the secret is read from Secrets Manager (`INGRESS_HMAC_SECRET_ARN`) once per cold start, in the handler's static
holder, not per request. The secret string is the raw secret (the CloudFormation template creates it with
`GenerateSecretString` without a JSON template); it is not parsed as JSON. If both `INGRESS_HMAC_SECRET_ARN` and
`INGRESS_HMAC_SECRET` are set, the ARN wins; the log says which source was used, never the value. The AWS call uses the
URL-connection HTTP client to keep the jar small. The Lambda needs `secretsmanager:GetSecretValue` on that secret and a
route to Secrets Manager (NAT).

## What can go wrong

- **Cold start.** The Kafka producer is created once per Lambda container (lazy static holder) and reused by
  later invocations. The first request after a cold start pays for JVM start, producer creation and the
  connection to the brokers (can take seconds). Never create a producer per request.
- **Network.** To reach MSK the Lambda must run in the VPC (subnets and security groups allowing port 9092/9098 to the
  brokers). A VPC Lambda also needs a route for anything else it calls. If MSK is unreachable, requests fail
  with 503 after the producer timeouts.
- **Idempotent producer.** It only removes duplicates caused by the producer's own internal retries within one
  producer session. It does not protect against the same HTTP request being sent twice.
- **503 means "retry".** The event was not confirmed as stored. The client should retry with backoff.
- **Duplicates after timeout.** If the broker stored the event but the response was lost (client or API Gateway
  timeout), the client retries and the event is published twice. Downstream consumers must therefore be
  idempotent, using the `event-id` header (send a stable `ChangeEventHeader.eventId` so retries share the id).
- **Ordering.** Key = first record id, so events for the same record keep their order within a partition.

## Run locally

Needs a Kafka broker (see the repo's docker setup) and Java 21.

```bash
mvn -pl ingress-lambda -am package

KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
INGRESS_PORT=8080 \
INGRESS_API_KEY=dev-key \
INGRESS_HMAC_SECRET=dev-secret \
java -jar ingress-lambda/target/ingress-lambda.jar
```

| Variable | Default | Meaning |
|----------|---------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | required | Kafka brokers |
| `KAFKA_SECURITY` | `plaintext` | `plaintext` or `iam` |
| `INGRESS_PORT` | `8080` | local server port |
| `INGRESS_API_KEY` | unset | if set, local server requires header `x-api-key` (mimics API Gateway API key) |
| `INGRESS_HMAC_SECRET` | unset | if set, `X-Signature` is required (local use) |
| `INGRESS_HMAC_SECRET_ARN` | unset | Secrets Manager secret holding the raw HMAC secret; wins over `INGRESS_HMAC_SECRET` |
| `INGRESS_MAX_BODY_BYTES` | `262144` | body size limit |

Try it (leave out `x-api-key` / `X-Signature` if you did not set those variables):

```bash
BODY=samples/account-create.json
SIG=$(openssl dgst -sha256 -hmac 'dev-secret' -hex < $BODY | sed 's/^.* //')

curl -i -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -H 'x-api-key: dev-key' \
  -H "X-Signature: sha256=$SIG" \
  --data-binary @$BODY

curl http://localhost:8080/health
```

Lambda handler class for deployment: `io.github.i1ich.mskei.ingress.IngressHandler::handleRequest`
(same jar, `target/ingress-lambda.jar`).
