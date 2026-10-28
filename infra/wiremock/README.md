# WireMock downstream stub

Stands in for the downstream HTTP API in the local demo. Mappings live in `mappings/downstream.json`.

| Request                                   | Response |
|-------------------------------------------|----------|
| `POST /downstream/accounts`, `/orders`    | 200      |
| same, body has `accountId` `001xx000000FAIL1AAA`   | 503 on the first call, 200 afterwards (WireMock scenario `Started` -> `recovered`) |
| same, body has `accountId` `001xx000000REJECTAAA`  | 400 always (goes straight to the DLQ) |
| `POST /downstream` (legacy)               | 200      |

Two magic account ids drive the demo:

- `001xx000000FAIL1AAA` shows the retry ladder: the first delivery fails, the retry succeeds.
- `001xx000000REJECTAAA` shows a permanent failure and the DLQ.

The scenario state is global per WireMock process: the "fail once" happens once per account endpoint until
WireMock is restarted (or `POST /__admin/scenarios/reset` is called).

## Image for AWS

`Dockerfile` bakes `mappings/` into `wiremock/wiremock:3.10.0`; the ECS delivery task runs it as a sidecar
(`DOWNSTREAM_URL=http://localhost:8080/downstream`). The build context is the **repo root** so the path to the
mappings is the same as in the repo:

    docker build -f infra/wiremock/Dockerfile -t downstream-mock .
