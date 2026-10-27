# Sample events

Fake Salesforce Change Data Capture events (all IDs are made up) for trying the ingress.
Send one with:

```bash
curl -i -X POST http://localhost:8088/events -H 'Content-Type: application/json' \
  --data-binary @samples/account-create.json
```

Use `--data-binary` (not `-d`) so the body is sent byte for byte; this matters when an HMAC signature is checked.

| File | What it is | Ingress result |
|------|------------|----------------|
| `account-create.json` | Account `CREATE` with name, phone, city | 202 |
| `account-update.json` | Account `UPDATE`, only changed fields present | 202 |
| `account-delete.json` | Account `DELETE`, header only | 202 |
| `account-create-with-industry.json` | Account `CREATE` with `Industry` (field added in `AccountEvent` v2) | 202 |
| `account-update-flaky-downstream.json` | Account id `001xx000000FAIL1AAA`: the WireMock downstream answers 503 once, then 200, so delivery goes through `integration.retry.1m` and succeeds | 202 |
| `account-update-rejected-downstream.json` | Account id `001xx000000REJECTAAA`: the downstream always answers 400, so delivery sends it straight to the DLQ | 202 |
| `account-unknown-entity.json` | `Opportunity` event. Valid for the ingress, but the stream processor only knows `Account`, so it ends in the DLQ | 202 |
| `invalid-missing-header.json` | No `ChangeEventHeader` | 400 `validation_failed` |
| `invalid-bad-changetype.json` | `changeType` is `EXPLODE` (must be CREATE, UPDATE, DELETE or UNDELETE) | 400 `validation_failed` |
