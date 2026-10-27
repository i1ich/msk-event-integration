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
| `account-unknown-entity.json` | `Opportunity` event. Valid for the ingress, but the stream processor only knows `Account`, so it ends in the DLQ | 202 |
| `invalid-missing-header.json` | No `ChangeEventHeader` | 400 `validation_failed` |
| `invalid-bad-changetype.json` | `changeType` is `EXPLODE` (must be CREATE, UPDATE, DELETE or UNDELETE) | 400 `validation_failed` |
