# Cost

Status: placeholders. No prices are recorded yet. All figures are filled in during M4 from the AWS pricing pages and the actual bill. Do not add estimated numbers without a source and a date.

## Guardrails (mandatory)

1. **Create a budget alert first**, before any other AWS resource. Suggested starting threshold: `$20` per month, with notifications at 50%, 80%, and 100%. `TODO (M4)`: record the budget name and date created.
2. **Deploy, demo, and tear down on the same day.** Do not leave MSK, RDS, NAT, or ECS running overnight.
3. **Run the teardown checklist** in [runbooks/teardown.md](runbooks/teardown.md) after every session, and record that it was completed.
4. **Record actual cost** in the table below once the Cost Explorer data is available (usually after a day or more).

## Billable components

| Component | Configuration | Unit price (source, date) | Hours or units used | Estimated cost | Actual cost | Notes |
|---|---|---|---|---|---|---|
| MSK | Serverless or provisioned 2 x `kafka.t3.small` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | Provisioned bills per broker-hour and storage. |
| RDS PostgreSQL | `db.t4g.micro` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | Plus storage and backup retention. |
| MSK Connect | Debezium connector | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | Billed per MCU-hour. |
| NAT gateway | 1 gateway | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | Hourly charge plus data processing. |
| Elastic IP | Attached to NAT | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | Charged when not attached in some cases. |
| VPC interface endpoints | Count `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | Per endpoint per AZ-hour plus data. |
| ECS Fargate | stream-processor, delivery-service | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | vCPU and memory per second. |
| API Gateway | REST, `POST /events` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | Per request. |
| Lambda | ingress-lambda | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | Requests and duration. |
| CloudWatch | Logs, metrics, alarms | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | Log retention drives cost. |
| S3 | Artifacts | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | Small. Empty at teardown. |
| Glue Schema Registry | Schemas | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | Check the current pricing page. |
| Data transfer | Cross-AZ and outbound | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | Easy to overlook. |

## Session log

| Date (UTC) | Config | Deployed | Torn down | Teardown checklist done | Total cost | Notes |
|---|---|---|---|---|---|---|
| `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` |

## Cost by option

`TODO (M4)`: compare serverless vs provisioned MSK, measured on the same workload. Use measured values only.
