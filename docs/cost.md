# Cost

Status: prices checked 2026-10-27 against the AWS pricing pages (`aws.amazon.com/<service>/pricing/`). Most sa-east-1 (São Paulo) unit prices are **not readable** from those pages, because their regional tables are rendered by JavaScript. Those rows say so explicitly. Nothing here is a sa-east-1 price unless its source column says the page showed it. Verify every "not readable" item in the AWS Pricing Calculator (region sa-east-1) before the M4 deploy. Actual cost is recorded after the run.

## Guardrails (mandatory)

1. **Create the AWS Budget first**, before any other AWS resource: `msk-ei-budget` (`infra/cloudformation/budget.yaml`), monthly limit $20, email at 80% actual and 100% forecast (per `CONTRACT.md`). `TODO (M4)`: record the date created.
2. **Deploy, demo, and tear down on the same day.** Do not leave MSK, RDS, NAT, or ECS running overnight.
3. **Run the teardown checklist** in [runbooks/teardown.md](runbooks/teardown.md) after every session, and record that it was completed. Check for leftovers (NAT gateways, Elastic IPs, RDS instances, MSK clusters, ECS services, ECR images, CloudWatch log groups) before closing the session.
4. **Record actual cost** in the table below once Cost Explorer data is available (usually after a day or more).

## Unit prices, sa-east-1

| Item | sa-east-1 unit price | Source (checked 2026-10-27) | Reference, labelled (not sa-east-1) |
|---|---|---|---|
| MSK provisioned `kafka.t3.small` broker-hour | **not readable**, verify in calculator | https://aws.amazon.com/msk/pricing/ | None for t3.small on the page. kafka.m5.large $0.21/h appears in a us-east-1 example |
| MSK provisioned storage GB-month | **not readable**, verify in calculator | https://aws.amazon.com/msk/pricing/ | $0.10/GB-month, US East example text |
| MSK Serverless | **not offered in sa-east-1** | https://docs.aws.amazon.com/msk/latest/developerguide/serverless.html | Available in 14 other regions, none in South America |
| MSK Connect MCU-hour | **not readable**, verify in calculator | https://aws.amazon.com/msk/pricing/ | $0.11/MCU-hour, N. Virginia example |
| RDS PostgreSQL `db.t4g.micro` single-AZ instance-hour | **not readable**, verify in calculator | https://aws.amazon.com/rds/postgresql/pricing/ | None shown |
| RDS gp3 storage GB-month | **not readable**, verify in calculator | https://aws.amazon.com/rds/postgresql/pricing/ | None shown |
| NAT gateway hour | **not readable**, verify in calculator | https://aws.amazon.com/vpc/pricing/ | $0.045/h, US East (Ohio) example |
| NAT gateway per GB processed | **not readable**, verify in calculator | https://aws.amazon.com/vpc/pricing/ | $0.045/GB, US East (Ohio) example |
| Public IPv4 address hour (in use or idle) | $0.005/h, page states the rate applies across all commercial regions | https://aws.amazon.com/vpc/pricing/ | Same value; São Paulo not named on the page, confirm in calculator |
| Interface VPC endpoint per AZ-hour and per GB | **not on the fetched page** | https://aws.amazon.com/vpc/pricing/ | Not used in the M4 design (ADR 0005) |
| S3 and DynamoDB gateway endpoints | **free**: "no data processing or hourly charges" for gateway endpoints | https://aws.amazon.com/vpc/pricing/ | Region not stated on the page |
| Fargate vCPU-hour (Linux/x86, on-demand) | **not readable**, verify in calculator | https://aws.amazon.com/fargate/pricing/ | $0.000011244/vCPU-s, N. Virginia example, about $0.0405/vCPU-h (derived, x3600) |
| Fargate GB-hour (Linux/x86, on-demand) | **not readable**, verify in calculator | https://aws.amazon.com/fargate/pricing/ | $0.000001235/GB-s, N. Virginia example, about $0.00445/GB-h (derived, x3600) |
| Lambda requests | **not readable**, verify in calculator | https://aws.amazon.com/lambda/pricing/ | $0.20 per million, N. Virginia example |
| Lambda GB-second (x86) | **not readable**, verify in calculator | https://aws.amazon.com/lambda/pricing/ | $0.0000166667/GB-s, N. Virginia example |
| Lambda free tier | 1 million requests/month and 400,000 GB-s/month (not region-specific on the page) | https://aws.amazon.com/lambda/pricing/ | n/a |
| API Gateway REST, per million calls (first tier) | **not readable**, verify in calculator | https://aws.amazon.com/api-gateway/pricing/ | $3.50/M, example regions listed on the page do not include sa-east-1 |
| Secrets Manager per secret-month | **not readable**, verify in calculator | https://aws.amazon.com/secrets-manager/pricing/ | $0.40/secret-month and $0.05 per 10,000 API calls in an unnamed-region example |
| Glue Schema Registry | **free**: "Usage of the AWS Glue Schema Registry is offered at no additional charge." No per-unit charge listed | https://aws.amazon.com/glue/pricing/ | Page does not separately address schema storage; confirm in the calculator |
| CloudWatch Logs ingestion per GB | **not readable**, verify in calculator | https://aws.amazon.com/cloudwatch/pricing/ | $0.50/GB, US East example |
| CloudWatch Logs storage per GB-month | **not readable**, verify in calculator | https://aws.amazon.com/cloudwatch/pricing/ | $0.03/GB archival, region not labelled |
| CloudWatch dashboard per month | **not readable** (dollar amount not in text) | https://aws.amazon.com/cloudwatch/pricing/ | Free tier: 3 dashboards |
| CloudWatch standard metric alarm per month | **not readable**, verify in calculator | https://aws.amazon.com/cloudwatch/pricing/ | $0.10/alarm metric, US East example |
| CloudWatch free tier | 5 GB logs, 10 custom metrics, 3 dashboards, 10 alarm metrics per month | https://aws.amazon.com/cloudwatch/pricing/ | n/a |
| ECR private storage GB-month | **not readable**, verify in calculator | https://aws.amazon.com/ecr/pricing/ | $0.10/GB-month, illustrative example, region not labelled |
| S3 Standard storage GB-month | **not readable**, verify in calculator | https://aws.amazon.com/s3/pricing/ | None shown. Expected negligible at demo size |
| AWS Budgets | **not verified**: the pricing URL returned HTTP 404 | https://aws.amazon.com/budgets/pricing/ | Check the Budgets page in the console before relying on a free budget count |

Note on RDS: the T4g CPU-credit rate of $0.075/vCPU-hour on the RDS page applies to unlimited-mode burst credits, not the instance-hour, and is not included in the estimate below.

## Estimate: one demo run (about 2.5–3 hours of everything running)

Quantities come from `CONTRACT.md`: 2 MSK brokers (`kafka.t3.small`, 10 GiB each), 1 MSK Connect worker at 1 MCU, 1 RDS `db.t4g.micro` single-AZ with 20 GiB gp3, 1 NAT gateway with its public IPv4, 2 Fargate tasks at 0.5 vCPU / 1 GB, 1 ingress Lambda, 2 Secrets Manager secrets, and small CloudWatch, ECR and S3 usage.

**The per-hour total cannot be computed yet.** The largest hourly items (MSK brokers, RDS, NAT, MSK Connect in sa-east-1) are not readable from the pages. The table lists the quantities and the one hourly rate that is known.

| Item | Quantity | sa-east-1 hourly | Known partial figure |
|---|---|---|---|
| MSK brokers `kafka.t3.small` | 2 | not readable | none |
| MSK storage 10 GiB | 2 | not readable | none |
| MSK Connect worker | 1 MCU | not readable | us-east-1 reference: $0.11/h |
| RDS `db.t4g.micro` | 1 | not readable | none |
| RDS gp3 20 GiB | 1 | not readable | none |
| NAT gateway | 1 | not readable | none |
| Public IPv4 (NAT) | 1 | $0.005/h (page: all commercial regions) | $0.005/h |
| Fargate vCPU | 2 × 0.5 | not readable | us-east-1 reference: about $0.04/vCPU-h |
| Fargate memory | 2 × 1 GB | not readable | us-east-1 reference: about $0.0045/GB-h |
| Lambda (ingress) | demo volume | not readable | free tier covers demo volume (1M requests, 400k GB-s per month) |
| Secrets Manager | 2 secrets | not readable | none |
| Glue Schema Registry | schemas | $0 | $0 |
| CloudWatch, ECR, S3 | small | not readable | expected small, free tiers cover much of it |

Per-run total (about 3 hours): `hourly total × 3`. `TODO (M4)`: fill in after the calculator check.

## If teardown is forgotten

The number that matters for the $20 budget is the hourly total with everything running:

- Per day: `24 × hourly total`
- Per month (730 h): `730 × hourly total`

Threshold: a hourly total above **$0.027/h** (= $20 / 730 h) exceeds the $20 monthly budget by itself if left running for a whole month. For reference, the always-on items (MSK brokers, RDS, NAT, MSK Connect) account for most of the hourly total. Fargate and Lambda are small by comparison.

`TODO (M4)`: fill in the per-day and per-month numbers once the sa-east-1 hourly total is known.

## Actual cost of the M4 run (filled after the run from Cost Explorer)

| Item | Hours or units used | Actual cost (USD) | Notes |
|---|---|---|---|
| MSK (provisioned, 2 brokers) | `TODO (M4)` | `TODO (M4)` | |
| MSK Connect (1 MCU) | `TODO (M4)` | `TODO (M4)` | |
| RDS `db.t4g.micro` + gp3 | `TODO (M4)` | `TODO (M4)` | |
| NAT gateway + data processed | `TODO (M4)` | `TODO (M4)` | |
| Public IPv4 | `TODO (M4)` | `TODO (M4)` | |
| Fargate (2 tasks) | `TODO (M4)` | `TODO (M4)` | |
| Lambda, API Gateway, Secrets Manager | `TODO (M4)` | `TODO (M4)` | |
| CloudWatch, ECR, S3, data transfer | `TODO (M4)` | `TODO (M4)` | |
| **Total** | | `TODO (M4)` | |

## Session log

| Date (UTC) | Config | Deployed | Torn down | Teardown checklist done | Total cost | Notes |
|---|---|---|---|---|---|---|
| `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` | `TODO (M4)` |

## Cost by option

`TODO (M4)`: compare the MSK provisioned cluster with MSK Serverless, measured on the same workload. MSK Serverless is not available in sa-east-1, so this comparison can only be made in a supported region with measured values.
