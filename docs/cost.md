# Cost

Status: sa-east-1 (São Paulo) on-demand prices read on 2026-10-27 from the **AWS Price List bulk API**
(`https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/<Service>/current/sa-east-1/index.json`,
publication date 2026-10-14). The pricing web pages render regional tables with JavaScript, so the bulk files are
the readable source. Re-check before each deploy; actual cost is recorded after the run.

## Guardrails (mandatory)

1. **Create the AWS Budget first**, before any other AWS resource: `msk-ei-budget` (`infra/cloudformation/budget.yaml`), monthly limit 15 USD (email at 80% actual and 100% forecast) plus `msk-ei-daily-cost`, 3 USD per day (email at 100% actual). Created 2026-10-28 with `scripts/create-budget.sh <email> 15 3`.
2. **Deploy, demo, and tear down on the same day.** Do not leave MSK, RDS, NAT, or ECS running overnight.
3. **Run the teardown checklist** in [runbooks/teardown.md](runbooks/teardown.md) after every session, and record that it was completed. Check for leftovers (NAT gateways, Elastic IPs, RDS instances, MSK clusters, ECS services, ECR images, CloudWatch log groups) before closing the session.
4. **Record actual cost** in the table below once Cost Explorer data is available (usually after a day or more).

## Unit prices, sa-east-1 (on-demand, USD)

| Item | Price | Price List service |
|---|---|---|
| MSK broker `kafka.t3.small` | 0.0736 per broker-hour | AmazonMSK |
| MSK broker storage | 0.19 per GB-month | AmazonMSK |
| MSK Connect | 0.175 per MCU-hour | AmazonMSK |
| MSK Serverless | not available in sa-east-1 (ADR 0006) | – |
| RDS PostgreSQL `db.t4g.micro` Single-AZ | 0.034 per hour | AmazonRDS |
| RDS gp3 storage | 0.219 per GB-month | AmazonRDS |
| NAT gateway | 0.093 per hour + 0.093 per GB processed | AmazonEC2 |
| Public IPv4 address (the NAT's Elastic IP) | 0.005 per hour | AmazonVPC |
| Interface VPC endpoint (for comparison, ADR 0005) | 0.021 per endpoint per AZ-hour | AmazonVPC |
| Fargate Linux/x86 | 0.0696 per vCPU-hour, 0.0076 per GB-hour | AmazonECS |
| Lambda | 0.0000166667 per GB-second (free tier applies) | AWSLambda |
| API Gateway REST | 4.25 per million requests | AmazonApiGateway |
| Secrets Manager | 0.40 per secret-month | AWSSecretsManager |
| CloudWatch Logs | 0.90 per GB ingested, 0.0408 per GB-month stored | AmazonCloudWatch |
| CloudWatch alarms | 0.10 per alarm-month (standard) | AmazonCloudWatch |
| ECR storage | 0.10 per GB-month | AmazonECR |
| Glue Schema Registry | free (no Schema Registry line in the AWSGlue price list; the Glue pricing page lists it as no additional charge) | AWSGlue |

## Estimate: running cost per hour (contract sizes)

| Component | Calculation | USD per hour |
|---|---|---|
| MSK, 2 × `kafka.t3.small` | 2 × 0.0736 | 0.147 |
| MSK storage, 2 × 10 GB | 20 × 0.19 / 730 | 0.005 |
| MSK Connect, 1 MCU | 1 × 0.175 | 0.175 |
| RDS `db.t4g.micro` + 20 GB gp3 | 0.034 + 20 × 0.219 / 730 | 0.040 |
| NAT gateway + its public IPv4 | 0.093 + 0.005 | 0.098 |
| Fargate, 2 tasks × (0.5 vCPU, 1 GB) | 2 × (0.5 × 0.0696 + 1 × 0.0076) | 0.085 |
| Secrets (3) and alarms (~6), prorated | (3 × 0.40 + 6 × 0.10) / 730 | 0.002 |
| Lambda, API Gateway, logs, ECR, S3 | demo traffic only | < 0.01 |
| **Total** | | **≈ 0.55** |

- **One demo run** (deploy ~1 h + demo ~1 h + teardown ~0.5–1 h, everything billed while it exists): ≈ 3 h × 0.55 ≈ **1.7 USD**.
- **If teardown is forgotten:** ≈ 13 USD per day, ≈ 400 USD per month. The daily budget (3 USD) alerts after about 5–6 hours; the monthly 15 USD is used up in about **27 hours**.
  This is why the budget comes first and `scripts/check-leftovers.sh` runs after every teardown.
- Biggest items: MSK Connect (32%), MSK brokers (27%), NAT (18%). Cheapest way to shorten a run: create the
  connector last and delete it first.
- NAT vs endpoints (ADR 0005): 5 interface endpoints × 2 AZs × 0.021 = 0.21 per hour (0.105 in one AZ) versus 0.098 for one NAT.

## Actual cost of the M4 run (filled after the run from Cost Explorer)

| Item | Hours or units used | Actual cost (USD) | Notes |
|---|---|---|---|
| MSK (provisioned, 2 brokers) | pending AWS run | pending AWS run | |
| MSK Connect (1 MCU) | pending AWS run | pending AWS run | |
| RDS `db.t4g.micro` + gp3 | pending AWS run | pending AWS run | |
| NAT gateway + data processed | pending AWS run | pending AWS run | |
| Public IPv4 | pending AWS run | pending AWS run | |
| Fargate (2 tasks) | pending AWS run | pending AWS run | |
| Lambda, API Gateway, Secrets Manager | pending AWS run | pending AWS run | |
| CloudWatch, ECR, S3, data transfer | pending AWS run | pending AWS run | |
| **Total** | | pending AWS run | |

## Session log

| Date (UTC) | Config | Deployed | Torn down | Teardown checklist done | Total cost | Notes |
|---|---|---|---|---|---|---|
| 2026-10-28 | dev, sa-east-1, provisioned | artifacts, network only (02:03–02:13) | yes, 02:16 | yes (`check-leftovers.sh`; NAT `deleted`, no EIPs, no endpoints) | ≈ 0.02 USD (NAT ~10 min), covered by free-plan credits | Stopped at `msk`: `SubscriptionRequiredException` — the account is on the AWS **Free plan**, which does not include Amazon MSK. Budgets `msk-ei-monthly-cost` (15 USD) and `msk-ei-daily-cost` (3 USD) created before the run. |

## Cost by option

Pending (needs a region with MSK Serverless): compare the MSK provisioned cluster with MSK Serverless, measured on the same workload. MSK Serverless is not available in sa-east-1, so this comparison can only be made in a supported region with measured values.
