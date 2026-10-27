# Runbook: teardown

Run teardown the same day as the deploy. Billable resources keep charging until deleted. Work through this checklist after deleting the stacks, and tick each item only after you confirm it in the console or CLI.

## Checklist of billable resources

- [ ] **MSK cluster** (serverless or provisioned) deleted.
- [ ] **MSK Connect connector** (Debezium `orders-source`) deleted, and its custom plugin and log delivery if they exist.
- [ ] **RDS PostgreSQL instance** (`db.t4g.micro`) deleted, with its snapshots if no longer needed. Check for final snapshot retention.
- [ ] **NAT gateway** deleted, and its **Elastic IP** released.
- [ ] **ECS services and tasks** (stream-processor, delivery-service) stopped; cluster and task definitions removed.
- [ ] **VPC endpoints** (interface endpoints) deleted.
- [ ] **Load balancers**, if any were created, deleted.
- [ ] **API Gateway** stage and API deleted, and ingress-lambda removed.
- [ ] **CloudWatch log groups** deleted (log storage keeps billing after the resources are gone).
- [ ] **CloudWatch alarms and dashboards** deleted.
- [ ] **S3 bucket** used for artifacts or logs emptied and deleted.
- [ ] **Glue Schema Registry** schemas and registry deleted.
- [ ] **Secrets Manager secrets** (for example the HMAC secret) deleted or scheduled for deletion.

## Verify

- [ ] Stacks deleted: `aws cloudformation list-stacks --stack-status-filter DELETE_COMPLETE` shows all project stacks, and no active stacks remain.
- [ ] Cost Explorer shows no new charges for the day after teardown. `TODO (M4)`: record the observed cost.
- [ ] Budget alert still in place.

## Notes

- A failed stack deletion leaves resources behind. Inspect `DELETE_FAILED` stacks and remove the blocking resources manually.
- Do not delete the budget alert during teardown.
