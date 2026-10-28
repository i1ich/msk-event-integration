# Runbook: teardown

Deploy, demo, tear down the same day. Billable resources keep charging until deleted.

## Command

```bash
scripts/teardown.sh dev            # asks for confirmation
scripts/teardown.sh dev --yes      # no prompt
scripts/teardown.sh dev --all      # also deletes the OIDC and budget stacks (normally kept; they are free)
```

What it does:

1. Scales the ECS services to 0, empties the artifacts bucket, deletes every schema in the Glue registry `dev-msk-ei`
   (right before its stack, so the Debezium converter cannot re-register).
2. Deletes the stacks in reverse order: `observability`, `processing`, `ingress`, `msk-connect`, `ecr`, `database`,
   `schema-registry`, `topics`, `msk`, `network`, `artifacts` (stacks that do not exist are skipped).
3. Runs `scripts/check-leftovers.sh dev` and exits non-zero if anything is still found.

From GitHub Actions: workflow **Deploy to AWS**, action `teardown` (keeps budget and OIDC; `--all` is local only because it
removes the role the workflow runs with).

Typical duration: MSK deletion 15-25 min, RDS 5-10 min, MSK Connect 5-10 min, the rest a few minutes each. The VPC stack can
take longer: Lambda functions in the VPC leave network interfaces behind and AWS releases them with a delay, up to roughly
20-40 min. The M4 run records the actual figures.

## Checklist of billable resources

`check-leftovers.sh` verifies each of these; tick them only after it reports clean.

- [ ] **MSK cluster** deleted (list-clusters-v2 with the name prefix).
- [ ] **MSK Connect connector** and **custom plugin** deleted.
- [ ] **RDS instance** deleted, no automated backups or snapshots left (template uses no final snapshot).
- [ ] **NAT gateway** deleted and its **Elastic IP** released (the Elastic IP bills when unattached too).
- [ ] **ECS services, cluster** deleted.
- [ ] **Lambda functions** (ingress, topics, dbinit) deleted; **API Gateway** deleted.
- [ ] **VPC** and its subnets, route tables, security groups deleted.
- [ ] **ECR repositories** deleted (images included).
- [ ] **S3 artifacts bucket** emptied and deleted.
- [ ] **CloudWatch log groups, alarms, dashboard** deleted (log groups are explicit and removed with their stacks).
- [ ] **Glue registry and schemas** deleted.
- [ ] **Secrets Manager secrets** (HMAC, database): deleted or scheduled for deletion (scheduled ones do not bill).
- [ ] **SNS topic** deleted.

Final check, any time:

```bash
scripts/check-leftovers.sh dev
```

It also queries the Resource Groups Tagging API for `Project=msk-event-integration, Env=dev` (the tagging index can lag a few
minutes after deletion). Also look at Cost Explorer the next day: no new charges should appear. The budget stays in place.

## What can go wrong

- **`DELETE_FAILED` on the `network` stack (subnet or security group in use).** Lambda network interfaces are still being
  released. Wait 20-40 min and re-run `scripts/teardown.sh dev`; if it persists, find the ENIs
  (`aws ec2 describe-network-interfaces --filters Name=vpc-id,Values=<vpc>`), wait until they are `available`, and delete them.
- **`DELETE_FAILED` on `artifacts`.** The bucket is not empty (an upload raced the teardown). Empty it again and re-run.
- **ECS service stuck draining.** The script scales to 0 first. If a stack still hangs, `aws ecs update-service --desired-count 0`
  and `aws ecs delete-service --force`, then re-run.
- **`DELETE_FAILED` on `schema-registry`.** Schemas still exist. Re-run (the script deletes them again) or delete them in the console.
- **Custom resource stuck on delete (topics, database).** Check its Lambda log group. If the Lambda cannot run, retry
  the delete with the resource retained (`aws cloudformation delete-stack --retain-resources <LogicalId>`), then remove the
  retained resource by hand.
- **Leftovers reported after a clean teardown.** Delete them by hand, then re-run `check-leftovers.sh` until it prints `clean`.
