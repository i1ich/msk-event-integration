#!/usr/bin/env bash
# List anything still billable (or still present) for this env/region. Exit 1 if anything is found.
# Usage: scripts/check-leftovers.sh [env]
# Read-only: only describe/list calls.
set -uo pipefail   # no -e: one failing list call must not hide the others

ENV_NAME="${1:-${ENV_NAME:-dev}}"
# shellcheck source=lib/aws-common.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib/aws-common.sh"
validate_env_name
require_cmd aws

PREFIX="${ENV_NAME}-msk-ei"
FOUND=0

# check <label> <command...>: runs the command (text output) and reports non-empty output as a leftover.
check() {
  local label="$1" out
  shift
  if ! out="$("$@" 2>&1)"; then
    printf '  [??] %-34s query failed: %s\n' "$label" "$(printf '%s' "$out" | head -n 1)"
    FOUND=1   # unknown is treated as "not verified clean"
    return
  fi
  out="$(printf '%s\n' "$out" | sed '/^None$/d;/^$/d')"
  if [ -n "$out" ]; then
    printf '  [!!] %-34s\n' "$label"
    printf '%s\n' "$out" | sed 's/^/         /'
    FOUND=1
  else
    printf '  [ok] %-34s\n' "$label"
  fi
}

# Same, for a command whose output needs tabs turned into newlines.
check_lines() {
  local label="$1"
  shift
  check "$label" bash -c '"$@" | tr "\t" "\n"' _ "$@"
}

TAG_FILTERS=("Name=tag:Project,Values=$PROJECT_TAG" "Name=tag:Env,Values=$ENV_NAME")

log "Leftover check for env '$ENV_NAME' in $AWS_REGION"
print_identity

check "CloudFormation stacks" aws cloudformation list-stacks \
  --stack-status-filter CREATE_IN_PROGRESS CREATE_FAILED CREATE_COMPLETE ROLLBACK_IN_PROGRESS ROLLBACK_FAILED \
  ROLLBACK_COMPLETE DELETE_IN_PROGRESS DELETE_FAILED UPDATE_IN_PROGRESS UPDATE_COMPLETE_CLEANUP_IN_PROGRESS \
  UPDATE_COMPLETE UPDATE_FAILED UPDATE_ROLLBACK_IN_PROGRESS UPDATE_ROLLBACK_FAILED \
  UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS UPDATE_ROLLBACK_COMPLETE REVIEW_IN_PROGRESS \
  --query "StackSummaries[?starts_with(StackName, '${PREFIX}-')].[StackName,StackStatus]" --output text

check "MSK clusters" aws kafka list-clusters-v2 --cluster-name-filter "$PREFIX" \
  --query 'ClusterInfoList[].[ClusterName,State]' --output text
check "MSK Connect connectors" aws kafkaconnect list-connectors --connector-name-prefix "$PREFIX" \
  --query 'connectors[].[connectorName,connectorState]' --output text
check "MSK Connect custom plugins" aws kafkaconnect list-custom-plugins \
  --query "customPlugins[?contains(name, '${PREFIX}')].[name,customPluginState]" --output text
check "RDS instances" aws rds describe-db-instances \
  --query "DBInstances[?starts_with(DBInstanceIdentifier, '${PREFIX}')].[DBInstanceIdentifier,DBInstanceStatus]" --output text
check "NAT gateways (pending/available)" aws ec2 describe-nat-gateways \
  --filter "${TAG_FILTERS[@]}" "Name=state,Values=pending,available" \
  --query 'NatGateways[].[NatGatewayId,State]' --output text
check "Elastic IPs (unattached, tagged)" aws ec2 describe-addresses --filters "${TAG_FILTERS[@]}" \
  --query 'Addresses[?AssociationId==null].[AllocationId,PublicIp]' --output text
check "VPCs (tagged)" aws ec2 describe-vpcs --filters "${TAG_FILTERS[@]}" \
  --query 'Vpcs[].VpcId' --output text

# ECS: any cluster with the prefix is a leftover; list its services too.
ECS_CLUSTERS="$(aws ecs list-clusters --query "clusterArns[?contains(@, '${PREFIX}')]" --output text 2>/dev/null | tr '\t' '\n')"
check "ECS clusters" printf '%s\n' "$ECS_CLUSTERS"
for c in $ECS_CLUSTERS; do
  check "ECS services in ${c##*/}" aws ecs list-services --cluster "$c" --query 'serviceArns' --output text
done

check "Lambda functions" aws lambda list-functions \
  --query "Functions[?starts_with(FunctionName, '${PREFIX}')].FunctionName" --output text
check "S3 buckets" aws s3api list-buckets \
  --query "Buckets[?contains(Name, '${PREFIX}')].Name" --output text
check "ECR repositories" aws ecr describe-repositories \
  --query "repositories[?starts_with(repositoryName, '${PREFIX}')].repositoryName" --output text
check "CloudWatch log groups" aws logs describe-log-groups \
  --query "logGroups[?contains(logGroupName, '${PREFIX}')].logGroupName" --output text
check "Glue registries" aws glue list-registries \
  --query "Registries[?starts_with(RegistryName, '${PREFIX}')].RegistryName" --output text
check "Secrets (incl. pending deletion)" aws secretsmanager list-secrets --include-planned-deletion \
  --query "SecretList[?starts_with(Name, '${PREFIX}')].[Name,DeletedDate]" --output text

check_lines "Tagged resources (Project+Env)" aws resourcegroupstaggingapi get-resources \
  --tag-filters "Key=Project,Values=$PROJECT_TAG" "Key=Env,Values=$ENV_NAME" \
  --query 'ResourceTagMappingList[].ResourceARN' --output text

echo
if [ "$FOUND" -eq 0 ]; then
  echo "RESULT: clean. Nothing billable found for '$ENV_NAME' in $AWS_REGION."
  echo "Note: the tagging API can lag a few minutes behind deletions; secrets scheduled for deletion do not bill."
  exit 0
fi
echo "RESULT: leftovers found (or a query failed). Delete them (see docs/runbooks/teardown.md) and re-run."
exit 1
