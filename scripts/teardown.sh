#!/usr/bin/env bash
# Delete everything deploy.sh created, in reverse contract order, then run check-leftovers.sh.
# Usage: scripts/teardown.sh [env] [--yes] [--all]
#   --all  also delete the OIDC stack and the budget stack (otherwise both are kept; they are free)
set -euo pipefail

ENV_NAME="${ENV_NAME:-dev}"
ALL=false
POSITIONAL=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    --yes) ASSUME_YES=true ;;
    --all) ALL=true ;;
    -h|--help) sed -n '2,5p' "$0"; exit 0 ;;
    --*) echo "unknown option: $1" >&2; exit 2 ;;
    *) POSITIONAL+=("$1") ;;
  esac
  shift
done
[ "${#POSITIONAL[@]}" -gt 0 ] && ENV_NAME="${POSITIONAL[0]}"
export ENV_NAME ASSUME_YES

# shellcheck source=lib/aws-common.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib/aws-common.sh"
validate_env_name
require_cmd aws

START=$SECONDS
log "Teardown of env '$ENV_NAME'"
print_identity
confirm "Delete ALL stacks of env '$ENV_NAME' in $AWS_REGION$([ "$ALL" = true ] && echo ', plus the OIDC and budget stacks')?" || die "aborted by user"

# Stacks 11 .. 1 (reverse of deploy order). Stack 5 (schema-registry) gets its schemas deleted first.
STACKS=(processing ingress observability msk-connect ecr database schema-registry topics msk network artifacts)

# Delete every schema in the registry (the ones from register-schemas-glue.sh and the one Debezium auto-registered).
delete_glue_schemas() {
  local registry="${ENV_NAME}-msk-ei" name left
  if ! aws glue get-registry --registry-id "RegistryName=$registry" >/dev/null 2>&1; then
    info "Glue registry $registry does not exist, nothing to delete"
    return 0
  fi
  log "Deleting Glue schemas in registry $registry"
  for name in $(aws glue list-schemas --registry-id "RegistryName=$registry" --query 'Schemas[].SchemaName' --output text); do
    info "delete schema $name"
    aws glue delete-schema --schema-id "SchemaName=$name,RegistryName=$registry" >/dev/null
  done
  # Deletion is asynchronous; wait until the registry is empty so the registry stack can be deleted.
  for _ in $(seq 1 30); do
    left="$(aws glue list-schemas --registry-id "RegistryName=$registry" --query 'length(Schemas)' --output text)"
    [ "$left" = "0" ] && return 0
    sleep 5
  done
  warn "schemas still present in $registry after 150 s; stack deletion may fail (re-run teardown)"
}

# Empty the artifacts bucket (versioning is off, so removing the objects is enough).
empty_artifacts_bucket() {
  local bucket
  if ! stack_exists "$(stack_name artifacts)"; then return 0; fi
  bucket="$(export_value ArtifactsBucket)"
  if [ -z "$bucket" ] || [ "$bucket" = "None" ]; then
    warn "artifacts bucket export not found; skipping empty"
    return 0
  fi
  log "Emptying s3://$bucket"
  aws s3 rm "s3://$bucket" --recursive --only-show-errors
}

# Delete ECS services' tasks quickly: scale to zero first so the service delete does not wait for draining twice.
scale_down_ecs() {
  local cluster svc
  stack_exists "$(stack_name processing)" || return 0
  cluster="$(export_value EcsClusterName)"
  [ -n "$cluster" ] && [ "$cluster" != "None" ] || return 0
  for svc in $(aws ecs list-services --cluster "$cluster" --query 'serviceArns[]' --output text); do
    info "scale ${svc##*/} to 0"
    aws ecs update-service --cluster "$cluster" --service "$svc" --desired-count 0 >/dev/null || true
  done
}

scale_down_ecs
empty_artifacts_bucket

for s in "${STACKS[@]}"; do
  # schemas go right before their registry (Debezium may auto-register until the connector is gone)
  if [ "$s" = "schema-registry" ]; then delete_glue_schemas; fi
  delete_stack "$(stack_name "$s")"
done

if [ "$ALL" = "true" ]; then
  delete_stack "$OIDC_STACK"
  delete_stack "$BUDGET_STACK"
else
  info "kept $BUDGET_STACK and $OIDC_STACK (use --all to delete them)"
fi

log "Teardown finished in $(( (SECONDS - START) / 60 )) min $(( (SECONDS - START) % 60 )) s; checking for leftovers"
bash scripts/check-leftovers.sh "$ENV_NAME"
