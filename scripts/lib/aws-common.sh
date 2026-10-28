#!/usr/bin/env bash
# Shared helpers for the AWS scripts (deploy, teardown, smoke test, leftovers check).
# Source it; do not execute it. The caller sets ENV_NAME (default dev) after sourcing, or before.
# shellcheck shell=bash

# Git Bash on Windows rewrites arguments that start with "/" (log group names, for example) before calling
# aws.exe. Turn that off; it has no effect on Linux/macOS. Because of it, always pass LOCAL paths to the aws
# CLI as relative paths (scripts cd to the repo root first).
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$COMMON_DIR/../.." && pwd)"
cd "$ROOT" || exit 1

export AWS_REGION="${AWS_REGION:-sa-east-1}"
export AWS_DEFAULT_REGION="$AWS_REGION"
export AWS_PAGER=""
ENV_NAME="${ENV_NAME:-dev}"
ASSUME_YES="${ASSUME_YES:-false}"
PROJECT_TAG="msk-event-integration"
# shellcheck disable=SC2034  # used by the scripts that source this file
BUDGET_STACK="msk-ei-budget"
# shellcheck disable=SC2034
OIDC_STACK="msk-ei-github-oidc"
CFN_DIR="infra/cloudformation"

log()  { printf '\n==> %s\n' "$*"; }
info() { printf '    %s\n' "$*"; }
warn() { printf 'WARNING: %s\n' "$*" >&2; }
die()  { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

require_cmd() {
  local c
  for c in "$@"; do
    command -v "$c" >/dev/null 2>&1 || die "required command not found: $c"
  done
}

# Validates ENV_NAME against the contract pattern [a-z][a-z0-9-]{1,15}.
validate_env_name() {
  [[ "$ENV_NAME" =~ ^[a-z][a-z0-9-]{1,15}$ ]] || die "invalid env name '$ENV_NAME' (pattern [a-z][a-z0-9-]{1,15})"
}

# stack_name <suffix>  ->  dev-msk-ei-<suffix>
stack_name() { printf '%s-msk-ei-%s' "$ENV_NAME" "$1"; }

stack_exists() { aws cloudformation describe-stacks --stack-name "$1" >/dev/null 2>&1; }

# stack_output <stack> <OutputKey>
stack_output() {
  aws cloudformation describe-stacks --stack-name "$1" \
    --query "Stacks[0].Outputs[?OutputKey=='$2'].OutputValue | [0]" --output text
}

# export_value <ExportName without env prefix>  ->  value of ${ENV_NAME}-msk-ei-<Name>
export_value() {
  aws cloudformation list-exports \
    --query "Exports[?Name=='${ENV_NAME}-msk-ei-$1'].Value | [0]" --output text
}

# Physical ids of a stack's resources of one type, one per line (used when the contract defines no output).
stack_resources() {
  aws cloudformation describe-stack-resources --stack-name "$1" \
    --query "StackResources[?ResourceType=='$2'].PhysicalResourceId" --output text | tr '\t' '\n'
}

# Short git sha; a dirty tree gets a unique suffix so a new artifact key / image tag forces a stack update.
git_sha() {
  local sha
  sha="$(git rev-parse --short=8 HEAD 2>/dev/null || echo nogit)"
  if [ -n "$(git status --porcelain 2>/dev/null)" ]; then
    sha="${sha}-dirty$(date +%s)"
  fi
  printf '%s' "$sha"
}

# confirm "<question>" : y/N prompt, skipped (accepted) when ASSUME_YES=true.
confirm() {
  if [ "$ASSUME_YES" = "true" ]; then
    info "(--yes) $1 -> yes"
    return 0
  fi
  local answer
  read -r -p "$1 [y/N] " answer || answer=""
  case "$answer" in y|Y|yes|YES) return 0 ;; *) return 1 ;; esac
}

# Prints the caller identity with the account id masked (last 4 digits only) and the region.
print_identity() {
  local acct
  acct="$(aws sts get-caller-identity --query Account --output text)" || die "aws sts get-caller-identity failed (no valid credentials?)"
  info "AWS account: ********${acct: -4}   region: $AWS_REGION   env: $ENV_NAME"
}

# deploy_stack <template file name in infra/cloudformation> <stack name> [Key=Value ...]
deploy_stack() {
  local template="$1" stack="$2"
  shift 2
  local start=$SECONDS
  log "Deploying $stack ($template)"
  local args=(cloudformation deploy
    --template-file "$CFN_DIR/$template"
    --stack-name "$stack"
    --no-fail-on-empty-changeset
    --capabilities CAPABILITY_NAMED_IAM
    --tags "Project=$PROJECT_TAG" "Env=$ENV_NAME")
  if [ "$#" -gt 0 ]; then
    args+=(--parameter-overrides "$@")
  fi
  if ! aws "${args[@]}"; then
    warn "stack $stack failed. Last failed events:"
    aws cloudformation describe-stack-events --stack-name "$stack" --max-items 40 \
      --query "StackEvents[?contains(ResourceStatus,'FAILED')].[LogicalResourceId,ResourceStatusReason]" \
      --output text >&2 || true
    die "deploy of $stack failed (see docs/runbooks/deploy.md, section 'What can go wrong')"
  fi
  info "$stack done in $((SECONDS - start))s"
}

# delete_stack <stack>: no-op when the stack does not exist.
delete_stack() {
  local stack="$1" start=$SECONDS
  if ! stack_exists "$stack"; then
    info "skip $stack (does not exist)"
    return 0
  fi
  log "Deleting $stack"
  aws cloudformation delete-stack --stack-name "$stack"
  if ! aws cloudformation wait stack-delete-complete --stack-name "$stack"; then
    aws cloudformation describe-stack-events --stack-name "$stack" --max-items 40 \
      --query "StackEvents[?ResourceStatus=='DELETE_FAILED'].[LogicalResourceId,ResourceStatusReason]" \
      --output text >&2 || true
    die "deleting $stack failed (see docs/runbooks/teardown.md, section 'What can go wrong')"
  fi
  info "$stack deleted in $((SECONDS - start))s"
}
