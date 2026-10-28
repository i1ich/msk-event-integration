#!/usr/bin/env bash
# Deploy the account-level AWS Budget (stack msk-ei-budget). Run once, BEFORE the first deploy.
# Usage: scripts/create-budget.sh <alert-email> [monthly-limit-usd]   (default limit: 20)
set -euo pipefail

# shellcheck source=lib/aws-common.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib/aws-common.sh"

EMAIL="${1:-}"
LIMIT="${2:-20}"
[ -n "$EMAIL" ] || die "usage: scripts/create-budget.sh <alert-email> [monthly-limit-usd]"
[[ "$LIMIT" =~ ^[0-9]+$ ]] || die "limit must be a whole number of USD"

require_cmd aws
print_identity

deploy_stack budget.yaml "$BUDGET_STACK" "AlertEmail=$EMAIL" "MonthlyLimitUsd=$LIMIT"

log "Budget in place: ${LIMIT} USD/month, alerts to the given address (confirm the SNS/email subscription if AWS sends one)."
