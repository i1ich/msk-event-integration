#!/usr/bin/env bash
# End-to-end smoke test against a deployed env (contract section "Smoke test").
# Usage: scripts/smoke-test.sh [env]
# Optional overrides: DELIVERY_LOG_GROUP, DB_INIT_FUNCTION, HMAC_SECRET_ARN.
# 1. POST a signed account event to ingress, expect 202 + eventId
# 2. delivery-service log shows "Delivered" for that eventId within 120 s (ingress -> MSK -> streams -> delivery)
# 3. CDC: invoke the DbInit Lambda {"action":"smoke-order"}, expect 3 deliveries to /downstream/orders within 180 s
set -euo pipefail

ENV_NAME="${1:-${ENV_NAME:-dev}}"
# shellcheck source=lib/aws-common.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib/aws-common.sh"
validate_env_name
require_cmd aws curl openssl

SAMPLE="samples/account-create-with-industry.json"
FAILED=0
pass() { printf '  PASS  %s\n' "$*"; }
fail() { printf '  FAIL  %s\n' "$*"; FAILED=1; }
finish() {
  echo
  if [ "$FAILED" -eq 0 ]; then echo "SMOKE TEST: PASS"; exit 0; fi
  echo "SMOKE TEST: FAIL"
  exit 1
}

# poll <timeout s> <command...> : succeeds as soon as the command succeeds.
poll() {
  local timeout="$1" end
  shift
  end=$((SECONDS + timeout))
  while [ "$SECONDS" -lt "$end" ]; do
    if "$@"; then return 0; fi
    sleep 5
  done
  return 1
}

log "Smoke test for env '$ENV_NAME' in $AWS_REGION"
print_identity

# ---- Discover what the contract does not export ----------------------------------------------------------------
INGRESS_STACK="$(stack_name ingress)"
API_URL="$(stack_output "$INGRESS_STACK" ApiUrl)"
API_KEY_ID="$(stack_output "$INGRESS_STACK" ApiKeyId)"
[ -n "$API_URL" ] && [ "$API_URL" != "None" ] || { fail "ApiUrl output not found on $INGRESS_STACK"; finish; }

DELIVERY_LOG_GROUP="${DELIVERY_LOG_GROUP:-$(stack_resources "$(stack_name processing)" AWS::Logs::LogGroup | grep -i 'delivery' | head -n 1 || true)}"
[ -n "$DELIVERY_LOG_GROUP" ] || { fail "delivery-service log group not found in $(stack_name processing)"; finish; }
HMAC_SECRET_ARN="${HMAC_SECRET_ARN:-$(stack_resources "$INGRESS_STACK" AWS::SecretsManager::Secret | head -n 1 || true)}"
DB_INIT_FUNCTION="${DB_INIT_FUNCTION:-$(stack_output "$(stack_name database)" DbInitFunctionName)}"
info "api: $API_URL"
info "delivery log group: $DELIVERY_LOG_GROUP"

# ---- 1. signed POST --------------------------------------------------------------------------------------------
log "1/3 POST signed event to ingress"
API_KEY="$(aws apigateway get-api-key --api-key "$API_KEY_ID" --include-value --query value --output text)"
SECRET="$(aws secretsmanager get-secret-value --secret-id "$HMAC_SECRET_ARN" --query SecretString --output text)"
case "$SECRET" in
  \{*) SECRET="$(printf '%s' "$SECRET" | sed -n 's/.*"\(secret\|password\|hmac\)" *: *"\([^"]*\)".*/\2/p' | head -n 1)" ;;
esac
[ -n "$SECRET" ] || { fail "could not read the HMAC secret"; finish; }

SIG="sha256=$(openssl dgst -sha256 -hmac "$SECRET" -hex < "$SAMPLE" | sed 's/^.* //')"
START_MS=$(( ($(date +%s) - 30) * 1000 ))
mkdir -p target
HTTP_CODE="$(curl -sS -o target/smoke-response.json -w '%{http_code}' -X POST "${API_URL%/}/events" \
  -H 'Content-Type: application/json' -H "x-api-key: $API_KEY" -H "X-Signature: $SIG" \
  --data-binary "@$SAMPLE")" || HTTP_CODE="000"
EVENT_ID="$(sed -n 's/.*"eventId" *: *"\([^"]*\)".*/\1/p' target/smoke-response.json | head -n 1)"
if [ "$HTTP_CODE" = "202" ] && [ -n "$EVENT_ID" ]; then
  pass "ingress accepted the event (HTTP 202, eventId=$EVENT_ID)"
else
  fail "ingress returned HTTP $HTTP_CODE, body: $(head -c 300 target/smoke-response.json)"
  finish
fi

# ---- 2. delivery log for the eventId ---------------------------------------------------------------------------
log "2/3 wait for delivery of $EVENT_ID (up to 120 s)"
delivered_event() {
  local n
  n="$(aws logs filter-log-events --log-group-name "$DELIVERY_LOG_GROUP" --start-time "$START_MS" \
    --filter-pattern "\"Delivered\" \"$EVENT_ID\"" --query 'length(events)' --output text 2>/dev/null || echo 0)"
  [ "$n" != "None" ] && [ "${n:-0}" -ge 1 ]
}
if poll 120 delivered_event; then
  pass "delivery-service logged Delivered for $EVENT_ID (ingress -> MSK -> stream-processor -> delivery)"
else
  fail "no 'Delivered' log line for $EVENT_ID within 120 s in $DELIVERY_LOG_GROUP"
fi

# ---- 3. CDC path -----------------------------------------------------------------------------------------------
log "3/3 CDC: insert/update/delete one order via $DB_INIT_FUNCTION (up to 180 s)"
ORDERS_START_MS=$(( $(date +%s) * 1000 - 5000 ))
if aws lambda invoke --function-name "$DB_INIT_FUNCTION" --cli-binary-format raw-in-base64-out \
     --payload '{"action":"smoke-order"}' target/smoke-invoke.json --query FunctionError --output text \
     | grep -qv '^None$'; then
  fail "DbInit Lambda returned an error: $(head -c 300 target/smoke-invoke.json)"
else
  delivered_orders() {
    local n
    n="$(aws logs filter-log-events --log-group-name "$DELIVERY_LOG_GROUP" --start-time "$ORDERS_START_MS" \
      --filter-pattern '"Delivered" "/orders"' --query 'length(events)' --output text 2>/dev/null || echo 0)"
    [ "$n" != "None" ] && [ "${n:-0}" -ge 3 ]
  }
  if poll 180 delivered_orders; then
    pass "3 deliveries to /downstream/orders (create, update, delete) via Debezium"
  else
    fail "fewer than 3 deliveries to /downstream/orders within 180 s (check the connector state and the stream-processor logs)"
  fi
fi

finish
