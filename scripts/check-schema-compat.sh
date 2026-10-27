#!/usr/bin/env bash
# Check that the latest schema files are compatible with the latest registered version of their
# subject. Needs a running registry (SCHEMA_REGISTRY_URL, default http://localhost:8081).
# Exits non-zero if any schema is incompatible. Subjects not registered yet (404) are skipped.
# No jq/python needed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SR="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
CT='Content-Type: application/vnd.schemaregistry.v1+json'

# Turn a .avsc file into {"schema":"<escaped, single-line>"}
schema_payload() {
  local esc
  esc=$(tr -d '\r' < "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e 's/\t/ /g' | tr '\n' ' ')
  printf '{"schema":"%s"}' "$esc"
}

failed=0

check_dir() {
  local dir="$1" subject="$2" latest resp code body
  # lexical order: the last file is the latest version
  latest=$(printf '%s\n' "$dir"/*.avsc | sort | tail -n 1)
  resp=$(curl -sS -X POST -H "$CT" --data "$(schema_payload "$latest")" \
    "$SR/compatibility/subjects/$subject/versions/latest" -w '\n%{http_code}')
  code="${resp##*$'\n'}"
  body="${resp%$'\n'*}"
  case "$code" in
    200)
      if printf '%s' "$body" | grep -q '"is_compatible":true'; then
        echo "OK           $subject  $(basename "$latest")"
      else
        echo "INCOMPATIBLE $subject  $(basename "$latest"): $body" >&2
        failed=1
      fi ;;
    404) echo "SKIP         $subject: not registered yet (404)" ;;
    *)   echo "ERROR        $subject: HTTP $code $body" >&2; failed=1 ;;
  esac
}

check_dir "$ROOT/schemas/account" "integration.account.v1-value"
check_dir "$ROOT/schemas/order"   "integration.order.v1-value"

exit "$failed"
