#!/usr/bin/env bash
# Register Avro schemas in Schema Registry (idempotent). No jq/python needed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=wait-for.sh
. "$ROOT/scripts/wait-for.sh"

SR="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
CT='Content-Type: application/vnd.schemaregistry.v1+json'

wait_for_url "$SR/subjects" 120

# Turn a .avsc file into {"schema":"<escaped, single-line>"}
schema_payload() {
  local esc
  esc=$(tr -d '\r' < "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e 's/\t/ /g' | tr '\n' ' ')
  printf '{"schema":"%s"}' "$esc"
}

register_dir() {
  local dir="$1" subject="$2" f resp
  echo "Subject $subject"
  curl -sS -f -X PUT -H "$CT" --data '{"compatibility":"BACKWARD"}' "$SR/config/$subject" >/dev/null
  # glob expansion is sorted lexically: v1 before v2
  for f in "$dir"/*.avsc; do
    [ -e "$f" ] || { echo "  no schemas in $dir" >&2; return 1; }
    resp=$(curl -sS -X POST -H "$CT" --data "$(schema_payload "$f")" \
      "$SR/subjects/$subject/versions" -w '\n%{http_code}')
    case "${resp##*$'\n'}" in
      200) echo "  $(basename "$f") -> ${resp%$'\n'*}" ;;
      *)   echo "  FAILED $(basename "$f"): $resp" >&2; return 1 ;;
    esac
  done
}

register_dir "$ROOT/schemas/account" "integration.account.v1-value"
register_dir "$ROOT/schemas/order"   "integration.order.v1-value"

echo "Registered subjects:"
curl -sS "$SR/subjects"
echo
