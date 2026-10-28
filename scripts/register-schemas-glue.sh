#!/usr/bin/env bash
# Register the Avro schemas from schemas/ in the AWS Glue Schema Registry "<env>-msk-ei" (idempotent).
# Usage: scripts/register-schemas-glue.sh [env]
#   - schema missing  : aws glue create-schema with the first file (v1), BACKWARD, AVRO
#   - later files     : aws glue register-schema-version (an identical definition returns the existing version)
# Schema name = topic name (contract). erp.public.orders is NOT registered here: the Debezium converter does it.
set -euo pipefail

ENV_NAME="${1:-${ENV_NAME:-dev}}"
# shellcheck source=lib/aws-common.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib/aws-common.sh"
validate_env_name
require_cmd aws

REGISTRY="${ENV_NAME}-msk-ei"

# Wait until a schema version is AVAILABLE (registration can be PENDING for a moment).
wait_available() {
  local vid="$1" status=""
  for _ in $(seq 1 30); do
    status="$(aws glue get-schema-version --schema-version-id "$vid" --query Status --output text)"
    case "$status" in
      AVAILABLE) return 0 ;;
      FAILURE)   die "schema version $vid ended in FAILURE (incompatible with the previous version?)" ;;
    esac
    sleep 2
  done
  die "schema version $vid still $status after 60 s"
}

# register_schema <schema name> <dir with *.avsc>   (glob order is lexical: v1 before v2)
register_schema() {
  local name="$1" dir="$2" f created=false vid
  local files=("$dir"/*.avsc)
  [ -e "${files[0]}" ] || die "no .avsc files in $dir"
  log "Schema $name (registry $REGISTRY)"

  if aws glue get-schema --schema-id "SchemaName=$name,RegistryName=$REGISTRY" >/dev/null 2>&1; then
    info "schema exists"
  else
    info "creating from $(basename "${files[0]}")"
    vid="$(aws glue create-schema \
      --registry-id "RegistryName=$REGISTRY" \
      --schema-name "$name" \
      --data-format AVRO \
      --compatibility BACKWARD \
      --schema-definition "file://${files[0]}" \
      --tags "Project=$PROJECT_TAG,Env=$ENV_NAME" \
      --query SchemaVersionId --output text)"
    wait_available "$vid"
    created=true
  fi

  for f in "${files[@]}"; do
    if [ "$created" = "true" ] && [ "$f" = "${files[0]}" ]; then
      continue
    fi
    vid="$(aws glue register-schema-version \
      --schema-id "SchemaName=$name,RegistryName=$REGISTRY" \
      --schema-definition "file://$f" \
      --query SchemaVersionId --output text)"
    wait_available "$vid"
    info "$(basename "$f") -> version id $vid"
  done

  # The services run with auto-registration off, and the Glue serializer finds a schema only by its EXACT text.
  # The generated classes add "avro.java.string" to the latest schema, so register that text as the latest version
  # (same canonical schema as the last .avsc, proven by GeneratedSchemasTest; BACKWARD check still applies).
  local generated
  generated="$(mktemp)"
  java -cp "$PROCESSOR_JAR" io.github.i1ich.mskei.common.GeneratedSchemas "$name" > "$generated"
  vid="$(aws glue register-schema-version \
    --schema-id "SchemaName=$name,RegistryName=$REGISTRY" \
    --schema-definition "file://$generated" \
    --query SchemaVersionId --output text)"
  rm -f "$generated"
  wait_available "$vid"
  info "generated class schema -> version id $vid"
}

# Any fat jar that contains common + the generated classes (built by deploy.sh before this script runs).
PROCESSOR_JAR="${PROCESSOR_JAR:-stream-processor/target/stream-processor.jar}"
[ -f "$PROCESSOR_JAR" ] || die "missing $PROCESSOR_JAR (run: mvn -B -q package -DskipTests)"
require_cmd java

register_schema "integration.account.v1" "schemas/account"
register_schema "integration.order.v1"   "schemas/order"

log "Schemas in registry $REGISTRY:"
aws glue list-schemas --registry-id "RegistryName=$REGISTRY" --query 'Schemas[].SchemaName' --output text
