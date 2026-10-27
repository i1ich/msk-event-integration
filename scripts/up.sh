#!/usr/bin/env bash
# Start the whole local stack, then register connector and schemas.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=wait-for.sh
. "$ROOT/scripts/wait-for.sh"

docker compose up -d --build

wait_for_url "http://localhost:8081/subjects" 180
wait_for_url "http://localhost:8083/connectors" 240
wait_for_url "http://localhost:${INGRESS_HOST_PORT:-8088}/health" 180

bash "$ROOT/scripts/register-connector.sh"
bash "$ROOT/scripts/register-schemas.sh"

echo
echo "Stack is up: ingress :${INGRESS_HOST_PORT:-8088}, kafka :9092, schema-registry :8081, connect :8083, postgres :${POSTGRES_HOST_PORT:-5433}, wiremock :8089"
echo "Optional UI: docker compose --profile ui up -d kafka-ui  (http://localhost:8090)"
