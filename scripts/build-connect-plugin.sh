#!/usr/bin/env bash
# Builds the MSK Connect custom plugin zip (Debezium PostgreSQL + AWS Glue Schema Registry converter)
# from pinned Maven coordinates in infra/connect/plugin/pom.xml. No ad-hoc downloads.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mvn -B -q -f "$ROOT/infra/connect/plugin/pom.xml" clean package

ZIP="$ROOT/infra/connect/plugin/target/debezium-postgres-glue.zip"
echo "Plugin zip: $ZIP"
echo "Size: $(du -h "$ZIP" | cut -f1) ($(wc -c < "$ZIP") bytes)"
