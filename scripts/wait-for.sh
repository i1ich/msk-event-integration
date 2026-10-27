#!/usr/bin/env bash
# Helper: source this file, or run:  bash scripts/wait-for.sh <url> [timeout_seconds]
set -euo pipefail

# wait_for_url <url> [timeout_seconds=180] -> returns 0 once URL answers HTTP 200
wait_for_url() {
  local url="$1" timeout="${2:-180}" waited=0 code
  printf 'Waiting for %s ' "$url"
  while true; do
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$url" 2>/dev/null || true)
    if [ "$code" = "200" ]; then
      echo " OK"
      return 0
    fi
    if [ "$waited" -ge "$timeout" ]; then
      echo " TIMEOUT after ${timeout}s (last HTTP code: ${code:-none})" >&2
      return 1
    fi
    printf '.'
    sleep 3
    waited=$((waited + 3))
  done
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  [ $# -ge 1 ] || { echo "usage: $0 <url> [timeout_seconds]" >&2; exit 2; }
  wait_for_url "$@"
fi
