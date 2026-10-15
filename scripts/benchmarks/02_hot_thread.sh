#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

HOTTHREAD_BIN="${TMP_DIR}/hotthread"
(cd "${SCRIPT_DIR}/hotthread" && go build -o "${HOTTHREAD_BIN}" .)

run_benchmark() {
  local version="$1"
  log "=========================================================="
  log "Running Phase 2: Hot-Thread Lock Contention (${version})"
  log "=========================================================="

  local bin
  if [ "${version}" == "modern" ]; then
    bin=$(build_modern_binary)
  else
    bin=$(build_early_binary)
  fi

  stop_api
  reset_postgres
  reset_nats
  start_api "${bin}"

  # Give server 1s to stabilize
  sleep 1

  log "Launching hot-thread worker barrage (15s, 200 workers)..."
  local output
  output=$("${HOTTHREAD_BIN}" -url http://localhost:8080 -duration 15s -workers 200 2>&1)
  echo "${output}"

  local rps
  rps=$(echo "${output}" | grep "Achieved RPS:" | awk '{print $3}')
  local errors
  errors=$(echo "${output}" | grep "Errors:" | awk '{print $2}')
  local reqs
  reqs=$(echo "${output}" | grep "Total Requests:" | awk '{print $3}')

  log "Checking Postgres logs for deadlock / lock timeouts..."
  local deadlocks
  deadlocks=$(docker logs --since 30s kozlony-postgres 2>&1 | grep -i "deadlock" | wc -l || echo 0)
  log "Postgres deadlocks detected: ${deadlocks}"

  echo "${version},${reqs},${rps},${errors},${deadlocks}" >> "${TMP_DIR}/phase2_results.csv"
  stop_api
}

rm -f "${TMP_DIR}/phase2_results.csv"
run_benchmark "modern"
run_benchmark "early"

log "=========================================================="
log "PHASE 2 SUMMARY COMPARISON"
log "=========================================================="
cat "${TMP_DIR}/phase2_results.csv"
