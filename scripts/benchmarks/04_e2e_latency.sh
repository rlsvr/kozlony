#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

E2E_BIN="${TMP_DIR}/e2elatency"
(cd "${SCRIPT_DIR}/e2elatency" && go build -o "${E2E_BIN}" .)
stress_bin=$(build_stress_binary)

run_probe() {
  local version="$1"
  local bg_clients="$2"

  log "----------------------------------------------------------"
  log "Measuring E2E Latency: ${version} (Background SSE: ${bg_clients})"
  log "----------------------------------------------------------"

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

  local stress_pid=""
  if [ "${bg_clients}" -gt 0 ]; then
    log "Starting ${bg_clients} background SSE clients..."
    "${stress_bin}" -url http://localhost:8080 -rps 100 -workers 30 -with-clients "${bg_clients}" -duration 20s >/dev/null 2>&1 &
    stress_pid=$!
    sleep 4
  fi

  local output
  output=$("${E2E_BIN}" -url http://localhost:8080 -probes 60 -interval 100ms 2>&1)
  echo "${output}"

  local p50
  p50=$(echo "${output}" | grep "P50:" | awk '{print $2}')
  local p90
  p90=$(echo "${output}" | grep "P90:" | awk '{print $2}')
  local p99
  p99=$(echo "${output}" | grep "P99:" | awk '{print $2}')

  if [ -n "${stress_pid}" ]; then
    kill "${stress_pid}" >/dev/null 2>&1 || true
    wait "${stress_pid}" 2>/dev/null || true
  fi
  stop_api

  echo "${version},${bg_clients},${p50:-N/A},${p90:-N/A},${p99:-N/A}" >> "${TMP_DIR}/phase4_results.csv"
}

rm -f "${TMP_DIR}/phase4_results.csv"

# Idle baseline
run_probe "modern" 0
run_probe "early" 0

# Under 2,000 background clients load
run_probe "modern" 2000
run_probe "early" 2000

log "=========================================================="
log "PHASE 4 SUMMARY COMPARISON (P50, P90, P99 in ms)"
log "=========================================================="
cat "${TMP_DIR}/phase4_results.csv"
