#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

STORM_BIN="${TMP_DIR}/reconnectstorm"
(cd "${SCRIPT_DIR}/reconnectstorm" && go build -o "${STORM_BIN}" .)

run_benchmark() {
  local version="$1"
  local count="$2"

  log "=========================================================="
  log "Running Phase 5: Reconnect Storm (${version}, ${count} clients)"
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

  sleep 1

  local output
  output=$("${STORM_BIN}" -url http://localhost:8080 -clients "${count}" -timeout 20s 2>&1)
  echo "${output}"

  local established
  established=$(echo "${output}" | grep "Established:" | awk '{print $2}')
  local failed
  failed=$(echo "${output}" | grep "Failed:" | awk '{print $2}')
  local elapsed
  elapsed=$(echo "${output}" | grep "Elapsed:" | awk '{print $2}')
  local rate
  rate=$(echo "${output}" | grep "Handshake Rate:" | awk '{print $3}')

  stop_api
  echo "${version},${count},${established},${failed},${elapsed},${rate}" >> "${TMP_DIR}/phase5_results.csv"
}

rm -f "${TMP_DIR}/phase5_results.csv"
run_benchmark "modern" 5000
run_benchmark "early" 5000

log "=========================================================="
log "PHASE 5 SUMMARY COMPARISON"
log "=========================================================="
cat "${TMP_DIR}/phase5_results.csv"
