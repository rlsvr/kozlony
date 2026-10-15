#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"
stress_bin=$(build_stress_binary)

measure_resource() {
  local version="$1"
  local clients="$2"

  log "----------------------------------------------------------"
  log "Measuring ${version} with ${clients} SSE clients..."
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

  local pid
  pid=$(cat "${TMP_DIR}/api.pid")

  # Warm up baseline (3s)
  sleep 3

  local client_arg=()
  if [ "${clients}" -gt 0 ]; then
    client_arg=("-with-clients" "${clients}")
  fi

  # Run light background load (200 RPS) to make fanout active
  "${stress_bin}" -url http://localhost:8080 -rps 200 -duration 12s -workers 50 "${client_arg[@]}" > "${TMP_DIR}/footprint_${version}_${clients}.log" 2>&1 &
  local stress_pid=$!

  # Wait 6s for clients to connect and stream
  sleep 6

  # Sample RSS in KB
  local rss_kb
  rss_kb=$(ps -o rss= -p "${pid}" 2>/dev/null || echo 0)
  local rss_mb=$(echo "scale=1; ${rss_kb} / 1024" | bc)

  # Sample CPU% of API
  local cpu_pct
  cpu_pct=$(ps -o %cpu= -p "${pid}" 2>/dev/null || echo 0)

  wait "${stress_pid}" 2>/dev/null || true
  stop_api

  log "Result (${version}, ${clients} SSE): RSS = ${rss_mb} MB | CPU = ${cpu_pct}%"
  echo "${version},${clients},${rss_mb},${cpu_pct}" >> "${TMP_DIR}/phase3_results.csv"
}

rm -f "${TMP_DIR}/phase3_results.csv"

for c in 0 1000 5000 8000; do
  measure_resource "modern" "${c}"
done

for c in 0 1000 5000 8000; do
  measure_resource "early" "${c}"
done

log "=========================================================="
log "PHASE 3 SUMMARY COMPARISON"
log "=========================================================="
cat "${TMP_DIR}/phase3_results.csv"
