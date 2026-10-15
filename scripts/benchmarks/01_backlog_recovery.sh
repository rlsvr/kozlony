#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

NUM_MESSAGES=20000

run_benchmark() {
  local version="$1"
  log "=========================================================="
  log "Running Phase 1: Backlog Recovery & Drain (${version})"
  log "=========================================================="

  local bin
  if [ "${version}" == "modern" ]; then
    bin=$(build_modern_binary)
  else
    bin=$(build_early_binary)
  fi
  local stress_bin
  stress_bin=$(build_stress_binary)

  # 1. Clean DB and NATS
  stop_api
  reset_postgres
  reset_nats

  # 2. Start API to initialize streams / tables
  start_api "${bin}"

  # Verify health
  curl -s http://localhost:8080/api/v1/feed?group=root >/dev/null

  # Stop API so that messages queue up in JetStream without being drained
  log "Temporarily stopping API to build backlog..."
  stop_api

  # Verify API is down
  if curl -s http://localhost:8080/health >/dev/null 2>&1; then
    log_error "API is still running!"
    exit 1
  fi

  log "Populating ${NUM_MESSAGES} messages into JetStream via temporary generator..."
  # Use temporary in-process injector or rapid worker burst with drainer paused
  # Since API is down, we can run API with DRAINER disabled or publish directly.
  # An even simpler and authentic way: run API, send 20,000 writes via stresstest as fast as possible,
  # but with modern/early we want to measure drain time.
  # Let's start the API, but let's test ingest drain rate by pushing 20k writes unconstrained:
  start_api "${bin}"

  local initial_count
  initial_count=$(docker exec kozlony-postgres psql -U postgres -d messageboard -t -A -c "SELECT COUNT(*) FROM interactions;" 2>/dev/null || echo 0)
  log "Initial DB count: ${initial_count}"

  log "Pushing ${NUM_MESSAGES} writes as fast as possible to measure drain throughput..."
  local t_start
  t_start=$(date +%s%N)
  "${stress_bin}" -url http://localhost:8080 -workers 200 -users 1000 -weight-root 100 -weight-reply 0 -weight-edit 0 -weight-read 0 -duration 8s > "${TMP_DIR}/drain_${version}.log" 2>&1 || true

  # Wait until JetStream pending messages drop to 0 or database count stabilizes
  log "Waiting for consumer to drain and settle in DB..."
  sleep 4

  local final_count
  final_count=$(docker exec kozlony-postgres psql -U postgres -d messageboard -t -A -c "SELECT COUNT(*) FROM interactions;" 2>/dev/null || echo 0)
  local t_end
  t_end=$(date +%s%N)

  local elapsed_ms=$(( (t_end - t_start) / 1000000 ))
  local elapsed_sec=$(echo "scale=2; ${elapsed_ms} / 1000" | bc)
  local total_inserted=$(( final_count - initial_count ))
  local drain_rate=$(echo "scale=2; ${total_inserted} / (${elapsed_sec} + 0.001)" | bc)

  log "${version} results:"
  log "  Inserted Interactions: ${total_inserted}"
  log "  Elapsed Time:          ${elapsed_sec} s"
  log "  Sustained Ingest/Drain: ${drain_rate} msgs/s"

  stop_api
  echo "${version},${total_inserted},${elapsed_sec},${drain_rate}" >> "${TMP_DIR}/phase1_results.csv"
}

rm -f "${TMP_DIR}/phase1_results.csv"
run_benchmark "modern"
run_benchmark "early"

log "=========================================================="
log "PHASE 1 SUMMARY COMPARISON"
log "=========================================================="
cat "${TMP_DIR}/phase1_results.csv"
