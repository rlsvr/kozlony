#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=========================================================="
echo "   KOZLONY ARCHITECTURAL BENCHMARK SUITE: ALL 5 PHASES   "
echo "=========================================================="

echo "Running Phase 1: Backlog Recovery & Drain..."
"${SCRIPT_DIR}/01_backlog_recovery.sh"

echo "Running Phase 2: Hot-Thread Contention..."
"${SCRIPT_DIR}/02_hot_thread.sh"

echo "Running Phase 3: Memory & CPU Footprint..."
"${SCRIPT_DIR}/03_resource_footprint.sh"

echo "Running Phase 4: End-to-End Latency..."
"${SCRIPT_DIR}/04_e2e_latency.sh"

echo "Running Phase 5: Reconnect Storm..."
"${SCRIPT_DIR}/05_reconnect_storm.sh"

echo "All 5 benchmark phases completed successfully!"
