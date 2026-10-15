#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
EARLY_COMMIT="4ed5a167c3e97ab852111bd5f14dea7aa9f3c6eb"
TMP_DIR="/tmp/kozlony-bench"

mkdir -p "${TMP_DIR}"

log() {
  echo -e "\033[1;34m[BENCH]\033[0m $*"
}

log_warn() {
  echo -e "\033[1;33m[WARN]\033[0m $*"
}

log_error() {
  echo -e "\033[1;31m[ERROR]\033[0m $*"
}

# Reset Postgres interactions
reset_postgres() {
  log "Resetting PostgreSQL interactions table..."
  docker exec kozlony-postgres psql -U postgres -d messageboard -c "TRUNCATE interactions RESTART IDENTITY CASCADE;" >/dev/null 2>&1 || true
}

# Reset NATS JetStream data and restart container
reset_nats() {
  log "Resetting NATS container & JetStream storage..."
  docker compose -f "${ROOT_DIR}/docker-compose.yml" stop nats >/dev/null 2>&1 || true
  docker run --rm -v kozlony_nats_data:/data alpine rm -rf /data/* >/dev/null 2>&1 || true
  docker compose -f "${ROOT_DIR}/docker-compose.yml" up -d nats >/dev/null 2>&1
  # Wait for NATS to be healthy
  for i in {1..30}; do
    if curl -s http://localhost:8223/healthz | grep -q "ok" 2>/dev/null; then
      break
    fi
    sleep 0.2
  done
}

# Clean environment: stops any running API processes on port 8080
kill_port_8080() {
  local pids
  pids=$(lsof -ti :8080 2>/dev/null || true)
  if [ -n "${pids}" ]; then
    log "Stopping existing processes on port 8080: ${pids}"
    kill -9 ${pids} >/dev/null 2>&1 || true
    sleep 0.5
  fi
}

# Compile modern binary
build_modern_binary() {
  local bin_path="${TMP_DIR}/kozlony-modern"
  log "Building modern Kozlony binary (${bin_path})..." >&2
  (cd "${ROOT_DIR}" && go build -o "${bin_path}" ./cmd/kozlony)
  echo "${bin_path}"
}

# Compile early binary using git worktree
build_early_binary() {
  local wt_path="${TMP_DIR}/worktree-early"
  local bin_path="${TMP_DIR}/kozlony-early"
  
  if [ ! -f "${bin_path}" ]; then
    log "Preparing early worktree at ${EARLY_COMMIT}..." >&2
    rm -rf "${wt_path}"
    git -C "${ROOT_DIR}" worktree add "${wt_path}" "${EARLY_COMMIT}" --detach >/dev/null 2>&1
    log "Building early Kozlony binary (${bin_path})..." >&2
    (cd "${wt_path}" && go build -o "${bin_path}" ./cmd/kozlony)
    git -C "${ROOT_DIR}" worktree remove "${wt_path}" --force >/dev/null 2>&1 || rm -rf "${wt_path}"
  fi
  echo "${bin_path}"
}

# Build stress test binary
build_stress_binary() {
  local bin_path="${ROOT_DIR}/bin/stresstest"
  if [ ! -f "${bin_path}" ]; then
    log "Building stresstest tool..." >&2
    (cd "${ROOT_DIR}" && go build -o "${bin_path}" ./tools/stress)
  fi
  echo "${bin_path}"
}

# Start binary and wait for health
start_api() {
  local bin="$1"
  kill_port_8080
  log "Starting server: ${bin}..."
  "${bin}" > "${TMP_DIR}/api.log" 2>&1 &
  local pid=$!
  echo "${pid}" > "${TMP_DIR}/api.pid"

  # Wait for port 8080
  for i in {1..50}; do
    if curl -s http://localhost:8080/health >/dev/null 2>&1 || curl -s http://localhost:8080/api/v1/feed?group=root >/dev/null 2>&1; then
      log "Server is up (PID ${pid})."
      return 0
    fi
    if ! kill -0 "${pid}" 2>/dev/null; then
      log_error "Server exited prematurely. Log:"
      cat "${TMP_DIR}/api.log"
      return 1
    fi
    sleep 0.2
  done
  log_error "Server failed to start within timeout."
  return 1
}

# Stop API cleanly
stop_api() {
  if [ -f "${TMP_DIR}/api.pid" ]; then
    local pid
    pid=$(cat "${TMP_DIR}/api.pid")
    if kill -0 "${pid}" 2>/dev/null; then
      log "Stopping API server (PID ${pid})..."
      kill "${pid}" >/dev/null 2>&1 || true
      wait "${pid}" 2>/dev/null || true
    fi
    rm -f "${TMP_DIR}/api.pid"
  fi
  kill_port_8080
}
