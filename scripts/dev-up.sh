#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
DOCKER_BIN="${DOCKER_BIN:-docker}"
if ! command -v "$DOCKER_BIN" >/dev/null 2>&1 || ! "$DOCKER_BIN" info >/dev/null 2>&1; then
  for candidate in "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe" "/usr/bin/docker.exe"; do
    if [[ -x "$candidate" ]] && "$candidate" info >/dev/null 2>&1; then DOCKER_BIN="$candidate"; break; fi
  done
fi
# Observer runs on the host so it can use the host Codex app-server/config;
# MinIO, databases and Worker Slaves are containerized here.
"$DOCKER_BIN" compose -f deploy/docker-compose.yml up -d --build --remove-orphans minio minio-init observer-db driver-db slave-a-db slave-b-db slave-a slave-b
for _ in {1..30}; do
  if "$DOCKER_BIN" compose -f deploy/docker-compose.yml exec -T observer-db pg_isready -U loom -d loom_observer >/dev/null 2>&1; then break; fi
  sleep 1
done

export LOOM_DATABASE_URL="${LOOM_DATABASE_URL:-postgresql+asyncpg://loom:loom@127.0.0.1:15432/loom_observer}"
export LOOM_CODING_AGENT_BACKEND="${LOOM_CODING_AGENT_BACKEND:-codex}"
export LOOM_CODEX_MODEL="${LOOM_CODEX_MODEL:-deepseek-v4-flash}"
export LOOM_CODING_AGENT_DEADLINE_SECONDS="${LOOM_CODING_AGENT_DEADLINE_SECONDS:-86400}"
export LOOM_CODING_AGENT_POLL_INTERVAL_SECONDS="${LOOM_CODING_AGENT_POLL_INTERVAL_SECONDS:-5}"
export LOOM_CODING_AGENT_PROTOCOL_FAILURE_SECONDS="${LOOM_CODING_AGENT_PROTOCOL_FAILURE_SECONDS:-60}"
export LOOM_WORKER_OPERATION_TIMEOUT_SECONDS="${LOOM_WORKER_OPERATION_TIMEOUT_SECONDS:-90}"
export LOOM_CAPABILITY_OPERATION_TIMEOUT_SECONDS="${LOOM_CAPABILITY_OPERATION_TIMEOUT_SECONDS:-30}"
export LOOM_WORKSPACE_ROOT="${LOOM_WORKSPACE_ROOT:-$ROOT_DIR}"
export LOOM_SLAVE_A_URL="${LOOM_SLAVE_A_URL:-http://127.0.0.1:8081}"
export LOOM_SLAVE_B_URL="${LOOM_SLAVE_B_URL:-http://127.0.0.1:8082}"
export LOOM_S3_ENDPOINT_URL="${LOOM_S3_ENDPOINT_URL:-http://127.0.0.1:9000}"
export LOOM_S3_BUCKET="${LOOM_S3_BUCKET:-loom-content}"
export LOOM_S3_ACCESS_KEY="${LOOM_S3_ACCESS_KEY:-loom}"
export LOOM_S3_SECRET_KEY="${LOOM_S3_SECRET_KEY:-loom-content-secret}"
export LOOM_S3_REGION="${LOOM_S3_REGION:-us-east-1}"
export LOOM_S3_PREFIX="${LOOM_S3_PREFIX:-}"
OBSERVER_PORT="${LOOM_OBSERVER_PORT:-18080}"
echo "Loom v2 is available at http://localhost:${OBSERVER_PORT} (backend=$LOOM_CODING_AGENT_BACKEND, model=$LOOM_CODEX_MODEL)"
exec .venv/bin/uvicorn loom_v2.observer.app:app --host 0.0.0.0 --port "$OBSERVER_PORT"
