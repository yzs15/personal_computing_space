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
"$DOCKER_BIN" compose -f deploy/docker-compose.yml up -d --build --remove-orphans observer-db slave-a-db slave-b-db slave-a slave-b
for _ in {1..30}; do
  if "$DOCKER_BIN" compose -f deploy/docker-compose.yml exec -T observer-db pg_isready -U loom -d loom_observer >/dev/null 2>&1; then break; fi
  sleep 1
done

export LOOM_DATABASE_URL="${LOOM_DATABASE_URL:-postgresql+asyncpg://loom:loom@127.0.0.1:15432/loom_observer}"
export LOOM_CODING_AGENT_BACKEND="${LOOM_CODING_AGENT_BACKEND:-codex}"
export LOOM_CODEX_MODEL="${LOOM_CODEX_MODEL:-deepseek-v4-flash}"
export LOOM_WORKSPACE_ROOT="${LOOM_WORKSPACE_ROOT:-$ROOT_DIR}"
echo "Loom v2 is available at http://localhost:8080 (backend=$LOOM_CODING_AGENT_BACKEND, model=$LOOM_CODEX_MODEL)"
exec .venv/bin/uvicorn loom_v2.observer.app:app --host 0.0.0.0 --port 8080
