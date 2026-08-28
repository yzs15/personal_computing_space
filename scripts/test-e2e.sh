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
OBSERVER_PORT="${LOOM_OBSERVER_PORT:-18080}"
curl -fsS "http://localhost:${OBSERVER_PORT}/healthz"
curl -fsS "http://localhost:${OBSERVER_PORT}/" | grep -q "Run drawer"
"$DOCKER_BIN" compose -f deploy/docker-compose.yml ps --status running
echo "Loom v2 smoke checks passed"
