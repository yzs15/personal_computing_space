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

INTERNAL_SECRET_FILE="${LOOM_INTERNAL_API_SECRET_FILE:-$ROOT_DIR/secrets/internal_api_secret}"
if [[ ! -s "$INTERNAL_SECRET_FILE" ]]; then
  echo "Missing non-empty secret file: $INTERNAL_SECRET_FILE" >&2
  echo "Create it before starting the stack (see README.md)." >&2
  exit 1
fi

export LOOM_INTERNAL_API_SECRET_FILE="$INTERNAL_SECRET_FILE"
"$DOCKER_BIN" compose -f deploy/docker-compose.yml up -d --build --remove-orphans minio minio-init observer-db slave-a-db slave-b-db observer driver slave-a slave-b

echo "Loom v2 Observer: http://localhost:18080"
echo "Driver and Slaves run on the internal Compose network (Observer is the public entry point)"
