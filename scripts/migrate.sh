#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
DOCKER_BIN="${DOCKER_BIN:-docker}"
"$DOCKER_BIN" compose -f deploy/docker-compose.yml exec observer python -c 'import asyncio; from loom_v2.observer.app import app; asyncio.run(app.state.repo.init_db())'
"$DOCKER_BIN" compose -f deploy/docker-compose.yml exec -T observer-db \
  psql -U "${POSTGRES_USER:-loom}" -d "${POSTGRES_DB:-loom_observer}" \
  -v ON_ERROR_STOP=1 -f - < migrations/008_run_lifecycle_state_machine.sql
