#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
DOCKER_BIN="${DOCKER_BIN:-docker}"
"$DOCKER_BIN" compose -f deploy/docker-compose.yml exec observer python -c 'import asyncio; from loom_v2.observer.app import app; asyncio.run(app.state.repo.init_db())'
