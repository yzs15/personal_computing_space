#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
DOCKER_BIN="${DOCKER_BIN:-docker}"
ROLE="${1:-observer}"
case "$ROLE" in
  observer|slave) ;;
  *)
    echo "usage: $0 [observer|slave] [compose-service]" >&2
    exit 2
    ;;
esac
if [[ -n "${2:-}" ]]; then
  SERVICE="$2"
elif [[ "$ROLE" == "slave" ]]; then
  SERVICE="slave-a"
else
  SERVICE="observer"
fi

# Keep the manual migration command on the same Alembic adapter used by the
# application startup path.  This avoids importing a service composition root
# (and avoids a second SQL/psql migration implementation).
"$DOCKER_BIN" compose -f deploy/docker-compose.yml exec -T "$SERVICE" \
  env LOOM_MIGRATION_ROLE="$ROLE" python -c '
import asyncio
import os

from sqlalchemy.ext.asyncio import create_async_engine

from loom_v2.db.migrations import apply_migrations


async def main() -> None:
    engine = create_async_engine(os.environ["LOOM_DATABASE_URL"])
    async with engine.begin() as connection:
        await apply_migrations(connection, os.environ["LOOM_MIGRATION_ROLE"])
    await engine.dispose()


asyncio.run(main())
'
