"""Application adapter for the role-local Alembic schema baseline.

This is intentionally a small integration wrapper. Alembic revisions own all
schema evolution; the application only injects its already-open connection,
the role-local migration context, and the PostgreSQL concurrency lock.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.engine import Connection


MigrationRole = Literal["observer", "slave"]

_ALEMBIC_ROOT = Path(__file__).resolve().parents[2] / "migrations" / "alembic"
def _config(role: MigrationRole, connection: Connection) -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_ROOT))
    config.set_main_option("sqlalchemy.url", "")
    config.attributes["connection"] = connection
    config.attributes["role"] = role
    return config


def _upgrade(connection: Connection, role: MigrationRole) -> None:
    # Alembic serializes revisions in its version table but does not acquire a
    # cross-process application lock. Keep startup safe when multiple role
    # replicas race to initialize the same PostgreSQL database.
    connection.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
        {"lock_key": f"loom:alembic:{role}"},
    )
    config = _config(role, connection)
    command.upgrade(config, "head")


async def apply_migrations(connection: AsyncConnection, role: MigrationRole) -> None:
    """Upgrade one role-local PostgreSQL database to the Alembic head."""

    if connection.dialect.name != "postgresql":
        raise RuntimeError("alembic_requires_postgresql")
    await connection.run_sync(lambda sync_connection: _upgrade(sync_connection, role))


__all__ = ["MigrationRole", "apply_migrations"]
