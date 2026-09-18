"""Alembic environment shared by the role-local Loom databases."""

from __future__ import annotations

from logging.config import fileConfig
from typing import cast

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine import Connection


config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None


def _role() -> str:
    role = str(config.attributes.get("role") or "")
    if role not in {"observer", "slave"}:
        raise RuntimeError("migration_role_required")
    return role


def _version_table() -> str:
    return f"loom_{_role()}_alembic_version"


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table=_version_table(),
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    injected_connection = config.attributes.get("connection")
    if injected_connection is not None:
        _run_migrations(cast(Connection, injected_connection))
        return

    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        _run_migrations(connection)


def _run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        version_table=_version_table(),
    )
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
