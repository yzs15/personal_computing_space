"""Small helpers shared by role-aware Alembic revisions."""

from __future__ import annotations

from alembic import op


def migration_role() -> str:
    """Return the role injected by the application migration adapter.

    Alembic's operation context is optional in its type declarations because
    the same module can be imported while generating offline SQL. Revisions
    treat a missing context as no role, which safely makes role-local DDL a
    no-op outside the application adapter.
    """

    migration_context = op.get_context()
    config = migration_context.config
    if config is None:
        return ""
    return str(config.attributes.get("role") or "")


__all__ = ["migration_role"]
