"""Initial schema for the first public Loom release.

The project was not deployed outside internal testing before this revision was
cut.  The historical development migrations were therefore squashed into one
role-local baseline.  Future releases must add a new Alembic revision instead
of editing this file.
"""

from alembic import op

from loom_v2.db.alembic_context import migration_role


revision = "001_release_baseline"
down_revision = None
branch_labels = None
depends_on = None


def _observer() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            run_id VARCHAR(128) PRIMARY KEY,
            task_ref VARCHAR(256) NOT NULL,
            goal VARCHAR(2048) NOT NULL,
            closure_contract JSONB,
            allow_reassignment BOOLEAN NOT NULL DEFAULT FALSE,
            draft JSONB NOT NULL,
            committed JSONB,
            execution_id VARCHAR(128),
            execution_epoch INTEGER NOT NULL DEFAULT 1,
            state VARCHAR(64) NOT NULL,
            outcome JSONB,
            attempts JSONB NOT NULL DEFAULT '[]'::jsonb,
            events JSONB NOT NULL DEFAULT '[]'::jsonb,
            capability_packages JSONB NOT NULL DEFAULT '[]'::jsonb,
            capability_activations JSONB NOT NULL DEFAULT '[]'::jsonb,
            dynamic_nodes JSONB NOT NULL DEFAULT '[]'::jsonb
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS idempotency (
            operation_id VARCHAR(256) PRIMARY KEY,
            receipt JSONB NOT NULL
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS runtime_agents (
            workspace_id VARCHAR(128) NOT NULL,
            role VARCHAR(32) NOT NULL,
            agent_id VARCHAR(128) NOT NULL,
            instance_id VARCHAR(128) NOT NULL,
            endpoint_url VARCHAR(512) NOT NULL,
            protocol_version VARCHAR(64) NOT NULL,
            capabilities JSONB NOT NULL DEFAULT '{}'::jsonb,
            epoch BIGINT NOT NULL DEFAULT 0,
            lease_id_hash VARCHAR(128) NOT NULL DEFAULT '',
            lease_state VARCHAR(32) NOT NULL DEFAULT 'expired',
            last_seen_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ,
            PRIMARY KEY (workspace_id, role, agent_id, instance_id)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS driver_threads (
            workspace_id VARCHAR(128) NOT NULL,
            conversation_ref VARCHAR(256) NOT NULL,
            thread_id VARCHAR(256) NOT NULL,
            model VARCHAR(256) NOT NULL,
            workspace_root VARCHAR(512) NOT NULL,
            last_turn_id VARCHAR(256),
            turn_state VARCHAR(32) NOT NULL DEFAULT 'idle',
            active_request_id VARCHAR(256),
            driver_epoch BIGINT NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ,
            PRIMARY KEY (workspace_id, conversation_ref),
            UNIQUE (workspace_id, conversation_ref),
            UNIQUE (workspace_id, thread_id)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS driver_requests (
            driver_id VARCHAR(128) NOT NULL,
            request_id VARCHAR(256) NOT NULL,
            command VARCHAR(128) NOT NULL,
            response JSONB NOT NULL,
            PRIMARY KEY (driver_id, request_id)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS message_receipts (
            workspace_id VARCHAR(128) NOT NULL,
            request_id VARCHAR(256) NOT NULL,
            conversation_ref VARCHAR(256) NOT NULL,
            prompt TEXT NOT NULL,
            state VARCHAR(32) NOT NULL,
            run_id VARCHAR(128),
            assistant_text TEXT,
            outcome JSONB,
            claim_token VARCHAR(128),
            attempt_count INTEGER NOT NULL DEFAULT 0,
            next_attempt_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (workspace_id, request_id)
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_runtime_agents_workspace_role_agent ON runtime_agents (workspace_id, role, agent_id)")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_agents_active_agent ON runtime_agents (workspace_id, role, agent_id) WHERE lease_state = 'active'")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_agents_active_driver ON runtime_agents (workspace_id) WHERE role = 'driver' AND lease_state = 'active'")
    op.execute("CREATE INDEX IF NOT EXISTS idx_driver_threads_workspace_conversation ON driver_threads (workspace_id, conversation_ref)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_driver_requests_driver_request ON driver_requests (driver_id, request_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_message_receipts_conversation ON message_receipts (workspace_id, conversation_ref, created_at)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_message_receipts_dispatch ON message_receipts (state, next_attempt_at)")


def _slave() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS slave_attempts (
            attempt_id VARCHAR(128) PRIMARY KEY,
            slave_id VARCHAR(128) NOT NULL,
            workspace_id VARCHAR(128) NOT NULL,
            operation VARCHAR(128) NOT NULL,
            payload JSONB NOT NULL,
            state VARCHAR(64) NOT NULL,
            result JSONB
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS slave_replica (
            slave_id VARCHAR(128) PRIMARY KEY,
            workspace_id VARCHAR(128) NOT NULL,
            state VARCHAR(32) NOT NULL,
            capability_activations JSONB NOT NULL DEFAULT '[]'::jsonb
        )
        """
    )


def upgrade() -> None:
    if op.get_bind().dialect.name != "postgresql":
        return
    role = migration_role()
    if role == "observer":
        _observer()
    elif role == "slave":
        _slave()

