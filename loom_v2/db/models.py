from sqlalchemy import BIGINT, JSON, Boolean, DateTime, Index, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, SlaveBase


class RunRow(Base):
    __tablename__ = "runs"

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    task_ref: Mapped[str] = mapped_column(String(256), nullable=False)
    goal: Mapped[str] = mapped_column(String(2048), nullable=False)
    closure_contract: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    allow_reassignment: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    draft: Mapped[dict] = mapped_column(JSON, nullable=False)
    committed: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    execution_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    execution_epoch: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    state: Mapped[str] = mapped_column(String(64), nullable=False)
    outcome: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    attempts: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    events: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    capability_packages: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    capability_activations: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    dynamic_nodes: Mapped[list] = mapped_column(JSON, nullable=False, default=list)


class IdempotencyRow(Base):
    __tablename__ = "idempotency"

    operation_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    receipt: Mapped[dict] = mapped_column(JSON, nullable=False)


class RuntimeAgentRow(Base):
    __tablename__ = "runtime_agents"
    __table_args__ = (
        Index("idx_runtime_agents_workspace_role_agent", "workspace_id", "role", "agent_id"),
        Index(
            "uq_runtime_agents_active_agent",
            "workspace_id",
            "role",
            "agent_id",
            unique=True,
            postgresql_where=text("lease_state = 'active'"),
            sqlite_where=text("lease_state = 'active'"),
        ),
        Index(
            "uq_runtime_agents_active_driver",
            "workspace_id",
            unique=True,
            postgresql_where=text("role = 'driver' AND lease_state = 'active'"),
            sqlite_where=text("role = 'driver' AND lease_state = 'active'"),
        ),
    )

    workspace_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    role: Mapped[str] = mapped_column(String(32), primary_key=True)
    agent_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    instance_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    endpoint_url: Mapped[str] = mapped_column(String(512), nullable=False)
    protocol_version: Mapped[str] = mapped_column(String(64), nullable=False)
    capabilities: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    epoch: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    lease_id_hash: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    lease_state: Mapped[str] = mapped_column(String(32), nullable=False, default="expired")
    last_seen_at: Mapped[object | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[object | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[object | None] = mapped_column(DateTime(timezone=True), nullable=True)


class DriverThreadRow(Base):
    __tablename__ = "driver_threads"
    __table_args__ = (
        UniqueConstraint("workspace_id", "conversation_ref", name="uq_driver_thread_conversation"),
        UniqueConstraint("workspace_id", "thread_id", name="uq_driver_thread_id"),
        Index("idx_driver_threads_workspace_conversation", "workspace_id", "conversation_ref"),
    )

    workspace_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    conversation_ref: Mapped[str] = mapped_column(String(256), primary_key=True)
    thread_id: Mapped[str] = mapped_column(String(256), nullable=False)
    model: Mapped[str] = mapped_column(String(256), nullable=False)
    workspace_root: Mapped[str] = mapped_column(String(512), nullable=False)
    last_turn_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    turn_state: Mapped[str] = mapped_column(String(32), nullable=False, default="idle")
    active_request_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    driver_epoch: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    updated_at: Mapped[object | None] = mapped_column(DateTime(timezone=True), nullable=True)


class DriverRequestRow(Base):
    __tablename__ = "driver_requests"
    __table_args__ = (
        UniqueConstraint("driver_id", "request_id", name="uq_driver_request"),
        Index("idx_driver_requests_driver_request", "driver_id", "request_id"),
    )

    driver_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    request_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    command: Mapped[str] = mapped_column(String(128), nullable=False)
    response: Mapped[dict] = mapped_column(JSON, nullable=False)


class MessageReceiptRow(Base):
    __tablename__ = "message_receipts"
    __table_args__ = (
        Index("idx_message_receipts_conversation", "workspace_id", "conversation_ref", "created_at"),
        Index("idx_message_receipts_dispatch", "state", "next_attempt_at"),
    )

    workspace_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    request_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    conversation_ref: Mapped[str] = mapped_column(String(256), nullable=False)
    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    payload_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    run_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    assistant_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    outcome: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    claim_token: Mapped[str | None] = mapped_column(String(128), nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_attempt_at: Mapped[object | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)


class SlaveAttemptRow(SlaveBase):
    """Role-local execution ledger owned by one Slave PostgreSQL instance."""

    __tablename__ = "slave_attempts"

    attempt_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    slave_id: Mapped[str] = mapped_column(String(128), nullable=False)
    workspace_id: Mapped[str] = mapped_column(String(128), nullable=False)
    operation: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    state: Mapped[str] = mapped_column(String(64), nullable=False)
    result: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class SlaveReplicaRow(SlaveBase):
    """Current WorkspaceReplica fact; it cannot write Observer run state."""

    __tablename__ = "slave_replica"

    slave_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    workspace_id: Mapped[str] = mapped_column(String(128), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    digest: Mapped[str] = mapped_column(String(256), nullable=False, default="")
