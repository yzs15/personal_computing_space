from sqlalchemy import JSON, Boolean, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, SlaveBase


class RunRow(Base):
    __tablename__ = "runs"

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    task_ref: Mapped[str] = mapped_column(String(256), nullable=False)
    goal: Mapped[str] = mapped_column(String(2048), nullable=False)
    allow_reassignment: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    draft: Mapped[dict] = mapped_column(JSON, nullable=False)
    committed: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    execution_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    execution_epoch: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    state: Mapped[str] = mapped_column(String(64), nullable=False)
    outcome: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    attempts: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    events: Mapped[list] = mapped_column(JSON, nullable=False, default=list)


class IdempotencyRow(Base):
    __tablename__ = "idempotency"

    operation_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    receipt: Mapped[dict] = mapped_column(JSON, nullable=False)


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
