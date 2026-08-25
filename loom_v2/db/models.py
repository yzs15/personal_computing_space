from sqlalchemy import JSON, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class RunRow(Base):
    __tablename__ = "runs"

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    task_ref: Mapped[str] = mapped_column(String(256), nullable=False)
    goal: Mapped[str] = mapped_column(String(2048), nullable=False)
    draft: Mapped[dict] = mapped_column(JSON, nullable=False)
    committed: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    execution_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    execution_epoch: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    state: Mapped[str] = mapped_column(String(64), nullable=False)
    attempts: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    events: Mapped[list] = mapped_column(JSON, nullable=False, default=list)


class IdempotencyRow(Base):
    __tablename__ = "idempotency"

    operation_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    receipt: Mapped[dict] = mapped_column(JSON, nullable=False)
