from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class DomainErrorEnvelope(BaseModel):
    """Stable, non-sensitive error shape for Driver/Observer/Slave boundaries."""

    model_config = ConfigDict(extra="forbid")

    code: str
    category: str = "domain"
    retryable: bool = False
    operation_ref: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class DomainError(RuntimeError):
    def __init__(self, envelope: DomainErrorEnvelope) -> None:
        self.envelope = envelope
        super().__init__(envelope.code)


def error(code: str, *, category: str = "domain", retryable: bool = False, operation_ref: str | None = None, **details: Any) -> DomainError:
    return DomainError(DomainErrorEnvelope(code=code, category=category, retryable=retryable, operation_ref=operation_ref, details=details))
