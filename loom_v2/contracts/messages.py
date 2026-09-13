from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import Field

from .types import ContractModel


MessageReceiptState = Literal[
    "accepted",
    "queued",
    "in_flight",
    "completed",
    "retryable",
    "failed",
    "interrupted",
]


class MessageReceipt(ContractModel):
    workspace_id: str = Field(min_length=1)
    request_id: str = Field(min_length=1)
    conversation_ref: str = Field(min_length=1)
    prompt: str
    state: MessageReceiptState
    run_id: str | None = None
    assistant_text: str | None = None
    outcome: dict[str, Any] | None = None
    claim_token: str | None = None
    attempt_count: int = Field(default=0, ge=0)
    next_attempt_at: datetime | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
