from __future__ import annotations

import hashlib
import json
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


def message_payload_digest(conversation_ref: str, prompt: str) -> str:
    """Return the stable digest used by the message idempotency contract."""
    payload = {"conversation_ref": conversation_ref, "text": prompt}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class MessageReceipt(ContractModel):
    workspace_id: str = Field(min_length=1)
    request_id: str = Field(min_length=1)
    conversation_ref: str = Field(min_length=1)
    prompt: str
    payload_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    state: MessageReceiptState
    run_id: str | None = None
    assistant_text: str | None = None
    outcome: dict[str, Any] | None = None
    claim_token: str | None = None
    attempt_count: int = Field(default=0, ge=0)
    next_attempt_at: datetime | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
