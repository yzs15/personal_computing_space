from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class DomainErrorEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str
    category: str
    retryable: bool = False
    safe_message: str
    operation_ref: str | None = None
    expected_version: str | None = None
    observed_version: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)
