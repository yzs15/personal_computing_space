from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _digest(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(payload).hexdigest()


class ConstraintRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    constraint_id: str
    version: str = "1"
    digest: str


class ConstraintSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    subject: list[str] = Field(default_factory=list)
    predicate: dict[str, Any]
    source: str = "Requester"
    fate: str = "preserve"
    relaxability: str = "RequesterMayRelax"
    enforcement: str = "AdmissionBlock"
    propagation: dict[str, Any] = Field(default_factory=dict)
    evidence_requirements: list[dict[str, Any]] = Field(default_factory=list)

    @field_validator("predicate")
    @classmethod
    def reject_callable_predicate(cls, value: dict[str, Any]) -> dict[str, Any]:
        if any(callable(item) for item in value.values()):
            raise ValueError("callable predicates are not part of the canonical contract")
        return value


class Constraint(ConstraintSpec):
    constraint_id: str = ""

    def model_post_init(self, __context: Any) -> None:
        if not self.constraint_id:
            self.constraint_id = f"constraint-{_digest(self.predicate)[:16]}"

    def ref(self) -> ConstraintRef:
        return ConstraintRef(
            constraint_id=self.constraint_id,
            version="1",
            digest=_digest(self.model_dump(mode="json", exclude={"constraint_id"})),
        )
