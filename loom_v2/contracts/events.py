from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ProvenanceLink(BaseModel):
    model_config = ConfigDict(extra="forbid")

    relation: str
    source_ref: str
    target_ref: str
    details: dict[str, Any] = Field(default_factory=dict)


class RunEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    sequence: int
    activity: str
    phase: str
    outcome: dict[str, Any] | None = None
    causal_operation_id: str
    evidence_refs: list[str] = Field(default_factory=list)
    provenance: list[ProvenanceLink] = Field(default_factory=list)


class ResourceEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resource_ref: str
    sequence: int
    state: str
    readiness_evidence: str | None = None
    manager_generation: int = 1
    causal_operation_id: str
