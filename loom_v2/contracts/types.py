from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .constraints import Constraint, ConstraintRef
from .terms import TypedTerm


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ResourceRef(ContractModel):
    resource_id: str
    version_or_digest: str | None = None
    access_binding: dict[str, Any] = Field(default_factory=dict)
    identity_criterion: str | None = None
    provenance: list[dict[str, Any]] = Field(default_factory=list)


class DataApplication(ContractModel):
    logical_inputs: list[ResourceRef] = Field(default_factory=list)
    schema_digest: str = ""
    identity_criterion: str = "content_digest"
    expected_cardinality: str = "unknown"


class DataSystems(ContractModel):
    locality: str = "workspace"
    classification: str = "internal"
    access_profile: dict[str, Any] = Field(default_factory=dict)
    retention_profile: dict[str, Any] = Field(default_factory=dict)


class ProgramApplication(ContractModel):
    operation_ref: str = ""
    semantics_digest: str = ""
    input_schema: str = ""
    output_schema: str = ""
    success_semantics: dict[str, Any] = Field(default_factory=dict)


class ProgramSystems(ContractModel):
    executor_kind: str = "python"
    effect_class: str = "Known"
    permissions: list[str] = Field(default_factory=list)
    package_ref: ResourceRef | None = None
    replay_safety: str = "Idempotent"


class ComputeApplication(ContractModel):
    capability_intent: str = "cpu"
    requirement_refs: list[str] = Field(default_factory=list)
    result_expectation: dict[str, Any] = Field(default_factory=dict)


class ComputeSystems(ContractModel):
    requirement_refs: list[str] = Field(default_factory=list)
    admission_requirements: list[str] = Field(default_factory=list)


class ComputeRequirement(ContractModel):
    requirement_id: str = ""
    key: str
    value: Any
    view: str
    constraint_ref: ConstraintRef | None = None

    def model_post_init(self, __context: Any) -> None:
        if not self.requirement_id:
            self.requirement_id = "requirement-" + hashlib.sha256(self.key.encode()).hexdigest()[:12]


class TypedHole(ContractModel):
    hole_id: str
    type: str = "ComputeRealization"
    constraint_refs: list[ConstraintRef] = Field(default_factory=list)
    status: str = "unbound"
    binding_ref: str | None = None


class ComputeSpec(ContractModel):
    capability_requirements: list[TypedTerm] = Field(default_factory=list)
    operation_ref: str = ""
    requirements: list[ComputeRequirement] = Field(default_factory=list)
    typed_holes: list[TypedHole] = Field(default_factory=list)


class ComputeBinding(ContractModel):
    binding_id: str
    hole_id: str
    capability_descriptor_ref: ResourceRef
    capability_package_ref: ResourceRef | None = None
    target_resource_ref: ResourceRef
    realization_digest: str
    constraint_evidence_refs: list[str] = Field(default_factory=list)
    bound_by: str = "driver"
    bound_at: str = ""


class TaskClosure(ContractModel):
    closure_id: str = "closure-default"
    schema_version: str = "1"
    data: DataApplication = Field(default_factory=DataApplication)
    data_systems: DataSystems = Field(default_factory=DataSystems)
    program: ProgramApplication = Field(default_factory=ProgramApplication)
    program_systems: ProgramSystems = Field(default_factory=ProgramSystems)
    compute: ComputeSpec = Field(default_factory=ComputeSpec)
    compute_systems: ComputeSystems = Field(default_factory=ComputeSystems)
    terms: list[TypedTerm] = Field(default_factory=list)
    constraints: list[Constraint] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def minimal(cls, **kwargs: Any) -> "TaskClosure":
        return cls(**kwargs)

    def canonical_digest(self) -> str:
        payload = json.dumps(self.model_dump(mode="json", exclude_none=True), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        return hashlib.sha256(payload).hexdigest()


class ClosureContract(ContractModel):
    closure_id: str
    goal: str
    declared_constraints: list[Constraint] = Field(default_factory=list)
    body: TaskClosure


class ClosureVersion(ContractModel):
    version_id: str
    closure_id: str
    parent_version: str | None = None
    kind: str = "draft"
    snapshot: TaskClosure
    snapshot_digest: str
    patch_cursor: int = 0
    registry_digest: str = ""


class Execution(ContractModel):
    execution_id: str
    closure_version_ref: str
    state: str = "created"
    execution_epoch: int = 1
