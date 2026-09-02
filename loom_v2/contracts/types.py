from __future__ import annotations

import hashlib
import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

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
    terms: list[TypedTerm] = Field(default_factory=list)


class DataSystems(ContractModel):
    locality: str = "workspace"
    classification: str = "internal"
    access_profile: dict[str, Any] = Field(default_factory=dict)
    retention_profile: dict[str, Any] = Field(default_factory=dict)
    terms: list[TypedTerm] = Field(default_factory=list)


class IoContract(ContractModel):
    """Content-addressed input/output contract for an executable node."""

    schema_version: Literal["io.v1"] = "io.v1"
    input_schema_ref: ResourceRef | None = None
    output_schema_ref: ResourceRef | None = None
    success_semantics: Any | None = None
    success_validator_ref: ResourceRef | None = None


class ValidationEvidence(ContractModel):
    """Deterministic schema/validator evidence accepted by Observer."""

    evidence_id: str
    attempt_id: str
    execution_epoch: int
    validator_ref: ResourceRef | None = None
    schema_ref: ResourceRef | None = None
    input_digest: str | None = None
    output_digest: str | None = None
    result: Literal["pass", "fail"]
    errors: list[dict[str, Any]] = Field(default_factory=list)
    issuer: Literal["slave", "observer"]
    created_at: str


class ProgramApplication(ContractModel):
    operation_ref: str = ""
    semantics_digest: str = ""
    # These fields remain the v2 application view.  Executable validation is
    # anchored by ``io_contract_ref``; the repository will resolve the view
    # from that content-addressed contract instead of trusting free text.
    input_schema: Any | None = None
    output_schema: Any | None = None
    success_semantics: Any | None = None
    io_contract_ref: ResourceRef | None = None
    terms: list[TypedTerm] = Field(default_factory=list)


class ProgramSystems(ContractModel):
    executor_kind: str = "python"
    effect_class: str = "Known"
    permissions: list[str] = Field(default_factory=list)
    package_ref: ResourceRef | None = None
    replay_safety: str = "Idempotent"
    terms: list[TypedTerm] = Field(default_factory=list)


class ComputeApplication(ContractModel):
    capability_intent: str = "cpu"
    requirement_refs: list[str] = Field(default_factory=list)
    result_expectation: dict[str, Any] = Field(default_factory=dict)
    terms: list[TypedTerm] = Field(default_factory=list)


class ComputeSystems(ContractModel):
    requirement_refs: list[str] = Field(default_factory=list)
    admission_requirements: list[str] = Field(default_factory=list)
    terms: list[TypedTerm] = Field(default_factory=list)


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
    # A provider-fillable hole is part of a package definition and is
    # resolved only when that package is activated on a concrete Slave.
    provider_fillable: bool = False


class ComputeSpec(ContractModel):
    capability_requirements: list[TypedTerm] = Field(default_factory=list)
    operation_ref: str = ""
    requirements: list[ComputeRequirement] = Field(default_factory=list)
    typed_holes: list[TypedHole] = Field(default_factory=list)
    terms: list[TypedTerm] = Field(default_factory=list)


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
    executor_descriptor_digest: str | None = None
    runtime_profile: dict[str, Any] = Field(default_factory=dict)
    activation_ref: str | None = None


class CapabilityPackageVersion(ContractModel):
    """Immutable, content-addressed realization of a closure subgraph."""

    package_id: str
    package_version: str
    package_closure_version_ref: str
    source_run_ref: str
    source_closure_version_ref: str
    operation_descriptor_ref: ResourceRef | str
    operation_descriptor_digest: str
    program_content_ref: ResourceRef
    program_digest: str
    # Nullable only so persisted pre-I/O records can be read; every new
    # executable materialization/provision/readiness path rejects ``None``.
    io_contract_ref: ResourceRef | None = None
    effective_constraint_refs: list[ConstraintRef] = Field(default_factory=list)
    provider_fillable_hole_refs: list[str] = Field(default_factory=list)
    scope: Literal["run_bound", "workspace_reusable"] = "run_bound"
    publication_state: Literal["candidate", "published", "abandoned"] = "candidate"
    provenance: list[dict[str, Any]] = Field(default_factory=list)
    executor_kind: str = "subprocess_json_v1"
    executor_operation: str = "run_code"
    effect_class: str = "Sandboxed"
    permissions: list[str] = Field(default_factory=list)
    replay_safety: str = "DeclaredByPackage"
    captures_run_state: bool = False
    captured_secret_refs: list[str] = Field(default_factory=list)
    captured_path_refs: list[str] = Field(default_factory=list)
    semantic_closed: bool = True
    allowed_node_package_refs: list[ResourceRef] = Field(default_factory=list)
    max_nodes: int | None = None
    max_live_nodes: int | None = None
    package_digest: str = ""

    @model_validator(mode="after")
    def validate_orchestration_policy(self) -> "CapabilityPackageVersion":
        fields_set = bool(self.allowed_node_package_refs or self.max_nodes is not None or self.max_live_nodes is not None)
        if self.executor_kind != "orchestrator_python_v1":
            if fields_set:
                raise ValueError("orchestration_fields_require_orchestrator")
            return self
        if (
            not self.allowed_node_package_refs
            or self.max_nodes is None
            or self.max_live_nodes is None
            or self.max_nodes <= 0
            or self.max_live_nodes <= 0
            or self.max_live_nodes > self.max_nodes
            or self.executor_operation != "orchestrate"
            or self.io_contract_ref is None
        ):
            raise ValueError("orchestration_package_invalid")
        return self

    @property
    def version_ref(self) -> str:
        return f"capability-package://{self.package_id}/{self.package_version}"

    def model_post_init(self, __context: Any) -> None:
        if not self.package_digest:
            payload = self.model_dump(mode="json", exclude={"package_digest"})
            # Access bindings may contain machine-local paths; they must not
            # change the package identity or prevent activation on another
            # Slave.  The content digest remains the portable identity.
            if isinstance(payload.get("program_content_ref"), dict):
                ref = payload["program_content_ref"]
                payload["program_content_ref"] = {key: ref.get(key) for key in ("resource_id", "version_or_digest", "identity_criterion") if ref.get(key) is not None}
            if isinstance(payload.get("allowed_node_package_refs"), list):
                payload["allowed_node_package_refs"] = [
                    {key: ref.get(key) for key in ("resource_id", "version_or_digest", "identity_criterion") if ref.get(key) is not None}
                    if isinstance(ref, dict) else ref
                    for ref in payload["allowed_node_package_refs"]
                ]
            self.package_digest = hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
            ).hexdigest()


class CapabilityPackageActivation(ContractModel):
    package_version_ref: str
    target_slave: str
    activation_closure_version_ref: str
    compute_binding_ref: str
    evidence_refs: list[str] = Field(default_factory=list)
    activation_state: Literal[
        "not_installed", "provisioning", "ready", "degraded", "stopped", "failed", "lost"
    ] = "not_installed"
    runtime_profile: dict[str, Any] = Field(default_factory=dict)
    activation_digest: str = ""

    def model_post_init(self, __context: Any) -> None:
        if not self.activation_digest:
            payload = self.model_dump(mode="json", exclude={"activation_digest"})
            self.activation_digest = hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
            ).hexdigest()


class CapabilityPackage(ContractModel):
    package_id: str
    versions: list[CapabilityPackageVersion] = Field(default_factory=list)
    activations: list[CapabilityPackageActivation] = Field(default_factory=list)

    @property
    def latest_version(self) -> CapabilityPackageVersion | None:
        return self.versions[-1] if self.versions else None


class CapabilityProvisionCommand(ContractModel):
    command_id: str
    package_version_ref: str
    package_digest: str
    target_slave: str
    workspace_id: str = "workspace-default"
    activation_closure_version_ref: str = ""
    compute_binding: ComputeBinding | None = None
    program_content_ref: ResourceRef | None = None
    idempotency_key: str = ""
    session_generation: int = 1


class CapabilityHealthReport(ContractModel):
    report_id: str
    package_version_ref: str
    package_digest: str
    target_slave: str
    activation_state: Literal["ready", "degraded", "failed", "stopped"]
    evidence_refs: list[str] = Field(default_factory=list)
    details: dict[str, Any] = Field(default_factory=dict)
    session_generation: int = 1


class NodeIntent(ContractModel):
    """Deterministic node request emitted by an orchestration program."""

    intent_id: str = Field(min_length=1)
    execution_id: str = Field(min_length=1)
    package_ref: ResourceRef
    input_refs: list[ResourceRef] = Field(default_factory=list)


class DynamicNode(ContractModel):
    """Observer-accepted execution record derived from a NodeIntent."""

    node_id: str = Field(min_length=1)
    parent_execution_ref: str = Field(min_length=1)
    intent_id: str = Field(min_length=1)
    package_ref: ResourceRef
    package_digest: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    input_refs: list[ResourceRef] = Field(default_factory=list)
    state: Literal["accepted", "dispatched", "completed", "failed", "decision_required"] = "accepted"


class ResourceEventFrame(ContractModel):
    event_id: str
    resource_type: str = "capability_package_activation"
    resource_ref: str
    event_type: str
    package_version_ref: str | None = None
    package_digest: str | None = None
    target_slave: str | None = None
    session_generation: int = 1
    evidence_refs: list[str] = Field(default_factory=list)
    payload: dict[str, Any] = Field(default_factory=dict)


class NodeInputBinding(ContractModel):
    """Binding of one executable node to a content-addressed input."""

    node_id: str
    input_ref: ResourceRef
    provenance: list[dict[str, Any]] = Field(default_factory=list)


class TaskClosure(ContractModel):
    closure_id: str = "closure-default"
    schema_version: str = "1"
    data: DataApplication = Field(default_factory=DataApplication)
    data_systems: DataSystems = Field(default_factory=DataSystems)
    program: ProgramApplication = Field(default_factory=ProgramApplication)
    program_systems: ProgramSystems = Field(default_factory=ProgramSystems)
    node_input_bindings: list[NodeInputBinding] = Field(default_factory=list)
    compute: ComputeSpec = Field(default_factory=ComputeSpec)
    compute_bindings: list[ComputeBinding] = Field(default_factory=list)
    compute_application: ComputeApplication = Field(default_factory=ComputeApplication)
    compute_systems: ComputeSystems = Field(default_factory=ComputeSystems)
    terms: list[TypedTerm] = Field(default_factory=list)
    constraints: list[Constraint] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def minimal(cls, **kwargs: Any) -> "TaskClosure":
        return cls(**kwargs)

    def canonical_digest(self) -> str:
        payload = self.model_dump(mode="json", exclude_none=True)

        def strip_deployment_bindings(value: Any) -> Any:
            if isinstance(value, dict):
                return {
                    key: strip_deployment_bindings(item)
                    for key, item in value.items()
                    if key != "access_binding"
                }
            if isinstance(value, list):
                return [strip_deployment_bindings(item) for item in value]
            return value

        payload = json.dumps(strip_deployment_bindings(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        return hashlib.sha256(payload).hexdigest()


class ClosureContract(ContractModel):
    closure_id: str
    goal: str
    origin_conversation_ref: str = ""
    user_id: str = "user-default"
    workspace_id: str = "workspace-default"
    required_success_criteria: list[dict[str, Any]] = Field(default_factory=list)
    allowed_effects: list[str] = Field(default_factory=list)
    resource_budget: dict[str, Any] = Field(default_factory=dict)
    recovery_policy: dict[str, Any] = Field(default_factory=dict)
    result_expectations: list[dict[str, Any]] = Field(default_factory=list)
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
