from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .constraints import Constraint, ConstraintRef
from .terms import TypedTerm
from loom_v2.digest import digest_json


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ResourceRef(ContractModel):
    resource_id: str
    version_or_digest: str | None = None
    access_binding: dict[str, Any] = Field(default_factory=dict)
    identity_criterion: str | None = None
    provenance: list[dict[str, Any]] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_identity(self) -> "ResourceRef":
        if self.resource_id.startswith("content://sha256/") and self.version_or_digest is not None:
            raise ValueError("redundant_content_digest")
        return self

    @property
    def digest(self) -> str | None:
        if self.resource_id.startswith("content://sha256/"):
            return self.resource_id.removeprefix("content://sha256/").lower()
        return self.version_or_digest.lower() if self.version_or_digest else None


class DataApplication(ContractModel):
    logical_inputs: list[ResourceRef] = Field(default_factory=list)
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
    # These fields remain the v2 application view.  Executable validation is
    # anchored by ``io_contract_ref``; the repository will resolve the view
    # from that content-addressed contract instead of trusting free text.
    input_schema: Any | None = None
    output_schema: Any | None = None
    success_semantics: Any | None = None
    io_contract_ref: ResourceRef | None = None
    terms: list[TypedTerm] = Field(default_factory=list)


class ExecutionContract(ContractModel):
    """How a capability package is started and invoked."""

    kind: str = "process:json_stdio"
    version: str = "1"


class ProgramSystems(ContractModel):
    execution: ExecutionContract = Field(default_factory=ExecutionContract)
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
            self.requirement_id = "requirement-" + digest_json(self.key, domain="loom/requirement/v1")[:12]


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
    constraint_evidence_refs: list[str] = Field(default_factory=list)
    bound_by: str = "driver"
    bound_at: str = ""
    runtime_profile: dict[str, Any] = Field(default_factory=dict)
    activation_ref: str | None = None


class CapabilityPackageBody(ContractModel):
    """Marker base for package-specific bodies."""


class FunctionCapabilityPackageBody(CapabilityPackageBody):
    operation_descriptor_ref: ResourceRef | str
    program_content_ref: ResourceRef
    io_contract_ref: ResourceRef | None = None
    effective_constraint_refs: list[ConstraintRef] = Field(default_factory=list)
    provider_fillable_hole_refs: list[str] = Field(default_factory=list)
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

    @property
    def program_digest(self) -> str:
        return self.program_content_ref.digest or ""


class HttpServiceEndpoint(ContractModel):
    endpoint_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]{0,63}$")
    operation_descriptor_ref: ResourceRef
    path: str
    io_contract_ref: ResourceRef
    effect_class: str
    permissions: list[str] = Field(default_factory=list)
    replay_safety: str

    @model_validator(mode="after")
    def validate_refs(self) -> "HttpServiceEndpoint":
        descriptor_digest = self.operation_descriptor_ref.digest
        if self.operation_descriptor_ref.identity_criterion != "descriptor_digest" or not _is_sha256(descriptor_digest):
            raise ValueError("service_operation_descriptor_invalid")
        if (
            self.io_contract_ref.identity_criterion not in {None, "content_digest"}
            or not self.io_contract_ref.resource_id.startswith("content://sha256/")
            or not _is_sha256(self.io_contract_ref.digest)
        ):
            raise ValueError("service_io_contract_ref_invalid")
        _validate_http_path(self.path, "service_endpoint_path_invalid")
        return self


class ServiceCapabilityPackageBody(CapabilityPackageBody):
    """Strict body for a long-lived, multi-endpoint HTTP capability service."""

    image_ref: str
    container_port: int = Field(ge=1, le=65535)
    health_path: str
    endpoints: list[HttpServiceEndpoint] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_service(self) -> "ServiceCapabilityPackageBody":
        if not re.fullmatch(r"[^@/]+(?:/[^@/]+)*/[^@/]+@sha256:[0-9a-f]{64}", self.image_ref):
            raise ValueError("service_image_ref_invalid")
        _validate_http_path(self.health_path, "service_health_path_invalid")
        ids = [endpoint.endpoint_id for endpoint in self.endpoints]
        paths = [endpoint.path for endpoint in self.endpoints]
        descriptors = [endpoint.operation_descriptor_ref.digest for endpoint in self.endpoints]
        if len(set(ids)) != len(ids) or len(set(paths)) != len(paths) or len(set(descriptors)) != len(descriptors):
            raise ValueError("service_endpoint_duplicate")
        if self.health_path in paths:
            raise ValueError("service_health_endpoint_conflict")
        return self


def _is_sha256(value: str | None) -> bool:
    return bool(value and re.fullmatch(r"[0-9a-fA-F]{64}", value))


def _validate_http_path(value: str, error: str) -> None:
    # v1 paths are origin-form paths only; query, fragment, host and scheme
    # are not part of an endpoint identity.
    if not value.startswith("/") or "?" in value or "#" in value or "://" in value:
        raise ValueError(error)


class PythonModuleCapabilityPackageBody(CapabilityPackageBody):
    module_content_ref: ResourceRef
    exported_symbols: list[str] = Field(default_factory=list)
    dependency_manifest: dict[str, Any] = Field(default_factory=dict)


class CapabilityPackageVersion(ContractModel):
    """Immutable package envelope with a type-specific body.

    New wire data uses ``package_type``, ``execution`` and ``body``.  The body
    is owned by the package type and is intentionally not flattened into this
    envelope.
    """

    package_type: str = "function"
    package_id: str
    package_version: str
    package_closure_version_ref: str
    source_run_ref: str
    source_closure_version_ref: str
    scope: Literal["run_bound", "workspace_reusable"] = "run_bound"
    publication_state: Literal["candidate", "published", "abandoned"] = "candidate"
    provenance: list[dict[str, Any]] = Field(default_factory=list)
    execution: ExecutionContract = Field(default_factory=ExecutionContract)
    body: FunctionCapabilityPackageBody | ServiceCapabilityPackageBody | PythonModuleCapabilityPackageBody
    package_digest: str = ""

    @model_validator(mode="before")
    @classmethod
    def parse_body(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        body_types = {
            "function": FunctionCapabilityPackageBody,
            "service": ServiceCapabilityPackageBody,
            "module": PythonModuleCapabilityPackageBody,
        }
        package_type = value.get("package_type", "function")
        if package_type not in body_types:
            raise ValueError(f"unsupported_package_type:{package_type}")
        if "body" in value:
            return {**value, "body": body_types[package_type].model_validate(value["body"])}
        return value

    @model_validator(mode="after")
    def validate_orchestration_policy(self) -> "CapabilityPackageVersion":
        if self.package_type == "service":
            if self.execution.kind != "container:http" or self.execution.version != "1":
                raise ValueError("unsupported_execution_contract")
            if not isinstance(self.body, ServiceCapabilityPackageBody):
                raise ValueError("service_package_body_invalid")
            return self
        if not isinstance(self.body, FunctionCapabilityPackageBody):
            return self
        body = self.function_body
        fields_set = bool(body.allowed_node_package_refs or body.max_nodes is not None or body.max_live_nodes is not None)
        if self.execution.kind != "container:python_orchestrator":
            if fields_set:
                raise ValueError("orchestration_fields_require_orchestrator")
            return self
        if (
            not body.allowed_node_package_refs
            or body.max_nodes is None
            or body.max_live_nodes is None
            or body.max_nodes <= 0
            or body.max_live_nodes <= 0
            or body.max_live_nodes > body.max_nodes
            or body.io_contract_ref is None
        ):
            raise ValueError("orchestration_package_invalid")
        return self

    @property
    def function_body(self) -> FunctionCapabilityPackageBody:
        if not isinstance(self.body, FunctionCapabilityPackageBody):
            raise TypeError(f"package_type_has_no_function_body:{self.package_type}")
        return self.body

    @property
    def version_ref(self) -> str:
        return f"capability-package://{self.package_id}/{self.package_version}"

    def model_post_init(self, __context: Any) -> None:
        payload = self._identity_payload()
        computed = digest_json(payload, domain="loom/package/v1")
        if self.package_digest:
            declared = self.package_digest.lower()
            if len(declared) != 64 or any(char not in "0123456789abcdef" for char in declared):
                raise ValueError("invalid_package_digest")
            if declared != computed:
                raise ValueError("package_digest_mismatch")
            self.package_digest = declared
        else:
            self.package_digest = computed

    def _identity_payload(self) -> dict[str, Any]:
        """Immutable execution identity; lifecycle/provenance are excluded."""
        payload = self.model_dump(mode="json", include={"package_type", "execution", "body"})

        def normalize(value: Any) -> Any:
            if isinstance(value, dict):
                # ResourceRef carries deployment-local access/provenance data;
                # only its immutable identity belongs in package_digest.
                if "resource_id" in value:
                    return {
                        key: value[key]
                        for key in ("resource_id", "version_or_digest", "identity_criterion")
                        if value.get(key) is not None
                    }
                return {key: normalize(item) for key, item in value.items()}
            if isinstance(value, list):
                return [normalize(item) for item in value]
            return value

        return normalize(payload)


class CapabilityPackageActivation(ContractModel):
    package_version_ref: str
    package_digest: str
    target_slave: str
    activation_closure_version_ref: str
    compute_binding_ref: str
    session_generation: int = 1
    evidence_refs: list[str] = Field(default_factory=list)
    activation_state: Literal[
        "not_installed", "provisioning", "ready", "degraded", "stopped", "failed", "lost"
    ] = "not_installed"
    runtime_profile: dict[str, Any] = Field(default_factory=dict)


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
    input_refs: list[ResourceRef] = Field(default_factory=list)
    state: Literal["accepted", "dispatched", "completed", "failed", "decision_required"] = "accepted"

    @property
    def package_digest(self) -> str | None:
        return self.package_ref.digest


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

        return digest_json(strip_deployment_bindings(payload), domain="loom/closure/v1")


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


class Execution(ContractModel):
    execution_id: str
    closure_version_ref: str
    state: str = "created"
    execution_epoch: int = 1
