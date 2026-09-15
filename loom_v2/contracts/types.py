from __future__ import annotations

import json
import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from .constraints import Constraint, ConstraintRef
from .package_contracts import package_contract_registry
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
        if (
            self.resource_id.startswith("content://sha256/")
            and self.version_or_digest is not None
            and self.identity_criterion != "descriptor_digest"
        ):
            raise ValueError("redundant_content_digest")
        return self

    @property
    def digest(self) -> str | None:
        if self.identity_criterion == "descriptor_digest" and self.version_or_digest:
            return self.version_or_digest.lower()
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


class CapabilityExport(ContractModel):
    """Runtime-neutral operation projection exported by a package."""

    capability_descriptor_ref: ResourceRef
    io_contract_ref: ResourceRef
    effect_class: str
    permissions: list[str] = Field(default_factory=list)
    replay_safety: str
    runtime_binding: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_refs(self) -> "CapabilityExport":
        descriptor_digest = self.capability_descriptor_ref.version_or_digest
        if (
            not self.capability_descriptor_ref.resource_id
            or self.capability_descriptor_ref.identity_criterion != "descriptor_digest"
            or not _is_sha256(descriptor_digest)
        ):
            raise ValueError("capability_descriptor_invalid")
        if (
            self.io_contract_ref.identity_criterion != "content_digest"
            or not re.fullmatch(
                r"content://sha256/[0-9a-f]{64}", self.io_contract_ref.resource_id
            )
            or self.io_contract_ref.version_or_digest is not None
        ):
            raise ValueError("capability_io_contract_ref_invalid")
        if len(self.permissions) != len(set(self.permissions)):
            raise ValueError("capability_permissions_duplicate")
        return self


def _is_sha256(value: str | None) -> bool:
    return bool(value and re.fullmatch(r"[0-9a-f]{64}", value))


class CapabilityPackageVersion(ContractModel):
    """Immutable runtime-neutral package envelope."""

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
    capability_exports: list[CapabilityExport] = Field(min_length=1)
    body: dict[str, JsonValue]
    package_digest: str = ""

    @model_validator(mode="after")
    def validate_contract(self) -> "CapabilityPackageVersion":
        descriptor_keys = [
            (item.capability_descriptor_ref.resource_id, item.capability_descriptor_ref.digest)
            for item in self.capability_exports
        ]
        if len(descriptor_keys) != len(set(descriptor_keys)):
            raise ValueError("capability_export_duplicate")
        package_contract_registry.validate(
            (self.package_type, self.execution.kind, self.execution.version),
            self.model_dump(
                mode="json",
                include={"package_type", "execution", "capability_exports", "body"},
            ),
        )
        return self

    def export_for(self, descriptor_ref: ResourceRef) -> CapabilityExport:
        # Descriptor refs are a distinct identity class.  Do not accept a
        # bare resource id or a digest carried under another criterion just
        # because its derived ``digest`` happens to match an export.  This is
        # the boundary that keeps a dynamic node bound to the exact public
        # capability export selected by the caller.
        if (
            descriptor_ref.identity_criterion != "descriptor_digest"
            or not descriptor_ref.resource_id
            or not _is_sha256(descriptor_ref.version_or_digest)
        ):
            raise ValueError("capability_descriptor_invalid")
        matches = [
            item
            for item in self.capability_exports
            if item.capability_descriptor_ref.resource_id == descriptor_ref.resource_id
            and item.capability_descriptor_ref.identity_criterion
            == descriptor_ref.identity_criterion
            and item.capability_descriptor_ref.version_or_digest
            == descriptor_ref.version_or_digest
        ]
        if len(matches) != 1:
            raise ValueError("capability_export_not_found")
        return matches[0]

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
        payload = self.model_dump(mode="json", include={"package_type", "execution", "capability_exports", "body"})
        resource_ref_fields = {
            "resource_id",
            "version_or_digest",
            "access_binding",
            "identity_criterion",
            "provenance",
        }

        def normalize(value: Any, key: str | None = None) -> Any:
            if isinstance(value, dict):
                # ResourceRef carries deployment-local access/provenance data;
                # only its immutable identity belongs in package_digest.
                # Do not infer a ref from ``resource_id`` alone when the
                # object also has contract-specific fields: a plugin body is
                # allowed to use that ordinary property name, and discarding
                # its siblings would create digest collisions.
                if "resource_id" in value and set(value) <= resource_ref_fields:
                    resource_id = str(value.get("resource_id") or "")
                    version_or_digest = value.get("version_or_digest")
                    if resource_id.startswith("content://sha256/"):
                        resource_id = resource_id.lower()
                        # A content-backed operation descriptor has two
                        # distinct hashes: the ContentStore object identity
                        # in resource_id and the domain-separated descriptor
                        # identity in version_or_digest. Preserve both.
                        if value.get("identity_criterion") == "descriptor_digest":
                            version_or_digest = (
                                str(version_or_digest).lower()
                                if version_or_digest is not None
                                else None
                            )
                        else:
                            version_or_digest = None
                    elif isinstance(version_or_digest, str) and re.fullmatch(r"[0-9a-fA-F]{64}", version_or_digest):
                        version_or_digest = version_or_digest.lower()
                    return {
                        "resource_id": resource_id,
                        **({"version_or_digest": version_or_digest} if version_or_digest is not None else {}),
                        **({"identity_criterion": str(value["identity_criterion"]).lower()} if value.get("identity_criterion") is not None else {}),
                    }
                return {item_key: normalize(item, item_key) for item_key, item in value.items()}
            if isinstance(value, list):
                normalized = [normalize(item, key) for item in value]
                # Capability exports and common permission lists have set
                # semantics. Package-specific body arrays retain the order
                # declared by their contract.
                if key == "capability_exports":
                    normalized.sort(
                        key=lambda item: (
                            str((item.get("capability_descriptor_ref") or {}).get("resource_id", "")),
                            str((item.get("capability_descriptor_ref") or {}).get("version_or_digest", "")),
                        )
                        if isinstance(item, dict)
                        else str(item)
                    )
                elif key in {
                    "permissions",
                    "provider_fillable_hole_refs",
                    "allowed_node_package_refs",
                    "captured_secret_refs",
                    "captured_path_refs",
                    "effective_constraint_refs",
                }:
                    normalized.sort(key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
                return normalized
            return value

        return normalize(payload)


class CapabilityPackageActivation(ContractModel):
    package_version_ref: str
    package_digest: str
    target_slave: str
    activation_closure_version_ref: str
    compute_binding_ref: str
    desired_state: Literal["running", "stopped"]
    activation_revision: int = Field(ge=1)
    last_idempotency_key: str
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
    idempotency_key: str = Field(min_length=1)
    activation_revision: int = Field(ge=1)


class CapabilityDeprovisionCommand(ContractModel):
    command_id: str
    package_version_ref: str
    package_digest: str
    target_slave: str
    workspace_id: str = "workspace-default"
    idempotency_key: str = Field(min_length=1)
    activation_revision: int = Field(ge=1)


class CapabilityHealthReport(ContractModel):
    report_id: str
    package_version_ref: str
    package_digest: str
    target_slave: str
    activation_state: Literal["ready", "degraded", "failed", "stopped"]
    evidence_refs: list[str] = Field(default_factory=list)
    details: dict[str, Any] = Field(default_factory=dict)
    activation_revision: int = Field(ge=1)


class NodeIntent(ContractModel):
    """Deterministic node request emitted by an orchestration program."""

    intent_id: str = Field(min_length=1)
    execution_id: str = Field(min_length=1)
    package_ref: ResourceRef
    capability_descriptor_ref: ResourceRef
    input_refs: list[ResourceRef] = Field(default_factory=list)


class DynamicNode(ContractModel):
    """Observer-accepted execution record derived from a NodeIntent."""

    node_id: str = Field(min_length=1)
    parent_execution_ref: str = Field(min_length=1)
    intent_id: str = Field(min_length=1)
    package_ref: ResourceRef
    capability_descriptor_ref: ResourceRef
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
