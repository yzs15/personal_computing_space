"""Pydantic models for service-to-service HTTP request envelopes.

These models intentionally stop at the HTTP boundary.  They validate envelope
shape and nested contract models; workspace, lease, fencing, content and
runtime admission rules remain in the owning service.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .types import (
    CapabilityDeprovisionCommand,
    CapabilityHealthReport,
    CapabilityPackageVersion,
    CapabilityProvisionCommand,
    ComputeBinding,
    ContractModel,
    ClosureContract,
    ResourceRef,
    TaskClosure,
)


class AgentLeaseRequest(ContractModel):
    """Heartbeat/release envelope shared by Driver and Slave agents."""

    instance_id: str = Field(min_length=1)
    workspace_id: str = Field(min_length=1)
    lease_id: str = Field(min_length=1)
    epoch: int | None = None
    driver_epoch: int | None = None
    role: Literal["driver", "slave"] = "driver"


class PublicRequest(BaseModel):
    """Stable public envelope with the API's historical extra-field behavior."""

    model_config = ConfigDict(extra="ignore")


class ContentPutRequest(PublicRequest):
    content: Any | None = None
    media_type: Any = ""


class OpenRunRequest(PublicRequest):
    run_id: str | None = None
    task_ref: str | None = None
    goal: str | None = None
    closure_contract: ClosureContract | None = None
    allow_reassignment: bool = False
    user_id: str | None = None
    workspace_id: str | None = None


class PublicMessageRequest(PublicRequest):
    # Preserve the endpoint's domain-specific validation codes for missing,
    # blank, or wrongly typed values instead of replacing them with FastAPI's
    # generic request-validation envelope.
    text: Any = None
    request_id: Any = None
    conversation_ref: Any = None
    workspace_id: str | None = None


class PatchRequest(PublicRequest):
    base_draft_version: str | None = None
    base_snapshot_digest: str | None = None
    operation_id: str | None = None
    ops: list[dict[str, Any]] = Field(default_factory=list)


class CommitRequest(PublicRequest):
    draft_version: str | None = None
    draft_digest: str | None = None


class StartRequest(PublicRequest):
    closure_version: str | None = None


class ResolveRequest(PublicRequest):
    decision: str | None = None


class IdempotencyRequest(PublicRequest):
    idempotency_key: str | None = None


class CapabilityActivationRequest(PublicRequest):
    target_slaves: list[str] = Field(default_factory=list)
    approved_digest: str | None = None
    idempotency_key: str | None = None
    compute_binding: ComputeBinding | None = None


class CapabilityHealthRequest(ContractModel):
    """Lease-authenticated health report sent by a Slave."""

    agent_id: str = Field(min_length=1)
    instance_id: str = Field(min_length=1)
    lease_id: str = Field(min_length=1)
    epoch: int
    workspace_id: str = Field(min_length=1)
    report: CapabilityHealthReport


class DriverMessageRequest(ContractModel):
    text: Any = None
    request_id: Any = None
    conversation_ref: Any = None
    workspace_id: str | None = None


class WorkspaceRequest(ContractModel):
    workspace_id: str | None = None


class CapabilityTargetRequest(ContractModel):
    """Driver capability endpoint envelope.

    ``target_slave`` and ``package_version_ref`` remain accepted as the
    singular/legacy spelling at this boundary.  The endpoint resolves them to
    the canonical plural/current fields before invoking the service.
    """

    target_slaves: list[str] = Field(default_factory=list)
    target_slave: str | None = None
    package_ref: ResourceRef | str | None = None
    package_version_ref: ResourceRef | str | None = None
    compute_binding: ComputeBinding | None = None
    idempotency_key: str | None = None
    approved_digest: str | None = None

    @property
    def effective_target_slaves(self) -> list[str]:
        if self.target_slaves:
            return self.target_slaves
        return [self.target_slave] if self.target_slave else []

    @property
    def effective_package_ref(self) -> ResourceRef | str | None:
        return self.package_ref or self.package_version_ref


class SlaveDispatchRequest(ContractModel):
    attempt_id: str = Field(min_length=1)
    execution_id: str = Field(min_length=1)
    execution_epoch: int = 1
    workspace_id: str | None = None
    operation: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    closure: TaskClosure | None = None
    binding: ComputeBinding | None = None
    driver_id: str | None = None
    driver_epoch: int | None = None


class SlaveProvisionRequest(ContractModel):
    command: CapabilityProvisionCommand
    package: CapabilityPackageVersion
    driver_id: str | None = None
    driver_epoch: int | None = None


class SlaveDeprovisionRequest(ContractModel):
    command: CapabilityDeprovisionCommand
    package: CapabilityPackageVersion | None = None
    driver_id: str | None = None
    driver_epoch: int | None = None


__all__ = [
    "AgentLeaseRequest",
    "CapabilityHealthRequest",
    "CapabilityTargetRequest",
    "CapabilityActivationRequest",
    "CommitRequest",
    "ContentPutRequest",
    "DriverMessageRequest",
    "IdempotencyRequest",
    "OpenRunRequest",
    "PatchRequest",
    "PublicMessageRequest",
    "ResolveRequest",
    "SlaveDeprovisionRequest",
    "SlaveDispatchRequest",
    "SlaveProvisionRequest",
    "StartRequest",
    "WorkspaceRequest",
]
