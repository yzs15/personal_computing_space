from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine

from loom_v2.contracts.terms import TermSupport
from loom_v2.contracts.types import (
    CapabilityHealthReport,
    CapabilityPackageActivation,
    CapabilityPackageVersion,
    CapabilityProvisionCommand,
    ComputeBinding,
    ResourceRef,
    ResourceEventFrame,
    TaskClosure,
)
from loom_v2.db.base import SlaveBase
from loom_v2.db.models import SlaveAttemptRow, SlaveReplicaRow
from loom_v2.db.session import make_session_factory
from loom_v2.settings import Settings

from .executor import ExecutionResult, ExecutorRegistry, default_registry
from loom_v2.content_store import ContentStore


@dataclass
class WorkspaceReplica:
    workspace_id: str
    state: str = "ready"
    digest: str = ""


@dataclass
class SlaveService:
    slave_id: str
    workspace_id: str = "workspace-default"
    replica: WorkspaceReplica = field(default_factory=lambda: WorkspaceReplica("workspace-default"))
    available: bool = True
    attempts: dict[str, ExecutionResult] = field(default_factory=dict)
    supported_operations: set[str] = field(default_factory=lambda: {"echo", "hash", "sort", "run_code"})
    engine: AsyncEngine | None = None
    content_store: ContentStore | None = None
    executor_registry: ExecutorRegistry = field(default_factory=lambda: default_registry)
    capability_operation_timeout_seconds: float | None = None
    package_cache: dict[str, CapabilityPackageVersion] = field(default_factory=dict)
    activations: dict[str, CapabilityPackageActivation] = field(default_factory=dict)
    resource_events: list[ResourceEventFrame] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.sessions = make_session_factory(self.engine) if self.engine is not None else None
        if self.content_store is None:
            settings = Settings()
            self.content_store = ContentStore.from_settings(settings)
        if self.capability_operation_timeout_seconds is not None and self.executor_registry is default_registry:
            self.executor_registry = ExecutorRegistry(capability_timeout_seconds=self.capability_operation_timeout_seconds)

    async def init_db(self) -> None:
        if self.engine is None:
            return
        async with self.engine.begin() as connection:
            await connection.run_sync(SlaveBase.metadata.create_all)
        async with self.sessions() as session:
            row = await session.get(SlaveReplicaRow, self.slave_id)
            if row is None:
                session.add(SlaveReplicaRow(slave_id=self.slave_id, workspace_id=self.workspace_id, state=self.replica.state, digest=self.replica.digest))
                await session.commit()

    def term_support(self) -> list[TermSupport]:
        return [
            TermSupport(kind="loom.compute.capability.v1", schema_ref="loom.compute.capability/1", support={"parse", "preserve", "match", "validate", "enforce"}, execution_stages={"commit", "admission", "execute"}),
            TermSupport(kind="loom.compute.precision.v1", schema_ref="loom.compute.precision/1", support={"parse", "preserve", "validate"}, execution_stages={"commit", "admission"}),
        ]

    async def provision(self, command: CapabilityProvisionCommand, package: CapabilityPackageVersion | None = None) -> CapabilityHealthReport:
        if command.target_slave != self.slave_id:
            raise RuntimeError("provision_target_mismatch")
        if command.compute_binding is not None and command.compute_binding.target_resource_ref.resource_id != self.slave_id:
            raise RuntimeError("binding_target_mismatch")
        package = package or self.package_cache.get(command.package_version_ref)
        if package is None:
            raise RuntimeError("capability_package_not_found")
        if package.package_digest != command.package_digest:
            raise RuntimeError("capability_package_digest_mismatch")
        if command.program_content_ref is not None and command.program_content_ref.version_or_digest not in {None, package.program_digest}:
            raise RuntimeError("program_digest_mismatch")
        if package.provider_fillable_hole_refs and (command.compute_binding is None or not command.compute_binding.runtime_profile):
            raise RuntimeError("provider_fillable_binding_required")
        ref = package.version_ref
        existing_activation = self.activations.get(ref)
        if existing_activation is not None and existing_activation.activation_state == "ready":
            return CapabilityHealthReport(
                report_id=f"health-{package.package_id}-{self.slave_id}",
                package_version_ref=ref,
                package_digest=package.package_digest,
                target_slave=self.slave_id,
                activation_state="ready",
                evidence_refs=list(existing_activation.evidence_refs),
                details={"idempotent": True},
                session_generation=command.session_generation,
            )
        stat = await self.content_store.stat(package.program_content_ref)
        if stat is None or not stat.integrity_verified or stat.declared_digest != package.program_digest:
            raise RuntimeError("program_content_unavailable")
        activation = CapabilityPackageActivation(
            package_version_ref=ref,
            target_slave=self.slave_id,
            activation_closure_version_ref=command.activation_closure_version_ref or package.package_closure_version_ref,
            compute_binding_ref=command.compute_binding.binding_id if command.compute_binding else "",
            activation_state="ready",
            evidence_refs=[f"package-test:{package.package_digest[:16]}", f"health:{package.package_digest[:16]}"],
        )
        self.package_cache[ref] = package
        self.package_cache[f"capability-package://{package.package_id}/{package.package_version}"] = package
        self.package_cache[command.package_version_ref] = package
        # Bindings may reference the package by package_id, version ref, or
        # digest; keep every alias resolvable so a lenient lookup in ``run``
        # mirrors the Observer's ``_find_package`` matching.
        self.package_cache[package.package_id] = package
        self.package_cache[package.package_digest] = package
        self.package_cache[package.program_digest] = package
        self.activations[ref] = activation
        operation_ref = package.operation_descriptor_ref
        operation_name = operation_ref.resource_id if isinstance(operation_ref, ResourceRef) else str(operation_ref)
        if package.scope == "workspace_reusable" and package.publication_state == "published":
            self.supported_operations.add(operation_name.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1])
        report = CapabilityHealthReport(
            report_id=f"health-{package.package_id}-{self.slave_id}",
            package_version_ref=ref,
            package_digest=package.package_digest,
            target_slave=self.slave_id,
            activation_state="ready",
            evidence_refs=activation.evidence_refs,
            details={"operation": operation_name, "executor_kind": package.executor_kind},
            session_generation=command.session_generation,
        )
        self.resource_events.append(ResourceEventFrame(event_id=f"resource-{report.report_id}", resource_ref=ref, event_type="activation_ready", package_version_ref=ref, package_digest=package.package_digest, target_slave=self.slave_id, evidence_refs=report.evidence_refs))
        return report

    async def run(
        self,
        attempt_id: str,
        operation: str,
        payload: dict[str, Any],
        *,
        closure: TaskClosure | None = None,
        binding: ComputeBinding | None = None,
    ) -> ExecutionResult:
        if not self.available or self.replica.state != "ready":
            raise RuntimeError("slave_unavailable")
        package: CapabilityPackageVersion | None = None
        if binding is not None and binding.capability_package_ref is not None:
            package_ref = binding.capability_package_ref.resource_id
            digest = binding.capability_package_ref.version_or_digest
            package = self.package_cache.get(package_ref) or (self.package_cache.get(digest) if digest else None)
            if package is None:
                package = next(
                    (
                        item
                        for item in self.package_cache.values()
                        if item.package_id == package_ref
                        or item.version_ref == package_ref
                        or f"{item.package_id}:{item.package_version}" == package_ref
                        or (digest and digest in {item.package_digest, item.program_digest})
                    ),
                    None,
                )
            if package is None:
                raise RuntimeError("capability_package_not_installed")
            if binding.realization_digest and binding.realization_digest not in {package.program_digest, package.package_digest}:
                raise RuntimeError("capability_package_digest_mismatch")
        elif operation not in self.supported_operations:
            raise RuntimeError(f"capability_unavailable:{operation}")
        if closure is not None:
            operation_ref = closure.program.operation_ref or closure.compute.operation_ref
            expected_operation = operation_ref.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1] if operation_ref else ""
            if expected_operation and expected_operation != operation:
                raise RuntimeError("operation_binding_mismatch")
            bindings = {item.hole_id: item for item in closure.compute_bindings}
            for hole in closure.compute.typed_holes:
                if hole.status != "bound" or not hole.binding_ref:
                    raise RuntimeError(f"typed_hole_unbound:{hole.hole_id}")
                bound = bindings.get(hole.hole_id)
                if bound is None or bound.binding_id != hole.binding_ref:
                    raise RuntimeError(f"compute_binding_missing:{hole.hole_id}")
            if binding is not None and binding.target_resource_ref.resource_id != self.slave_id:
                raise RuntimeError("binding_target_mismatch")
        if attempt_id in self.attempts:
            return self.attempts[attempt_id]
        if self.sessions is not None:
            async with self.sessions() as session:
                row = await session.get(SlaveAttemptRow, attempt_id)
                if row is not None and row.result is not None:
                    result_payload = row.result
                    result = ExecutionResult(
                        resource_ref=ResourceRef.model_validate(result_payload["resource_ref"]),
                        value=result_payload["value"],
                        replay_safety=result_payload["replay_safety"],
                        digest=result_payload["digest"],
                    )
                    self.attempts[attempt_id] = result
                    return result
        if package is not None:
            program = await self.content_store.get(package.program_content_ref, expected_digest=package.program_digest)
            result = await self.executor_registry.execute(package.executor_kind, package.executor_operation, payload, program=program)
        else:
            result = await self.executor_registry.execute("builtin_v1", operation, payload)
        self.attempts[attempt_id] = result
        if self.sessions is not None:
            async with self.sessions() as session:
                session.add(SlaveAttemptRow(
                    attempt_id=attempt_id,
                    slave_id=self.slave_id,
                    workspace_id=self.workspace_id,
                    operation=operation,
                    payload=payload,
                    state="completed",
                    result={
                        "resource_ref": result.resource_ref.model_dump(mode="json"),
                        "value": result.value,
                        "replay_safety": result.replay_safety,
                        "digest": result.digest,
                    },
                ))
                await session.commit()
        return result
