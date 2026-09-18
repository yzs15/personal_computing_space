from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncEngine

from loom_v2.contracts.terms import TermSupport
from loom_v2.contracts.types import (
    CapabilityExport,
    CapabilityHealthReport,
    CapabilityDeprovisionCommand,
    CapabilityPackageActivation,
    CapabilityPackageVersion,
    CapabilityProvisionCommand,
    ComputeBinding,
    ExecutionContract,
    IoContract,
    ResourceRef,
    ResourceEventFrame,
    TaskClosure,
    ValidationEvidence,
)
from loom_v2.db.base import SlaveBase
from loom_v2.db.models import SlaveAttemptRow, SlaveReplicaRow
from loom_v2.db.migrations import apply_migrations
from loom_v2.db.session import make_session_factory
from loom_v2.settings import Settings

from .executor import ExecutionResult, ExecutorRegistry, default_registry
from .runtime_plugins import RuntimePluginError, RuntimePluginHost
from loom_v2.content_store import ContentStore
from loom_v2.digest import canonical_json_bytes, digest_bytes
from loom_v2.contracts.io_schema import ValidationError as SchemaValidationError, validate, validate_schema
from loom_v2.contracts.package_contracts import DRIVER_ORCHESTRATOR_KEY
from loom_v2.contracts.refs import operation_name


@dataclass
class WorkspaceReplica:
    workspace_id: str
    state: str = "ready"


@dataclass
class SlaveService:
    slave_id: str
    workspace_id: str = "workspace-default"
    replica: WorkspaceReplica = field(default_factory=lambda: WorkspaceReplica("workspace-default"))
    available: bool = True
    attempts: dict[str, ExecutionResult] = field(default_factory=dict)
    supported_operations: set[str] = field(default_factory=lambda: {"run_code"})
    engine: AsyncEngine | None = None
    content_store: ContentStore | None = None
    settings: Settings | None = None
    executor_registry: ExecutorRegistry = field(default_factory=lambda: default_registry)
    capability_operation_timeout_seconds: float | None = None
    package_cache: dict[str, CapabilityPackageVersion] = field(default_factory=dict)
    activations: dict[str, CapabilityPackageActivation] = field(default_factory=dict)
    resource_events: list[ResourceEventFrame] = field(default_factory=list)
    runtime_plugin_host: RuntimePluginHost | None = None
    _activation_locks: dict[str, asyncio.Lock] = field(default_factory=dict, init=False, repr=False)
    _pending_health_reports: dict[str, CapabilityHealthReport] = field(default_factory=dict, init=False, repr=False)
    _last_reported_health: dict[str, tuple[int, str]] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        self.settings = self.settings or Settings()
        self.sessions = make_session_factory(self.engine) if self.engine is not None else None
        if self.content_store is None:
            self.content_store = ContentStore.from_settings(self.settings)
        if self.runtime_plugin_host is None:
            self.runtime_plugin_host = RuntimePluginHost(
                plugin_dir=self.settings.runtime_plugin_dir,
                socket_dir=self.settings.runtime_plugin_socket_dir,
                startup_timeout=self.settings.runtime_plugin_startup_timeout_seconds,
                call_timeout=self.settings.runtime_plugin_call_timeout_seconds,
            )
        if self.capability_operation_timeout_seconds is not None and self.executor_registry is default_registry:
            self.executor_registry = ExecutorRegistry(capability_timeout_seconds=self.capability_operation_timeout_seconds)

    async def init_db(self) -> None:
        if self.engine is None:
            if self.runtime_plugin_host is not None:
                await self.runtime_plugin_host.start()
            return
        async with self.engine.begin() as connection:
            if connection.dialect.name == "postgresql":
                await apply_migrations(connection, "slave")
            else:
                # SQLite is used only by hermetic tests; production role-local
                # databases always use the ordered PostgreSQL migrations.
                await connection.run_sync(SlaveBase.metadata.create_all)
        async with self.sessions() as session:
            row = await session.get(SlaveReplicaRow, self.slave_id)
            if row is None:
                session.add(SlaveReplicaRow(slave_id=self.slave_id, workspace_id=self.workspace_id, state=self.replica.state))
                await session.commit()
            await self._load_persisted_activations(session)
        if self.runtime_plugin_host is not None:
            await self.runtime_plugin_host.start()
            await self.reconcile_activations()

    async def _load_persisted_activations(self, session: Any | None = None) -> None:
        """Restore package payloads and activation facts before reconciliation."""
        if self.sessions is None:
            return
        owns_session = session is None
        if owns_session:
            session = self.sessions()
            session = await session.__aenter__()
        try:
            row = await session.get(SlaveReplicaRow, self.slave_id)
            for item in (row.capability_activations if row is not None else []):
                try:
                    package = CapabilityPackageVersion.model_validate(item["package_payload"])
                    activation = CapabilityPackageActivation(
                        package_version_ref=item["package_version_ref"],
                        package_digest=item["package_digest"],
                        target_slave=item.get("slave_id", self.slave_id),
                        activation_closure_version_ref=item.get("activation_closure_version_ref", ""),
                        compute_binding_ref=item.get("compute_binding_ref", ""),
                        desired_state=item["desired_state"],
                        activation_revision=int(item["activation_revision"]),
                        last_idempotency_key=str(item["last_idempotency_key"]),
                        activation_state=item.get("activation_state", "provisioning"),
                        evidence_refs=list(item.get("evidence_refs") or []),
                        runtime_profile=dict(item.get("runtime_profile") or {}),
                    )
                except (TypeError, ValueError):
                    continue
                self._cache_package(package, package.version_ref)
                self.activations[self._activation_key(package.version_ref)] = activation
        finally:
            if owns_session:
                await session.__aexit__(None, None, None)

    async def _persist_activation(
        self,
        package: CapabilityPackageVersion,
        activation: CapabilityPackageActivation,
        *,
        runtime_plugin_id: str = "",
        runtime_handle: dict[str, Any] | None = None,
        last_error_code: str = "",
    ) -> None:
        if self.sessions is None:
            return
        key = self._activation_key(activation.package_version_ref)
        now = datetime.now(timezone.utc)
        values = {
            "activation_key": key,
            "workspace_id": self.workspace_id,
            "slave_id": self.slave_id,
            "package_version_ref": activation.package_version_ref,
            "package_digest": activation.package_digest.lower(),
            "package_payload": package.model_dump(mode="json"),
            "runtime_plugin_id": runtime_plugin_id,
            "desired_state": activation.desired_state,
            "activation_state": activation.activation_state,
            "activation_closure_version_ref": activation.activation_closure_version_ref,
            "compute_binding_ref": activation.compute_binding_ref,
            "activation_revision": activation.activation_revision,
            "evidence_refs": list(activation.evidence_refs),
            "runtime_profile": dict(activation.runtime_profile),
            "runtime_handle": dict(runtime_handle or {}),
            "last_idempotency_key": activation.last_idempotency_key,
            "last_error_code": last_error_code,
            "updated_at": now.isoformat(),
        }
        async with self.sessions() as session:
            row = await session.get(SlaveReplicaRow, self.slave_id)
            if row is None:
                row = SlaveReplicaRow(slave_id=self.slave_id, workspace_id=self.workspace_id, state=self.replica.state, capability_activations=[])
                session.add(row)
            records = [dict(item) for item in (row.capability_activations or []) if item.get("activation_key") != key]
            records.append(values)
            row.capability_activations = records
            await session.commit()

    async def reconcile_activations(self) -> dict[str, Any]:
        """Reconcile persisted desired state after plugin/Slave startup."""
        if self.runtime_plugin_host is None or not self.activations:
            return {}
        entries: list[dict[str, Any]] = []
        packages: dict[str, CapabilityPackageVersion] = {}
        if self.sessions is not None:
            async with self.sessions() as session:
                replica_row = await session.get(SlaveReplicaRow, self.slave_id)
                for row in (replica_row.capability_activations if replica_row is not None else []):
                    package_payload = row.get("package_payload") or {}
                    try:
                        package = CapabilityPackageVersion.model_validate(package_payload)
                    except (TypeError, ValueError):
                        continue
                    # Runtime plugins own long-lived activations. One-shot
                    # executors do not need reconciliation.
                    if not self._uses_runtime_plugin(package):
                        continue
                    activation_key = self._activation_key(package.version_ref)
                    packages[activation_key] = package
                    entries.append({
                        "activation_key": activation_key,
                        "workspace_id": row.get("workspace_id", self.workspace_id),
                        "slave_id": row.get("slave_id", self.slave_id),
                        "target_slave": row.get("slave_id", self.slave_id),
                        "package_version_ref": package.version_ref,
                        "package_digest": package.package_digest,
                        "package_type": package_payload.get("package_type"),
                        "execution": package_payload.get("execution"),
                        "package_payload": package_payload,
                        "desired_state": row["desired_state"],
                        "activation_revision": row["activation_revision"],
                        "runtime_profile": row.get("runtime_profile") or {},
                    })
        else:
            for key, activation in self.activations.items():
                package = self._find_cached_package(activation.package_version_ref, activation.package_digest)
                if package is None or not self._uses_runtime_plugin(package):
                    continue
                entries.append({"activation_key": key, "workspace_id": self.workspace_id, "slave_id": self.slave_id, "target_slave": self.slave_id, "package_version_ref": package.version_ref, "package_digest": package.package_digest, "package_type": package.package_type, "execution": package.execution.model_dump(mode="json"), "package_payload": package.model_dump(mode="json"), "desired_state": activation.desired_state, "activation_revision": activation.activation_revision, "runtime_profile": activation.runtime_profile})
        try:
            response = await self.runtime_plugin_host.reconcile(entries)
        except RuntimePluginError:
            # Treat a provider that cannot be reached as a lost runtime
            # observation.  Desired state remains running in persistence, so
            # a later provider restart can reconcile it again.
            response = {}
        for entry in entries:
            key = entry["activation_key"]
            activation = self.activations.get(key)
            if activation is None:
                continue
            item = next((item for group in response.values() if isinstance(group, dict) for item in group.get("activations", []) if item.get("activation_key") == key), None)
            if item is None:
                # A persisted activation whose execution contract no longer
                # has a loaded provider is no longer safely runnable.  Mark
                # it lost instead of leaving a stale ready projection.
                if entry.get("desired_state", "running") != "stopped":
                    updated = activation.model_copy(update={"activation_state": "lost"})
                    self.activations[key] = updated
                    package = packages.get(key) or self._find_cached_package(updated.package_version_ref, updated.package_digest)
                    if package is not None:
                        await self._persist_activation(package, updated, runtime_plugin_id="", last_error_code="runtime_plugin_not_found")
                continue
            state = str(item.get("activation_state") or ("ready" if (item.get("inspect") or {}).get("running") else activation.activation_state))
            update_fields: dict[str, Any] = {"activation_state": state if state in {"ready", "degraded", "failed", "stopped", "lost"} else activation.activation_state}
            if isinstance(item.get("runtime_profile"), dict):
                update_fields["runtime_profile"] = dict(item["runtime_profile"])
            updated = activation.model_copy(update=update_fields)
            self.activations[key] = updated
            package = packages.get(key) or self._find_cached_package(updated.package_version_ref, updated.package_digest)
            if package is not None:
                await self._persist_activation(package, updated, runtime_plugin_id=str((item or {}).get("runtime_plugin_id") or ""))
        return response

    async def close(self) -> None:
        if self.runtime_plugin_host is not None:
            await self.runtime_plugin_host.close()

    def term_support(self) -> list[TermSupport]:
        return [
            TermSupport(kind="loom.compute.capability.v1", schema_ref="loom.compute.capability/1", support={"parse", "preserve", "match", "validate", "enforce"}, execution_stages={"commit", "admission", "execute"}),
            TermSupport(kind="loom.compute.precision.v1", schema_ref="loom.compute.precision/1", support={"parse", "preserve", "validate"}, execution_stages={"commit", "admission"}),
        ]

    @staticmethod
    def _package_cache_key(package_ref: str, package_digest: str) -> str:
        return f"{package_ref}#digest:{package_digest.lower()}"

    @staticmethod
    def _activation_key(package_ref: str) -> str:
        return package_ref

    @staticmethod
    def _package_aliases(package: CapabilityPackageVersion, command_ref: str) -> set[str]:
        # Lifecycle and dispatch use one canonical coordinate. ``command_ref``
        # is retained in this helper only because callers cache immediately
        # after validating it equals ``package.version_ref``.
        return {command_ref, package.version_ref}

    def _cache_package(self, package: CapabilityPackageVersion, command_ref: str) -> None:
        aliases = self._package_aliases(package, command_ref)
        for alias in aliases:
            self.package_cache[self._package_cache_key(alias, package.package_digest)] = package
            self.package_cache[alias] = package

    def _find_cached_package(self, package_ref: str, package_digest: str | None = None) -> CapabilityPackageVersion | None:
        if package_digest:
            requested_digest = package_digest.lower()
            exact = self.package_cache.get(self._package_cache_key(package_ref, requested_digest))
            if exact is not None and exact.package_digest.lower() == requested_digest:
                return exact
            for item in self.package_cache.values():
                if item.package_digest.lower() != requested_digest:
                    continue
                if package_ref == item.version_ref:
                    return item
            return None
        return self.package_cache.get(package_ref)

    def capability_snapshot(self) -> set[str]:
        operations = set(self.supported_operations)
        for activation in self.activations.values():
            if activation.desired_state != "running" or activation.activation_state != "ready":
                continue
            package = self._find_cached_package(
                activation.package_version_ref, activation.package_digest
            )
            if package is None:
                continue
            operations.update(
                operation_name(item.capability_descriptor_ref.resource_id)
                for item in package.capability_exports
            )
        return operations

    @staticmethod
    def _lifecycle_retry(
        existing: CapabilityPackageActivation | None,
        *,
        desired_state: str,
        package_digest: str,
        activation_revision: int,
        idempotency_key: str,
    ) -> bool:
        if existing is None:
            return False
        if existing.package_digest.lower() != package_digest.lower():
            raise RuntimeError("capability_package_identity_conflict")
        if activation_revision < existing.activation_revision:
            raise RuntimeError("stale_activation_revision")
        if activation_revision == existing.activation_revision:
            if existing.desired_state != desired_state:
                raise RuntimeError("activation_revision_conflict")
            # A package activation is immutable and may be reused by a later
            # Run.  Observer revisions are scoped to that Run, while the
            # Slave keeps the ready/stopped projection across Runs.  Once the
            # requested terminal state is already established, a different
            # per-Run idempotency key is therefore a safe replay rather than
            # a conflicting command.  Non-terminal states remain fenced so a
            # stale command cannot overwrite an in-flight lifecycle change.
            terminal_state = (
                existing.activation_state == "ready"
                if desired_state == "running"
                else existing.activation_state == "stopped"
            )
            if existing.last_idempotency_key != idempotency_key and not terminal_state:
                raise RuntimeError("activation_revision_conflict")
            return True
        return False

    def _accept_activation_revision(self, activation_key: str, *, retry: bool) -> None:
        if retry:
            return
        self._pending_health_reports.pop(activation_key, None)

    def _remember_health_report(
        self, activation_key: str, report: CapabilityHealthReport
    ) -> CapabilityHealthReport:
        """Record a synchronous lifecycle report as already observed.

        ``provision``/``deprovision`` return their health report to the
        caller synchronously.  The background health loop must therefore not
        emit a second report for the same revision and state before any
        actual runtime transition occurs.
        """
        self._last_reported_health[activation_key] = (
            report.activation_revision,
            report.activation_state,
        )
        return report

    @staticmethod
    def _selected_export(
        package: CapabilityPackageVersion,
        binding: ComputeBinding | None,
    ) -> CapabilityExport:
        if binding is None:
            raise RuntimeError("capability_descriptor_binding_required")
        try:
            return package.export_for(binding.capability_descriptor_ref)
        except ValueError as exc:
            raise RuntimeError("capability_descriptor_binding_mismatch") from exc

    @staticmethod
    def _body_ref(package: CapabilityPackageVersion, field_name: str) -> ResourceRef:
        try:
            return ResourceRef.model_validate(package.body[field_name])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(f"{field_name}_required") from exc

    def _uses_runtime_plugin(self, package: CapabilityPackageVersion) -> bool:
        if (
            package.package_type,
            package.execution.kind,
            package.execution.version,
        ) == DRIVER_ORCHESTRATOR_KEY:
            return False
        return not self.executor_registry.supports(package.package_type, package.execution)

    async def _validate_package_exports(self, package: CapabilityPackageVersion) -> None:
        for capability_export in package.capability_exports:
            contract = await self._load_io_contract(capability_export.io_contract_ref)
            for schema_ref in (contract.input_schema_ref, contract.output_schema_ref):
                if schema_ref is None:
                    continue
                try:
                    schema = json.loads(await self.content_store.get(schema_ref))
                    validate_schema(schema)
                except (FileNotFoundError, TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise RuntimeError("io_contract_invalid") from exc
            if contract.success_validator_ref is not None:
                try:
                    await self.content_store.get(contract.success_validator_ref)
                except (FileNotFoundError, ValueError) as exc:
                    raise RuntimeError("io_contract_invalid") from exc

    async def _validate_input_payload(
        self,
        capability_export: CapabilityExport,
        payload: dict[str, Any],
    ) -> None:
        contract = await self._load_io_contract(capability_export.io_contract_ref)
        if contract.input_schema_ref is None:
            return
        try:
            schema = json.loads(await self.content_store.get(contract.input_schema_ref))
            validate_schema(schema)
            errors = validate(schema, payload)
        except (FileNotFoundError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError("payload_schema_mismatch") from exc
        if errors:
            raise RuntimeError("payload_schema_mismatch")

    async def _provision_runtime_locked(
        self,
        command: CapabilityProvisionCommand,
        package: CapabilityPackageVersion,
    ) -> CapabilityHealthReport:
        if self.runtime_plugin_host is None:
            raise RuntimeError("runtime_plugin_not_found")
        if package.package_digest.lower() != command.package_digest.lower():
            raise RuntimeError("capability_package_digest_mismatch")
        await self._validate_package_exports(package)
        ref = package.version_ref
        for item in self.activations.values():
            if item.package_version_ref == ref and item.package_digest.lower() != package.package_digest.lower():
                # A package coordinate is immutable.  Never start a second
                # container for the same coordinate under another digest.
                raise RuntimeError("capability_package_identity_conflict")
        activation_key = self._activation_key(ref)
        existing = self.activations.get(activation_key)
        retry = self._lifecycle_retry(
            existing,
            desired_state="running",
            package_digest=package.package_digest,
            activation_revision=command.activation_revision,
            idempotency_key=command.idempotency_key,
        )
        self._accept_activation_revision(activation_key, retry=retry)
        if retry and existing is not None and existing.activation_state == "ready":
            return self._remember_health_report(activation_key, CapabilityHealthReport(
                report_id=f"health-{uuid4().hex}",
                package_version_ref=ref,
                package_digest=package.package_digest,
                target_slave=self.slave_id,
                activation_state="ready",
                activation_revision=command.activation_revision,
                evidence_refs=list(existing.evidence_refs),
                details={"idempotent": True},
            ))
        # Write desired-running before touching the runtime.  A crash between
        # this write and plugin completion is recovered by startup reconcile.
        values = {
            "activation_closure_version_ref": command.activation_closure_version_ref
            or package.package_closure_version_ref,
            "compute_binding_ref": command.compute_binding.binding_id
            if command.compute_binding
            else "",
            "desired_state": "running",
            "activation_revision": command.activation_revision,
            "last_idempotency_key": command.idempotency_key,
            "activation_state": "provisioning",
        }
        pending = (
            existing.model_copy(update=values)
            if existing is not None
            else CapabilityPackageActivation(
                package_version_ref=ref,
                package_digest=package.package_digest,
                target_slave=self.slave_id,
                evidence_refs=[],
                runtime_profile={},
                **values,
            )
        )
        self._cache_package(package, command.package_version_ref)
        self.activations[activation_key] = pending
        await self._persist_activation(package, pending)
        try:
            response = await self.runtime_plugin_host.provision(package, command)
        except (RuntimePluginError, RuntimeError) as exc:
            code = getattr(exc, "code", None) or str(exc) or "runtime_plugin_unavailable"
            failed = pending.model_copy(update={"activation_state": "failed"})
            self.activations[activation_key] = failed
            await self._persist_activation(package, failed, last_error_code=code)
            raise RuntimeError(code) from exc
        state = str(response.get("activation_state") or response.get("state") or "ready")
        if state not in {"ready", "degraded", "failed"}:
            raise RuntimeError("runtime_plugin_protocol_error")
        activation = pending.model_copy(
            update={
                "activation_state": state,
                "evidence_refs": [
                    str(item)
                    for item in response.get("evidence_refs", [])
                    if item is not None
                ],
                "runtime_profile": dict(response.get("runtime_profile") or {}),
            }
        )
        self.activations[activation_key] = activation
        await self._persist_activation(
            package,
            activation,
            runtime_plugin_id=str(response.get("runtime_plugin_id") or ""),
            runtime_handle=dict(response.get("runtime_handle") or {}),
            last_error_code=str((response.get("details") or {}).get("code") or "") if state == "failed" else "",
        )
        report = CapabilityHealthReport(
            report_id=f"health-{uuid4().hex}",
            package_version_ref=ref,
            package_digest=package.package_digest,
            target_slave=self.slave_id,
            activation_state=state,
            activation_revision=command.activation_revision,
            evidence_refs=activation.evidence_refs,
            details={
                "runtime_plugin_id": str(response.get("runtime_plugin_id") or ""),
                "package_version_ref": ref,
                "package_digest": package.package_digest,
                **dict(response.get("details") or {}),
            },
        )
        self.resource_events.append(
            ResourceEventFrame(
                event_id=f"resource-{report.report_id}",
                resource_ref=ref,
                event_type="activation_ready" if state == "ready" else f"activation_{state}",
                package_version_ref=ref,
                package_digest=package.package_digest,
                target_slave=self.slave_id,
                evidence_refs=report.evidence_refs,
            )
        )
        return self._remember_health_report(activation_key, report)

    async def provision(self, command: CapabilityProvisionCommand, package: CapabilityPackageVersion | None = None) -> CapabilityHealthReport:
        if command.target_slave != self.slave_id:
            raise RuntimeError("provision_target_mismatch")
        if command.compute_binding is not None and command.compute_binding.target_resource_ref.resource_id != self.slave_id:
            raise RuntimeError("binding_target_mismatch")
        package = package or self._find_cached_package(command.package_version_ref, command.package_digest)
        if package is None:
            raise RuntimeError("capability_package_not_found")
        try:
            package = CapabilityPackageVersion.model_validate(package.model_dump(mode="json"))
        except (TypeError, ValueError) as exc:
            raise RuntimeError("capability_package_invalid") from exc
        if command.package_version_ref != package.version_ref:
            raise RuntimeError("capability_package_ref_mismatch")
        if (
            package.package_type,
            package.execution.kind,
            package.execution.version,
        ) == DRIVER_ORCHESTRATOR_KEY:
            raise RuntimeError("driver_side_executor_required")
        activation_key = self._activation_key(package.version_ref)
        lock = self._activation_locks.setdefault(activation_key, asyncio.Lock())
        async with lock:
            if self._uses_runtime_plugin(package):
                return await self._provision_runtime_locked(command, package)
            return await self._provision_executor_locked(command, package)

    async def _provision_executor_locked(
        self,
        command: CapabilityProvisionCommand,
        package: CapabilityPackageVersion,
    ) -> CapabilityHealthReport:
        try:
            await self._validate_package_exports(package)
        except (FileNotFoundError, RuntimeError, ValueError, TypeError) as exc:
            raise RuntimeError("io_contract_invalid") from exc
        if package.package_digest.lower() != command.package_digest.lower():
            raise RuntimeError("capability_package_digest_mismatch")
        provider_fillable = list(package.body.get("provider_fillable_hole_refs") or [])
        if provider_fillable and (command.compute_binding is None or not command.compute_binding.runtime_profile):
            raise RuntimeError("provider_fillable_binding_required")
        ref = package.version_ref
        activation_key = self._activation_key(ref)
        existing_activation = self.activations.get(activation_key)
        retry = self._lifecycle_retry(
            existing_activation,
            desired_state="running",
            package_digest=package.package_digest,
            activation_revision=command.activation_revision,
            idempotency_key=command.idempotency_key,
        )
        self._accept_activation_revision(activation_key, retry=retry)
        if retry and existing_activation is not None and existing_activation.activation_state == "ready":
            return self._remember_health_report(activation_key, CapabilityHealthReport(
                report_id=f"health-{uuid4().hex}",
                package_version_ref=ref,
                package_digest=package.package_digest,
                target_slave=self.slave_id,
                activation_state="ready",
                activation_revision=command.activation_revision,
                evidence_refs=list(existing_activation.evidence_refs),
                details={"idempotent": True},
            ))
        pending_values = {
            "activation_closure_version_ref": command.activation_closure_version_ref
            or package.package_closure_version_ref,
            "compute_binding_ref": command.compute_binding.binding_id
            if command.compute_binding
            else "",
            "desired_state": "running",
            "activation_revision": command.activation_revision,
            "last_idempotency_key": command.idempotency_key,
            "activation_state": "provisioning",
        }
        pending = (
            existing_activation.model_copy(update=pending_values)
            if existing_activation is not None
            else CapabilityPackageActivation(
                package_version_ref=ref,
                package_digest=package.package_digest,
                target_slave=self.slave_id,
                evidence_refs=[],
                runtime_profile={},
                **pending_values,
            )
        )
        self._cache_package(package, command.package_version_ref)
        self.activations[activation_key] = pending
        await self._persist_activation(package, pending)
        program_ref = self._body_ref(package, "program_content_ref")
        stat = await self.content_store.stat(program_ref)
        if stat is None or not stat.integrity_verified or stat.declared_digest != program_ref.digest:
            failed = pending.model_copy(update={"activation_state": "failed"})
            self.activations[activation_key] = failed
            await self._persist_activation(
                package, failed, last_error_code="program_content_unavailable"
            )
            raise RuntimeError("program_content_unavailable")
        activation = pending.model_copy(
            update={
                "activation_state": "ready",
                "evidence_refs": [
                    f"package-test:{package.package_digest[:16]}",
                    f"health:{package.package_digest[:16]}",
                ],
            }
        )
        self.activations[activation_key] = activation
        await self._persist_activation(package, activation)
        operation_name = package.capability_exports[0].capability_descriptor_ref.resource_id
        report = CapabilityHealthReport(
            report_id=f"health-{uuid4().hex}",
            package_version_ref=ref,
            package_digest=package.package_digest,
            target_slave=self.slave_id,
            activation_state="ready",
            activation_revision=command.activation_revision,
            evidence_refs=activation.evidence_refs,
            details={
                "operation": operation_name,
                "execution": package.execution.model_dump(mode="json"),
                "executor_descriptor_ref": self.executor_registry.get_for(
                    package.package_type, package.execution
                ).descriptor.descriptor_ref,
                "package_version_ref": package.version_ref,
                "package_digest": package.package_digest,
            },
        )
        self.resource_events.append(ResourceEventFrame(event_id=f"resource-{report.report_id}", resource_ref=ref, event_type="activation_ready", package_version_ref=ref, package_digest=package.package_digest, target_slave=self.slave_id, evidence_refs=report.evidence_refs))
        return self._remember_health_report(activation_key, report)

    async def deprovision(
        self,
        command: CapabilityDeprovisionCommand,
        package: CapabilityPackageVersion | None = None,
    ) -> CapabilityHealthReport:
        if command.target_slave != self.slave_id:
            raise RuntimeError("provision_target_mismatch")
        package = package or self._find_cached_package(command.package_version_ref, command.package_digest)
        if package is None:
            raise RuntimeError("capability_package_not_found")
        try:
            package = CapabilityPackageVersion.model_validate(package.model_dump(mode="json"))
        except (TypeError, ValueError) as exc:
            raise RuntimeError("capability_package_invalid") from exc
        if command.package_version_ref != package.version_ref:
            raise RuntimeError("capability_package_ref_mismatch")
        if not self._uses_runtime_plugin(package):
            raise RuntimeError("unsupported_deprovision_contract")
        lock = self._activation_locks.setdefault(self._activation_key(package.version_ref), asyncio.Lock())
        async with lock:
            return await self._deprovision_locked(command, package)

    async def _deprovision_locked(
        self,
        command: CapabilityDeprovisionCommand,
        package: CapabilityPackageVersion,
    ) -> CapabilityHealthReport:
        if package.package_digest.lower() != command.package_digest.lower():
            raise RuntimeError("capability_package_digest_mismatch")
        if self.runtime_plugin_host is None:
            raise RuntimeError("runtime_plugin_not_found")
        ref = package.version_ref
        activation_key = self._activation_key(ref)
        existing = self.activations.get(activation_key)
        retry = self._lifecycle_retry(
            existing,
            desired_state="stopped",
            package_digest=package.package_digest,
            activation_revision=command.activation_revision,
            idempotency_key=command.idempotency_key,
        )
        self._accept_activation_revision(activation_key, retry=retry)
        evidence_refs = (
            list(existing.evidence_refs)
            if existing is not None
            else [f"deprovision:{package.package_digest[:16]}"]
        )
        desired_values = {
            "desired_state": "stopped",
            "activation_revision": command.activation_revision,
            "last_idempotency_key": command.idempotency_key,
        }
        desired = (
            existing.model_copy(update=desired_values)
            if existing is not None
            else CapabilityPackageActivation(
                package_version_ref=ref,
                package_digest=package.package_digest,
                target_slave=self.slave_id,
                activation_closure_version_ref=package.package_closure_version_ref,
                compute_binding_ref="",
                activation_state="not_installed",
                evidence_refs=evidence_refs,
                runtime_profile={},
                **desired_values,
            )
        )
        self._cache_package(package, command.package_version_ref)
        self.activations[activation_key] = desired
        await self._persist_activation(package, desired)
        if retry and existing is not None and existing.activation_state == "stopped":
            return self._remember_health_report(activation_key, CapabilityHealthReport(
                report_id=f"health-{uuid4().hex}",
                package_version_ref=ref,
                package_digest=package.package_digest,
                target_slave=self.slave_id,
                activation_state="stopped",
                activation_revision=command.activation_revision,
                evidence_refs=evidence_refs,
                details={"idempotent": True},
            ))
        # Promotion may create a second lifecycle coordinate for the same
        # immutable package digest while the original activation is still
        # serving.  Keep the shared runtime alive when stopping only one
        # coordinate; the remaining activation owns the same digest-backed
        # container.
        shared_ready = any(
            item is not existing
            and item.package_digest.lower() == package.package_digest.lower()
            and item.desired_state == "running"
            and item.activation_state in {"ready", "degraded", "provisioning"}
            for item in self.activations.values()
        )
        if shared_ready:
            activation = desired.model_copy(update={"activation_state": "stopped"})
            self.activations[activation_key] = activation
            await self._persist_activation(package, activation)
            report = CapabilityHealthReport(
                report_id=f"health-{uuid4().hex}",
                package_version_ref=ref,
                package_digest=package.package_digest,
                target_slave=self.slave_id,
                activation_state="stopped",
                activation_revision=command.activation_revision,
                evidence_refs=evidence_refs,
                details={"shared_runtime_preserved": True},
            )
            return self._remember_health_report(activation_key, report)
        try:
            response = await self.runtime_plugin_host.deprovision(package, command)
        except (RuntimePluginError, RuntimeError) as exc:
            code = getattr(exc, "code", None) or str(exc) or "runtime_plugin_unavailable"
            await self._persist_activation(package, desired, last_error_code=code)
            raise RuntimeError(code) from exc
        activation = desired.model_copy(update={"activation_state": "stopped"})
        self.activations[activation_key] = activation
        await self._persist_activation(
            package,
            activation,
            runtime_plugin_id=str(response.get("runtime_plugin_id") or ""),
        )
        report = CapabilityHealthReport(
            report_id=f"health-{uuid4().hex}",
            package_version_ref=ref,
            package_digest=package.package_digest,
            target_slave=self.slave_id,
            activation_state="stopped",
            activation_revision=command.activation_revision,
            evidence_refs=evidence_refs,
            details={"runtime_plugin_id": str(response.get("runtime_plugin_id") or ""), "idempotent": bool(response.get("idempotent", False))},
        )
        self.resource_events.append(
            ResourceEventFrame(
                event_id=f"resource-{report.report_id}",
                resource_ref=ref,
                event_type="activation_stopped",
                package_version_ref=ref,
                package_digest=package.package_digest,
                target_slave=self.slave_id,
                evidence_refs=evidence_refs,
            )
        )
        return self._remember_health_report(activation_key, report)

    async def inspect_activation_health(self) -> list[CapabilityHealthReport]:
        reports: list[CapabilityHealthReport] = []
        for activation_key, pending in list(self._pending_health_reports.items()):
            lock = self._activation_locks.setdefault(activation_key, asyncio.Lock())
            async with lock:
                activation = self.activations.get(activation_key)
                if (
                    activation is None
                    or activation.desired_state != "running"
                    or activation.activation_revision != pending.activation_revision
                ):
                    self._pending_health_reports.pop(activation_key, None)
                    continue
                reports.append(pending)

        for activation_key in list(self.activations):
            lock = self._activation_locks.setdefault(activation_key, asyncio.Lock())
            async with lock:
                activation = self.activations.get(activation_key)
                if activation is None or (
                    activation_key in self._pending_health_reports
                    or activation.desired_state != "running"
                ):
                    continue
                package = self._find_cached_package(
                    activation.package_version_ref, activation.package_digest
                )
                if package is None or not self._uses_runtime_plugin(package):
                    continue

                details: dict[str, Any] = {}
                if activation.activation_state == "failed":
                    state = "failed"
                elif self.runtime_plugin_host is None:
                    state = "failed"
                    details["code"] = "runtime_plugin_not_found"
                else:
                    try:
                        inspected = await self.runtime_plugin_host.inspect(
                            package,
                            {
                                **activation.model_dump(mode="json"),
                                "workspace_id": self.workspace_id,
                                "slave_id": self.slave_id,
                            },
                        )
                        details["inspect"] = inspected
                        if inspected.get("identity_match") and inspected.get("healthy"):
                            state = "ready"
                        elif inspected.get("exists") and inspected.get("running"):
                            state = "degraded"
                        else:
                            state = "failed"
                    except (RuntimePluginError, RuntimeError) as exc:
                        state = "failed"
                        details["code"] = getattr(exc, "code", None) or str(exc)

                health = (activation.activation_revision, state)
                if self._last_reported_health.get(activation_key) == health:
                    continue
                if state != activation.activation_state:
                    activation = activation.model_copy(update={"activation_state": state})
                    self.activations[activation_key] = activation
                    await self._persist_activation(package, activation)
                report = CapabilityHealthReport(
                    report_id=f"health-{uuid4().hex}",
                    package_version_ref=activation.package_version_ref,
                    package_digest=activation.package_digest,
                    target_slave=self.slave_id,
                    activation_state=state,
                    activation_revision=activation.activation_revision,
                    evidence_refs=list(activation.evidence_refs),
                    details=details,
                )
                self._pending_health_reports[activation_key] = report
                reports.append(report)
        return reports

    def acknowledge_health_report(self, report_id: str) -> None:
        for activation_key, report in list(self._pending_health_reports.items()):
            if report.report_id != report_id:
                continue
            self._last_reported_health[activation_key] = (
                report.activation_revision,
                report.activation_state,
            )
            self._pending_health_reports.pop(activation_key, None)
            return

    async def _load_io_contract(self, ref: ResourceRef) -> IoContract:
        raw = await self.content_store.get(ref)
        try:
            return IoContract.model_validate(json.loads(raw))
        except (TypeError, json.JSONDecodeError, ValueError) as exc:
            raise RuntimeError("io_contract_invalid") from exc

    async def _admission_payload(
        self,
        payload: dict[str, Any],
        *,
        operation: str,
        closure: TaskClosure | None,
        capability_export: CapabilityExport | None,
    ) -> dict[str, Any]:
        """Resolve and validate the immutable input binding at the Slave.

        Driver-provided payloads are deliberately ignored when a closure has
        a ``NodeInputBinding``.  The binding is the execution input authority;
        the Slave rereads it from the shared ContentStore so a mutated
        dispatch envelope cannot bypass readiness validation.
        """

        if closure is None or not closure.node_input_bindings:
            return payload
        operation_ref = closure.program.operation_ref or closure.compute.operation_ref
        operation_name = operation_ref.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1] if operation_ref else operation
        candidates = {operation_ref, operation_name, operation, "default"}
        input_binding = next((item for item in closure.node_input_bindings if item.node_id in candidates), None)
        if input_binding is None:
            raise RuntimeError("input_binding_missing")

        contract_ref = capability_export.io_contract_ref if capability_export is not None else closure.program.io_contract_ref
        contract = await self._load_io_contract(contract_ref) if contract_ref is not None else None
        try:
            raw = await self.content_store.get(input_binding.input_ref)
            value = json.loads(raw)
        except (FileNotFoundError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError("payload_schema_mismatch") from exc

        if contract is not None and contract.input_schema_ref is not None:
            try:
                schema = json.loads(await self.content_store.get(contract.input_schema_ref))
                validate_schema(schema)
                errors = validate(schema, value)
            except (FileNotFoundError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError("payload_schema_mismatch") from exc
            if errors:
                raise RuntimeError("payload_schema_mismatch")
        return value if isinstance(value, dict) else {"value": value}

    @staticmethod
    def _evidence(
        *,
        attempt_id: str,
        execution_epoch: int,
        schema_ref: ResourceRef | None,
        validator_ref: ResourceRef | None,
        input_digest: str | None,
        output_digest: str | None,
        result: str,
        errors: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        evidence = ValidationEvidence(
            evidence_id=f"evidence-{attempt_id}-{uuid4().hex[:10]}",
            attempt_id=attempt_id,
            execution_epoch=execution_epoch,
            validator_ref=validator_ref,
            schema_ref=schema_ref,
            input_digest=input_digest,
            output_digest=output_digest,
            result=result,
            errors=list(errors or []),
            issuer="slave",
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        return evidence.model_dump(mode="json")

    async def _validate_terminal_result(
        self,
        result: ExecutionResult,
        *,
        attempt_id: str,
        execution_epoch: int,
        operation: str = "",
        closure: TaskClosure | None,
        capability_export: CapabilityExport | None,
    ) -> ExecutionResult:
        contract_ref = capability_export.io_contract_ref if capability_export is not None else closure.program.io_contract_ref if closure is not None else None
        if contract_ref is None:
            return result
        contract = await self._load_io_contract(contract_ref)
        input_digest = None
        if closure is not None and closure.node_input_bindings:
            operation_ref = closure.program.operation_ref or closure.compute.operation_ref
            candidates = {operation_ref, operation_ref.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1], "default"}
            input_binding = next((item for item in closure.node_input_bindings if item.node_id in candidates), None)
            input_digest = input_binding.input_ref.digest if input_binding is not None else None
        evidence: list[dict[str, Any]] = []
        if contract.output_schema_ref is not None:
            try:
                schema_raw = await self.content_store.get(contract.output_schema_ref)
                schema = json.loads(schema_raw)
                validate_schema(schema)
                errors = validate(schema, result.value)
            except (FileNotFoundError, TypeError, ValueError, json.JSONDecodeError) as exc:
                errors = [
                    SchemaValidationError(
                        path="$",
                        keyword="schema",
                        message="output schema could not be evaluated",
                        expected="valid output schema",
                        observed=str(exc),
                    )
                ]
            if errors:
                failed = self._evidence(
                    attempt_id=attempt_id,
                    execution_epoch=execution_epoch,
                    schema_ref=contract.output_schema_ref,
                    validator_ref=contract.success_validator_ref,
                    input_digest=input_digest,
                    output_digest=result.digest,
                    result="fail",
                    errors=[item.model_dump(mode="json") for item in errors],
                )
                return replace(
                    result,
                    terminal_state="failed",
                    terminal_error={"code": "output_schema_mismatch", "errors": failed["errors"]},
                    validation_evidence=[failed],
                )
            evidence.append(
                self._evidence(
                    attempt_id=attempt_id,
                    execution_epoch=execution_epoch,
                    schema_ref=contract.output_schema_ref,
                    validator_ref=contract.success_validator_ref,
                    input_digest=input_digest,
                    output_digest=result.digest,
                    result="pass",
                )
            )
        if contract.success_validator_ref is not None:
            plugin = await self.content_store.get(contract.success_validator_ref)
            plugin_input = result.value if isinstance(result.value, dict) else {"value": result.value}
            plugin_result = await self.executor_registry.invoke_for(
                "function",
                ExecutionContract(kind="process:json_stdio", version="1"),
                plugin_input,
                program=plugin,
            )
            payload = plugin_result.value if isinstance(plugin_result.value, dict) else {}
            plugin_status = payload.get("result")
            plugin_errors = payload.get("errors") if isinstance(payload.get("errors"), list) else []
            plugin_evidence = self._evidence(
                attempt_id=attempt_id,
                execution_epoch=execution_epoch,
                schema_ref=contract.output_schema_ref,
                validator_ref=contract.success_validator_ref,
                input_digest=input_digest,
                output_digest=result.digest,
                result="pass" if plugin_status == "pass" else "fail",
                errors=plugin_errors,
            )
            evidence.append(plugin_evidence)
            if plugin_status != "pass":
                return replace(
                    result,
                    terminal_state="failed",
                    terminal_error={"code": "success_validation_failed", "errors": plugin_errors},
                    validation_evidence=evidence,
                )
        elif contract.success_semantics is not None:
            return replace(
                result,
                terminal_state="decision_required",
                terminal_error={"code": "attestation_required"},
                validation_evidence=evidence,
            )
        return replace(result, validation_evidence=evidence)

    async def run(
        self,
        attempt_id: str,
        operation: str,
        payload: dict[str, Any],
        *,
        closure: TaskClosure | None = None,
        binding: ComputeBinding | None = None,
        execution_epoch: int = 1,
    ) -> ExecutionResult:
        if not self.available or self.replica.state != "ready":
            raise RuntimeError("slave_unavailable")
        package: CapabilityPackageVersion | None = None
        package_required = False
        if binding is not None and binding.capability_package_ref is not None:
            package_ref = binding.capability_package_ref.resource_id
            digest = binding.capability_package_ref.digest
            package = self._find_cached_package(package_ref, digest)
            if package is None:
                raise RuntimeError("capability_package_not_installed")
        elif operation not in self.supported_operations:
            raise RuntimeError(f"capability_unavailable:{operation}")
        else:
            # Application operations are materialized as content-addressed
            # capability packages.  There is no in-process builtin fallback.
            package_required = True
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
        if package_required:
            raise RuntimeError("capability_package_required")
        if package is not None:
            try:
                # Cached package objects contain mutable JSON collections.
                # Reconstruct at the dispatch boundary so an in-process
                # mutation cannot bypass contract or digest validation.
                package = CapabilityPackageVersion.model_validate(
                    package.model_dump(mode="json")
                )
            except (TypeError, ValueError) as exc:
                raise RuntimeError("capability_package_invalid") from exc
        capability_export = self._selected_export(package, binding) if package is not None else None
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
                        terminal_state=result_payload.get("terminal_state", "completed"),
                        terminal_error=result_payload.get("terminal_error"),
                        validation_evidence=result_payload.get("validation_evidence", []),
                        provenance=result_payload.get("provenance"),
                    )
                    self.attempts[attempt_id] = result
                    return result
        payload = await self._admission_payload(payload, operation=operation, closure=closure, capability_export=capability_export)
        if capability_export is not None:
            await self._validate_input_payload(capability_export, payload)
        if package is not None and self._uses_runtime_plugin(package):
            activation_key = self._activation_key(package.version_ref)
            activation = self.activations.get(activation_key)
            if activation is None or activation.activation_state != "ready":
                raise RuntimeError("capability_activation_not_ready")
            assert capability_export is not None
            if self.runtime_plugin_host is None:
                raise RuntimeError("runtime_plugin_not_found")
            try:
                activation_payload = activation.model_dump(mode="json")
                # Workspace/Slave identity is role-local context rather than
                # part of CapabilityPackageActivation's portable contract.
                # The runtime plugin still needs it to verify managed
                # container labels before issuing an HTTP request.
                activation_payload.update({"workspace_id": self.workspace_id, "slave_id": self.slave_id})
                response = await self.runtime_plugin_host.invoke(
                    package,
                    capability_export,
                    payload,
                    activation=activation_payload,
                    attempt_id=attempt_id,
                    deadline_seconds=self.capability_operation_timeout_seconds,
                )
            except (RuntimePluginError, RuntimeError) as exc:
                raise RuntimeError(getattr(exc, "code", None) or str(exc) or "runtime_plugin_unavailable") from exc
            if "value" not in response:
                raise RuntimeError("runtime_plugin_protocol_error")
            value = response["value"]
            result = ExecutionResult(
                resource_ref=ResourceRef(
                    resource_id=f"content://sha256/{digest_bytes(canonical_json_bytes(value))}",
                    identity_criterion="content_digest",
                ),
                value=value,
                replay_safety=str(response.get("replay_safety") or capability_export.replay_safety),
                terminal_state=str(response.get("terminal_state") or "completed"),
                terminal_error=response.get("terminal_error"),
                provenance={
                    "package_version_ref": package.version_ref,
                    "package_digest": package.package_digest,
                    "capability_descriptor_ref": capability_export.capability_descriptor_ref.model_dump(mode="json"),
                    "execution": package.execution.model_dump(mode="json"),
                    "runtime_plugin_id": str(response.get("runtime_plugin_id") or ""),
                    **dict(response.get("provenance") or {}),
                },
            )
        elif package is not None:
            program_ref = self._body_ref(package, "program_content_ref")
            program = await self.content_store.get(program_ref, expected_digest=program_ref.digest)
            adapter = self.executor_registry.get_for(package.package_type, package.execution)
            result = await adapter.invoke(payload, program=program)
            descriptor = adapter.descriptor
            result = replace(
                result,
                provenance={
                    "package_version_ref": package.version_ref,
                    "package_digest": package.package_digest,
                    "program_content_ref": program_ref.model_dump(mode="json"),
                    "execution": package.execution.model_dump(mode="json"),
                    "executor_descriptor_ref": descriptor.descriptor_ref,
                },
            )
        else:
            # The no-package branch is rejected above; keep this guard local
            # to the execution boundary in case future callers bypass it.
            raise RuntimeError("capability_package_required")
        result = await self._validate_terminal_result(
            result,
            attempt_id=attempt_id,
            execution_epoch=execution_epoch,
            operation=operation,
            closure=closure,
            capability_export=capability_export,
        )
        # Every execution result is a content-addressed JSON artifact.  The
        # executor/runtime may calculate a digest for its local protocol, but
        # the shared ContentStore is the authoritative source of the public
        # ResourceRef.  Persisting here covers both ordinary executors and
        # runtime-plugin invocations (including container:http).
        if self.content_store is None:
            raise RuntimeError("content_store_unavailable")
        result_ref = await self.content_store.put(
            canonical_json_bytes(result.value),
            media_type="application/json",
        )
        result = replace(result, resource_ref=result_ref)
        self.attempts[attempt_id] = result
        if self.sessions is not None:
            async with self.sessions() as session:
                session.add(SlaveAttemptRow(
                    attempt_id=attempt_id,
                    slave_id=self.slave_id,
                    workspace_id=self.workspace_id,
                    operation=operation,
                    payload=payload,
                    state=result.terminal_state,
                    result={
                        "resource_ref": result.resource_ref.model_dump(mode="json"),
                        "value": result.value,
                        "replay_safety": result.replay_safety,
                        "terminal_state": result.terminal_state,
                        "terminal_error": result.terminal_error,
                        "validation_evidence": result.validation_evidence,
                        "provenance": result.provenance,
                    },
                ))
                await session.commit()
        return result
