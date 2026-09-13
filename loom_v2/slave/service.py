from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy import inspect

from loom_v2.contracts.terms import TermSupport
from loom_v2.contracts.types import (
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
from loom_v2.db.session import make_session_factory
from loom_v2.settings import Settings

from .executor import ExecutionResult, ExecutorRegistry, default_registry
from .runtime_plugins import RuntimePluginError, RuntimePluginHost
from loom_v2.content_store import ContentStore
from loom_v2.digest import canonical_json_bytes, digest_bytes
from loom_v2.contracts.io_schema import ValidationError as SchemaValidationError, validate, validate_schema


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
    executor_registry: ExecutorRegistry = field(default_factory=lambda: default_registry)
    capability_operation_timeout_seconds: float | None = None
    package_cache: dict[str, CapabilityPackageVersion] = field(default_factory=dict)
    activations: dict[str, CapabilityPackageActivation] = field(default_factory=dict)
    resource_events: list[ResourceEventFrame] = field(default_factory=list)
    runtime_plugin_host: RuntimePluginHost | None = None
    _activation_locks: dict[str, asyncio.Lock] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        self.sessions = make_session_factory(self.engine) if self.engine is not None else None
        if self.content_store is None:
            settings = Settings()
            self.content_store = ContentStore.from_settings(settings)
        if self.runtime_plugin_host is None:
            settings = Settings()
            self.runtime_plugin_host = RuntimePluginHost(
                plugin_dir=settings.runtime_plugin_dir,
                socket_dir=settings.runtime_plugin_socket_dir,
                startup_timeout=settings.runtime_plugin_startup_timeout_seconds,
                call_timeout=settings.runtime_plugin_call_timeout_seconds,
            )
        if self.capability_operation_timeout_seconds is not None and self.executor_registry is default_registry:
            self.executor_registry = ExecutorRegistry(capability_timeout_seconds=self.capability_operation_timeout_seconds)

    async def init_db(self) -> None:
        if self.engine is None:
            if self.runtime_plugin_host is not None:
                await self.runtime_plugin_host.start()
            return
        async with self.engine.begin() as connection:
            await connection.run_sync(SlaveBase.metadata.create_all)
            def ensure_activation_column(sync_connection: Any) -> None:
                columns = {str(item.get("name")) for item in inspect(sync_connection).get_columns("slave_replica")}
                if "capability_activations" not in columns:
                    column_type = "JSONB" if sync_connection.dialect.name == "postgresql" else "JSON"
                    default = "'[]'::jsonb" if sync_connection.dialect.name == "postgresql" else "'[]'"
                    sync_connection.exec_driver_sql(
                        f"ALTER TABLE slave_replica ADD COLUMN capability_activations {column_type} NOT NULL DEFAULT {default}"
                    )
            await connection.run_sync(ensure_activation_column)
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
                        session_generation=int(item.get("session_generation", 1)),
                        activation_state=item.get("activation_state", "provisioning"),
                        evidence_refs=list(item.get("evidence_refs") or []),
                        runtime_profile=dict(item.get("runtime_profile") or {}),
                    )
                except (TypeError, ValueError):
                    continue
                self._cache_package(package, package.version_ref)
                self.activations[str(item.get("activation_key") or self._package_cache_key(package.version_ref, package.package_digest))] = activation
        finally:
            if owns_session:
                await session.__aexit__(None, None, None)

    async def _persist_activation(
        self,
        package: CapabilityPackageVersion,
        activation: CapabilityPackageActivation,
        *,
        desired_state: str,
        runtime_plugin_id: str = "",
        runtime_handle: dict[str, Any] | None = None,
        idempotency_key: str = "",
        last_error_code: str = "",
    ) -> None:
        if self.sessions is None:
            return
        key = self._package_cache_key(activation.package_version_ref, activation.package_digest)
        now = datetime.now(timezone.utc)
        values = {
            "activation_key": key,
            "workspace_id": self.workspace_id,
            "slave_id": self.slave_id,
            "package_version_ref": activation.package_version_ref,
            "package_digest": activation.package_digest.lower(),
            "package_payload": package.model_dump(mode="json"),
            "runtime_plugin_id": runtime_plugin_id,
            "desired_state": desired_state,
            "activation_state": activation.activation_state,
            "activation_closure_version_ref": activation.activation_closure_version_ref,
            "compute_binding_ref": activation.compute_binding_ref,
            "session_generation": activation.session_generation,
            "evidence_refs": list(activation.evidence_refs),
            "runtime_profile": dict(activation.runtime_profile),
            "runtime_handle": dict(runtime_handle or {}),
            "last_idempotency_key": idempotency_key,
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
                    # Runtime plugins own long-lived service activations;
                    # one-shot function executions do not need reconciliation
                    # and must retain their existing activation facts.
                    if package_payload.get("package_type", "function") != "service":
                        continue
                    packages[row["activation_key"]] = self._find_cached_package(row["package_version_ref"], row["package_digest"])  # type: ignore[assignment]
                    entries.append({
                        "activation_key": row["activation_key"],
                        "workspace_id": row.get("workspace_id", self.workspace_id),
                        "slave_id": row.get("slave_id", self.slave_id),
                        "package_type": package_payload.get("package_type"),
                        "execution": package_payload.get("execution"),
                        "package_payload": package_payload,
                        "desired_state": row.get("desired_state", "running"),
                        "runtime_profile": row.get("runtime_profile") or {},
                    })
        else:
            for key, activation in self.activations.items():
                package = self._find_cached_package(activation.package_version_ref, activation.package_digest)
                if package is None or package.package_type != "service":
                    continue
                entries.append({"activation_key": key, "workspace_id": self.workspace_id, "slave_id": self.slave_id, "package_type": package.package_type, "execution": package.execution.model_dump(mode="json"), "package_payload": package.model_dump(mode="json"), "desired_state": "running" if activation.activation_state != "stopped" else "stopped", "runtime_profile": activation.runtime_profile})
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
                        await self._persist_activation(package, updated, desired_state="running", runtime_plugin_id="", last_error_code="runtime_plugin_not_found")
                continue
            state = str(item.get("activation_state") or ("ready" if (item.get("inspect") or {}).get("running") else activation.activation_state))
            update_fields: dict[str, Any] = {"activation_state": state if state in {"ready", "degraded", "failed", "stopped", "lost"} else activation.activation_state}
            if isinstance(item.get("runtime_profile"), dict):
                update_fields["runtime_profile"] = dict(item["runtime_profile"])
            updated = activation.model_copy(update=update_fields)
            self.activations[key] = updated
            package = packages.get(key) or self._find_cached_package(updated.package_version_ref, updated.package_digest)
            if package is not None:
                await self._persist_activation(package, updated, desired_state=entry.get("desired_state", "running"), runtime_plugin_id=str((item or {}).get("runtime_plugin_id") or ""))
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
    def _package_aliases(package: CapabilityPackageVersion, command_ref: str) -> set[str]:
        return {
            command_ref,
            package.version_ref,
            f"{package.package_id}:{package.package_version}",
            package.package_closure_version_ref,
            package.package_version,
            package.package_id,
            package.package_digest,
        }

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
                if package_ref in {
                    item.package_id,
                    item.version_ref,
                    f"{item.package_id}:{item.package_version}",
                    item.package_closure_version_ref,
                    item.package_version,
                }:
                    return item
            return None
        return self.package_cache.get(package_ref)

    @staticmethod
    def _operation_name(operation_ref: str) -> str:
        return operation_ref.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1]

    @staticmethod
    def _service_endpoint(
        package: CapabilityPackageVersion,
        operation: str,
        binding: ComputeBinding | None,
    ) -> Any:
        body = package.body
        if not hasattr(body, "endpoints"):
            raise RuntimeError("service_package_body_invalid")
        endpoints = list(body.endpoints)
        descriptor = binding.capability_descriptor_ref if binding is not None else None
        if descriptor is not None:
            matches = [
                endpoint
                for endpoint in endpoints
                if endpoint.operation_descriptor_ref.resource_id == descriptor.resource_id
                and endpoint.operation_descriptor_ref.digest == descriptor.digest
            ]
            if len(matches) == 1:
                return matches[0]
            if len(matches) > 1:
                raise RuntimeError("service_endpoint_binding_mismatch")
            raise RuntimeError("service_endpoint_binding_mismatch")
        candidates = {operation, SlaveService._operation_name(operation)}
        matches = [
            endpoint
            for endpoint in endpoints
            if endpoint.endpoint_id in candidates
            or endpoint.path in candidates
            or endpoint.operation_descriptor_ref.resource_id in candidates
            or SlaveService._operation_name(endpoint.operation_descriptor_ref.resource_id) in candidates
        ]
        if len(matches) != 1:
            raise RuntimeError("service_endpoint_not_found")
        return matches[0]

    async def _validate_service_package(self, package: CapabilityPackageVersion) -> None:
        if package.package_type != "service" or not hasattr(package.body, "endpoints"):
            raise RuntimeError("service_package_body_invalid")
        for endpoint in package.body.endpoints:
            contract = await self._load_io_contract(endpoint.io_contract_ref)
            for schema_ref in (contract.input_schema_ref, contract.output_schema_ref):
                if schema_ref is None:
                    continue
                try:
                    schema = json.loads(await self.content_store.get(schema_ref))
                    validate_schema(schema)
                except (FileNotFoundError, TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise RuntimeError("io_contract_invalid") from exc

    async def _validate_service_input_payload(
        self,
        package: CapabilityPackageVersion,
        operation: str,
        payload: dict[str, Any],
        binding: ComputeBinding | None,
    ) -> None:
        """Validate direct service dispatches even without a closure binding."""
        endpoint = self._service_endpoint(package, operation, binding)
        contract = await self._load_io_contract(endpoint.io_contract_ref)
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
            if contract.success_validator_ref is not None:
                try:
                    await self.content_store.get(contract.success_validator_ref)
                except (FileNotFoundError, ValueError) as exc:
                    raise RuntimeError("io_contract_invalid") from exc

    async def _provision_service(
        self,
        command: CapabilityProvisionCommand,
        package: CapabilityPackageVersion,
    ) -> CapabilityHealthReport:
        key = self._package_cache_key(package.version_ref, package.package_digest)
        lock = self._activation_locks.setdefault(key, asyncio.Lock())
        async with lock:
            return await self._provision_service_locked(command, package)

    async def _provision_service_locked(
        self,
        command: CapabilityProvisionCommand,
        package: CapabilityPackageVersion,
    ) -> CapabilityHealthReport:
        if self.runtime_plugin_host is None:
            raise RuntimeError("service_runtime_unavailable")
        if package.package_digest.lower() != command.package_digest.lower():
            raise RuntimeError("capability_package_digest_mismatch")
        await self._validate_service_package(package)
        ref = package.version_ref
        coordinate_refs = {ref, f"{package.package_id}:{package.package_version}"}
        for item in self.activations.values():
            if item.package_version_ref in coordinate_refs and item.package_digest.lower() != package.package_digest.lower():
                # A package coordinate is immutable.  Never start a second
                # container for the same coordinate under another digest.
                raise RuntimeError("capability_package_identity_conflict")
        activation_key = self._package_cache_key(ref, package.package_digest)
        existing = self.activations.get(activation_key)
        # Write desired-running before touching the runtime.  A crash between
        # this write and plugin completion is recovered by startup reconcile.
        pending = existing or CapabilityPackageActivation(
            package_version_ref=ref,
            package_digest=package.package_digest,
            target_slave=self.slave_id,
            activation_closure_version_ref=command.activation_closure_version_ref or package.package_closure_version_ref,
            compute_binding_ref=command.compute_binding.binding_id if command.compute_binding else "",
            session_generation=command.session_generation,
            activation_state="provisioning",
            evidence_refs=[],
            runtime_profile={},
        )
        if pending.activation_state != "provisioning":
            pending = pending.model_copy(update={"activation_state": "provisioning", "session_generation": command.session_generation})
        self._cache_package(package, command.package_version_ref)
        self.activations[activation_key] = pending
        await self._persist_activation(package, pending, desired_state="running", idempotency_key=command.idempotency_key)
        try:
            response = await self.runtime_plugin_host.provision(package, command)
        except (RuntimePluginError, RuntimeError) as exc:
            code = getattr(exc, "code", None) or str(exc) or "runtime_plugin_unavailable"
            failed = pending.model_copy(update={"activation_state": "failed"})
            self.activations[activation_key] = failed
            await self._persist_activation(package, failed, desired_state="running", idempotency_key=command.idempotency_key, last_error_code=code)
            raise RuntimeError(code) from exc
        state = str(response.get("activation_state") or response.get("state") or "ready")
        if state not in {"ready", "degraded", "failed", "stopped"}:
            raise RuntimeError("runtime_plugin_protocol_error")
        if existing is not None and existing.activation_state == "ready" and state == "ready":
            await self._persist_activation(package, existing, desired_state="running", runtime_plugin_id=str(response.get("runtime_plugin_id") or ""), idempotency_key=command.idempotency_key)
            return CapabilityHealthReport(
                report_id=f"health-{package.package_id}-{self.slave_id}",
                package_version_ref=ref,
                package_digest=package.package_digest,
                target_slave=self.slave_id,
                activation_state="ready",
                evidence_refs=list(existing.evidence_refs),
                details={"idempotent": True, "runtime_plugin_id": str(response.get("runtime_plugin_id") or "")},
                session_generation=command.session_generation,
            )
        activation = CapabilityPackageActivation(
            package_version_ref=ref,
            package_digest=package.package_digest,
            target_slave=self.slave_id,
            activation_closure_version_ref=command.activation_closure_version_ref or package.package_closure_version_ref,
            compute_binding_ref=command.compute_binding.binding_id if command.compute_binding else "",
            session_generation=command.session_generation,
            activation_state=state,
            evidence_refs=[str(item) for item in response.get("evidence_refs", []) if item is not None],
            runtime_profile=dict(response.get("runtime_profile") or {}),
        )
        self.activations[activation_key] = activation
        await self._persist_activation(
            package,
            activation,
            desired_state="running" if state != "stopped" else "stopped",
            runtime_plugin_id=str(response.get("runtime_plugin_id") or ""),
            runtime_handle=dict(response.get("runtime_handle") or {}),
            idempotency_key=command.idempotency_key,
            last_error_code=str((response.get("details") or {}).get("code") or "") if state == "failed" else "",
        )
        if package.scope == "workspace_reusable" and package.publication_state == "published" and state == "ready":
            for endpoint in package.body.endpoints:
                self.supported_operations.add(self._operation_name(endpoint.operation_descriptor_ref.resource_id))
        report = CapabilityHealthReport(
            report_id=f"health-{package.package_id}-{self.slave_id}",
            package_version_ref=ref,
            package_digest=package.package_digest,
            target_slave=self.slave_id,
            activation_state=state,
            evidence_refs=activation.evidence_refs,
            details={
                "runtime_plugin_id": str(response.get("runtime_plugin_id") or ""),
                "package_version_ref": ref,
                "package_digest": package.package_digest,
                **dict(response.get("details") or {}),
            },
            session_generation=command.session_generation,
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
        return report

    async def provision(self, command: CapabilityProvisionCommand, package: CapabilityPackageVersion | None = None) -> CapabilityHealthReport:
        if command.target_slave != self.slave_id:
            raise RuntimeError("provision_target_mismatch")
        if command.compute_binding is not None and command.compute_binding.target_resource_ref.resource_id != self.slave_id:
            raise RuntimeError("binding_target_mismatch")
        package = package or self._find_cached_package(command.package_version_ref, command.package_digest)
        if package is None:
            raise RuntimeError("capability_package_not_found")
        if package.package_type == "service":
            return await self._provision_service(command, package)
        if package.execution.kind == "container:python_orchestrator":
            raise RuntimeError("driver_side_executor_required")
        if package.function_body.io_contract_ref is None:
            raise RuntimeError("io_contract_required")
        try:
            await self._load_io_contract(package.function_body.io_contract_ref)
        except (FileNotFoundError, RuntimeError, ValueError, TypeError) as exc:
            raise RuntimeError("io_contract_invalid") from exc
        if package.package_digest.lower() != command.package_digest.lower():
            raise RuntimeError("capability_package_digest_mismatch")
        if package.function_body.provider_fillable_hole_refs and (command.compute_binding is None or not command.compute_binding.runtime_profile):
            raise RuntimeError("provider_fillable_binding_required")
        ref = package.version_ref
        activation_key = self._package_cache_key(ref, package.package_digest)
        existing_activation = self.activations.get(activation_key)
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
        stat = await self.content_store.stat(package.function_body.program_content_ref)
        if stat is None or not stat.integrity_verified or stat.declared_digest != package.function_body.program_digest:
            raise RuntimeError("program_content_unavailable")
        activation = CapabilityPackageActivation(
            package_version_ref=ref,
            package_digest=package.package_digest,
            target_slave=self.slave_id,
            activation_closure_version_ref=command.activation_closure_version_ref or package.package_closure_version_ref,
            compute_binding_ref=command.compute_binding.binding_id if command.compute_binding else "",
            session_generation=command.session_generation,
            activation_state="ready",
            evidence_refs=[f"package-test:{package.package_digest[:16]}", f"health:{package.package_digest[:16]}"],
        )
        self._cache_package(package, command.package_version_ref)
        self.activations[activation_key] = activation
        await self._persist_activation(package, activation, desired_state="running", idempotency_key=command.idempotency_key)
        operation_ref = package.function_body.operation_descriptor_ref
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
            details={
                "operation": operation_name,
                "execution": package.execution.model_dump(mode="json"),
                "executor_descriptor_ref": self.executor_registry.get(package.execution).descriptor.descriptor_ref,
                "package_version_ref": package.version_ref,
                "package_digest": package.package_digest,
            },
            session_generation=command.session_generation,
        )
        self.resource_events.append(ResourceEventFrame(event_id=f"resource-{report.report_id}", resource_ref=ref, event_type="activation_ready", package_version_ref=ref, package_digest=package.package_digest, target_slave=self.slave_id, evidence_refs=report.evidence_refs))
        return report

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
        if package.package_type != "service":
            raise RuntimeError("unsupported_deprovision_contract")
        lock = self._activation_locks.setdefault(self._package_cache_key(package.version_ref, package.package_digest), asyncio.Lock())
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
            raise RuntimeError("service_runtime_unavailable")
        try:
            response = await self.runtime_plugin_host.deprovision(package, command)
        except (RuntimePluginError, RuntimeError) as exc:
            raise RuntimeError(getattr(exc, "code", None) or str(exc) or "runtime_plugin_unavailable") from exc
        ref = package.version_ref
        activation_key = self._package_cache_key(ref, package.package_digest)
        existing = self.activations.get(activation_key)
        evidence_refs = list(existing.evidence_refs) if existing is not None else [f"deprovision:{package.package_digest[:16]}"]
        if existing is None:
            activation = CapabilityPackageActivation(
                package_version_ref=ref,
                package_digest=package.package_digest,
                target_slave=self.slave_id,
                activation_closure_version_ref=package.package_closure_version_ref,
                compute_binding_ref="",
                session_generation=command.session_generation,
                activation_state="stopped",
                evidence_refs=evidence_refs,
            )
        else:
            activation = existing.model_copy(update={"activation_state": "stopped", "session_generation": command.session_generation})
        self.activations[activation_key] = activation
        # Keep the local capability projection aligned with desired state;
        # a stopped service must not continue to advertise its operations.
        for endpoint in package.body.endpoints:
            operation_name = self._operation_name(endpoint.operation_descriptor_ref.resource_id)
            still_ready = any(
                item.activation_state == "ready"
                and (cached := self._find_cached_package(item.package_version_ref, item.package_digest)) is not None
                and cached.package_type == "service"
                and any(self._operation_name(other.operation_descriptor_ref.resource_id) == operation_name for other in cached.body.endpoints)
                for key, item in self.activations.items()
                if key != activation_key
            )
            if not still_ready:
                self.supported_operations.discard(operation_name)
        await self._persist_activation(
            package,
            activation,
            desired_state="stopped",
            runtime_plugin_id=str(response.get("runtime_plugin_id") or ""),
            idempotency_key=command.idempotency_key,
        )
        report = CapabilityHealthReport(
            report_id=f"health-{package.package_id}-{self.slave_id}-stopped",
            package_version_ref=ref,
            package_digest=package.package_digest,
            target_slave=self.slave_id,
            activation_state="stopped",
            evidence_refs=evidence_refs,
            details={"runtime_plugin_id": str(response.get("runtime_plugin_id") or ""), "idempotent": bool(response.get("idempotent", False))},
            session_generation=command.session_generation,
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
        return report

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
        package: CapabilityPackageVersion | None,
        binding: ComputeBinding | None = None,
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
        binding = next((item for item in closure.node_input_bindings if item.node_id in candidates), None)
        if binding is None:
            raise RuntimeError("input_binding_missing")

        if package is not None and package.package_type == "service":
            endpoint = self._service_endpoint(package, operation, binding)
            contract_ref = endpoint.io_contract_ref
        else:
            contract_ref = (
                package.function_body.io_contract_ref
                if package is not None and package.function_body.io_contract_ref is not None
                else closure.program.io_contract_ref
            )
        contract = await self._load_io_contract(contract_ref) if contract_ref is not None else None
        try:
            raw = await self.content_store.get(binding.input_ref)
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
        package: CapabilityPackageVersion | None,
        binding: ComputeBinding | None,
    ) -> ExecutionResult:
        if package is not None and package.package_type == "service":
            endpoint = self._service_endpoint(package, operation or (closure.program.operation_ref if closure is not None else ""), binding)
            contract_ref = endpoint.io_contract_ref
        else:
            contract_ref = package.function_body.io_contract_ref if package is not None and package.function_body.io_contract_ref is not None else closure.program.io_contract_ref if closure is not None else None
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
            plugin_result = await self.executor_registry.invoke(ExecutionContract(kind="process:json_stdio", version="1"), plugin_input, program=plugin)
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
        payload = await self._admission_payload(payload, operation=operation, closure=closure, package=package, binding=binding)
        if package is not None and package.package_type == "service":
            activation_key = self._package_cache_key(package.version_ref, package.package_digest)
            activation = self.activations.get(activation_key)
            if activation is None or activation.activation_state != "ready":
                raise RuntimeError("service_activation_not_ready")
            endpoint = self._service_endpoint(package, operation, binding)
            await self._validate_service_input_payload(package, operation, payload, binding)
            if self.runtime_plugin_host is None:
                raise RuntimeError("service_runtime_unavailable")
            try:
                activation_payload = activation.model_dump(mode="json")
                # Workspace/Slave identity is role-local context rather than
                # part of CapabilityPackageActivation's portable contract.
                # The runtime plugin still needs it to verify managed
                # container labels before issuing an HTTP request.
                activation_payload.update({"workspace_id": self.workspace_id, "slave_id": self.slave_id})
                response = await self.runtime_plugin_host.invoke(
                    package,
                    endpoint,
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
                replay_safety=str(response.get("replay_safety") or endpoint.replay_safety),
                terminal_state=str(response.get("terminal_state") or "completed"),
                terminal_error=response.get("terminal_error"),
                provenance={
                    "package_version_ref": package.version_ref,
                    "package_digest": package.package_digest,
                    "endpoint_id": endpoint.endpoint_id,
                    "operation_descriptor_ref": endpoint.operation_descriptor_ref.model_dump(mode="json"),
                    "image_ref": package.body.image_ref,
                    "execution": package.execution.model_dump(mode="json"),
                    "runtime_plugin_id": str(response.get("runtime_plugin_id") or ""),
                    **dict(response.get("provenance") or {}),
                },
            )
        elif package is not None:
            program = await self.content_store.get(package.function_body.program_content_ref, expected_digest=package.function_body.program_digest)
            result = await self.executor_registry.invoke(package.execution, payload, program=program)
            descriptor = self.executor_registry.get(package.execution).descriptor
            result = replace(
                result,
                provenance={
                    "package_version_ref": package.version_ref,
                    "package_digest": package.package_digest,
                    "program_content_ref": package.function_body.program_content_ref.model_dump(mode="json"),
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
            package=package,
            binding=binding,
        )
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
