from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncEngine

from loom_v2.contracts.terms import TermSupport
from loom_v2.contracts.types import (
    CapabilityHealthReport,
    CapabilityPackageActivation,
    CapabilityPackageVersion,
    CapabilityProvisionCommand,
    ComputeBinding,
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
from loom_v2.content_store import ContentStore
from loom_v2.contracts.io_schema import ValidationError as SchemaValidationError, validate, validate_schema


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
        if package.io_contract_ref is None:
            raise RuntimeError("io_contract_required")
        try:
            await self._load_io_contract(package.io_contract_ref)
        except (FileNotFoundError, RuntimeError, ValueError, TypeError) as exc:
            raise RuntimeError("io_contract_invalid") from exc
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

        contract_ref = (
            package.io_contract_ref
            if package is not None and package.io_contract_ref is not None
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
        closure: TaskClosure | None,
        package: CapabilityPackageVersion | None,
        binding: ComputeBinding | None,
    ) -> ExecutionResult:
        contract_ref = package.io_contract_ref if package is not None and package.io_contract_ref is not None else closure.program.io_contract_ref if closure is not None else None
        if contract_ref is None:
            return result
        contract = await self._load_io_contract(contract_ref)
        input_digest = None
        if closure is not None and closure.node_input_bindings:
            operation_ref = closure.program.operation_ref or closure.compute.operation_ref
            candidates = {operation_ref, operation_ref.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1], "default"}
            input_binding = next((item for item in closure.node_input_bindings if item.node_id in candidates), None)
            input_digest = input_binding.input_ref.version_or_digest if input_binding is not None else None
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
            plugin_result = await self.executor_registry.execute("subprocess_json_v1", "run_code", plugin_input, program=plugin)
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
                        terminal_state=result_payload.get("terminal_state", "completed"),
                        terminal_error=result_payload.get("terminal_error"),
                        validation_evidence=result_payload.get("validation_evidence", []),
                    )
                    self.attempts[attempt_id] = result
                    return result
        payload = await self._admission_payload(payload, operation=operation, closure=closure, package=package)
        if package is not None:
            program = await self.content_store.get(package.program_content_ref, expected_digest=package.program_digest)
            result = await self.executor_registry.execute(package.executor_kind, package.executor_operation, payload, program=program)
        else:
            result = await self.executor_registry.execute("builtin_v1", operation, payload)
        result = await self._validate_terminal_result(
            result,
            attempt_id=attempt_id,
            execution_epoch=execution_epoch,
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
                        "digest": result.digest,
                        "terminal_state": result.terminal_state,
                        "terminal_error": result.terminal_error,
                        "validation_evidence": result.validation_evidence,
                    },
                ))
                await session.commit()
        return result
