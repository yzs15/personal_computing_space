from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from loom_v2.contracts.types import (
    CapabilityPackageVersion,
    CapabilityProvisionCommand,
    ComputeBinding,
    DynamicNode,
    NodeInputBinding,
    NodeIntent,
    ResourceRef,
    TaskClosure,
)
from loom_v2.observer.repository import ObserverRepository
from loom_v2.driver.worker import WorkerSession, WorkerUnavailableError
from loom_v2.slave.executor import ExecutionResult, default_registry
from loom_v2.slave.service import SlaveService

from .orchestrator import DockerOrchestrationExecutor, OrchestrationExecutorError, OrchestrationProgramError


@dataclass
class _PendingNode:
    intent_id: str
    task: asyncio.Task[ResourceRef]


class DynamicOrchestrationRuntime:
    """Run a committed orchestration package and its dynamic child nodes."""

    def __init__(
        self,
        *,
        repository: ObserverRepository,
        executor: DockerOrchestrationExecutor,
        slaves: dict[str, SlaveService] | None = None,
        workers: dict[str, WorkerSession] | None = None,
        driver_id: str | None = None,
        driver_epoch: int | None = None,
    ) -> None:
        self.repository = repository
        self.executor = executor
        self.slaves = slaves or {}
        self.workers = workers or {}
        self.driver_id = driver_id
        self.driver_epoch = driver_epoch
        self._round_robin_cursor: dict[str, int] = {}

    async def run(self, run_id: str) -> tuple[Any, ExecutionResult]:
        record = await self.repository.get_run(run_id)
        if record.execution_id is None or record.state != "running":
            raise RuntimeError("execution_not_running")
        snapshot = record.committed.snapshot if record.committed is not None else record.draft.snapshot
        orchestration_ref = snapshot.program_systems.package_ref
        if orchestration_ref is None or snapshot.program_systems.executor_kind != "orchestrator_python_v1":
            raise RuntimeError("orchestration_package_not_bound")
        try:
            orchestration_package = await self.repository.get_capability_package(orchestration_ref, run_id=run_id)
            if orchestration_package.publication_state == "abandoned":
                raise RuntimeError("capability_package_abandoned")
            if (
                orchestration_package.replay_safety != "DeterministicByEventLog"
                or orchestration_package.captures_run_state
                or orchestration_package.captured_secret_refs
                or orchestration_package.captured_path_refs
            ):
                raise RuntimeError("orchestration_not_replayable")
            program = await self.repository.content_store.get(
                orchestration_package.program_content_ref,
                expected_digest=orchestration_package.program_digest,
            )
            operation_ref = snapshot.program.operation_ref or snapshot.compute.operation_ref
            input_binding = self.repository._input_binding_for(snapshot, operation_ref)
            if input_binding is None:
                raise RuntimeError("orchestration_input_missing")
        except BaseException as exc:
            if not isinstance(exc, asyncio.CancelledError):
                current = await self.repository.get_run(run_id)
                if current.state == "running":
                    await self.repository.fail_run(run_id, {"code": str(exc) or "orchestration_setup_failed"})
            raise

        pending: dict[str, _PendingNode] = {}
        intent_sequence = 0

        async def read_json(ref: ResourceRef):
            return await self.repository._load_json_content(ref)

        async def emit_node(package_ref: ResourceRef, input_refs: list[ResourceRef]) -> str:
            nonlocal intent_sequence
            intent_sequence += 1
            intent_id = f"intent-{intent_sequence:04d}"
            try:
                node_package = await self.repository.get_capability_package(package_ref, run_id=run_id)
            except KeyError as exc:
                try:
                    candidate = await self.repository.get_capability_package(
                        ResourceRef(resource_id=package_ref.resource_id),
                        run_id=run_id,
                    )
                except KeyError:
                    try:
                        await self.repository.get_capability_package(package_ref)
                    except KeyError:
                        raise RuntimeError("node_package_not_found") from exc
                    raise RuntimeError("capability_package_scope_mismatch") from exc
                if package_ref.version_or_digest and package_ref.version_or_digest.lower() != candidate.package_digest.lower():
                    raise RuntimeError("node_package_digest_mismatch") from exc
                node_package = candidate
            existing = next((node for node in record.dynamic_nodes if node.intent_id == intent_id), None)
            if existing is not None and existing.state == "completed":
                if existing.package_ref != package_ref or existing.input_refs != input_refs:
                    raise RuntimeError("node_intent_conflict")
                current_record = await self.repository.get_run(run_id)
                previous = next(
                    (
                        attempt.get("result_ref")
                        for attempt in reversed(current_record.attempts)
                        if attempt.get("node_id") == existing.node_id and attempt.get("state") == "completed" and attempt.get("result_ref")
                    ),
                    None,
                )
                if previous is None:
                    raise RuntimeError("replay_result_missing")
                task = asyncio.create_task(asyncio.sleep(0, result=ResourceRef.model_validate(previous)))
                pending[intent_id] = _PendingNode(intent_id=intent_id, task=task)
                return intent_id
            previous_event = next(
                (
                    event
                    for event in reversed(record.events)
                    if event.get("phase") in {"node_accepted", "node_reassigned"}
                    and event.get("node_id") == existing.node_id
                ),
                None,
            ) if existing is not None else None
            previous_target = (
                (
                    previous_event.get("selected_target")
                    or previous_event.get("to_target")
                    or previous_event.get("target")
                )
                if previous_event is not None
                else None
            )
            await self.repository.refresh_slaves()
            current_record = await self.repository.get_run(run_id)
            target = (
                previous_target
                if existing is not None and previous_target and not record.allow_reassignment
                else self._select_target(node_package, current_record)
            )
            intent = NodeIntent(
                intent_id=intent_id,
                execution_id=record.execution_id,
                package_ref=package_ref,
                input_refs=input_refs,
            )
            node = await self.repository.accept_node_intent(run_id, intent, selected_target=target)
            if node.state == "completed":
                current_record = await self.repository.get_run(run_id)
                previous = next(
                    (
                        attempt.get("result_ref")
                        for attempt in reversed(current_record.attempts)
                        if attempt.get("node_id") == node.node_id and attempt.get("state") == "completed" and attempt.get("result_ref")
                    ),
                    None,
                )
                if previous is None:
                    raise RuntimeError("replay_result_missing")
                task = asyncio.create_task(asyncio.sleep(0, result=ResourceRef.model_validate(previous)))
            else:
                task = asyncio.create_task(self._execute_node(run_id, node, target))
            pending[intent_id] = _PendingNode(intent_id=intent_id, task=task)
            return intent_id

        async def result(handle: str) -> ResourceRef:
            item = pending.get(handle)
            if item is None:
                raise RuntimeError("node_handle_not_found")
            return await item.task

        try:
            final_ref = await self.executor.run(
                program,
                input_binding.input_ref,
                read_json=read_json,
                emit_node=emit_node,
                result=result,
            )
            if pending:
                await asyncio.gather(*(item.task for item in pending.values()))
        except BaseException as exc:
            for item in pending.values():
                if not item.task.done():
                    item.task.cancel()
            if pending:
                await asyncio.gather(*(item.task for item in pending.values()), return_exceptions=True)
            if not isinstance(exc, asyncio.CancelledError):
                reason = exc.reason if isinstance(exc, OrchestrationProgramError) else {"code": str(exc) or "orchestration_executor_error"}
                if isinstance(exc, OrchestrationExecutorError):
                    reason = {"code": str(exc) or "orchestration_executor_error"}
                current = await self.repository.get_run(run_id)
                if current.state not in {"awaiting_decision", "failed", "cancelled"}:
                    await self.repository.fail_run(run_id, reason)
            raise
        completed = await self.repository.complete_orchestration(run_id, final_ref)
        value = await self.repository._load_json_content(final_ref)
        terminal_state = "completed" if completed.state == "completed" else "decision_required"
        execution_result = ExecutionResult(
            resource_ref=final_ref,
            value=value,
            replay_safety=orchestration_package.replay_safety,
            digest=final_ref.version_or_digest or "",
            terminal_state=terminal_state,
            terminal_error=completed.outcome.get("terminal_error") if completed.outcome else None,
        )
        return completed, execution_result

    def _select_target(
        self,
        package: CapabilityPackageVersion,
        record: Any,
        *,
        exclude_targets: set[str] | None = None,
    ) -> str:
        closure = record.committed.snapshot if getattr(record, "committed", None) is not None else record.draft.snapshot
        allowed_effects = set(record.closure_contract.allowed_effects) if record.closure_contract is not None else set()
        if package.permissions and not set(package.permissions).issubset(allowed_effects):
            raise RuntimeError("node_permission_denied")
        available = sorted(
            slave_id
            for slave_id in self.repository.slave_capabilities
            if self.repository._slave_supports_package(slave_id, package)
            and (slave_id in self.workers or slave_id in self.slaves)
            and slave_id not in (exclude_targets or set())
        )
        if not available:
            raise RuntimeError("node_target_unavailable")
        locality = next(
            (
                requirement.value
                for requirement in closure.compute.requirements
                if requirement.key == "loom.data.locality.v1"
            ),
            None,
        )
        if locality is not None:
            requested = str(locality)
            if requested not in available:
                raise RuntimeError("node_target_unavailable")
            return requested
        activation_targets = {
            activation.target_slave
            for activation in record.capability_activations
            if activation.package_version_ref
            in {
                package.version_ref,
                f"{package.package_id}:{package.package_version}",
            }
            and activation.activation_state == "ready"
        }
        candidates = [slave_id for slave_id in available if slave_id in activation_targets] or available
        cursor_key = package.version_ref
        cursor = self._round_robin_cursor.get(cursor_key, 0)
        target = candidates[cursor % len(candidates)]
        self._round_robin_cursor[cursor_key] = cursor + 1
        return target

    async def _execute_node(self, run_id: str, node: DynamicNode, target: str) -> ResourceRef:
        try:
            current = await self.repository.get_run(run_id)
            active_attempt = next(
                (
                    item
                    for item in reversed(current.attempts)
                    if item.get("node_id") == node.node_id and item.get("state") in {"created", "running"}
                ),
                None,
            )
            if node.state == "accepted":
                attempt = await self.repository.dispatch_dynamic_node(run_id, node.node_id, target=target)
            elif node.state == "dispatched" and active_attempt is not None:
                attempt = active_attempt
                await self.repository.refresh_slaves()
                source = self.repository.slave_instances.get(
                    (str(attempt["target"]), str(attempt["target_instance_id"]))
                )
                if source is not None and source.get("lease_state") in {"expired", "released"}:
                    attempt = await self._recover_attempt(run_id, node, attempt)
            else:
                raise RuntimeError("dynamic_node_not_dispatchable")
            while True:
                try:
                    return await self._execute_attempt(run_id, node, attempt)
                except WorkerUnavailableError:
                    attempt = await self._recover_attempt(run_id, node, attempt)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            try:
                record = await self.repository.get_run(run_id)
                attempt = next((item for item in reversed(record.attempts) if item.get("node_id") == node.node_id), None)
                await self.repository.fail_dynamic_node(
                    run_id,
                    node.node_id,
                    attempt_id=attempt.get("attempt_id") if attempt else None,
                    error={"code": str(exc) or "dynamic_node_failed"},
                )
            except Exception:
                pass
            raise RuntimeError("dynamic_node_failed") from exc

    async def _recover_attempt(
        self,
        run_id: str,
        node: DynamicNode,
        attempt: dict[str, Any],
    ) -> dict[str, Any]:
        source_target = str(attempt["target"])
        source_instance_id = str(attempt["target_instance_id"])
        source_worker = self.workers.get(source_target)
        recovery_timeout = float(getattr(source_worker, "operation_timeout", 90.0))
        loop = asyncio.get_running_loop()
        deadline = loop.time() + recovery_timeout
        while True:
            await self.repository.refresh_slaves()
            source = self.repository.slave_instances.get((source_target, source_instance_id))
            if source is not None and source.get("lease_state") in {"expired", "released"}:
                break
            if loop.time() >= deadline:
                raise RuntimeError("node_recovery_exhausted")
            await asyncio.sleep(min(0.1, max(0.0, deadline - loop.time())))

        package = await self.repository.get_capability_package(node.package_ref, run_id=run_id)
        while loop.time() < deadline:
            await self.repository.refresh_slaves()
            record = await self.repository.get_run(run_id)
            try:
                replacement_target = self._select_target(
                    package,
                    record,
                    exclude_targets={source_target},
                )
                return await self.repository.reassign_dynamic_node(
                    run_id,
                    node.node_id,
                    lost_attempt_id=str(attempt["attempt_id"]),
                    expected_execution_id=str(record.execution_id),
                    expected_execution_epoch=int(record.execution_epoch),
                    target=replacement_target,
                    reason="worker_lease_expired",
                )
            except (RuntimeError, ValueError) as exc:
                if str(exc) != "node_target_unavailable":
                    raise
            await asyncio.sleep(min(0.1, max(0.0, deadline - loop.time())))
        raise RuntimeError("node_recovery_exhausted")

    async def _execute_attempt(
        self,
        run_id: str,
        node: DynamicNode,
        attempt: dict[str, Any],
    ) -> ResourceRef:
        package = await self.repository.get_capability_package(node.package_ref, run_id=run_id)
        attempt_id = str(attempt["attempt_id"])
        target = str(attempt["target"])
        record = await self.repository.get_run(run_id)
        operation_ref = package.operation_descriptor_ref
        operation_ref = operation_ref.resource_id if isinstance(operation_ref, ResourceRef) else str(operation_ref)
        if len(node.input_refs) == 1:
            execution_input_ref = node.input_refs[0]
        else:
            values = [await self.repository._load_json_content(input_ref) for input_ref in node.input_refs]
            execution_input_ref = await self.repository.put_content({"inputs": values}, media_type="application/json")
        binding = ComputeBinding(
            binding_id=f"binding-{node.node_id}",
            hole_id=node.node_id,
            capability_descriptor_ref=ResourceRef(resource_id=operation_ref, identity_criterion="descriptor_digest"),
            capability_package_ref=node.package_ref,
            target_resource_ref=ResourceRef(resource_id=target),
            realization_digest=node.package_digest,
            executor_descriptor_digest=default_registry.get(package.executor_kind).descriptor.digest,
        )
        closure = TaskClosure(
            closure_id=node.node_id,
            program={"operation_ref": operation_ref, "io_contract_ref": package.io_contract_ref.model_dump(mode="json")},
            node_input_bindings=[
                NodeInputBinding(node_id=operation_ref, input_ref=execution_input_ref)
            ],
        )
        command = CapabilityProvisionCommand(
            command_id=f"provision-{node.node_id}",
            package_version_ref=f"{package.package_id}:{package.package_version}",
            package_digest=package.package_digest,
            target_slave=target,
            workspace_id=record.closure_contract.workspace_id if record.closure_contract else "workspace-default",
            activation_closure_version_ref=record.committed.version_id if record.committed else package.package_closure_version_ref,
            compute_binding=binding,
            program_content_ref=package.program_content_ref,
            idempotency_key=f"{record.execution_id}-{node.node_id}-{package.package_digest}",
        )
        operation = self.repository._operation_name(operation_ref)
        worker = self.workers.get(target)
        if worker is not None:
            report = await worker.provision(command=command, package=package, driver_id=self.driver_id, driver_epoch=self.driver_epoch)
            await self.repository.record_capability_health(report, run_id=run_id)
            result = await worker.dispatch(
                attempt_id=attempt_id,
                execution_id=record.execution_id,
                execution_epoch=record.execution_epoch,
                workspace_id=record.closure_contract.workspace_id if record.closure_contract else "workspace-default",
                operation=operation,
                payload={},
                closure=closure,
                binding=binding,
                driver_id=self.driver_id,
                driver_epoch=self.driver_epoch,
            )
        else:
            slave = self.slaves.get(target)
            if slave is None:
                raise RuntimeError(f"node_target_unavailable:{target}")
            report = await slave.provision(command, package)
            await self.repository.record_capability_health(report, run_id=run_id)
            result = await slave.run(
                attempt_id,
                operation,
                {},
                closure=closure,
                binding=binding,
                execution_epoch=record.execution_epoch,
            )
        completed_node = await self.repository.record_dynamic_node_result(
            run_id,
            node.node_id,
            {
                "attempt_id": attempt_id,
                "execution_id": record.execution_id,
                "execution_epoch": record.execution_epoch,
                "value": result.value,
                "digest": result.digest,
                "terminal_state": result.terminal_state,
                "terminal_error": result.terminal_error,
                "validation_evidence": result.validation_evidence,
            },
        )
        if completed_node.state != "completed":
            raise RuntimeError("dynamic_node_failed")
        updated = await self.repository.get_run(run_id)
        attempt = next(item for item in updated.attempts if item.get("attempt_id") == attempt_id)
        return ResourceRef.model_validate(attempt["result_ref"])
