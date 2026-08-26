from __future__ import annotations

import os
import asyncio
import json
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from loom_v2.coding_agents.base import CodingAgentProvider
from loom_v2.contracts.types import ComputeBinding, TaskClosure
from loom_v2.observer.repository import ObserverRepository
from loom_v2.observer.worker import WorkerSession
from loom_v2.slave.service import SlaveService
from loom_v2.slave.executor import ExecutionResult, execute_operation

from .tools import DriverTools
from .mcp import DriverMCP, ObserverStatus


@dataclass
class ActiveTurn:
    run_id: str | None
    turn_ref: str | None = None
    interrupt_requested: bool = False
    interrupt_sent: bool = False


class DriverService:
    def __init__(
        self,
        repository: ObserverRepository,
        provider: CodingAgentProvider,
        executor=execute_operation,
        slaves: dict[str, SlaveService] | None = None,
        workers: dict[str, WorkerSession] | None = None,
    ) -> None:
        self.repository = repository
        self.provider = provider
        self.tools = DriverTools(repository)
        self.executor = executor
        self.slaves = slaves or {}
        self.workers = workers or {}
        self.active_turns: dict[str, ActiveTurn] = {}

    async def run_prompt(self, conversation_ref: str, prompt: str) -> dict[str, Any]:
        timeout_seconds = float(os.getenv("LOOM_CODING_AGENT_TIMEOUT_SECONDS", "90"))
        try:
            async with asyncio.timeout(timeout_seconds):
                return await self._run_prompt(conversation_ref, prompt)
        except TimeoutError as exc:
            raise RuntimeError("coding_agent_timeout") from exc

    async def interrupt(self, conversation_ref: str) -> dict[str, Any]:
        active = self.active_turns.get(conversation_ref)
        if active is None:
            raise ValueError("conversation_not_active")
        active.interrupt_requested = True
        await self.provider.interrupt(active.turn_ref)
        active.interrupt_sent = active.turn_ref is not None
        return {
            "conversation_ref": conversation_ref,
            "run_id": active.run_id,
            "status": "interrupt_requested",
        }

    async def _run_prompt(self, conversation_ref: str, prompt: str) -> dict[str, Any]:
        mcp = DriverMCP(self.repository, conversation_ref)
        active = ActiveTurn(run_id=None)
        self.active_turns[conversation_ref] = active
        patches = 0
        committed: str | None = None
        assistant_parts: list[str] = []
        pending_assistant_parts: list[str] = []
        interrupted = False
        try:
            set_tool_handler = getattr(self.provider, "set_tool_handler", None)
            if callable(set_tool_handler):
                set_tool_handler(DriverMCP.tool_specs(), mcp.call)
            await self.provider.start(conversation_ref, os.getenv("LOOM_WORKSPACE_ROOT", "/workspace"))
            async for event in self.provider.send_turn(prompt):
                if event.kind == "turn_started":
                    active.turn_ref = event.payload.get("turn_id") or active.turn_ref
                if active.interrupt_requested and not active.interrupt_sent:
                    await self.provider.interrupt(active.turn_ref)
                    active.interrupt_sent = active.turn_ref is not None
                if event.kind == "turn_interrupted":
                    interrupted = True
                    break
                if active.interrupt_requested:
                    # Discard late planning events after the user requested a stop.
                    continue
                if event.kind == "open_run":
                    opened = await mcp.call("loom_open_run", event.payload)
                    active.run_id = opened["run_id"]
                    run = await self.repository.begin_refinement(active.run_id)
                    await self.repository.append_message(active.run_id, "user", prompt)
                    for pending in pending_assistant_parts:
                        await self.repository.append_message(active.run_id, "assistant", pending)
                    pending_assistant_parts.clear()
                elif event.kind == "tool_call":
                    tool_name = str(event.payload.get("tool", ""))
                    # Codex dynamic tools execute inside the provider before the
                    # normalized event reaches this loop. Adopt the scoped MCP
                    # state here so the Driver's execution lane sees the same
                    # Run that the agent opened.
                    if active.run_id is None and mcp.run_id is not None:
                        active.run_id = mcp.run_id
                        await self.repository.begin_refinement(active.run_id)
                        await self.repository.append_message(active.run_id, "user", prompt)
                        for pending in pending_assistant_parts:
                            await self.repository.append_message(active.run_id, "assistant", pending)
                        pending_assistant_parts.clear()
                    if tool_name == "loom_apply_plan_patch":
                        patches += 1
                    elif tool_name == "loom_commit_plan" and active.run_id is not None:
                        record = await self.repository.get_run(active.run_id)
                        committed = record.committed.version_id if record.committed is not None else committed
                    continue
                elif event.kind == "apply_plan_patch":
                    if active.run_id is None:
                        raise RuntimeError("run_not_open")
                    await self.tools.apply_plan_patch(active.run_id, f"patch-{uuid4().hex[:12]}", event.payload.get("ops", []))
                    patches += 1
                elif event.kind == "assistant_text":
                    text = str(event.payload.get("text", "")).strip()
                    if text:
                        if active.run_id is None:
                            pending_assistant_parts.append(text)
                        else:
                            await self.repository.append_message(active.run_id, "assistant", text)
                        assistant_parts.append(text)
                elif event.kind == "commit_plan":
                    if active.run_id is None:
                        raise RuntimeError("run_not_open")
                    committed = (await self.tools.commit_plan(active.run_id))["closure_version"]
                elif event.kind == "start_run":
                    if active.run_id is None:
                        raise RuntimeError("run_not_open")
                    if committed is None:
                        record = await self.repository.get_run(active.run_id)
                        committed = record.committed.version_id if record.committed is not None else None
                    if committed is None:
                        raise RuntimeError("closure_not_committed")
                    await self.tools.start_run(active.run_id, committed)
                elif event.kind == "inspect_plan_readiness":
                    if active.run_id is None:
                        raise RuntimeError("run_not_open")
                    readiness = await self.tools.inspect_plan_readiness(active.run_id)
                    if not readiness["ready"]:
                        raise ValueError("readiness_blocked:" + json.dumps(readiness["blockers"], ensure_ascii=False, sort_keys=True))
            if interrupted or active.interrupt_requested:
                if active.run_id is None:
                    return {
                        "run_id": None,
                        "conversation_ref": conversation_ref,
                        "closure_version": committed,
                        "patches": patches,
                        "state": "cancelled",
                        "status": "interrupted",
                        "resource_ref": None,
                        "execution_id": None,
                        "execution_epoch": 1,
                        "assistant_text": "\n\n".join(assistant_parts),
                    }
                cancelled = await self.repository.cancel_run(active.run_id)
                return {
                    "run_id": active.run_id,
                    "conversation_ref": conversation_ref,
                    "closure_version": committed,
                    "patches": patches,
                    "state": cancelled.state,
                    "status": "interrupted",
                    "resource_ref": None,
                    "execution_id": None,
                    "execution_epoch": cancelled.execution_epoch,
                    "assistant_text": "\n\n".join(assistant_parts),
                }
            if active.run_id is None:
                return {
                    "run_id": None,
                    "conversation_ref": conversation_ref,
                    "closure_version": None,
                    "patches": patches,
                    "state": "idle",
                    "status": "completed",
                    "resource_ref": None,
                    "execution_id": None,
                    "execution_epoch": 1,
                    "assistant_text": "\n\n".join(assistant_parts),
                }
            record = await self.repository.get_run(active.run_id)
            if committed is None and record.committed is not None:
                committed = record.committed.version_id
            # The coding agent is the only elaborator and explicitly decides
            # when to commit and start. A turn that merely opens/refines a Run
            # is a valid non-terminal conversation turn; do not invent a
            # commit or execution on its behalf.
            if record.execution_id is None:
                return {
                    "run_id": active.run_id,
                    "conversation_ref": conversation_ref,
                    "closure_version": committed,
                    "patches": patches,
                    "state": record.state,
                    "status": ObserverStatus.status(record.state),
                    "resource_ref": None,
                    "execution_id": None,
                    "execution_epoch": record.execution_epoch,
                    "assistant_text": "\n\n".join(assistant_parts),
                }
            execution = {
                "execution_id": record.execution_id,
                "state": record.state,
                "execution_epoch": record.execution_epoch,
            }
            snapshot = record.committed.snapshot if record.committed is not None else TaskClosure.minimal()
            operation = self._operation_name(snapshot.program.operation_ref or snapshot.compute.operation_ref) or "echo"
            payload = self._execution_payload(snapshot, operation, prompt)
            binding = self._binding_for_operation(snapshot, operation)
            if self.workers or self.slaves:
                target = binding.target_resource_ref.resource_id if binding is not None else "slave-a"
                record = await self.repository.get_run(active.run_id)
                attempt_id = next((item["attempt_id"] for item in record.attempts if item.get("target") == target), None)
                if attempt_id is None:
                    raise RuntimeError("attempt_not_created")
                worker = self.workers.get(target)
                if worker is not None:
                    workspace_id = record.closure_contract.workspace_id if record.closure_contract else "workspace-default"
                    result = await worker.dispatch(
                        attempt_id=attempt_id,
                        execution_id=execution["execution_id"],
                        execution_epoch=execution["execution_epoch"],
                        workspace_id=workspace_id,
                        operation=operation,
                        payload=payload,
                        closure=snapshot,
                        binding=binding,
                    )
                else:
                    slave = self.slaves.get(target)
                    if slave is None:
                        raise RuntimeError(f"capability_unavailable:{target}")
                    result = await slave.run(
                        attempt_id,
                        operation,
                        payload,
                        closure=snapshot,
                        binding=binding,
                    )
            else:
                result = await self.executor(operation, payload)
            completed = await self.repository.record_result(
                active.run_id,
                {"resource_ref": result.resource_ref.resource_id, "digest": result.digest, "value": result.value},
            )
            return {
                "run_id": active.run_id,
                "conversation_ref": conversation_ref,
                "closure_version": committed,
                "patches": patches,
                "state": completed.state,
                "status": "completed",
                "resource_ref": result.resource_ref.resource_id,
                "execution_id": execution["execution_id"],
                "execution_epoch": execution["execution_epoch"],
                "assistant_text": "\n\n".join(assistant_parts),
            }
        except asyncio.CancelledError:
            if active.run_id is not None:
                if active.interrupt_requested:
                    await self.repository.cancel_run(active.run_id)
                else:
                    await self.repository.fail_run(active.run_id, "driver_cancelled")
            raise
        except Exception as exc:
            if active.run_id is not None:
                await self.repository.fail_run(active.run_id, str(exc))
            raise
        finally:
            if self.active_turns.get(conversation_ref) is active:
                self.active_turns.pop(conversation_ref, None)
            await self.provider.close()

    @staticmethod
    def _operation_name(operation_ref: str) -> str:
        if not operation_ref:
            return ""
        return operation_ref.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1]

    @staticmethod
    def _execution_payload(snapshot: TaskClosure, operation: str, prompt: str) -> dict[str, Any]:
        payload = snapshot.metadata.get("execution_payload", {})
        payload = dict(payload) if isinstance(payload, dict) else {}
        if operation in {"echo", "hash"}:
            payload.setdefault("text", prompt)
        elif operation == "sort" and "items" not in payload:
            items = snapshot.metadata.get("items", [])
            if not items:
                try:
                    decoded = json.loads(prompt)
                    items = decoded if isinstance(decoded, list) else []
                except (TypeError, json.JSONDecodeError):
                    items = []
            payload["items"] = items
        return payload

    @staticmethod
    def _binding_for_operation(snapshot: TaskClosure, operation: str) -> ComputeBinding | None:
        if not snapshot.compute_bindings:
            return None
        return snapshot.compute_bindings[0]
