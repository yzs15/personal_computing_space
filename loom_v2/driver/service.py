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
from loom_v2.slave.service import SlaveService
from loom_v2.slave.executor import ExecutionResult, execute_operation

from .tools import DriverTools


@dataclass
class ActiveTurn:
    run_id: str
    turn_ref: str | None = None
    interrupt_requested: bool = False
    interrupt_sent: bool = False


class DriverService:
    def __init__(self, repository: ObserverRepository, provider: CodingAgentProvider, executor=execute_operation, slaves: dict[str, SlaveService] | None = None) -> None:
        self.repository = repository
        self.provider = provider
        self.tools = DriverTools(repository)
        self.executor = executor
        self.slaves = slaves or {}
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
        run = await self.repository.open_run(None, conversation_ref, prompt)
        await self.repository.append_message(run.run_id, "user", prompt)
        await self.repository.begin_refinement(run.run_id)
        active = ActiveTurn(run_id=run.run_id)
        self.active_turns[conversation_ref] = active
        patches = 0
        committed: str | None = None
        assistant_parts: list[str] = []
        interrupted = False
        try:
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
                if event.kind == "apply_plan_patch":
                    await self.tools.apply_plan_patch(run.run_id, f"patch-{uuid4().hex[:12]}", event.payload.get("ops", []))
                    patches += 1
                elif event.kind == "assistant_text":
                    text = str(event.payload.get("text", "")).strip()
                    if text:
                        await self.repository.append_message(run.run_id, "assistant", text)
                        assistant_parts.append(text)
                elif event.kind == "commit_plan":
                    committed = (await self.tools.commit_plan(run.run_id))["closure_version"]
                elif event.kind == "inspect_plan_readiness":
                    readiness = await self.tools.inspect_plan_readiness(run.run_id)
                    if not readiness["ready"]:
                        raise ValueError("readiness_blocked:" + json.dumps(readiness["blockers"], ensure_ascii=False, sort_keys=True))
            if interrupted or active.interrupt_requested:
                cancelled = await self.repository.cancel_run(run.run_id)
                return {
                    "run_id": run.run_id,
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
            if committed is None:
                committed = (await self.tools.commit_plan(run.run_id))["closure_version"]
            execution = await self.tools.start_run(run.run_id, committed)
            record = await self.repository.get_run(run.run_id)
            snapshot = record.committed.snapshot if record.committed is not None else TaskClosure.minimal()
            operation = self._operation_name(snapshot.program.operation_ref or snapshot.compute.operation_ref) or "echo"
            payload = self._execution_payload(snapshot, operation, prompt)
            binding = self._binding_for_operation(snapshot, operation)
            if self.slaves:
                target = binding.target_resource_ref.resource_id if binding is not None else "slave-a"
                slave = self.slaves.get(target)
                if slave is None:
                    raise RuntimeError(f"capability_unavailable:{target}")
                result = await slave.run(
                    execution["execution_id"],
                    operation,
                    payload,
                    closure=snapshot,
                    binding=binding,
                )
            else:
                result = await self.executor(operation, payload)
            completed = await self.repository.record_result(
                run.run_id,
                {"resource_ref": result.resource_ref.resource_id, "digest": result.digest, "value": result.value},
            )
            return {
                "run_id": run.run_id,
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
            if active.interrupt_requested:
                await self.repository.cancel_run(run.run_id)
            else:
                await self.repository.fail_run(run.run_id, "driver_cancelled")
            raise
        except Exception as exc:
            await self.repository.fail_run(run.run_id, str(exc))
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
