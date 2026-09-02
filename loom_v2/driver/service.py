from __future__ import annotations

import os
import asyncio
import json
import shutil
from contextlib import suppress
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from loom_v2.coding_agents.base import CodingAgentError, CodingAgentProvider
from loom_v2.contracts.types import CapabilityProvisionCommand, ComputeBinding, ResourceRef, TaskClosure
from loom_v2.observer.repository import ObserverRepository
from loom_v2.driver.worker import WorkerSession
from loom_v2.slave.service import SlaveService
from loom_v2.slave.executor import ExecutionResult, execute_operation
from loom_v2.settings import Settings
from loom_v2.coding_agents.turn import TurnContext
from loom_v2.driver.coordinator import DriverTurnCoordinator
from loom_v2.contracts.messages import message_payload_digest
from loom_v2.contracts.errors import DomainError, DomainErrorEnvelope

from .orchestrator import DockerOrchestrationExecutor
from .orchestration_runtime import DynamicOrchestrationRuntime
from .remote_repository import RemoteObserverRepository

from .tools import DriverTools
from .mcp import DriverMCP, ObserverStatus


@dataclass
class ActiveTurn:
    run_id: str | None
    turn_ref: str | None = None
    interrupt_requested: bool = False
    interrupt_sent: bool = False
    deadline_exceeded: bool = False


class DriverService:
    def __init__(
        self,
        repository: ObserverRepository,
        provider: CodingAgentProvider,
        executor=execute_operation,
        slaves: dict[str, SlaveService] | None = None,
        workers: dict[str, WorkerSession] | None = None,
        deadline_seconds: float | None = None,
        orchestration_executor: DockerOrchestrationExecutor | None = None,
        content_store: Any | None = None,
    ) -> None:
        self.control_client = repository if hasattr(repository, "command") and not hasattr(repository, "get_run") else None
        self.repository = repository
        self.remote_repository = RemoteObserverRepository(repository, content_store=content_store) if self.control_client is not None else None
        self.content_store = content_store
        self.provider = provider
        self.tools = DriverTools(repository)
        self.executor = executor
        self.slaves = slaves or {}
        self.workers = workers or {}
        self.orchestration_executor = orchestration_executor or DockerOrchestrationExecutor.from_settings(Settings())
        configured_deadline = os.getenv("LOOM_CODING_AGENT_DEADLINE_SECONDS", "86400")
        self.deadline_seconds = deadline_seconds if deadline_seconds is not None else float(configured_deadline)
        self.active_turns: dict[str, ActiveTurn] = {}
        self._execution_tasks: dict[str, asyncio.Task[Any]] = {}
        self.coordinator = DriverTurnCoordinator(
            provider,
            workspace_id=getattr(repository, "workspace_id", "workspace-default"),
            driver_epoch=int(getattr(repository, "driver_epoch", 0) or 0),
            control_client=self.control_client,
            turn_executor=self._coordinator_execute,
            manage_provider_context=False,
        )

    async def _coordinator_execute(self, context: TurnContext, prompt: str) -> dict[str, Any]:
        if self.control_client is not None:
            return await self._run_prompt_remote(context.conversation_ref, prompt, request_id=context.request_id, claim_token=context.claim_token, context=context)
        return await self._run_prompt(context.conversation_ref, prompt, request_id=context.request_id)

    async def run_prompt(self, conversation_ref: str, prompt: str, request_id: str | None = None, payload_digest: str | None = None) -> dict[str, Any]:
        if request_id is not None and (self.control_client is None or hasattr(self.control_client, "claim_message")):
            return await self.coordinator.submit(
                {
                    "request_id": request_id,
                    "conversation_ref": conversation_ref,
                    "prompt": prompt,
                    "payload_digest": payload_digest or message_payload_digest(conversation_ref, prompt),
                },
                prompt,
            )
        if self.control_client is not None:
            return await self._run_prompt_remote(conversation_ref, prompt, request_id=request_id)
        # A live app-server conversation is allowed to stay quiet while the
        # coding agent thinks.  This wait is an absolute safety deadline, not
        # an idle/progress timeout; child operations enforce their own
        # operation-specific limits at the Worker/Slave boundary.
        work = asyncio.create_task(self._run_prompt(conversation_ref, prompt, request_id=request_id))
        try:
            done, _pending = await asyncio.wait({work}, timeout=self.deadline_seconds)
            if done:
                return work.result()

            # Mark the active turn before cancelling it so the run's failure
            # reason distinguishes a conversation deadline from an external
            # task cancellation.
            active = self.active_turns.get(conversation_ref)
            if active is not None:
                active.deadline_exceeded = True
            work.cancel()
            try:
                await work
            except asyncio.CancelledError:
                pass
            raise RuntimeError("coding_agent_deadline_exceeded")
        except asyncio.CancelledError:
            if not work.done():
                work.cancel()
                try:
                    await work
                except asyncio.CancelledError:
                    pass
            raise

    async def interrupt(self, conversation_ref: str) -> dict[str, Any]:
        if self.control_client is not None and hasattr(self.control_client, "claim_message"):
            return await self.coordinator.interrupt(conversation_ref)
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

    async def _run_prompt(self, conversation_ref: str, prompt: str, request_id: str | None = None) -> dict[str, Any]:
        mcp = DriverMCP(
            self.repository,
            conversation_ref,
            request_id=request_id,
            prompt=prompt,
            run_executor=self._execute_and_wait_local,
        )
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
                    await mcp.call("loom_open_run", event.payload)
                    await self._attach_open_run(mcp, active, prompt, pending_assistant_parts)
                elif event.kind == "tool_call":
                    tool_name = str(event.payload.get("tool", ""))
                    # Codex dynamic tools execute inside the provider before the
                    # normalized event reaches this loop. Adopt the scoped MCP
                    # state here so the Driver's execution lane sees the same
                    # Run that the agent opened.
                    await self._attach_open_run(mcp, active, prompt, pending_assistant_parts)
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
                    await mcp.call("loom_start_run", {"closure_version": committed})
                elif event.kind == "inspect_plan_readiness":
                    if active.run_id is None:
                        raise RuntimeError("run_not_open")
                    readiness = await self.tools.inspect_plan_readiness(active.run_id)
                    if not readiness["ready"]:
                        raise DomainError(DomainErrorEnvelope(code="readiness_blocked", details={"blockers": readiness["blockers"]}))
                elif event.kind in {"agent_error", "agent_stalled"}:
                    reason = dict(event.payload)
                    reason.setdefault("code", "coding_agent_error")
                    raise CodingAgentError(reason)
                elif event.kind in {"agent_warning", "thread_status", "goal_status"}:
                    if active.run_id is not None:
                        await self.repository.record_agent_signal(active.run_id, event.kind, event.payload)
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
            completed = await self.repository.get_run(active.run_id)
            response = DriverMCP.run_view(completed)
            response.update({
                "run_id": active.run_id,
                "conversation_ref": conversation_ref,
                "closure_version": committed,
                "patches": patches,
                "execution_id": completed.execution_id,
                "execution_epoch": completed.execution_epoch,
                "assistant_text": "\n\n".join(assistant_parts),
            })
            if isinstance(response.get("resource_ref"), dict):
                response["resource_ref"] = response["resource_ref"].get("resource_id")
            return response
        except asyncio.CancelledError:
            if active.run_id is not None:
                current = await self.repository.get_run(active.run_id)
                if active.interrupt_requested:
                    if current.state not in {"completed", "failed", "cancelled", "closed"}:
                        await self.repository.cancel_run(active.run_id)
                elif active.deadline_exceeded:
                    if current.state in {"thinking", "running"}:
                        await self.repository.fail_run(active.run_id, "coding_agent_deadline_exceeded")
                elif current.state in {"thinking", "running"}:
                    await self.repository.fail_run(active.run_id, "driver_cancelled")
            raise
        except Exception as exc:
            if active.run_id is not None:
                reason = (
                    exc.reason
                    if isinstance(exc, CodingAgentError)
                    else exc.envelope.model_dump(mode="json")
                    if isinstance(exc, DomainError)
                    else str(exc)
                )
                current = await self.repository.get_run(active.run_id)
                if current.state in {"completed", "awaiting_decision", "cancelled", "failed", "closed"}:
                    response = DriverMCP.run_view(current)
                    response.update({
                        "run_id": active.run_id,
                        "conversation_ref": conversation_ref,
                        "closure_version": committed or (current.committed.version_id if current.committed else None),
                        "patches": patches,
                        "execution_id": current.execution_id,
                        "execution_epoch": current.execution_epoch,
                        "assistant_text": "\n\n".join(assistant_parts),
                        "agent_error": reason if isinstance(reason, dict) else {"code": str(reason), "message": str(reason)},
                    })
                    if isinstance(response.get("resource_ref"), dict):
                        response["resource_ref"] = response["resource_ref"].get("resource_id")
                    return response
                reason_code = reason.get("code") if isinstance(reason, dict) else None
                if current.state in {"thinking", "running"} and reason_code != "readiness_blocked":
                    await self.repository.fail_run(active.run_id, reason)
            raise
        finally:
            if self.active_turns.get(conversation_ref) is active:
                self.active_turns.pop(conversation_ref, None)
            await self.provider.close()

    async def _run_prompt_remote(self, conversation_ref: str, prompt: str, *, request_id: str | None = None, claim_token: str | None = None, context: TurnContext | None = None) -> dict[str, Any]:
        """Run a turn through Observer's fenced command API.

        This path deliberately keeps the coding-agent process in Driver while
        all authoritative state mutations are sent to Observer.
        """
        control = self.control_client
        assert control is not None
        request_id = request_id or f"request-{uuid4().hex}"
        active = ActiveTurn(run_id=None)
        self.active_turns[conversation_ref] = active
        binding = None
        getter = getattr(control, "thread_binding", None) or getattr(control, "thread_get", None)
        try:
            if getter is not None:
                binding = await getter(conversation_ref)
        except Exception:
            if self.active_turns.get(conversation_ref) is active:
                self.active_turns.pop(conversation_ref, None)
            raise
        existing_thread_id = binding.get("thread_id") if isinstance(binding, dict) else None
        if isinstance(binding, dict) and binding.get("active_request_id") == request_id:
            persisted_state = str(binding.get("turn_state") or "")
            status = {
                "completed": "completed",
                "interrupted": "interrupted",
                "recovery_pending": "interrupted",
                "starting": "in_progress",
                "in_progress": "in_progress",
            }.get(persisted_state)
            if status is not None:
                if self.active_turns.get(conversation_ref) is active:
                    self.active_turns.pop(conversation_ref, None)
                return {
                    "run_id": None,
                    "conversation_ref": conversation_ref,
                    "status": status,
                    "state": "completed" if status == "completed" else persisted_state,
                    "thread_id": existing_thread_id,
                    "assistant_text": "",
                }
        set_tool_handler = getattr(self.provider, "set_tool_handler", None)
        async def execute_remote(run_id: str, run_prompt: str) -> Any:
            return await self._execute_and_wait_remote(run_id, run_prompt, expected_epoch=context.driver_epoch if context is not None else None)

        mcp = DriverMCP(
            control,
            conversation_ref,
            request_id=request_id,
            prompt=prompt,
            content_store=self.content_store,
            run_executor=execute_remote,
        )
        begin_turn = getattr(self.provider, "begin_turn", None)
        provider_context: TurnContext | None = None
        use_context_api = callable(begin_turn)
        if callable(set_tool_handler) and not use_context_api:
            set_tool_handler(DriverMCP.tool_specs(), mcp.call)
        try:
            if use_context_api:
                provider_context = await begin_turn(
                    conversation_ref,
                    request_id,
                    os.getenv("LOOM_WORKSPACE_ROOT", "/workspace"),
                    existing_thread_id,
                    DriverMCP.tool_specs(),
                    mcp.call,
                )
                provider_context.claim_token = claim_token or provider_context.claim_token
                provider_context.driver_epoch = int(getattr(control, "driver_epoch", 0) or 0)
                thread_id = provider_context.thread_id or conversation_ref
            else:
                try:
                    thread_id = await self.provider.start(conversation_ref, os.getenv("LOOM_WORKSPACE_ROOT", "/workspace"), existing_thread_id=existing_thread_id)
                except TypeError:
                    if existing_thread_id:
                        raise
                    thread_id = await self.provider.start(conversation_ref, os.getenv("LOOM_WORKSPACE_ROOT", "/workspace"))
            if context is not None:
                context.thread_id = thread_id
                if provider_context is not None:
                    context.process = provider_context.process
                    context.dynamic_tools = list(provider_context.dynamic_tools)
                    context.mcp_handler = provider_context.mcp_handler
        except Exception as exc:
            if self.active_turns.get(conversation_ref) is active:
                self.active_turns.pop(conversation_ref, None)
            with suppress(Exception):
                await self.provider.close()
            if claim_token:
                failure_code = str(exc.reason.get("code")) if isinstance(exc, CodingAgentError) and isinstance(exc.reason, dict) else str(exc)
                if failure_code != "stale_driver_epoch":
                    failure_state = "failed" if failure_code == "codex_thread_unavailable" else "retryable"
                    with suppress(Exception):
                        await control.update_message(
                            request_id,
                            claim_token=claim_token,
                            state=failure_state,
                            outcome={"error": {"code": failure_code, "message": str(exc)[:1024]}},
                        )
            raise
        try:
            if not existing_thread_id:
                await control.thread_bind({"workspace_id": getattr(control, "workspace_id", "workspace-default"), "conversation_ref": conversation_ref, "thread_id": thread_id, "model": getattr(self.provider, "model", None) or os.getenv("LOOM_CODEX_MODEL", "deepseek-v4-flash"), "workspace_root": os.getenv("LOOM_WORKSPACE_ROOT", "/workspace"), "turn_state": "idle", "driver_epoch": int(getattr(control, "driver_epoch", 0) or 0)})
            elif isinstance(binding, dict) and binding.get("turn_state") == "recovery_pending":
                read_thread = getattr(self.provider, "read_thread", None)
                if callable(read_thread):
                    if provider_context is not None:
                        await read_thread(provider_context)
                    else:
                        await read_thread()
                await control.turn_state(
                    conversation_ref,
                    "interrupted",
                    request_id=binding.get("active_request_id"),
                    turn_id=binding.get("last_turn_id"),
                )
            await control.turn_state(conversation_ref, "starting", request_id=request_id)
        except Exception:
            if self.active_turns.get(conversation_ref) is active:
                self.active_turns.pop(conversation_ref, None)
            with suppress(Exception):
                await self.provider.close()
            raise
        run_id = None
        assistant_parts: list[str] = []
        pending_assistant_parts: list[tuple[int, str]] = []
        turn_id = None
        patches = 0
        committed_version: str | None = None
        try:
            event_stream = self.provider.send_turn(provider_context, prompt) if provider_context is not None else self.provider.send_turn(prompt)
            async for event in event_stream:
                self._assert_turn_epoch(context, control)
                if event.kind == "turn_started":
                    turn_id = event.payload.get("turn_id") or turn_id
                    active.turn_ref = turn_id
                    if context is not None:
                        context.turn_id = turn_id
                    await control.turn_state(conversation_ref, "in_progress", request_id=request_id, turn_id=turn_id)
                elif event.kind == "open_run":
                    opened = await mcp.call("loom_open_run", event.payload)
                    run_id = opened.get("run_id")
                    if run_id:
                        await control.command(
                            "message.append",
                            {"run_id": run_id, "role": "user", "content": prompt, "request_id": request_id},
                            request_id=f"{request_id}:user",
                        )
                        for sequence, pending in pending_assistant_parts:
                            await control.command(
                                "message.append",
                                {"run_id": run_id, "role": "assistant", "content": pending},
                                request_id=f"{request_id}:assistant:{sequence}",
                            )
                        pending_assistant_parts.clear()
                elif event.kind == "tool_call":
                    tool_name = str(event.payload.get("tool", ""))
                    if run_id is None and mcp.run_id is not None:
                        run_id = mcp.run_id
                        await control.command(
                            "message.append",
                            {"run_id": run_id, "role": "user", "content": prompt, "request_id": request_id},
                            request_id=f"{request_id}:user",
                        )
                        for sequence, pending in pending_assistant_parts:
                            await control.command(
                                "message.append",
                                {"run_id": run_id, "role": "assistant", "content": pending},
                                request_id=f"{request_id}:assistant:{sequence}",
                            )
                        pending_assistant_parts.clear()
                    if tool_name == "loom_open_run" and run_id:
                        current = await control.command("run.get", {"run_id": run_id})
                        if current.get("state") == "opened":
                            await control.command("run.begin", {"run_id": run_id}, request_id=f"{request_id}:begin")
                    if tool_name == "loom_apply_plan_patch":
                        patches += 1
                    elif tool_name == "loom_commit_plan" and run_id:
                        current = await control.command("run.get", {"run_id": run_id})
                        committed_version = current.get("committed_version") or committed_version
                elif event.kind == "apply_plan_patch" and run_id:
                    await control.command(
                        "run.patch",
                        {"run_id": run_id, "operation_id": f"{request_id}:patch:{patches}", "ops": event.payload.get("ops", [])},
                        request_id=f"{request_id}:patch:{patches}",
                    )
                    patches += 1
                elif event.kind == "commit_plan" and run_id:
                    current = await control.command("run.get", {"run_id": run_id})
                    self._ensure_orchestration_runtime(current.get("draft") or {})
                    committed = await control.command(
                        "run.commit",
                        {"run_id": run_id, "version_id": current.get("draft_version"), "digest": current.get("draft_digest")},
                        request_id=f"{request_id}:commit",
                    )
                    committed_version = committed.get("closure_version")
                elif event.kind == "start_run" and run_id:
                    current = await control.command("run.get", {"run_id": run_id})
                    committed_version = committed_version or current.get("committed_version")
                    if not committed_version:
                        raise RuntimeError("closure_not_committed")
                    await mcp.call("loom_start_run", {"closure_version": committed_version})
                elif event.kind == "inspect_plan_readiness" and run_id:
                    readiness = await control.command("run.readiness", {"run_id": run_id})
                    if not readiness.get("ready", False):
                        raise DomainError(DomainErrorEnvelope(code="readiness_blocked", details={"blockers": readiness.get("blockers", [])}))
                    self._ensure_orchestration_runtime(readiness)
                elif event.kind == "assistant_text":
                    text = str(event.payload.get("text", "")).strip()
                    if text:
                        assistant_parts.append(text)
                        if run_id:
                            await control.command("message.append", {"run_id": run_id, "role": "assistant", "content": text}, request_id=f"{request_id}:assistant:{len(assistant_parts)}")
                        else:
                            pending_assistant_parts.append((len(assistant_parts), text))
                        if claim_token:
                            await control.update_message(
                                request_id,
                                claim_token=claim_token,
                                state="in_flight",
                                assistant_text="\n\n".join(assistant_parts),
                                run_id=run_id,
                            )
                elif event.kind == "turn_interrupted":
                    await control.turn_state(conversation_ref, "interrupted", request_id=request_id, turn_id=turn_id)
                    result = {"run_id": run_id, "conversation_ref": conversation_ref, "status": "interrupted", "state": "cancelled", "assistant_text": "\n\n".join(assistant_parts)}
                    if claim_token:
                        await control.update_message(request_id, claim_token=claim_token, state="interrupted", assistant_text=result["assistant_text"], run_id=run_id, outcome={"error": {"code": "user_interrupt"}})
                    return result
                elif event.kind in {"thread_status", "goal_status"}:
                    if claim_token:
                        await control.update_message(
                            request_id,
                            claim_token=claim_token,
                            state="in_flight",
                            assistant_text="\n\n".join(assistant_parts),
                            run_id=run_id,
                        )
                elif event.kind in {"agent_error", "agent_stalled"}:
                    raise CodingAgentError(dict(event.payload))
            await control.turn_state(conversation_ref, "completed", request_id=request_id, turn_id=turn_id)
            result = {"run_id": run_id, "conversation_ref": conversation_ref, "status": "completed", "state": "completed" if run_id else "idle", "assistant_text": "\n\n".join(assistant_parts), "thread_id": thread_id, "closure_version": committed_version, "patches": patches}
            if run_id:
                with suppress(Exception):
                    current = await control.command("run.get", {"run_id": run_id})
                    result["state"] = current.get("state", result["state"])
                    result["status"] = current.get("status", result["status"])
                    result["outcome"] = current.get("outcome")
                    result["disposition"] = (current.get("outcome") or {}).get("disposition")
                    result["decision"] = (current.get("outcome") or {}).get("decision")
                    result["decision_hint"] = {
                        "repair": "repair_plan_and_retry",
                        "attestation": "resolve_run_accept_or_abandon",
                    }.get(result["decision"], "none")
            if claim_token:
                await control.update_message(
                    request_id,
                    claim_token=claim_token,
                    state="completed",
                    assistant_text=result["assistant_text"],
                    run_id=run_id,
                    outcome=result.get("outcome") or {"result": {"status": result.get("status"), "run_id": run_id}},
                )
            return result
        except asyncio.CancelledError:
            if run_id:
                with suppress(Exception):
                    current = await control.command("run.get", {"run_id": run_id})
                    if current.get("state") not in {"completed", "failed", "cancelled", "closed"}:
                        await control.command("run.cancel", {"run_id": run_id, "reason": "driver_cancelled"}, request_id=f"{request_id}:run-cancelled")
            if claim_token:
                with suppress(Exception):
                    await control.update_message(request_id, claim_token=claim_token, state="interrupted", assistant_text="\n\n".join(assistant_parts), run_id=run_id, outcome={"error": {"code": "driver_cancelled"}})
            raise
        except Exception as exc:
            with suppress(Exception):
                await control.turn_state(conversation_ref, "interrupted", request_id=request_id, turn_id=turn_id)
            failure = (
                exc.reason
                if isinstance(exc, CodingAgentError)
                else exc.envelope.model_dump(mode="json")
                if isinstance(exc, DomainError)
                else {"code": str(exc), "message": str(exc)}
            )
            if run_id and failure.get("code") != "stale_driver_epoch":
                with suppress(Exception):
                    current = await control.command("run.get", {"run_id": run_id})
                    if current.get("state") in {"completed", "awaiting_decision", "cancelled", "failed", "closed"}:
                        if claim_token:
                            await control.update_message(
                                request_id,
                                claim_token=claim_token,
                                state="completed",
                                assistant_text="\n\n".join(assistant_parts),
                                run_id=run_id,
                                outcome=current.get("outcome"),
                            )
                        return {
                            "run_id": run_id,
                            "conversation_ref": conversation_ref,
                            "status": current.get("status") or "completed",
                            "state": current.get("state"),
                            "outcome": current.get("outcome"),
                            "assistant_text": "\n\n".join(assistant_parts),
                            "agent_error": failure,
                        }
                    if current.get("state") in {"thinking", "running"} and failure.get("code") != "readiness_blocked":
                        await control.command("run.fail", {"run_id": run_id, "reason": failure}, request_id=f"{request_id}:run-failed")
            if claim_token:
                with suppress(Exception):
                    await control.update_message(request_id, claim_token=claim_token, state="failed", assistant_text="\n\n".join(assistant_parts), run_id=run_id, outcome={"error": failure})
            raise
        finally:
            if self.active_turns.get(conversation_ref) is active:
                self.active_turns.pop(conversation_ref, None)
            if provider_context is not None:
                with suppress(Exception):
                    await self.provider.end_turn(provider_context)
            else:
                await self.provider.close()

    @staticmethod
    def _assert_turn_epoch(context: TurnContext | None, control: Any, *, expected_epoch: int | None = None) -> None:
        if context is None and expected_epoch is None:
            return
        current_epoch = getattr(control, "driver_epoch", None)
        if current_epoch is not None and int(current_epoch) != int(expected_epoch if expected_epoch is not None else context.driver_epoch):
            raise RuntimeError("stale_driver_epoch")

    @staticmethod
    def _ensure_orchestration_runtime(run_payload: dict[str, Any]) -> None:
        snapshot = run_payload.get("snapshot") if isinstance(run_payload, dict) else None
        systems = snapshot.get("program_systems") if isinstance(snapshot, dict) else None
        orchestration = bool(run_payload.get("orchestration")) or (
            isinstance(systems, dict) and systems.get("executor_kind") == "orchestrator_python_v1"
        )
        if orchestration and shutil.which("docker") is None:
            raise RuntimeError("orchestrator_runtime_unavailable")

    async def _dispatch_remote_execution(self, run_id: str, prompt: str, *, expected_epoch: int | None = None) -> None:
        control = self.control_client
        if expected_epoch is not None:
            self._assert_turn_epoch(None, control, expected_epoch=expected_epoch)
        if control is None or not self.workers:
            if control is None:
                return
            await self._refresh_remote_workers()
        if not self.workers:
            raise RuntimeError("node_target_unavailable")
        current = await control.command("run.get", {"run_id": run_id})
        execution_id = current.get("execution_id")
        if not execution_id:
            return
        execution_epoch = int(current.get("execution_epoch", 1))
        snapshot_payload = (current.get("committed") or current.get("draft") or {}).get("snapshot")
        if not snapshot_payload:
            return
        snapshot = TaskClosure.model_validate(snapshot_payload)
        if snapshot.program_systems.executor_kind == "orchestrator_python_v1":
            assert self.remote_repository is not None
            await self.remote_repository.refresh_slaves()
            runtime = DynamicOrchestrationRuntime(
                repository=self.remote_repository,
                executor=self.orchestration_executor,
                workers=self.workers,
                driver_id=control.driver_id,
                driver_epoch=control.driver_epoch,
            )
            await runtime.run(run_id)
            return
        attempt = next((item for item in reversed(current.get("attempts", [])) if item.get("execution_epoch", execution_epoch) == execution_epoch and item.get("state") in {"created", "running"}), None)
        if attempt is None:
            return
        target = str(attempt.get("target") or "slave-a")
        worker = self.workers.get(target)
        if worker is None:
            raise RuntimeError(f"capability_unavailable:{target}")
        operation = self._operation_name(snapshot.program.operation_ref or snapshot.compute.operation_ref) or "echo"
        payload: dict[str, Any]
        if operation in {"echo", "hash"}:
            payload = {"text": prompt}
        elif operation == "sort":
            try:
                decoded = json.loads(prompt)
                payload = {"items": decoded if isinstance(decoded, list) else []}
            except (TypeError, json.JSONDecodeError):
                payload = {"items": []}
        else:
            payload = {}
        binding = snapshot.compute_bindings[0] if snapshot.compute_bindings else None
        if binding is not None and binding.capability_package_ref is not None and self.remote_repository is not None:
            package = await self.remote_repository.get_capability_package(binding.capability_package_ref, run_id=run_id)
            provision_command = CapabilityProvisionCommand(
                command_id=f"provision-{package.package_id}-{target}",
                package_version_ref=f"{package.package_id}:{package.package_version}",
                package_digest=package.package_digest,
                target_slave=target,
                workspace_id=getattr(control, "workspace_id", "workspace-default"),
                activation_closure_version_ref=(current.get("committed") or {}).get("version_id") or package.package_closure_version_ref,
                compute_binding=binding,
                program_content_ref=package.program_content_ref,
                idempotency_key=f"run-{execution_id}-{package.package_digest}-{target}",
            )
            report = await worker.provision(
                command=provision_command,
                package=package,
                driver_id=getattr(control, "driver_id", None),
                driver_epoch=getattr(control, "driver_epoch", None),
            )
            await control.command(
                "capability.health",
                {"run_id": run_id, "report": report.model_dump(mode="json")},
                request_id=f"health:{execution_id}:{target}:{package.package_digest}",
            )
        result = await worker.dispatch(attempt_id=str(attempt["attempt_id"]), execution_id=str(execution_id), execution_epoch=execution_epoch, workspace_id=getattr(control, "workspace_id", "workspace-default"), operation=operation, payload=payload, closure=snapshot, binding=binding, driver_id=getattr(control, "driver_id", None), driver_epoch=getattr(control, "driver_epoch", None))
        await control.command("run.result", {"run_id": run_id, "result": {"attempt_id": attempt["attempt_id"], "execution_id": execution_id, "execution_epoch": execution_epoch, "resource_ref": result.resource_ref.model_dump(mode="json"), "digest": result.digest, "value": result.value, "terminal_state": result.terminal_state, "terminal_error": result.terminal_error, "validation_evidence": result.validation_evidence}}, request_id=f"execution:{execution_id}:{execution_epoch}")

    async def _refresh_remote_workers(self) -> None:
        if self.control_client is None:
            return
        agents = await self.control_client.list_slaves()
        for agent in agents:
            if agent.get("lease_state") != "active" or not agent.get("endpoint_url"):
                continue
            slave_id = str(agent["agent_id"])
            self.workers[slave_id] = WorkerSession(
                slave_id,
                str(agent["endpoint_url"]),
                operation_timeout=float(os.getenv("LOOM_WORKER_OPERATION_TIMEOUT_SECONDS", "90")),
                internal_api_secret=getattr(self.control_client, "internal_api_secret", None),
            )

    async def provision_capability(
        self,
        package_ref: str | ResourceRef,
        target_slave: str,
        *,
        compute_binding: ComputeBinding | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Provision a published package on a Slave and report health to Observer."""
        if self.control_client is None or self.remote_repository is None:
            raise RuntimeError("driver_control_unavailable")
        await self._refresh_remote_workers()
        worker = self.workers.get(target_slave)
        if worker is None:
            raise RuntimeError("slave_not_found")
        package = await self.remote_repository.get_capability_package(package_ref)
        command = CapabilityProvisionCommand(
            command_id=f"provision-{package.package_id}-{target_slave}",
            package_version_ref=f"{package.package_id}:{package.package_version}",
            package_digest=package.package_digest,
            target_slave=target_slave,
            workspace_id=self.control_client.workspace_id,
            activation_closure_version_ref=package.package_closure_version_ref,
            compute_binding=compute_binding,
            program_content_ref=package.program_content_ref,
            idempotency_key=idempotency_key or f"promote-{package.package_digest}-{target_slave}",
        )
        report = await worker.provision(
            command=command,
            package=package,
            driver_id=self.control_client.driver_id,
            driver_epoch=self.control_client.driver_epoch,
        )
        await self.control_client.command(
            "capability.health",
            {"report": report.model_dump(mode="json")},
            request_id=f"health:{package.package_digest}:{target_slave}",
        )
        return report.model_dump(mode="json")

    async def _attach_open_run(
        self,
        mcp: DriverMCP,
        active: ActiveTurn,
        prompt: str,
        pending_assistant_parts: list[str],
    ) -> None:
        if active.run_id is not None or mcp.run_id is None:
            return
        active.run_id = mcp.run_id
        await self.repository.begin_refinement(active.run_id)
        await self.repository.append_message(active.run_id, "user", prompt)
        for pending in pending_assistant_parts:
            await self.repository.append_message(active.run_id, "assistant", pending)
        pending_assistant_parts.clear()

    async def _execute_and_wait_local(self, run_id: str, prompt: str) -> dict[str, Any]:
        """Execute one started Run exactly once and return its projection."""
        record = await self.repository.get_run(run_id)
        if record.state in {"completed", "failed", "cancelled", "awaiting_decision", "closed"}:
            return DriverMCP.run_view(record)
        if record.state != "running":
            raise RuntimeError("execution_not_running")
        key = f"local:{run_id}:{record.execution_id}:{record.execution_epoch}"
        task = self._execution_tasks.get(key)
        if task is None:
            task = asyncio.create_task(self._dispatch_execution(run_id, prompt), name=f"run-execution:{run_id}")
            self._execution_tasks[key] = task
        try:
            while True:
                done, _pending = await asyncio.wait({task}, timeout=0.2)
                if done:
                    completed, _result = task.result()
                    break
                current = await self.repository.get_run(run_id)
                if current.state == "cancelled":
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
                    return DriverMCP.run_view(current)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            current = await self.repository.get_run(run_id)
            if current.state in {"thinking", "running"}:
                await self.repository.fail_run(run_id, {"code": str(exc) or "driver_execution_failed", "message": str(exc)[:1024]})
            raise
        finally:
            if task.done():
                self._execution_tasks.pop(key, None)
        return DriverMCP.run_view(completed)

    async def _execute_and_wait_remote(self, run_id: str, prompt: str, *, expected_epoch: int | None = None) -> dict[str, Any]:
        control = self.control_client
        if control is None:
            raise RuntimeError("driver_control_unavailable")
        current = await control.command("run.get", {"run_id": run_id})
        state = str(current.get("state") or "")
        if state in {"completed", "failed", "cancelled", "awaiting_decision", "closed"}:
            return current
        if state != "running":
            raise RuntimeError("execution_not_running")
        if expected_epoch is not None:
            self._assert_turn_epoch(None, control, expected_epoch=expected_epoch)
        key = f"remote:{run_id}:{current.get('execution_id')}:{current.get('execution_epoch', 1)}"
        task = self._execution_tasks.get(key)
        if task is None:
            task = asyncio.create_task(
                self._dispatch_remote_execution(run_id, prompt, expected_epoch=expected_epoch),
                name=f"remote-run-execution:{run_id}",
            )
            self._execution_tasks[key] = task
        try:
            while True:
                done, _pending = await asyncio.wait({task}, timeout=0.2)
                if done:
                    task.result()
                    break
                current = await control.command("run.get", {"run_id": run_id})
                if current.get("state") == "cancelled":
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
                    return current
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            current = await control.command("run.get", {"run_id": run_id})
            if current.get("state") in {"thinking", "running"} and str(getattr(exc, "reason", "")) != "stale_driver_epoch":
                with suppress(Exception):
                    await control.command("run.fail", {"run_id": run_id, "reason": {"code": str(exc) or "driver_execution_failed", "message": str(exc)[:1024]}}, request_id=f"run-fail:{run_id}")
            raise
        finally:
            if task.done():
                self._execution_tasks.pop(key, None)
        return await control.command("run.get", {"run_id": run_id})

    async def _dispatch_execution(self, run_id: str, prompt: str) -> tuple[Any, ExecutionResult]:
        """Dispatch exactly once after the coding agent commits and starts a Run.

        ``loom_start_run`` is an execution boundary, not merely another plan
        mutation.  Dispatching here means a later app-server/turn failure does
        not roll back work that has already completed.
        """
        record = await self.repository.get_run(run_id)
        if record.execution_id is None:
            raise RuntimeError("execution_not_started")
        execution_id = record.execution_id
        execution_epoch = record.execution_epoch
        snapshot = record.committed.snapshot if record.committed is not None else TaskClosure.minimal()
        if snapshot.program_systems.executor_kind == "orchestrator_python_v1":
            runtime = DynamicOrchestrationRuntime(
                repository=self.repository,
                executor=self.orchestration_executor,
                slaves=self.slaves,
                workers=self.workers,
                driver_id=getattr(self.control_client, "driver_id", None),
                driver_epoch=getattr(self.control_client, "driver_epoch", None),
            )
            return await runtime.run(run_id)
        operation = self._operation_name(snapshot.program.operation_ref or snapshot.compute.operation_ref) or "echo"
        payload = await self._execution_payload(snapshot, operation, prompt)
        binding = self._binding_for_operation(snapshot)
        current_attempt = next(
            (
                item
                for item in reversed(record.attempts)
                if item.get("execution_epoch", execution_epoch) == execution_epoch
                and item.get("state") in {"created", "running"}
            ),
            None,
        )
        target = str(current_attempt.get("target")) if current_attempt is not None else binding.target_resource_ref.resource_id if binding is not None else "slave-a"
        if binding is not None and binding.target_resource_ref.resource_id != target:
            binding = binding.model_copy(update={"target_resource_ref": ResourceRef(resource_id=target)})
        attempt_id = next(
            (
                item["attempt_id"]
                for item in reversed(record.attempts)
                if item.get("target") == target
                and item.get("execution_epoch", execution_epoch) == execution_epoch
                and item.get("state") in {"created", "running"}
            ),
            None,
        )
        if attempt_id is None:
            raise RuntimeError("attempt_not_created")
        if self.workers or self.slaves:
            worker = self.workers.get(target)
            if worker is not None:
                if binding is not None and binding.capability_package_ref is not None:
                    package = await self.repository.get_capability_package(binding.capability_package_ref, run_id=run_id)
                    command = CapabilityProvisionCommand(
                        command_id=f"provision-{package.package_id}-{target}",
                        package_version_ref=f"{package.package_id}:{package.package_version}",
                        package_digest=package.package_digest,
                        target_slave=target,
                        workspace_id=record.closure_contract.workspace_id if record.closure_contract else "workspace-default",
                        activation_closure_version_ref=record.committed.version_id if record.committed else package.package_closure_version_ref,
                        compute_binding=binding,
                        program_content_ref=package.program_content_ref,
                        idempotency_key=f"run-{execution_id}-{package.package_digest}-{target}",
                    )
                    report = await worker.provision(command=command, package=package, driver_id=getattr(self.control_client, "driver_id", None), driver_epoch=getattr(self.control_client, "driver_epoch", None))
                    await self.repository.record_capability_health(report, run_id=run_id)
                workspace_id = record.closure_contract.workspace_id if record.closure_contract else "workspace-default"
                result = await worker.dispatch(
                    attempt_id=attempt_id,
                    execution_id=execution_id,
                    execution_epoch=execution_epoch,
                    workspace_id=workspace_id,
                    operation=operation,
                    payload=payload,
                    closure=snapshot,
                    binding=binding,
                    driver_id=getattr(self.control_client, "driver_id", None),
                    driver_epoch=getattr(self.control_client, "driver_epoch", None),
                )
            else:
                slave = self.slaves.get(target)
                if slave is None:
                    raise RuntimeError(f"capability_unavailable:{target}")
                if binding is not None and binding.capability_package_ref is not None:
                    package = await self.repository.get_capability_package(binding.capability_package_ref, run_id=run_id)
                    command = CapabilityProvisionCommand(
                        command_id=f"provision-{package.package_id}-{target}",
                        package_version_ref=f"{package.package_id}:{package.package_version}",
                        package_digest=package.package_digest,
                        target_slave=target,
                        workspace_id=record.closure_contract.workspace_id if record.closure_contract else "workspace-default",
                        activation_closure_version_ref=record.committed.version_id if record.committed else package.package_closure_version_ref,
                        compute_binding=binding,
                        program_content_ref=package.program_content_ref,
                        idempotency_key=f"run-{execution_id}-{package.package_digest}-{target}",
                    )
                    report = await slave.provision(command, package)
                    await self.repository.record_capability_health(report, run_id=run_id)
                result = await slave.run(
                    attempt_id,
                    operation,
                    payload,
                    closure=snapshot,
                    binding=binding,
                    execution_epoch=execution_epoch,
                )
        else:
            result = await self.executor(operation, payload)
        completed = await self.repository.record_result(
            run_id,
            {
                "attempt_id": attempt_id,
                "execution_id": execution_id,
                "execution_epoch": execution_epoch,
                "resource_ref": result.resource_ref.model_dump(mode="json"),
                "digest": result.digest,
                "value": result.value,
                "terminal_state": result.terminal_state,
                "terminal_error": result.terminal_error,
                "validation_evidence": result.validation_evidence,
            },
        )
        return completed, result

    @staticmethod
    def _operation_name(operation_ref: str) -> str:
        if not operation_ref:
            return ""
        return operation_ref.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1]

    async def _execution_payload(self, snapshot: TaskClosure, operation: str, prompt: str) -> dict[str, Any]:
        # Executable input is always a content-addressed NodeInputBinding.
        # Resolve it at dispatch time so the closure carries only semantic
        # references and no mutable/raw payload in metadata.
        operation_ref = snapshot.program.operation_ref or snapshot.compute.operation_ref
        binding = self.repository._input_binding_for(snapshot, operation_ref)
        if binding is not None:
            value = await self.repository._load_json_content(binding.input_ref)
            return dict(value) if isinstance(value, dict) else {"value": value}

        payload: dict[str, Any] = {}
        if operation in {"echo", "hash"}:
            payload.setdefault("text", prompt)
        elif operation == "sort" and "items" not in payload:
            try:
                decoded = json.loads(prompt)
                items = decoded if isinstance(decoded, list) else []
            except (TypeError, json.JSONDecodeError):
                items = []
            payload["items"] = items
        return payload

    @staticmethod
    def _binding_for_operation(snapshot: TaskClosure) -> ComputeBinding | None:
        if not snapshot.compute_bindings:
            return None
        return snapshot.compute_bindings[0]
