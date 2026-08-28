from __future__ import annotations

import asyncio
from contextlib import suppress
import json
import os
import time
from collections.abc import AsyncIterator
from typing import Any, Awaitable, Callable

from .base import AgentEvent


class CodexAppServerProvider:
    def __init__(
        self,
        model: str | None = None,
        executable: str = "codex",
        poll_interval_seconds: float | None = None,
        protocol_failure_seconds: float | None = None,
    ) -> None:
        self.model = model or os.getenv("LOOM_CODEX_MODEL", "deepseek-v4-flash")
        self.executable = executable
        self.poll_interval_seconds = (
            poll_interval_seconds
            if poll_interval_seconds is not None
            else float(os.getenv("LOOM_CODING_AGENT_POLL_INTERVAL_SECONDS", "5"))
        )
        self.protocol_failure_seconds = (
            protocol_failure_seconds
            if protocol_failure_seconds is not None
            else float(os.getenv("LOOM_CODING_AGENT_PROTOCOL_FAILURE_SECONDS", "60"))
        )
        self.clock: Callable[[], float] = time.monotonic
        self.process: asyncio.subprocess.Process | None = None
        self.request_id = 0
        configured_message_limit = os.getenv("LOOM_CODEX_MESSAGE_LIMIT_BYTES", str(16 * 1024 * 1024))
        try:
            self.message_limit_bytes = max(1024, int(configured_message_limit))
        except ValueError:
            self.message_limit_bytes = 16 * 1024 * 1024
        self._read_buffer = bytearray()
        self.thread_id: str | None = None
        self.current_turn_id: str | None = None
        self.dynamic_tools: list[dict[str, Any]] = []
        self.tool_handler: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]] | None = None
        self._poll_task: asyncio.Task[None] | None = None
        self._poll_requests: dict[int, tuple[str, float]] = {}
        self.last_protocol_heartbeat_at: float | None = None
        self.poll_failure_started_at: float | None = None
        self._protocol_failure_reason: dict[str, Any] | None = None
        self._protocol_failure_event: asyncio.Event | None = None
        self._protocol_failure_reported = False

    def set_tool_handler(
        self,
        tools: list[dict[str, Any]],
        handler: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]],
    ) -> None:
        self.dynamic_tools = list(tools)
        self.tool_handler = handler

    async def start(self, conversation_ref: str, workspace_root: str) -> str:
        try:
            self._read_buffer.clear()
            self.process = await asyncio.create_subprocess_exec(
                self.executable,
                "app-server",
                "--listen",
                "stdio://",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except (FileNotFoundError, OSError) as exc:
            raise RuntimeError("coding_agent_unavailable") from exc
        initialize_id = self._next_id()
        await self._send(
            {
                "jsonrpc": "2.0",
                "id": initialize_id,
                "method": "initialize",
                "params": {
                    "clientInfo": {"name": "loom-v2", "version": "0.1.0"},
                    "capabilities": {"experimentalApi": True},
                },
            }
        )
        await self._read_response(initialize_id)
        thread_request_id = self._next_id()
        thread_params: dict[str, Any] = {
            "model": self.model,
            "cwd": workspace_root,
            "runtimeWorkspaceRoots": [workspace_root],
            "metadata": {"conversation_ref": conversation_ref},
        }
        if self.dynamic_tools:
            thread_params["dynamicTools"] = self.dynamic_tools
        await self._send({"jsonrpc": "2.0", "id": thread_request_id, "method": "thread/start", "params": thread_params})
        response = await self._read_response(thread_request_id)
        self.thread_id = str(response.get("result", {}).get("thread", {}).get("id", conversation_ref))
        return self.thread_id

    async def send_turn(self, user_message: str) -> AsyncIterator[AgentEvent]:
        if self.process is None:
            raise RuntimeError("coding_agent_unavailable")
        self.current_turn_id = None
        turn_request_id = self._next_id()
        await self._send({"jsonrpc": "2.0", "id": turn_request_id, "method": "turn/start", "params": {"threadId": self.thread_id, "input": [{"type": "text", "text": user_message}]}})
        self._reset_protocol_health()
        turn_status = "inProgress"
        self._poll_requests.clear()
        self._poll_task = asyncio.create_task(self._poll_loop())
        read_task = asyncio.create_task(self._read_message())
        try:
            while True:
                assert self._protocol_failure_event is not None
                health_task = asyncio.create_task(self._protocol_failure_event.wait())
                done, _ = await asyncio.wait({read_task, health_task}, return_when=asyncio.FIRST_COMPLETED)
                if health_task in done and health_task.result() and not read_task.done():
                    read_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await read_task
                    self._protocol_failure_reported = True
                    yield AgentEvent("agent_stalled", self._protocol_failure_payload())
                    break
                health_task.cancel()
                with suppress(asyncio.CancelledError):
                    await health_task
                try:
                    message = read_task.result()
                except Exception as exc:
                    yield AgentEvent(
                        "agent_error",
                        {
                            "code": "coding_agent_protocol_error",
                            "source": "protocol_health",
                            "message": str(exc)[:1024],
                        },
                    )
                    break
                read_task = asyncio.create_task(self._read_message())
                message_id = message.get("id")
                # JSON-RPC requests/notifications from app-server can carry an
                # ``id`` from the same numeric namespace as our outbound
                # requests.  Route messages by method first; otherwise a
                # dynamic tool call whose id happens to equal ``turn/start``
                # would be mistaken for that response and silently dropped.
                has_method = "method" in message
                if not has_method and message_id == turn_request_id:
                    if "error" in message:
                        yield AgentEvent("agent_error", self._rpc_error_reason(message.get("error")))
                        break
                    else:
                        self._mark_protocol_heartbeat()
                        started_turn = message.get("result", {}).get("turn", {}) if isinstance(message.get("result"), dict) else {}
                        if isinstance(started_turn, dict):
                            if started_turn.get("id"):
                                self.current_turn_id = str(started_turn["id"])
                            if started_turn.get("status"):
                                turn_status = str(started_turn["status"])
                            if started_turn.get("error"):
                                yield AgentEvent("agent_error", self._turn_error_reason(started_turn["error"], source="turn/start"))
                                break
                    continue
                if not has_method and isinstance(message_id, int) and message_id in self._poll_requests:
                    poll_kind, _sent_at = self._poll_requests.pop(message_id)
                    if "error" in message:
                        failed = self._mark_protocol_failure(poll_kind, self._error_message(message.get("error")))
                        yield AgentEvent("agent_warning", {"source": poll_kind, "message": self._error_message(message.get("error"))})
                        if failed:
                            self._protocol_failure_reported = True
                            yield AgentEvent("agent_stalled", self._protocol_failure_payload())
                            break
                        continue
                    self._mark_protocol_heartbeat()
                    poll_result = message.get("result", {})
                    if poll_kind == "thread/read":
                        params = self._thread_status(poll_result)
                        if params.get("turn_id"):
                            self.current_turn_id = params["turn_id"]
                        if params.get("turn_status"):
                            turn_status = params["turn_status"]
                        yield AgentEvent("thread_status", params)
                        if params.get("error"):
                            yield AgentEvent("agent_error", params["error"])
                            break
                        if params.get("turn_status") == "failed":
                            yield AgentEvent(
                                "agent_error",
                                {
                                    "code": "coding_agent_turn_failed",
                                    "source": "thread/read",
                                    "message": "Codex turn failed",
                                    "turn_id": params.get("turn_id") or self.current_turn_id,
                                },
                            )
                            break
                        if params["status"] == "systemError":
                            yield AgentEvent("agent_error", {"code": "thread_system_error", "source": "thread/read", "message": "Codex thread entered systemError"})
                            break
                    elif poll_kind == "thread/goal/get":
                        goal = poll_result.get("goal") if isinstance(poll_result, dict) else None
                        goal_status = goal.get("status") if isinstance(goal, dict) else None
                        if goal_status in {"blocked", "usageLimited", "budgetLimited"}:
                            goal_code = {
                                "blocked": "coding_agent_blocked",
                                "usageLimited": "coding_agent_usage_limited",
                                "budgetLimited": "coding_agent_budget_limited",
                            }[goal_status]
                            yield AgentEvent(
                                "agent_error",
                                {
                                    "code": goal_code,
                                    "source": "thread/goal/get",
                                    "goal_status": goal_status,
                                    "message": f"Codex goal status is {goal_status}",
                                },
                            )
                            break
                        continue
                if not has_method:
                    # Unknown responses belong to a request handled by another
                    # lifecycle phase. They are not notifications and therefore
                    # do not count as a protocol heartbeat for this turn.
                    continue
                method = str(message.get("method", ""))
                params = message.get("params", {})
                self._mark_protocol_heartbeat()
                if method == "item/tool/call":
                    await self._handle_dynamic_tool_call(message)
                    yield AgentEvent("tool_call", params)
                    continue
                if method == "turn/completed":
                    turn = params.get("turn", params)
                    turn_id = turn.get("id") if isinstance(turn, dict) else None
                    if turn_id is None:
                        turn_id = params.get("turnId") or params.get("turn_id")
                    if turn_id is not None:
                        self.current_turn_id = str(turn_id)
                    status = turn.get("status") if isinstance(turn, dict) else None
                    status = status or params.get("status")
                    turn_status = str(status or "completed")
                    if isinstance(turn, dict) and turn.get("error"):
                        yield AgentEvent("agent_error", self._turn_error_reason(turn["error"], source="turn/completed"))
                    elif turn_status == "failed":
                        yield AgentEvent("agent_error", {"code": "coding_agent_turn_failed", "source": "turn/completed", "message": "Codex turn failed"})
                    if turn_status in {"interrupted", "cancelled", "canceled"}:
                        yield AgentEvent("turn_interrupted", {"turn_id": self.current_turn_id} if self.current_turn_id else {})
                    break
                if method == "turn/started":
                    turn = params.get("turn", params)
                    turn_id = turn.get("id") if isinstance(turn, dict) else None
                    if turn_id is None:
                        turn_id = params.get("turnId") or params.get("turn_id")
                    if turn_id is not None:
                        self.current_turn_id = str(turn_id)
                    turn_status = str((turn.get("status") if isinstance(turn, dict) else None) or "inProgress")
                    yield AgentEvent("turn_started", {"turn_id": self.current_turn_id} if self.current_turn_id else {})
                    if isinstance(turn, dict) and turn.get("error"):
                        yield AgentEvent("agent_error", self._turn_error_reason(turn["error"], source="turn/started"))
                        break
                    continue
                if method == "thread/status/changed":
                    status_payload = self._status_payload(params.get("status"))
                    yield AgentEvent("thread_status", {**status_payload, "thread_id": params.get("threadId") or self.thread_id})
                    if status_payload["status"] == "systemError":
                        yield AgentEvent("agent_error", {"code": "thread_system_error", "source": method, "message": "Codex thread entered systemError"})
                        break
                    continue
                if method == "thread/goal/updated":
                    goal = params.get("goal", params) if isinstance(params, dict) else {}
                    goal_status = goal.get("status") if isinstance(goal, dict) else None
                    if goal_status in {"blocked", "usageLimited", "budgetLimited"}:
                        goal_code = {
                            "blocked": "coding_agent_blocked",
                            "usageLimited": "coding_agent_usage_limited",
                            "budgetLimited": "coding_agent_budget_limited",
                        }[goal_status]
                        yield AgentEvent(
                            "agent_error",
                            {
                                "code": goal_code,
                                "source": method,
                                "goal_status": goal_status,
                                "message": f"Codex goal status is {goal_status}",
                            },
                        )
                        break
                    if isinstance(goal, dict):
                        yield AgentEvent(
                            "goal_status",
                            {
                                "source": method,
                                "status": goal.get("status"),
                                "thread_id": goal.get("threadId") or self.thread_id,
                                "updated_at": goal.get("updatedAt"),
                                "token_budget": goal.get("tokenBudget"),
                                "tokens_used": goal.get("tokensUsed"),
                                "time_used_seconds": goal.get("timeUsedSeconds"),
                            },
                        )
                    else:
                        yield AgentEvent("goal_status", {"source": method})
                    continue
                if method == "error":
                    reason = self._turn_error_reason(params.get("error") or params, source=method)
                    reason["will_retry"] = bool(params.get("willRetry", False)) if isinstance(params, dict) else False
                    yield AgentEvent("agent_warning" if reason["will_retry"] else "agent_error", reason)
                    continue
                if method == "warning":
                    yield AgentEvent("agent_warning", {"source": method, **(params if isinstance(params, dict) else {"message": str(params)})})
                    continue
                if "codexErrorInfo" in method or method == "message/codexErrorInfo":
                    info_payload = dict(params) if isinstance(params, dict) else {"message": str(params)}
                    info_payload["codex_error_info"] = info_payload.get("codexErrorInfo") or info_payload.get("codex_error_info")
                    info_payload.setdefault("message", "Codex reported codexErrorInfo")
                    yield AgentEvent("agent_error", {"code": "codex_error_info", "source": method, **info_payload})
                    continue
                if method.startswith("thread/"):
                    yield AgentEvent(method, params)
                    continue
                if method == "item/completed":
                    item = params.get("item", {})
                    if item.get("type") == "agentMessage" and item.get("text"):
                        yield AgentEvent("assistant_text", {"text": item["text"]})
                    continue
                if method.startswith("item/") or method.startswith("turn/"):
                    yield AgentEvent(method, params)
        finally:
            if not read_task.done():
                read_task.cancel()
            with suppress(asyncio.CancelledError):
                await read_task
            if self._poll_task is not None:
                self._poll_task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await self._poll_task
                self._poll_task = None
            self._poll_requests.clear()

    async def _poll_loop(self) -> None:
        """Send bounded status reads; protocol liveness is independent of content progress."""
        interval = max(0.001, self.poll_interval_seconds)
        while True:
            await asyncio.sleep(interval)
            if self.process is None or not self.thread_id:
                continue
            now = self.clock()
            for request_id, (method, sent_at) in tuple(self._poll_requests.items()):
                if now - sent_at >= max(0.0, self.protocol_failure_seconds):
                    self._mark_protocol_failure(method, "app-server poll response timed out")
            for method, params in (
                ("thread/read", {"threadId": self.thread_id, "includeTurns": True}),
                ("thread/goal/get", {"threadId": self.thread_id}),
            ):
                if any(request_method == method for request_method, _sent_at in self._poll_requests.values()):
                    continue
                request_id = self._next_id()
                self._poll_requests[request_id] = (method, now)
                try:
                    await self._send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
                except Exception as exc:
                    self._poll_requests.pop(request_id, None)
                    self._mark_protocol_failure(method, str(exc))

    def _reset_protocol_health(self) -> None:
        self.last_protocol_heartbeat_at = self.clock()
        self.poll_failure_started_at = None
        self._protocol_failure_reason = None
        self._protocol_failure_event = asyncio.Event()
        self._protocol_failure_reported = False

    def _mark_protocol_heartbeat(self) -> None:
        self.last_protocol_heartbeat_at = self.clock()
        if self._protocol_failure_event is not None and not self._protocol_failure_reported:
            self._protocol_failure_event.clear()
            self.poll_failure_started_at = None
            self._protocol_failure_reason = None

    def _mark_protocol_failure(self, source: str, message: str) -> bool:
        now = self.clock()
        if self.poll_failure_started_at is None:
            self.poll_failure_started_at = now
        self._protocol_failure_reason = {"source": source, "message": message[:1024]}
        failed = now - self.poll_failure_started_at >= max(0.0, self.protocol_failure_seconds)
        if failed and self._protocol_failure_event is not None and not self._protocol_failure_reported:
            self._protocol_failure_event.set()
        return failed

    def _protocol_failure_payload(self) -> dict[str, Any]:
        started = self.poll_failure_started_at if self.poll_failure_started_at is not None else self.clock()
        reason = self._protocol_failure_reason or {"source": "protocol_health", "message": "app-server is not responding"}
        return {
            "code": "coding_agent_stalled",
            "source": "protocol_health",
            "thread_id": self.thread_id,
            "turn_id": self.current_turn_id,
            "failure_source": reason.get("source"),
            "message": reason.get("message"),
            "failure_age_seconds": max(0.0, self.clock() - started),
        }

    @classmethod
    def _status_payload(cls, status: Any) -> dict[str, Any]:
        if isinstance(status, str):
            status_type = status
            flags: list[str] = []
        elif isinstance(status, dict):
            status_type = str(status.get("type") or status.get("status") or "")
            flags = [str(item) for item in (status.get("activeFlags") or status.get("active_flags") or [])]
        else:
            status_type = ""
            flags = []
        return {"status": status_type, "active_flags": flags}

    @classmethod
    def _thread_status(cls, result: Any) -> dict[str, Any]:
        thread = result.get("thread", {}) if isinstance(result, dict) else {}
        status_payload = cls._status_payload(thread.get("status"))
        turns = thread.get("turns") or []
        current_turn: dict[str, Any] | None = None
        for turn in turns:
            if isinstance(turn, dict) and turn.get("id") == cls._current_turn_hint(turns):
                current_turn = turn
                break
        if current_turn is None and turns:
            current_turn = turns[-1] if isinstance(turns[-1], dict) else None
        turn_id = str(current_turn.get("id")) if isinstance(current_turn, dict) and current_turn.get("id") else None
        turn_status = str(current_turn.get("status")) if isinstance(current_turn, dict) and current_turn.get("status") else None
        turn_error = cls._turn_error_reason(current_turn.get("error"), source="thread/read") if isinstance(current_turn, dict) and current_turn.get("error") else None
        return {
            **status_payload,
            "thread_id": thread.get("id"),
            "updated_at": thread.get("updatedAt"),
            "turn_id": turn_id,
            "turn_status": turn_status,
            "error": turn_error,
        }

    @staticmethod
    def _current_turn_hint(turns: list[Any]) -> str | None:
        for turn in turns:
            if isinstance(turn, dict) and str(turn.get("status", "")).lower() == "inprogress":
                return str(turn.get("id"))
        return None

    @classmethod
    def _turn_error_reason(cls, error: Any, source: str) -> dict[str, Any]:
        if isinstance(error, dict):
            reason = {
                "source": source,
                "message": str(error.get("message") or "Codex reported an error")[:1024],
                "code": str(error.get("code") or "coding_agent_error"),
            }
            info = error.get("codexErrorInfo")
            if info is not None:
                reason["codex_error_info"] = info
            details = error.get("additionalDetails")
            if details:
                reason["additional_details"] = str(details)[:512]
            return reason
        return {"code": "coding_agent_error", "source": source, "message": str(error or "Codex reported an error")[:1024]}

    @classmethod
    def _rpc_error_reason(cls, error: Any) -> dict[str, Any]:
        if isinstance(error, dict):
            return {
                "code": "coding_agent_rpc_error",
                "source": "rpc",
                "message": cls._error_message(error)[:1024],
                "rpc_code": error.get("code"),
            }
        return {"code": "coding_agent_rpc_error", "source": "rpc", "message": str(error)[:1024]}

    @staticmethod
    def _error_message(error: Any) -> str:
        if isinstance(error, dict):
            return str(error.get("message") or error.get("data") or error.get("code") or "Codex request failed")
        return str(error or "Codex request failed")

    async def interrupt(self, turn_ref: str | None = None) -> None:
        turn_ref = turn_ref or self.current_turn_id
        if self.process is not None and turn_ref:
            await self._send(
                {
                    "jsonrpc": "2.0",
                    "id": self._next_id(),
                    "method": "turn/interrupt",
                    "params": {"threadId": self.thread_id, "turnId": turn_ref},
                }
            )

    async def close(self) -> None:
        if self.process is not None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.process.kill()
                await self.process.wait()
            self.process = None
            self.thread_id = None
            self.current_turn_id = None
            self._read_buffer.clear()

    async def _handle_dynamic_tool_call(self, message: dict[str, Any]) -> None:
        request_id = message.get("id")
        params = message.get("params", {})
        tool_name = str(params.get("tool", ""))
        arguments = params.get("arguments", {})
        if not isinstance(arguments, dict):
            arguments = {}
        try:
            if self.tool_handler is None:
                raise RuntimeError("driver_mcp_unavailable")
            result = await self.tool_handler(tool_name, arguments)
            content = {"contentItems": [{"type": "inputText", "text": json.dumps(result, ensure_ascii=False)}], "success": True}
        except Exception as exc:
            content = {
                "contentItems": [{"type": "inputText", "text": json.dumps({"code": str(exc)}, ensure_ascii=False)}],
                "success": False,
            }
        await self._send({"jsonrpc": "2.0", "id": request_id, "result": content})

    def _next_id(self) -> int:
        self.request_id += 1
        return self.request_id

    async def _send(self, message: dict[str, Any]) -> None:
        if self.process is None or self.process.stdin is None:
            raise RuntimeError("coding_agent_unavailable")
        self.process.stdin.write((json.dumps(message) + "\n").encode())
        await self.process.stdin.drain()

    async def _read_message(self) -> dict[str, Any]:
        if self.process is None or self.process.stdout is None:
            raise RuntimeError("coding_agent_unavailable")
        # app-server uses a long-lived JSONL stream.  ``readline()`` delegates
        # to asyncio's StreamReader separator limit (64 KiB by default), so a
        # large tool result or reasoning item raises ``LimitOverrunError``
        # before we can decode it.  Read bounded chunks and apply our own
        # explicit message limit instead.
        while True:
            separator = self._read_buffer.find(b"\n")
            if separator >= 0:
                if separator > self.message_limit_bytes:
                    raise RuntimeError("coding_agent_message_too_large")
                line = bytes(self._read_buffer[:separator]).rstrip(b"\r")
                del self._read_buffer[: separator + 1]
                if not line.strip():
                    continue
                try:
                    message = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise RuntimeError("coding_agent_protocol_error") from exc
                if not isinstance(message, dict):
                    raise RuntimeError("coding_agent_protocol_error")
                return message
            if len(self._read_buffer) > self.message_limit_bytes:
                raise RuntimeError("coding_agent_message_too_large")
            chunk = await self.process.stdout.read(64 * 1024)
            if not chunk:
                raise RuntimeError("coding_agent_unavailable")
            self._read_buffer.extend(chunk)

    async def _read_response(self, request_id: int) -> dict[str, Any]:
        while True:
            message = await self._read_message()
            if message.get("id") != request_id:
                continue
            if "error" in message:
                raise RuntimeError("coding_agent_protocol_error")
            return message
