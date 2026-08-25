from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from typing import Any

from .base import AgentEvent


class CodexAppServerProvider:
    def __init__(self, model: str | None = None, executable: str = "codex") -> None:
        self.model = model or os.getenv("LOOM_CODEX_MODEL", "deepseek-v4-flash")
        self.executable = executable
        self.process: asyncio.subprocess.Process | None = None
        self.request_id = 0
        self.thread_id: str | None = None

    async def start(self, conversation_ref: str, workspace_root: str) -> str:
        try:
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
        await self._send({"jsonrpc": "2.0", "id": initialize_id, "method": "initialize", "params": {"clientInfo": {"name": "loom-v2", "version": "0.1.0"}}})
        await self._read_response(initialize_id)
        await self._send({"jsonrpc": "2.0", "method": "initialized", "params": {}})
        thread_request_id = self._next_id()
        await self._send({"jsonrpc": "2.0", "id": thread_request_id, "method": "thread/start", "params": {"model": self.model, "cwd": workspace_root, "metadata": {"conversation_ref": conversation_ref}}})
        response = await self._read_response(thread_request_id)
        self.thread_id = str(response.get("result", {}).get("thread", {}).get("id", conversation_ref))
        return self.thread_id

    async def send_turn(self, user_message: str) -> AsyncIterator[AgentEvent]:
        if self.process is None:
            raise RuntimeError("coding_agent_unavailable")
        turn_request_id = self._next_id()
        await self._send({"jsonrpc": "2.0", "id": turn_request_id, "method": "turn/start", "params": {"threadId": self.thread_id, "input": [{"type": "text", "text": user_message}]}})
        while True:
            message = await self._read_message()
            if message.get("id") == turn_request_id:
                continue
            method = message.get("method", "")
            if method == "turn/completed":
                break
            if method == "item/completed":
                item = message.get("params", {}).get("item", {})
                if item.get("type") == "agentMessage" and item.get("text"):
                    yield AgentEvent("assistant_text", {"text": item["text"]})
                continue
            if method.startswith("item/") or method.startswith("turn/"):
                yield AgentEvent(method, message.get("params", {}))

    async def interrupt(self, turn_ref: str) -> None:
        if self.process is not None:
            await self._send({"jsonrpc": "2.0", "id": self._next_id(), "method": "turn/interrupt", "params": {"turnId": turn_ref}})

    async def close(self) -> None:
        if self.process is not None:
            self.process.terminate()
            await self.process.wait()
            self.process = None
            self.thread_id = None

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
        line = await self.process.stdout.readline()
        if not line:
            raise RuntimeError("coding_agent_unavailable")
        try:
            return json.loads(line)
        except json.JSONDecodeError as exc:
            raise RuntimeError("coding_agent_protocol_error") from exc

    async def _read_response(self, request_id: int) -> dict[str, Any]:
        while True:
            message = await self._read_message()
            if message.get("id") != request_id:
                continue
            if "error" in message:
                raise RuntimeError("coding_agent_protocol_error")
            return message
