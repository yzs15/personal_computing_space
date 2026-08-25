from __future__ import annotations

import os
import asyncio
from typing import Any
from uuid import uuid4

from loom_v2.coding_agents.base import CodingAgentProvider
from loom_v2.observer.repository import ObserverRepository
from loom_v2.slave.executor import ExecutionResult, execute_operation

from .tools import DriverTools


class DriverService:
    def __init__(self, repository: ObserverRepository, provider: CodingAgentProvider, executor=execute_operation) -> None:
        self.repository = repository
        self.provider = provider
        self.tools = DriverTools(repository)
        self.executor = executor

    async def run_prompt(self, conversation_ref: str, prompt: str) -> dict[str, Any]:
        timeout_seconds = float(os.getenv("LOOM_CODING_AGENT_TIMEOUT_SECONDS", "90"))
        try:
            async with asyncio.timeout(timeout_seconds):
                return await self._run_prompt(conversation_ref, prompt)
        except TimeoutError as exc:
            raise RuntimeError("coding_agent_timeout") from exc

    async def _run_prompt(self, conversation_ref: str, prompt: str) -> dict[str, Any]:
        run = await self.repository.open_run(None, conversation_ref, prompt)
        await self.repository.append_message(run.run_id, "user", prompt)
        await self.provider.start(conversation_ref, os.getenv("LOOM_WORKSPACE_ROOT", "/workspace"))
        patches = 0
        committed: str | None = None
        assistant_parts: list[str] = []
        try:
            async for event in self.provider.send_turn(prompt):
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
            if committed is None:
                committed = (await self.tools.commit_plan(run.run_id))["closure_version"]
            execution = await self.tools.start_run(run.run_id, committed)
            result: ExecutionResult = await self.executor("echo", {"text": prompt})
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
                "resource_ref": result.resource_ref.resource_id,
                "execution_id": execution["execution_id"],
                "execution_epoch": execution["execution_epoch"],
                "assistant_text": "\n\n".join(assistant_parts),
            }
        finally:
            await self.provider.close()
