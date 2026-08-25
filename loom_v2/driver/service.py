from __future__ import annotations

from typing import Any
from uuid import uuid4

from loom_v2.coding_agents.base import CodingAgentProvider
from loom_v2.observer.repository import ObserverRepository

from .tools import DriverTools


class DriverService:
    def __init__(self, repository: ObserverRepository, provider: CodingAgentProvider) -> None:
        self.repository = repository
        self.provider = provider
        self.tools = DriverTools(repository)

    async def run_prompt(self, conversation_ref: str, prompt: str) -> dict[str, Any]:
        run = await self.repository.open_run(None, conversation_ref, prompt)
        await self.provider.start(conversation_ref, "/workspace")
        patches = 0
        committed: str | None = None
        try:
            async for event in self.provider.send_turn(prompt):
                if event.kind == "apply_plan_patch":
                    await self.tools.apply_plan_patch(run.run_id, f"patch-{uuid4().hex[:12]}", event.payload.get("ops", []))
                    patches += 1
                elif event.kind == "commit_plan":
                    committed = (await self.tools.commit_plan(run.run_id))["closure_version"]
            if committed is None:
                committed = (await self.tools.commit_plan(run.run_id))["closure_version"]
            execution = await self.tools.start_run(run.run_id, committed)
            return {"run_id": run.run_id, "closure_version": committed, "patches": patches, **execution}
        finally:
            await self.provider.close()
