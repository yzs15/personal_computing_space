from __future__ import annotations

from typing import Any

from loom_v2.observer.repository import ObserverRepository


class DriverTools:
    def __init__(self, repository: ObserverRepository) -> None:
        self.repository = repository

    async def apply_plan_patch(self, run_id: str, operation_id: str, ops: list[dict[str, Any]]) -> dict[str, Any]:
        run = await self.repository.get_run(run_id)
        receipt = await self.repository.apply_patch(run_id, run.draft_version, run.draft_digest, operation_id, ops)
        return {"receipt": receipt.receipt, "draft_version": receipt.draft_version, "draft_digest": receipt.draft_digest, "patch_cursor": receipt.patch_cursor}

    async def commit_plan(self, run_id: str) -> dict[str, Any]:
        run = await self.repository.get_run(run_id)
        version = await self.repository.commit(run_id, run.draft_version, run.draft_digest)
        return {"closure_version": version.version_id, "snapshot_digest": version.snapshot_digest}

    async def start_run(self, run_id: str, closure_version: str) -> dict[str, Any]:
        return await self.repository.start(run_id, closure_version)
