"""The narrow repository surface used by Driver planning tools."""

from __future__ import annotations

from typing import Any, Protocol

from loom_v2.contracts.types import ClosureVersion


class PlanRepository(Protocol):
    """Planning operations shared by DriverTools implementations."""

    async def get_run(self, run_id: str) -> Any: ...

    async def apply_patch(self, run_id: str, base_draft_version: str, base_snapshot_digest: str, operation_id: str, ops: list[dict[str, Any]]) -> Any: ...

    async def commit(self, run_id: str, version_id: str, digest: str) -> ClosureVersion: ...

    async def start(self, run_id: str, version_id: str) -> dict[str, Any]: ...

    async def inspect_readiness(self, run_id: str) -> dict[str, Any]: ...
