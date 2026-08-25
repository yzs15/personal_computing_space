from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from loom_v2.contracts.terms import TermSupport

from .executor import ExecutionResult, execute_operation


@dataclass
class WorkspaceReplica:
    workspace_id: str
    state: str = "ready"
    digest: str = ""


@dataclass
class SlaveService:
    slave_id: str
    workspace_id: str = "workspace-default"
    replica: WorkspaceReplica = field(default_factory=lambda: WorkspaceReplica("workspace-default"))
    available: bool = True
    attempts: dict[str, ExecutionResult] = field(default_factory=dict)

    def term_support(self) -> list[TermSupport]:
        return [
            TermSupport(kind="loom.compute.capability.v1", schema_ref="loom.compute.capability/1", support={"parse", "preserve", "match", "validate", "enforce"}, execution_stages={"commit", "admission", "execute"}),
            TermSupport(kind="loom.compute.precision.v1", schema_ref="loom.compute.precision/1", support={"parse", "preserve", "validate"}, execution_stages={"commit", "admission"}),
        ]

    async def run(self, attempt_id: str, operation: str, payload: dict[str, Any]) -> ExecutionResult:
        if not self.available or self.replica.state != "ready":
            raise RuntimeError("slave_unavailable")
        if attempt_id in self.attempts:
            return self.attempts[attempt_id]
        result = await execute_operation(operation, payload)
        self.attempts[attempt_id] = result
        return result
