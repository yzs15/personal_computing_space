from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine

from loom_v2.contracts.terms import TermSupport
from loom_v2.contracts.types import ComputeBinding, ResourceRef, TaskClosure
from loom_v2.db.base import SlaveBase
from loom_v2.db.models import SlaveAttemptRow, SlaveReplicaRow
from loom_v2.db.session import make_session_factory

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
    supported_operations: set[str] = field(default_factory=lambda: {"echo", "hash", "sort"})
    engine: AsyncEngine | None = None

    def __post_init__(self) -> None:
        self.sessions = make_session_factory(self.engine) if self.engine is not None else None

    async def init_db(self) -> None:
        if self.engine is None:
            return
        async with self.engine.begin() as connection:
            await connection.run_sync(SlaveBase.metadata.create_all)
        async with self.sessions() as session:
            row = await session.get(SlaveReplicaRow, self.slave_id)
            if row is None:
                session.add(SlaveReplicaRow(slave_id=self.slave_id, workspace_id=self.workspace_id, state=self.replica.state, digest=self.replica.digest))
                await session.commit()

    def term_support(self) -> list[TermSupport]:
        return [
            TermSupport(kind="loom.compute.capability.v1", schema_ref="loom.compute.capability/1", support={"parse", "preserve", "match", "validate", "enforce"}, execution_stages={"commit", "admission", "execute"}),
            TermSupport(kind="loom.compute.precision.v1", schema_ref="loom.compute.precision/1", support={"parse", "preserve", "validate"}, execution_stages={"commit", "admission"}),
        ]

    async def run(
        self,
        attempt_id: str,
        operation: str,
        payload: dict[str, Any],
        *,
        closure: TaskClosure | None = None,
        binding: ComputeBinding | None = None,
    ) -> ExecutionResult:
        if not self.available or self.replica.state != "ready":
            raise RuntimeError("slave_unavailable")
        if operation not in self.supported_operations:
            raise RuntimeError(f"capability_unavailable:{operation}")
        if closure is not None:
            operation_ref = closure.program.operation_ref or closure.compute.operation_ref
            expected_operation = operation_ref.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1] if operation_ref else ""
            if expected_operation and expected_operation != operation:
                raise RuntimeError("operation_binding_mismatch")
            bindings = {item.hole_id: item for item in closure.compute_bindings}
            for hole in closure.compute.typed_holes:
                if hole.status != "bound" or not hole.binding_ref:
                    raise RuntimeError(f"typed_hole_unbound:{hole.hole_id}")
                bound = bindings.get(hole.hole_id)
                if bound is None or bound.binding_id != hole.binding_ref:
                    raise RuntimeError(f"compute_binding_missing:{hole.hole_id}")
            if binding is not None and binding.target_resource_ref.resource_id != self.slave_id:
                raise RuntimeError("binding_target_mismatch")
        if attempt_id in self.attempts:
            return self.attempts[attempt_id]
        if self.sessions is not None:
            async with self.sessions() as session:
                row = await session.get(SlaveAttemptRow, attempt_id)
                if row is not None and row.result is not None:
                    result_payload = row.result
                    result = ExecutionResult(
                        resource_ref=ResourceRef.model_validate(result_payload["resource_ref"]),
                        value=result_payload["value"],
                        replay_safety=result_payload["replay_safety"],
                        digest=result_payload["digest"],
                    )
                    self.attempts[attempt_id] = result
                    return result
        result = await execute_operation(operation, payload)
        self.attempts[attempt_id] = result
        if self.sessions is not None:
            async with self.sessions() as session:
                session.add(SlaveAttemptRow(
                    attempt_id=attempt_id,
                    slave_id=self.slave_id,
                    workspace_id=self.workspace_id,
                    operation=operation,
                    payload=payload,
                    state="completed",
                    result={
                        "resource_ref": result.resource_ref.model_dump(mode="json"),
                        "value": result.value,
                        "replay_safety": result.replay_safety,
                        "digest": result.digest,
                    },
                ))
                await session.commit()
        return result
