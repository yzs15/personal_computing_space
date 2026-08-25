from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from loom_v2.contracts.constraints import Constraint
from loom_v2.contracts.refinement import is_monotonic_tightening
from loom_v2.contracts.types import ClosureVersion, ComputeSpec, TaskClosure, TypedHole
from loom_v2.db.base import Base
from loom_v2.db.models import IdempotencyRow, RunRow
from loom_v2.db.session import make_session_factory


@dataclass
class PatchReceipt:
    receipt: str
    run_id: str
    kind: str
    draft_version: str
    draft_digest: str
    snapshot: TaskClosure
    patch_cursor: int


@dataclass
class RunRecord:
    run_id: str
    task_ref: str
    goal: str
    draft: ClosureVersion
    allow_reassignment: bool = False
    committed: ClosureVersion | None = None
    execution_id: str | None = None
    execution_epoch: int = 1
    state: str = "opened"
    outcome: dict[str, Any] | None = None
    events: list[dict[str, Any]] = field(default_factory=list)
    attempts: list[dict[str, Any]] = field(default_factory=list)

    @property
    def draft_version(self) -> str:
        return self.draft.version_id

    @property
    def draft_digest(self) -> str:
        return self.draft.snapshot_digest


class ObserverRepository:
    """Deterministic state authority; SQL persistence is added behind this boundary."""

    def __init__(self, engine: AsyncEngine | None = None) -> None:
        self.runs: dict[str, RunRecord] = {}
        self.idempotency: dict[str, PatchReceipt] = {}
        self.slave_availability: dict[str, bool] = {"slave-a": True, "slave-b": True}
        self.engine = engine
        self.sessions = make_session_factory(engine) if engine is not None else None

    async def init_db(self) -> None:
        if self.engine is None:
            return
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    async def _persist(self, record: RunRecord) -> None:
        if self.sessions is None:
            return
        async with self.sessions() as session:
            row = await session.get(RunRow, record.run_id)
            values = {
                "run_id": record.run_id,
                "task_ref": record.task_ref,
                "goal": record.goal,
                "allow_reassignment": record.allow_reassignment,
                "draft": record.draft.model_dump(mode="json"),
                "committed": record.committed.model_dump(mode="json") if record.committed else None,
                "execution_id": record.execution_id,
                "execution_epoch": record.execution_epoch,
                "state": record.state,
                "outcome": record.outcome,
                "attempts": record.attempts,
                "events": record.events,
            }
            if row is None:
                row = RunRow(**values)
                session.add(row)
            else:
                for key, value in values.items():
                    setattr(row, key, value)
            await session.commit()

    async def _load(self, run_id: str) -> RunRecord:
        if run_id in self.runs:
            return self.runs[run_id]
        if self.sessions is None:
            raise KeyError(run_id)
        async with self.sessions() as session:
            row = await session.get(RunRow, run_id)
            if row is None:
                raise KeyError(run_id)
            record = RunRecord(
                run_id=row.run_id,
                task_ref=row.task_ref,
                goal=row.goal,
                draft=ClosureVersion.model_validate(row.draft),
                allow_reassignment=row.allow_reassignment,
                committed=ClosureVersion.model_validate(row.committed) if row.committed else None,
                execution_id=row.execution_id,
                execution_epoch=row.execution_epoch,
                state=row.state,
                outcome=row.outcome,
                attempts=row.attempts or [],
                events=row.events or [],
            )
            self.runs[run_id] = record
            return record

    async def open_run(self, run_id: str | None, task_ref: str, goal: str, allow_reassignment: bool = False) -> RunRecord:
        run_id = run_id or f"run-{uuid4().hex[:12]}"
        snapshot = TaskClosure.minimal(closure_id=task_ref, metadata={"goal": goal})
        version = ClosureVersion(
            version_id=f"draft-{uuid4().hex[:12]}",
            closure_id=task_ref,
            snapshot=snapshot,
            snapshot_digest=snapshot.canonical_digest(),
            patch_cursor=0,
        )
        record = RunRecord(run_id=run_id, task_ref=task_ref, goal=goal, allow_reassignment=allow_reassignment, draft=version)
        self.runs[run_id] = record
        await self._persist(record)
        return record

    async def apply_patch(
        self,
        run_id: str,
        base_draft_version: str,
        base_snapshot_digest: str,
        operation_id: str,
        ops: list[dict[str, Any]],
    ) -> PatchReceipt:
        if operation_id in self.idempotency:
            return self.idempotency[operation_id]
        record = await self._load(run_id)
        if record.draft.version_id != base_draft_version or record.draft.snapshot_digest != base_snapshot_digest:
            raise ValueError("version_conflict")
        snapshot = record.draft.snapshot.model_copy(deep=True)
        for operation in ops:
            kind = operation.get("kind")
            if kind == "set_result_expectation":
                snapshot.metadata["result_expectation"] = operation.get("value", {})
            elif kind == "set_compute_spec":
                snapshot.compute = ComputeSpec.model_validate(operation["value"])
            elif kind == "add_constraint":
                snapshot.constraints.append(Constraint.model_validate(operation["value"]))
            elif kind == "tighten_constraint":
                value = operation["value"]
                if not is_monotonic_tightening(value["parent"], value["child"]):
                    raise ValueError("constraint_not_monotonic")
                snapshot.metadata.setdefault("refined_constraints", []).append(value["child"])
            elif kind == "set_program_ref":
                snapshot.program.operation_ref = operation["value"]
            elif kind == "add_typed_hole":
                snapshot.compute.typed_holes.append(TypedHole.model_validate(operation["value"]))
            elif kind == "bind_compute_hole":
                snapshot.metadata.setdefault("patch_ops", []).append(operation)
            else:
                raise ValueError(f"unsupported_patch:{kind}")
        new_version = ClosureVersion(
            version_id=f"draft-{uuid4().hex[:12]}",
            closure_id=record.task_ref,
            parent_version=record.draft.version_id,
            snapshot=snapshot,
            snapshot_digest=snapshot.canonical_digest(),
            patch_cursor=record.draft.patch_cursor + 1,
        )
        record.draft = new_version
        receipt = PatchReceipt(
            receipt=f"receipt-{uuid4().hex[:12]}",
            run_id=run_id,
            kind="draft",
            draft_version=new_version.version_id,
            draft_digest=new_version.snapshot_digest,
            snapshot=snapshot,
            patch_cursor=new_version.patch_cursor,
        )
        self.idempotency[operation_id] = receipt
        record.events.append({"phase": "draft_patched", "operation_id": operation_id, "version": new_version.version_id})
        if self.sessions is not None:
            async with self.sessions() as session:
                session.add(IdempotencyRow(operation_id=operation_id, receipt={"receipt": receipt.receipt, "run_id": receipt.run_id, "kind": receipt.kind, "draft_version": receipt.draft_version, "draft_digest": receipt.draft_digest, "patch_cursor": receipt.patch_cursor}))
                await session.commit()
        await self._persist(record)
        return receipt

    async def commit(self, run_id: str, version_id: str, digest: str) -> ClosureVersion:
        record = await self._load(run_id)
        if record.draft.version_id != version_id or record.draft.snapshot_digest != digest:
            raise ValueError("version_conflict")
        committed = record.draft.model_copy(update={"kind": "committed", "version_id": f"committed-{uuid4().hex[:12]}"})
        record.committed = committed
        record.state = "committed"
        record.events.append({"phase": "committed", "version": committed.version_id})
        await self._persist(record)
        return committed

    async def start(self, run_id: str, version_id: str) -> dict[str, Any]:
        record = await self._load(run_id)
        if record.committed is None or record.committed.version_id != version_id:
            raise ValueError("closure_not_committed")
        if record.execution_id:
            return {"execution_id": record.execution_id, "state": record.state, "execution_epoch": record.execution_epoch}
        record.execution_id = f"execution-{uuid4().hex[:12]}"
        record.state = "running"
        record.attempts.append({"attempt_id": f"attempt-{uuid4().hex[:12]}", "target": "slave-a", "state": "created"})
        record.events.append({"phase": "execution_started", "execution_id": record.execution_id})
        await self._persist(record)
        return {"execution_id": record.execution_id, "state": record.state, "execution_epoch": record.execution_epoch}

    async def get_run(self, run_id: str) -> RunRecord:
        return await self._load(run_id)

    async def record_result(self, run_id: str, result: dict[str, Any]) -> RunRecord:
        record = await self._load(run_id)
        if record.state != "running":
            raise ValueError("execution_not_running")
        record.outcome = result
        record.state = "completed"
        record.events.append({"phase": "execution_completed", "execution_id": record.execution_id, "resource_ref": result.get("resource_ref")})
        await self._persist(record)
        return record

    async def close_run(self, run_id: str) -> RunRecord:
        record = await self._load(run_id)
        if record.state not in {"completed", "failed", "cancelled"}:
            raise ValueError("run_not_terminal")
        record.state = "closed"
        record.events.append({"phase": "run_closed", "execution_id": record.execution_id})
        await self._persist(record)
        return record

    async def set_slave_availability(self, slave_id: str, available: bool) -> None:
        self.slave_availability[slave_id] = available

    async def reconcile(self, run_id: str) -> RunRecord:
        record = await self._load(run_id)
        if record.allow_reassignment and record.execution_id and record.attempts and not self.slave_availability.get("slave-a", True):
            if not any(attempt["target"] == "slave-b" for attempt in record.attempts):
                record.attempts.append({"attempt_id": f"attempt-{uuid4().hex[:12]}", "target": "slave-b", "state": "created", "reason": "slave_a_unavailable", "provenance": {"from": "slave-a", "to": "slave-b"}})
                record.events.append({"phase": "reassigned", "execution_id": record.execution_id, "from": "slave-a", "to": "slave-b"})
                await self._persist(record)
        return record

    async def terminal(self, payload: dict[str, Any]) -> dict[str, Any]:
        attempt = next((item for record in self.runs.values() for item in record.attempts if item["attempt_id"] == payload.get("attempt_id")), None)
        if attempt is None or payload.get("execution_epoch") != 2:
            raise ValueError("stale_execution_epoch")
        attempt["state"] = "completed"
        return {"accepted": True}
