from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from copy import deepcopy
import json
from typing import Any
from uuid import uuid4

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from loom_v2.contracts.constraints import Constraint
from loom_v2.contracts.refinement import is_monotonic_tightening
from loom_v2.contracts.types import ClosureContract, ClosureVersion, ComputeBinding, ComputeSpec, TaskClosure, TypedHole
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
    readiness: dict[str, Any] = field(default_factory=dict)


@dataclass
class RunRecord:
    run_id: str
    task_ref: str
    goal: str
    draft: ClosureVersion
    closure_contract: ClosureContract | None = None
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
        self.slave_capabilities: dict[str, dict[str, Any]] = {
            "slave-a": {"operations": {"echo", "hash", "sort"}},
            "slave-b": {"operations": {"echo", "hash", "sort"}},
        }
        self.engine = engine
        self.sessions = make_session_factory(engine) if engine is not None else None

    async def init_db(self) -> None:
        if self.engine is None:
            return
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
            if connection.dialect.name == "postgresql":
                await connection.execute(text("ALTER TABLE runs ADD COLUMN IF NOT EXISTS closure_contract JSONB"))

    @staticmethod
    def _record_from_row(row: RunRow) -> RunRecord:
        draft_payload = ObserverRepository._normalize_version_payload(row.draft)
        committed_payload = ObserverRepository._normalize_version_payload(row.committed) if row.committed else None
        contract_payload = ObserverRepository._normalize_contract_payload(row.closure_contract) if row.closure_contract else None
        return RunRecord(
            run_id=row.run_id,
            task_ref=row.task_ref,
            goal=row.goal,
            draft=ClosureVersion.model_validate(draft_payload),
            closure_contract=ClosureContract.model_validate(contract_payload) if contract_payload else None,
            allow_reassignment=row.allow_reassignment,
            committed=ClosureVersion.model_validate(committed_payload) if committed_payload else None,
            execution_id=row.execution_id,
            execution_epoch=row.execution_epoch,
            state=row.state,
            outcome=row.outcome,
            attempts=row.attempts or [],
            events=row.events or [],
        )

    @staticmethod
    def _normalize_task_closure_payload(payload: dict[str, Any]) -> dict[str, Any]:
        normalized = deepcopy(payload)
        for section in ("program", "compute"):
            values = normalized.get(section)
            if not isinstance(values, dict):
                continue
            operation_ref = values.get("operation_ref")
            if isinstance(operation_ref, dict):
                values["operation_ref"] = (
                    operation_ref.get("program_ref")
                    or operation_ref.get("operation_ref")
                    or operation_ref.get("ref")
                    or ""
                )
        return normalized

    @classmethod
    def _normalize_version_payload(cls, payload: dict[str, Any] | None) -> dict[str, Any] | None:
        if payload is None:
            return None
        normalized = deepcopy(payload)
        snapshot = normalized.get("snapshot")
        if isinstance(snapshot, dict):
            normalized["snapshot"] = cls._normalize_task_closure_payload(snapshot)
        return normalized

    @classmethod
    def _normalize_contract_payload(cls, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = deepcopy(payload)
        body = normalized.get("body")
        if isinstance(body, dict):
            normalized["body"] = cls._normalize_task_closure_payload(body)
        return normalized

    async def _persist(self, record: RunRecord) -> None:
        if self.sessions is None:
            return
        async with self.sessions() as session:
            row = await session.get(RunRow, record.run_id)
            values = {
                "run_id": record.run_id,
                "task_ref": record.task_ref,
                "goal": record.goal,
                "closure_contract": record.closure_contract.model_dump(mode="json") if record.closure_contract else None,
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
            record = self._record_from_row(row)
            self.runs[run_id] = record
            return record

    async def _all_records(self) -> list[RunRecord]:
        if self.sessions is None:
            return list(self.runs.values())
        async with self.sessions() as session:
            rows = (await session.scalars(select(RunRow).order_by(RunRow.run_id))).all()
        records: list[RunRecord] = []
        for row in rows:
            record = self.runs.get(row.run_id)
            if record is None:
                record = self._record_from_row(row)
                self.runs[row.run_id] = record
            records.append(record)
        return records

    @staticmethod
    def _record_order_key(record: RunRecord) -> str:
        timestamps = [event.get("created_at") for event in record.events if isinstance(event.get("created_at"), str)]
        return max(timestamps, default="")

    @staticmethod
    def _conversation_status_for_state(state: str) -> str:
        return {
            "opened": "idle",
            "closed": "idle",
            "thinking": "thinking",
            "committed": "executing",
            "running": "executing",
            "completed": "completed",
            "cancelled": "interrupted",
            "failed": "failed",
        }.get(state, "idle")

    @classmethod
    def _conversation_status(cls, records: list[RunRecord]) -> str:
        if not records:
            return "idle"
        latest = max(records, key=cls._record_order_key)
        return cls._conversation_status_for_state(latest.state)

    @staticmethod
    def _operation_name(operation_ref: str) -> str:
        if not operation_ref:
            return ""
        return operation_ref.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1]

    def _canonical_target_resource_id(self, resource_id: str) -> str:
        """Resolve common capability-URI spellings to a registered Slave id."""
        if resource_id in self.slave_capabilities:
            return resource_id
        normalized = resource_id.rstrip("/")
        for slave_id in self.slave_capabilities:
            if normalized.endswith(f"/{slave_id}") or normalized.endswith(f":{slave_id}"):
                return slave_id
        return resource_id

    def _evaluate_readiness(self, snapshot: TaskClosure) -> dict[str, Any]:
        operation_ref = snapshot.program.operation_ref or snapshot.compute.operation_ref
        operation = self._operation_name(operation_ref)
        blockers: list[dict[str, Any]] = []
        bindings_by_hole = {binding.hole_id: binding for binding in snapshot.compute_bindings}

        for hole in snapshot.compute.typed_holes:
            binding = bindings_by_hole.get(hole.hole_id)
            if hole.status != "bound" or not hole.binding_ref:
                blockers.append({"code": "typed_hole_unbound", "hole_id": hole.hole_id})
                continue
            if binding is None or binding.binding_id != hole.binding_ref:
                blockers.append({"code": "compute_binding_missing", "hole_id": hole.hole_id, "binding_ref": hole.binding_ref})
                continue
            target = self._canonical_target_resource_id(binding.target_resource_ref.resource_id)
            capability = self.slave_capabilities.get(target)
            if capability is None:
                blockers.append({"code": "capability_unavailable", "hole_id": hole.hole_id, "target_resource_ref": target})
                continue
            if not self.slave_availability.get(target, False):
                blockers.append({"code": "slave_unavailable", "hole_id": hole.hole_id, "target_resource_ref": target})
            if operation and operation not in capability.get("operations", set()):
                blockers.append({"code": "capability_unavailable", "operation": operation, "target_resource_ref": target})

        if operation and not snapshot.compute.typed_holes:
            default_target = "slave-a"
            capability = self.slave_capabilities.get(default_target, {})
            if not self.slave_availability.get(default_target, False):
                blockers.append({"code": "slave_unavailable", "target_resource_ref": default_target})
            if operation not in capability.get("operations", set()):
                blockers.append({"code": "capability_unavailable", "operation": operation, "target_resource_ref": default_target})

        return {
            "ready": not blockers,
            "operation_ref": operation_ref,
            "operation": operation,
            "blockers": blockers,
            "bindings": [binding.model_dump(mode="json") for binding in snapshot.compute_bindings],
        }

    @staticmethod
    def _readiness_error(readiness: dict[str, Any]) -> ValueError:
        return ValueError("readiness_blocked:" + json.dumps(readiness["blockers"], sort_keys=True, ensure_ascii=False))

    async def open_run(
        self,
        run_id: str | None,
        task_ref: str,
        goal: str,
        allow_reassignment: bool = False,
        closure_contract: ClosureContract | None = None,
        *,
        user_id: str = "user-default",
        workspace_id: str = "workspace-default",
    ) -> RunRecord:
        run_id = run_id or f"run-{uuid4().hex[:12]}"
        if closure_contract is None:
            snapshot = TaskClosure.minimal(closure_id=task_ref, metadata={"goal": goal})
            closure_contract = ClosureContract(
                closure_id=task_ref,
                goal=goal,
                origin_conversation_ref=task_ref,
                user_id=user_id,
                workspace_id=workspace_id,
                resource_budget={"max_node_concurrency": 1, "max_attempts": 1},
                recovery_policy={"allow_reassignment": allow_reassignment},
                body=snapshot,
            )
        else:
            if closure_contract.goal != goal:
                raise ValueError("closure_goal_mismatch")
            snapshot = closure_contract.body.model_copy(deep=True)
            snapshot.closure_id = closure_contract.closure_id
            snapshot.metadata.setdefault("goal", goal)
            closure_contract = closure_contract.model_copy(
                update={
                    "origin_conversation_ref": task_ref,
                    "user_id": user_id,
                    "workspace_id": workspace_id,
                    "body": snapshot,
                },
                deep=True,
            )
        version = ClosureVersion(
            version_id=f"draft-{uuid4().hex[:12]}",
            closure_id=closure_contract.closure_id,
            snapshot=snapshot,
            snapshot_digest=snapshot.canonical_digest(),
            patch_cursor=0,
        )
        record = RunRecord(
            run_id=run_id,
            task_ref=task_ref,
            goal=goal,
            draft=version,
            closure_contract=closure_contract,
            allow_reassignment=allow_reassignment,
        )
        record.events.append({"phase": "run_opened", "run_id": run_id, "created_at": datetime.now(timezone.utc).isoformat()})
        self.runs[run_id] = record
        await self._persist(record)
        return record

    async def append_message(self, run_id: str, role: str, content: str) -> dict[str, Any]:
        if role not in {"user", "assistant"}:
            raise ValueError("unsupported_message_role")
        if not content:
            raise ValueError("empty_message")
        record = await self._load(run_id)
        message = {
            "phase": "message",
            "message_id": f"message-{uuid4().hex[:12]}",
            "role": role,
            "content": content,
            "run_id": run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        record.events.append(message)
        await self._persist(record)
        return message

    async def begin_refinement(self, run_id: str) -> RunRecord:
        record = await self._load(run_id)
        if record.state == "opened":
            record.state = "thinking"
            record.events.append(
                {"phase": "refinement_started", "run_id": run_id, "created_at": datetime.now(timezone.utc).isoformat()}
            )
            await self._persist(record)
        return record

    async def inspect_readiness(self, run_id: str, version_id: str | None = None) -> dict[str, Any]:
        record = await self._load(run_id)
        if version_id is None or version_id == record.draft.version_id:
            snapshot = record.draft.snapshot
            inspected_version = record.draft.version_id
        elif record.committed is not None and version_id == record.committed.version_id:
            snapshot = record.committed.snapshot
            inspected_version = record.committed.version_id
        else:
            raise ValueError("version_conflict")
        readiness = self._evaluate_readiness(snapshot)
        record.events.append(
            {
                "phase": "readiness_inspected",
                "run_id": run_id,
                "version": inspected_version,
                "ready": readiness["ready"],
                "blockers": readiness["blockers"],
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        await self._persist(record)
        return {"run_id": run_id, "version": inspected_version, **readiness}

    async def cancel_run(self, run_id: str, reason: str = "user_interrupt") -> RunRecord:
        record = await self._load(run_id)
        if record.state in {"completed", "closed", "failed"}:
            return record
        record.state = "cancelled"
        record.outcome = {"reason": reason}
        record.events.append(
            {
                "phase": "run_cancelled",
                "run_id": run_id,
                "reason": reason,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        await self._persist(record)
        return record

    async def fail_run(self, run_id: str, reason: str) -> RunRecord:
        record = await self._load(run_id)
        if record.state in {"completed", "closed", "cancelled"}:
            return record
        record.state = "failed"
        record.outcome = {"reason": reason}
        record.events.append(
            {
                "phase": "run_failed",
                "run_id": run_id,
                "reason": reason,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        await self._persist(record)
        return record

    async def list_conversations(self) -> list[dict[str, Any]]:
        groups: dict[str, list[RunRecord]] = {}
        for record in await self._all_records():
            groups.setdefault(record.task_ref, []).append(record)
        summaries: list[tuple[str, dict[str, Any]]] = []
        for conversation_ref, records in groups.items():
            records.sort(key=self._record_order_key)
            summaries.append(
                (
                    self._record_order_key(records[-1]),
                    {
                        "conversation_ref": conversation_ref,
                        "title": records[-1].goal or "New conversation",
                        "run_count": len(records),
                        "latest_run_id": records[-1].run_id,
                        "status": self._conversation_status(records),
                    },
                )
            )
        return [summary for _, summary in sorted(summaries, key=lambda item: item[0])]

    async def get_conversation(self, conversation_ref: str) -> dict[str, Any]:
        records = [record for record in await self._all_records() if record.task_ref == conversation_ref]
        if not records:
            raise KeyError(conversation_ref)
        records.sort(key=self._record_order_key)
        messages: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        run_summaries: list[dict[str, Any]] = []
        for record in records:
            has_user_message = False
            for event in record.events:
                events.append(event)
                if event.get("phase") != "message" or event.get("role") not in {"user", "assistant"}:
                    continue
                message = {
                    "message_id": event.get("message_id", f"legacy-{record.run_id}"),
                    "role": event["role"],
                    "content": event.get("content", ""),
                    "run_id": event.get("run_id", record.run_id),
                }
                messages.append(message)
                has_user_message = has_user_message or message["role"] == "user"
            if not has_user_message:
                messages.append({"message_id": f"legacy-{record.run_id}", "role": "user", "content": record.goal, "run_id": record.run_id})
            run_summaries.append(
                {
                    "run_id": record.run_id,
                    "state": record.state,
                    "status": self._conversation_status_for_state(record.state),
                    "committed_version": record.committed.version_id if record.committed else None,
                    "execution_id": record.execution_id,
                    "outcome": record.outcome,
                }
            )
        return {
            "conversation_ref": conversation_ref,
            "status": self._conversation_status(records),
            "messages": messages,
            "runs": run_summaries,
            "events": events,
        }

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
            # ``kind`` is the canonical wire field.  ``op``/``operation``
            # are accepted as equivalent spellings because JSON tool callers
            # commonly emit one of those names when translating a plan.
            kind = operation.get("kind") or operation.get("op") or operation.get("operation")
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
                value = operation.get("value")
                if isinstance(value, dict):
                    value = value.get("program_ref") or value.get("operation_ref") or value.get("ref")
                if not isinstance(value, str) or not value:
                    raise ValueError("invalid_program_ref")
                snapshot.program.operation_ref = value
            elif kind == "add_typed_hole":
                hole = TypedHole.model_validate(operation["value"])
                if any(existing.hole_id == hole.hole_id for existing in snapshot.compute.typed_holes):
                    raise ValueError(f"duplicate_typed_hole:{hole.hole_id}")
                snapshot.compute.typed_holes.append(hole)
            elif kind == "bind_compute_hole":
                value = operation.get("value", {})
                if isinstance(value, dict) and "binding" in value:
                    value = value["binding"]
                binding = ComputeBinding.model_validate(value)
                target_resource_id = self._canonical_target_resource_id(binding.target_resource_ref.resource_id)
                if target_resource_id != binding.target_resource_ref.resource_id:
                    binding.target_resource_ref = binding.target_resource_ref.model_copy(update={"resource_id": target_resource_id})
                hole = next((item for item in snapshot.compute.typed_holes if item.hole_id == binding.hole_id), None)
                if hole is None:
                    raise ValueError(f"typed_hole_not_found:{binding.hole_id}")
                snapshot.compute_bindings = [item for item in snapshot.compute_bindings if item.hole_id != binding.hole_id]
                snapshot.compute_bindings.append(binding)
                hole.status = "bound"
                hole.binding_ref = binding.binding_id
            elif kind == "set_execution_payload":
                payload = operation.get("value", {})
                if not isinstance(payload, dict):
                    raise ValueError("invalid_execution_payload")
                snapshot.metadata["execution_payload"] = payload
            else:
                raise ValueError(f"unsupported_patch:{kind}")
        new_version = ClosureVersion(
            version_id=f"draft-{uuid4().hex[:12]}",
            closure_id=record.draft.closure_id,
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
        receipt.readiness = self._evaluate_readiness(snapshot)
        self.idempotency[operation_id] = receipt
        record.events.append({"phase": "draft_patched", "operation_id": operation_id, "version": new_version.version_id})
        if self.sessions is not None:
            async with self.sessions() as session:
                session.add(IdempotencyRow(operation_id=operation_id, receipt={"receipt": receipt.receipt, "run_id": receipt.run_id, "kind": receipt.kind, "draft_version": receipt.draft_version, "draft_digest": receipt.draft_digest, "patch_cursor": receipt.patch_cursor, "readiness": receipt.readiness}))
                await session.commit()
        await self._persist(record)
        return receipt

    async def commit(self, run_id: str, version_id: str, digest: str) -> ClosureVersion:
        record = await self._load(run_id)
        if record.draft.version_id != version_id or record.draft.snapshot_digest != digest:
            raise ValueError("version_conflict")
        readiness = self._evaluate_readiness(record.draft.snapshot)
        record.events.append(
            {
                "phase": "readiness_inspected",
                "run_id": run_id,
                "version": record.draft.version_id,
                "ready": readiness["ready"],
                "blockers": readiness["blockers"],
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        if not readiness["ready"]:
            await self._persist(record)
            raise self._readiness_error(readiness)
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
        readiness = self._evaluate_readiness(record.committed.snapshot)
        record.events.append(
            {
                "phase": "readiness_inspected",
                "run_id": run_id,
                "version": record.committed.version_id,
                "ready": readiness["ready"],
                "blockers": readiness["blockers"],
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        if not readiness["ready"]:
            await self._persist(record)
            raise self._readiness_error(readiness)
        if record.execution_id:
            return {"execution_id": record.execution_id, "state": record.state, "execution_epoch": record.execution_epoch}
        record.execution_id = f"execution-{uuid4().hex[:12]}"
        record.state = "running"
        target = "slave-a"
        if record.committed.snapshot.compute_bindings:
            target = record.committed.snapshot.compute_bindings[0].target_resource_ref.resource_id
        record.attempts.append({"attempt_id": f"attempt-{uuid4().hex[:12]}", "target": target, "state": "created"})
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

    async def set_slave_capabilities(self, slave_id: str, operations: set[str]) -> None:
        self.slave_capabilities.setdefault(slave_id, {})["operations"] = set(operations)

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
