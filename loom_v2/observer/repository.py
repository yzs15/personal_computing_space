from __future__ import annotations

import ast
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from copy import deepcopy
import json
import hashlib
import hmac
import os
import shutil
from typing import Any
from uuid import uuid4

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from loom_v2.contracts.constraints import Constraint, ConstraintRef
from loom_v2.contracts.refinement import is_monotonic_tightening
from loom_v2.contracts.types import (
    CapabilityPackage,
    CapabilityPackageActivation,
    CapabilityPackageVersion,
    ClosureContract,
    ClosureVersion,
    ComputeBinding,
    ComputeSpec,
    DynamicNode,
    IoContract,
    NodeIntent,
    ResourceRef,
    TaskClosure,
    TypedHole,
    NodeInputBinding,
    ValidationEvidence,
)
from loom_v2.db.base import Base
from loom_v2.db.models import DriverRequestRow, DriverThreadRow, IdempotencyRow, MessageReceiptRow, RunRow, RuntimeAgentRow
from loom_v2.contracts.agents import AgentLease, AgentRegistration, DriverCommand, DriverThreadBinding
from loom_v2.contracts.messages import MessageReceipt, MessageReceiptState, message_payload_digest
from loom_v2.db.session import make_session_factory
from loom_v2.content_store import ContentStore, canonical_json_bytes
from loom_v2.contracts.io_schema import ValidationError as SchemaValidationError, validate, validate_schema
from loom_v2.settings import Settings
from loom_v2.slave.executor import default_registry
from loom_v2.contracts.errors import DomainError, DomainErrorEnvelope
from loom_v2.observer.orchestration_preflight import (
    OrchestrationPreflightError,
    check_program,
    validate_entry_signature,
)


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
    capability_packages: list[CapabilityPackageVersion] = field(default_factory=list)
    capability_activations: list[CapabilityPackageActivation] = field(default_factory=list)
    dynamic_nodes: list[DynamicNode] = field(default_factory=list)

    @property
    def draft_version(self) -> str:
        return self.draft.version_id

    @property
    def draft_digest(self) -> str:
        return self.draft.snapshot_digest


RUN_TRANSITIONS: dict[str, dict[str, str]] = {
    "opened": {"refinement_started": "thinking", "run_cancelled": "cancelled"},
    "thinking": {"committed": "committed", "run_failed": "failed", "run_cancelled": "cancelled"},
    "committed": {"execution_started": "running", "run_cancelled": "cancelled"},
    "running": {
        "run_succeeded": "completed",
        "run_needs_decision": "awaiting_decision",
        "run_failed": "failed",
        "run_cancelled": "cancelled",
    },
    "awaiting_decision": {
        "decision_accepted": "completed",
        "decision_abandoned": "failed",
        "refinement_started": "thinking",
        "run_cancelled": "cancelled",
    },
    "completed": {"run_closed": "closed"},
    "failed": {"run_closed": "closed"},
    "cancelled": {"run_closed": "closed"},
    "closed": {},
}


def _select_slave_agents(agents: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for agent in agents:
        slave_id = str(agent.get("agent_id") or "")
        if not slave_id:
            continue
        current = selected.get(slave_id)
        if current is None or (
            agent.get("lease_state") == "active"
            and current.get("lease_state") != "active"
        ):
            selected[slave_id] = dict(agent)
    return selected


def _transition(record: RunRecord, event: str, *, reason: Any | None = None) -> str:
    current = record.state
    target = RUN_TRANSITIONS.get(current, {}).get(event)
    if target is None:
        raise ValueError("illegal_state_transition")
    if event == "refinement_started" and current == "awaiting_decision":
        if not isinstance(record.outcome, dict) or record.outcome.get("decision") != "repair":
            raise ValueError("illegal_state_transition")
    if event == "decision_accepted":
        if not isinstance(record.outcome, dict) or record.outcome.get("decision") != "attestation":
            raise ValueError("illegal_state_transition")
    record.state = target
    transition_event: dict[str, Any] = {
        "phase": "run_transition",
        "event": event,
        "from_state": current,
        "to_state": target,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if reason is not None:
        transition_event["reason"] = deepcopy(reason)
    record.events.append(transition_event)
    return target


def _run_record_payload(record: RunRecord) -> dict[str, Any]:
    return {
        "run_id": record.run_id,
        "task_ref": record.task_ref,
        "goal": record.goal,
        "allow_reassignment": record.allow_reassignment,
        "state": record.state,
        "status": ObserverRepository._conversation_status_for_state(record.state) if "ObserverRepository" in globals() else record.state,
        "draft_version": record.draft.version_id,
        "draft_digest": record.draft.snapshot_digest,
        "draft": record.draft.model_dump(mode="json"),
        "committed": record.committed.model_dump(mode="json") if record.committed else None,
        "closure_contract": record.closure_contract.model_dump(mode="json") if record.closure_contract else None,
        "committed_version": record.committed.version_id if record.committed else None,
        "execution_id": record.execution_id,
        "execution_epoch": record.execution_epoch,
        "outcome": record.outcome,
        "attempts": record.attempts,
        "events": record.events,
        "dynamic_nodes": [node.model_dump(mode="json") for node in record.dynamic_nodes],
        "capability_packages": [package.model_dump(mode="json") for package in record.capability_packages],
        "capability_activations": [activation.model_dump(mode="json") for activation in record.capability_activations],
    }


class ObserverRepository:
    """Deterministic state authority; SQL persistence is added behind this boundary."""

    def __init__(
        self,
        engine: AsyncEngine | None = None,
        content_store: ContentStore | None = None,
        *,
        orchestrator_runtime_available: bool | None = None,
    ) -> None:
        self.runs: dict[str, RunRecord] = {}
        self.idempotency: dict[str, PatchReceipt] = {}
        self.slave_agents: dict[str, dict[str, Any]] = {}
        self.slave_instances: dict[tuple[str, str], dict[str, Any]] = {}
        self.slave_capabilities: dict[str, dict[str, Any]] = {}
        self.engine = engine
        self.sessions = make_session_factory(engine) if engine is not None else None
        if content_store is None:
            settings = Settings()
            content_store = ContentStore.from_settings(settings)
        self.content_store = content_store
        # A deployed Observer deliberately has no Docker CLI/socket.  The
        # legacy in-process test profile can retain the local preflight check;
        # production Driver readiness is responsible for its own runtime.
        self.orchestrator_runtime_available = orchestrator_runtime_available
        # Promotion mutates a RunRecord's package list.  The single Observer
        # process serializes concurrent retries so two requests cannot both
        # pass the existence check before either persists its derivative.
        self._promotion_lock = asyncio.Lock()
        self._dynamic_intent_locks: dict[str, asyncio.Lock] = {}
        # Runtime agent state is kept in-memory when no database is configured
        # (the hermetic test profile) and mirrored to Observer PostgreSQL when
        # one is available. Lease ids are never persisted in plaintext.
        self.agents: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        if engine is None:
            from loom_v2.slave.service import SlaveService

            now = datetime.now(timezone.utc)
            for slave_id in ("slave-a", "slave-b"):
                embedded_slave = SlaveService(slave_id, content_store=self.content_store)
                instance_id = f"embedded-{slave_id}"
                self.agents[("workspace-default", "slave", slave_id, instance_id)] = {
                    "workspace_id": "workspace-default",
                    "role": "slave",
                    "agent_id": slave_id,
                    "instance_id": instance_id,
                    "endpoint_url": f"http://{slave_id}",
                    "protocol_version": "loom.v1",
                    "capabilities": {
                        "operations": sorted(embedded_slave.supported_operations),
                        "executor_descriptors": [
                            descriptor.kind for descriptor in embedded_slave.executor_registry.descriptors()
                        ],
                        "term_support": [item.model_dump(mode="json") for item in embedded_slave.term_support()],
                    },
                    "epoch": 1,
                    "lease_id_hash": "",
                    "lease_state": "active",
                    "last_seen_at": now,
                    "created_at": now,
                    "updated_at": now,
                }
            seeded = [dict(item) for item in self.agents.values()]
            self._store_slave_snapshot(seeded)
        self.driver_threads: dict[tuple[str, str], DriverThreadBinding] = {}
        self._driver_requests: dict[tuple[str, str], dict[str, Any]] = {}
        self._driver_request_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self.agent_events: list[dict[str, Any]] = []
        self.message_receipts: dict[tuple[str, str], MessageReceipt] = {}
        self.message_receipt_events: list[dict[str, Any]] = []
        self._message_receipt_lock = asyncio.Lock()

    @staticmethod
    def _message_receipt_from_row(row: MessageReceiptRow) -> MessageReceipt:
        values = {
            column: getattr(row, column)
            for column in (
                "workspace_id", "request_id", "conversation_ref", "prompt", "payload_digest", "state",
                "run_id", "assistant_text", "outcome", "claim_token", "attempt_count",
                "next_attempt_at", "created_at", "updated_at",
            )
        }
        return MessageReceipt.model_validate(values)

    @staticmethod
    def _message_receipt_values(receipt: MessageReceipt) -> dict[str, Any]:
        return receipt.model_dump(mode="python")

    async def _load_message_receipt(self, workspace_id: str, request_id: str) -> MessageReceipt | None:
        key = (workspace_id, request_id)
        cached = self.message_receipts.get(key)
        if cached is not None:
            return cached
        if self.sessions is None:
            return None
        async with self.sessions() as session:
            row = await session.get(MessageReceiptRow, key)
            if row is None:
                return None
            receipt = self._message_receipt_from_row(row)
            self.message_receipts[key] = receipt
            return receipt

    async def _persist_message_receipt(self, receipt: MessageReceipt) -> None:
        key = (receipt.workspace_id, receipt.request_id)
        previous = self.message_receipts.get(key)
        previous_state = previous.state if previous is not None else None
        if self.sessions is None:
            self.message_receipts[key] = receipt
            if previous_state != receipt.state:
                self._record_message_receipt_event(receipt)
            return
        values = self._message_receipt_values(receipt)
        async with self.sessions() as session:
            row = await session.get(MessageReceiptRow, key)
            if row is None:
                session.add(MessageReceiptRow(**values))
            else:
                if previous_state is None:
                    previous_state = row.state
                for name, value in values.items():
                    setattr(row, name, value)
            await session.commit()
        self.message_receipts[key] = receipt
        if previous_state != receipt.state:
            self._record_message_receipt_event(receipt)

    def _record_message_receipt_event(self, receipt: MessageReceipt) -> None:
        self.message_receipt_events.append(
            {
                "phase": "message_receipt",
                "workspace_id": receipt.workspace_id,
                "conversation_ref": receipt.conversation_ref,
                "request_id": receipt.request_id,
                "state": receipt.state,
                "created_at": receipt.updated_at.isoformat(),
            }
        )

    async def create_or_get_message_receipt(
        self,
        workspace_id: str,
        request_id: str,
        conversation_ref: str,
        prompt: str,
    ) -> MessageReceipt:
        async with self._message_receipt_lock:
            return await self._create_or_get_message_receipt(workspace_id, request_id, conversation_ref, prompt)

    async def _create_or_get_message_receipt(
        self,
        workspace_id: str,
        request_id: str,
        conversation_ref: str,
        prompt: str,
    ) -> MessageReceipt:
        digest = message_payload_digest(conversation_ref, prompt)
        existing = await self._load_message_receipt(workspace_id, request_id)
        if existing is not None:
            if existing.payload_digest != digest or existing.conversation_ref != conversation_ref or existing.prompt != prompt:
                raise ValueError("request_id_reused")
            return existing
        now = datetime.now(timezone.utc)
        receipt = MessageReceipt(
            workspace_id=workspace_id,
            request_id=request_id,
            conversation_ref=conversation_ref,
            prompt=prompt,
            payload_digest=digest,
            state="accepted",
            attempt_count=0,
            next_attempt_at=now,
            created_at=now,
            updated_at=now,
        )
        if self.sessions is not None:
            # A concurrent create can race the composite primary key.  The
            # first committed receipt remains authoritative; reload it when
            # the insert loses the race.
            from sqlalchemy.exc import IntegrityError

            try:
                await self._persist_message_receipt(receipt)
            except IntegrityError:
                existing = await self._load_message_receipt(workspace_id, request_id)
                if existing is None:
                    raise
                if existing.payload_digest != digest:
                    raise ValueError("request_id_reused")
                return existing
        else:
            await self._persist_message_receipt(receipt)
        return receipt

    async def get_message_receipt(self, workspace_id: str, request_id: str) -> MessageReceipt | None:
        return await self._load_message_receipt(workspace_id, request_id)

    async def list_message_receipts(
        self,
        workspace_id: str,
        conversation_ref: str | None = None,
    ) -> list[MessageReceipt]:
        if self.sessions is None:
            receipts = list(self.message_receipts.values())
        else:
            async with self.sessions() as session:
                statement = select(MessageReceiptRow).where(MessageReceiptRow.workspace_id == workspace_id)
                if conversation_ref is not None:
                    statement = statement.where(MessageReceiptRow.conversation_ref == conversation_ref)
                rows = (await session.scalars(statement.order_by(MessageReceiptRow.created_at))).all()
            receipts = [self._message_receipt_from_row(row) for row in rows]
            for receipt in receipts:
                self.message_receipts[(receipt.workspace_id, receipt.request_id)] = receipt
        if conversation_ref is not None:
            receipts = [item for item in receipts if item.workspace_id == workspace_id and item.conversation_ref == conversation_ref]
        else:
            receipts = [item for item in receipts if item.workspace_id == workspace_id]
        return sorted(receipts, key=lambda item: item.created_at)

    async def list_dispatchable_message_receipts(self, workspace_id: str) -> list[MessageReceipt]:
        now = datetime.now(timezone.utc)
        if self.sessions is None:
            receipts = list(self.message_receipts.values())
        else:
            async with self.sessions() as session:
                dispatch_states = ["accepted", "queued", "retryable"]
                rows = (
                    await session.scalars(
                select(MessageReceiptRow)
                        .where(MessageReceiptRow.workspace_id == workspace_id)
                        .where(MessageReceiptRow.state.in_(dispatch_states))
                        .order_by(MessageReceiptRow.created_at)
                    )
                ).all()
            receipts = [self._message_receipt_from_row(row) for row in rows]
            for receipt in receipts:
                self.message_receipts[(receipt.workspace_id, receipt.request_id)] = receipt
        return [
            receipt
            for receipt in receipts
            if receipt.workspace_id == workspace_id
            and (
                (
                    receipt.state in {"accepted", "queued", "retryable"}
                    and (receipt.next_attempt_at is None or self._utc_timestamp(receipt.next_attempt_at) <= now.timestamp())
                )
            )
        ]

    async def queue_message_receipt(self, workspace_id: str, request_id: str) -> MessageReceipt:
        async with self._message_receipt_lock:
            receipt = await self._load_message_receipt(workspace_id, request_id)
            if receipt is None:
                raise KeyError(request_id)
            if receipt.state in {"accepted", "retryable"}:
                receipt = receipt.model_copy(update={"state": "queued", "updated_at": datetime.now(timezone.utc)})
                await self._persist_message_receipt(receipt)
            return receipt

    async def retry_message_receipt(self, workspace_id: str, request_id: str, *, reason: dict[str, Any] | None = None) -> MessageReceipt:
        async with self._message_receipt_lock:
            receipt = await self._load_message_receipt(workspace_id, request_id)
            if receipt is None:
                raise KeyError(request_id)
            if receipt.state in {"completed", "failed", "interrupted"}:
                return receipt
            if receipt.state == "in_flight" and receipt.claim_token:
                return receipt
            now = datetime.now(timezone.utc)
            delay = min(60.0, max(0.25, 0.25 * (2 ** min(receipt.attempt_count, 8))))
            updated = receipt.model_copy(
                update={
                    "state": "retryable",
                    "next_attempt_at": datetime.fromtimestamp(now.timestamp() + delay, tz=timezone.utc),
                    "updated_at": now,
                    "outcome": {"error": deepcopy(reason or {"code": "driver_unavailable"})},
                }
            )
            await self._persist_message_receipt(updated)
            return updated

    async def interrupt_message_receipt(self, workspace_id: str, request_id: str) -> MessageReceipt:
        async with self._message_receipt_lock:
            receipt = await self._load_message_receipt(workspace_id, request_id)
            if receipt is None:
                raise KeyError(request_id)
            if receipt.state in {"completed", "failed", "interrupted"}:
                return receipt
            if receipt.state == "in_flight":
                return receipt
            now = datetime.now(timezone.utc)
            updated = receipt.model_copy(
                update={
                    "state": "interrupted",
                    "claim_token": None,
                    "next_attempt_at": None,
                    "updated_at": now,
                    "outcome": {"error": {"code": "user_interrupt"}},
                }
            )
            await self._persist_message_receipt(updated)
            return updated

    async def claim_message_receipt(
        self,
        workspace_id: str,
        request_id: str,
        *,
        payload_digest: str,
        claim_token: str,
        conversation_ref: str | None = None,
    ) -> MessageReceipt:
        async with self._message_receipt_lock:
            return await self._claim_message_receipt(
                workspace_id,
                request_id,
                payload_digest=payload_digest,
                claim_token=claim_token,
                conversation_ref=conversation_ref,
            )

    async def _claim_message_receipt(
        self,
        workspace_id: str,
        request_id: str,
        *,
        payload_digest: str,
        claim_token: str,
        conversation_ref: str | None = None,
    ) -> MessageReceipt:
        receipt = await self._load_message_receipt(workspace_id, request_id)
        if receipt is None:
            raise KeyError(request_id)
        if receipt.payload_digest != payload_digest:
            raise ValueError("request_id_reused")
        if conversation_ref is not None and receipt.conversation_ref != conversation_ref:
            raise ValueError("request_id_reused")
        if receipt.state in {"completed", "failed", "interrupted"}:
            return receipt
        if receipt.state == "in_flight":
            return receipt
        if receipt.state not in {"accepted", "queued", "retryable", "in_flight"}:
            raise ValueError("invalid_receipt_state")
        now = datetime.now(timezone.utc)
        updated = receipt.model_copy(
            update={
                "state": "in_flight",
                "claim_token": claim_token,
                "attempt_count": receipt.attempt_count + 1,
                "next_attempt_at": None,
                "updated_at": now,
            }
        )
        await self._persist_message_receipt(updated)
        return updated

    async def update_message_receipt(
        self,
        workspace_id: str,
        request_id: str,
        *,
        claim_token: str,
        state: MessageReceiptState | None = None,
        assistant_text: str | None = None,
        run_id: str | None = None,
        outcome: dict[str, Any] | None = None,
    ) -> MessageReceipt:
        async with self._message_receipt_lock:
            return await self._update_message_receipt(
                workspace_id,
                request_id,
                claim_token=claim_token,
                state=state,
                assistant_text=assistant_text,
                run_id=run_id,
                outcome=outcome,
            )

    async def _update_message_receipt(
        self,
        workspace_id: str,
        request_id: str,
        *,
        claim_token: str,
        state: MessageReceiptState | None = None,
        assistant_text: str | None = None,
        run_id: str | None = None,
        outcome: dict[str, Any] | None = None,
    ) -> MessageReceipt:
        receipt = await self._load_message_receipt(workspace_id, request_id)
        if receipt is None:
            raise KeyError(request_id)
        if receipt.state in {"completed", "failed", "interrupted"}:
            if state is None or state == receipt.state:
                return receipt
            raise ValueError("receipt_terminal")
        if receipt.claim_token != claim_token:
            raise ValueError("stale_claim_token")
        new_state = state or receipt.state
        if new_state not in {"in_flight", "completed", "failed", "interrupted", "retryable"}:
            raise ValueError("invalid_receipt_transition")
        now = datetime.now(timezone.utc)
        updates: dict[str, Any] = {"state": new_state, "updated_at": now}
        if assistant_text is not None:
            updates["assistant_text"] = assistant_text
        if run_id is not None:
            updates["run_id"] = run_id
        if outcome is not None:
            updates["outcome"] = deepcopy(outcome)
        if new_state in {"completed", "failed", "interrupted", "retryable"}:
            updates["claim_token"] = None
        if new_state == "retryable":
            updates["next_attempt_at"] = now
        updated = receipt.model_copy(update=updates)
        await self._persist_message_receipt(updated)
        return updated

    async def release_message_receipt(self, workspace_id: str, request_id: str, *, claim_token: str) -> MessageReceipt:
        return await self.update_message_receipt(
            workspace_id,
            request_id,
            claim_token=claim_token,
            state="retryable",
            outcome={"error": {"code": "delivery_released"}},
        )

    async def recover_message_receipts_for_driver(self, workspace_id: str) -> int:
        """Fence claims from a prior Driver epoch in the single-driver deployment."""
        async with self._message_receipt_lock:
            now = datetime.now(timezone.utc)
            if self.sessions is None:
                recovered = 0
                for key, receipt in list(self.message_receipts.items()):
                    if receipt.workspace_id != workspace_id or receipt.state != "in_flight":
                        continue
                    self.message_receipts[key] = receipt.model_copy(
                        update={
                            "state": "retryable",
                            "claim_token": None,
                            "next_attempt_at": now,
                            "updated_at": now,
                        }
                    )
                    recovered += 1
                return recovered
            async with self.sessions() as session:
                rows = (
                    await session.scalars(
                        select(MessageReceiptRow)
                        .where(
                            MessageReceiptRow.workspace_id == workspace_id,
                            MessageReceiptRow.state == "in_flight",
                        )
                        .with_for_update()
                    )
                ).all()
                for row in rows:
                    row.state = "retryable"
                    row.claim_token = None
                    row.next_attempt_at = now
                    row.updated_at = now
                await session.commit()
                for row in rows:
                    receipt = self._message_receipt_from_row(row)
                    self.message_receipts[(receipt.workspace_id, receipt.request_id)] = receipt
                return len(rows)

    @staticmethod
    def _lease_hash(lease_id: str) -> str:
        return hashlib.sha256(lease_id.encode("utf-8")).hexdigest()

    @staticmethod
    def _utc_timestamp(value: Any) -> float | None:
        """Normalize lease timestamps from timezone-aware and SQLite values."""
        if value is None or not hasattr(value, "timestamp"):
            return None
        if getattr(value, "tzinfo", None) is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.timestamp()

    async def register_agent(self, registration: AgentRegistration) -> AgentLease:
        """Register one runtime agent and issue a fenced lease.

        Driver registration is single-active per workspace. Slave registration
        is single-active per workspace and agent id. Registering a new
        instance expires the previous lease and fences its heartbeats.
        """
        if not isinstance(registration, AgentRegistration):
            registration = AgentRegistration.model_validate(registration)
        now = datetime.now(timezone.utc)
        workspace_id = registration.workspace_id
        epoch = 0
        if self.sessions is None:
            for key, item in list(self.agents.items()):
                if key[0] != workspace_id:
                    continue
                if key[1] == registration.role and (registration.role == "driver" or key[2] == registration.agent_id):
                    epoch = max(epoch, int(item.get("epoch", 0)))
                if registration.role == "driver" and key[1] == "driver" and item.get("lease_state") == "active":
                    item["lease_state"] = "expired"
                    item["updated_at"] = now
                if registration.role == "slave" and key[1] == "slave" and key[2] == registration.agent_id and item.get("lease_state") == "active":
                    item["lease_state"] = "expired"
                    item["updated_at"] = now
            key = (workspace_id, registration.role, registration.agent_id, registration.instance_id)
            previous = self.agents.get(key)
            if previous is not None:
                epoch = max(epoch, int(previous.get("epoch", 0)))
            if registration.role == "driver":
                epoch += 1
            else:
                epoch = max(epoch, 1)
            lease_id = f"lease-{uuid4().hex}"
            self.agents[key] = {
                "workspace_id": workspace_id,
                "role": registration.role,
                "agent_id": registration.agent_id,
                "instance_id": registration.instance_id,
                "endpoint_url": registration.endpoint_url.rstrip("/"),
                "protocol_version": registration.protocol_version,
                "capabilities": deepcopy(registration.capabilities),
                "epoch": epoch,
                "lease_id_hash": self._lease_hash(lease_id),
                "lease_state": "active",
                "last_seen_at": now,
                "created_at": previous.get("created_at", now) if previous else now,
                "updated_at": now,
            }
            self.agent_events.append({"phase": "driver_registered" if registration.role == "driver" else "slave_registered", "workspace_id": workspace_id, "agent_id": registration.agent_id, "instance_id": registration.instance_id, "epoch": epoch, "created_at": now.isoformat()})
            if registration.role == "driver":
                for thread_key, thread in list(self.driver_threads.items()):
                    if thread_key[0] == workspace_id and thread.turn_state in {"starting", "in_progress"}:
                        self.driver_threads[thread_key] = thread.model_copy(update={"turn_state": "recovery_pending", "driver_epoch": epoch})
            thread_bindings = [
                binding.model_dump(mode="json")
                for key, binding in self.driver_threads.items()
                if key[0] == workspace_id
            ] if registration.role == "driver" else []
            recovery_runs = [
                _run_record_payload(item)
                for item in self.runs.values()
                if item.closure_contract is not None
                and item.closure_contract.workspace_id == workspace_id
                and item.state in {"thinking", "running"}
            ] if registration.role == "driver" else []
            if registration.role == "driver":
                for receipt_key, receipt in list(self.message_receipts.items()):
                    if receipt.workspace_id == workspace_id and receipt.state == "in_flight":
                        self.message_receipts[receipt_key] = receipt.model_copy(
                            update={
                                "state": "retryable",
                                "claim_token": None,
                                "next_attempt_at": now,
                                "updated_at": now,
                            }
                        )
            return AgentLease(
                agent_id=registration.agent_id,
                instance_id=registration.instance_id,
                workspace_id=workspace_id,
                lease_id=lease_id,
                epoch=epoch,
                heartbeat_interval_seconds=5.0,
                thread_bindings=thread_bindings,
                recovery_runs=recovery_runs,
            )

        recovered_receipt_rows: list[MessageReceiptRow] = []
        async with self.sessions() as session:
            rows = (
                await session.scalars(
                    select(RuntimeAgentRow)
                    .where(RuntimeAgentRow.workspace_id == workspace_id)
                    .with_for_update()
                )
            ).all()
            for row in rows:
                if row.role == registration.role and (registration.role == "driver" or row.agent_id == registration.agent_id):
                    epoch = max(epoch, int(row.epoch or 0))
                if registration.role == "driver" and row.role == "driver" and row.lease_state == "active":
                    row.lease_state = "expired"
                    row.updated_at = now
                if registration.role == "slave" and row.role == "slave" and row.agent_id == registration.agent_id and row.lease_state == "active":
                    row.lease_state = "expired"
                    row.updated_at = now
            row = next((item for item in rows if item.role == registration.role and item.agent_id == registration.agent_id and item.instance_id == registration.instance_id), None)
            if row is None:
                row = RuntimeAgentRow(
                    workspace_id=workspace_id,
                    role=registration.role,
                    agent_id=registration.agent_id,
                    instance_id=registration.instance_id,
                    endpoint_url=registration.endpoint_url.rstrip("/"),
                    protocol_version=registration.protocol_version,
                    capabilities=deepcopy(registration.capabilities),
                    epoch=0,
                    lease_id_hash="",
                    lease_state="expired",
                    created_at=now,
                    updated_at=now,
                )
                session.add(row)
            if registration.role == "driver":
                epoch += 1
            else:
                epoch = max(epoch, int(row.epoch or 0), 1)
            lease_id = f"lease-{uuid4().hex}"
            row.instance_id = registration.instance_id
            row.endpoint_url = registration.endpoint_url.rstrip("/")
            row.protocol_version = registration.protocol_version
            row.capabilities = deepcopy(registration.capabilities)
            row.epoch = epoch
            row.lease_id_hash = self._lease_hash(lease_id)
            row.lease_state = "active"
            row.last_seen_at = now
            row.updated_at = now
            self.agent_events.append({"phase": "driver_registered" if registration.role == "driver" else "slave_registered", "workspace_id": workspace_id, "agent_id": registration.agent_id, "instance_id": registration.instance_id, "epoch": epoch, "created_at": now.isoformat()})
            if registration.role == "driver":
                thread_rows = (await session.scalars(select(DriverThreadRow).where(DriverThreadRow.workspace_id == workspace_id))).all()
                for thread in thread_rows:
                    if thread.turn_state in {"starting", "in_progress"}:
                        thread.turn_state = "recovery_pending"
                        thread.driver_epoch = epoch
                        cached = self.driver_threads.get((workspace_id, thread.conversation_ref))
                        if cached is not None:
                            self.driver_threads[(workspace_id, thread.conversation_ref)] = cached.model_copy(update={"turn_state": "recovery_pending", "driver_epoch": epoch})
                recovered_receipt_rows = (
                    await session.scalars(
                        select(MessageReceiptRow)
                        .where(
                            MessageReceiptRow.workspace_id == workspace_id,
                            MessageReceiptRow.state == "in_flight",
                        )
                        .with_for_update()
                    )
                ).all()
                for receipt_row in recovered_receipt_rows:
                    receipt_row.state = "retryable"
                    receipt_row.claim_token = None
                    receipt_row.next_attempt_at = now
                    receipt_row.updated_at = now
            await session.commit()
            for receipt_row in recovered_receipt_rows:
                receipt = self._message_receipt_from_row(receipt_row)
                self.message_receipts[(receipt.workspace_id, receipt.request_id)] = receipt
        thread_bindings = []
        recovery_runs = []
        if registration.role == "driver":
            thread_bindings = [
                DriverThreadBinding.model_validate({column: getattr(thread, column) for column in ("workspace_id", "conversation_ref", "thread_id", "model", "workspace_root", "last_turn_id", "turn_state", "active_request_id", "driver_epoch")}).model_dump(mode="json")
                for thread in thread_rows
            ]
            recovery_runs = [
                _run_record_payload(item)
                for item in await self._all_records()
                if item.closure_contract is not None
                and item.closure_contract.workspace_id == workspace_id
                and item.state in {"thinking", "running"}
            ]
        return AgentLease(
            agent_id=registration.agent_id,
            instance_id=registration.instance_id,
            workspace_id=workspace_id,
            lease_id=lease_id,
            epoch=epoch,
            heartbeat_interval_seconds=5.0,
            thread_bindings=thread_bindings,
            recovery_runs=recovery_runs,
        )

    async def _agent_record(self, workspace_id: str, role: str, agent_id: str, instance_id: str | None = None) -> dict[str, Any] | None:
        key = (workspace_id, role, agent_id)
        if self.sessions is None:
            matching = [
                item
                for candidate_key, item in self.agents.items()
                if candidate_key[:3] == key and (instance_id is None or item.get("instance_id") == instance_id)
            ]
            if not matching:
                return None
            matching.sort(key=lambda item: item.get("updated_at") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
            return matching[0]
        async with self.sessions() as session:
            query = select(RuntimeAgentRow).where(RuntimeAgentRow.workspace_id == workspace_id, RuntimeAgentRow.role == role, RuntimeAgentRow.agent_id == agent_id)
            if instance_id is not None:
                query = query.where(RuntimeAgentRow.instance_id == instance_id)
            row = (await session.scalars(query.order_by(RuntimeAgentRow.updated_at.desc()))).first()
            if row is None:
                return None
            return {
                "workspace_id": row.workspace_id,
                "role": row.role,
                "agent_id": row.agent_id,
                "instance_id": row.instance_id,
                "endpoint_url": row.endpoint_url,
                "protocol_version": row.protocol_version,
                "capabilities": row.capabilities or {},
                "epoch": row.epoch,
                "lease_id_hash": row.lease_id_hash,
                "lease_state": row.lease_state,
                "last_seen_at": row.last_seen_at,
            }

    async def heartbeat_agent(self, agent_id: str, instance_id: str, lease_id: str, epoch: int, *, workspace_id: str | None = None, role: str = "driver") -> AgentLease:
        record = None
        if workspace_id is not None:
            await self.list_agents(workspace_id, role=role)
            record = await self._agent_record(workspace_id, role, agent_id, instance_id)
        else:
            candidates = [item for key, item in self.agents.items() if key[1] == role and key[2] == agent_id]
            if candidates:
                record = candidates[0]
                workspace_id = str(record["workspace_id"])
            elif self.sessions is not None:
                async with self.sessions() as session:
                    row = (await session.scalars(select(RuntimeAgentRow).where(RuntimeAgentRow.agent_id == agent_id, RuntimeAgentRow.role == role))).first()
                    if row is not None:
                        workspace_id = row.workspace_id
                        record = {
                            "workspace_id": row.workspace_id,
                            "role": row.role,
                            "agent_id": row.agent_id,
                            "instance_id": row.instance_id,
                            "endpoint_url": row.endpoint_url,
                            "protocol_version": row.protocol_version,
                            "capabilities": row.capabilities or {},
                            "epoch": row.epoch,
                            "lease_id_hash": row.lease_id_hash,
                            "lease_state": row.lease_state,
                        }
        if record is None or workspace_id is None or record.get("instance_id") != instance_id or record.get("lease_state") != "active" or int(record.get("epoch", 0)) != int(epoch) or not hmac.compare_digest(str(record.get("lease_id_hash", "")), self._lease_hash(lease_id)):
            raise ValueError("stale_driver_epoch" if role == "driver" else "stale_agent_lease")
        now = datetime.now(timezone.utc)
        if self.sessions is None:
            record["last_seen_at"] = now
            record["updated_at"] = now
        else:
            async with self.sessions() as session:
                row = (await session.scalars(select(RuntimeAgentRow).where(RuntimeAgentRow.workspace_id == workspace_id, RuntimeAgentRow.role == role, RuntimeAgentRow.agent_id == agent_id, RuntimeAgentRow.instance_id == instance_id))).first()
                if row is not None:
                    row.last_seen_at = now
                    row.updated_at = now
                    await session.commit()
        return AgentLease(agent_id=agent_id, instance_id=instance_id, workspace_id=workspace_id, lease_id=lease_id, epoch=int(epoch), heartbeat_interval_seconds=5.0)

    async def release_agent(self, agent_id: str, instance_id: str, lease_id: str, epoch: int, *, workspace_id: str | None = None, role: str = "driver") -> None:
        await self.heartbeat_agent(agent_id, instance_id, lease_id, epoch, workspace_id=workspace_id, role=role)
        if workspace_id is None:
            raise ValueError("agent_not_found")
        now = datetime.now(timezone.utc)
        if self.sessions is None:
            record = next(
                item
                for candidate_key, item in self.agents.items()
                if candidate_key[:3] == (workspace_id, role, agent_id) and item.get("instance_id") == instance_id
            )
            record["lease_state"] = "released"
            record["updated_at"] = now
        else:
            async with self.sessions() as session:
                row = (await session.scalars(select(RuntimeAgentRow).where(RuntimeAgentRow.workspace_id == workspace_id, RuntimeAgentRow.role == role, RuntimeAgentRow.agent_id == agent_id, RuntimeAgentRow.instance_id == instance_id))).first()
                if row is not None:
                    row.lease_state = "released"
                    row.updated_at = now
                    await session.commit()

    async def list_agents(self, workspace_id: str, role: str | None = None) -> list[dict[str, Any]]:
        lease_ttl = float(os.getenv("LOOM_AGENT_LEASE_TTL_SECONDS", "15"))
        cutoff = datetime.now(timezone.utc).timestamp() - max(1.0, lease_ttl)
        if self.sessions is None:
            rows = []
            for key, item in self.agents.items():
                if key[0] != workspace_id or (role is not None and key[1] != role):
                    continue
                seen_timestamp = self._utc_timestamp(item.get("last_seen_at"))
                if item.get("lease_state") == "active" and seen_timestamp is not None and seen_timestamp < cutoff:
                    item["lease_state"] = "expired"
                rows.append(dict(item))
        else:
            async with self.sessions() as session:
                query = select(RuntimeAgentRow).where(RuntimeAgentRow.workspace_id == workspace_id)
                if role is not None:
                    query = query.where(RuntimeAgentRow.role == role)
                db_rows = (await session.scalars(query.order_by(RuntimeAgentRow.role, RuntimeAgentRow.agent_id))).all()
                rows = []
                expired = False
                for row in db_rows:
                    seen_timestamp = self._utc_timestamp(row.last_seen_at)
                    if row.lease_state == "active" and seen_timestamp is not None and seen_timestamp < cutoff:
                        row.lease_state = "expired"
                        row.updated_at = datetime.now(timezone.utc)
                        expired = True
                    rows.append({
                        "workspace_id": row.workspace_id,
                        "role": row.role,
                        "agent_id": row.agent_id,
                        "instance_id": row.instance_id,
                        "endpoint_url": row.endpoint_url,
                        "protocol_version": row.protocol_version,
                        "capabilities": row.capabilities or {},
                        "epoch": row.epoch,
                        "lease_state": row.lease_state,
                        "last_seen_at": row.last_seen_at.isoformat() if row.last_seen_at else None,
                    })
                if expired:
                    await session.commit()
        for row in rows:
            if hasattr(row.get("last_seen_at"), "isoformat"):
                row["last_seen_at"] = row["last_seen_at"].isoformat()
        return rows

    def _store_slave_snapshot(self, agents: list[dict[str, Any]]) -> None:
        self.slave_instances = {
            (str(agent.get("agent_id") or ""), str(agent.get("instance_id") or "")): dict(agent)
            for agent in agents
            if agent.get("agent_id")
        }
        self.slave_agents = _select_slave_agents(agents)
        self.slave_capabilities = {}
        for slave_id, agent in self.slave_agents.items():
            if agent.get("lease_state") != "active":
                continue
            capabilities = dict(agent.get("capabilities") or {})
            self.slave_capabilities[slave_id] = {
                **capabilities,
                "operations": set(capabilities.get("operations", [])),
            }

    async def refresh_slaves(self, workspace_id: str | None = None) -> list[dict[str, Any]]:
        agents = await self.list_agents(workspace_id or "workspace-default", role="slave")
        self._store_slave_snapshot(agents)
        return agents

    async def bind_thread(self, binding: DriverThreadBinding) -> dict[str, Any]:
        existing_thread = next(
            (
                current
                for key, current in self.driver_threads.items()
                if key[0] == binding.workspace_id
                and current.thread_id == binding.thread_id
                and key != (binding.workspace_id, binding.conversation_ref)
            ),
            None,
        )
        if existing_thread is not None:
            raise ValueError("thread_binding_conflict")
        if self.sessions is not None:
            async with self.sessions() as session:
                conflict = (
                    await session.scalars(
                        select(DriverThreadRow).where(
                            DriverThreadRow.workspace_id == binding.workspace_id,
                            DriverThreadRow.thread_id == binding.thread_id,
                            DriverThreadRow.conversation_ref != binding.conversation_ref,
                        )
                    )
                ).first()
                if conflict is not None:
                    raise ValueError("thread_binding_conflict")
                row = await session.get(DriverThreadRow, (binding.workspace_id, binding.conversation_ref))
                values = binding.model_dump(mode="json")
                values["updated_at"] = datetime.now(timezone.utc)
                if row is None:
                    session.add(DriverThreadRow(**values))
                else:
                    for key, value in values.items():
                        if key not in {"workspace_id", "conversation_ref"}:
                            setattr(row, key, value)
                await session.commit()
        self.driver_threads[(binding.workspace_id, binding.conversation_ref)] = binding
        return binding.model_dump(mode="json")

    async def thread_bind(self, binding: DriverThreadBinding | dict[str, Any]) -> dict[str, Any]:
        return await self.bind_thread(binding if isinstance(binding, DriverThreadBinding) else DriverThreadBinding.model_validate(binding))

    async def get_thread(self, workspace_id: str, conversation_ref: str) -> dict[str, Any] | None:
        key = (workspace_id, conversation_ref)
        if key in self.driver_threads:
            return self.driver_threads[key].model_dump(mode="json")
        if self.sessions is None:
            return None
        async with self.sessions() as session:
            row = await session.get(DriverThreadRow, key)
        if row is None:
            return None
        binding = DriverThreadBinding.model_validate({column: getattr(row, column) for column in ("workspace_id", "conversation_ref", "thread_id", "model", "workspace_root", "last_turn_id", "turn_state", "active_request_id", "driver_epoch")})
        self.driver_threads[key] = binding
        return binding.model_dump(mode="json")

    async def thread_get(self, workspace_id: str, conversation_ref: str) -> dict[str, Any] | None:
        return await self.get_thread(workspace_id, conversation_ref)

    async def set_turn_state(self, workspace_id: str, conversation_ref: str, *, turn_state: str, request_id: str | None = None, turn_id: str | None = None, driver_epoch: int | None = None) -> dict[str, Any]:
        current = await self.get_thread(workspace_id, conversation_ref)
        if current is None:
            raise KeyError(conversation_ref)
        current.update({"turn_state": turn_state})
        if request_id is not None:
            current["active_request_id"] = request_id
        if turn_id is not None:
            current["last_turn_id"] = turn_id
        if driver_epoch is not None:
            current["driver_epoch"] = driver_epoch
        return await self.bind_thread(DriverThreadBinding.model_validate(current))

    async def turn_state(self, workspace_id: str, conversation_ref: str, turn_state: str, **kwargs: Any) -> dict[str, Any]:
        return await self.set_turn_state(workspace_id, conversation_ref, turn_state=turn_state, **kwargs)

    async def _assert_run_workspace(self, run_id: str, workspace_id: str) -> RunRecord:
        record = await self._load(run_id)
        contract_workspace = record.closure_contract.workspace_id if record.closure_contract is not None else "workspace-default"
        if contract_workspace != workspace_id:
            raise ValueError("workspace_binding_mismatch")
        return record

    async def _assert_package_workspace(self, package_ref: str | ResourceRef, workspace_id: str, *, run_id: str | None = None) -> CapabilityPackageVersion:
        if run_id is not None:
            await self._assert_run_workspace(run_id, workspace_id)
        package = await self.get_capability_package(package_ref, run_id=run_id)
        source = await self._load(package.source_run_ref)
        source_workspace = source.closure_contract.workspace_id if source.closure_contract is not None else "workspace-default"
        if source_workspace != workspace_id:
            raise ValueError("workspace_binding_mismatch")
        return package

    async def execute_driver_command(self, command: DriverCommand) -> dict[str, Any]:
        request_key = (command.driver_id, command.request_id)
        lock = self._driver_request_locks.setdefault(request_key, asyncio.Lock())
        async with lock:
            return await self._execute_driver_command(command)

    async def _execute_driver_command(self, command: DriverCommand) -> dict[str, Any]:
        allowed = {
            "run.open", "run.begin", "run.get", "run.patch", "run.commit", "run.start", "run.close", "run.cancel", "run.fail", "run.resolve",
            "run.readiness", "run.recovery.list", "run.recovery.mark", "message.append", "message.claim", "message.update", "message.release", "agent_signal.record",
            "run.result",
            "thread.bind", "thread.get", "turn.state", "capability.list", "capability.get", "capability.health",
            "node.accept", "node.dispatch", "node.reassign", "node.result", "node.fail",
        }
        if command.command not in allowed:
            raise ValueError("driver_command_not_allowed")
        requested_workspace = command.arguments.get("workspace_id")
        agents = await self.list_agents(str(requested_workspace or "workspace-default"), role="driver")
        if requested_workspace is None and not agents:
            if self.sessions is None:
                all_workspaces = {key[0] for key in self.agents if key[1] == "driver"}
            else:
                async with self.sessions() as session:
                    all_workspaces = set(
                        (await session.scalars(select(RuntimeAgentRow.workspace_id).where(RuntimeAgentRow.role == "driver"))).all()
                    )
            for workspace in all_workspaces:
                agents.extend(await self.list_agents(workspace, role="driver"))
        agent = next((item for item in agents if item["agent_id"] == command.driver_id and item["lease_state"] == "active"), None)
        if agent is None or agent["instance_id"] != command.instance_id or int(agent["epoch"]) != int(command.driver_epoch):
            raise ValueError("stale_driver_epoch")
        record = await self._agent_record(agent["workspace_id"], "driver", command.driver_id, command.instance_id)
        if record is None or not hmac.compare_digest(str(record.get("lease_id_hash", "")), self._lease_hash(command.lease_id)):
            raise ValueError("stale_driver_epoch")
        request_key = (command.driver_id, command.request_id)
        if request_key in self._driver_requests:
            receipt = self._driver_requests[request_key]
            if receipt.get("command") != command.command:
                raise ValueError("request_id_reused")
            return deepcopy(receipt["response"])
        if self.sessions is not None:
            async with self.sessions() as session:
                existing = await session.get(DriverRequestRow, request_key)
                if existing is not None:
                    if existing.command != command.command:
                        raise ValueError("request_id_reused")
                    return deepcopy(existing.response)
        args = dict(command.arguments)
        workspace_id = str(agent["workspace_id"])
        run_scoped_commands = {
            "run.get", "run.begin", "run.patch", "run.commit", "run.start", "run.close", "run.cancel", "run.fail", "run.resolve",
            "run.readiness", "message.append", "agent_signal.record", "capability.health",
            "node.accept", "node.dispatch", "node.reassign", "node.result", "node.fail",
        }
        if command.command in run_scoped_commands and args.get("run_id") is not None:
            try:
                await self._assert_run_workspace(str(args["run_id"]), workspace_id)
            except KeyError:
                if command.command != "node.fail":
                    raise
        if command.command == "capability.get":
            package_ref = args.get("package_ref")
            if isinstance(package_ref, dict):
                package_ref = ResourceRef.model_validate(package_ref)
            args["package_ref"] = package_ref
            await self._assert_package_workspace(package_ref, workspace_id, run_id=args.get("run_id"))
        elif command.command == "capability.health" and args.get("run_id") is None:
            report = args.get("report") or {}
            await self._assert_package_workspace(str(report.get("package_version_ref") or ""), workspace_id)
        if command.command == "run.open":
            contract = args.get("closure_contract")
            result = (await self.open_run(args.get("run_id"), str(args.get("task_ref") or args.get("conversation_ref") or ""), str(args.get("goal") or (contract or {}).get("goal") or ""), bool(args.get("allow_reassignment", False)), ClosureContract.model_validate(contract) if contract else None, user_id=str(args.get("user_id", "user-default")), workspace_id=workspace_id)).__dict__
            result = {"run_id": result["run_id"], "state": result["state"], "draft_version": result["draft"].version_id, "draft_digest": result["draft"].snapshot_digest}
        elif command.command == "run.begin":
            result = _run_record_payload(await self.begin_refinement(str(args["run_id"])))
        elif command.command == "run.get":
            result = _run_record_payload(await self.get_run(str(args["run_id"])))
        elif command.command == "run.patch":
            run = await self.get_run(str(args["run_id"]))
            receipt = await self.apply_patch(run.run_id, run.draft_version, run.draft_digest, str(args.get("operation_id") or command.request_id), list(args.get("ops", [])))
            result = {"receipt": receipt.receipt, "run_id": receipt.run_id, "draft_version": receipt.draft_version, "draft_digest": receipt.draft_digest, "patch_cursor": receipt.patch_cursor, "readiness": receipt.readiness}
        elif command.command == "run.commit":
            result_version = await self.commit(str(args["run_id"]), str(args["version_id"]), str(args["digest"]))
            result = {"closure_version": result_version.version_id, "snapshot_digest": result_version.snapshot_digest}
        elif command.command == "run.start":
            result = await self.start(str(args["run_id"]), str(args["version_id"]))
        elif command.command == "run.result":
            reported = dict(args["result"])
            final_ref = reported.pop("orchestration_final_ref", None)
            if final_ref is not None:
                result = _run_record_payload(await self.complete_orchestration(str(args["run_id"]), ResourceRef.model_validate(final_ref)))
            else:
                result = _run_record_payload(await self.record_result(str(args["run_id"]), reported))
        elif command.command == "run.close":
            result = _run_record_payload(await self.close_run(str(args["run_id"])))
        elif command.command == "run.cancel":
            reason = args.get("reason", "user_interrupt")
            if not isinstance(reason, (str, dict)):
                reason = str(reason)
            result = _run_record_payload(await self.cancel_run(str(args["run_id"]), reason))
        elif command.command == "run.fail":
            reason = args.get("reason", {"code": "driver_failure"})
            if not isinstance(reason, (str, dict)):
                reason = str(reason)
            result = _run_record_payload(await self.fail_run(str(args["run_id"]), reason))
        elif command.command == "run.resolve":
            result = _run_record_payload(await self.resolve_run(str(args["run_id"]), str(args.get("decision") or "")))
        elif command.command == "run.readiness":
            result = await self.inspect_readiness(str(args["run_id"]), args.get("version_id"))
        elif command.command == "run.recovery.list":
            result = {
                "runs": [
                    _run_record_payload(item)
                    for item in await self._all_records()
                    if item.state in {"thinking", "running"}
                    and (item.closure_contract is None or item.closure_contract.workspace_id == workspace_id)
                ]
            }
        elif command.command == "run.recovery.mark":
            active_run_ids = {str(run_id) for run_id in args.get("active_run_ids", [])}
            for run_id in active_run_ids:
                await self._assert_run_workspace(run_id, workspace_id)
            result = {"recovered": await self.recover_stale_runs(active_run_ids)}
        elif command.command == "message.append":
            result = await self.append_message(str(args["run_id"]), str(args["role"]), str(args["content"]), request_id=args.get("request_id") or command.request_id)
        elif command.command == "message.claim":
            receipt = await self.claim_message_receipt(
                workspace_id,
                str(args["request_id"]),
                payload_digest=str(args["payload_digest"]),
                claim_token=str(args["claim_token"]),
                conversation_ref=args.get("conversation_ref"),
            )
            result = receipt.model_dump(mode="json")
        elif command.command == "message.update":
            receipt = await self.update_message_receipt(
                workspace_id,
                str(args["request_id"]),
                claim_token=str(args["claim_token"]),
                state=args.get("state"),
                assistant_text=args.get("assistant_text"),
                run_id=args.get("run_id"),
                outcome=args.get("outcome"),
            )
            result = receipt.model_dump(mode="json")
        elif command.command == "message.release":
            receipt = await self.release_message_receipt(
                workspace_id,
                str(args["request_id"]),
                claim_token=str(args["claim_token"]),
            )
            result = receipt.model_dump(mode="json")
        elif command.command == "agent_signal.record":
            result = await self.record_agent_signal(str(args["run_id"]), str(args["kind"]), dict(args.get("payload", {})))
        elif command.command == "thread.bind":
            payload = dict(args)
            payload["workspace_id"] = workspace_id
            payload["driver_epoch"] = command.driver_epoch
            result = await self.bind_thread(DriverThreadBinding.model_validate(payload))
        elif command.command == "thread.get":
            result = await self.get_thread(workspace_id, str(args["conversation_ref"])) or {}
        elif command.command == "turn.state":
            result = await self.set_turn_state(workspace_id, str(args["conversation_ref"]), turn_state=str(args["turn_state"]), request_id=args.get("request_id"), turn_id=args.get("turn_id"), driver_epoch=command.driver_epoch)
        elif command.command == "capability.list":
            if args.get("run_id") is not None:
                await self._assert_run_workspace(str(args["run_id"]), workspace_id)
            await self.refresh_slaves(workspace_id)
            slave_agents = list(self.slave_agents.values())
            capabilities = [
                {
                    "resource_id": agent["agent_id"],
                    "available": agent.get("lease_state") == "active",
                    "operations": sorted((agent.get("capabilities") or {}).get("operations", [])),
                    "executor_descriptors": (agent.get("capabilities") or {}).get("executor_descriptors", []),
                    "term_support": (agent.get("capabilities") or {}).get("term_support", []),
                }
                for agent in slave_agents
            ]
            result = {
                "capabilities": capabilities,
                "packages": [item.model_dump(mode="json") for item in await self.list_capability_packages(run_id=args.get("run_id"), include_abandoned=bool(args.get("include_abandoned", False)))],
            }
        elif command.command == "capability.get":
            result = (await self.get_capability_package(args["package_ref"], run_id=args.get("run_id"))).model_dump(mode="json")
        elif command.command == "capability.health":
            from loom_v2.contracts.types import CapabilityHealthReport
            result = (await self.record_capability_health(CapabilityHealthReport.model_validate(args["report"]), run_id=args.get("run_id"))).model_dump(mode="json")
        elif command.command == "node.accept":
            node = await self.accept_node_intent(str(args["run_id"]), NodeIntent.model_validate(args["intent"]), selected_target=str(args["selected_target"]))
            result = node.model_dump(mode="json")
        elif command.command == "node.dispatch":
            attempt = await self.dispatch_dynamic_node(str(args["run_id"]), str(args["node_id"]), target=str(args["target"]))
            result = {"attempt": attempt}
        elif command.command == "node.reassign":
            attempt = await self.reassign_dynamic_node(
                str(args["run_id"]),
                str(args["node_id"]),
                lost_attempt_id=str(args["lost_attempt_id"]),
                expected_execution_id=str(args["expected_execution_id"]),
                expected_execution_epoch=int(args["expected_execution_epoch"]),
                target=str(args["target"]),
                reason=str(args.get("reason") or "worker_lease_expired"),
            )
            result = {
                "node": {"node_id": str(args["node_id"]), "state": "dispatched"},
                "attempt": attempt,
            }
        elif command.command == "node.result":
            node = await self.record_dynamic_node_result(str(args["run_id"]), str(args["node_id"]), dict(args["result"]))
            result = node.model_dump(mode="json")
        elif command.command == "node.fail":
            node = await self.fail_dynamic_node(
                str(args["run_id"]),
                str(args["node_id"]),
                attempt_id=args.get("attempt_id"),
                error=dict(args.get("reason") or {}),
            )
            result = node.model_dump(mode="json")
        else:
            raise ValueError(f"unsupported_driver_command:{command.command}")
        self._driver_requests[request_key] = {"command": command.command, "response": deepcopy(result)}
        if self.sessions is not None:
            async with self.sessions() as session:
                session.add(DriverRequestRow(driver_id=command.driver_id, request_id=command.request_id, command=command.command, response=deepcopy(result)))
                await session.commit()
        return result

    async def init_db(self) -> None:
        if self.engine is None:
            return
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
            if connection.dialect.name == "postgresql":
                await connection.execute(text("ALTER TABLE runs ADD COLUMN IF NOT EXISTS closure_contract JSONB"))
                await connection.execute(text("ALTER TABLE runs ADD COLUMN IF NOT EXISTS capability_packages JSONB NOT NULL DEFAULT '[]'::jsonb"))
                await connection.execute(text("ALTER TABLE runs ADD COLUMN IF NOT EXISTS capability_activations JSONB NOT NULL DEFAULT '[]'::jsonb"))
                await connection.execute(text("ALTER TABLE runs ADD COLUMN IF NOT EXISTS dynamic_nodes JSONB NOT NULL DEFAULT '[]'::jsonb"))
            await connection.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_agents_active_agent "
                    "ON runtime_agents (workspace_id, role, agent_id) "
                    "WHERE lease_state = 'active'"
                )
            )
            await connection.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_agents_active_driver "
                    "ON runtime_agents (workspace_id) "
                    "WHERE role = 'driver' AND lease_state = 'active'"
                )
            )

    async def recover_stale_runs(self, active_run_ids: set[str] | None = None) -> list[str]:
        """Fence Runs left active by a lost Observer/Driver process.

        ``thinking`` and ``running`` are leased states: they require a live
        Driver owner.  A process restart cannot reconstruct that in-memory
        owner, so persisted records in those states must not remain visible as
        active forever.  The optional owner set lets a caller preserve Runs
        that it has explicitly reattached before running recovery; the normal
        Observer startup path passes no owners because its Driver starts empty.
        """

        owners = active_run_ids or set()
        recovered: list[str] = []
        for record in await self._all_records():
            if record.run_id in owners or record.state not in {"thinking", "running"}:
                continue
            if record.state == "running" and record.execution_id is not None:
                snapshot = record.committed.snapshot if record.committed is not None else record.draft.snapshot
                if snapshot.program_systems.executor_kind == "orchestrator_python_v1":
                    package = (
                        self._find_package(snapshot.program_systems.package_ref, run_id=record.run_id)
                        if snapshot.program_systems.package_ref
                        else None
                    )
                    replayable = (
                        package is not None
                        and package.replay_safety == "DeterministicByEventLog"
                        and not package.captures_run_state
                        and not package.captured_secret_refs
                        and not package.captured_path_refs
                    )
                    if replayable:
                        reason = {"code": "driver_restarted", "action": "replay_orchestration"}
                        record.execution_epoch += 1
                        for attempt in record.attempts:
                            if attempt.get("state") in {"created", "running"}:
                                attempt["state"] = "failed"
                                attempt["terminal_error"] = reason
                        for node in record.dynamic_nodes:
                            if node.state == "dispatched":
                                node.state = "accepted"
                        record.events.append(
                            {
                                "phase": "run_recovered",
                                "run_id": record.run_id,
                                "execution_id": record.execution_id,
                                "reason": reason,
                                "execution_epoch": record.execution_epoch,
                                "created_at": datetime.now(timezone.utc).isoformat(),
                            }
                        )
                        await self._persist(record)
                        recovered.append(record.run_id)
                        continue
            reason = {
                "code": "orchestration_not_replayable"
                if record.state == "running"
                and record.committed is not None
                and record.committed.snapshot.program_systems.executor_kind == "orchestrator_python_v1"
                else "observer_restarted"
            }
            for attempt in record.attempts:
                if attempt.get("state") in {"created", "running"}:
                    attempt["state"] = "failed"
                    attempt["terminal_error"] = reason
            _transition(record, "run_failed", reason=reason)
            record.outcome = {
                "disposition": "failed",
                "decision": None,
                "terminal_state": "failed",
                "terminal_error": reason,
                "reason": reason,
                "execution_id": record.execution_id,
                "execution_epoch": record.execution_epoch,
            }
            record.events.append(
                {
                    "phase": "run_recovered",
                    "run_id": record.run_id,
                    "execution_id": record.execution_id,
                    "reason": reason,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            await self._persist(record)
            recovered.append(record.run_id)
        return recovered

    async def put_content(self, content: Any, *, media_type: str) -> ResourceRef:
        """Canonicalize and persist content through the single upload path."""

        normalized_media_type = str(media_type or "").split(";", 1)[0].strip().lower()
        if not normalized_media_type:
            raise ValueError("media_type_required")

        json_media_types = {
            "application/json",
            "application/schema+json",
            "application/vnd.loom.io-contract+json",
        }
        if normalized_media_type in json_media_types:
            if isinstance(content, (bytes, bytearray, str)):
                try:
                    parsed = json.loads(content)
                except (TypeError, json.JSONDecodeError) as exc:
                    raise ValueError("invalid_json") from exc
            else:
                parsed = content
            if normalized_media_type == "application/schema+json":
                validate_schema(parsed)
            elif normalized_media_type == "application/vnd.loom.io-contract+json":
                try:
                    IoContract.model_validate(parsed)
                except Exception as exc:
                    raise ValueError("io_contract_invalid") from exc
            body = canonical_json_bytes(parsed)
        elif isinstance(content, str):
            body = content.encode("utf-8")
        elif isinstance(content, (bytes, bytearray)):
            body = bytes(content)
        else:
            raise TypeError("content_must_be_string_or_bytes")
        return await self.content_store.put(body, media_type=normalized_media_type)

    @staticmethod
    def _record_from_row(row: RunRow) -> RunRecord:
        draft_payload = ObserverRepository._normalize_version_payload(row.draft)
        committed_payload = ObserverRepository._normalize_version_payload(row.committed) if row.committed else None
        contract_payload = ObserverRepository._normalize_contract_payload(row.closure_contract) if row.closure_contract else None
        raw_dynamic_nodes = getattr(row, "dynamic_nodes", None) or []
        if raw_dynamic_nodes:
            dynamic_nodes = [DynamicNode.model_validate(item) for item in raw_dynamic_nodes]
        else:
            dynamic_nodes = ObserverRepository._dynamic_nodes_from_events(row.events or [])
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
            capability_packages=[CapabilityPackageVersion.model_validate(item) for item in (getattr(row, "capability_packages", None) or [])],
            capability_activations=[CapabilityPackageActivation.model_validate(item) for item in (getattr(row, "capability_activations", None) or [])],
            dynamic_nodes=dynamic_nodes,
        )

    @staticmethod
    def _dynamic_nodes_from_events(events: list[dict[str, Any]]) -> list[DynamicNode]:
        nodes: dict[str, DynamicNode] = {}
        for event in events:
            phase = event.get("phase")
            node_id = event.get("node_id")
            if phase == "node_accepted" and node_id:
                try:
                    package_ref = ResourceRef.model_validate(event["package_ref"])
                    input_refs = [ResourceRef.model_validate(item) for item in event.get("input_refs", [])]
                    nodes[node_id] = DynamicNode(
                        node_id=node_id,
                        parent_execution_ref=str(event.get("execution_id") or ""),
                        intent_id=str(event.get("intent_id") or ""),
                        package_ref=package_ref,
                        package_digest=str(event.get("package_digest") or ""),
                        input_refs=input_refs,
                        state="accepted",
                    )
                except (TypeError, ValueError):
                    continue
            elif node_id and phase in {"node_reassigned", "node_dispatched"} and node_id in nodes:
                nodes[node_id].state = "dispatched"
            elif node_id and phase == "node_completed" and node_id in nodes:
                state = str(event.get("terminal_state") or "completed")
                if state in {"completed", "failed", "decision_required"}:
                    nodes[node_id].state = state
        return list(nodes.values())

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
                "capability_packages": [item.model_dump(mode="json") for item in record.capability_packages],
                "capability_activations": [item.model_dump(mode="json") for item in record.capability_activations],
                "dynamic_nodes": [item.model_dump(mode="json") for item in record.dynamic_nodes],
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
            "awaiting_decision": "awaiting_decision",
            "cancelled": "interrupted",
            "failed": "failed",
        }.get(state, "idle")

    @staticmethod
    def _message_receipt_status(state: str) -> str:
        return {
            "accepted": "queued",
            "queued": "queued",
            "in_flight": "thinking",
            "retryable": "queued",
            "completed": "completed",
            "failed": "failed",
            "interrupted": "interrupted",
        }.get(state, "queued")

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

    def _slave_supports_package(self, slave_id: str, package: CapabilityPackageVersion) -> bool:
        details = self.slave_capabilities.get(slave_id)
        agent = self.slave_agents.get(slave_id)
        if details is None or agent is None or agent.get("lease_state") != "active":
            return False
        declared_operations = details.get("operations", set())
        operations = {declared_operations} if isinstance(declared_operations, str) else set(declared_operations)
        operation = package.executor_operation or "run_code"
        if operation not in operations:
            return False
        declared_executors = (
            details.get("executor_kinds")
            or details.get("executors")
            or details.get("executor_descriptors")
        )
        if declared_executors:
            raw_executors = [declared_executors] if isinstance(declared_executors, str) else declared_executors
            executor_kinds = {
                str(item.get("kind")) if isinstance(item, dict) else str(item)
                for item in raw_executors
            }
            return package.executor_kind in executor_kinds
        return True

    def _slave_is_active(self, slave_id: str) -> bool:
        agent = self.slave_agents.get(slave_id)
        return agent is not None and agent.get("lease_state") == "active"

    @staticmethod
    def _package_ref(package: CapabilityPackageVersion) -> str:
        return f"capability-package://{package.package_id}/{package.package_version}"

    @classmethod
    def _reusable_matches_candidate(
        cls,
        reusable: CapabilityPackageVersion,
        candidate: CapabilityPackageVersion,
    ) -> bool:
        if reusable.scope != "workspace_reusable" or reusable.publication_state != "published":
            return False
        if reusable.package_id != candidate.package_id:
            return False
        # New versions carry an explicit lineage entry.  The deterministic
        # version/source fallback also recognizes versions produced before
        # lineage matching was fixed.
        candidate_ref = cls._package_ref(candidate)
        if any(
            entry.get("derived_from") == candidate_ref
            and entry.get("derived_from_digest") == candidate.package_digest
            for entry in reusable.provenance
        ):
            return True
        return (
            reusable.package_version == f"{candidate.package_version}-reusable"
            and reusable.program_digest == candidate.program_digest
            and reusable.source_run_ref == candidate.source_run_ref
            and reusable.source_closure_version_ref == candidate.source_closure_version_ref
        )

    def _package_visible_to_run(self, package: CapabilityPackageVersion, run_id: str | None) -> bool:
        if run_id is None:
            return package.scope == "run_bound" or (
                package.scope == "workspace_reusable" and package.publication_state == "published"
            )
        run = self.runs.get(run_id)
        if run is None:
            return False
        if package.scope == "run_bound":
            return package.source_run_ref == run_id
        if package.scope != "workspace_reusable" or package.publication_state != "published":
            return False
        source = self.runs.get(package.source_run_ref)
        if source is None or source.closure_contract is None or run.closure_contract is None:
            return False
        return source.closure_contract.workspace_id == run.closure_contract.workspace_id

    def _find_package(self, package_ref: str | ResourceRef, *, run_id: str | None = None) -> CapabilityPackageVersion | None:
        resource_id = package_ref.resource_id if isinstance(package_ref, ResourceRef) else package_ref
        digest = (
            package_ref.version_or_digest.lower()
            if isinstance(package_ref, ResourceRef) and package_ref.version_or_digest
            else None
        )
        candidates: list[CapabilityPackageVersion] = []
        for record in self.runs.values():
            for package in record.capability_packages:
                if not self._package_visible_to_run(package, run_id):
                    continue
                if resource_id not in {
                    package.package_id,
                    self._package_ref(package),
                    f"{package.package_id}:{package.package_version}",
                    package.package_version,
                    package.package_closure_version_ref,
                    package.package_digest,
                    package.program_digest,
                }:
                    continue
                if digest is not None and digest != package.package_digest.lower():
                    continue
                candidates.append(package)
        if not candidates:
            return None
        # Prefer a published reusable version when resolving without a run
        # context, then keep selection deterministic across persisted runs.
        candidates.sort(
            key=lambda item: (
                item.scope != "workspace_reusable",
                item.package_id,
                item.package_version,
                item.source_run_ref,
            )
        )
        return candidates[0]

    async def list_capability_packages(self, *, run_id: str | None = None, include_abandoned: bool = False) -> list[CapabilityPackageVersion]:
        records = [await self._load(run_id)] if run_id else await self._all_records()
        packages = [package for record in records for package in record.capability_packages]
        if not include_abandoned:
            packages = [package for package in packages if package.publication_state != "abandoned"]
        return packages

    async def get_capability_package(self, package_ref: str | ResourceRef, *, run_id: str | None = None) -> CapabilityPackageVersion:
        if self.sessions is not None:
            await self._all_records()
        package = self._find_package(package_ref, run_id=run_id)
        if package is None:
            raise KeyError(package_ref)
        return package

    async def get_capability_package_aggregate(self, package_ref: str) -> CapabilityPackage:
        version = await self.get_capability_package(package_ref)
        records = await self._all_records()
        activations = [activation for record in records for activation in record.capability_activations if activation.package_version_ref in {f"{version.package_id}:{version.package_version}", self._package_ref(version), version.version_ref}]
        versions = [item for record in records for item in record.capability_packages if item.package_id == version.package_id]
        return CapabilityPackage(package_id=version.package_id, versions=versions, activations=activations)

    async def abandon_capability_package(self, package_ref: str, *, idempotency_key: str | None = None) -> CapabilityPackageVersion:
        package = await self.get_capability_package(package_ref)
        if package.publication_state == "abandoned":
            return package
        package.publication_state = "abandoned"
        for record in self.runs.values():
            if any(item.package_id == package.package_id and item.package_version == package.package_version for item in record.capability_packages):
                record.events.append({"phase": "capability_package_abandoned", "package_ref": self._package_ref(package), "idempotency_key": idempotency_key, "created_at": datetime.now(timezone.utc).isoformat()})
                await self._persist(record)
                break
        return package

    async def promote_capability_package(self, package_ref: str | ResourceRef, *, idempotency_key: str | None = None, approved_digest: str | None = None) -> CapabilityPackageVersion:
        async with self._promotion_lock:
            # Resolve the input first.  A request normally carries the
            # candidate ref (for example ``.../v1``), while the derived
            # published version has a different identity (``.../v1-reusable``).
            # Idempotency therefore has to follow the candidate -> reusable
            # lineage, not compare the request string with the reusable ref.
            candidate = await self.get_capability_package(package_ref)
            if candidate.publication_state == "published" and candidate.scope == "workspace_reusable":
                return candidate

            candidate_ref = self._package_ref(candidate)
            reusable_version = f"{candidate.package_version}-reusable"
            for record in await self._all_records():
                reusable = next(
                    (
                        item
                        for item in record.capability_packages
                        if self._reusable_matches_candidate(item, candidate)
                    ),
                    None,
                )
                if reusable is not None:
                    return reusable

            if candidate.publication_state == "abandoned":
                raise ValueError("capability_package_abandoned")
            if approved_digest and approved_digest not in {candidate.package_digest, candidate.program_digest}:
                raise ValueError("promotion_digest_mismatch")
            if not candidate.semantic_closed:
                raise ValueError("package_promotion_denied:semantic_not_closed")
            if candidate.captures_run_state or candidate.captured_secret_refs or candidate.captured_path_refs:
                raise ValueError("package_captures_run_state")
            source_record = next(
                (
                    record
                    for record in self.runs.values()
                    if any(
                        item.package_id == candidate.package_id
                        and item.package_version == candidate.package_version
                        and item.package_digest == candidate.package_digest
                        for item in record.capability_packages
                    )
                ),
                None,
            )
            if source_record is None:
                raise KeyError(package_ref)
            if source_record.state not in {"completed", "closed", "failed", "cancelled"}:
                raise ValueError("run_not_terminal")
            reusable_payload = candidate.model_dump(mode="json")
            reusable_payload.update({"package_version": reusable_version, "scope": "workspace_reusable", "publication_state": "published", "provenance": [*candidate.provenance, {"derived_from": candidate_ref, "derived_from_digest": candidate.package_digest, "idempotency_key": idempotency_key}], "package_digest": ""})
            reusable = CapabilityPackageVersion.model_validate(reusable_payload)
            for record in await self._all_records():
                conflicting = next(
                    (
                        item
                        for item in record.capability_packages
                        if item.package_id == reusable.package_id
                        and item.package_version == reusable.package_version
                    ),
                    None,
                )
                if conflicting is not None:
                    if conflicting.package_digest == reusable.package_digest:
                        return conflicting
                    raise ValueError("capability_package_identity_conflict")
            source_record.capability_packages.append(reusable)
            source_record.events.append({"phase": "capability_package_promoted", "package_ref": self._package_ref(reusable), "derived_from": candidate_ref, "derived_from_digest": candidate.package_digest, "idempotency_key": idempotency_key, "created_at": datetime.now(timezone.utc).isoformat()})
            await self._persist(source_record)
            return reusable

    async def record_capability_health(self, report: Any, *, run_id: str | None = None) -> CapabilityPackageActivation:
        """Accept a verified Slave health report into the activation projection."""
        package = await self.get_capability_package(report.package_version_ref, run_id=run_id)
        if package.package_digest != report.package_digest:
            raise ValueError("capability_package_digest_mismatch")
        record = (
            self.runs.get(run_id)
            if run_id is not None
            else next(
                (
                    item
                    for item in self.runs.values()
                    if any(
                        p.package_id == package.package_id
                        and p.package_version == package.package_version
                        for p in item.capability_packages
                    )
                ),
                None,
            )
        )
        if record is None:
            raise KeyError(report.package_version_ref)
        activation_ref = package.version_ref
        current = next((item for item in record.capability_activations if item.package_version_ref == activation_ref and item.target_slave == report.target_slave), None)
        if current is not None and report.session_generation < current.runtime_profile.get("session_generation", report.session_generation):
            raise ValueError("stale_session_generation")
        activation = CapabilityPackageActivation(
            package_version_ref=activation_ref,
            target_slave=report.target_slave,
            activation_closure_version_ref=package.package_closure_version_ref,
            compute_binding_ref="",
            evidence_refs=list(report.evidence_refs),
            activation_state=report.activation_state,
            runtime_profile={**(dict(report.details.get("runtime_profile", {})) if isinstance(report.details, dict) else {}), "session_generation": report.session_generation},
        )
        record.capability_activations = [item for item in record.capability_activations if not (item.package_version_ref == activation_ref and item.target_slave == report.target_slave)]
        record.capability_activations.append(activation)
        if report.activation_state == "ready" and package.scope == "workspace_reusable" and package.publication_state == "published":
            operation_ref = package.operation_descriptor_ref
            operation_name = operation_ref.resource_id if isinstance(operation_ref, ResourceRef) else str(operation_ref)
            self.slave_capabilities.setdefault(report.target_slave, {}).setdefault("operations", set()).add(self._operation_name(operation_name))
        record.events.append({"phase": "capability_health_report", "package_ref": activation_ref, "target_slave": report.target_slave, "activation_state": report.activation_state, "evidence_refs": report.evidence_refs, "created_at": datetime.now(timezone.utc).isoformat()})
        await self._persist(record)
        return activation

    @staticmethod
    def _input_binding_for(snapshot: TaskClosure, operation_ref: str) -> NodeInputBinding | None:
        candidates = {operation_ref, ObserverRepository._operation_name(operation_ref), "default"}
        return next((binding for binding in snapshot.node_input_bindings if binding.node_id in candidates), None)

    @classmethod
    def _input_digest_for(cls, snapshot: TaskClosure) -> str | None:
        operation_ref = snapshot.program.operation_ref or snapshot.compute.operation_ref
        binding = cls._input_binding_for(snapshot, operation_ref)
        return binding.input_ref.version_or_digest if binding is not None else None

    @staticmethod
    def _result_digest(value: Any) -> str:
        return hashlib.sha256(canonical_json_bytes(value)).hexdigest()

    @staticmethod
    def _check_attempt_budget(record: RunRecord) -> None:
        budget = record.closure_contract.resource_budget if record.closure_contract is not None else {}
        raw_max_attempts = budget.get("max_attempts")
        if raw_max_attempts is None:
            return
        try:
            max_attempts = int(raw_max_attempts)
        except (TypeError, ValueError) as exc:
            raise ValueError("orchestration_attempt_limit_exceeded") from exc
        if max_attempts < 0 or len(record.attempts) >= max_attempts:
            raise ValueError("orchestration_attempt_limit_exceeded")

    @staticmethod
    def _require_content_ref(ref: ResourceRef) -> None:
        prefix = "content://sha256/"
        if not ref.resource_id.startswith(prefix):
            raise ValueError("content_ref_required")
        resource_digest = ref.resource_id.removeprefix(prefix)
        digest = ref.version_or_digest or ""
        if len(digest) != 64 or any(char not in "0123456789abcdefABCDEF" for char in digest):
            raise ValueError("content_ref_required")
        if resource_digest.lower() != digest.lower():
            raise ValueError("content_ref_digest_mismatch")
        if ref.identity_criterion not in {None, "content_digest"}:
            raise ValueError("content_ref_required")

    @classmethod
    def _collect_content_ref_digests(cls, value: Any, output: set[str]) -> None:
        if isinstance(value, dict):
            if "resource_id" in value and "version_or_digest" in value:
                try:
                    ref = ResourceRef.model_validate(value)
                    cls._require_content_ref(ref)
                    output.add((ref.version_or_digest or "").lower())
                except (TypeError, ValueError):
                    pass
            for item in value.values():
                cls._collect_content_ref_digests(item, output)
        elif isinstance(value, list):
            for item in value:
                cls._collect_content_ref_digests(item, output)

    async def _load_json_content(self, ref: ResourceRef) -> Any:
        self._require_content_ref(ref)
        raw = await self.content_store.get(ref)
        try:
            return json.loads(raw)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid_json") from exc

    @staticmethod
    def _schema_blocker(
        *,
        code: str,
        schema_ref: ResourceRef,
        input_ref: ResourceRef | None = None,
        errors: list[SchemaValidationError] | None = None,
    ) -> dict[str, Any]:
        blocker: dict[str, Any] = {
            "code": code,
            "schema_digest": schema_ref.version_or_digest,
            "errors": [item.model_dump(mode="json") for item in (errors or [])],
        }
        if input_ref is not None:
            blocker["input_ref"] = input_ref.resource_id
        return blocker

    async def _evaluate_input_schema(self, snapshot: TaskClosure, operation_ref: str) -> list[dict[str, Any]]:
        contract_ref = snapshot.program.io_contract_ref
        if contract_ref is None:
            return []
        blockers: list[dict[str, Any]] = []
        try:
            contract_payload = await self._load_json_content(contract_ref)
            io_contract = IoContract.model_validate(contract_payload)
        except (FileNotFoundError, ValueError, TypeError):
            blockers.append({"code": "io_contract_unavailable", "io_contract_ref": contract_ref.resource_id})
            return blockers
        schema_ref = io_contract.input_schema_ref
        if schema_ref is None:
            return blockers
        binding = self._input_binding_for(snapshot, operation_ref)
        if binding is None:
            blockers.append(
                {
                    "code": "payload_missing",
                    "node_id": operation_ref or "default",
                    "schema_digest": schema_ref.version_or_digest,
                }
            )
            return blockers
        try:
            schema = await self._load_json_content(schema_ref)
            validate_schema(schema)
            value = await self._load_json_content(binding.input_ref)
        except (FileNotFoundError, ValueError, TypeError) as exc:
            error = SchemaValidationError(
                path="$",
                keyword="json",
                message="input content is not valid JSON",
                expected="JSON value",
                observed=str(exc),
            )
            blockers.append(self._schema_blocker(code="payload_schema_mismatch", schema_ref=schema_ref, input_ref=binding.input_ref, errors=[error]))
            return blockers
        errors = validate(schema, value)
        if errors:
            blockers.append(self._schema_blocker(code="payload_schema_mismatch", schema_ref=schema_ref, input_ref=binding.input_ref, errors=errors))
        return blockers

    async def _evaluate_orchestration_package(self, snapshot: TaskClosure, *, run_id: str | None = None) -> list[dict[str, Any]]:
        package_ref = snapshot.program_systems.package_ref
        if package_ref is None:
            return [{"code": "orchestration_package_not_bound"}]
        package = self._find_package(package_ref, run_id=run_id)
        if package is None:
            foreign = self._find_package(package_ref)
            if foreign is not None:
                return [{"code": "capability_package_scope_mismatch", "package_ref": self._package_ref(foreign)}]
            return [{"code": "orchestration_package_not_found"}]
        if package.executor_kind != "orchestrator_python_v1":
            return [{"code": "orchestration_package_invalid"}]
        blockers: list[dict[str, Any]] = []
        if (
            package.replay_safety != "DeterministicByEventLog"
            or package.captures_run_state
            or package.captured_secret_refs
            or package.captured_path_refs
        ):
            blockers.append({"code": "orchestration_not_replayable", "package_ref": self._package_ref(package)})
        if self.orchestrator_runtime_available is not False and shutil.which("docker") is None:
            blockers.append({"code": "orchestrator_runtime_unavailable"})
        if run_id is not None and not self._package_visible_to_run(package, run_id):
            blockers.append({"code": "capability_package_scope_mismatch", "package_ref": self._package_ref(package)})
        if package.publication_state == "abandoned":
            blockers.append({"code": "capability_package_abandoned", "package_ref": self._package_ref(package)})
        run_record = self.runs.get(run_id) if run_id is not None else None
        budget = run_record.closure_contract.resource_budget if run_record is not None and run_record.closure_contract is not None else {}
        for field_name, budget_key, code in (
            ("max_nodes", "max_nodes", "orchestration_node_limit_exceeded"),
            ("max_live_nodes", "max_live_nodes", "orchestration_live_node_limit_exceeded"),
        ):
            package_limit = getattr(package, field_name)
            budget_limit = budget.get(budget_key)
            try:
                exceeds_budget = budget_limit is not None and package_limit is not None and int(package_limit) > int(budget_limit)
            except (TypeError, ValueError):
                exceeds_budget = True
            if exceeds_budget:
                blockers.append({"code": code, "package_ref": self._package_ref(package), "limit": package_limit, "budget": budget_limit})
        closure_contract_ref = snapshot.program.io_contract_ref
        package_contract_ref = package.io_contract_ref
        if (
            closure_contract_ref is None
            or package_contract_ref is None
            or (closure_contract_ref.version_or_digest or closure_contract_ref.resource_id)
            != (package_contract_ref.version_or_digest or package_contract_ref.resource_id)
        ):
            blockers.append({"code": "io_contract_mismatch", "package_ref": self._package_ref(package)})
        stat = await self.content_store.stat(package.program_content_ref)
        if stat is None or not stat.integrity_verified or stat.declared_digest != package.program_digest:
            blockers.append({"code": "package_content_unavailable", "package_ref": self._package_ref(package)})
        elif stat.media_type != "text/x-python":
            blockers.append({"code": "orchestration_program_invalid", "package_ref": self._package_ref(package)})
        else:
            try:
                source = (await self.content_store.get(package.program_content_ref)).decode("utf-8")
                ast.parse(source)
            except (UnicodeDecodeError, SyntaxError):
                blockers.append({"code": "orchestration_program_invalid", "package_ref": self._package_ref(package)})
            else:
                signature_blockers = validate_entry_signature(source)
                if signature_blockers:
                    blockers.append(
                        {
                            "code": "orchestration_program_type_error",
                            "diagnostics": [
                                {
                                    "file": "orchestration.py",
                                    "line": 1,
                                    "column": 1,
                                    "end_line": 1,
                                    "end_column": 1,
                                    "severity": "error",
                                    "rule": str(item.get("code", "invalid_entry_annotation")),
                                    "message": str(item.get("message", "invalid orchestration entry signature")),
                                }
                                for item in signature_blockers
                            ],
                        }
                    )
                else:
                    try:
                        diagnostics = await check_program(source)
                    except OrchestrationPreflightError as exc:
                        blockers.append({"code": exc.code, **exc.details})
                    else:
                        unresolved = [item for item in diagnostics if item.get("rule") == "reportUndefinedVariable"]
                        type_errors = [item for item in diagnostics if item.get("rule") != "reportUndefinedVariable"]
                        if unresolved:
                            blockers.append({"code": "orchestration_program_unresolved_name", "diagnostics": unresolved})
                        if type_errors:
                            blockers.append({"code": "orchestration_program_type_error", "diagnostics": type_errors})
        for node_package_ref in package.allowed_node_package_refs:
            node_package = self._find_package(node_package_ref, run_id=run_id)
            if node_package is None:
                blockers.append({"code": "node_package_not_found", "package_ref": node_package_ref.resource_id})
                continue
            if node_package.executor_kind != "subprocess_json_v1" or node_package.executor_operation != "run_code":
                blockers.append({"code": "node_package_invalid", "package_ref": node_package_ref.resource_id})
            if run_id is not None and not self._package_visible_to_run(node_package, run_id):
                blockers.append({"code": "capability_package_scope_mismatch", "package_ref": self._package_ref(node_package)})
            if node_package.io_contract_ref is None:
                blockers.append({"code": "node_io_contract_unavailable", "package_ref": self._package_ref(node_package)})
            else:
                try:
                    IoContract.model_validate(await self._load_json_content(node_package.io_contract_ref))
                except (FileNotFoundError, TypeError, ValueError):
                    blockers.append({"code": "node_io_contract_unavailable", "package_ref": self._package_ref(node_package)})
            stat = await self.content_store.stat(node_package.program_content_ref)
            if stat is None or not stat.integrity_verified or stat.declared_digest != node_package.program_digest:
                blockers.append({"code": "package_content_unavailable", "package_ref": self._package_ref(node_package)})
            if not any(self._slave_supports_package(slave_id, node_package) for slave_id in self.slave_capabilities):
                blockers.append({"code": "node_target_unavailable", "package_ref": self._package_ref(node_package)})
        return blockers

    async def _evaluate_readiness(self, snapshot: TaskClosure, *, run_id: str | None = None) -> dict[str, Any]:
        if run_id is not None:
            record = self.runs.get(run_id)
            if record is not None:
                workspace_id = record.closure_contract.workspace_id if record.closure_contract is not None else "workspace-default"
                await self.refresh_slaves(workspace_id)
        operation_ref = snapshot.program.operation_ref or snapshot.compute.operation_ref
        operation = self._operation_name(operation_ref)
        blockers: list[dict[str, Any]] = []
        inline_io_fields = [
            field_name
            for field_name in ("input_schema", "output_schema", "success_semantics")
            if getattr(snapshot.program, field_name, None) is not None
        ]
        if inline_io_fields:
            blockers.append(
                {
                    "code": "inline_io_contract_forbidden",
                    "fields": inline_io_fields,
                }
            )
        blockers.extend(await self._evaluate_input_schema(snapshot, operation_ref))
        is_orchestration = snapshot.program_systems.executor_kind == "orchestrator_python_v1"
        if is_orchestration:
            blockers.extend(await self._evaluate_orchestration_package(snapshot, run_id=run_id))
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
            if not self._slave_is_active(target):
                blockers.append({"code": "slave_unavailable", "hole_id": hole.hole_id, "target_resource_ref": target})
            package = self._find_package(binding.capability_package_ref, run_id=run_id) if binding.capability_package_ref else None
            if binding.capability_package_ref and package is None:
                foreign = self._find_package(binding.capability_package_ref)
                if foreign is not None:
                    blockers.append({"code": "capability_package_scope_mismatch", "hole_id": hole.hole_id, "source_run_ref": foreign.source_run_ref})
                else:
                    blockers.append({"code": "capability_package_not_found", "hole_id": hole.hole_id})
            elif package is not None:
                if run_id is not None and not self._package_visible_to_run(package, run_id):
                    blockers.append({"code": "capability_package_scope_mismatch", "hole_id": hole.hole_id, "source_run_ref": package.source_run_ref})
                closure_contract_ref = snapshot.program.io_contract_ref
                package_contract_ref = package.io_contract_ref
                if (
                    closure_contract_ref is None
                    or package_contract_ref is None
                    or (closure_contract_ref.version_or_digest or closure_contract_ref.resource_id)
                    != (package_contract_ref.version_or_digest or package_contract_ref.resource_id)
                ):
                    blockers.append(
                        {
                            "code": "io_contract_mismatch",
                            "hole_id": hole.hole_id,
                            "closure_io_contract_ref": closure_contract_ref.resource_id if closure_contract_ref else None,
                            "package_io_contract_ref": package_contract_ref.resource_id if package_contract_ref else None,
                        }
                    )
                if package.publication_state == "abandoned":
                    blockers.append({"code": "capability_package_abandoned", "package_ref": self._package_ref(package)})
                stat = await self.content_store.stat(package.program_content_ref)
                if stat is None or not stat.integrity_verified or stat.declared_digest != package.program_digest:
                    blockers.append({"code": "package_content_unavailable", "hole_id": hole.hole_id})
                if binding.realization_digest and binding.realization_digest not in {package.program_digest, package.package_digest}:
                    blockers.append({"code": "capability_package_digest_mismatch", "hole_id": hole.hole_id})
                if package.provider_fillable_hole_refs and not binding.runtime_profile:
                    blockers.append({"code": "provider_fillable_hole_unbound", "hole_id": hole.hole_id, "hole_refs": package.provider_fillable_hole_refs})
                if "run_code" not in capability.get("operations", set()):
                    blockers.append({"code": "capability_unavailable", "operation": "run_code", "target_resource_ref": target})
                try:
                    expected_executor_digest = default_registry.get(package.executor_kind).descriptor.digest
                    if binding.executor_descriptor_digest and binding.executor_descriptor_digest != expected_executor_digest:
                        blockers.append({"code": "executor_descriptor_mismatch", "hole_id": hole.hole_id})
                except ValueError:
                    blockers.append({"code": "executor_unavailable", "executor_kind": package.executor_kind})
                descriptor_ref = package.operation_descriptor_ref
                descriptor_name = descriptor_ref.resource_id if isinstance(descriptor_ref, ResourceRef) else str(descriptor_ref)
                if operation and self._operation_name(descriptor_name) != operation:
                    blockers.append({"code": "operation_descriptor_mismatch", "operation": operation})
            elif operation and operation not in capability.get("operations", set()):
                blockers.append({"code": "capability_unavailable", "operation": operation, "target_resource_ref": target})

        if operation and not snapshot.compute.typed_holes and not is_orchestration:
            default_target = "slave-a"
            capability = self.slave_capabilities.get(default_target, {})
            if not self._slave_is_active(default_target):
                blockers.append({"code": "slave_unavailable", "target_resource_ref": default_target})
            if operation not in capability.get("operations", set()):
                blockers.append({"code": "capability_unavailable", "operation": operation, "target_resource_ref": default_target})

        return {
            "ready": not blockers,
            "operation_ref": operation_ref,
            "operation": operation,
            "blockers": blockers,
            "bindings": [binding.model_dump(mode="json") for binding in snapshot.compute_bindings],
            "orchestration": is_orchestration,
        }

    @staticmethod
    def _readiness_error(readiness: dict[str, Any]) -> DomainError:
        return DomainError(
            DomainErrorEnvelope(
                code="readiness_blocked",
                details={"blockers": deepcopy(readiness.get("blockers", []))},
            )
        )

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
                resource_budget={"max_node_concurrency": 1},
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

    async def append_message(self, run_id: str, role: str, content: str, *, request_id: str | None = None) -> dict[str, Any]:
        if role not in {"user", "assistant"}:
            raise ValueError("unsupported_message_role")
        if not content:
            raise ValueError("empty_message")
        record = await self._load(run_id)
        if request_id:
            existing = next((event for event in record.events if event.get("phase") == "message" and event.get("request_id") == request_id), None)
            if existing is not None:
                return existing
        message = {
            "phase": "message",
            "message_id": f"message-{uuid4().hex[:12]}",
            "role": role,
            "content": content,
            "run_id": run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        if request_id:
            message["request_id"] = request_id
        record.events.append(message)
        await self._persist(record)
        return message

    async def begin_refinement(self, run_id: str) -> RunRecord:
        record = await self._load(run_id)
        if record.state == "opened":
            _transition(record, "refinement_started")
            await self._persist(record)
        elif record.state not in {"thinking"}:
            raise ValueError("illegal_state_transition")
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
        readiness = await self._evaluate_readiness(snapshot, run_id=run_id)
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

    async def cancel_run(self, run_id: str, reason: str | dict[str, Any] = "user_interrupt") -> RunRecord:
        record = await self._load(run_id)
        if record.state in {"completed", "closed", "failed", "cancelled"}:
            return record
        _transition(record, "run_cancelled", reason=reason)
        record.outcome = {
            "disposition": "cancelled",
            "decision": None,
            "terminal_state": record.outcome.get("terminal_state") if isinstance(record.outcome, dict) else None,
            "terminal_error": reason,
            "reason": reason,
            "execution_id": record.execution_id,
            "execution_epoch": record.execution_epoch,
        }
        await self._persist(record)
        return record

    async def fail_run(self, run_id: str, reason: str | dict[str, Any]) -> RunRecord:
        record = await self._load(run_id)
        if record.state in {"completed", "closed", "cancelled", "failed"}:
            return record
        _transition(record, "run_failed", reason=reason)
        record.outcome = {
            "disposition": "failed",
            "decision": None,
            "terminal_state": None,
            "terminal_error": reason,
            "reason": reason,
            "execution_id": record.execution_id,
            "execution_epoch": record.execution_epoch,
        }
        await self._persist(record)
        return record

    async def record_agent_signal(self, run_id: str, kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Persist non-terminal app-server status/warning signals on a Run."""
        record = await self._load(run_id)
        event = {
            "phase": "coding_agent_signal",
            "run_id": run_id,
            "kind": kind,
            "payload": deepcopy(payload),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        record.events.append(event)
        await self._persist(record)
        return event

    async def list_conversations(self, workspace_id: str = "workspace-default") -> list[dict[str, Any]]:
        groups: dict[str, list[RunRecord]] = {}
        for record in await self._all_records():
            groups.setdefault(record.task_ref, []).append(record)
        receipts = await self.list_message_receipts(workspace_id)
        receipt_groups: dict[str, list[MessageReceipt]] = {}
        for receipt in receipts:
            receipt_groups.setdefault(receipt.conversation_ref, []).append(receipt)
            groups.setdefault(receipt.conversation_ref, [])
        summaries: list[tuple[str, dict[str, Any]]] = []
        for conversation_ref, records in groups.items():
            records.sort(key=self._record_order_key)
            conversation_receipts = receipt_groups.get(conversation_ref, [])
            latest_receipt = conversation_receipts[-1] if conversation_receipts else None
            latest_record = records[-1] if records else None
            latest_key = latest_receipt.created_at.isoformat() if latest_receipt else self._record_order_key(latest_record) if latest_record else ""
            title = (latest_receipt.prompt if latest_receipt else latest_record.goal if latest_record else "New conversation") or "New conversation"
            status = self._conversation_status(records)
            if latest_receipt is not None and (latest_record is None or latest_receipt.updated_at.isoformat() >= self._record_order_key(latest_record)):
                status = self._message_receipt_status(latest_receipt.state)
            summaries.append(
                (
                    latest_key,
                    {
                        "conversation_ref": conversation_ref,
                        "title": title,
                        "run_count": len(records),
                        "latest_run_id": latest_record.run_id if latest_record else (latest_receipt.run_id if latest_receipt else None),
                        "status": status,
                    },
                )
            )
        return [summary for _, summary in sorted(summaries, key=lambda item: item[0])]

    async def get_conversation(self, conversation_ref: str, workspace_id: str = "workspace-default") -> dict[str, Any]:
        records = [record for record in await self._all_records() if record.task_ref == conversation_ref]
        receipts = await self.list_message_receipts(workspace_id, conversation_ref)
        if not records and not receipts:
            raise KeyError(conversation_ref)
        records.sort(key=self._record_order_key)
        messages: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        run_summaries: list[dict[str, Any]] = []
        receipt_run_ids = {receipt.run_id for receipt in receipts if receipt.run_id}
        receipts_by_run = {receipt.run_id: receipt for receipt in receipts if receipt.run_id}
        for receipt in receipts:
            messages.append(
                {
                    "message_id": f"receipt-{receipt.request_id}",
                    "role": "user",
                    "content": receipt.prompt,
                    "run_id": receipt.run_id,
                    "request_id": receipt.request_id,
                    "status": self._message_receipt_status(receipt.state),
                    "_projection_order": receipt.created_at.isoformat() + ":0",
                }
            )
            if receipt.assistant_text:
                messages.append(
                    {
                        "message_id": f"receipt-{receipt.request_id}-assistant",
                        "role": "assistant",
                        "content": receipt.assistant_text,
                        "run_id": receipt.run_id,
                        "request_id": receipt.request_id,
                        "_projection_order": receipt.created_at.isoformat() + ":1",
                    }
                )
        receipt_events = [
            deepcopy(event)
            for event in self.message_receipt_events
            if event.get("workspace_id") == workspace_id and event.get("conversation_ref") == conversation_ref
        ]
        known_receipts = {str(event.get("request_id")) for event in receipt_events}
        for receipt in receipts:
            if receipt.request_id not in known_receipts:
                receipt_events.append(
                    {
                        "phase": "message_receipt",
                        "workspace_id": workspace_id,
                        "request_id": receipt.request_id,
                        "conversation_ref": conversation_ref,
                        "state": receipt.state,
                        "created_at": receipt.updated_at.isoformat(),
                    }
                )
        events.extend(receipt_events)
        for record in records:
            has_user_message = False
            for event in record.events:
                events.append(event)
                if event.get("phase") != "message" or event.get("role") not in {"user", "assistant"}:
                    continue
                if record.run_id in receipt_run_ids:
                    continue
                message = {
                    "message_id": event.get("message_id", f"legacy-{record.run_id}"),
                    "role": event["role"],
                    "content": event.get("content", ""),
                    "run_id": event.get("run_id", record.run_id),
                    "_projection_order": str(event.get("created_at") or self._record_order_key(record)),
                }
                messages.append(message)
                has_user_message = has_user_message or message["role"] == "user"
            if not has_user_message and record.run_id not in receipt_run_ids:
                messages.append({"message_id": f"legacy-{record.run_id}", "role": "user", "content": record.goal, "run_id": record.run_id, "_projection_order": self._record_order_key(record)})
            run_summaries.append(
                {
                    "run_id": record.run_id,
                    "state": record.state,
                    "status": self._message_receipt_status(receipts_by_run[record.run_id].state) if record.run_id in receipts_by_run else self._conversation_status_for_state(record.state),
                    "committed_version": record.committed.version_id if record.committed else None,
                    "execution_id": record.execution_id,
                    "outcome": receipts_by_run.get(record.run_id).outcome if receipts_by_run.get(record.run_id) is not None and receipts_by_run[record.run_id].outcome is not None else record.outcome,
                    "receipt_state": receipts_by_run[record.run_id].state if record.run_id in receipts_by_run else None,
                    "dynamic_nodes": [node.model_dump(mode="json") for node in record.dynamic_nodes],
                    "capability_packages": [package.model_dump(mode="json") for package in record.capability_packages],
                    "capability_activations": [activation.model_dump(mode="json") for activation in record.capability_activations],
                }
            )
        messages.sort(key=lambda item: str(item.get("_projection_order") or ""))
        for message in messages:
            message.pop("_projection_order", None)
        events.sort(key=lambda item: str(item.get("created_at") or ""))
        status = self._conversation_status(records)
        if receipts:
            latest_receipt = receipts[-1]
            latest_record = records[-1] if records else None
            if latest_record is None or latest_receipt.updated_at.isoformat() >= self._record_order_key(latest_record):
                status = self._message_receipt_status(latest_receipt.state)
        return {
            "conversation_ref": conversation_ref,
            "status": status,
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
        if record.state not in {"opened", "thinking", "awaiting_decision"}:
            raise ValueError("illegal_state_transition")
        if record.state == "awaiting_decision" and (
            not isinstance(record.outcome, dict) or record.outcome.get("decision") != "repair"
        ):
            raise ValueError("illegal_state_transition")
        reopen = record.state == "awaiting_decision"
        begin = record.state == "opened"
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
            elif kind == "set_io_contract_ref":
                value = operation.get("value")
                if isinstance(value, dict) and "io_contract_ref" in value:
                    value = value["io_contract_ref"]
                try:
                    snapshot.program.io_contract_ref = ResourceRef.model_validate(value)
                except Exception as exc:
                    raise ValueError("io_contract_ref_required") from exc
            elif kind == "add_typed_hole":
                hole = TypedHole.model_validate(operation["value"])
                if any(existing.hole_id == hole.hole_id for existing in snapshot.compute.typed_holes):
                    raise ValueError(f"duplicate_typed_hole:{hole.hole_id}")
                snapshot.compute.typed_holes.append(hole)
            elif kind == "materialize_capability_package_candidate":
                value = dict(operation.get("value") or operation)
                package_id = str(value.get("package_id") or f"package-{uuid4().hex[:12]}")
                package_version = str(value.get("package_version") or "v1")
                io_contract_payload = value.get("io_contract_ref")
                if not io_contract_payload:
                    raise ValueError("io_contract_required")
                try:
                    io_contract_ref = ResourceRef.model_validate(io_contract_payload)
                    IoContract.model_validate(await self._load_json_content(io_contract_ref))
                except (FileNotFoundError, TypeError, ValueError) as exc:
                    if str(exc) in {"io_contract_invalid", "invalid_json"}:
                        raise
                    raise ValueError("io_contract_invalid") from exc
                program_ref_payload = value.get("program_content_ref") or value.get("program_ref")
                if isinstance(program_ref_payload, ResourceRef):
                    program_ref = program_ref_payload
                elif isinstance(program_ref_payload, dict):
                    program_ref = ResourceRef.model_validate(program_ref_payload)
                else:
                    raise ValueError("program_content_ref_required")
                self._require_content_ref(program_ref)
                expected_digest = str(value.get("expected_program_digest") or value.get("program_digest") or "")
                actual_digest = program_ref.version_or_digest or ""
                if expected_digest and expected_digest != actual_digest:
                    raise ValueError("program_digest_mismatch")
                operation_ref = value.get("operation_descriptor_ref") or snapshot.program.operation_ref or snapshot.compute.operation_ref
                if isinstance(operation_ref, dict):
                    operation_ref = ResourceRef.model_validate(operation_ref)
                else:
                    operation_ref = ResourceRef(resource_id=str(operation_ref), identity_criterion="descriptor_digest")
                descriptor_identity = operation_ref.resource_id if isinstance(operation_ref, ResourceRef) else str(operation_ref)
                if not descriptor_identity:
                    raise ValueError("operation_descriptor_required")
                descriptor_digest = str(value.get("operation_descriptor_digest") or hashlib.sha256(descriptor_identity.encode()).hexdigest())
                allowed_node_package_refs = [ResourceRef.model_validate(item) for item in value.get("allowed_node_package_refs", [])]
                max_nodes = value.get("max_nodes")
                max_live_nodes = value.get("max_live_nodes")
                package = CapabilityPackageVersion(
                    package_id=package_id,
                    package_version=package_version,
                    package_closure_version_ref=f"package-closure-{uuid4().hex[:12]}",
                    source_run_ref=record.run_id,
                    source_closure_version_ref=record.draft.version_id,
                    operation_descriptor_ref=operation_ref,
                    operation_descriptor_digest=descriptor_digest,
                    program_content_ref=program_ref,
                    program_digest=actual_digest,
                    io_contract_ref=io_contract_ref,
                    effective_constraint_refs=[Constraint.model_validate(item).ref() if isinstance(item, dict) else ConstraintRef.model_validate(item) for item in value.get("effective_constraint_refs", [])],
                    provider_fillable_hole_refs=[str(item) for item in value.get("provider_fillable_hole_refs", [])],
                    provenance=[{"source": "coding_agent", "run_id": record.run_id}],
                    executor_kind=str(value.get("executor_kind") or "subprocess_json_v1"),
                    executor_operation=str(value.get("executor_operation") or "run_code"),
                    effect_class=str(value.get("effect_class") or "Sandboxed"),
                    permissions=[str(item) for item in value.get("permissions", [])],
                    replay_safety=str(
                        value.get("replay_safety")
                        or (
                            "DeterministicByEventLog"
                            if str(value.get("executor_kind") or "subprocess_json_v1") == "orchestrator_python_v1"
                            else "DeclaredByPackage"
                        )
                    ),
                    captures_run_state=bool(value.get("captures_run_state", value.get("run_specific_capture", False))),
                    captured_secret_refs=[str(item) for item in value.get("captured_secret_refs", value.get("secret_refs", []))],
                    captured_path_refs=[str(item) for item in value.get("captured_path_refs", value.get("raw_path_refs", []))],
                    semantic_closed=bool(value.get("semantic_closed", True)),
                    allowed_node_package_refs=allowed_node_package_refs,
                    max_nodes=int(max_nodes) if max_nodes is not None else None,
                    max_live_nodes=int(max_live_nodes) if max_live_nodes is not None else None,
                )
                # Re-materializing the exact immutable package is idempotent,
                # but a logical coordinate must not silently change meaning
                # within one Run.  A caller that changes the program or any
                # other package field must choose a new version (or id).
                existing = next(
                    (
                        item
                        for item in record.capability_packages
                        if item.package_id == package.package_id
                        and item.package_version == package.package_version
                    ),
                    None,
                )
                if existing is None:
                    record.capability_packages.append(package)
                elif existing.package_digest != package.package_digest:
                    raise ValueError("capability_package_identity_conflict")
                else:
                    package = existing
                if package.executor_kind == "orchestrator_python_v1":
                    snapshot.program_systems.executor_kind = package.executor_kind
                    snapshot.program_systems.package_ref = ResourceRef(resource_id=self._package_ref(package), version_or_digest=package.package_digest)
                    snapshot.program_systems.replay_safety = package.replay_safety
                snapshot.metadata.setdefault("capability_package_refs", []).append(self._package_ref(package))
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
                value = operation.get("value", {})
                if not isinstance(value, dict) or "input_ref" not in value:
                    raise ValueError("input_ref_required")
                try:
                    input_ref = ResourceRef.model_validate(value["input_ref"])
                except Exception as exc:
                    raise ValueError("input_ref_required") from exc
                node_id = str(value.get("node_id") or snapshot.program.operation_ref or snapshot.compute.operation_ref or "default")
                binding = NodeInputBinding(node_id=node_id, input_ref=input_ref, provenance=list(value.get("provenance", [])))
                snapshot.node_input_bindings = [item for item in snapshot.node_input_bindings if item.node_id != node_id]
                snapshot.node_input_bindings.append(binding)
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
        if begin:
            _transition(record, "refinement_started")
        elif reopen:
            _transition(record, "refinement_started")
            record.outcome = None
        receipt = PatchReceipt(
            receipt=f"receipt-{uuid4().hex[:12]}",
            run_id=run_id,
            kind="draft",
            draft_version=new_version.version_id,
            draft_digest=new_version.snapshot_digest,
            snapshot=snapshot,
            patch_cursor=new_version.patch_cursor,
        )
        receipt.readiness = await self._evaluate_readiness(snapshot, run_id=run_id)
        self.idempotency[operation_id] = receipt
        record.events.append({"phase": "draft_patched", "operation_id": operation_id, "version": new_version.version_id})
        package_refs = snapshot.metadata.get("capability_package_refs", [])
        if package_refs:
            record.events.append({"phase": "capability_package_candidate_materialized", "operation_id": operation_id, "package_refs": package_refs})
        if self.sessions is not None:
            async with self.sessions() as session:
                session.add(IdempotencyRow(operation_id=operation_id, receipt={"receipt": receipt.receipt, "run_id": receipt.run_id, "kind": receipt.kind, "draft_version": receipt.draft_version, "draft_digest": receipt.draft_digest, "patch_cursor": receipt.patch_cursor, "readiness": receipt.readiness}))
                await session.commit()
        await self._persist(record)
        return receipt

    async def commit(self, run_id: str, version_id: str, digest: str) -> ClosureVersion:
        record = await self._load(run_id)
        if record.state != "thinking":
            raise ValueError("illegal_state_transition")
        if record.draft.version_id != version_id or record.draft.snapshot_digest != digest:
            raise ValueError("version_conflict")
        readiness = await self._evaluate_readiness(record.draft.snapshot, run_id=run_id)
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
        _transition(record, "committed")
        record.events.append({"phase": "committed", "version": committed.version_id})
        await self._persist(record)
        return committed

    async def start(self, run_id: str, version_id: str) -> dict[str, Any]:
        record = await self._load(run_id)
        if record.state == "running" and record.execution_id is not None:
            if record.committed is None or record.committed.version_id != version_id:
                raise ValueError("closure_not_committed")
            return {"execution_id": record.execution_id, "state": record.state, "execution_epoch": record.execution_epoch}
        if record.state != "committed":
            raise ValueError("illegal_state_transition")
        if record.committed is None or record.committed.version_id != version_id:
            raise ValueError("closure_not_committed")
        readiness = await self._evaluate_readiness(record.committed.snapshot, run_id=run_id)
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
        if record.committed.snapshot.program_systems.executor_kind != "orchestrator_python_v1":
            self._check_attempt_budget(record)
        previous_execution_id = record.execution_id
        if previous_execution_id is None:
            record.execution_epoch = 1
        else:
            record.execution_epoch += 1
        record.execution_id = f"execution-{uuid4().hex[:12]}"
        record.outcome = None
        _transition(record, "execution_started")
        if record.committed.snapshot.program_systems.executor_kind != "orchestrator_python_v1":
            target = "slave-a"
            if record.committed.snapshot.compute_bindings:
                target = self._canonical_target_resource_id(
                    record.committed.snapshot.compute_bindings[0].target_resource_ref.resource_id
                )
            record.attempts.append({"attempt_id": f"attempt-{uuid4().hex[:12]}", "target": target, "state": "created", "execution_epoch": record.execution_epoch})
        record.events.append(
            {
                "phase": "execution_started",
                "execution_id": record.execution_id,
                "execution_epoch": record.execution_epoch,
            }
        )
        if record.committed.snapshot.program_systems.executor_kind == "orchestrator_python_v1":
            record.events.append(
                {
                    "phase": "orchestration_started",
                    "run_id": run_id,
                    "execution_id": record.execution_id,
                    "execution_epoch": record.execution_epoch,
                    "package_ref": record.committed.snapshot.program_systems.package_ref.model_dump(mode="json")
                    if record.committed.snapshot.program_systems.package_ref is not None
                    else None,
                    "provenance": self._orchestration_provenance(
                        record,
                        self._find_package(
                            record.committed.snapshot.program_systems.package_ref,
                            run_id=run_id,
                        )
                        if record.committed.snapshot.program_systems.package_ref is not None
                        else None,
                    ),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        await self._persist(record)
        return {"execution_id": record.execution_id, "state": record.state, "execution_epoch": record.execution_epoch}

    async def get_run(self, run_id: str) -> RunRecord:
        return await self._load(run_id)

    async def record_result(self, run_id: str, result: dict[str, Any]) -> RunRecord:
        record = await self._load(run_id)
        if record.state != "running":
            raise ValueError("execution_not_running")

        # Terminal reports are scoped to the exact attempt and execution epoch
        # that Observer created.  Never let a late retry (or a report for a
        # different attempt) mutate the current Run projection.
        attempt_id = result.get("attempt_id")
        attempt = next((item for item in record.attempts if item.get("attempt_id") == attempt_id), None)
        if attempt is None:
            raise ValueError("stale_attempt")
        try:
            execution_epoch = int(result.get("execution_epoch"))
        except (TypeError, ValueError) as exc:
            raise ValueError("stale_execution_epoch") from exc
        if execution_epoch != record.execution_epoch:
            raise ValueError("stale_execution_epoch")
        if attempt.get("execution_epoch", execution_epoch) != execution_epoch:
            raise ValueError("stale_execution_epoch")
        if attempt.get("state") not in {"created", "running"}:
            raise ValueError("stale_attempt")
        if result.get("execution_id") != record.execution_id:
            raise ValueError("stale_execution_id")

        if "value" not in result or "digest" not in result:
            raise ValueError("output_missing")
        expected_digest = self._result_digest(result["value"])
        if result.get("digest") != expected_digest:
            raise ValueError("output_digest_mismatch")
        raw_resource_ref = result.get("resource_ref")
        if raw_resource_ref is not None:
            try:
                resource_ref = ResourceRef.model_validate(raw_resource_ref)
            except Exception as exc:
                raise ValueError("invalid_resource_ref") from exc
            if resource_ref.version_or_digest != expected_digest:
                raise ValueError("resource_ref_digest_mismatch")

        # Validate Slave-produced evidence before accepting it.  Evidence is
        # descriptive, but it is still fenced to this attempt/epoch and to
        # the contract refs Observer resolved below.
        raw_evidence = result.get("validation_evidence") or []
        if not isinstance(raw_evidence, list):
            raise ValueError("invalid_validation_evidence")
        evidence: list[dict[str, Any]] = []
        for item in raw_evidence:
            try:
                parsed = ValidationEvidence.model_validate(item)
            except Exception as exc:
                raise ValueError("invalid_validation_evidence") from exc
            if parsed.attempt_id != attempt_id or parsed.execution_epoch != execution_epoch:
                raise ValueError("stale_validation_evidence")
            if parsed.issuer != "slave":
                raise ValueError("invalid_validation_evidence")
            output_digest = result.get("digest")
            if parsed.output_digest is not None and output_digest is not None and parsed.output_digest != output_digest:
                raise ValueError("validation_evidence_digest_mismatch")
            evidence.append(parsed.model_dump(mode="json"))

        snapshot = record.committed.snapshot if record.committed is not None else None
        contract: IoContract | None = None
        output_schema_ref: ResourceRef | None = None
        if snapshot is not None and snapshot.program.io_contract_ref is not None:
            try:
                contract = IoContract.model_validate(await self._load_json_content(snapshot.program.io_contract_ref))
            except (FileNotFoundError, TypeError, ValueError) as exc:
                raise ValueError("io_contract_invalid") from exc
            output_schema_ref = contract.output_schema_ref

        expected_input_digest = self._input_digest_for(snapshot) if snapshot is not None else None
        expected_validator_ref = contract.success_validator_ref if contract else None
        def _same_ref(left: ResourceRef | None, right: ResourceRef | None) -> bool:
            if left is None or right is None:
                return left is right
            return (left.version_or_digest or left.resource_id) == (right.version_or_digest or right.resource_id)

        for item in evidence:
            parsed_schema = ResourceRef.model_validate(item["schema_ref"]) if item.get("schema_ref") is not None else None
            parsed_validator = ResourceRef.model_validate(item["validator_ref"]) if item.get("validator_ref") is not None else None
            if output_schema_ref is not None and not _same_ref(parsed_schema, output_schema_ref):
                raise ValueError("validation_evidence_schema_mismatch")
            if not _same_ref(parsed_validator, expected_validator_ref):
                raise ValueError("validation_evidence_validator_mismatch")
            if item.get("input_digest") is not None and item.get("input_digest") != expected_input_digest:
                raise ValueError("validation_evidence_input_digest_mismatch")
        # Observer is the final authority: it repeats output schema
        # validation even when a Slave supplied a pass evidence.  A mismatch
        # is a failed Attempt/ExecutionOutcome and a decision point for the
        # coding agent; it must never be projected as completed.
        observer_evidence: dict[str, Any] | None = None
        if output_schema_ref is not None:
            try:
                schema = await self._load_json_content(output_schema_ref)
                validate_schema(schema)
                schema_errors = validate(schema, result.get("value"))
            except (FileNotFoundError, TypeError, ValueError) as exc:
                schema_errors = [
                    SchemaValidationError(
                        path="$",
                        keyword="schema",
                        message="output schema could not be evaluated",
                        expected="valid output schema",
                        observed=str(exc),
                    )
                ]
            observer_evidence = ValidationEvidence(
                evidence_id=f"evidence-{attempt_id}-{uuid4().hex[:10]}",
                attempt_id=attempt_id,
                execution_epoch=execution_epoch,
                validator_ref=contract.success_validator_ref if contract else None,
                schema_ref=output_schema_ref,
                input_digest=self._input_digest_for(snapshot) if snapshot is not None else None,
                output_digest=result.get("digest"),
                result="fail" if schema_errors else "pass",
                errors=[item.model_dump(mode="json") for item in schema_errors],
                issuer="observer",
                created_at=datetime.now(timezone.utc).isoformat(),
            ).model_dump(mode="json")
            evidence.append(observer_evidence)

            if schema_errors:
                attempt["state"] = "failed"
                terminal_error = {"code": "output_schema_mismatch", "errors": observer_evidence["errors"]}
                outcome = {
                    **result,
                    "disposition": "awaiting_decision",
                    "decision": "repair",
                    "resource_ref": None,
                    "terminal_state": "failed",
                    "terminal_error": terminal_error,
                    "error": terminal_error,
                    "validation_evidence": evidence,
                }
                record.outcome = outcome
                _transition(record, "run_needs_decision", reason=terminal_error)
                record.events.append(
                    {
                        "phase": "io_schema_rejected",
                        "execution_id": record.execution_id,
                        "attempt_id": attempt_id,
                        "execution_epoch": execution_epoch,
                        "evidence_id": observer_evidence["evidence_id"],
                        "error": outcome["terminal_error"],
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                await self._persist(record)
                return record

        validator_terminal_error: dict[str, Any] | None = None
        if expected_validator_ref is not None:
            validator_errors: list[dict[str, Any]] = []
            validator_status = "fail"
            try:
                validator_program = await self.content_store.get(expected_validator_ref)
                validator_input = result["value"] if isinstance(result["value"], dict) else {"value": result["value"]}
                validator_result = await default_registry.execute(
                    "subprocess_json_v1",
                    "run_code",
                    validator_input,
                    program=validator_program,
                )
                validator_payload = validator_result.value if isinstance(validator_result.value, dict) else {}
                validator_status = str(validator_payload.get("result") or "fail")
                raw_errors = validator_payload.get("errors")
                if isinstance(raw_errors, list):
                    validator_errors = [
                        {
                            "path": str(item.get("path", "$")),
                            "keyword": str(item.get("keyword", "validator")),
                            "message": str(item.get("message", "validator failed"))[:512],
                        }
                        for item in raw_errors
                        if isinstance(item, dict)
                    ]
            except Exception as exc:
                validator_errors = [{"path": "$", "keyword": "validator", "message": str(exc)[:512]}]
            validator_evidence = ValidationEvidence(
                evidence_id=f"evidence-{attempt_id}-{uuid4().hex[:10]}",
                attempt_id=attempt_id,
                execution_epoch=execution_epoch,
                validator_ref=expected_validator_ref,
                schema_ref=output_schema_ref,
                input_digest=expected_input_digest,
                output_digest=result.get("digest"),
                result="pass" if validator_status == "pass" else "fail",
                errors=validator_errors,
                issuer="observer",
                created_at=datetime.now(timezone.utc).isoformat(),
            ).model_dump(mode="json")
            evidence.append(validator_evidence)
            if validator_status != "pass":
                validator_terminal_error = {"code": "success_validation_failed", "errors": validator_errors}

        terminal_state = str(result.get("terminal_state") or "completed")
        if terminal_state not in {"completed", "failed", "decision_required"}:
            raise ValueError("invalid_terminal_state")
        if validator_terminal_error is not None and terminal_state == "completed":
            terminal_state = "failed"
        terminal_error = validator_terminal_error or result.get("terminal_error")
        if terminal_state == "failed":
            attempt["state"] = "failed"
            record.outcome = {
                **result,
                "disposition": "awaiting_decision",
                "decision": "repair",
                "resource_ref": None,
                "terminal_state": "failed",
                "terminal_error": terminal_error,
                "error": terminal_error,
                "validation_evidence": evidence,
            }
            _transition(record, "run_needs_decision", reason=terminal_error)
            record.events.append(
                {
                    "phase": "io_schema_rejected" if terminal_error and terminal_error.get("code") in {"output_schema_mismatch", "success_validation_failed"} else "execution_failed",
                    "execution_id": record.execution_id,
                    "attempt_id": attempt_id,
                    "execution_epoch": execution_epoch,
                    "error": terminal_error,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        elif terminal_state == "decision_required":
            attempt["state"] = "decision_required"
            record.outcome = {
                **result,
                "disposition": "awaiting_decision",
                "decision": "attestation",
                "terminal_state": "decision_required",
                "terminal_error": None,
                "validation_evidence": evidence,
            }
            _transition(record, "run_needs_decision")
            record.events.append(
                {
                    "phase": "attestation_required",
                    "execution_id": record.execution_id,
                    "attempt_id": attempt_id,
                    "execution_epoch": execution_epoch,
                    "error": terminal_error,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        else:
            attempt["state"] = "completed"
            record.outcome = {
                **result,
                "disposition": "completed",
                "decision": None,
                "terminal_state": "completed",
                "validation_evidence": evidence,
            }
            _transition(record, "run_succeeded")
            record.events.append(
                {
                    "phase": "io_schema_validated" if observer_evidence is not None else "execution_completed",
                    "execution_id": record.execution_id,
                    "attempt_id": attempt_id,
                    "execution_epoch": execution_epoch,
                    "resource_ref": result.get("resource_ref"),
                    "evidence_id": observer_evidence["evidence_id"] if observer_evidence else None,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        await self._persist(record)
        return record

    async def close_run(self, run_id: str) -> RunRecord:
        record = await self._load(run_id)
        if record.state not in {"completed", "failed", "cancelled"}:
            raise ValueError("run_not_terminal")
        _transition(record, "run_closed")
        record.events.append({"phase": "run_closed", "execution_id": record.execution_id})
        await self._persist(record)
        return record

    async def resolve_run(self, run_id: str, decision: str) -> RunRecord:
        record = await self._load(run_id)
        decision = str(decision)
        if record.state == "completed" and decision == "accept":
            return record
        if record.state == "failed" and decision == "abandon":
            return record
        if record.state != "awaiting_decision" or not isinstance(record.outcome, dict):
            raise ValueError("illegal_state_transition")
        pending_decision = record.outcome.get("decision")
        outcome = deepcopy(record.outcome)
        if decision == "accept":
            if pending_decision != "attestation":
                raise ValueError("invalid_decision")
            _transition(record, "decision_accepted")
            outcome["disposition"] = "completed"
            outcome["decision"] = None
            outcome["terminal_error"] = None
        elif decision == "abandon":
            _transition(record, "decision_abandoned")
            outcome["disposition"] = "failed"
            outcome["decision"] = None
        else:
            raise ValueError("invalid_decision")
        record.outcome = outcome
        record.events.append(
            {
                "phase": "run_decision_resolved",
                "decision": decision,
                "from_state": "awaiting_decision",
                "to_state": record.state,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        await self._persist(record)
        return record

    async def set_slave_capabilities(self, slave_id: str, operations: set[str]) -> None:
        self.slave_capabilities.setdefault(slave_id, {})["operations"] = set(operations)

    def _orchestration_provenance(
        self,
        record: RunRecord,
        orchestration_package: CapabilityPackageVersion | None = None,
    ) -> dict[str, Any]:
        snapshot = record.committed.snapshot if record.committed is not None else record.draft.snapshot
        package_ref = snapshot.program_systems.package_ref
        package = orchestration_package
        if package is None and package_ref is not None:
            package = self._find_package(package_ref, run_id=record.run_id)
        return {
            "parent_execution_ref": record.execution_id,
            "closure_version_ref": record.committed.version_id if record.committed is not None else record.draft.version_id,
            "orchestration_package_ref": package_ref.model_dump(mode="json") if package_ref is not None else None,
            "orchestration_package_digest": package.package_digest if package is not None else None,
            "orchestration_program_ref": package.program_content_ref.model_dump(mode="json") if package is not None else None,
            "orchestration_program_digest": package.program_digest if package is not None else None,
        }

    async def accept_node_intent(
        self,
        run_id: str,
        intent: NodeIntent,
        *,
        selected_target: str,
    ) -> DynamicNode:
        lock = self._dynamic_intent_locks.setdefault(run_id, asyncio.Lock())
        async with lock:
            return await self._accept_node_intent(run_id, intent, selected_target=selected_target)

    async def _accept_node_intent(
        self,
        run_id: str,
        intent: NodeIntent,
        *,
        selected_target: str,
    ) -> DynamicNode:
        """Deterministically materialize a program-emitted NodeIntent.

        This method never invokes an LLM and never creates new operation or
        schema semantics.  It only validates the already-authorized package,
        content refs, limits, and target, then records a DynamicNode.
        """

        if not isinstance(intent, NodeIntent):
            intent = NodeIntent.model_validate(intent)
        record = await self._load(run_id)
        if record.state != "running" or record.execution_id is None:
            raise ValueError("execution_not_running")
        if intent.execution_id != record.execution_id:
            raise ValueError("node_intent_execution_mismatch")

        snapshot = record.committed.snapshot if record.committed is not None else record.draft.snapshot
        orchestration_ref = snapshot.program_systems.package_ref
        if orchestration_ref is None or snapshot.program_systems.executor_kind != "orchestrator_python_v1":
            raise ValueError("orchestration_package_not_bound")
        orchestration_package = self._find_package(orchestration_ref, run_id=run_id)
        if orchestration_package is None:
            foreign = self._find_package(orchestration_ref)
            if foreign is not None:
                raise ValueError("capability_package_scope_mismatch")
            raise ValueError("orchestration_package_not_found")
        if orchestration_package.executor_kind != "orchestrator_python_v1":
            raise ValueError("orchestration_package_not_found")

        if not any(
            event.get("phase") == "node_requested" and event.get("intent_id") == intent.intent_id
            for event in record.events
        ):
            record.events.append(
                {
                    "phase": "node_requested",
                    "run_id": run_id,
                    "execution_id": record.execution_id,
                    "execution_epoch": record.execution_epoch,
                    "intent_id": intent.intent_id,
                    "intent": intent.model_dump(mode="json"),
                    "provenance": self._orchestration_provenance(record, orchestration_package),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            await self._persist(record)

        existing = next((node for node in record.dynamic_nodes if node.intent_id == intent.intent_id), None)
        if existing is not None:
            if existing.package_ref == intent.package_ref and existing.input_refs == intent.input_refs:
                return existing
            raise ValueError("node_intent_conflict")
        if len(record.dynamic_nodes) >= orchestration_package.max_nodes:
            raise ValueError("orchestration_node_limit_exceeded")
        live_nodes = [node for node in record.dynamic_nodes if node.state not in {"completed", "failed", "decision_required"}]
        if len(live_nodes) >= orchestration_package.max_live_nodes:
            raise ValueError("orchestration_live_node_limit_exceeded")

        def same_ref(left: ResourceRef, right: ResourceRef) -> bool:
            left_digest = (left.version_or_digest or "").lower()
            right_digest = (right.version_or_digest or "").lower()
            return left.resource_id == right.resource_id and left_digest == right_digest

        if not any(same_ref(intent.package_ref, allowed) for allowed in orchestration_package.allowed_node_package_refs):
            raise ValueError("node_package_not_allowed")
        try:
            node_package = await self.get_capability_package(intent.package_ref, run_id=run_id)
        except KeyError as exc:
            raise ValueError("node_package_not_found") from exc
        if (intent.package_ref.version_or_digest or "").lower() != node_package.package_digest.lower():
            raise ValueError("node_package_digest_mismatch")
        if not self._package_visible_to_run(node_package, record.run_id):
            raise ValueError("capability_package_scope_mismatch")
        if node_package.publication_state == "abandoned":
            raise ValueError("capability_package_abandoned")
        if node_package.executor_kind != "subprocess_json_v1" or node_package.executor_operation != "run_code":
            raise ValueError("node_package_invalid")

        workspace_id = record.closure_contract.workspace_id if record.closure_contract is not None else "workspace-default"
        await self.refresh_slaves(workspace_id)

        if node_package.io_contract_ref is None:
            raise ValueError("node_io_contract_unavailable")
        try:
            contract_payload = await self._load_json_content(node_package.io_contract_ref)
            contract = IoContract.model_validate(contract_payload)
        except (FileNotFoundError, TypeError, ValueError):
            raise ValueError("node_io_contract_unavailable")
        if contract.input_schema_ref is not None:
            try:
                schema = await self._load_json_content(contract.input_schema_ref)
                validate_schema(schema)
                values = [await self._load_json_content(input_ref) for input_ref in intent.input_refs]
                candidate_values = ([values[0]] if len(values) == 1 else [{"inputs": values}])
                errors: list[SchemaValidationError] = []
                for candidate in candidate_values:
                    errors = validate(schema, candidate)
                    if not errors:
                        break
                if errors:
                    raise ValueError(
                        "node_input_schema_mismatch:"
                        + json.dumps([item.model_dump(mode="json") for item in errors], sort_keys=True, ensure_ascii=False)
                    )
            except (FileNotFoundError, TypeError, ValueError) as exc:
                if str(exc).startswith("node_input_schema_mismatch"):
                    raise
                raise ValueError("node_input_schema_mismatch") from exc

        authorized_digests: set[str] = set()
        for binding in snapshot.node_input_bindings:
            try:
                self._require_content_ref(binding.input_ref)
                authorized_digests.add((binding.input_ref.version_or_digest or "").lower())
                root_value = await self._load_json_content(binding.input_ref)
            except (FileNotFoundError, TypeError, ValueError):
                continue
            self._collect_content_ref_digests(root_value, authorized_digests)
        for attempt in record.attempts:
            if attempt.get("state") != "completed" or not attempt.get("result_ref"):
                continue
            try:
                result_ref = ResourceRef.model_validate(attempt["result_ref"])
                self._require_content_ref(result_ref)
                authorized_digests.add((result_ref.version_or_digest or "").lower())
            except (TypeError, ValueError):
                continue
        for input_ref in intent.input_refs:
            try:
                self._require_content_ref(input_ref)
                await self._load_json_content(input_ref)
            except (FileNotFoundError, TypeError, ValueError) as exc:
                raise ValueError("node_input_unavailable") from exc
            if (input_ref.version_or_digest or "").lower() not in authorized_digests:
                raise ValueError("node_input_not_allowed")

        if not isinstance(selected_target, str) or not selected_target:
            raise ValueError("node_target_unavailable")
        target = self._canonical_target_resource_id(selected_target)
        if not self._slave_supports_package(target, node_package):
            raise ValueError("node_target_unavailable")
        if record.closure_contract is not None and node_package.permissions:
            if not set(node_package.permissions).issubset(set(record.closure_contract.allowed_effects)):
                raise ValueError("node_permission_denied")
        locality = next(
            (
                requirement.value
                for requirement in snapshot.compute.requirements
                if requirement.key == "loom.data.locality.v1"
            ),
            None,
        )
        if locality is not None and self._canonical_target_resource_id(str(locality)) != target:
            raise ValueError("node_target_unavailable")

        if len(record.dynamic_nodes) >= orchestration_package.max_nodes:
            raise ValueError("orchestration_node_limit_exceeded")
        live_nodes = [node for node in record.dynamic_nodes if node.state not in {"completed", "failed", "decision_required"}]
        if len(live_nodes) >= orchestration_package.max_live_nodes:
            raise ValueError("orchestration_live_node_limit_exceeded")

        node_id = f"node-{record.execution_id}-{len(record.dynamic_nodes) + 1}"
        node = DynamicNode(
            node_id=node_id,
            parent_execution_ref=record.execution_id,
            intent_id=intent.intent_id,
            package_ref=intent.package_ref,
            package_digest=node_package.package_digest,
            input_refs=intent.input_refs,
            state="accepted",
        )
        record.dynamic_nodes.append(node)
        provenance = self._orchestration_provenance(record, orchestration_package)
        record.events.extend(
            [
                {
                    "phase": "node_accepted",
                    "run_id": run_id,
                    "execution_id": record.execution_id,
                    "execution_epoch": record.execution_epoch,
                    "node_id": node.node_id,
                    "intent_id": intent.intent_id,
                    "package_ref": intent.package_ref.model_dump(mode="json"),
                    "package_digest": node.package_digest,
                    "input_refs": [item.model_dump(mode="json") for item in intent.input_refs],
                    "selected_target": target,
                    "reason": "capability_match",
                    "provenance": {
                        **provenance,
                        "node_package_ref": node.package_ref.model_dump(mode="json"),
                        "node_package_digest": node.package_digest,
                    },
                    "created_at": datetime.now(timezone.utc).isoformat(),
                },
            ]
        )
        await self._persist(record)
        return node

    async def dispatch_dynamic_node(self, run_id: str, node_id: str, *, target: str) -> dict[str, Any]:
        record = await self._load(run_id)
        if record.state != "running" or record.execution_id is None:
            raise ValueError("execution_not_running")
        node = next((item for item in record.dynamic_nodes if item.node_id == node_id), None)
        if node is None:
            raise ValueError("dynamic_node_not_found")
        if node.state != "accepted":
            raise ValueError("dynamic_node_not_dispatchable")
        if not isinstance(target, str) or not target:
            raise ValueError("node_target_unavailable")
        workspace_id = record.closure_contract.workspace_id if record.closure_contract is not None else "workspace-default"
        await self.refresh_slaves(workspace_id)
        normalized_target = self._canonical_target_resource_id(target)
        accepted_event = next(
            (
                event
                for event in reversed(record.events)
                if event.get("phase") in {"node_accepted", "node_reassigned"} and event.get("node_id") == node_id
            ),
            None,
        )
        accepted_target = accepted_event.get("selected_target") if accepted_event is not None else None
        if accepted_target is None and accepted_event is not None:
            accepted_target = accepted_event.get("target")
        if accepted_target is not None and accepted_target != normalized_target:
            raise ValueError("node_target_mismatch")
        node_package = self._find_package(node.package_ref, run_id=run_id)
        if node_package is None or not self._slave_supports_package(normalized_target, node_package):
            raise ValueError("node_target_unavailable")
        target_agent = self.slave_agents[normalized_target]
        self._check_attempt_budget(record)
        attempt = {
            "attempt_id": f"attempt-{uuid4().hex[:12]}",
            "node_id": node_id,
            "target": normalized_target,
            "target_instance_id": target_agent["instance_id"],
            "target_agent_epoch": int(target_agent["epoch"]),
            "state": "created",
            "execution_epoch": record.execution_epoch,
            "replaces_attempt_id": None,
            "replaced_by_attempt_id": None,
        }
        record.attempts.append(attempt)
        node.state = "dispatched"
        record.events.append(
            {
                "phase": "node_dispatched",
                "run_id": run_id,
                "execution_id": record.execution_id,
                "execution_epoch": record.execution_epoch,
                "node_id": node_id,
                "attempt_id": attempt["attempt_id"],
                "target": normalized_target,
                "target_instance_id": attempt["target_instance_id"],
                "target_agent_epoch": attempt["target_agent_epoch"],
                "package_ref": node.package_ref.model_dump(mode="json"),
                "package_digest": node.package_digest,
                "input_refs": [item.model_dump(mode="json") for item in node.input_refs],
                "provenance": {
                    **self._orchestration_provenance(record),
                    "node_package_ref": node.package_ref.model_dump(mode="json"),
                    "node_package_digest": node.package_digest,
                },
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        await self._persist(record)
        return deepcopy(attempt)

    async def reassign_dynamic_node(
        self,
        run_id: str,
        node_id: str,
        *,
        lost_attempt_id: str,
        expected_execution_id: str,
        expected_execution_epoch: int,
        target: str,
        reason: str,
    ) -> dict[str, Any]:
        lock = self._dynamic_intent_locks.setdefault(run_id, asyncio.Lock())
        async with lock:
            record = await self._load(run_id)
            if record.state != "running" or record.execution_id is None:
                raise ValueError("execution_not_running")
            if expected_execution_id != record.execution_id:
                raise ValueError("stale_execution_id")
            if int(expected_execution_epoch) != record.execution_epoch:
                raise ValueError("stale_execution_epoch")
            node = next((item for item in record.dynamic_nodes if item.node_id == node_id), None)
            if node is None:
                raise ValueError("dynamic_node_not_found")
            old_attempt = next(
                (
                    item
                    for item in record.attempts
                    if item.get("attempt_id") == lost_attempt_id and item.get("node_id") == node_id
                ),
                None,
            )
            if old_attempt is None:
                raise ValueError("stale_attempt")
            replacement_id = old_attempt.get("replaced_by_attempt_id")
            if replacement_id:
                replacement = next(
                    (item for item in record.attempts if item.get("attempt_id") == replacement_id),
                    None,
                )
                if replacement is None:
                    raise ValueError("stale_attempt")
                return deepcopy(replacement)
            if node.state != "dispatched":
                raise ValueError("dynamic_node_not_dispatchable")
            active_attempts = [
                item
                for item in record.attempts
                if item.get("node_id") == node_id and item.get("state") in {"created", "running"}
            ]
            if active_attempts != [old_attempt]:
                raise ValueError("stale_attempt")
            if old_attempt.get("execution_epoch") != record.execution_epoch:
                raise ValueError("stale_attempt")
            policy = record.closure_contract.recovery_policy if record.closure_contract is not None else {}
            if not bool(policy.get("allow_reassignment", False)):
                raise ValueError("reassignment_not_allowed")

            package = self._find_package(node.package_ref, run_id=run_id)
            if package is None:
                raise ValueError("node_target_unavailable")
            if record.closure_contract is not None and package.permissions:
                if not set(package.permissions).issubset(set(record.closure_contract.allowed_effects)):
                    raise ValueError("node_permission_denied")
            for input_ref in node.input_refs:
                try:
                    self._require_content_ref(input_ref)
                    await self._load_json_content(input_ref)
                except (FileNotFoundError, TypeError, ValueError) as exc:
                    raise ValueError("node_input_unavailable") from exc
            self._check_attempt_budget(record)

            workspace_id = record.closure_contract.workspace_id if record.closure_contract is not None else "workspace-default"
            await self.refresh_slaves(workspace_id)
            source_key = (
                str(old_attempt.get("target") or ""),
                str(old_attempt.get("target_instance_id") or ""),
            )
            source_agent = self.slave_instances.get(source_key)
            if source_agent is None or int(source_agent.get("epoch") or 0) != int(old_attempt.get("target_agent_epoch") or 0):
                raise ValueError("worker_lease_not_lost")
            if source_agent.get("lease_state") not in {"expired", "released"}:
                raise ValueError("worker_lease_active")
            normalized_target = self._canonical_target_resource_id(target)
            if (
                not normalized_target
                or normalized_target == old_attempt.get("target")
                or not self._slave_supports_package(normalized_target, package)
            ):
                raise ValueError("node_target_unavailable")
            snapshot = record.committed.snapshot if record.committed is not None else record.draft.snapshot
            locality = next(
                (
                    requirement.value
                    for requirement in snapshot.compute.requirements
                    if requirement.key == "loom.data.locality.v1"
                ),
                None,
            )
            if locality is not None and self._canonical_target_resource_id(str(locality)) != normalized_target:
                raise ValueError("node_target_unavailable")
            target_agent = self.slave_agents[normalized_target]
            updated = deepcopy(record)
            updated_old_attempt = next(
                item for item in updated.attempts if item.get("attempt_id") == lost_attempt_id
            )
            updated_node = next(item for item in updated.dynamic_nodes if item.node_id == node_id)
            replacement = {
                "attempt_id": f"attempt-{uuid4().hex[:12]}",
                "node_id": node_id,
                "target": normalized_target,
                "target_instance_id": target_agent["instance_id"],
                "target_agent_epoch": int(target_agent["epoch"]),
                "state": "created",
                "execution_epoch": record.execution_epoch,
                "replaces_attempt_id": old_attempt["attempt_id"],
                "replaced_by_attempt_id": None,
            }
            updated_old_attempt["state"] = "lost"
            updated_old_attempt["terminal_error"] = {
                "code": "worker_lost",
                "lease_state": source_agent["lease_state"],
            }
            updated_old_attempt["replaced_by_attempt_id"] = replacement["attempt_id"]
            updated.attempts.append(replacement)
            updated_node.state = "dispatched"
            contract = updated.closure_contract
            updated.events.append(
                {
                    "phase": "node_reassigned",
                    "run_id": run_id,
                    "execution_id": updated.execution_id,
                    "execution_epoch": updated.execution_epoch,
                    "node_id": node_id,
                    "lost_attempt_id": updated_old_attempt["attempt_id"],
                    "replacement_attempt_id": replacement["attempt_id"],
                    "from_target": updated_old_attempt["target"],
                    "from_instance_id": updated_old_attempt["target_instance_id"],
                    "from_agent_epoch": updated_old_attempt["target_agent_epoch"],
                    "to_target": replacement["target"],
                    "to_instance_id": replacement["target_instance_id"],
                    "to_agent_epoch": replacement["target_agent_epoch"],
                    "reason": reason,
                    "source_lease_state": source_agent["lease_state"],
                    "reassignment_authorization": {
                        "policy": "allow_reassignment",
                        "value": True,
                        "closure_version_ref": updated.committed.version_id if updated.committed is not None else updated.draft.version_id,
                        "user_id": contract.user_id if contract is not None else "user-default",
                        "origin_conversation_ref": contract.origin_conversation_ref if contract is not None else updated.task_ref,
                    },
                    "package_ref": updated_node.package_ref.model_dump(mode="json"),
                    "package_digest": updated_node.package_digest,
                    "package_replay_safety": package.replay_safety,
                    "input_refs": [item.model_dump(mode="json") for item in updated_node.input_refs],
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            await self._persist(updated)
            self.runs[run_id] = updated
            return deepcopy(replacement)

    async def record_dynamic_node_result(self, run_id: str, node_id: str, result: dict[str, Any]) -> DynamicNode:
        record = await self._load(run_id)
        if record.state != "running" or record.execution_id is None:
            raise ValueError("execution_not_running")
        if not isinstance(result, dict):
            raise ValueError("invalid_result")
        node = next((item for item in record.dynamic_nodes if item.node_id == node_id), None)
        if node is None:
            raise ValueError("dynamic_node_not_found")
        if result.get("execution_id") != record.execution_id:
            raise ValueError("stale_execution_id")
        try:
            execution_epoch = int(result.get("execution_epoch"))
        except (TypeError, ValueError) as exc:
            raise ValueError("stale_execution_epoch") from exc
        if execution_epoch != record.execution_epoch:
            raise ValueError("stale_execution_epoch")
        attempt_id = result.get("attempt_id")
        attempt = next(
            (
                item
                for item in reversed(record.attempts)
                if item.get("attempt_id") == attempt_id and item.get("node_id") == node_id
            ),
            None,
        )
        if attempt is None or attempt.get("execution_epoch", execution_epoch) != execution_epoch:
            raise ValueError("stale_attempt")
        if attempt.get("state") not in {"created", "running"}:
            raise ValueError("stale_attempt")
        if "value" not in result or "digest" not in result:
            raise ValueError("output_missing")
        value = result["value"]
        expected_digest = self._result_digest(value)
        if result.get("digest") != expected_digest:
            raise ValueError("output_digest_mismatch")
        reported_terminal_state = str(result.get("terminal_state") or "completed")
        if reported_terminal_state not in {"completed", "failed", "decision_required"}:
            raise ValueError("invalid_terminal_state")

        contract: IoContract | None = None
        output_schema_ref: ResourceRef | None = None
        raw_validation_evidence = result.get("validation_evidence")
        if raw_validation_evidence is None:
            validation_evidence = []
        elif not isinstance(raw_validation_evidence, list):
            raise ValueError("invalid_validation_evidence")
        else:
            validation_evidence = []
            for item in raw_validation_evidence:
                try:
                    parsed_evidence = ValidationEvidence.model_validate(item)
                except Exception as exc:
                    raise ValueError("invalid_validation_evidence") from exc
                if parsed_evidence.attempt_id != attempt_id or parsed_evidence.execution_epoch != execution_epoch:
                    raise ValueError("stale_validation_evidence")
                if parsed_evidence.issuer != "slave":
                    raise ValueError("invalid_validation_evidence")
                if parsed_evidence.output_digest is not None and parsed_evidence.output_digest != expected_digest:
                    raise ValueError("validation_evidence_digest_mismatch")
                validation_evidence.append(parsed_evidence.model_dump(mode="json"))
        try:
            node_package = await self.get_capability_package(node.package_ref, run_id=run_id)
        except KeyError as exc:
            raise ValueError("node_package_not_found") from exc
        if node_package.package_digest != node.package_digest:
            raise ValueError("node_package_digest_mismatch")
        if node_package.io_contract_ref is None:
            raise ValueError("node_io_contract_unavailable")
        try:
            contract = IoContract.model_validate(await self._load_json_content(node_package.io_contract_ref))
            output_schema_ref = contract.output_schema_ref
            def same_ref(left: ResourceRef | None, right: ResourceRef | None) -> bool:
                if left is None or right is None:
                    return left is right
                return (left.version_or_digest or left.resource_id) == (right.version_or_digest or right.resource_id)

            for item in validation_evidence:
                evidence_schema = ResourceRef.model_validate(item["schema_ref"]) if item.get("schema_ref") is not None else None
                evidence_validator = ResourceRef.model_validate(item["validator_ref"]) if item.get("validator_ref") is not None else None
                if not same_ref(evidence_schema, output_schema_ref):
                    raise ValueError("validation_evidence_schema_mismatch")
                if not same_ref(evidence_validator, contract.success_validator_ref):
                    raise ValueError("validation_evidence_validator_mismatch")
            schema_errors: list[SchemaValidationError] = []
            if output_schema_ref is not None:
                schema = await self._load_json_content(output_schema_ref)
                validate_schema(schema)
                schema_errors = validate(schema, value)
        except (FileNotFoundError, TypeError, ValueError) as exc:
            if str(exc).startswith("validation_evidence_"):
                raise
            schema_errors = [
                SchemaValidationError(
                    path="$",
                    keyword="schema",
                    message="node output schema could not be evaluated",
                    expected="valid output schema",
                    observed=str(exc),
                )
            ]

        if not schema_errors and contract is not None and contract.success_validator_ref is not None:
            validator_errors: list[dict[str, Any]] = []
            validator_status = "fail"
            try:
                validator_program = await self.content_store.get(contract.success_validator_ref)
                validator_input = value if isinstance(value, dict) else {"value": value}
                validator_result = await default_registry.execute(
                    "subprocess_json_v1",
                    "run_code",
                    validator_input,
                    program=validator_program,
                )
                validator_payload = validator_result.value if isinstance(validator_result.value, dict) else {}
                validator_status = str(validator_payload.get("result") or "fail")
                raw_errors = validator_payload.get("errors")
                if isinstance(raw_errors, list):
                    validator_errors = [item for item in raw_errors if isinstance(item, dict)]
            except Exception as exc:
                validator_errors = [{"path": "$", "keyword": "validator", "message": str(exc)[:512]}]
            validation_evidence.append(
                ValidationEvidence(
                    evidence_id=f"evidence-{attempt_id}-{uuid4().hex[:10]}",
                    attempt_id=attempt_id,
                    execution_epoch=execution_epoch,
                    validator_ref=contract.success_validator_ref,
                    schema_ref=output_schema_ref,
                    output_digest=expected_digest,
                    result="pass" if validator_status == "pass" and not validator_errors else "fail",
                    errors=validator_errors,
                    issuer="observer",
                    created_at=datetime.now(timezone.utc).isoformat(),
                ).model_dump(mode="json")
            )
            if validator_status != "pass" or validator_errors:
                schema_errors.append(
                    SchemaValidationError(
                        path="$",
                        keyword="validator",
                        message="node output validator failed",
                        expected="validator pass",
                        observed=json.dumps(validator_errors, ensure_ascii=False),
                    )
                )

        result_ref = await self.content_store.put(canonical_json_bytes(value), media_type="application/json")
        if schema_errors:
            terminal_state = "failed"
            terminal_error = {
                "code": "success_validation_failed" if any(item.keyword == "validator" for item in schema_errors) else "output_schema_mismatch",
                "errors": [item.model_dump(mode="json") for item in schema_errors],
            }
        else:
            terminal_state = reported_terminal_state
            terminal_error = result.get("terminal_error") if isinstance(result.get("terminal_error"), dict) else None
        attempt["state"] = terminal_state
        attempt["result_ref"] = result_ref.model_dump(mode="json")
        attempt["value"] = value
        attempt["digest"] = expected_digest
        attempt["terminal_error"] = terminal_error
        attempt["validation_evidence"] = validation_evidence
        node.state = terminal_state
        if terminal_state in {"failed", "decision_required"}:
            decision = "attestation" if terminal_state == "decision_required" else "repair"
            record.outcome = {
                "disposition": "awaiting_decision",
                "decision": decision,
                "terminal_state": terminal_state,
                "terminal_error": terminal_error,
                "resource_ref": result_ref.model_dump(mode="json") if decision == "attestation" else None,
                "value": value,
                "digest": expected_digest,
                "node_id": node_id,
                "attempt_id": attempt_id,
                "execution_id": record.execution_id,
                "execution_epoch": record.execution_epoch,
            }
            _transition(record, "run_needs_decision", reason=terminal_error)
        record.events.append(
            {
                "phase": "node_completed",
                "run_id": run_id,
                "execution_id": record.execution_id,
                "node_id": node_id,
                "attempt_id": attempt_id,
                "target": attempt.get("target"),
                "execution_epoch": execution_epoch,
                "package_ref": node.package_ref.model_dump(mode="json"),
                "package_digest": node.package_digest,
                "input_refs": [item.model_dump(mode="json") for item in node.input_refs],
                "result_ref": result_ref.model_dump(mode="json"),
                "digest": expected_digest,
                "terminal_state": terminal_state,
                "terminal_error": terminal_error,
                "validation_evidence": validation_evidence,
                "provenance": {
                    **self._orchestration_provenance(record),
                    "node_package_ref": node.package_ref.model_dump(mode="json"),
                    "node_package_digest": node.package_digest,
                    "attempt_id": attempt_id,
                    "target": attempt.get("target"),
                    "slave_id": attempt.get("target"),
                },
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        await self._persist(record)
        return node

    async def fail_dynamic_node(
        self,
        run_id: str,
        node_id: str,
        *,
        attempt_id: str | None = None,
        error: dict[str, Any] | None = None,
    ) -> DynamicNode:
        record = await self._load(run_id)
        node = next((item for item in record.dynamic_nodes if item.node_id == node_id), None)
        if node is None:
            raise ValueError("dynamic_node_not_found")
        if error is not None and not isinstance(error, dict):
            raise ValueError("invalid_error")
        reason = error or {"code": "dynamic_node_failed"}
        if attempt_id is not None:
            requested_attempt = next(
                (item for item in record.attempts if item.get("attempt_id") == attempt_id and item.get("node_id") == node_id),
                None,
            )
            if (
                requested_attempt is None
                or requested_attempt.get("execution_epoch", record.execution_epoch) != record.execution_epoch
                or requested_attempt.get("state") not in {"created", "running"}
            ):
                raise ValueError("stale_attempt")
        node.state = "failed"
        failed_attempt = None
        for attempt in record.attempts:
            if attempt.get("node_id") == node_id and (attempt_id is None or attempt.get("attempt_id") == attempt_id):
                failed_attempt = attempt
                if attempt.get("state") in {"created", "running"}:
                    attempt["state"] = "failed"
                    attempt["terminal_error"] = reason
        _transition(record, "run_needs_decision", reason=reason)
        record.outcome = {
            "disposition": "awaiting_decision",
            "decision": "repair",
            "terminal_state": "failed",
            "terminal_error": reason,
            "resource_ref": None,
            "execution_id": record.execution_id,
            "execution_epoch": record.execution_epoch,
        }
        record.events.append(
            {
                "phase": "node_completed",
                "run_id": run_id,
                "execution_id": record.execution_id,
                "node_id": node_id,
                "attempt_id": attempt_id,
                "target": failed_attempt.get("target") if failed_attempt is not None else None,
                "execution_epoch": record.execution_epoch,
                "package_ref": node.package_ref.model_dump(mode="json"),
                "package_digest": node.package_digest,
                "input_refs": [item.model_dump(mode="json") for item in node.input_refs],
                "terminal_state": "failed",
                "terminal_error": reason,
                "provenance": {
                    **self._orchestration_provenance(record),
                    "node_package_ref": node.package_ref.model_dump(mode="json"),
                    "node_package_digest": node.package_digest,
                    "attempt_id": attempt_id,
                    "target": failed_attempt.get("target") if failed_attempt is not None else None,
                },
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        record.events.append(
            {
                "phase": "orchestration_failed",
                "run_id": run_id,
                "execution_id": record.execution_id,
                "execution_epoch": record.execution_epoch,
                "node_id": node_id,
                "error": reason,
                "provenance": self._orchestration_provenance(record),
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        await self._persist(record)
        return node

    async def complete_orchestration(self, run_id: str, final_ref: ResourceRef) -> RunRecord:
        record = await self._load(run_id)
        if record.state != "running" or record.execution_id is None:
            raise ValueError("execution_not_running")
        snapshot = record.committed.snapshot if record.committed is not None else record.draft.snapshot
        if snapshot.program_systems.executor_kind != "orchestrator_python_v1":
            raise ValueError("orchestration_not_running")
        if any(node.state in {"failed", "decision_required"} for node in record.dynamic_nodes):
            raise ValueError("dynamic_nodes_not_successful")
        if any(node.state != "completed" for node in record.dynamic_nodes):
            raise ValueError("dynamic_nodes_not_terminal")
        contract: IoContract | None = None
        value: Any = None
        final_loaded = False
        schema_errors: list[SchemaValidationError] = []
        if snapshot.program.io_contract_ref is None:
            raise ValueError("io_contract_unavailable")
        try:
            value = await self._load_json_content(final_ref)
            final_loaded = True
            contract = IoContract.model_validate(await self._load_json_content(snapshot.program.io_contract_ref))
            output_schema_ref = contract.output_schema_ref
            if output_schema_ref is not None:
                schema = await self._load_json_content(output_schema_ref)
                validate_schema(schema)
                schema_errors = validate(schema, value)
        except (FileNotFoundError, TypeError, ValueError) as exc:
            schema_errors = [
                SchemaValidationError(
                    path="$",
                    keyword="schema",
                    message="final output schema could not be evaluated",
                    expected="valid final output",
                    observed=str(exc),
                )
            ]
        digest = self._result_digest(value) if final_loaded else ""
        if final_loaded and (final_ref.version_or_digest or "").lower() != digest:
            schema_errors.append(
                SchemaValidationError(
                    path="$",
                    keyword="digest",
                    message="final ref digest mismatch",
                    expected=digest,
                    observed=final_ref.version_or_digest,
                )
            )
        orchestration_package = self._find_package(snapshot.program_systems.package_ref, run_id=run_id) if snapshot.program_systems.package_ref else None
        orchestration_provenance = self._orchestration_provenance(record, orchestration_package)
        lineage: list[dict[str, Any]] = []
        completed_digests: set[str] = set()
        for node in record.dynamic_nodes:
            attempt = next(
                (
                    item
                    for item in reversed(record.attempts)
                    if item.get("node_id") == node.node_id
                    and item.get("state") == "completed"
                    and item.get("result_ref")
                ),
                None,
            )
            if attempt is None:
                continue
            result_ref = ResourceRef.model_validate(attempt["result_ref"])
            completed_digests.add((result_ref.version_or_digest or "").lower())
            lineage.append(
                {
                    "node_id": node.node_id,
                    "intent_id": node.intent_id,
                    "package_ref": node.package_ref.model_dump(mode="json"),
                    "package_digest": node.package_digest,
                    "input_refs": [item.model_dump(mode="json") for item in node.input_refs],
                    "attempt_id": attempt.get("attempt_id"),
                    "target": attempt.get("target"),
                    "execution_epoch": attempt.get("execution_epoch", record.execution_epoch),
                    "result_ref": attempt["result_ref"],
                    "provenance": {
                        **orchestration_provenance,
                        "node_package_ref": node.package_ref.model_dump(mode="json"),
                        "node_package_digest": node.package_digest,
                        "attempt_id": attempt.get("attempt_id"),
                        "target": attempt.get("target"),
                        "slave_id": attempt.get("target"),
                    },
                }
            )
        if final_loaded and digest not in completed_digests:
            schema_errors.append(
                SchemaValidationError(
                    path="$",
                    keyword="lineage",
                    message="final output must reference a completed dynamic node",
                    expected="completed node result",
                    observed=final_ref.resource_id,
                )
            )
        if not schema_errors and contract is not None and contract.success_validator_ref is not None:
            validator_errors: list[dict[str, Any]] = []
            try:
                validator_program = await self.content_store.get(contract.success_validator_ref)
                validator_input = value if isinstance(value, dict) else {"value": value}
                validator_result = await default_registry.execute("subprocess_json_v1", "run_code", validator_input, program=validator_program)
                validator_payload = validator_result.value if isinstance(validator_result.value, dict) else {}
                if validator_payload.get("result") != "pass":
                    raw_errors = validator_payload.get("errors")
                    validator_errors = [item for item in raw_errors if isinstance(item, dict)] if isinstance(raw_errors, list) else []
            except Exception as exc:
                validator_errors = [{"path": "$", "keyword": "validator", "message": str(exc)[:512]}]
            if validator_errors:
                schema_errors.append(
                    SchemaValidationError(
                        path="$",
                        keyword="validator",
                        message="final output validator failed",
                        expected="validator pass",
                        observed=json.dumps(validator_errors, ensure_ascii=False),
                    )
                )
        requires_attestation = bool(
            not schema_errors
            and contract is not None
            and contract.success_validator_ref is None
            and contract.success_semantics is not None
        )
        if schema_errors:
            error_code = (
                "orchestration_lineage_mismatch"
                if any(item.keyword == "lineage" for item in schema_errors)
                else "output_schema_mismatch"
            )
            terminal_error = {"code": error_code, "errors": [item.model_dump(mode="json") for item in schema_errors]}
            _transition(record, "run_needs_decision", reason=terminal_error)
            record.outcome = {
                "disposition": "awaiting_decision",
                "decision": "repair",
                "terminal_state": "failed",
                "terminal_error": terminal_error,
                "resource_ref": None,
                "execution_id": record.execution_id,
                "execution_epoch": record.execution_epoch,
                "provenance": orchestration_provenance,
            }
            record.events.append(
                {
                    "phase": "orchestration_failed",
                    "run_id": run_id,
                    "execution_id": record.execution_id,
                    "execution_epoch": record.execution_epoch,
                    "terminal_state": "failed",
                    "error": record.outcome["terminal_error"],
                    "provenance": orchestration_provenance,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            await self._persist(record)
            return record
        if requires_attestation:
            _transition(record, "run_needs_decision")
            record.outcome = {
                "disposition": "awaiting_decision",
                "decision": "attestation",
                "terminal_state": "decision_required",
                "terminal_error": None,
                "resource_ref": final_ref.model_dump(mode="json"),
                "value": value,
                "digest": digest,
                "execution_id": record.execution_id,
                "execution_epoch": record.execution_epoch,
                "provenance": orchestration_provenance,
            }
            record.events.append(
                {
                    "phase": "orchestration_decision_required",
                    "run_id": run_id,
                    "execution_id": record.execution_id,
                    "execution_epoch": record.execution_epoch,
                    "terminal_state": "decision_required",
                    "resource_ref": final_ref.model_dump(mode="json"),
                    "provenance": orchestration_provenance,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            await self._persist(record)
            return record
        _transition(record, "run_succeeded")
        record.outcome = {
            "terminal_state": "completed",
            "disposition": "completed",
            "decision": None,
            "resource_ref": final_ref.model_dump(mode="json"),
            "value": value,
                "digest": digest,
                "execution_id": record.execution_id,
                "execution_epoch": record.execution_epoch,
                "lineage": lineage,
                "provenance": orchestration_provenance,
            }
        record.events.append(
            {
                "phase": "orchestration_completed",
                "run_id": run_id,
                "execution_id": record.execution_id,
                "execution_epoch": record.execution_epoch,
                "terminal_state": "completed",
                "resource_ref": final_ref.model_dump(mode="json"),
                "lineage": lineage,
                "provenance": orchestration_provenance,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        await self._persist(record)
        return record
