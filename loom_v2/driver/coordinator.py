from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Awaitable, Callable
from uuid import uuid4

from loom_v2.coding_agents.turn import TurnContext


@dataclass
class _PendingTurn:
    request_id: str
    conversation_ref: str
    prompt: str
    payload_digest: str
    claim_token: str
    future: asyncio.Future[Any]
    cancelled: bool = False


class DriverTurnCoordinator:
    """Serializes turns per conversation and across the shared Codex lane."""

    def __init__(
        self,
        provider: Any,
        *,
        workspace_id: str = "workspace-default",
        driver_epoch: int = 0,
        control_client: Any | None = None,
        turn_executor: Callable[[TurnContext, str], Awaitable[Any]] | None = None,
        manage_provider_context: bool = True,
    ) -> None:
        self.provider = provider
        self.workspace_id = workspace_id
        self.driver_epoch = driver_epoch
        self.control_client = control_client
        self.turn_executor = turn_executor
        self.manage_provider_context = manage_provider_context
        self.owner_generation = uuid4().hex
        self._lane = asyncio.Lock()
        self._queues: dict[str, deque[_PendingTurn]] = defaultdict(deque)
        self._workers: dict[str, asyncio.Task[None]] = {}
        self._active: dict[str, TurnContext] = {}
        self._waiting: dict[str, _PendingTurn] = {}
        self._pending_by_request: dict[str, asyncio.Future[Any]] = {}
        self._submit_locks: dict[str, asyncio.Lock] = {}
        self._completed: dict[str, Any] = {}
        self._shutdown = False

    @property
    def active_contexts(self) -> dict[str, TurnContext]:
        return dict(self._active)

    async def submit(self, receipt: Any, prompt: str | None = None) -> Any:
        request_id = str(self._value(receipt, "request_id"))
        conversation_ref = str(self._value(receipt, "conversation_ref"))
        prompt = str(prompt if prompt is not None else self._value(receipt, "prompt", self._value(receipt, "text", "")))
        payload_digest = str(self._value(receipt, "payload_digest", ""))
        submit_lock = self._submit_locks.setdefault(request_id, asyncio.Lock())
        async with submit_lock:
            completed = self._completed.get(request_id)
            if completed is not None:
                return completed
            existing = self._pending_by_request.get(request_id)
            if existing is None:
                claim_token = uuid4().hex
                if self.control_client is not None:
                    claim = await self.control_client.claim_message(
                        request_id,
                        payload_digest,
                        conversation_ref=conversation_ref,
                        claim_token=claim_token,
                    )
                    claim_state = str(claim.get("state") or "")
                    if claim_state in {"completed", "failed", "interrupted"}:
                        self._completed[request_id] = claim
                        return claim
                    if claim_state == "in_flight" and claim.get("claim_token") != claim_token:
                        # Another Driver epoch owns the claim. Do not cache
                        # this in-flight state; a later retry can consult
                        # Observer after the owner is fenced or completes.
                        return claim
                    claim_token = str(claim.get("claim_token") or claim_token)
                loop = asyncio.get_running_loop()
                future: asyncio.Future[Any] = loop.create_future()
                pending = _PendingTurn(request_id, conversation_ref, prompt, payload_digest, claim_token, future)
                self._pending_by_request[request_id] = future
                self._queues[conversation_ref].append(pending)
                worker = self._workers.get(conversation_ref)
                if worker is None or worker.done():
                    self._workers[conversation_ref] = asyncio.create_task(self._conversation_worker(conversation_ref))
            else:
                future = existing
        return await asyncio.shield(future)

    async def interrupt(self, conversation_ref: str) -> dict[str, Any]:
        context = self._active.get(conversation_ref)
        if context is not None:
            if int(context.driver_epoch) != int(self.driver_epoch):
                raise RuntimeError("stale_driver_epoch")
            context.interrupt_requested = True
            interrupt = getattr(self.provider, "interrupt", None)
            if interrupt is not None:
                await interrupt(context)
            return {"conversation_ref": conversation_ref, "request_id": context.request_id, "status": "interrupt_requested"}
        queue = self._queues.get(conversation_ref)
        if queue:
            pending = queue.popleft()
            return await self._interrupt_pending(conversation_ref, pending)
        waiting = self._waiting.get(conversation_ref)
        if waiting is not None:
            waiting.cancelled = True
            self._waiting.pop(conversation_ref, None)
            return await self._interrupt_pending(conversation_ref, waiting)
        raise ValueError("conversation_not_active")

    async def shutdown(self) -> None:
        self._shutdown = True
        workers = list(self._workers.values())
        for worker in workers:
            worker.cancel()
        for worker in workers:
            try:
                await worker
            except asyncio.CancelledError:
                pass
        self._workers.clear()
        force_shutdown = getattr(self.provider, "force_shutdown", None)
        if force_shutdown is not None:
            await force_shutdown()

    async def _conversation_worker(self, conversation_ref: str) -> None:
        try:
            while not self._shutdown:
                queue = self._queues.get(conversation_ref)
                if not queue:
                    return
                pending = queue.popleft()
                self._waiting[conversation_ref] = pending
                async with self._lane:
                    context: TurnContext | None = None
                    try:
                        if pending.cancelled:
                            continue
                        context = await self._begin_context(pending)
                        self._waiting.pop(conversation_ref, None)
                        self._active[conversation_ref] = context
                        result = await self._execute(context, pending.prompt)
                        self._completed[pending.request_id] = result
                        if not pending.future.done():
                            pending.future.set_result(result)
                    except asyncio.CancelledError:
                        if not pending.future.done():
                            pending.future.cancel()
                        raise
                    except Exception as exc:
                        if not pending.future.done():
                            pending.future.set_exception(exc)
                    finally:
                        if context is not None:
                            self._active.pop(conversation_ref, None)
                            end_turn = getattr(self.provider, "end_turn", None)
                            if end_turn is not None:
                                try:
                                    await end_turn(context)
                                except Exception:
                                    pass
                        self._pending_by_request.pop(pending.request_id, None)
                        if self._waiting.get(conversation_ref) is pending:
                            self._waiting.pop(conversation_ref, None)
        finally:
            self._workers.pop(conversation_ref, None)

    async def _interrupt_pending(self, conversation_ref: str, pending: _PendingTurn) -> dict[str, Any]:
        result = {"conversation_ref": conversation_ref, "request_id": pending.request_id, "status": "interrupted"}
        if not pending.future.done():
            pending.future.set_result(result)
        self._pending_by_request.pop(pending.request_id, None)
        if self.control_client is not None:
            try:
                await self.control_client.update_message(
                    pending.request_id,
                    claim_token=pending.claim_token,
                    state="interrupted",
                    outcome={"error": {"code": "user_interrupt"}},
                )
            except Exception:
                # A newer Driver epoch may have fenced this queued claim;
                # Observer remains authoritative for its final state.
                pass
        return result

    async def _begin_context(self, pending: _PendingTurn) -> TurnContext:
        begin_turn = getattr(self.provider, "begin_turn", None) if self.manage_provider_context else None
        if begin_turn is None:
            return TurnContext(
                workspace_id=self.workspace_id,
                conversation_ref=pending.conversation_ref,
                request_id=pending.request_id,
                claim_token=pending.claim_token,
                driver_epoch=self.driver_epoch,
                owner_generation=self.owner_generation,
            )
        context = await begin_turn(
            pending.conversation_ref,
            pending.request_id,
            "/workspace",
            None,
            [],
            None,
        )
        context.claim_token = pending.claim_token
        context.driver_epoch = self.driver_epoch
        context.owner_generation = self.owner_generation
        return context

    async def _execute(self, context: TurnContext, prompt: str) -> Any:
        if self.turn_executor is not None:
            return await self.turn_executor(context, prompt)
        send_turn = getattr(self.provider, "send_turn", None)
        if send_turn is None:
            return {"request_id": context.request_id, "status": "completed"}
        assistant_parts: list[str] = []
        async for event in send_turn(context, prompt):
            if getattr(event, "kind", "") == "assistant_text":
                assistant_parts.append(str(event.payload.get("text", "")))
        return {"request_id": context.request_id, "conversation_ref": context.conversation_ref, "status": "completed", "assistant_text": "\n\n".join(assistant_parts)}

    @staticmethod
    def _value(value: Any, key: str, default: Any = "") -> Any:
        if isinstance(value, dict):
            return value.get(key, default)
        return getattr(value, key, default)
