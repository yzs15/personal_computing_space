from __future__ import annotations

import asyncio
from typing import Any

from loom_v2.contracts.messages import MessageReceipt


class ObserverMessageDispatcher:
    """Single recoverable delivery loop for Observer message receipts."""

    def __init__(self, repository: Any, gateway: Any, *, workspace_id: str, forward_timeout: float = 86400.0, scan_interval: float = 0.5) -> None:
        self.repository = repository
        self.gateway = gateway
        self.workspace_id = workspace_id
        self.forward_timeout = forward_timeout
        self.scan_interval = max(0.01, scan_interval)
        self._task: asyncio.Task[None] | None = None
        self._wake_event = asyncio.Event()
        self._stop_event = asyncio.Event()

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self.running:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="observer-message-dispatcher")
        self._wake_event.set()

    async def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    def wake(self) -> None:
        self._wake_event.set()

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            delivered = False
            try:
                receipts = await self.repository.list_dispatchable_message_receipts(self.workspace_id)
                if receipts:
                    delivered = True
                    await self._deliver(receipts[0])
            except asyncio.CancelledError:
                raise
            except Exception:
                # Individual delivery errors are recorded by _deliver.  A
                # repository failure should not kill the sole dispatcher; the
                # next scan gives it a chance to recover after a transient DB
                # outage.
                await asyncio.sleep(min(2.0, self.scan_interval * 2))
            if delivered:
                continue
            self._wake_event.clear()
            try:
                await asyncio.wait_for(self._wake_event.wait(), timeout=self.scan_interval)
            except asyncio.TimeoutError:
                pass

    async def _deliver(self, receipt: MessageReceipt) -> None:
        queued = await self.repository.queue_message_receipt(receipt.workspace_id, receipt.request_id)
        payload = {
            "workspace_id": queued.workspace_id,
            "request_id": queued.request_id,
            "conversation_ref": queued.conversation_ref,
            "text": queued.prompt,
            "payload_digest": queued.payload_digest,
        }
        try:
            await self.gateway.forward(
                "/driver/v1/messages",
                payload,
                timeout=self.forward_timeout,
            )
            current = await self.repository.get_message_receipt(queued.workspace_id, queued.request_id)
            if current is not None and current.state == "queued":
                await self.repository.retry_message_receipt(
                    queued.workspace_id,
                    queued.request_id,
                    reason={"code": "driver_did_not_claim"},
                )
        except Exception as exc:
            current = await self.repository.get_message_receipt(queued.workspace_id, queued.request_id)
            if current is not None and current.state != "in_flight":
                await self.repository.retry_message_receipt(
                    queued.workspace_id,
                    queued.request_id,
                    reason={"code": "driver_unavailable", "message": str(exc)[:512]},
                )
