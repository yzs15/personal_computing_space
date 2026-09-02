import asyncio

import pytest

from loom_v2.observer.dispatcher import ObserverMessageDispatcher
from loom_v2.observer.repository import ObserverRepository


class Gateway:
    def __init__(self, fail=False):
        self.fail = fail
        self.payloads = []

    async def forward(self, path, payload, *, timeout=None):
        self.payloads.append((path, payload, timeout))
        if self.fail:
            raise RuntimeError("driver_unavailable")
        return {"accepted": True}


@pytest.mark.asyncio
async def test_dispatcher_marks_transport_failure_retryable():
    repo = ObserverRepository()
    receipt = await repo.create_or_get_message_receipt("workspace-default", "req", "conversation", "hello")
    gateway = Gateway(fail=True)
    dispatcher = ObserverMessageDispatcher(repo, gateway, workspace_id="workspace-default", scan_interval=0.01)
    await dispatcher.start()
    await asyncio.sleep(0.05)
    await dispatcher.stop()
    current = await repo.get_message_receipt("workspace-default", receipt.request_id)
    assert current is not None and current.state == "retryable"
    assert gateway.payloads and gateway.payloads[0][2] == 86400.0


@pytest.mark.asyncio
async def test_dispatcher_recreated_rescans_queued_receipt():
    repo = ObserverRepository()
    receipt = await repo.create_or_get_message_receipt("workspace-default", "req2", "conversation", "hello")
    await repo.queue_message_receipt("workspace-default", receipt.request_id)
    gateway2 = Gateway()
    dispatcher2 = ObserverMessageDispatcher(repo, gateway2, workspace_id="workspace-default", scan_interval=0.01)
    await dispatcher2.start()
    await asyncio.sleep(0.03)
    await dispatcher2.stop()
    assert gateway2.payloads
