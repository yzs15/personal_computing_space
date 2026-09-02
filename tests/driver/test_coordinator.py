import asyncio

import pytest

from loom_v2.coding_agents.turn import TurnContext
from loom_v2.driver.coordinator import DriverTurnCoordinator


class RecordingProvider:
    def __init__(self):
        self.contexts = []

    async def begin_turn(self, conversation_ref, request_id, workspace_root, existing_thread_id, tools, handler):
        context = TurnContext("workspace-default", conversation_ref, request_id, "claim", 1, "owner")
        self.contexts.append(context)
        return context

    async def end_turn(self, context):
        return None

    async def force_shutdown(self):
        return None


@pytest.mark.asyncio
async def test_coordinator_fifo_and_global_lane():
    provider = RecordingProvider()
    active = 0
    max_active = 0

    async def execute(context, prompt):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return {"request_id": context.request_id, "prompt": prompt}

    coordinator = DriverTurnCoordinator(provider, turn_executor=execute)
    receipts = [
        {"request_id": "a1", "conversation_ref": "a", "prompt": "one", "payload_digest": "d1"},
        {"request_id": "a2", "conversation_ref": "a", "prompt": "two", "payload_digest": "d2"},
        {"request_id": "b1", "conversation_ref": "b", "prompt": "three", "payload_digest": "d3"},
    ]
    results = await asyncio.gather(*(coordinator.submit(item) for item in receipts))
    assert [context.request_id for context in provider.contexts] == ["a1", "a2", "b1"] or [context.request_id for context in provider.contexts] == ["a1", "b1", "a2"]
    assert max_active == 1
    assert [result["prompt"] for result in results] == ["one", "two", "three"]
    await coordinator.shutdown()


@pytest.mark.asyncio
async def test_duplicate_request_id_uses_one_context():
    provider = RecordingProvider()

    async def execute(context, prompt):
        await asyncio.sleep(0.01)
        return {"request_id": context.request_id}

    coordinator = DriverTurnCoordinator(provider, turn_executor=execute)
    receipt = {"request_id": "same", "conversation_ref": "a", "prompt": "one", "payload_digest": "d"}
    first, second = await asyncio.gather(coordinator.submit(receipt), coordinator.submit(receipt))
    assert first == second
    assert len(provider.contexts) == 1
    await coordinator.shutdown()

