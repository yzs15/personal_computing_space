import asyncio

import pytest

from loom_v2.coding_agents.base import AgentEvent
from loom_v2.driver.service import DriverService
from loom_v2.observer.repository import ObserverRepository


class BlockingProvider:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.released = asyncio.Event()
        self.interrupt_calls: list[str | None] = []

    async def start(self, conversation_ref: str, workspace_root: str) -> str:
        return f"thread:{conversation_ref}"

    async def send_turn(self, user_message: str):
        yield AgentEvent(
            "open_run",
            {
                "closure_contract": {
                    "closure_id": "closure-interrupt",
                    "goal": user_message,
                    "body": {"closure_id": "closure-interrupt"},
                }
            },
        )
        yield AgentEvent("turn_started", {"turn_id": "turn-blocking"})
        self.started.set()
        await self.released.wait()
        yield AgentEvent("turn_interrupted", {"turn_id": "turn-blocking"})

    async def interrupt(self, turn_ref: str | None = None) -> None:
        self.interrupt_calls.append(turn_ref)
        self.released.set()

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_interrupt_cancels_turn_without_execution():
    repo = ObserverRepository()
    provider = BlockingProvider()
    execution_calls = 0

    async def executor(*_args, **_kwargs):
        nonlocal execution_calls
        execution_calls += 1
        raise AssertionError("execution must not start after interrupt")

    driver = DriverService(repo, provider, executor=executor)
    task = asyncio.create_task(driver.run_prompt("conversation-interrupt", "long task"))
    await asyncio.wait_for(provider.started.wait(), timeout=1)

    requested = await driver.interrupt("conversation-interrupt")
    result = await task

    assert requested == {
        "conversation_ref": "conversation-interrupt",
        "run_id": result["run_id"],
        "status": "interrupt_requested",
    }
    assert result["state"] == "cancelled"
    assert result["status"] == "interrupted"
    assert provider.interrupt_calls == ["turn-blocking"]
    assert execution_calls == 0
    conversation = await repo.get_conversation("conversation-interrupt")
    assert conversation["status"] == "interrupted"
    assert conversation["runs"][0]["state"] == "cancelled"
