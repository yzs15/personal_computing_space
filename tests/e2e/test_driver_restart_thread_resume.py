import pytest

from loom_v2.coding_agents.base import AgentEvent
from loom_v2.driver.service import DriverService


class Control:
    workspace_id = "workspace-default"
    driver_epoch = 2

    def __init__(self):
        self.states = []

    async def thread_binding(self, _conversation_ref):
        return {"thread_id": "thread-7", "turn_state": "idle"}

    async def thread_bind(self, _binding):
        return {}

    async def turn_state(self, _conversation_ref, state, **kwargs):
        self.states.append(state)
        return {}

    async def command(self, *_args, **_kwargs):
        return {}


class Provider:
    model = "deepseek-v4-flash"

    def __init__(self):
        self.started_with_thread_id = None

    async def start(self, _conversation_ref, _workspace_root, existing_thread_id=None):
        self.started_with_thread_id = existing_thread_id
        return existing_thread_id or "thread-new"

    async def send_turn(self, _message):
        if False:
            yield AgentEvent("noop")

    async def interrupt(self, _turn_ref=None):
        return None

    async def close(self):
        return None


@pytest.mark.asyncio
async def test_driver_restart_uses_observer_thread_binding():
    provider = Provider()
    control = Control()
    service = DriverService(control, provider)
    result = await service.run_prompt("conversation-1", "continue", request_id="req-2")
    assert provider.started_with_thread_id == "thread-7"
    assert result["thread_id"] == "thread-7"
    assert control.states == ["starting", "completed"]


@pytest.mark.asyncio
async def test_remote_start_failure_closes_provider_and_clears_active_turn():
    class FailingProvider(Provider):
        def __init__(self):
            super().__init__()
            self.closed = False

        async def start(self, _conversation_ref, _workspace_root, existing_thread_id=None):
            raise RuntimeError("codex_thread_unavailable")

        async def close(self):
            self.closed = True

    provider = FailingProvider()
    control = Control()
    service = DriverService(control, provider)
    with pytest.raises(RuntimeError, match="codex_thread_unavailable"):
        await service.run_prompt("conversation-1", "continue", request_id="req-fail")
    assert provider.closed is True
    assert "conversation-1" not in service.active_turns


@pytest.mark.asyncio
async def test_replayed_request_id_after_restart_does_not_start_second_turn():
    class CountingProvider(Provider):
        def __init__(self):
            super().__init__()
            self.turns = 0

        async def send_turn(self, _message):
            self.turns += 1
            if False:
                yield AgentEvent("noop")

    class PersistedControl(Control):
        def __init__(self):
            super().__init__()
            self.binding = {"thread_id": "thread-7", "turn_state": "completed", "active_request_id": "req-1"}

        async def thread_binding(self, _conversation_ref):
            return self.binding

    control = PersistedControl()
    provider = CountingProvider()
    service = DriverService(control, provider)
    result = await service.run_prompt("conversation-1", "retry", request_id="req-1")
    assert result["status"] == "completed"
    assert result["thread_id"] == "thread-7"
    assert provider.turns == 0
