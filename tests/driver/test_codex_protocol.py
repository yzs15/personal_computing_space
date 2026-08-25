import pytest

from loom_v2.coding_agents.codex import CodexAppServerProvider


def test_codex_provider_defaults_to_requested_model():
    assert CodexAppServerProvider().model == "deepseek-v4-flash"


@pytest.mark.asyncio
async def test_codex_agent_message_is_normalized():
    provider = CodexAppServerProvider()
    provider.process = object()
    messages = iter(
        [
            {"method": "item/completed", "params": {"item": {"type": "agentMessage", "text": "答案"}}},
            {"method": "turn/completed", "params": {}},
        ]
    )

    async def fake_send(_message):
        return None

    async def fake_read_message():
        return next(messages)

    provider._send = fake_send
    provider._read_message = fake_read_message
    provider.thread_id = "thread-1"

    events = [event async for event in provider.send_turn("问题")]

    assert [(event.kind, event.payload) for event in events] == [("assistant_text", {"text": "答案"})]
