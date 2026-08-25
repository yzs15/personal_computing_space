import pytest

from loom_v2.slave.executor import execute_operation


@pytest.mark.asyncio
async def test_echo_execution_returns_resource_ref():
    result = await execute_operation("echo", {"text": "hello"})
    assert result.resource_ref.resource_id
    assert result.value == {"text": "hello"}


@pytest.mark.asyncio
async def test_hash_execution_is_replay_safe():
    first = await execute_operation("hash", {"text": "hello"})
    second = await execute_operation("hash", {"text": "hello"})
    assert first.value == second.value
    assert first.replay_safety == "Idempotent"
