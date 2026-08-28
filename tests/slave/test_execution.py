import pytest

from loom_v2.contracts.types import ComputeSpec, TaskClosure, TypedHole
from loom_v2.slave.executor import SubprocessJSONV1Adapter, execute_operation
from loom_v2.slave.service import SlaveService


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


@pytest.mark.asyncio
async def test_slave_rejects_unbound_typed_hole_before_execution():
    service = SlaveService("slave-a")
    closure = TaskClosure(program={"operation_ref": "loom://sort"}, compute=ComputeSpec(operation_ref="loom://sort", typed_holes=[TypedHole(hole_id="h_sort")]))

    with pytest.raises(RuntimeError, match="typed_hole_unbound:h_sort"):
        await service.run("attempt-unbound", "sort", {"items": [2, 1]}, closure=closure)


@pytest.mark.asyncio
async def test_slave_rejects_unsupported_operation():
    service = SlaveService("slave-a", supported_operations={"echo"})

    with pytest.raises(RuntimeError, match="capability_unavailable:sort"):
        await service.run("attempt-unsupported", "sort", {"items": [2, 1]})


@pytest.mark.asyncio
async def test_subprocess_capability_timeout_is_independently_configurable(monkeypatch):
    monkeypatch.setenv("LOOM_CAPABILITY_OPERATION_TIMEOUT_SECONDS", "0.01")
    adapter = SubprocessJSONV1Adapter()
    program = b"import time; time.sleep(0.10); print('{}')"

    with pytest.raises(RuntimeError, match="capability_timeout"):
        await adapter.execute("run_code", {}, program=program)
