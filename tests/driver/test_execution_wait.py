"""Local and remote execution waiters share ownership/cancellation semantics."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from loom_v2.coding_agents.fake import FakeCodingAgentProvider
from loom_v2.driver.mcp import DriverMCP
from loom_v2.driver.service import DriverService
from loom_v2.observer.repository import ObserverRepository


@pytest.fixture(params=["local", "remote"])
def execution_lane(request, monkeypatch):
    state = {"run_id": "run", "state": "running", "execution_id": "exec", "execution_epoch": 1}
    repo = ObserverRepository()
    driver = DriverService(repo, FakeCodingAgentProvider())
    failure = AsyncMock()
    repo.get_run = AsyncMock(side_effect=lambda _: SimpleNamespace(**state))
    repo.fail_run = failure
    monkeypatch.setattr(DriverMCP, "run_view", staticmethod(lambda record: dict(vars(record))))

    async def command(name, arguments, **kwargs):
        if name == "run.get":
            return dict(state)
        assert name == "run.fail"
        await failure(arguments["run_id"], arguments["reason"])

    driver.control_client = SimpleNamespace(command=command, driver_epoch=1)
    entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def dispatch(*args, **kwargs):
        entered.set()
        try:
            await release.wait()
            state["state"] = "completed"
            return (SimpleNamespace(**state), None) if request.param == "local" else None
        except asyncio.CancelledError:
            cancelled.set()
            raise

    dispatch_mock = AsyncMock(side_effect=dispatch)
    monkeypatch.setattr(driver, "_dispatch_execution" if request.param == "local" else "_dispatch_remote_execution", dispatch_mock)
    wait = driver._execute_and_wait_local if request.param == "local" else driver._execute_and_wait_remote
    return driver, state, wait, dispatch_mock, failure, entered, release, cancelled


@pytest.mark.asyncio
async def test_concurrent_waiters_share_one_dispatch(execution_lane):
    driver, state, wait, dispatch, failure, entered, release, _ = execution_lane
    first = asyncio.create_task(wait("run", "prompt"))
    await asyncio.wait_for(entered.wait(), 2)
    second = asyncio.create_task(wait("run", "prompt"))
    await asyncio.sleep(0)
    release.set()
    results = await asyncio.wait_for(asyncio.gather(first, second), 2)
    assert [result["state"] for result in results] == ["completed", "completed"]
    assert dispatch.await_count == 1
    assert driver._execution_tasks == {}
    failure.assert_not_awaited()


@pytest.mark.asyncio
async def test_authoritative_cancellation_stops_execution(execution_lane):
    driver, state, wait, _, failure, entered, _, cancelled = execution_lane
    waiter = asyncio.create_task(wait("run", "prompt"))
    await asyncio.wait_for(entered.wait(), 2)
    state["state"] = "cancelled"
    assert (await asyncio.wait_for(waiter, 2))["state"] == "cancelled"
    assert cancelled.is_set()
    assert driver._execution_tasks == {}
    failure.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancelling_waiter_preserves_execution_for_next_waiter(execution_lane):
    driver, state, wait, dispatch, failure, entered, release, cancelled = execution_lane
    first = asyncio.create_task(wait("run", "prompt"))
    await asyncio.wait_for(entered.wait(), 2)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert not cancelled.is_set()
    assert len(driver._execution_tasks) == 1
    second = asyncio.create_task(wait("run", "prompt"))
    await asyncio.sleep(0)
    release.set()
    assert (await asyncio.wait_for(second, 2))["state"] == "completed"
    assert dispatch.await_count == 1
    assert driver._execution_tasks == {}
    failure.assert_not_awaited()


@pytest.mark.asyncio
async def test_dispatch_failure_cleans_up_and_preserves_error(execution_lane):
    driver, _, wait, dispatch, failure, *_ = execution_lane
    dispatch.side_effect = RuntimeError("worker_unavailable")
    with pytest.raises(RuntimeError, match="^worker_unavailable$"):
        await wait("run", "prompt")
    failure.assert_awaited_once_with("run", {"code": "worker_unavailable", "message": "worker_unavailable"})
    assert driver._execution_tasks == {}
