from sqlalchemy.ext.asyncio import create_async_engine
from copy import deepcopy

from loom_v2.observer.repository import ObserverRepository
from loom_v2.contracts.types import ClosureContract, TaskClosure
from loom_v2.db.models import RunRow
from loom_v2.db.session import make_session_factory


async def test_observer_repository_persists_run_snapshot():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    repo = ObserverRepository(engine)
    await repo.init_db()
    created = await repo.open_run("persisted-run", "task-1", "echo")
    restored_repo = ObserverRepository(engine)
    await restored_repo.init_db()
    restored = await restored_repo.get_run(created.run_id)
    assert restored.task_ref == "task-1"
    assert restored.draft.snapshot.metadata["goal"] == "echo"


async def test_observer_repository_persists_closure_contract():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    repo = ObserverRepository(engine)
    await repo.init_db()
    contract = ClosureContract(
        closure_id="closure-1",
        goal="sort numbers",
        required_success_criteria=[{"criterion_id": "sorted"}],
        resource_budget={"max_attempts": 2},
        body=TaskClosure(program={"operation_ref": "loom://sort"}),
    )
    created = await repo.open_run("contract-run", "conversation-contract", contract.goal, closure_contract=contract)
    restored_repo = ObserverRepository(engine)
    await restored_repo.init_db()
    restored = await restored_repo.get_run(created.run_id)
    assert restored.closure_contract is not None
    assert restored.closure_contract.resource_budget == {"max_attempts": 2}


async def test_observer_repository_normalizes_legacy_structured_operation_ref():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    repo = ObserverRepository(engine)
    await repo.init_db()
    created = await repo.open_run("legacy-op-run", "legacy-op-conversation", "echo")
    sessions = make_session_factory(engine)
    async with sessions() as session:
        row = await session.get(RunRow, created.run_id)
        draft = deepcopy(row.draft)
        draft["snapshot"]["program"]["operation_ref"] = {"program_ref": "loom://echo"}
        row.draft = draft
        await session.commit()

    restored = await ObserverRepository(engine).get_run(created.run_id)
    assert restored.draft.snapshot.program.operation_ref == "loom://echo"


async def test_repository_recovers_orphaned_refinement_after_restart():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    repo = ObserverRepository(engine)
    await repo.init_db()
    created = await repo.open_run("orphan-refinement", "conversation-recovery", "echo")
    await repo.begin_refinement(created.run_id)

    restored = ObserverRepository(engine)
    await restored.init_db()
    recovered = await restored.recover_stale_runs()

    assert recovered == [created.run_id]
    record = await restored.get_run(created.run_id)
    assert record.state == "failed"
    assert record.outcome == {"reason": {"code": "observer_restarted"}}
    assert record.events[-1]["phase"] == "run_recovered"
    assert record.events[-1]["reason"] == {"code": "observer_restarted"}


async def test_repository_recovers_orphaned_execution_and_fences_attempt():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    repo = ObserverRepository(engine)
    await repo.init_db()
    created = await repo.open_run("orphan-execution", "conversation-recovery-exec", "echo")
    committed = await repo.commit(created.run_id, created.draft.version_id, created.draft.snapshot_digest)
    await repo.start(created.run_id, committed.version_id)

    restored = ObserverRepository(engine)
    await restored.init_db()
    recovered = await restored.recover_stale_runs()

    assert recovered == [created.run_id]
    record = await restored.get_run(created.run_id)
    assert record.state == "failed"
    assert record.execution_id is not None
    assert record.attempts[0]["state"] == "failed"
    assert record.outcome == {"reason": {"code": "observer_restarted"}}


async def test_repository_recovery_does_not_change_terminal_runs():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    repo = ObserverRepository(engine)
    await repo.init_db()
    created = await repo.open_run("completed-run", "conversation-recovery-terminal", "echo")
    created.state = "completed"
    created.outcome = {"value": "ok"}
    await repo._persist(created)

    restored = ObserverRepository(engine)
    await restored.init_db()
    assert await restored.recover_stale_runs() == []
    record = await restored.get_run(created.run_id)
    assert record.state == "completed"
    assert record.outcome == {"value": "ok"}
