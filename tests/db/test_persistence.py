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
