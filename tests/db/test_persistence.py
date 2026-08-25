from sqlalchemy.ext.asyncio import create_async_engine

from loom_v2.observer.repository import ObserverRepository


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
