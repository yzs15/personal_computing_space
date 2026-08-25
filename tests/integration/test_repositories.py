import pytest

from loom_v2.observer.repository import ObserverRepository


@pytest.mark.asyncio
async def test_patch_is_cas_and_idempotent():
    repo = ObserverRepository()
    run = await repo.open_run("run-1", "task-1", "echo")
    base_version = run.draft_version
    base_digest = run.draft_digest
    first = await repo.apply_patch(
        run.run_id,
        base_version,
        base_digest,
        "op-1",
        [{"kind": "set_result_expectation", "value": {"kind": "content"}}],
    )
    replay = await repo.apply_patch(
        run.run_id,
        base_version,
        base_digest,
        "op-1",
        [{"kind": "set_result_expectation", "value": {"kind": "content"}}],
    )
    assert replay.receipt == first.receipt
    with pytest.raises(ValueError, match="version_conflict"):
        await repo.apply_patch(run.run_id, base_version, base_digest, "op-2", [])
