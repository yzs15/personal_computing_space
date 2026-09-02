import hashlib

import pytest

from loom_v2.content_store import canonical_json_bytes
from loom_v2.contracts.errors import DomainError
from loom_v2.observer.repository import ObserverRepository


def _digest(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


async def _started_run(repo: ObserverRepository, run_id: str) -> tuple[object, dict[str, object]]:
    record = await repo.open_run(run_id, f"conversation-{run_id}", "echo")
    await repo.begin_refinement(record.run_id)
    committed = await repo.commit(record.run_id, record.draft_version, record.draft_digest)
    started = await repo.start(record.run_id, committed.version_id)
    return record, started


@pytest.mark.asyncio
async def test_readiness_failure_keeps_run_in_thinking() -> None:
    repo = ObserverRepository()
    record = await repo.open_run("run-readiness-state", "conversation-readiness-state", "sort")
    await repo.begin_refinement(record.run_id)
    patched = await repo.apply_patch(
        record.run_id,
        record.draft_version,
        record.draft_digest,
        "readiness-blocker",
        [
            {"kind": "set_program_ref", "value": "loom://sort"},
            {"kind": "set_compute_spec", "value": {"operation_ref": "loom://sort"}},
            {"kind": "add_typed_hole", "value": {"hole_id": "h_sort"}},
        ],
    )

    with pytest.raises(DomainError, match="readiness_blocked"):
        await repo.commit(record.run_id, patched.draft_version, patched.draft_digest)

    assert (await repo.get_run(record.run_id)).state == "thinking"


@pytest.mark.asyncio
async def test_record_result_projects_completed_outcome() -> None:
    repo = ObserverRepository()
    record, started = await _started_run(repo, "run-lifecycle-completed")
    current = await repo.get_run(record.run_id)
    value = {"text": "ok"}

    updated = await repo.record_result(
        record.run_id,
        {
            "attempt_id": current.attempts[0]["attempt_id"],
            "execution_id": started["execution_id"],
            "execution_epoch": started["execution_epoch"],
            "value": value,
            "digest": _digest(value),
            "terminal_state": "completed",
        },
    )

    assert updated.state == "completed"
    assert updated.outcome["disposition"] == "completed"
    assert updated.outcome["decision"] is None


@pytest.mark.asyncio
async def test_attestation_accept_preserves_raw_execution_result() -> None:
    repo = ObserverRepository()
    record, started = await _started_run(repo, "run-lifecycle-attestation")
    current = await repo.get_run(record.run_id)
    value = {"text": "candidate"}
    digest = _digest(value)
    resource_ref = {"resource_id": "result-candidate", "version_or_digest": digest}

    pending = await repo.record_result(
        record.run_id,
        {
            "attempt_id": current.attempts[0]["attempt_id"],
            "execution_id": started["execution_id"],
            "execution_epoch": started["execution_epoch"],
            "value": value,
            "digest": digest,
            "resource_ref": resource_ref,
            "terminal_state": "decision_required",
        },
    )

    assert pending.state == "awaiting_decision"
    assert pending.outcome["disposition"] == "awaiting_decision"
    assert pending.outcome["decision"] == "attestation"
    assert pending.outcome["terminal_state"] == "decision_required"
    assert pending.outcome["resource_ref"] == resource_ref

    accepted = await repo.resolve_run(record.run_id, "accept")

    assert accepted.state == "completed"
    assert accepted.outcome["disposition"] == "completed"
    assert accepted.outcome["decision"] is None
    assert accepted.outcome["terminal_state"] == "decision_required"
    assert accepted.outcome["resource_ref"] == resource_ref


@pytest.mark.asyncio
async def test_repair_reopen_clears_outcome_and_rerun_fences_old_execution() -> None:
    repo = ObserverRepository()
    record, started = await _started_run(repo, "run-lifecycle-repair")
    current = await repo.get_run(record.run_id)
    value = {"text": "bad"}

    pending = await repo.record_result(
        record.run_id,
        {
            "attempt_id": current.attempts[0]["attempt_id"],
            "execution_id": started["execution_id"],
            "execution_epoch": started["execution_epoch"],
            "value": value,
            "digest": _digest(value),
            "terminal_state": "failed",
            "terminal_error": {"code": "execution_failed"},
        },
    )
    assert pending.state == "awaiting_decision"
    assert pending.outcome["decision"] == "repair"

    patched = await repo.apply_patch(
        record.run_id,
        pending.draft_version,
        pending.draft_digest,
        "repair-patch",
        [{"kind": "set_result_expectation", "value": {"kind": "content"}}],
    )
    reopened = await repo.get_run(record.run_id)
    assert reopened.state == "thinking"
    assert reopened.outcome is None

    committed = await repo.commit(record.run_id, patched.draft_version, patched.draft_digest)
    rerun = await repo.start(record.run_id, committed.version_id)

    assert rerun["execution_id"] != started["execution_id"]
    assert rerun["execution_epoch"] == started["execution_epoch"] + 1

    with pytest.raises(ValueError, match="stale_execution_epoch"):
        await repo.record_result(
            record.run_id,
            {
                "attempt_id": current.attempts[0]["attempt_id"],
                "execution_id": started["execution_id"],
                "execution_epoch": started["execution_epoch"],
                "value": value,
                "digest": _digest(value),
                "terminal_state": "completed",
            },
        )


@pytest.mark.asyncio
async def test_completed_run_rejects_commit_transition() -> None:
    repo = ObserverRepository()
    record, started = await _started_run(repo, "run-lifecycle-illegal")
    current = await repo.get_run(record.run_id)
    value = {"text": "ok"}
    await repo.record_result(
        record.run_id,
        {
            "attempt_id": current.attempts[0]["attempt_id"],
            "execution_id": started["execution_id"],
            "execution_epoch": started["execution_epoch"],
            "value": value,
            "digest": _digest(value),
            "terminal_state": "completed",
        },
    )

    with pytest.raises(ValueError, match="illegal_state_transition"):
        await repo.commit(record.run_id, current.draft_version, current.draft_digest)
