import asyncio

import pytest

from loom_v2.contracts.types import ComputeBinding, ResourceRef
from loom_v2.observer.repository import ObserverRepository


@pytest.mark.asyncio
async def test_candidate_is_run_bound_and_cannot_be_used_by_another_run():
    repo = ObserverRepository()
    first = await repo.open_run("run-a", "conversation-a", "matmul")
    patched = await repo.apply_patch(
        first.run_id,
        first.draft_version,
        first.draft_digest,
        "materialize-a",
        [
            {"kind": "set_program_ref", "value": "loom://matmul"},
            {"kind": "add_typed_hole", "value": {"hole_id": "h"}},
            {"kind": "materialize_capability_package_candidate", "value": {"package_id": "pkg", "program": "print(1)", "operation_descriptor_ref": "loom://matmul"}},
        ],
    )
    package = (await repo.get_run(first.run_id)).capability_packages[0]
    second = await repo.open_run("run-b", "conversation-b", "matmul")
    result = await repo.apply_patch(
        second.run_id,
        second.draft_version,
        second.draft_digest,
        "bind-b",
        [
            {"kind": "set_program_ref", "value": "loom://matmul"},
            {"kind": "add_typed_hole", "value": {"hole_id": "h"}},
            {"kind": "bind_compute_hole", "value": ComputeBinding(
                binding_id="binding-b", hole_id="h", capability_descriptor_ref=ResourceRef(resource_id="run-code"),
                capability_package_ref=ResourceRef(resource_id=package.version_ref), target_resource_ref=ResourceRef(resource_id="slave-a"),
                realization_digest=package.program_digest,
            ).model_dump(mode="json")},
        ],
    )
    assert any(blocker["code"] == "capability_package_scope_mismatch" for blocker in result.readiness["blockers"])
    assert all(item.package_id != "pkg" for item in await repo.list_capability_packages(run_id=second.run_id))


@pytest.mark.asyncio
async def test_promotion_derives_reusable_version_without_mutating_candidate():
    repo = ObserverRepository()
    run = await repo.open_run("run-promote", "conversation-promote", "check")
    receipt = await repo.apply_patch(
        run.run_id, run.draft_version, run.draft_digest, "materialize-promote",
        [{"kind": "materialize_capability_package_candidate", "value": {"package_id": "pkg-promote", "program": "print(1)", "operation_descriptor_ref": "loom://check"}}],
    )
    await repo.fail_run(run.run_id, "test")
    candidate = (await repo.get_run(run.run_id)).capability_packages[0]
    reusable = await repo.promote_capability_package(candidate.version_ref, approved_digest=candidate.package_digest)
    assert candidate.scope == "run_bound"
    assert candidate.publication_state == "candidate"
    assert reusable.scope == "workspace_reusable"
    assert reusable.publication_state == "published"
    assert reusable.package_version != candidate.package_version


@pytest.mark.asyncio
async def test_candidate_ref_promotion_is_idempotent():
    repo = ObserverRepository()
    run = await repo.open_run("run-promote-idempotent", "conversation-promote-idempotent", "check")
    await repo.apply_patch(
        run.run_id,
        run.draft_version,
        run.draft_digest,
        "materialize-promote-idempotent",
        [{"kind": "materialize_capability_package_candidate", "value": {"package_id": "pkg-idempotent", "program": "print(1)", "operation_descriptor_ref": "loom://check"}}],
    )
    await repo.fail_run(run.run_id, "test")
    candidate = (await repo.get_run(run.run_id)).capability_packages[0]

    first = await repo.promote_capability_package(candidate.version_ref, approved_digest=candidate.package_digest)
    second = await repo.promote_capability_package(candidate.version_ref, approved_digest=candidate.package_digest)
    third = await repo.promote_capability_package(candidate.version_ref, approved_digest=candidate.package_digest)

    assert second.version_ref == first.version_ref
    assert third.version_ref == first.version_ref
    published = [
        package
        for package in await repo.list_capability_packages(run_id=run.run_id, include_abandoned=True)
        if package.scope == "workspace_reusable" and package.publication_state == "published"
    ]
    assert len(published) == 1


@pytest.mark.asyncio
async def test_candidate_ref_promotion_is_idempotent_under_concurrent_retries():
    repo = ObserverRepository()
    run = await repo.open_run("run-promote-concurrent", "conversation-promote-concurrent", "check")
    await repo.apply_patch(
        run.run_id,
        run.draft_version,
        run.draft_digest,
        "materialize-promote-concurrent",
        [{"kind": "materialize_capability_package_candidate", "value": {"package_id": "pkg-concurrent", "program": "print(1)", "operation_descriptor_ref": "loom://check"}}],
    )
    await repo.fail_run(run.run_id, "test")
    candidate = (await repo.get_run(run.run_id)).capability_packages[0]

    results = await asyncio.gather(*(
        repo.promote_capability_package(candidate.version_ref, approved_digest=candidate.package_digest)
        for _ in range(3)
    ))

    assert {result.version_ref for result in results} == {f"capability-package://{candidate.package_id}/{candidate.package_version}-reusable"}
    published = [
        package
        for package in await repo.list_capability_packages(run_id=run.run_id, include_abandoned=True)
        if package.scope == "workspace_reusable" and package.publication_state == "published"
    ]
    assert len(published) == 1
