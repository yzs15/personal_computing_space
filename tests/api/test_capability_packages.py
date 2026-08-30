import asyncio

import pytest

from loom_v2.contracts.types import ClosureContract, ComputeBinding, ResourceRef, TaskClosure
from loom_v2.observer.repository import ObserverRepository


async def _empty_io_contract_ref(repo: ObserverRepository):
    return await repo.put_content(
        {
            "schema_version": "io.v1",
            "input_schema_ref": None,
            "output_schema_ref": None,
            "success_semantics": None,
            "success_validator_ref": None,
        },
        media_type="application/vnd.loom.io-contract+json",
    )


async def _program_ref(repo: ObserverRepository):
    return await repo.put_content("print(1)", media_type="text/x-python")


@pytest.mark.asyncio
async def test_candidate_is_run_bound_and_cannot_be_used_by_another_run():
    repo = ObserverRepository()
    first = await repo.open_run("run-a", "conversation-a", "matmul")
    program_ref = await _program_ref(repo)
    io_contract_ref = await _empty_io_contract_ref(repo)
    patched = await repo.apply_patch(
        first.run_id,
        first.draft_version,
        first.draft_digest,
        "materialize-a",
        [
            {"kind": "set_program_ref", "value": "loom://matmul"},
            {"kind": "add_typed_hole", "value": {"hole_id": "h"}},
            {"kind": "materialize_capability_package_candidate", "value": {"package_id": "pkg", "program_content_ref": program_ref.model_dump(mode="json"), "io_contract_ref": io_contract_ref.model_dump(mode="json"), "operation_descriptor_ref": "loom://matmul"}},
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
    program_ref = await _program_ref(repo)
    io_contract_ref = await _empty_io_contract_ref(repo)
    receipt = await repo.apply_patch(
        run.run_id, run.draft_version, run.draft_digest, "materialize-promote",
        [{"kind": "materialize_capability_package_candidate", "value": {"package_id": "pkg-promote", "program_content_ref": program_ref.model_dump(mode="json"), "io_contract_ref": io_contract_ref.model_dump(mode="json"), "operation_descriptor_ref": "loom://check"}}],
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
    program_ref = await _program_ref(repo)
    io_contract_ref = await _empty_io_contract_ref(repo)
    await repo.apply_patch(
        run.run_id,
        run.draft_version,
        run.draft_digest,
        "materialize-promote-idempotent",
        [{"kind": "materialize_capability_package_candidate", "value": {"package_id": "pkg-idempotent", "program_content_ref": program_ref.model_dump(mode="json"), "io_contract_ref": io_contract_ref.model_dump(mode="json"), "operation_descriptor_ref": "loom://check"}}],
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
    program_ref = await _program_ref(repo)
    io_contract_ref = await _empty_io_contract_ref(repo)
    await repo.apply_patch(
        run.run_id,
        run.draft_version,
        run.draft_digest,
        "materialize-promote-concurrent",
        [{"kind": "materialize_capability_package_candidate", "value": {"package_id": "pkg-concurrent", "program_content_ref": program_ref.model_dump(mode="json"), "io_contract_ref": io_contract_ref.model_dump(mode="json"), "operation_descriptor_ref": "loom://check"}}],
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


@pytest.mark.asyncio
async def test_materialization_requires_io_contract_and_preuploaded_program_ref() -> None:
    repo = ObserverRepository()
    run = await repo.open_run("run-package-contract-required", "conversation-package-contract-required", "check")
    program_ref = await repo.put_content("print(1)", media_type="text/x-python")
    contract_ref = await repo.put_content(
        {
            "schema_version": "io.v1",
            "input_schema_ref": None,
            "output_schema_ref": None,
            "success_semantics": None,
            "success_validator_ref": None,
        },
        media_type="application/vnd.loom.io-contract+json",
    )

    with pytest.raises(ValueError, match="io_contract_required"):
        await repo.apply_patch(
            run.run_id,
            run.draft_version,
            run.draft_digest,
            "materialize-contract-required",
            [
                {
                    "kind": "materialize_capability_package_candidate",
                    "value": {
                        "package_id": "pkg-contract-required",
                        "program_content_ref": program_ref.model_dump(mode="json"),
                        "operation_descriptor_ref": "loom://check",
                    },
                }
            ],
        )

    run = await repo.get_run(run.run_id)
    with pytest.raises(ValueError, match="program_content_ref_required"):
        await repo.apply_patch(
            run.run_id,
            run.draft_version,
            run.draft_digest,
            "materialize-inline-program",
            [
                {
                    "kind": "materialize_capability_package_candidate",
                    "value": {
                        "package_id": "pkg-inline-program",
                        "program": "print(1)",
                        "io_contract_ref": contract_ref.model_dump(mode="json"),
                        "operation_descriptor_ref": "loom://check",
                    },
                }
            ],
        )


@pytest.mark.asyncio
async def test_binding_package_with_different_io_contract_is_not_ready() -> None:
    repo = ObserverRepository()
    schema_ref = await repo.put_content({"type": "object"}, media_type="application/schema+json")
    closure_contract_ref = await repo.put_content(
        {
            "schema_version": "io.v1",
            "input_schema_ref": schema_ref.model_dump(mode="json"),
            "output_schema_ref": None,
            "success_semantics": None,
            "success_validator_ref": None,
        },
        media_type="application/vnd.loom.io-contract+json",
    )
    other_schema_ref = await repo.put_content({"type": "array"}, media_type="application/schema+json")
    package_contract_ref = await repo.put_content(
        {
            "schema_version": "io.v1",
            "input_schema_ref": other_schema_ref.model_dump(mode="json"),
            "output_schema_ref": None,
            "success_semantics": None,
            "success_validator_ref": None,
        },
        media_type="application/vnd.loom.io-contract+json",
    )
    program_ref = await repo.put_content("print(1)", media_type="text/x-python")
    closure = ClosureContract(
        closure_id="closure-package-contract",
        goal="check",
        body=TaskClosure(
            closure_id="closure-package-contract",
            program={"operation_ref": "loom://check", "io_contract_ref": closure_contract_ref.model_dump(mode="json")},
        ),
    )
    run = await repo.open_run("run-package-contract", "conversation-package-contract", "check", closure_contract=closure)
    materialized = await repo.apply_patch(
        run.run_id,
        run.draft_version,
        run.draft_digest,
        "materialize-package-contract",
        [
            {"kind": "add_typed_hole", "value": {"hole_id": "h_check"}},
            {
                "kind": "materialize_capability_package_candidate",
                "value": {
                    "package_id": "pkg-contract-mismatch",
                    "program_content_ref": program_ref.model_dump(mode="json"),
                    "io_contract_ref": package_contract_ref.model_dump(mode="json"),
                    "operation_descriptor_ref": "loom://check",
                },
            },
        ],
    )
    package = (await repo.get_run(run.run_id)).capability_packages[0]
    bound = await repo.apply_patch(
        run.run_id,
        materialized.draft_version,
        materialized.draft_digest,
        "bind-package-contract",
        [
            {
                "kind": "bind_compute_hole",
                "value": {
                    "binding_id": "binding-contract-mismatch",
                    "hole_id": "h_check",
                    "capability_descriptor_ref": {"resource_id": "capability://slave-a/check"},
                    "capability_package_ref": {"resource_id": package.version_ref},
                    "target_resource_ref": {"resource_id": "slave-a"},
                    "realization_digest": package.program_digest,
                },
            }
        ],
    )

    assert any(blocker["code"] == "io_contract_mismatch" for blocker in bound.readiness["blockers"])
