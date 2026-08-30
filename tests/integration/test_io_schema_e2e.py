import pytest

from loom_v2.contracts.types import ClosureContract, ComputeBinding, ResourceRef, TaskClosure
from loom_v2.driver.service import DriverService
from loom_v2.observer.repository import ObserverRepository
from loom_v2.slave.service import SlaveService


@pytest.mark.asyncio
async def test_content_refs_drive_readiness_dispatch_and_terminal_evidence():
    repo = ObserverRepository()
    input_schema_ref = await repo.put_content(
        {"type": "object", "required": ["scores"], "properties": {"scores": {"type": "array", "items": {"type": "number"}}}},
        media_type="application/schema+json",
    )
    output_schema_ref = await repo.put_content(
        {"type": "object", "required": ["average"], "properties": {"average": {"type": "number"}}},
        media_type="application/schema+json",
    )
    io_contract_ref = await repo.put_content(
        {
            "schema_version": "io.v1",
            "input_schema_ref": input_schema_ref.model_dump(mode="json"),
            "output_schema_ref": output_schema_ref.model_dump(mode="json"),
            "success_semantics": None,
            "success_validator_ref": None,
        },
        media_type="application/vnd.loom.io-contract+json",
    )
    program_ref = await repo.put_content(
        b'import sys,json; d=json.load(sys.stdin); print(json.dumps({"average": sum(d["scores"])/len(d["scores"])}))',
        media_type="text/x-python",
    )
    closure = ClosureContract(
        closure_id="closure-scores-e2e",
        goal="average scores",
        body=TaskClosure(
            closure_id="closure-scores-e2e",
            program={"operation_ref": "loom://average", "io_contract_ref": io_contract_ref.model_dump(mode="json")},
        ),
    )
    record = await repo.open_run("run-scores-e2e", "conversation-scores-e2e", "average scores", closure_contract=closure)
    wrapped_ref = await repo.put_content({"payload": {"scores": [80, 90]}}, media_type="application/json")
    bad = await repo.apply_patch(
        record.run_id,
        record.draft_version,
        record.draft_digest,
        "scores-input-wrapped",
        [
            {"kind": "set_execution_payload", "value": {"node_id": "loom://average", "input_ref": wrapped_ref.model_dump(mode="json")}},
        ],
    )
    assert any(item["code"] == "payload_schema_mismatch" for item in bad.readiness["blockers"])

    input_ref = await repo.put_content({"scores": [80, 90]}, media_type="application/json")
    program_patch = await repo.apply_patch(
        record.run_id,
        bad.draft_version,
        bad.draft_digest,
        "scores-materialize",
        [
            {"kind": "set_execution_payload", "value": {"node_id": "loom://average", "input_ref": input_ref.model_dump(mode="json")}},
            {"kind": "add_typed_hole", "value": {"hole_id": "h_average"}},
            {
                "kind": "materialize_capability_package_candidate",
                "value": {
                    "package_id": "scores-package",
                    "program_content_ref": program_ref.model_dump(mode="json"),
                    "io_contract_ref": io_contract_ref.model_dump(mode="json"),
                    "operation_descriptor_ref": "loom://average",
                },
            },
        ],
    )
    package = (await repo.get_run(record.run_id)).capability_packages[0]
    binding = ComputeBinding(
        binding_id="binding-scores-e2e",
        hole_id="h_average",
        capability_descriptor_ref=ResourceRef(resource_id="executor://subprocess_json_v1/1"),
        capability_package_ref=ResourceRef(
            resource_id=package.package_closure_version_ref,
            version_or_digest=package.package_digest,
        ),
        target_resource_ref=ResourceRef(resource_id="slave-a"),
        realization_digest=package.program_digest,
    )
    bound = await repo.apply_patch(
        record.run_id,
        program_patch.draft_version,
        program_patch.draft_digest,
        "scores-bind",
        [{"kind": "bind_compute_hole", "value": binding.model_dump(mode="json")}],
    )
    assert bound.readiness["ready"] is True
    committed = await repo.commit(record.run_id, bound.draft_version, bound.draft_digest)
    await repo.start(record.run_id, committed.version_id)

    driver = DriverService(repo, provider=object(), slaves={"slave-a": SlaveService("slave-a", content_store=repo.content_store)})
    completed, result = await driver._dispatch_execution(record.run_id, "average scores")

    assert completed.state == "completed"
    assert result.value == {"average": 85.0}
    assert completed.outcome["validation_evidence"]
