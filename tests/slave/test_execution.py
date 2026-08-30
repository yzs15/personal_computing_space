import pytest

from loom_v2.contracts.types import CapabilityPackageVersion, CapabilityProvisionCommand, ComputeSpec, NodeInputBinding, ResourceRef, TaskClosure, TypedHole
from loom_v2.content_store import canonical_json_bytes
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


@pytest.mark.asyncio
async def test_slave_provision_rejects_package_without_io_contract():
    service = SlaveService("slave-a")
    program_ref = await service.content_store.put(b"print('{}')", media_type="text/x-python")
    package = CapabilityPackageVersion.model_construct(
        package_id="pkg-contract-required",
        package_version="v1",
        package_closure_version_ref="closure",
        source_run_ref="run",
        source_closure_version_ref="version",
        operation_descriptor_ref=ResourceRef(resource_id="loom://check"),
        operation_descriptor_digest="descriptor",
        program_content_ref=program_ref,
        program_digest=program_ref.version_or_digest,
        io_contract_ref=None,
    )

    with pytest.raises(RuntimeError, match="io_contract_required"):
        await service.provision(
            CapabilityProvisionCommand(
                command_id="command-contract-required",
                package_version_ref=package.version_ref,
                package_digest=package.package_digest,
                target_slave="slave-a",
            ),
            package,
        )


async def _contract_ref(service: SlaveService, *, output_schema: dict, success_semantics=None, validator_ref=None):
    schema_ref = await service.content_store.put(canonical_json_bytes(output_schema), media_type="application/schema+json")
    body = {
        "schema_version": "io.v1",
        "input_schema_ref": None,
        "output_schema_ref": schema_ref.model_dump(mode="json"),
        "success_semantics": success_semantics,
        "success_validator_ref": validator_ref.model_dump(mode="json") if validator_ref else None,
    }
    return await service.content_store.put(canonical_json_bytes(body), media_type="application/vnd.loom.io-contract+json")


async def _input_contract_ref(service: SlaveService, *, input_schema: dict, output_schema: dict | None = None):
    input_ref = await service.content_store.put(canonical_json_bytes(input_schema), media_type="application/schema+json")
    output_ref = None
    if output_schema is not None:
        output_ref = await service.content_store.put(canonical_json_bytes(output_schema), media_type="application/schema+json")
    body = {
        "schema_version": "io.v1",
        "input_schema_ref": input_ref.model_dump(mode="json"),
        "output_schema_ref": output_ref.model_dump(mode="json") if output_ref else None,
        "success_semantics": None,
        "success_validator_ref": None,
    }
    contract_ref = await service.content_store.put(canonical_json_bytes(body), media_type="application/vnd.loom.io-contract+json")
    return contract_ref, input_ref


@pytest.mark.asyncio
async def test_slave_admission_reads_bound_input_from_content_store_not_driver_payload():
    service = SlaveService("slave-a")
    contract_ref, input_schema_ref = await _input_contract_ref(
        service,
        input_schema={"type": "object", "required": ["text"], "properties": {"text": {"type": "string"}}},
    )
    input_ref = await service.content_store.put(canonical_json_bytes({"text": "from-store"}), media_type="application/json")
    closure = TaskClosure(
        program={"operation_ref": "loom://echo", "io_contract_ref": contract_ref.model_dump(mode="json")},
        node_input_bindings=[NodeInputBinding(node_id="echo", input_ref=input_ref)],
    )

    result = await service.run("attempt-admission-ref", "echo", {"text": "tampered-driver-payload"}, closure=closure)

    assert result.value == {"text": "from-store"}


@pytest.mark.asyncio
async def test_slave_admission_rejects_bound_input_that_fails_input_schema():
    service = SlaveService("slave-a")
    contract_ref, _input_schema_ref = await _input_contract_ref(
        service,
        input_schema={"type": "object", "required": ["scores"], "properties": {"scores": {"type": "array"}}},
    )
    input_ref = await service.content_store.put(canonical_json_bytes({"payload": {"scores": [1, 2]}}), media_type="application/json")
    closure = TaskClosure(
        program={"operation_ref": "loom://echo", "io_contract_ref": contract_ref.model_dump(mode="json")},
        node_input_bindings=[NodeInputBinding(node_id="echo", input_ref=input_ref)],
    )

    with pytest.raises(RuntimeError, match="payload_schema_mismatch"):
        await service.run("attempt-admission-invalid", "echo", {"scores": [1, 2]}, closure=closure)


@pytest.mark.asyncio
async def test_slave_terminal_output_schema_pass_emits_validation_evidence():
    service = SlaveService("slave-a")
    contract_ref = await _contract_ref(service, output_schema={"type": "object", "required": ["text"]})
    closure = TaskClosure(program={"operation_ref": "loom://echo", "io_contract_ref": contract_ref.model_dump(mode="json")})

    result = await service.run("attempt-output-pass", "echo", {"text": "hello"}, closure=closure)

    assert result.terminal_state == "completed"
    assert result.validation_evidence
    assert result.validation_evidence[0]["result"] == "pass"
    assert result.validation_evidence[0]["issuer"] == "slave"


@pytest.mark.asyncio
async def test_slave_terminal_evidence_uses_dispatch_execution_epoch():
    service = SlaveService("slave-a")
    contract_ref = await _contract_ref(service, output_schema={"type": "object", "required": ["text"]})
    closure = TaskClosure(program={"operation_ref": "loom://echo", "io_contract_ref": contract_ref.model_dump(mode="json")})

    result = await service.run("attempt-output-epoch", "echo", {"text": "hello"}, closure=closure, execution_epoch=7)

    assert result.validation_evidence[0]["execution_epoch"] == 7


@pytest.mark.asyncio
async def test_slave_terminal_output_schema_failure_is_not_completed():
    service = SlaveService("slave-a")
    contract_ref = await _contract_ref(service, output_schema={"type": "object", "required": ["missing"]})
    closure = TaskClosure(program={"operation_ref": "loom://echo", "io_contract_ref": contract_ref.model_dump(mode="json")})

    result = await service.run("attempt-output-fail", "echo", {"text": "hello"}, closure=closure)

    assert result.terminal_state == "failed"
    assert result.terminal_error["code"] == "output_schema_mismatch"
    assert result.validation_evidence[0]["result"] == "fail"


@pytest.mark.asyncio
async def test_slave_without_validator_requires_attestation_for_semantic_success():
    service = SlaveService("slave-a")
    contract_ref = await _contract_ref(
        service,
        output_schema={"type": "object", "required": ["text"]},
        success_semantics={"criterion": "contains-greeting"},
    )
    closure = TaskClosure(program={"operation_ref": "loom://echo", "io_contract_ref": contract_ref.model_dump(mode="json")})

    result = await service.run("attempt-attestation", "echo", {"text": "hello"}, closure=closure)

    assert result.terminal_state == "decision_required"
    assert result.terminal_error["code"] == "attestation_required"


@pytest.mark.asyncio
async def test_slave_validator_plugin_failure_is_structured():
    service = SlaveService("slave-a")
    validator_ref = await service.content_store.put(
        b'import json; print(json.dumps({"result": "fail", "errors": [{"path": "$", "keyword": "criterion", "message": "failed"}]}))',
        media_type="text/x-python",
    )
    contract_ref = await _contract_ref(
        service,
        output_schema={"type": "object", "required": ["text"]},
        success_semantics={"criterion": "contains-greeting"},
        validator_ref=validator_ref,
    )
    closure = TaskClosure(program={"operation_ref": "loom://echo", "io_contract_ref": contract_ref.model_dump(mode="json")})

    result = await service.run("attempt-validator-fail", "echo", {"text": "hello"}, closure=closure)

    assert result.terminal_state == "failed"
    assert result.terminal_error["code"] == "success_validation_failed"
