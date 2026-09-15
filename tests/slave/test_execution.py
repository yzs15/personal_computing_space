import pytest

from loom_v2.contracts.types import CapabilityPackageVersion, CapabilityProvisionCommand, ComputeSpec, ExecutionContract, NodeInputBinding, ResourceRef, TaskClosure, TypedHole
from loom_v2.content_store import canonical_json_bytes
from loom_v2.slave.executor import ProcessJSONStdioV1Adapter, default_registry
from loom_v2.slave.service import SlaveService
from tests.support.run_code_fixture import make_run_code_fixture, provision_run_code_fixture


@pytest.mark.asyncio
async def test_subprocess_run_code_execution_returns_resource_ref():
    adapter = ProcessJSONStdioV1Adapter()
    result = await adapter.invoke(
        {"value": 3},
        program=b'import json,sys; d=json.load(sys.stdin); print(json.dumps({"value": d["value"] * 2}))',
    )
    assert result.resource_ref.resource_id
    assert result.value == {"value": 6}
    assert result.replay_safety == "DeclaredByPackage"


@pytest.mark.asyncio
async def test_subprocess_run_code_is_replay_safe():
    adapter = ProcessJSONStdioV1Adapter()
    program = b'import json,sys; d=json.load(sys.stdin); print(json.dumps({"value": d["value"] * 2}))'
    first = await adapter.invoke({"value": 3}, program=program)
    second = await adapter.invoke({"value": 3}, program=program)
    assert first.value == second.value
    assert first.replay_safety == "DeclaredByPackage"


def test_default_registry_has_only_subprocess_adapter():
    assert {descriptor.kind for descriptor in default_registry.descriptors()} == {"process:json_stdio"}


def test_default_registry_rejects_removed_builtin_executor():
    with pytest.raises(
        ValueError, match="unsupported_executor:function/builtin:function/1"
    ):
        default_registry.get_for(
            "function", ExecutionContract(kind="builtin:function", version="1")
        )


@pytest.mark.asyncio
async def test_slave_rejects_unbound_typed_hole_before_execution():
    service = SlaveService("slave-a", supported_operations={"test_double"})
    closure = TaskClosure(program={"operation_ref": "loom://test_double"}, compute=ComputeSpec(operation_ref="loom://test_double", typed_holes=[TypedHole(hole_id="h_test_double")]))

    with pytest.raises(RuntimeError, match="typed_hole_unbound:h_test_double"):
        await service.run("attempt-unbound", "test_double", {"value": 2}, closure=closure)


@pytest.mark.asyncio
async def test_slave_rejects_unsupported_operation():
    service = SlaveService("slave-a")

    with pytest.raises(RuntimeError, match="capability_unavailable:unknown_operation"):
        await service.run("attempt-unsupported", "unknown_operation", {"value": 2})


@pytest.mark.asyncio
async def test_slave_rejects_unbound_run_code_without_package():
    service = SlaveService("slave-a")

    with pytest.raises(RuntimeError, match="capability_package_required"):
        await service.run("attempt-unbound-run-code", "run_code", {"value": 2})


@pytest.mark.asyncio
async def test_subprocess_capability_timeout_is_independently_configurable(monkeypatch):
    monkeypatch.setenv("LOOM_CAPABILITY_OPERATION_TIMEOUT_SECONDS", "0.01")
    adapter = ProcessJSONStdioV1Adapter()
    program = b"import time; time.sleep(0.10); print('{}')"

    with pytest.raises(RuntimeError, match="capability_timeout"):
        await adapter.invoke({}, program=program)


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
        capability_exports=[],
        body={"program_content_ref": program_ref.model_dump(mode="json")},
        package_digest="",
    )

    with pytest.raises(RuntimeError, match="capability_package_invalid"):
        await service.provision(
            CapabilityProvisionCommand(
                command_id="command-contract-required",
                package_version_ref=package.version_ref,
                package_digest=package.package_digest,
                target_slave="slave-a",
                idempotency_key="command-contract-required",
                activation_revision=1,
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
    fixture = await make_run_code_fixture(
        service,
        operation="test_store_input",
        io_contract_ref=contract_ref,
        program=b'import json,sys; d=json.load(sys.stdin); print(json.dumps({"text": d["text"]}))',
    )
    await provision_run_code_fixture(service, fixture)
    closure = fixture.closure(io_contract_ref=contract_ref, input_refs=[input_ref])

    result = await service.run("attempt-admission-ref", fixture.operation, {"text": "tampered-driver-payload"}, closure=closure, binding=fixture.binding)

    assert result.value == {"text": "from-store"}


@pytest.mark.asyncio
async def test_slave_admission_rejects_bound_input_that_fails_input_schema():
    service = SlaveService("slave-a")
    contract_ref, _input_schema_ref = await _input_contract_ref(
        service,
        input_schema={"type": "object", "required": ["scores"], "properties": {"scores": {"type": "array"}}},
    )
    input_ref = await service.content_store.put(canonical_json_bytes({"payload": {"scores": [1, 2]}}), media_type="application/json")
    fixture = await make_run_code_fixture(
        service,
        operation="test_invalid_input",
        io_contract_ref=contract_ref,
        program=b'import json,sys; print(json.dumps({"ok": True}))',
    )
    await provision_run_code_fixture(service, fixture)
    closure = fixture.closure(io_contract_ref=contract_ref, input_refs=[input_ref])

    with pytest.raises(RuntimeError, match="payload_schema_mismatch"):
        await service.run("attempt-admission-invalid", fixture.operation, {"scores": [1, 2]}, closure=closure, binding=fixture.binding)


@pytest.mark.asyncio
async def test_slave_terminal_output_schema_pass_emits_validation_evidence():
    service = SlaveService("slave-a")
    contract_ref = await _contract_ref(service, output_schema={"type": "object", "required": ["text"]})
    fixture = await make_run_code_fixture(
        service,
        operation="test_output_pass",
        io_contract_ref=contract_ref,
        program=b'import json,sys; d=json.load(sys.stdin); print(json.dumps({"text": d["text"]}))',
    )
    await provision_run_code_fixture(service, fixture)
    closure = fixture.closure(io_contract_ref=contract_ref)

    result = await service.run("attempt-output-pass", fixture.operation, {"text": "hello"}, closure=closure, binding=fixture.binding)

    assert result.terminal_state == "completed"
    assert result.validation_evidence
    assert result.validation_evidence[0]["result"] == "pass"
    assert result.validation_evidence[0]["issuer"] == "slave"


@pytest.mark.asyncio
async def test_slave_terminal_evidence_uses_dispatch_execution_epoch():
    service = SlaveService("slave-a")
    contract_ref = await _contract_ref(service, output_schema={"type": "object", "required": ["text"]})
    fixture = await make_run_code_fixture(
        service,
        operation="test_output_epoch",
        io_contract_ref=contract_ref,
        program=b'import json,sys; d=json.load(sys.stdin); print(json.dumps({"text": d["text"]}))',
    )
    await provision_run_code_fixture(service, fixture)
    closure = fixture.closure(io_contract_ref=contract_ref)

    result = await service.run("attempt-output-epoch", fixture.operation, {"text": "hello"}, closure=closure, binding=fixture.binding, execution_epoch=7)

    assert result.validation_evidence[0]["execution_epoch"] == 7


@pytest.mark.asyncio
async def test_slave_terminal_output_schema_failure_is_not_completed():
    service = SlaveService("slave-a")
    contract_ref = await _contract_ref(service, output_schema={"type": "object", "required": ["missing"]})
    fixture = await make_run_code_fixture(
        service,
        operation="test_output_failure",
        io_contract_ref=contract_ref,
        program=b'import json,sys; print(json.dumps({"text": "hello"}))',
    )
    await provision_run_code_fixture(service, fixture)
    closure = fixture.closure(io_contract_ref=contract_ref)

    result = await service.run("attempt-output-fail", fixture.operation, {"text": "hello"}, closure=closure, binding=fixture.binding)

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
    fixture = await make_run_code_fixture(
        service,
        operation="test_attestation",
        io_contract_ref=contract_ref,
        program=b'import json,sys; d=json.load(sys.stdin); print(json.dumps({"text": d["text"]}))',
    )
    await provision_run_code_fixture(service, fixture)
    closure = fixture.closure(io_contract_ref=contract_ref)

    result = await service.run("attempt-attestation", fixture.operation, {"text": "hello"}, closure=closure, binding=fixture.binding)

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
    fixture = await make_run_code_fixture(
        service,
        operation="test_validator_failure",
        io_contract_ref=contract_ref,
        program=b'import json,sys; d=json.load(sys.stdin); print(json.dumps({"text": d["text"]}))',
    )
    await provision_run_code_fixture(service, fixture)

    closure = fixture.closure(io_contract_ref=contract_ref)
    result = await service.run("attempt-validator-fail", fixture.operation, {"text": "hello"}, closure=closure, binding=fixture.binding)

    assert result.terminal_state == "failed"
    assert result.terminal_error["code"] == "success_validation_failed"
