import pytest
from pydantic import ValidationError

from loom_v2.contracts.types import CapabilityPackageVersion, DynamicNode, NodeIntent, ResourceRef


def _ref(resource_id: str, digest: str | None = None) -> ResourceRef:
    return ResourceRef(resource_id=resource_id, version_or_digest=None if resource_id.startswith("content://sha256/") else digest)


def _orchestration_package(**overrides):
    fields = {
        "package_id": "orchestrate-average",
        "package_version": "v1",
        "package_closure_version_ref": "package-closure-1",
        "source_run_ref": "run-1",
        "source_closure_version_ref": "draft-1",
        "package_type": "function",
        "execution": {"kind": "container:python_orchestrator", "version": "1"},
        "body": {"operation_descriptor_ref": _ref("loom://orchestrate"), "program_content_ref": _ref("content://sha256/" + "a" * 64, "a" * 64), "io_contract_ref": _ref("content://sha256/" + "b" * 64, "b" * 64), "allowed_node_package_refs": [_ref("capability-package://summarize/v1", "c" * 64)], "max_nodes": 10, "max_live_nodes": 2},
    }
    body = dict(fields["body"])
    for key, val in overrides.items():
        if key in {"allowed_node_package_refs", "max_nodes", "max_live_nodes"}: body[key] = val
        elif key == "body": body.update(val)
        else: fields[key] = val
    fields["body"] = body
    return CapabilityPackageVersion(**fields)


def test_orchestration_package_carries_allowlist_and_limits():
    package = _orchestration_package()

    assert package.function_body.allowed_node_package_refs[0].resource_id == "capability-package://summarize/v1"
    assert package.function_body.max_nodes == 10
    assert package.function_body.max_live_nodes == 2
    assert package.package_digest


def test_orchestration_package_digest_includes_authorization_and_limits():
    left = _orchestration_package()
    right = _orchestration_package(max_nodes=11)

    assert left.package_digest != right.package_digest


@pytest.mark.parametrize(
    "overrides",
    [
        {"allowed_node_package_refs": []},
        {"max_nodes": 0},
        {"max_live_nodes": 0},
        {"max_live_nodes": 11},
        {"body": {"io_contract_ref": None}},
    ],
)
def test_orchestration_package_requires_valid_policy(overrides):
    with pytest.raises(ValidationError, match="orchestration_package_invalid"):
        _orchestration_package(**overrides)


def test_orchestration_fields_are_rejected_for_other_executors():
    with pytest.raises(ValidationError, match="orchestration_fields_require_orchestrator"):
        _orchestration_package(execution={"kind": "process:json_stdio", "version": "1"})


def test_node_intent_and_dynamic_node_round_trip():
    package_ref = _ref("capability-package://summarize/v1", "c" * 64)
    input_ref = _ref("content://sha256/" + "d" * 64, "d" * 64)
    intent = NodeIntent(
        intent_id="intent-1",
        execution_id="execution-1",
        package_ref=package_ref,
        input_refs=[input_ref],
    )
    node = DynamicNode(
        node_id="node-1",
        parent_execution_ref="execution-1",
        intent_id="intent-1",
        package_ref=package_ref,
        input_refs=[input_ref],
    )

    assert intent.model_dump(mode="json")["package_ref"]["version_or_digest"] == "c" * 64
    assert node.state == "accepted"
    assert node.model_dump(mode="json")["input_refs"][0]["resource_id"].startswith("content://sha256/")
