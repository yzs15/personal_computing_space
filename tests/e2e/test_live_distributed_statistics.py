"""Live end-to-end test for the multi-node distributed-statistics app.

Drives the running Observer (http://localhost:18080) over its public MCP
JSON-RPC transport, then verifies the map-reduce application executed across
both WorkspaceReplica Slaves (slave-a and slave-b) with a correct aggregate.
"""

import json
import math
import time
import importlib.util
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

OBSERVER_URL = "http://localhost:18080"
APP_DIR = Path(__file__).resolve().parents[2] / "examples" / "distributed-statistics"
SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "accept-bandgap.py"
SCRIPT_SPEC = importlib.util.spec_from_file_location("accept_bandgap", SCRIPT_PATH)
assert SCRIPT_SPEC is not None and SCRIPT_SPEC.loader is not None
SCRIPT_MODULE = importlib.util.module_from_spec(SCRIPT_SPEC)
SCRIPT_SPEC.loader.exec_module(SCRIPT_MODULE)
SlaveFailureController = SCRIPT_MODULE.SlaveFailureController


def test_failure_controller_stops_after_completed_and_running_nodes():
    commands: list[list[str]] = []
    controller = SlaveFailureController(
        docker_bin="docker",
        compose_file="deploy/docker-compose.yml",
        run_command=lambda command: commands.append(command),
    )

    controller.observe(
        {
            "execution_id": "execution-1",
            "execution_epoch": 1,
            "dynamic_nodes": [
                {"node_id": "node-complete", "state": "completed"},
                {"node_id": "node-running", "state": "dispatched"},
            ],
            "attempts": [
                {"node_id": "node-complete", "target": "slave-b", "state": "completed"},
                {"node_id": "node-running", "target": "slave-a", "state": "running"},
            ],
        }
    )
    controller.restore()

    assert commands == [
        ["docker", "compose", "-f", "deploy/docker-compose.yml", "stop", "slave-a"],
        ["docker", "compose", "-f", "deploy/docker-compose.yml", "start", "slave-a"],
    ]
    assert controller.execution_id == "execution-1"
    assert controller.execution_epoch == 1


def test_failure_controller_does_not_stop_before_partial_progress():
    commands: list[list[str]] = []
    controller = SlaveFailureController(
        docker_bin="docker",
        compose_file="deploy/docker-compose.yml",
        run_command=lambda command: commands.append(command),
    )

    controller.observe(
        {
            "dynamic_nodes": [{"node_id": "node-running", "state": "dispatched"}],
            "attempts": [{"node_id": "node-running", "target": "slave-a", "state": "created"}],
        }
    )
    controller.restore()

    assert commands == []


def _read(name: str) -> str:
    return (APP_DIR / name).read_text(encoding="utf-8")


def _mcp_call(
    client: httpx.Client,
    conversation_ref: str,
    name: str,
    arguments: dict,
    *,
    timeout: float = 30.0,
) -> dict:
    response = client.post(
        "/mcp",
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": name, "arguments": arguments}},
        headers={"X-Loom-Conversation-Ref": conversation_ref},
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    assert "result" in payload, payload
    result = payload["result"]
    text = (result.get("content") or [{}])[0].get("text") or "{}"
    value = json.loads(text)
    if result.get("isError"):
        raise RuntimeError(f"{name} failed: {value}")
    return value


def _put(client: httpx.Client, conversation_ref: str, content, media_type: str) -> dict:
    return _mcp_call(client, conversation_ref, "loom_put_content", {"content": content, "media_type": media_type})["resource_ref"]


def _get_run(client: httpx.Client, run_id: str) -> dict:
    response = client.get(f"/api/v1/runs/{run_id}", timeout=30.0)
    response.raise_for_status()
    return response.json()


@pytest.mark.e2e
def test_live_distributed_statistics_across_both_slaves():
    conversation_ref = f"e2e-distributed-statistics-{uuid4().hex[:10]}"
    with httpx.Client(base_url=OBSERVER_URL) as client:
        health = client.get("/healthz", timeout=10.0).json()
        assert health["ok"] is True

        # 1. Upload JSON Schema documents.
        parent_input_schema = _put(client, conversation_ref, json.loads(_read("io/parent.input.schema.json")), "application/schema+json")
        summarize_input_schema = _put(client, conversation_ref, json.loads(_read("io/summarize.input.schema.json")), "application/schema+json")
        summarize_output_schema = _put(client, conversation_ref, json.loads(_read("io/summarize.output.schema.json")), "application/schema+json")
        merge_input_schema = _put(client, conversation_ref, json.loads(_read("io/merge.input.schema.json")), "application/schema+json")
        merge_output_schema = _put(client, conversation_ref, json.loads(_read("io/merge.output.schema.json")), "application/schema+json")

        # 2. Upload io.v1 contract documents.
        def _contract(input_ref, output_ref) -> dict:
            return {
                "schema_version": "io.v1",
                "input_schema_ref": input_ref,
                "output_schema_ref": output_ref,
                "success_semantics": None,
                "success_validator_ref": None,
            }

        parent_contract = _put(client, conversation_ref, _contract(parent_input_schema, merge_output_schema), "application/vnd.loom.io-contract+json")
        summarize_contract = _put(client, conversation_ref, _contract(summarize_input_schema, summarize_output_schema), "application/vnd.loom.io-contract+json")
        merge_contract = _put(client, conversation_ref, _contract(merge_input_schema, merge_output_schema), "application/vnd.loom.io-contract+json")

        # 3. Upload capability package programs and the reference-based dataset.
        summarize_program = _put(client, conversation_ref, _read("programs/summarize.py"), "text/x-python")
        merge_program = _put(client, conversation_ref, _read("programs/merge.py"), "text/x-python")
        partition_refs = [
            _put(client, conversation_ref, json.loads(_read(f"data/partitions/p{i}.json")), "application/json")
            for i in range(1, 5)
        ]
        input_ref = _put(client, conversation_ref, {"partitions": partition_refs}, "application/json")

        # 4. Open the Run with the high-level closure contract.
        closure = json.loads(_read("closure.json"))
        closure["body"]["program"]["io_contract_ref"] = parent_contract
        opened = _mcp_call(client, conversation_ref, "loom_open_run", {"closure_contract": closure})
        run_id = opened["run_id"]

        # 5. Bind the execution payload and materialize the map/reduce packages.
        _mcp_call(
            client,
            conversation_ref,
            "loom_apply_plan_patch",
            {
                "ops": [
                    {"kind": "set_execution_payload", "value": {"node_id": "loom://orchestrate", "input_ref": input_ref}},
                    {
                        "kind": "materialize_capability_package_candidate",
                        "value": {
                            "package_id": "summarize",
                            "package_version": "v1",
                            "program_content_ref": summarize_program,
                            "io_contract_ref": summarize_contract,
                            "operation_descriptor_ref": "loom://summarize",
                        },
                    },
                    {
                        "kind": "materialize_capability_package_candidate",
                        "value": {
                            "package_id": "merge-summaries",
                            "package_version": "v1",
                            "program_content_ref": merge_program,
                            "io_contract_ref": merge_contract,
                            "operation_descriptor_ref": "loom://merge-summaries",
                        },
                    },
                ]
            },
        )
        packages = _mcp_call(client, conversation_ref, "loom_list_run_capability_packages", {})["packages"]
        summarize_package = next(item for item in packages if item["package_id"] == "summarize")
        merge_package = next(item for item in packages if item["package_id"] == "merge-summaries")
        summarize_ref = {"resource_id": f"capability-package://{summarize_package['package_id']}/{summarize_package['package_version']}", "version_or_digest": summarize_package["package_digest"]}
        merge_ref = {"resource_id": f"capability-package://{merge_package['package_id']}/{merge_package['package_version']}", "version_or_digest": merge_package["package_digest"]}

        # 6. Materialize the orchestration package (runs in Driver/Docker).
        orchestration_source = (
            _read("programs/orchestrate.py.tpl")
            .replace("SUMMARIZE_REF_PLACEHOLDER", json.dumps(summarize_ref))
            .replace("MERGE_REF_PLACEHOLDER", json.dumps(merge_ref))
        )
        orchestration_program = _put(client, conversation_ref, orchestration_source, "text/x-python")
        patched = _mcp_call(
            client,
            conversation_ref,
            "loom_apply_plan_patch",
            {
                "ops": [
                    {
                        "kind": "materialize_capability_package_candidate",
                        "value": {
                            "package_id": "orchestrate",
                            "package_version": "v1",
                            "program_content_ref": orchestration_program,
                            "io_contract_ref": parent_contract,
                            "operation_descriptor_ref": "loom://orchestrate",
                            "executor_kind": "orchestrator_python_v1",
                            "executor_operation": "orchestrate",
                            "allowed_node_package_refs": [summarize_ref, merge_ref],
                            "max_nodes": 6,
                            "max_live_nodes": 2,
                        },
                    }
                ]
            },
        )
        assert patched["readiness"]["ready"] is True, patched["readiness"].get("blockers")

        # 7. Commit, then start (which executes the orchestration end-to-end).
        committed = _mcp_call(client, conversation_ref, "loom_commit_plan", {})
        try:
            _mcp_call(client, conversation_ref, "loom_start_run", {"closure_version": committed["closure_version"]}, timeout=120.0)
        except Exception:
            # The Observer->Driver forward may time out before background
            # execution finishes; keep polling the persisted Run state.
            pass

        run = _poll_run(client, run_id, timeout=300.0)
        assert run["state"] == "completed", run.get("outcome")
        assert run["status"] == "completed"

        # 8. Verify multi-node distribution across both Slaves.
        accepted = [event for event in run["events"] if event.get("phase") in {"node_accepted", "node_reassigned"}]
        targets = {
            event.get("selected_target") or event.get("to_target") or event.get("target")
            for event in accepted
        }
        assert {"slave-a", "slave-b"} <= targets, f"expected both slaves, got {targets}"
        assert len(run["dynamic_nodes"]) == 5, len(run["dynamic_nodes"])

        summarize_version_ref = f"capability-package://{summarize_package['package_id']}/{summarize_package['package_version']}"
        merge_version_ref = f"capability-package://{merge_package['package_id']}/{merge_package['package_version']}"
        summarize_nodes = [node for node in run["dynamic_nodes"] if node["package_ref"]["resource_id"] == summarize_version_ref]
        merge_nodes = [node for node in run["dynamic_nodes"] if node["package_ref"]["resource_id"] == merge_version_ref]
        assert len(summarize_nodes) == 4
        assert len(merge_nodes) == 1
        assert all(node["state"] == "completed" for node in run["dynamic_nodes"])

        # 9. Verify the aggregate result is correct and content-addressed.
        resource_ref = run["outcome"]["resource_ref"]
        assert isinstance(resource_ref, dict) and resource_ref["resource_id"].startswith("content://sha256/")
        value = run["outcome"]["value"]
        assert value["count"] == 12
        assert value["sum"] == 57
        assert value["min"] == 0
        assert value["max"] == 10
        assert value["mean"] == pytest.approx(57 / 12)
        assert value["stddev"] == pytest.approx(math.sqrt(389 / 12 - (57 / 12) ** 2))

        # Cleanup: close the completed Run.
        client.post(f"/api/v1/runs/{run_id}/close", timeout=30.0)


def _poll_run(client: httpx.Client, run_id: str, *, timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run = _get_run(client, run_id)
        if run["state"] in {"completed", "awaiting_decision", "failed", "cancelled", "closed"}:
            return run
        time.sleep(2.0)
    raise TimeoutError(f"run {run_id} did not reach a terminal state")
