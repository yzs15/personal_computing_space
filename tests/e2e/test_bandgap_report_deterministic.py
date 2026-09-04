import httpx
import pytest
from datetime import datetime, timedelta, timezone
from loom_v2.driver.orchestration_runtime import DynamicOrchestrationRuntime

from loom_v2.contracts.types import ClosureContract, ResourceRef, TaskClosure
from loom_v2.driver.orchestrator import DockerOrchestrationExecutor
from loom_v2.driver.service import DriverService
from loom_v2.driver.worker import WorkerSession, WorkerUnavailableError
from loom_v2.observer.repository import ObserverRepository
from loom_v2.slave.app import create_app as create_slave_app


FAMILIES = ("perovskite", "rutile", "zincblende")
PARTITION_RECORDS = (
    (
        ("<sample><family>perovskite</family><e_gap>1.1</e_gap></sample>", 1.1),
        ("<sample><family>rutile</family><e_gap>1.2</e_gap></sample>", 1.2),
        ("<sample><family>zincblende</family><e_gap>1.3</e_gap></sample>", 1.3),
    ),
    (
        ("<sample><family>perovskite</family><e_gap>2.1</e_gap></sample>", 2.1),
        ("<sample><family>rutile</family><e_gap>2.2</e_gap></sample>", 2.2),
        ("<sample><family>zincblende</family><e_gap>2.3</e_gap></sample>", 2.3),
    ),
)


PARSE_PROGRAM = """
import json
import sys
import xml.etree.ElementTree

document = json.load(sys.stdin)
items = []
for xml_text in document["files"]:
    root = xml.etree.ElementTree.fromstring(xml_text)
    items.append({"family": root.findtext("family"), "e_gap": float(root.findtext("e_gap"))})
print(json.dumps({"items": items}))
"""

SUMMARIZE_PROGRAM = """
import json
import math
import sys

items = json.load(sys.stdin)["items"]
values = [item["e_gap"] for item in items]
count = len(values)
total = sum(values)
mean = total / count
variance = max(0.0, sum(value * value for value in values) / count - mean * mean)
by_family = {}
for family in sorted({item["family"] for item in items}):
    family_values = [item["e_gap"] for item in items if item["family"] == family]
    family_count = len(family_values)
    family_total = sum(family_values)
    family_mean = family_total / family_count
    family_variance = max(
        0.0,
        sum(value * value for value in family_values) / family_count - family_mean * family_mean,
    )
    by_family[family] = {
        "count": family_count,
        "sum": family_total,
        "min": min(family_values),
        "max": max(family_values),
        "mean": family_mean,
        "stddev": math.sqrt(family_variance),
    }
print(
    json.dumps(
        {
            "count": count,
            "sum": total,
            "min": min(values),
            "max": max(values),
            "mean": mean,
            "stddev": math.sqrt(variance),
            "by_family": by_family,
        }
    )
)
"""

MERGE_PROGRAM = """
import json
import math
import sys

summaries = json.load(sys.stdin)["inputs"]
values = [value for summary in summaries for value in (summary["min"], summary["max"])]
count = sum(summary["count"] for summary in summaries)
total = sum(summary["sum"] for summary in summaries)
mean = total / count
sum_squares = sum(summary["count"] * (summary["stddev"] ** 2 + summary["mean"] ** 2) for summary in summaries)
variance = max(0.0, sum_squares / count - mean * mean)
by_family = {}
for summary in summaries:
    for family, stats in summary["by_family"].items():
        accumulator = by_family.setdefault(
            family,
            {"count": 0, "sum": 0.0, "sum_squares": 0.0, "min": None, "max": None},
        )
        accumulator["count"] += stats["count"]
        accumulator["sum"] += stats["sum"]
        accumulator["sum_squares"] += stats["count"] * (stats["stddev"] ** 2 + stats["mean"] ** 2)
        accumulator["min"] = stats["min"] if accumulator["min"] is None else min(accumulator["min"], stats["min"])
        accumulator["max"] = stats["max"] if accumulator["max"] is None else max(accumulator["max"], stats["max"])
merged_families = {}
for family, accumulator in by_family.items():
    family_count = accumulator["count"]
    family_total = accumulator["sum"]
    family_mean = family_total / family_count
    family_variance = max(0.0, accumulator["sum_squares"] / family_count - family_mean * family_mean)
    merged_families[family] = {
        "count": family_count,
        "sum": family_total,
        "min": accumulator["min"],
        "max": accumulator["max"],
        "mean": family_mean,
        "stddev": math.sqrt(family_variance),
    }
print(
    json.dumps(
        {
            "overall": {
                "count": count,
                "sum": total,
                "min": min(values),
                "max": max(values),
                "mean": mean,
                "stddev": math.sqrt(variance),
            },
            "by_family": merged_families,
        }
    )
)
"""


async def _put_content(repo: ObserverRepository, value: object, media_type: str) -> ResourceRef:
    return await repo.put_content(value, media_type=media_type)


def _contract(input_ref: ResourceRef | None, output_ref: ResourceRef | None) -> dict:
    return {
        "schema_version": "io.v1",
        "input_schema_ref": input_ref.model_dump(mode="json") if input_ref else None,
        "output_schema_ref": output_ref.model_dump(mode="json") if output_ref else None,
        "success_semantics": None,
        "success_validator_ref": None,
    }


@pytest.mark.asyncio
async def test_deterministic_bandgap_report_executes_parse_summarize_merge_and_replays():
    repo = ObserverRepository()
    parent_input_schema = await _put_content(repo, {"type": "object", "required": ["partitions"]}, "application/schema+json")
    parse_input_schema = await _put_content(repo, {"type": "object", "required": ["files"]}, "application/schema+json")
    parsed_output_schema = await _put_content(repo, {"type": "object", "required": ["items"]}, "application/schema+json")
    summary_output_schema = await _put_content(
        repo,
        {
            "type": "object",
            "required": ["count", "sum", "min", "max", "mean", "stddev", "by_family"],
        },
        "application/schema+json",
    )
    merge_input_schema = await _put_content(
        repo,
        {"type": "object", "required": ["inputs"]},
        "application/schema+json",
    )
    merge_output_schema = await _put_content(
        repo,
        {"type": "object", "required": ["overall", "by_family"]},
        "application/schema+json",
    )

    parent_contract = await _put_content(
        repo,
        _contract(parent_input_schema, merge_output_schema),
        "application/vnd.loom.io-contract+json",
    )
    parse_contract = await _put_content(
        repo,
        _contract(parse_input_schema, parsed_output_schema),
        "application/vnd.loom.io-contract+json",
    )
    summarize_contract = await _put_content(
        repo,
        _contract(parsed_output_schema, summary_output_schema),
        "application/vnd.loom.io-contract+json",
    )
    merge_contract = await _put_content(
        repo,
        _contract(merge_input_schema, merge_output_schema),
        "application/vnd.loom.io-contract+json",
    )

    parse_program = await _put_content(repo, PARSE_PROGRAM, "text/x-python")
    summarize_program = await _put_content(repo, SUMMARIZE_PROGRAM, "text/x-python")
    merge_program = await _put_content(repo, MERGE_PROGRAM, "text/x-python")
    partition_refs = [
        await _put_content(
            repo,
            {"files": [record for record, _e_gap in partition]},
            "application/json",
        )
        for partition in PARTITION_RECORDS
    ]
    parent_input = await _put_content(
        repo,
        {"partitions": [ref.model_dump(mode="json") for ref in partition_refs]},
        "application/json",
    )
    closure = ClosureContract(
        closure_id="closure-bandgap-deterministic",
        goal="per-family band-gap statistics",
        body=TaskClosure(
            closure_id="closure-bandgap-deterministic",
            program={"operation_ref": "loom://orchestrate", "io_contract_ref": parent_contract.model_dump(mode="json")},
        ),
    )
    run = await repo.open_run(
        "run-bandgap-deterministic",
        "conversation-bandgap-deterministic",
        "per-family band-gap statistics",
        closure_contract=closure,
    )
    patched = await repo.apply_patch(
        run.run_id,
        run.draft_version,
        run.draft_digest,
        "materialize-bandgap-packages",
        [
            {"kind": "set_execution_payload", "value": {"node_id": "loom://orchestrate", "input_ref": parent_input.model_dump(mode="json")}},
            {
                "kind": "materialize_capability_package_candidate",
                "value": {
                    "package_id": "df_xml_parse",
                    "package_version": "v1",
                    "program_content_ref": parse_program.model_dump(mode="json"),
                    "io_contract_ref": parse_contract.model_dump(mode="json"),
                    "operation_descriptor_ref": "loom://df_xml_parse",
                },
            },
            {
                "kind": "materialize_capability_package_candidate",
                "value": {
                    "package_id": "summarize_bandgap",
                    "package_version": "v1",
                    "program_content_ref": summarize_program.model_dump(mode="json"),
                    "io_contract_ref": summarize_contract.model_dump(mode="json"),
                    "operation_descriptor_ref": "loom://summarize_bandgap",
                },
            },
            {
                "kind": "materialize_capability_package_candidate",
                "value": {
                    "package_id": "merge_bandgap_report",
                    "package_version": "v1",
                    "program_content_ref": merge_program.model_dump(mode="json"),
                    "io_contract_ref": merge_contract.model_dump(mode="json"),
                    "operation_descriptor_ref": "loom://merge_bandgap_report",
                },
            },
        ],
    )
    packages = (await repo.get_run(run.run_id)).capability_packages
    package_refs = {
        package.package_id: ResourceRef(resource_id=package.version_ref, version_or_digest=package.package_digest)
        for package in packages
    }
    orchestration_source = f"""
PARSE = {package_refs["df_xml_parse"].model_dump(mode="json")}
SUMMARIZE = {package_refs["summarize_bandgap"].model_dump(mode="json")}
MERGE = {package_refs["merge_bandgap_report"].model_dump(mode="json")}


async def orchestrate(ctx: "OrchestrationContext", input_ref: "ResourceRef") -> "ResourceRef":
    document = await ctx.read_json(input_ref)
    parse_handles = [ctx.emit_node(PARSE, [partition]) for partition in document["partitions"]]
    parsed = [await ctx.result(handle) for handle in parse_handles]
    summary_handles = [ctx.emit_node(SUMMARIZE, [parsed_ref]) for parsed_ref in parsed]
    summaries = [await ctx.result(handle) for handle in summary_handles]
    merged = ctx.emit_node(MERGE, summaries)
    return await ctx.result(merged)
"""
    orchestration_program = await _put_content(repo, orchestration_source, "text/x-python")
    final_patch = await repo.apply_patch(
        run.run_id,
        patched.draft_version,
        patched.draft_digest,
        "materialize-bandgap-orchestration",
        [
            {
                "kind": "materialize_capability_package_candidate",
                "value": {
                    "package_id": "orchestrate_bandgap",
                    "package_version": "v1",
                    "program_content_ref": orchestration_program.model_dump(mode="json"),
                    "io_contract_ref": parent_contract.model_dump(mode="json"),
                    "operation_descriptor_ref": "loom://orchestrate",
                    "executor_kind": "orchestrator_python_v1",
                    "executor_operation": "orchestrate",
                    "allowed_node_package_refs": [
                        package_refs["df_xml_parse"].model_dump(mode="json"),
                        package_refs["summarize_bandgap"].model_dump(mode="json"),
                        package_refs["merge_bandgap_report"].model_dump(mode="json"),
                    ],
                    "max_nodes": 6,
                    "max_live_nodes": 2,
                },
            },
        ],
    )
    assert final_patch.readiness["ready"] is True
    committed = await repo.commit(run.run_id, final_patch.draft_version, final_patch.draft_digest)
    started = await repo.start(run.run_id, committed.version_id)
    driver = DriverService(
        repo,
        provider=object(),
        workers={
            "slave-a": WorkerSession("slave-a", "http://slave-a", transport=httpx.ASGITransport(app=create_slave_app("slave-a"))),
            "slave-b": WorkerSession("slave-b", "http://slave-b", transport=httpx.ASGITransport(app=create_slave_app("slave-b"))),
        },
        orchestration_executor=DockerOrchestrationExecutor(image="python:3.12-slim"),
    )
    completed, result = await driver._dispatch_execution(run.run_id, "per-family band-gap statistics")

    assert completed.state == "completed"
    assert result.value["overall"]["count"] == 6
    assert result.value["overall"]["sum"] == pytest.approx(10.2)
    assert result.value["overall"]["min"] == pytest.approx(1.1)
    assert result.value["overall"]["max"] == pytest.approx(2.3)
    assert result.value["overall"]["mean"] == pytest.approx(1.7)
    assert result.value["overall"]["stddev"] == pytest.approx(0.506623)
    for family, expected_mean in zip(FAMILIES, (1.6, 1.7, 1.8), strict=True):
        assert result.value["by_family"][family]["count"] == 2
        assert result.value["by_family"][family]["mean"] == pytest.approx(expected_mean)
        assert result.value["by_family"][family]["stddev"] == pytest.approx(0.5)

    record = await repo.get_run(run.run_id)
    assert record.execution_id == started["execution_id"]
    assert len(record.dynamic_nodes) == 5
    assert all(node.state == "completed" for node in record.dynamic_nodes)
    assert result.resource_ref.resource_id.startswith("content://sha256/")
    assert all(package.scope == "run_bound" for package in record.capability_packages)
    accepted_ids = {event["node_id"] for event in record.events if event["phase"] == "node_accepted"}
    assert accepted_ids == {node.node_id for node in record.dynamic_nodes}

    replay_record = await repo.get_run(run.run_id)
    replay_record.state = "running"
    replay_record.outcome = None
    await repo._persist(replay_record)
    attempts_before_replay = list(record.attempts)
    replayed, replay_result = await driver._dispatch_execution(run.run_id, "per-family band-gap statistics")

    assert replayed.state == "completed"
    assert replay_result.resource_ref == result.resource_ref
    assert (await repo.get_run(run.run_id)).attempts == attempts_before_replay


@pytest.mark.asyncio
async def test_parse_package_promotion_and_abandon_are_explicit_user_decisions():
    repo = ObserverRepository()
    contract_ref = await _put_content(
        repo,
        _contract(None, None),
        "application/vnd.loom.io-contract+json",
    )
    program_ref = await _put_content(repo, PARSE_PROGRAM, "text/x-python")
    candidates = []
    for index, terminal_action in enumerate(("promote", "abandon")):
        run = await repo.open_run(
            f"run-bandgap-{terminal_action}",
            f"conversation-bandgap-{terminal_action}",
            "parse DFT output",
        )
        await repo.apply_patch(
            run.run_id,
            run.draft_version,
            run.draft_digest,
            f"materialize-df-xml-parse-{index}",
            [
                {
                    "kind": "materialize_capability_package_candidate",
                    "value": {
                        "package_id": f"df_xml_parse_{terminal_action}",
                        "package_version": "v1",
                        "program_content_ref": program_ref.model_dump(mode="json"),
                        "io_contract_ref": contract_ref.model_dump(mode="json"),
                        "operation_descriptor_ref": "loom://df_xml_parse",
                    },
                },
            ],
        )
        await repo.fail_run(run.run_id, "test-terminal")
        record = await repo.get_run(run.run_id)
        assert record.state == "failed"
        candidates.append(record.capability_packages[0])

    promote_candidate, abandon_candidate = candidates
    reusable = await repo.promote_capability_package(
        promote_candidate.version_ref,
        approved_digest=promote_candidate.package_digest,
    )
    abandoned = await repo.abandon_capability_package(abandon_candidate.version_ref)
    visible_promoted = await repo.list_capability_packages(run_id="run-bandgap-promote")
    visible_abandoned = await repo.list_capability_packages(run_id="run-bandgap-abandon")

    assert promote_candidate.scope == "run_bound"
    assert promote_candidate.publication_state == "candidate"
    assert reusable.scope == "workspace_reusable"
    assert reusable.publication_state == "published"
    assert any(package.package_id == reusable.package_id for package in visible_promoted)
    assert abandon_candidate.scope == "run_bound"
    assert abandoned.publication_state == "abandoned"
    assert visible_abandoned == []


@pytest.mark.asyncio
async def test_lost_slave_a_attempt_is_reassigned_to_slave_b():
    repo = ObserverRepository()
    input_schema = await _put_content(repo, {"type": "object"}, "application/schema+json")
    contract = await _put_content(repo, _contract(input_schema, None), "application/vnd.loom.io-contract+json")
    orchestration_program = await _put_content(
        repo,
        'async def orchestrate(ctx: "OrchestrationContext", input_ref: "ResourceRef") -> "ResourceRef":\n    return input_ref\n',
        "text/x-python",
    )
    node_program = await _put_content(repo, 'import json; print(json.dumps({"ok": True}))', "text/x-python")
    parent_input = await _put_content(repo, {"ok": True}, "application/json")
    closure = ClosureContract(
        closure_id="closure-disabled-slave",
        goal="dynamic availability probe",
        recovery_policy={"allow_reassignment": True},
        body=TaskClosure(
            closure_id="closure-disabled-slave",
            program={"operation_ref": "loom://orchestrate", "io_contract_ref": contract.model_dump(mode="json")},
        ),
    )
    run = await repo.open_run(
        "run-disabled-slave",
        "conversation-disabled-slave",
        "dynamic availability probe",
        allow_reassignment=True,
        closure_contract=closure,
    )
    patched = await repo.apply_patch(
        run.run_id,
        run.draft_version,
        run.draft_digest,
        "materialize-availability-probe",
        [
            {"kind": "set_execution_payload", "value": {"node_id": "loom://orchestrate", "input_ref": parent_input.model_dump(mode="json")}},
            {
                "kind": "materialize_capability_package_candidate",
                "value": {
                    "package_id": "probe",
                    "package_version": "v1",
                    "program_content_ref": node_program.model_dump(mode="json"),
                    "io_contract_ref": contract.model_dump(mode="json"),
                    "operation_descriptor_ref": "loom://probe",
                },
            },
        ],
    )
    node_package = next(package for package in (await repo.get_run(run.run_id)).capability_packages if package.package_id == "probe")
    node_ref = ResourceRef(resource_id=node_package.version_ref, version_or_digest=node_package.package_digest)
    orchestration_patch = await repo.apply_patch(
        run.run_id,
        patched.draft_version,
        patched.draft_digest,
        "materialize-availability-orchestration",
        [
            {
                "kind": "materialize_capability_package_candidate",
                "value": {
                    "package_id": "orchestrate_probe",
                    "package_version": "v1",
                    "program_content_ref": orchestration_program.model_dump(mode="json"),
                    "io_contract_ref": contract.model_dump(mode="json"),
                    "operation_descriptor_ref": "loom://orchestrate",
                    "executor_kind": "orchestrator_python_v1",
                    "executor_operation": "orchestrate",
                    "allowed_node_package_refs": [node_ref.model_dump(mode="json")],
                    "max_nodes": 1,
                    "max_live_nodes": 1,
                },
            },
        ],
    )
    assert orchestration_patch.readiness["ready"] is True
    committed = await repo.commit(run.run_id, orchestration_patch.draft_version, orchestration_patch.draft_digest)
    await repo.start(run.run_id, committed.version_id)

    class SingleNodeExecutor:
        async def run(self, _program, _input_ref, *, read_json, emit_node, result):
            handle = await emit_node(node_ref, [parent_input])
            return await result(handle)

    class LosingWorker:
        def __init__(self, delegate):
            self.delegate = delegate
            self.operation_timeout = delegate.operation_timeout

        async def provision(self, **arguments):
            return await self.delegate.provision(**arguments)

        async def dispatch(self, **arguments):
            key = next(
                key
                for key in repo.agents
                if key[1:3] == ("slave", "slave-a") and repo.agents[key].get("lease_state") == "active"
            )
            repo.agents[key]["last_seen_at"] = datetime.now(timezone.utc) - timedelta(seconds=60)
            await repo.refresh_slaves("workspace-default")
            raise WorkerUnavailableError("worker_unavailable")

    slave_a = WorkerSession(
        "slave-a",
        "http://slave-a",
        transport=httpx.ASGITransport(app=create_slave_app("slave-a")),
    )

    runtime = DynamicOrchestrationRuntime(
        repository=repo,
        executor=SingleNodeExecutor(),
        workers={
            "slave-a": LosingWorker(slave_a),
            "slave-b": WorkerSession("slave-b", "http://slave-b", transport=httpx.ASGITransport(app=create_slave_app("slave-b"))),
        },
    )
    completed, _result = await runtime.run(run.run_id)
    record = await repo.get_run(run.run_id)

    assert completed.state == "completed"
    assert [attempt["state"] for attempt in record.attempts] == ["lost", "completed"]
    assert [attempt["target"] for attempt in record.attempts] == ["slave-a", "slave-b"]
    assert record.execution_epoch == 1
