#!/usr/bin/env python3
"""Live acceptance test: band-gap statistics report (compute-service-desk scenario).

Targets a running Loom Observer over HTTP (default http://localhost:18080).

Modes:
  mcp   -- drive the scenario through the public MCP tool surface directly.
           This validates the band-gap scenario on the live infrastructure
           (Observer/Driver/Slaves/MinIO/orchestration) without an LLM.
  agent -- seed the corpus, then send ONE natural-language message and let the
           real Codex coding-agent refine and execute end-to-end (G1, the
           discriminating gate). No manual `loom_*` calls in this mode.

Gates (per docs/superpowers/specs/2026-09-02-compute-service-desk-acceptance-design.md):
  G1  real-agent refinement        (agent mode only)
  G2  capability-gap closure       (>=1 run_bound candidate package materialized)
  G3  statistical correctness      (report == independent ground truth)
  G5  provenance                   (outcome content-addressed; node/attempt trace)
G4 (slave failure injection) is off by default and available via --inject-failure.

Exit code 0 if all enabled gates pass, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from typing import Any, Callable
from urllib.parse import urljoin


DEFAULT_OBSERVER = "http://localhost:18080"
WORKSPACE_ID = "workspace-default"
FAMILIES = ["perovskite", "rutile", "zincblende", "wurtzite", "rocksalt"]


class SlaveFailureController:
    def __init__(
        self,
        *,
        docker_bin: str,
        compose_file: str,
        run_command: Callable[[list[str]], Any] | None = None,
    ) -> None:
        self.docker_bin = docker_bin
        self.compose_file = compose_file
        self.run_command = run_command or self._run_command
        self.injected = False
        self.stopped = False
        self.execution_id: str | None = None
        self.execution_epoch: int | None = None

    @staticmethod
    def _run_command(command: list[str]) -> None:
        subprocess.run(command, check=True)

    def observe(self, run: dict[str, Any]) -> None:
        if self.execution_id is None and run.get("execution_id"):
            self.execution_id = str(run["execution_id"])
            self.execution_epoch = int(run.get("execution_epoch", 1))
        if self.injected:
            return
        completed_node_ids = {
            str(node.get("node_id"))
            for node in run.get("dynamic_nodes") or []
            if node.get("state") == "completed"
        }
        active_slave_a = any(
            attempt.get("target") == "slave-a"
            and attempt.get("state") in {"created", "running"}
            and str(attempt.get("node_id")) not in completed_node_ids
            for attempt in run.get("attempts") or []
        )
        if not completed_node_ids or not active_slave_a:
            return
        self.run_command(
            [self.docker_bin, "compose", "-f", self.compose_file, "stop", "slave-a"]
        )
        self.injected = True
        self.stopped = True

    def restore(self) -> None:
        if not self.stopped:
            return
        self.run_command(
            [self.docker_bin, "compose", "-f", self.compose_file, "start", "slave-a"]
        )
        self.stopped = False


# --------------------------------------------------------------------------- helpers

def _request(
    method: str,
    url: str,
    payload: Any = None,
    headers: dict[str, str] | None = None,
    timeout: float = 30.0,
) -> tuple[int, bytes]:
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def _json(method: str, url: str, payload: Any = None, **kw: Any) -> Any:
    status, body = _request(method, url, payload, **kw)
    if status >= 400:
        raise RuntimeError(f"HTTP {status} on {method} {url}: {body[:300]!r}")
    return json.loads(body) if body else None


def mcp_call(observer: str, conversation_ref: str, name: str, arguments: dict, timeout: float = 60.0) -> dict:
    status, body = _request(
        "POST",
        urljoin(observer, "/mcp"),
        {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": name, "arguments": arguments}},
        headers={"X-Loom-Conversation-Ref": conversation_ref},
        timeout=timeout,
    )
    if status >= 400:
        raise RuntimeError(f"MCP HTTP {status} for {name}: {body[:300]!r}")
    payload = json.loads(body)
    result = payload.get("result") or {}
    text = (result.get("content") or [{}])[0].get("text") or "{}"
    value = json.loads(text)
    if result.get("isError"):
        raise RuntimeError(f"{name} failed: {value}")
    return value


def put_content(observer: str, conversation_ref: str, content: Any, media_type: str) -> dict:
    return mcp_call(observer, conversation_ref, "loom_put_content", {"content": content, "media_type": media_type})["resource_ref"]


def get_run(observer: str, run_id: str) -> dict:
    return _json("GET", urljoin(observer, f"/api/v1/runs/{run_id}"), timeout=30.0)


def get_conversation(observer: str, conversation_ref: str) -> dict:
    return _json("GET", urljoin(observer, f"/api/v1/conversations/{conversation_ref}"), timeout=30.0)


def fetch_content(observer: str, resource_id: str) -> Any:
    if not resource_id.startswith("content://sha256/"):
        return None
    digest = resource_id.split("/")[-1]
    status, body = _request("GET", urljoin(observer, f"/api/v1/content/{digest}"), timeout=30.0)
    if status != 200:
        raise RuntimeError(f"content fetch failed: HTTP {status}")
    try:
        return json.loads(body)
    except (ValueError, TypeError):
        return body.decode("utf-8", errors="replace")


# --------------------------------------------------------------------------- corpus

def make_corpus(seed: int, n: int) -> list[dict]:
    rng = random.Random(seed)
    items = []
    for i in range(n):
        family = FAMILIES[i % len(FAMILIES)]
        base = 1.0 + (i // len(FAMILIES)) * 0.3
        e_gap = round(base + rng.uniform(-0.4, 0.4), 3)
        items.append(
            {
                "sample": f"sample-{i:04d}",
                "family": family,
                "e_gap": e_gap,
                "functional": "PBE",
                "kpoints": 8,
            }
        )
    return items


def _stats(values: list[float]) -> dict:
    n = len(values)
    s = sum(values)
    sq = sum(x * x for x in values)
    mean = s / n
    var = max(0.0, sq / n - mean * mean)
    return {
        "count": n,
        "sum": round(s, 6),
        "min": round(min(values), 6),
        "max": round(max(values), 6),
        "mean": round(mean, 6),
        "stddev": round(math.sqrt(var), 6),
    }


def ground_truth(items: list[dict]) -> dict:
    overall = _stats([item["e_gap"] for item in items])
    by_family = {}
    for family in FAMILIES:
        values = [item["e_gap"] for item in items if item["family"] == family]
        if values:
            by_family[family] = _stats(values)
    outliers = []
    if overall["stddev"] > 0:
        for item in items:
            if abs(item["e_gap"] - overall["mean"]) > 3 * overall["stddev"]:
                outliers.append(item["sample"])
    return {"overall": overall, "by_family": by_family, "outliers": outliers}


# --------------------------------------------------------------------------- programs (mcp mode)

SUMMARIZE_PROGRAM = (
    "import json,sys,time\n"
    "d=json.load(sys.stdin)\n"
    "items=d[\"items\"]\n"
    "first_index=int(items[0][\"sample\"].rsplit(\"-\",1)[-1]) if items else 0\n"
    "time.sleep(3.0 if first_index % 2 == 0 else 0.5)\n"
    "def st(xs):\n"
    "  n=len(xs); s=sum(xs); sq=sum(x*x for x in xs); m=s/n\n"
    "  return {\"count\":n,\"sum\":round(s,6),\"min\":round(min(xs),6),\"max\":round(max(xs),6),\"mean\":round(m,6),\"stddev\":round(max(0.0,sq/n-m*m)**0.5,6)}\n"
    "vals=[x[\"e_gap\"] for x in items]\n"
    "by={}\n"
    "for x in items:\n"
    "  by.setdefault(x[\"family\"],[]).append(x[\"e_gap\"])\n"
    "print(json.dumps({\"count\":len(vals),\"sum\":round(sum(vals),6),\"min\":round(min(vals),6),\"max\":round(max(vals),6),\"mean\":round(sum(vals)/len(vals),6),\"stddev\":round(max(0.0,sum(v*v for v in vals)/len(vals)-(sum(vals)/len(vals))**2)**0.5,6),\"by_family\":{k:st(v) for k,v in by.items()}}))\n"
)

MERGE_PROGRAM = (
    "import json,sys,math\n"
    "d=json.load(sys.stdin)\n"
    "ins=d[\"inputs\"]\n"
    "def combine(groups):\n"
    "  vals=[v for g in groups for v in g]\n"
    "  return {\"count\":len(vals),\"sum\":round(sum(vals),6),\"min\":round(min(vals),6),\"max\":round(max(vals),6),\"mean\":round(sum(vals)/len(vals),6),\"stddev\":round(max(0.0,sum(v*v for v in vals)/len(vals)-(sum(vals)/len(vals))**2)**0.5,6)}\n"
    "allv=[v for it in ins for v in [it[\"min\"],it[\"max\"]]]\n"
    "count=sum(it[\"count\"] for it in ins)\n"
    "total=sum(it[\"sum\"] for it in ins)\n"
    "sq=sum(it[\"count\"]*(it[\"stddev\"]**2+it[\"mean\"]**2) for it in ins)\n"
    "mean=total/count\n"
    "by={}\n"
    "for it in ins:\n"
    "  for k,v in it[\"by_family\"].items():\n"
    "    by.setdefault(k,{\"count\":0,\"sum\":0.0,\"min\":None,\"max\":None,\"sumsq\":0.0})\n"
    "    g=by[k]\n"
    "    g[\"count\"]+=v[\"count\"]; g[\"sum\"]+=v[\"sum\"]; g[\"sumsq\"]+=v[\"count\"]*(v[\"stddev\"]**2+v[\"mean\"]**2)\n"
    "    g[\"min\"]=v[\"min\"] if g[\"min\"] is None else min(g[\"min\"],v[\"min\"])\n"
    "    g[\"max\"]=v[\"max\"] if g[\"max\"] is None else max(g[\"max\"],v[\"max\"])\n"
    "byf={k:{\"count\":g[\"count\"],\"sum\":round(g[\"sum\"],6),\"min\":g[\"min\"],\"max\":g[\"max\"],\"mean\":round(g[\"sum\"]/g[\"count\"],6),\"stddev\":round(max(0.0,g[\"sumsq\"]/g[\"count\"]-(g[\"sum\"]/g[\"count\"])**2)**0.5,6)} for k,g in by.items()}\n"
    "overall={\"count\":count,\"sum\":round(total,6),\"min\":min(it[\"min\"] for it in ins),\"max\":max(it[\"max\"] for it in ins),\"mean\":round(mean,6),\"stddev\":round(max(0.0,sq/count-mean*mean)**0.5,6)}\n"
    "print(json.dumps({\"overall\":overall,\"by_family\":byf}))\n"
)


def orchestration_source(summarize_ref: dict, merge_ref: dict) -> str:
    return (
        "SUMMARIZE = " + json.dumps(summarize_ref) + "\n"
        "MERGE = " + json.dumps(merge_ref) + "\n"
        'async def orchestrate(ctx: "OrchestrationContext", input_ref: "ResourceRef") -> "ResourceRef":\n'
        "    document = await ctx.read_json(input_ref)\n"
        "    handles = [ctx.emit_node(SUMMARIZE, [partition]) for partition in document[\"partitions\"]]\n"
        "    summaries = [await ctx.result(handle) for handle in handles]\n"
        "    merged = ctx.emit_node(MERGE, summaries)\n"
        "    return await ctx.result(merged)\n"
    )


# --------------------------------------------------------------------------- mcp mode

def run_mcp_mode(
    observer: str,
    corpus: list[dict],
    partitions: int,
    *,
    failure_controller: SlaveFailureController | None = None,
) -> tuple[dict, list[str]]:
    passed: list[str] = []
    conversation_ref = f"accept-bandgap-mcp-{uuid.uuid4().hex[:10]}"

    parent_input_schema = put_content(observer, conversation_ref, {"type": "object", "required": ["partitions"]}, "application/schema+json")
    sum_in_schema = put_content(observer, conversation_ref, {"type": "object", "required": ["items"]}, "application/schema+json")
    sum_out_schema = put_content(
        observer,
        conversation_ref,
        {"type": "object", "required": ["count", "by_family"]},
        "application/schema+json",
    )
    merge_in_schema = put_content(observer, conversation_ref, {"type": "object", "required": ["inputs"]}, "application/schema+json")
    merge_out_schema = put_content(
        observer,
        conversation_ref,
        {"type": "object", "required": ["overall", "by_family"]},
        "application/schema+json",
    )

    def contract(input_ref: dict, output_ref: dict) -> dict:
        return {"schema_version": "io.v1", "input_schema_ref": input_ref, "output_schema_ref": output_ref, "success_semantics": None, "success_validator_ref": None}

    parent_contract = put_content(observer, conversation_ref, contract(parent_input_schema, merge_out_schema), "application/vnd.loom.io-contract+json")
    summarize_contract = put_content(observer, conversation_ref, contract(sum_in_schema, sum_out_schema), "application/vnd.loom.io-contract+json")
    merge_contract = put_content(observer, conversation_ref, contract(merge_in_schema, merge_out_schema), "application/vnd.loom.io-contract+json")

    summarize_program = put_content(observer, conversation_ref, SUMMARIZE_PROGRAM, "text/x-python")
    merge_program = put_content(observer, conversation_ref, MERGE_PROGRAM, "text/x-python")

    chunks = [corpus[i::partitions] for i in range(partitions)]
    partitions_payload = [{"items": chunk} for chunk in chunks]
    partition_refs = [put_content(observer, conversation_ref, part, "application/json") for part in partitions_payload]
    input_ref = put_content(observer, conversation_ref, {"partitions": partition_refs}, "application/json")

    closure = {
        "closure_id": "closure-bandgap-mcp",
        "goal": "per-family band-gap statistics",
        "resource_budget": {"max_nodes": 8, "max_live_nodes": 2},
        "recovery_policy": {"allow_reassignment": failure_controller is not None},
        "body": {
            "closure_id": "closure-bandgap-mcp",
            "program": {"operation_ref": "loom://orchestrate", "io_contract_ref": parent_contract},
        },
    }
    opened = mcp_call(observer, conversation_ref, "loom_open_run", {"closure_contract": closure})
    run_id = opened["run_id"]

    mcp_call(
        observer,
        conversation_ref,
        "loom_apply_plan_patch",
        {
            "ops": [
                {"kind": "set_execution_payload", "value": {"node_id": "loom://orchestrate", "input_ref": input_ref}},
                {
                    "kind": "materialize_capability_package_candidate",
                    "value": {
                        "package_id": "summarize-bandgap",
                        "package_version": "v1",
                        "program_content_ref": summarize_program,
                        "io_contract_ref": summarize_contract,
                        "operation_descriptor_ref": "loom://summarize-bandgap",
                    },
                },
                {
                    "kind": "materialize_capability_package_candidate",
                    "value": {
                        "package_id": "merge-bandgap",
                        "package_version": "v1",
                        "program_content_ref": merge_program,
                        "io_contract_ref": merge_contract,
                        "operation_descriptor_ref": "loom://merge-bandgap",
                    },
                },
            ]
        },
    )
    packages = mcp_call(observer, conversation_ref, "loom_list_run_capability_packages", {})["packages"]
    summarize_package = next(item for item in packages if item["package_id"] == "summarize-bandgap")
    merge_package = next(item for item in packages if item["package_id"] == "merge-bandgap")
    summarize_ref = {"resource_id": f"capability-package://{summarize_package['package_id']}/{summarize_package['package_version']}", "version_or_digest": summarize_package["package_digest"]}
    merge_ref = {"resource_id": f"capability-package://{merge_package['package_id']}/{merge_package['package_version']}", "version_or_digest": merge_package["package_digest"]}

    orchestration_program = put_content(observer, conversation_ref, orchestration_source(summarize_ref, merge_ref), "text/x-python")
    patched = mcp_call(
        observer,
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
                        "max_nodes": 8,
                        "max_live_nodes": 2,
                    },
                }
            ]
        },
    )
    if not patched["readiness"]["ready"]:
        raise RuntimeError(f"readiness not ready: {patched['readiness'].get('blockers')}")
    passed.append("G2(part): materialized run_bound candidate packages")

    committed = mcp_call(observer, conversation_ref, "loom_commit_plan", {})
    monitor_stop = threading.Event()
    monitor_errors: list[BaseException] = []
    monitor_thread: threading.Thread | None = None
    if failure_controller is not None:
        def monitor() -> None:
            while not monitor_stop.wait(0.1):
                try:
                    current = get_run(observer, run_id)
                except Exception:
                    continue
                try:
                    failure_controller.observe(current)
                except BaseException as exc:
                    monitor_errors.append(exc)
                    return

        monitor_thread = threading.Thread(target=monitor, name="slave-failure-monitor", daemon=True)
        monitor_thread.start()
    try:
        try:
            mcp_call(observer, conversation_ref, "loom_start_run", {"closure_version": committed["closure_version"]}, timeout=120.0)
        except Exception:
            pass  # observer->driver forward may time out; keep polling persisted state

        run = _poll_run(observer, run_id, timeout=300.0)
        if monitor_errors:
            raise RuntimeError(f"failure injection failed: {monitor_errors[0]}")
        if run["state"] not in {"completed", "closed"}:
            raise RuntimeError(f"run did not complete: state={run['state']} outcome={run.get('outcome')}")
        passed.append("G1(infra): closure committed and executed to completion via MCP")

        report = _extract_report(observer, run)
        gt = ground_truth(corpus)
        _check_report(report, gt, passed)
        _check_provenance(run, passed)
        if failure_controller is not None:
            _check_failure_reassignment(run, failure_controller, passed)
        return run, passed
    finally:
        monitor_stop.set()
        if monitor_thread is not None:
            monitor_thread.join(timeout=5.0)
        if failure_controller is not None:
            failure_controller.restore()


def _extract_report(observer: str, run: dict) -> Any:
    outcome = run.get("outcome") or {}
    if isinstance(outcome.get("value"), dict):
        return outcome["value"]
    ref = outcome.get("resource_ref")
    if isinstance(ref, dict):
        return fetch_content(observer, ref.get("resource_id", ""))
    return None


def _find_stats(value: Any) -> dict | None:
    if isinstance(value, dict):
        if all(k in value for k in ("count", "sum", "mean", "stddev")) and isinstance(value.get("count"), (int, float)):
            return value
        for v in value.values():
            found = _find_stats(v)
            if found:
                return found
    elif isinstance(value, list):
        for v in value:
            found = _find_stats(v)
            if found:
                return found
    return None


def _check_report(report: Any, gt: dict, passed: list[str]) -> None:
    if not isinstance(report, dict):
        raise RuntimeError(f"report is not an object: {report!r}")
    overall = report.get("overall") if isinstance(report.get("overall"), dict) else _find_stats(report)
    if overall is None:
        raise RuntimeError(f"could not locate aggregate stats in report: {json.dumps(report, ensure_ascii=False)[:400]}")
    exp = gt["overall"]
    for key in ("count", "sum", "min", "max", "mean", "stddev"):
        got = overall.get(key)
        want = exp[key]
        if not isinstance(got, (int, float)) or not math.isclose(float(got), float(want), rel_tol=1e-4, abs_tol=1e-4):
            raise RuntimeError(f"aggregate {key}: got {got!r}, want {want}")
    by_family = report.get("by_family")
    if isinstance(by_family, dict) and gt.get("by_family"):
        for family, want in gt["by_family"].items():
            got = by_family.get(family)
            if not isinstance(got, dict):
                raise RuntimeError(f"by_family[{family}] missing from report")
            for key in ("count", "sum", "min", "max", "mean", "stddev"):
                if not math.isclose(float(got[key]), float(want[key]), rel_tol=1e-4, abs_tol=1e-4):
                    raise RuntimeError(f"by_family[{family}].{key}: got {got[key]!r}, want {want[key]}")
    passed.append(f"G3: report matches ground truth (count={exp['count']}, mean={exp['mean']})")


def _check_provenance(run: dict, passed: list[str]) -> None:
    accepted = [e for e in run.get("events") or [] if e.get("phase") in {"node_accepted", "node_reassigned"}]
    completed = [e for e in run.get("events") or [] if e.get("phase") == "node_completed"]
    if not accepted or not completed:
        raise RuntimeError(f"no dynamic node events: accepted={len(accepted)} completed={len(completed)}")
    outcome = run.get("outcome") or {}
    ref = outcome.get("resource_ref")
    if not (isinstance(ref, dict) and ref.get("resource_id", "").startswith("content://sha256/")):
        raise RuntimeError(f"outcome not content-addressed: {ref!r}")
    node_ids = {n["node_id"] for n in run.get("dynamic_nodes") or []}
    accepted_ids = {e.get("node_id") for e in accepted}
    if node_ids != accepted_ids:
        raise RuntimeError("dynamic node set does not match node_accepted events")
    for n in run.get("dynamic_nodes") or []:
        if n.get("state") != "completed":
            raise RuntimeError(f"node not completed: {n.get('node_id')} state={n.get('state')}")
    passed.append(f"G5: provenance ok ({len(node_ids)} nodes, all completed, outcome content-addressed)")


def _check_failure_reassignment(
    run: dict[str, Any],
    controller: SlaveFailureController,
    passed: list[str],
) -> None:
    if not controller.injected:
        raise RuntimeError("G4 failure was not injected after partial progress")
    if run.get("execution_id") != controller.execution_id or int(run.get("execution_epoch", 0)) != controller.execution_epoch:
        raise RuntimeError("G4 reassignment changed the Run execution identity")
    attempts = run.get("attempts") or []
    events = [event for event in run.get("events") or [] if event.get("phase") == "node_reassigned"]
    for event in events:
        old_attempt = next(
            (item for item in attempts if item.get("attempt_id") == event.get("lost_attempt_id")),
            None,
        )
        replacement = next(
            (item for item in attempts if item.get("attempt_id") == event.get("replacement_attempt_id")),
            None,
        )
        authorization = event.get("reassignment_authorization") or {}
        if (
            old_attempt is not None
            and replacement is not None
            and old_attempt.get("state") == "lost"
            and replacement.get("state") == "completed"
            and old_attempt.get("node_id") == replacement.get("node_id")
            and old_attempt.get("target") == "slave-a"
            and replacement.get("target") != "slave-a"
            and authorization.get("policy") == "allow_reassignment"
            and authorization.get("value") is True
            and event.get("package_ref")
            and event.get("input_refs")
        ):
            healthy_sibling = any(
                item.get("node_id") != old_attempt.get("node_id") and item.get("state") == "completed"
                for item in attempts
            )
            if not healthy_sibling:
                raise RuntimeError("G4 has no healthy sibling completion")
            passed.append(
                "G4: slave-a loss was reassigned with audit and unchanged Run epoch; "
                "ground-truth equality validates this workload, not generic cross-target semantic equivalence"
            )
            return
    raise RuntimeError("G4 reassignment audit chain is incomplete")


def _now() -> str:
    return time.strftime("%H:%M:%S", time.gmtime()) + f".{int(time.time() * 1000) % 1000:03d}Z"


def _poll_run(observer: str, run_id: str, *, timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run = get_run(observer, run_id)
        print(f"[{_now()}] poll_run state={run['state']}")
        if run["state"] in {"completed", "awaiting_decision", "failed", "cancelled", "closed"}:
            return run
        time.sleep(2.0)
    raise TimeoutError(f"run {run_id} did not reach a terminal state within {timeout}s")


# --------------------------------------------------------------------------- agent mode

def seed_corpus(observer: str, corpus: list[dict], partitions: int) -> str:
    chunks = [corpus[i::partitions] for i in range(partitions)]
    partition_payload = [{"items": chunk} for chunk in chunks]
    refs = [_json("POST", urljoin(observer, "/api/v1/content"), {"content": p, "media_type": "application/json"}) for p in partition_payload]
    manifest = _json("POST", urljoin(observer, "/api/v1/content"), {"content": {"partitions": refs}, "media_type": "application/json"})
    return manifest["resource_id"]


def run_agent_mode(observer: str, corpus: list[dict], partitions: int, timeout: float) -> tuple[dict, list[str]]:
    passed: list[str] = []
    manifest_ref = seed_corpus(observer, corpus, partitions)
    conversation_ref = f"accept-bandgap-agent-{uuid.uuid4().hex[:10]}"
    request_id = f"request-{uuid.uuid4().hex}"
    prompt = (
        "You are operating a distributed compute runtime. A band-gap dataset has been uploaded; "
        f"its manifest is the content ref {manifest_ref}, a JSON document "
        '{"partitions": [{"items": [{"family", "e_gap", ...}]}]}. '
        "Build and run a closure that computes per-family band-gap statistics "
        "(count, sum, min, max, mean, population stddev) for every family plus overall, "
        "using a map-reduce fan-out across slave-a and slave-b, then returns a report JSON. "
        "If the needed capabilities (e.g. summarize/merge packages) are not present, "
        "materialize them as capability-package candidates during refinement. "
        "Open the run, refine it until readiness is ready, commit, start, and let it complete."
    )
    print(f"[{_now()}] sending message (conversation={conversation_ref})")
    _json(
        "POST",
        urljoin(observer, "/api/v1/messages"),
        {"text": prompt, "request_id": request_id, "conversation_ref": conversation_ref, "workspace_id": WORKSPACE_ID},
        timeout=30.0,
    )
    print(f"[{_now()}] message accepted by observer")

    deadline = time.monotonic() + timeout
    run_id = None
    while time.monotonic() < deadline:
        poll_started = time.monotonic()
        conv = get_conversation(observer, conversation_ref)
        poll_elapsed = time.monotonic() - poll_started
        runs = conv.get("runs") or []
        if runs:
            run_id = runs[-1]["run_id"]
        status = conv.get("status")
        print(f"[{_now()}] poll_conv status={status} run_id={run_id} request={poll_elapsed:.2f}s")
        if status in {"completed", "failed", "interrupted"}:
            break
        time.sleep(5.0)
    if not run_id:
        raise TimeoutError(f"conversation {conversation_ref} never created a run within {timeout}s")
    run = _poll_run(observer, run_id, timeout=min(120.0, max(0.0, deadline - time.monotonic())))
    if run["state"] not in {"completed", "closed"}:
        raise RuntimeError(f"agent-driven run did not complete: state={run['state']} outcome={run.get('outcome')}")
    passed.append("G1: one natural-language message drove real-agent refinement to a completed run")

    run_packages = run.get("capability_packages") or []
    run_bound = [p for p in run_packages if isinstance(p, dict) and p.get("scope") == "run_bound"]
    if not run_bound:
        raise RuntimeError(f"no run_bound candidate packages materialized by agent: {run_packages}")
    passed.append(f"G2: agent materialized {len(run_bound)} run_bound candidate package(s)")

    report = _extract_report(observer, run)
    gt = ground_truth(corpus)
    _check_report(report, gt, passed)
    _check_provenance(run, passed)
    return run, passed


# --------------------------------------------------------------------------- main

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observer", default=DEFAULT_OBSERVER, help="Observer base URL")
    parser.add_argument("--mode", choices=["mcp", "agent"], default="agent", help="test driver mode")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--files", type=int, default=60, help="number of DFT samples")
    parser.add_argument("--partitions", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=1200.0, help="agent-mode poll timeout (seconds)")
    parser.add_argument("--inject-failure", action="store_true", help="G4: take slave-a down mid-run (mcp mode)")
    parser.add_argument("--docker-bin", default="docker", help="Docker CLI used for G4 failure injection")
    parser.add_argument("--compose-file", default="deploy/docker-compose.yml", help="Compose file used for G4 failure injection")
    args = parser.parse_args()

    if args.inject_failure and args.mode != "mcp":
        raise RuntimeError("--inject-failure is supported only in mcp mode")

    health = _json("GET", urljoin(args.observer, "/healthz"), timeout=10.0)
    if not health.get("ok"):
        raise RuntimeError(f"observer unhealthy: {health}")

    corpus = make_corpus(args.seed, args.files)
    if args.mode == "mcp":
        failure_controller = (
            SlaveFailureController(docker_bin=args.docker_bin, compose_file=args.compose_file)
            if args.inject_failure
            else None
        )
        run, passed = run_mcp_mode(
            args.observer,
            corpus,
            args.partitions,
            failure_controller=failure_controller,
        )
    else:
        run, passed = run_agent_mode(args.observer, corpus, args.partitions, args.timeout)

    print(f"mode={args.mode} run={run['run_id']} state={run['state']}")
    for line in passed:
        print(f"  PASS {line}")
    print("ALL GATES PASSED")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # report reason clearly, do not touch code
        print(f"FAIL: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
