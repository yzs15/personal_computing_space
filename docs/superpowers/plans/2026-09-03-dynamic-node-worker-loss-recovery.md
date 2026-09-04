# Dynamic Node Worker Loss Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow an authorized running DynamicNode to replace a lost Slave Attempt atomically, continue on another live Slave, and retain a complete audit trail without changing the Run execution generation.

**Architecture:** Slave liveness comes only from `runtime_agents` leases. Driver refreshes the registry at every scheduling/recovery boundary and owns target choice; Observer atomically validates and persists Attempt replacement through `node.reassign`; Slave remains an unchanged execution endpoint. `allow_reassignment=true` authorizes mechanical reassignment, while semantic consequences remain the coding agent/user's responsibility and are audited rather than certified by the platform.

**Tech Stack:** Python 3.14, asyncio, FastAPI, httpx, SQLAlchemy JSON projections, pytest/pytest-asyncio, Docker Compose.

**Repository rule:** Do not commit unless the user explicitly requests it. Commit commands below are optional checkpoints and must not be executed under the current instruction set.

---

### Task 1: Make Slave leases the liveness authority

**Files:**
- Modify: `loom_v2/observer/repository.py`
- Modify: `loom_v2/driver/remote_repository.py`
- Test: `tests/api/test_agent_registry.py`
- Test: `tests/driver/test_remote_repository.py`

- [ ] **Step 1: Write failing Observer registry tests**

Add a helper that registers concrete Slave instances and tests that the active instance snapshot, not a second availability boolean, drives support:

```python
async def _register_slave(repo: ObserverRepository, slave_id: str, instance_id: str):
    return await repo.register_agent(
        AgentRegistration(
            role="slave",
            agent_id=slave_id,
            instance_id=instance_id,
            workspace_id="workspace-default",
            endpoint_url=f"http://{slave_id}",
            protocol_version="loom.v1",
            capabilities={"operations": ["run_code"]},
        )
    )


@pytest.mark.asyncio
async def test_slave_runtime_snapshot_excludes_expired_instance():
    repo = ObserverRepository()
    await _register_slave(repo, "slave-a", "instance-a")
    key = ("workspace-default", "slave", "slave-a", "instance-a")
    repo.agents[key]["last_seen_at"] = datetime.now(timezone.utc) - timedelta(seconds=60)

    await repo.refresh_slaves("workspace-default")

    assert repo.slave_agents["slave-a"]["lease_state"] == "expired"
```

Copy the same `_register_slave()` helper into `tests/api/test_dynamic_nodes.py` for Tasks 2 and 3 so those tests do not depend on another test module.

- [ ] **Step 2: Write failing Remote repository tests**

Replace assertions on `slave_availability` with the concrete lease snapshot:

```python
@pytest.mark.asyncio
async def test_refresh_slaves_prefers_active_lease_over_stale_registration():
    repository = RemoteObserverRepository(MixedControl(), content_store=None)

    await repository.refresh_slaves()

    assert repository.slave_agents["slave-a"]["instance_id"] == "slave-a-old"
    assert repository.slave_agents["slave-b"]["instance_id"] == "slave-b-new"
    assert repository.slave_instances[("slave-b", "slave-b-expired")]["lease_state"] == "expired"
```

- [ ] **Step 3: Run focused tests to verify failure**

Run:

```bash
pytest -q tests/api/test_agent_registry.py tests/driver/test_remote_repository.py
```

Expected: FAIL because `slave_agents` and `ObserverRepository.refresh_slaves()` do not exist and current code uses `slave_availability`.

- [ ] **Step 4: Implement concrete Slave lease snapshots**

In both repository adapters, retain the best registration per stable Slave id:

```python
def _select_slave_agents(agents: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for agent in agents:
        slave_id = str(agent.get("agent_id") or "")
        if not slave_id:
            continue
        current = selected.get(slave_id)
        if current is None or (
            agent.get("lease_state") == "active"
            and current.get("lease_state") != "active"
        ):
            selected[slave_id] = dict(agent)
    return selected
```

Both adapters expose `async refresh_slaves(workspace_id: str | None = None)`. `ObserverRepository` defaults the workspace when omitted and calls `list_agents(..., role="slave")`; `RemoteObserverRepository` uses its control client's workspace. Each stores all registrations in `slave_instances[(slave_id, instance_id)]`, stores the best registration per stable id in `slave_agents[slave_id]`, and derives `slave_capabilities` from active selected registrations. `_slave_supports_package()` requires an active selected lease plus operation/executor support.

For the hermetic embedded profile, seed the two existing local Slave services as concrete in-memory registrations with stable test instance ids; do not seed them when an SQL engine is configured.

- [ ] **Step 5: Remove writes to `slave_availability` from registry refresh**

Delete the `slave_availability` fields and stop `list_agents()` from rewriting a boolean cache. Capability API responses may still expose a read-only `available` value derived from `lease_state == "active"`.

- [ ] **Step 6: Run focused tests**

Run:

```bash
pytest -q tests/api/test_agent_registry.py tests/driver/test_remote_repository.py
```

Expected: PASS.

- [ ] **Step 7: Optional commit checkpoint**

Only if the user explicitly requests commits:

```bash
git add loom_v2/observer/repository.py loom_v2/driver/remote_repository.py tests/api/test_agent_registry.py tests/driver/test_remote_repository.py
git commit -m "refactor: derive slave liveness from leases"
```

### Task 2: Bind Attempts to concrete Slave instances

**Files:**
- Modify: `loom_v2/observer/repository.py`
- Test: `tests/api/test_dynamic_nodes.py`
- Test: `tests/db/test_persistence.py`

- [ ] **Step 1: Write a failing dispatch-binding test**

Register `slave-a`, dispatch a dynamic node, and require concrete instance provenance:

```python
@pytest.mark.asyncio
async def test_dynamic_node_dispatch_binds_active_slave_instance():
    repo = ObserverRepository()
    await _register_slave(repo, "slave-a", "slave-a-instance")
    run, patched = await _dynamic_run(repo)
    committed = await repo.commit(run.run_id, patched.draft_version, patched.draft_digest)
    started = await repo.start(run.run_id, committed.version_id)
    record = await repo.get_run(run.run_id)
    package = next(item for item in record.capability_packages if item.package_id == "summarize")
    input_ref = next(item.input_ref for item in record.committed.snapshot.node_input_bindings if item.node_id == "loom://summarize")
    node = await repo.accept_node_intent(
        run.run_id,
        NodeIntent(
            intent_id="intent-instance-binding",
            execution_id=started["execution_id"],
            package_ref=ResourceRef(resource_id=package.version_ref, version_or_digest=package.package_digest),
            input_refs=[input_ref],
        ),
        selected_target="slave-a",
    )

    attempt = await repo.dispatch_dynamic_node(run.run_id, node.node_id, target="slave-a")

    assert attempt == {
        "attempt_id": attempt["attempt_id"],
        "node_id": node.node_id,
        "target": "slave-a",
        "target_instance_id": "slave-a-instance",
        "target_agent_epoch": 1,
        "state": "created",
        "execution_epoch": 1,
        "replaces_attempt_id": None,
        "replaced_by_attempt_id": None,
    }
```

- [ ] **Step 2: Run the test to verify failure**

Run:

```bash
pytest -q tests/api/test_dynamic_nodes.py::test_dynamic_node_dispatch_binds_active_slave_instance
```

Expected: FAIL because dispatch returns only an id and does not bind an instance.

- [ ] **Step 3: Return the full Attempt from initial dispatch**

Change `dispatch_dynamic_node()` to resolve the selected active registration at write time and return the persisted dictionary:

```python
attempt = {
    "attempt_id": f"attempt-{uuid4().hex[:12]}",
    "node_id": node_id,
    "target": normalized_target,
    "target_instance_id": target_agent["instance_id"],
    "target_agent_epoch": int(target_agent["epoch"]),
    "state": "created",
    "execution_epoch": record.execution_epoch,
    "replaces_attempt_id": None,
    "replaced_by_attempt_id": None,
}
record.attempts.append(attempt)
```

Update the `node.dispatch` command response and all direct callers to consume `{"attempt": attempt}` or the full returned Attempt consistently.

- [ ] **Step 4: Persist and restore the new fields**

Add a DB round-trip assertion that `target_instance_id`, `target_agent_epoch`, and replacement fields survive `RunRow.attempts` JSON persistence. No migration is required.

- [ ] **Step 5: Run focused tests**

Run:

```bash
pytest -q tests/api/test_dynamic_nodes.py tests/db/test_persistence.py
```

Expected: PASS.

- [ ] **Step 6: Optional commit checkpoint**

Only if explicitly requested:

```bash
git add loom_v2/observer/repository.py tests/api/test_dynamic_nodes.py tests/db/test_persistence.py
git commit -m "feat: bind attempts to slave instances"
```

### Task 3: Add atomic and idempotent `node.reassign`

**Files:**
- Modify: `loom_v2/observer/repository.py`
- Modify: `loom_v2/driver/control_client.py`
- Modify: `loom_v2/driver/remote_repository.py`
- Test: `tests/api/test_dynamic_nodes.py`
- Test: `tests/api/test_agent_registry.py`
- Test: `tests/driver/test_remote_repository.py`

- [ ] **Step 1: Write failing state-transition tests**

First extend the existing `_dynamic_run()` helper with `allow_reassignment` and `node_replay_safety` parameters, and add a local lease-expiry helper:

```python
async def _expire_slave(repo: ObserverRepository, slave_id: str) -> None:
    key = next(key for key in repo.agents if key[1:3] == ("slave", slave_id))
    repo.agents[key]["last_seen_at"] = datetime.now(timezone.utc) - timedelta(seconds=60)
    await repo.refresh_slaves("workspace-default")
```

Then cover the happy path, semantic-responsibility boundary, stale result, sibling result, and idempotency:

```python
@pytest.mark.asyncio
async def test_reassign_marks_old_attempt_lost_without_bumping_run_epoch():
    repo = ObserverRepository()
    await _register_slave(repo, "slave-a", "slave-a-instance")
    await _register_slave(repo, "slave-b", "slave-b-instance")
    run, patched = await _dynamic_run(
        repo, allow_reassignment=True, node_replay_safety="NonReplayable"
    )
    committed = await repo.commit(run.run_id, patched.draft_version, patched.draft_digest)
    started = await repo.start(run.run_id, committed.version_id)
    record = await repo.get_run(run.run_id)
    package = next(item for item in record.capability_packages if item.package_id == "summarize")
    input_ref = next(item.input_ref for item in record.committed.snapshot.node_input_bindings if item.node_id == "loom://summarize")
    node = await repo.accept_node_intent(
        run.run_id,
        NodeIntent(
            intent_id="intent-reassign",
            execution_id=started["execution_id"],
            package_ref=ResourceRef(resource_id=package.version_ref, version_or_digest=package.package_digest),
            input_refs=[input_ref],
        ),
        selected_target="slave-a",
    )
    old_attempt = await repo.dispatch_dynamic_node(run.run_id, node.node_id, target="slave-a")
    await _expire_slave(repo, "slave-a")

    replacement = await repo.reassign_dynamic_node(
        run.run_id,
        node.node_id,
        lost_attempt_id=old_attempt["attempt_id"],
        expected_execution_id=started["execution_id"],
        expected_execution_epoch=1,
        target="slave-b",
        reason="worker_lease_expired",
    )

    record = await repo.get_run(run.run_id)
    assert record.execution_epoch == 1
    assert record.attempts[0]["state"] == "lost"
    assert record.attempts[0]["replaced_by_attempt_id"] == replacement["attempt_id"]
    assert replacement["replaces_attempt_id"] == old_attempt["attempt_id"]
    assert record.events[-1]["package_replay_safety"] == "NonReplayable"
    assert record.events[-1]["reassignment_authorization"]["value"] is True
```

Also assert `allow_reassignment=false`, active source lease, locality mismatch, inaccessible input, and exhausted `max_attempts` are rejected without partial mutation.

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
pytest -q tests/api/test_dynamic_nodes.py -k 'reassign or stale_attempt or sibling'
```

Expected: FAIL because `reassign_dynamic_node()` and replacement projection do not exist.

- [ ] **Step 3: Implement Observer transaction logic**

Add `reassign_dynamic_node()` under the existing per-Run dynamic lock. Validate the current execution, active old Attempt, exact expired/released source instance, authorization, target capability/locality/input access, and attempt budget before mutation. Then atomically:

```python
old_attempt["state"] = "lost"
old_attempt["terminal_error"] = {
    "code": "worker_lost",
    "lease_state": source_agent["lease_state"],
}
old_attempt["replaced_by_attempt_id"] = replacement["attempt_id"]
record.attempts.append(replacement)
node.state = "dispatched"
record.events.append(node_reassigned_event)
await self._persist(record)
return deepcopy(replacement)
```

Do not increment `record.execution_epoch`. Do not reject solely because of `package.replay_safety`; record the declaration in the event.

- [ ] **Step 4: Add command allowlisting and adapter method**

Add `node.reassign` to both Observer and Driver control-client allowlists. Map it to `reassign_dynamic_node()` and add:

```python
async def reassign_dynamic_node(self, run_id: str, node_id: str, **arguments: Any) -> dict[str, Any]:
    payload = await self.control.command(
        "node.reassign",
        {"run_id": run_id, "node_id": node_id, **arguments},
        request_id=f"node-reassign:{arguments['lost_attempt_id']}:{arguments['target']}",
    )
    return dict(payload["attempt"])
```

- [ ] **Step 5: Update event replay**

Teach `_dynamic_nodes_from_events()` that `node_reassigned` contains a created replacement Attempt and leaves the DynamicNode in `dispatched`, not `accepted`.

- [ ] **Step 6: Verify stale and sibling fencing**

Run:

```bash
pytest -q tests/api/test_dynamic_nodes.py tests/api/test_agent_registry.py tests/driver/test_remote_repository.py
```

Expected: PASS, including acceptance of a healthy sibling result at the unchanged Run epoch.

- [ ] **Step 7: Optional commit checkpoint**

Only if explicitly requested:

```bash
git add loom_v2/observer/repository.py loom_v2/driver/control_client.py loom_v2/driver/remote_repository.py tests/api/test_dynamic_nodes.py tests/api/test_agent_registry.py tests/driver/test_remote_repository.py
git commit -m "feat: atomically reassign lost node attempts"
```

### Task 4: Classify Worker transport loss

**Files:**
- Modify: `loom_v2/driver/worker.py`
- Test: `tests/slave/test_worker_api.py`

- [ ] **Step 1: Write failing transport-classification tests**

```python
def dispatch_arguments() -> dict[str, Any]:
    return {
        "attempt_id": "attempt-worker-unavailable",
        "execution_id": "execution-worker-unavailable",
        "execution_epoch": 1,
        "workspace_id": "workspace-default",
        "operation": "echo",
        "payload": {},
        "closure": TaskClosure.minimal(),
        "binding": None,
    }


def application_failure_transport() -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json={"detail": "capability_exec_error"}, request=request)

    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_worker_session_classifies_transport_failure_as_unavailable():
    session = WorkerSession(
        "slave-a",
        "http://slave-a",
        transport=httpx.MockTransport(lambda request: (_ for _ in ()).throw(httpx.ConnectError("down", request=request))),
    )

    with pytest.raises(WorkerUnavailableError):
        await session.dispatch(**dispatch_arguments())


@pytest.mark.asyncio
async def test_worker_session_does_not_classify_application_failure_as_unavailable():
    session = WorkerSession("slave-a", "http://slave-a", transport=application_failure_transport())

    with pytest.raises(RuntimeError, match="capability_exec_error"):
        await session.dispatch(**dispatch_arguments())
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
pytest -q tests/slave/test_worker_api.py -k 'transport_failure or application_failure'
```

Expected: FAIL because `WorkerUnavailableError` does not exist.

- [ ] **Step 3: Implement the minimal typed error**

```python
class WorkerUnavailableError(RuntimeError):
    pass


async def _post(self, path: str, payload: dict[str, Any]) -> httpx.Response:
    try:
        async with asyncio.timeout(self.operation_timeout):
            async with httpx.AsyncClient(timeout=self.operation_timeout, transport=self.transport) as client:
                return await client.post(f"{self.base_url}{path}", json=payload, headers=self._headers())
    except (TimeoutError, httpx.TimeoutException) as exc:
        raise WorkerUnavailableError("worker_operation_timeout") from exc
    except httpx.TransportError as exc:
        raise WorkerUnavailableError("worker_unavailable") from exc
```

Keep HTTP response and terminal-report errors as ordinary `RuntimeError` values.

- [ ] **Step 4: Run Worker tests**

Run:

```bash
pytest -q tests/slave/test_worker_api.py
```

Expected: PASS.

- [ ] **Step 5: Optional commit checkpoint**

Only if explicitly requested:

```bash
git add loom_v2/driver/worker.py tests/slave/test_worker_api.py
git commit -m "feat: classify worker transport loss"
```

### Task 5: Add the Dynamic Runtime recovery loop

**Files:**
- Modify: `loom_v2/driver/orchestration_runtime.py`
- Modify: `loom_v2/driver/remote_repository.py`
- Test: `tests/driver/test_dynamic_orchestration_runtime.py`
- Test: `tests/e2e/test_bandgap_report_deterministic.py`

- [ ] **Step 1: Replace the invalid xfail with a failing recovery test**

Use a valid executor signature and valid JSON-emitting node program. Wrap the real ASGI `slave-a` session so provision succeeds, then its dispatch expires the registered instance and raises `WorkerUnavailableError`:

```python
class SingleNodeExecutor:
    async def run(self, _program, _input_ref, *, read_json, emit_node, result):
        handle = await emit_node(node_ref, [parent_input])
        return await result(handle)


async def _expire_runtime_slave(repo: ObserverRepository, slave_id: str) -> None:
    key = next(key for key in repo.agents if key[1:3] == ("slave", slave_id))
    repo.agents[key]["last_seen_at"] = datetime.now(timezone.utc) - timedelta(seconds=60)
    await repo.refresh_slaves("workspace-default")


class LosingWorker:
    def __init__(self, repo: ObserverRepository, delegate: WorkerSession) -> None:
        self.repo = repo
        self.delegate = delegate
        self.operation_timeout = delegate.operation_timeout

    async def provision(self, **arguments):
        return await self.delegate.provision(**arguments)

    async def dispatch(self, **arguments):
        await _expire_runtime_slave(self.repo, "slave-a")
        raise WorkerUnavailableError("worker_unavailable")


@pytest.mark.asyncio
async def test_runtime_reassigns_lost_dynamic_attempt_without_bumping_epoch():
    repo = ObserverRepository()
    run, _ = await _dynamic_fixture_for_target_selection(repo)
    record = await repo.get_run(run.run_id)
    package = next(item for item in record.capability_packages if item.package_id == "summarize")
    node_ref = ResourceRef(resource_id=package.version_ref, version_or_digest=package.package_digest)
    parent_input = next(item.input_ref for item in record.committed.snapshot.node_input_bindings if item.node_id == "loom://orchestrate")
    slave_a = WorkerSession("slave-a", "http://slave-a", transport=httpx.ASGITransport(app=create_slave_app("slave-a")))
    slave_b = WorkerSession("slave-b", "http://slave-b", transport=httpx.ASGITransport(app=create_slave_app("slave-b")))
    runtime = DynamicOrchestrationRuntime(
        repository=repo,
        executor=SingleNodeExecutor(),
        workers={"slave-a": LosingWorker(repo, slave_a), "slave-b": slave_b},
    )

    completed, _ = await runtime.run(run.run_id)

    record = await repo.get_run(run.run_id)
    assert completed.state == "completed"
    assert [attempt["state"] for attempt in record.attempts] == ["lost", "completed"]
    assert [attempt["target"] for attempt in record.attempts] == ["slave-a", "slave-b"]
    assert record.execution_epoch == 1
```

- [ ] **Step 2: Run the test to verify failure**

Run:

```bash
pytest -q tests/driver/test_dynamic_orchestration_runtime.py -k 'reassigns_lost'
```

Expected: FAIL because `_execute_node()` immediately fails the node.

- [ ] **Step 3: Refresh registry at scheduling boundaries**

Make target selection async or feed it a freshly refreshed snapshot before each call. Initial NodeIntent acceptance and every replacement choice must call `repository.refresh_slaves(...)` first. Keep one shared matching function for initial and replacement targets and allow excluding the lost stable target.

- [ ] **Step 4: Implement the per-node Attempt loop**

Restructure execution around a persisted Attempt:

```python
async def _execute_node(self, run_id: str, node: DynamicNode, target: str) -> ResourceRef:
    attempt = await self.repository.dispatch_dynamic_node(run_id, node.node_id, target=target)
    while True:
        try:
            return await self._execute_attempt(run_id, node, attempt)
        except WorkerUnavailableError:
            attempt = await self._recover_attempt(run_id, node, attempt)
```

`_recover_attempt()` calls `await repository.refresh_slaves()` and checks `repository.slave_instances[(attempt["target"], attempt["target_instance_id"])]` until that exact binding is no longer active, bounded by the source Worker's existing operation timeout. It selects a different target and calls `reassign_dynamic_node()`. If authorization, target availability, budget, or recovery time is exhausted, the existing outer failure path moves the node to `awaiting_decision` once.

- [ ] **Step 5: Keep completed sibling Attempts valid**

Add a two-node runtime test where `slave-a` is lost while a `slave-b` Attempt returns under the same execution epoch. Both the sibling result and replacement result must be accepted.

- [ ] **Step 6: Run runtime and deterministic E2E tests**

Run:

```bash
pytest -q tests/driver/test_dynamic_orchestration_runtime.py tests/e2e/test_bandgap_report_deterministic.py
```

Expected: PASS with no xfail for Slave loss recovery.

- [ ] **Step 7: Optional commit checkpoint**

Only if explicitly requested:

```bash
git add loom_v2/driver/orchestration_runtime.py loom_v2/driver/remote_repository.py tests/driver/test_dynamic_orchestration_runtime.py tests/e2e/test_bandgap_report_deterministic.py
git commit -m "feat: recover dynamic nodes after worker loss"
```

### Task 6: Delete obsolete availability and reconcile paths

**Files:**
- Modify: `loom_v2/observer/app.py`
- Modify: `loom_v2/observer/repository.py`
- Modify: `loom_v2/driver/mcp.py`
- Delete: `tests/e2e/test_reassignment.py`
- Modify: `tests/api/test_observer_run.py`
- Modify: related readiness/capability tests found by `rg slave_availability`

- [ ] **Step 1: Add replacement capability/readiness assertions**

Update tests to register active/expired Slave agents and assert public/MCP capability availability is derived from lease state:

```python
assert capability["available"] is (agent["lease_state"] == "active")
```

- [ ] **Step 2: Delete obsolete APIs and repository methods**

Remove:

```text
ObserverRepository.set_slave_availability
ObserverRepository.reconcile
POST /api/v1/slaves/{slave_id}/availability
POST /api/v1/runs/{run_id}/reconcile
```

Remove the hard-coded `slave-a → slave-b` and Run epoch mutation logic rather than retaining a compatibility wrapper.

- [ ] **Step 3: Remove all mutable availability references**

Run:

```bash
rg -n "slave_availability|set_slave_availability|/availability|/reconcile" loom_v2 tests
```

Expected: no production references; any remaining documentation occurrence describes deleted behavior.

- [ ] **Step 4: Run API, MCP, and readiness tests**

Run:

```bash
pytest -q tests/api tests/driver/test_mcp.py tests/api/test_readiness.py
```

Expected: PASS.

- [ ] **Step 5: Optional commit checkpoint**

Only if explicitly requested:

```bash
git add loom_v2/observer/app.py loom_v2/observer/repository.py loom_v2/driver/mcp.py tests/api tests/driver/test_mcp.py
git rm tests/e2e/test_reassignment.py
git commit -m "refactor: remove synthetic slave availability controls"
```

### Task 7: Implement real Docker Compose failure injection

**Files:**
- Modify: `scripts/accept-bandgap.py`
- Test: `tests/e2e/test_live_distributed_statistics.py`

- [ ] **Step 1: Write failure-controller unit tests**

Extract a small controller whose command runner is injectable and verify stop/start ordering without invoking Docker:

```python
def test_failure_controller_stops_after_completed_and_running_nodes():
    commands: list[list[str]] = []
    controller = SlaveFailureController(
        docker_bin="docker",
        compose_file="deploy/docker-compose.yml",
        run_command=lambda command: commands.append(command),
    )

    controller.observe(
        {
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
```

- [ ] **Step 2: Run the test to verify failure**

Run:

```bash
pytest -q tests/e2e/test_live_distributed_statistics.py -k failure_controller
```

Expected: FAIL because the controller does not exist and `--inject-failure` is currently unused.

- [ ] **Step 3: Implement the controller with the standard library**

Use `subprocess.run([...], check=True)` with explicit argument arrays. Start the monitor thread immediately before the blocking start/run call, stop only after one completed node and one active `slave-a` Attempt are visible, and always start `slave-a` in `finally` if it was stopped.

Add `--docker-bin` and `--compose-file` CLI options with defaults `docker` and `deploy/docker-compose.yml`; do not add a dependency.

- [ ] **Step 4: Assert G4 audit semantics**

Require old `lost` and replacement `completed` Attempts for the same node, unchanged Run execution identity/epoch, `node_reassigned` authorization/provenance, healthy sibling completion, and final ground-truth equality. State explicitly in output that ground-truth equality validates this workload, not generic cross-target semantic equivalence.

- [ ] **Step 5: Run non-destructive script tests**

Run:

```bash
pytest -q tests/e2e/test_live_distributed_statistics.py
python scripts/accept-bandgap.py --help
```

Expected: PASS; help lists functional failure-injection options. Do not run the real container-stop acceptance until final verification.

- [ ] **Step 6: Optional commit checkpoint**

Only if explicitly requested:

```bash
git add scripts/accept-bandgap.py tests/e2e/test_live_distributed_statistics.py
git commit -m "test: inject real slave container loss"
```

### Task 8: Verify the complete redesign

**Files:**
- Verify: `docs/superpowers/specs/2026-09-03-dynamic-node-worker-loss-recovery-design.md`
- Verify: all modified source and test files

- [ ] **Step 1: Run formatting and static checks already configured by the repository**

Run the configured commands from `pyproject.toml`; do not introduce a formatter or linter if none is configured.

- [ ] **Step 2: Run focused recovery suites**

Run:

```bash
pytest -q tests/api/test_agent_registry.py tests/api/test_dynamic_nodes.py tests/driver/test_remote_repository.py tests/driver/test_dynamic_orchestration_runtime.py tests/slave/test_worker_api.py tests/e2e/test_bandgap_report_deterministic.py
```

Expected: PASS with no xfail covering the repaired behavior.

- [ ] **Step 3: Run the full hermetic suite**

Run:

```bash
pytest -q
```

Expected: PASS.

- [ ] **Step 4: Run the real Compose G4 acceptance**

After confirming the target Compose project is the development stack, run:

```bash
python scripts/accept-bandgap.py --mode mcp --inject-failure --docker-bin docker --compose-file deploy/docker-compose.yml
```

Expected: G4 reports the `slave-a` Attempt as lost, replacement on `slave-b` completed, Run completed at the unchanged epoch, and the script restores `slave-a` before exit.

- [ ] **Step 5: Inspect the final diff**

Run:

```bash
git diff --check
git status --short
```

Expected: no whitespace errors; only task-related files plus the user's pre-existing modified/untracked files are present.
