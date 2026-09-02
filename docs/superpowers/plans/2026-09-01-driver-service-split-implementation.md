# Driver Service Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 Driver 从 Observer 进程移出，部署为拥有 Codex CLI、Docker CLI、Docker socket 和 Compose secrets 的独立服务，同时保持 Observer 唯一公网入口、Driver→Slave 直连和 Codex thread 可恢复。

**Architecture:** Observer 持有 Run/Node/Attempt/Event、agent lease 和 conversation→thread binding 的唯一权威状态；Driver 通过认证的内部 HTTP 控制协议读写 Observer，直接通过 WorkerSession 调用已注册 Slave；Observer 仅将公网消息/中断请求转发给当前 Workspace 的 active Driver。Driver 每次启动由 Observer 分配递增 `driver_epoch`，使用同一 `thread_id` 调用 Codex `thread/resume`。

**Tech Stack:** FastAPI/uvicorn、httpx、Pydantic v2、SQLAlchemy async、PostgreSQL、MinIO/S3、Docker Compose、Codex app-server JSON-RPC。

---

## 文件结构与职责

- Create: `loom_v2/contracts/agents.py` — agent 注册、lease、thread binding、Driver RPC 信封的 Pydantic 契约。
- Create: `loom_v2/driver/worker.py` — 从 Observer 包迁出的 `WorkerSession` HTTP 客户端。
- Create: `loom_v2/driver/control_client.py` — Driver→Observer 固定命令集 HTTP 客户端。
- Create: `loom_v2/driver/app.py` — Driver FastAPI 入口、注册/heartbeat 生命周期和消息/中断 endpoint。
- Create: `loom_v2/driver/bootstrap.py` — 读取 Compose secrets、生成 `CODEX_HOME/config.toml`、启动前校验。
- Modify: `loom_v2/contracts/__init__.py` — 导出 agent 契约。
- Modify: `loom_v2/db/models.py` — 新增 runtime agent、driver thread、driver request 幂等表。
- Modify: `loom_v2/observer/repository.py` — 实现 agent lease、thread binding、Driver RPC 命令 handler、恢复状态和远程 Slave registry。
- Modify: `loom_v2/observer/app.py` — 移除 Driver/Slave/Worker 实例，增加 agent/driver 内部路由和公网 gateway 转发。
- Modify: `loom_v2/driver/service.py` — 依赖控制客户端、传递 request_id、持久化 thread/turn 状态、恢复 active thread。
- Modify: `loom_v2/driver/mcp.py` — 使用控制客户端，保留 conversation scope 和幂等 request。
- Modify: `loom_v2/driver/orchestration_runtime.py` — 使用 Driver 控制客户端和迁移后的 WorkerSession。
- Modify: `loom_v2/coding_agents/base.py` — provider `start` 支持已有 thread id。
- Modify: `loom_v2/coding_agents/codex.py` — 实现 `thread/resume`、thread 状态查询和恢复错误。
- Modify: `loom_v2/coding_agents/fake.py` — 接受已有 thread id 以保持测试接口一致。
- Modify: `loom_v2/slave/app.py` — Slave 启动注册、heartbeat、Worker 内部认证。
- Modify: `loom_v2/settings.py` — Driver、registry、secret、Codex provider 配置。
- Modify: `Dockerfile` — 保持 Observer 精简，删除 Docker CLI 安装。
- Create: `Dockerfile.driver` — Python、Docker CLI、Node.js/npm 和固定 Codex CLI 版本。
- Create: `docker/driver-entrypoint.sh` — secrets/config/bootstrap 后执行 Driver uvicorn。
- Modify: `deploy/docker-compose.yml` — 增加独立 Driver，移除 Observer→Slave 依赖和 `driver-db`，加入 secrets/volume。
- Modify: `scripts/dev-up.sh` — 校验 secret 文件并启动完整 Compose 栈。
- Modify: `.env.example`、`README.md` — 新拓扑、secret、Codex model/base URL 和恢复说明。
- Create: `migrations/006_driver_service_split.sql` — runtime_agents、driver_threads、driver_requests 表和索引。
- Create/Modify: `tests/contracts/test_agents.py`、`tests/db/test_persistence.py`、`tests/api/test_driver_gateway.py`、`tests/api/test_agent_registry.py`、`tests/driver/test_control_client.py`、`tests/driver/test_driver_app.py`、`tests/driver/test_codex_resume.py`、`tests/e2e/test_driver_restart_thread_resume.py`、`tests/deploy/test_compose_config.py`。
- Modify: 现有 Driver/Slave/E2E 测试中的 `WorkerSession` import，从 `loom_v2.observer.worker` 改为 `loom_v2.driver.worker`。
- Delete: `loom_v2/observer/worker.py`，不保留 Observer-side WorkerSession 兼容路径。

## 约定的类型与协议

后续任务统一使用下列字段名，避免跨任务漂移：

```python
class AgentRegistration(ContractModel):
    role: Literal["driver", "slave"]
    agent_id: str
    instance_id: str
    workspace_id: str
    endpoint_url: str
    protocol_version: str
    capabilities: dict[str, Any] = Field(default_factory=dict)


class AgentLease(ContractModel):
    agent_id: str
    instance_id: str
    workspace_id: str
    lease_id: str
    epoch: int
    heartbeat_interval_seconds: float


class DriverCommand(ContractModel):
    request_id: str
    driver_id: str
    instance_id: str
    lease_id: str
    driver_epoch: int
    command: str
    arguments: dict[str, Any] = Field(default_factory=dict)
```

Observer 内部 RPC 命令集合固定为：

```text
run.open run.get run.patch run.commit run.start run.close run.cancel
run.readiness run.recovery.list run.recovery.mark
message.append agent_signal.record
thread.bind thread.get turn.state
capability.list capability.get capability.health
node.accept node.dispatch node.result node.fail
```

## Task 1: Agent 与 thread 持久化契约

**Files:**
- Create: `loom_v2/contracts/agents.py`
- Modify: `loom_v2/contracts/__init__.py`
- Modify: `loom_v2/db/models.py`
- Create: `migrations/006_driver_service_split.sql`
- Test: `tests/contracts/test_agents.py`
- Test: `tests/db/test_persistence.py`

- [ ] **Step 1: Write the failing contract tests**

```python
def test_driver_registration_requires_role_identity_and_endpoint():
    registration = AgentRegistration(
        role="driver",
        agent_id="driver-default",
        instance_id="instance-1",
        workspace_id="workspace-default",
        endpoint_url="http://driver:8090",
        protocol_version="driver.v1",
    )
    assert registration.role == "driver"
    assert registration.capabilities == {}


def test_driver_command_requires_current_epoch_fields():
    command = DriverCommand(
        request_id="request-1",
        driver_id="driver-default",
        instance_id="instance-1",
        lease_id="lease-1",
        driver_epoch=4,
        command="run.get",
    )
    assert command.driver_epoch == 4


def test_driver_thread_binding_round_trips_thread_id():
    binding = DriverThreadBinding(
        workspace_id="workspace-default",
        conversation_ref="conversation-1",
        thread_id="thread-1",
        model="deepseek-v4-flash",
        workspace_root="/workspace",
        turn_state="idle",
        driver_epoch=2,
    )
    assert DriverThreadBinding.model_validate(binding.model_dump()).thread_id == "thread-1"
```

- [ ] **Step 2: Run the focused tests and verify the expected missing-contract failure**

Run: `PYTHONPATH=. pytest -q tests/contracts/test_agents.py`

Expected: FAIL with an import or validation error because `loom_v2.contracts.agents` and its models do not exist yet.

- [ ] **Step 3: Implement the contracts and SQLAlchemy rows**

Add `AgentRegistration`, `AgentLease`, `DriverCommand`, `DriverThreadBinding`, and `DriverRequestReceipt` in `loom_v2/contracts/agents.py`. Add `RuntimeAgentRow`, `DriverThreadRow`, and `DriverRequestRow` with JSON capability/receipt columns and indexes on `(workspace_id, role, agent_id)`, `(workspace_id, conversation_ref)`, and `(driver_id, request_id)`.

Add `migrations/006_driver_service_split.sql` with the same columns and unique constraints. Export public contract names from `loom_v2/contracts/__init__.py`.

- [ ] **Step 4: Run focused tests and persistence checks**

Run: `PYTHONPATH=. pytest -q tests/contracts/test_agents.py tests/db/test_persistence.py`

Expected: PASS; SQLAlchemy metadata creates the new tables and round-trips JSON fields on SQLite test databases.

## Task 2: Observer agent registry and internal command handlers

**Files:**
- Modify: `loom_v2/observer/repository.py`
- Modify: `loom_v2/observer/app.py`
- Modify: `loom_v2/settings.py`
- Test: `tests/api/test_agent_registry.py`
- Test: `tests/api/test_driver_gateway.py`

- [ ] **Step 1: Write failing lease and gateway tests**

```python
def test_registering_new_driver_increments_epoch_and_fences_old_client(client):
    first = client.post("/internal/v1/agents/register", json=driver_registration("instance-1")).json()
    second = client.post("/internal/v1/agents/register", json=driver_registration("instance-2")).json()
    assert second["epoch"] == first["epoch"] + 1
    stale = client.post(
        "/internal/v1/driver/commands",
        json=driver_command(first, "run.get", {"run_id": "run-1"}),
    )
    assert stale.status_code == 409
    assert stale.json()["detail"] == "stale_driver_epoch"


def test_observer_forwards_message_to_active_driver(httpx_mock):
    registration = register_driver(httpx_mock, endpoint_url="http://driver:8090")
    httpx_mock.add_response(json={"run_id": None, "conversation_ref": "conversation-1", "status": "completed"})
    response = client.post("/api/v1/messages", json={"request_id": "req-1", "conversation_ref": "conversation-1", "text": "hello"})
    assert response.status_code == 200
    assert httpx_mock.get_request().url == "http://driver:8090/driver/v1/messages"


def test_observer_starts_without_registered_slave_or_driver(client):
    assert client.get("/healthz").json()["ok"] is True
    assert client.post("/api/v1/messages", json={"request_id": "req-1", "conversation_ref": "c", "text": "hello"}).status_code == 503
```

- [ ] **Step 2: Run tests to verify lease/gateway failures**

Run: `PYTHONPATH=. pytest -q tests/api/test_agent_registry.py tests/api/test_driver_gateway.py`

Expected: FAIL because Observer still instantiates `DriverService`, has no agent registry routes, and has no Driver HTTP gateway.

- [ ] **Step 3: Implement repository lease, registry, thread and command dispatch**

Add repository methods:

```python
async def register_agent(self, registration: AgentRegistration) -> AgentLease: ...
async def heartbeat_agent(self, agent_id: str, instance_id: str, lease_id: str, epoch: int) -> AgentLease: ...
async def release_agent(self, agent_id: str, instance_id: str, lease_id: str, epoch: int) -> None: ...
async def list_agents(self, workspace_id: str, role: str | None = None) -> list[dict[str, Any]]: ...
async def execute_driver_command(self, command: DriverCommand) -> dict[str, Any]: ...
```

Registration must lock the workspace Driver row, expire the old lease, increment epoch, generate an opaque lease id, persist it hashed, and return the new lease. Every command validates lease, epoch, workspace scope and `DriverRequestRow` idempotency before calling one explicit repository handler. Unknown command names return `400 driver_command_not_allowed`.

Replace `app.state.slaves`, `app.state.workers`, `app.state.driver`, and `app.state.mcp_server` construction in `observer/app.py` with repository-backed registry state and an `ObserverDriverGateway` HTTP client. Add:

```text
POST /internal/v1/agents/register
POST /internal/v1/agents/{agent_id}/heartbeat
POST /internal/v1/agents/{agent_id}/release
GET  /internal/v1/agents/slaves
POST /internal/v1/driver/commands
POST /driver-gateway/v1/registration-check (internal health helper)
```

Keep `/api/v1/messages` and `/api/v1/conversations/{conversation_ref}/interrupt` public, but forward to the active Driver endpoint with a bounded `httpx` timeout. Return `driver_unavailable` when no lease or endpoint is unavailable. Keep event streaming local to Observer.

Change `/api/v1/capabilities` to read registered Slave records. Change capability promotion to publish in Observer, call Driver command `capability.health/provision`, and persist returned health reports.

- [ ] **Step 4: Run focused API tests**

Run: `PYTHONPATH=. pytest -q tests/api/test_agent_registry.py tests/api/test_driver_gateway.py tests/api/test_capability_packages.py`

Expected: PASS; Observer has no Driver/Worker/Slave process objects and gateway/lease behavior matches the tests.

## Task 3: Move WorkerSession and implement Driver control client

**Files:**
- Create: `loom_v2/driver/worker.py`
- Create: `loom_v2/driver/control_client.py`
- Modify: `loom_v2/driver/service.py`
- Modify: `loom_v2/driver/mcp.py`
- Modify: `loom_v2/driver/orchestration_runtime.py`
- Delete: `loom_v2/observer/worker.py`
- Modify: all tests importing `loom_v2.observer.worker`
- Test: `tests/driver/test_control_client.py`

- [ ] **Step 1: Write failing control-client tests**

```python
@pytest.mark.asyncio
async def test_control_client_sends_lease_fenced_command(transport):
    client = ObserverControlClient(
        "http://observer:8080",
        driver_id="driver-default",
        instance_id="instance-1",
        lease_id="lease-1",
        driver_epoch=2,
        transport=transport,
    )
    result = await client.command("run.get", {"run_id": "run-1"})
    request = transport.requests[-1]
    assert request.url.path == "/internal/v1/driver/commands"
    assert request.json()["driver_epoch"] == 2
    assert result["run_id"] == "run-1"


@pytest.mark.asyncio
async def test_control_client_surfaces_stale_epoch(transport):
    client = configured_control_client(transport)
    with pytest.raises(RuntimeError, match="stale_driver_epoch"):
        await client.command("run.get", {"run_id": "run-1"})
```

- [ ] **Step 2: Run focused tests and verify the expected missing-client failure**

Run: `PYTHONPATH=. pytest -q tests/driver/test_control_client.py`

Expected: FAIL because `ObserverControlClient` does not exist.

- [ ] **Step 3: Move and implement clients**

Move the current `WorkerSession` implementation byte-for-byte to `loom_v2/driver/worker.py`, then update Driver/runtime/tests imports and delete `loom_v2/observer/worker.py`.

Implement `ObserverControlClient` with bounded `httpx.AsyncClient`, a fixed command allowlist, request idempotency, registration/heartbeat/release helpers, thread helpers, and typed result decoding. `DriverService`, `DriverMCP`, and `DynamicOrchestrationRuntime` receive a protocol-compatible control client instead of `ObserverRepository`; all state calls go through `command(...)`.

- [ ] **Step 4: Run Driver and Slave tests**

Run: `PYTHONPATH=. pytest -q tests/driver tests/slave tests/integration/test_minio_content_store.py`

Expected: PASS; WorkerSession behavior is unchanged and Driver code no longer imports from `loom_v2.observer.worker`.

## Task 4: Codex thread resume and Driver runtime metadata

**Files:**
- Modify: `loom_v2/coding_agents/base.py`
- Modify: `loom_v2/coding_agents/codex.py`
- Modify: `loom_v2/coding_agents/fake.py`
- Modify: `loom_v2/driver/service.py`
- Create: `tests/driver/test_codex_resume.py`
- Create: `tests/e2e/test_driver_restart_thread_resume.py`

- [ ] **Step 1: Write failing provider and service tests**

```python
@pytest.mark.asyncio
async def test_codex_provider_resumes_existing_thread(fake_rpc):
    provider = CodexAppServerProvider(executable=fake_rpc.executable, model="deepseek-v4-flash")
    await provider.start("conversation-1", "/workspace", existing_thread_id="thread-7")
    assert fake_rpc.methods == ["initialize", "thread/resume"]
    assert fake_rpc.params["thread/resume"]["threadId"] == "thread-7"


@pytest.mark.asyncio
async def test_driver_restart_uses_observer_thread_binding(control_client, provider):
    control_client.thread_binding.return_value = {"thread_id": "thread-7", "turn_state": "idle"}
    service = DriverService(control_client, provider)
    await service.run_prompt("conversation-1", "continue", request_id="req-2")
    assert provider.started_with_thread_id == "thread-7"
```

- [ ] **Step 2: Run tests to verify resume is missing**

Run: `PYTHONPATH=. pytest -q tests/driver/test_codex_resume.py`

Expected: FAIL because provider `start` has no `existing_thread_id` parameter and DriverService does not query thread bindings.

- [ ] **Step 3: Implement resume and persisted turn lifecycle**

Change the provider protocol to:

```python
async def start(self, conversation_ref: str, workspace_root: str, existing_thread_id: str | None = None) -> str: ...
```

When `existing_thread_id` is set, `CodexAppServerProvider.start` sends `thread/resume` with `threadId`, `model`, `cwd`, `runtimeWorkspaceRoots`, and dynamic tools; otherwise it keeps `thread/start`. Preserve the returned thread id and raise `codex_thread_unavailable` on resume failure.

At the beginning of `DriverService._run_prompt`, call `thread.get`; pass the returned id to provider `start`; bind newly-created threads through `thread.bind`. Persist `turn.state` before `turn/start`, after turn id is known, and on every terminal path. Include `request_id` in the persisted message and idempotency lookup. On startup/re-registration, mark old in-progress turns `recovery_pending`, resume their threads, inspect `thread/read`, and mark interrupted/retryable when the prior turn cannot continue.

- [ ] **Step 4: Run protocol, service and restart tests**

Run: `PYTHONPATH=. pytest -q tests/driver/test_codex_protocol.py tests/driver/test_codex_resume.py tests/driver/test_service.py tests/e2e/test_driver_restart_thread_resume.py`

Expected: PASS; new requests use existing thread ids, duplicate request ids do not create a second user turn, and hard restart keeps the same thread id.

## Task 5: Driver FastAPI service and Slave registration

**Files:**
- Create: `loom_v2/driver/app.py`
- Create: `loom_v2/driver/lifecycle.py`
- Modify: `loom_v2/slave/app.py`
- Modify: `loom_v2/slave/service.py`
- Modify: `loom_v2/settings.py`
- Test: `tests/driver/test_driver_app.py`
- Test: `tests/api/test_agent_registry.py`

- [ ] **Step 1: Write failing service tests**

```python
def test_driver_message_endpoint_requires_internal_token(driver_client):
    response = driver_client.post("/driver/v1/messages", json={"request_id": "r", "conversation_ref": "c", "text": "hello"})
    assert response.status_code == 401


def test_driver_registers_and_heartbeats_on_startup(observer_transport):
    app = create_driver_app(transport=observer_transport)
    with TestClient(app):
        assert observer_transport.requests[0].url.path == "/internal/v1/agents/register"
```

- [ ] **Step 2: Run focused tests and verify missing app failure**

Run: `PYTHONPATH=. pytest -q tests/driver/test_driver_app.py`

Expected: FAIL because `loom_v2.driver.app` and lifecycle registration do not exist.

- [ ] **Step 3: Implement Driver app and Slave registration**

`create_app` must construct `DriverService(ObserverControlClient, CodexAppServerProvider, WorkerSession map, DynamicOrchestrationRuntime)` and expose:

```text
GET  /healthz
POST /driver/v1/messages
POST /driver/v1/conversations/{conversation_ref}/interrupt
```

Startup generates `instance_id`, registers, stores `AgentLease`, starts heartbeat task, and loads recoverable thread bindings. Shutdown releases lease after cancelling heartbeat. Every endpoint requires the internal API secret and verifies the request workspace.

Slave startup performs the same register/heartbeat lifecycle with role `slave`, endpoint URL, executor descriptors, and term support. Slave Worker endpoints require the internal API secret but keep attempt/execution validation unchanged.

- [ ] **Step 4: Run service and registry tests**

Run: `PYTHONPATH=. pytest -q tests/driver/test_driver_app.py tests/api/test_agent_registry.py tests/slave/test_worker_api.py`

Expected: PASS; Driver and both Slaves register, heartbeat, and reject unauthenticated requests.

## Task 6: Container images, secrets and Compose topology

**Files:**
- Modify: `Dockerfile`
- Create: `Dockerfile.driver`
- Create: `docker/driver-entrypoint.sh`
- Modify: `deploy/docker-compose.yml`
- Modify: `scripts/dev-up.sh`
- Modify: `.env.example`
- Modify: `README.md`
- Test: `tests/deploy/test_compose_config.py`

- [ ] **Step 1: Write failing Compose assertions**

```python
def test_compose_has_independent_driver_and_no_driver_db(compose):
    assert compose["services"]["driver"]["build"]["dockerfile"] == "Dockerfile.driver"
    assert "driver-db" not in compose["services"]
    assert "observer" not in compose["services"]["observer"].get("depends_on", {})
    assert "slave-a" not in compose["services"]["observer"].get("depends_on", {})


def test_driver_mounts_secrets_workspace_and_docker_socket(compose):
    driver = compose["services"]["driver"]
    assert "codex_api_key" in driver["secrets"]
    assert any("/var/run/docker.sock:/var/run/docker.sock" in item for item in driver["volumes"])
    assert any("/workspace" in item for item in driver["volumes"])
```

- [ ] **Step 2: Run deployment tests and verify current topology failure**

Run: `PYTHONPATH=. pytest -q tests/deploy/test_compose_config.py`

Expected: FAIL because current Compose embeds Driver in Observer and has no `driver` service/secrets.

- [ ] **Step 3: Implement images, entrypoint and Compose**

Keep `Dockerfile` limited to Python dependencies and Observer uvicorn. `Dockerfile.driver` installs Docker CLI, Node.js/npm, and `@openai/codex@${CODEX_VERSION}`; do not copy secret files into the image.

`docker/driver-entrypoint.sh` must read `/run/secrets/codex_api_key` and `/run/secrets/internal_api_secret`, validate non-empty values, create `/var/lib/loom/codex/config.toml` using `LOOM_CODEX_MODEL` and `LOOM_CODEX_BASE_URL`, export only the configured provider key, then exec the Driver uvicorn command. Never echo secret values.

Compose changes:

- add top-level `codex_api_key` and `internal_api_secret` file secrets;
- add `driver` service on internal port 8090 with `LOOM_OBSERVER_URL=http://observer:8080`, `LOOM_SLAVE_A_URL=http://slave-a:8081`, `LOOM_SLAVE_B_URL=http://slave-b:8082`, `CODEX_HOME=/var/lib/loom/codex`, `LOOM_CODEX_MODEL=deepseek-v4-flash`, and configurable `LOOM_CODEX_BASE_URL`;
- mount Driver workspace, Codex state volume, Docker socket, and secrets;
- remove Observer Codex/Slave URL settings, Docker socket, WorkerSession dependencies and Slave `depends_on` entries;
- remove `driver-db` and `driver-db-data`;
- keep public Observer port 18080 and Slave debug ports 8081/8082;
- set Driver/Slave internal auth secret and registration retry settings.

Rewrite `scripts/dev-up.sh` to check both secret files, run `docker compose up -d --build` for MinIO, databases, Observer, Driver and two Slaves, and print the Observer URL. Update README with secret creation and `CODEX_VERSION` instructions.

- [ ] **Step 4: Run config and image smoke checks**

Run:

```bash
PYTHONPATH=. pytest -q tests/deploy/test_compose_config.py
docker compose -f deploy/docker-compose.yml config --quiet
```

Expected: PASS; Compose parses with no Driver DB, Observer has no Slave dependency, and Driver contains both secrets and required mounts.

## Task 7: Refactor remaining APIs, tests and recovery behavior

**Files:**
- Modify: `loom_v2/observer/app.py`
- Modify: `loom_v2/observer/repository.py`
- Modify: `loom_v2/driver/service.py`
- Modify: `loom_v2/driver/mcp.py`
- Modify: `loom_v2/driver/orchestration_runtime.py`
- Modify: `loom_v2/web/static/app.js`
- Modify: all existing tests affected by removed in-process Driver
- Test: `tests/e2e/test_driver_restart_thread_resume.py`

- [ ] **Step 1: Add failing end-to-end recovery assertions**

```python
@pytest.mark.asyncio
async def test_restart_keeps_thread_id_and_fences_old_driver(observer, driver_factory):
    first = await driver_factory(instance_id="one")
    thread_id = await first.send_message("conversation-1", "start", request_id="req-1")
    second = await driver_factory(instance_id="two")
    assert second.epoch == first.epoch + 1
    await second.send_message("conversation-1", "continue", request_id="req-2")
    assert second.provider.resumed_thread_id == thread_id
    with pytest.raises(RuntimeError, match="stale_driver_epoch"):
        await first.control.command("message.append", {"run_id": "run-1"})
```

- [ ] **Step 2: Run the focused E2E test and verify missing split behavior**

Run: `PYTHONPATH=. pytest -q tests/e2e/test_driver_restart_thread_resume.py`

Expected: FAIL because the current Observer owns one in-process Driver and cannot accept two registered instances or resume persisted threads.

- [ ] **Step 3: Complete refactor and remove obsolete paths**

Update all public routes that previously called `app.state.driver`, `app.state.workers`, or `app.state.slaves` to use repository/gateway/registry state. Keep public JSON shapes stable only where the new spec explicitly retains the endpoint; do not add compatibility shims for deleted internal objects.

Update UI message submission to generate a UUID `request_id` per user message. Ensure `GET /api/v1/runtime` reports the active Driver model/backend and `driver_unavailable` when no Driver is registered.

Move dynamic orchestration provisioning and terminal result handling to the Driver control client; Observer only validates and persists. Ensure recovered orchestration increments execution epoch and reuses completed node results.

- [ ] **Step 4: Run the full test suite**

Run: `PYTHONPATH=. pytest -q`

Expected: all existing and new tests pass, with no imports from `loom_v2.observer.worker` and no in-process Driver construction in Observer.

## Task 8: Verification and operational acceptance

**Files:**
- Modify: `README.md`
- Modify: `scripts/test-e2e.sh`
- Test: `tests/e2e/test_dynamic_distributed_analysis.py`
- Test: `tests/e2e/test_dynamic_orchestration_stress.py`

- [ ] **Step 1: Add deployment smoke checks**

Extend `scripts/test-e2e.sh` to assert:

```bash
curl -fsS http://localhost:18080/healthz
curl -fsS http://localhost:8081/healthz | grep -q '"replica":"ready"'
curl -fsS http://localhost:8082/healthz | grep -q '"replica":"ready"'
curl -fsS http://localhost:18080/api/v1/capabilities | grep -q 'slave-a'
curl -fsS http://localhost:18080/api/v1/capabilities | grep -q 'slave-b'
```

- [ ] **Step 2: Run protocol, security and E2E verification**

Run:

```bash
PYTHONPATH=. pytest -q
PYTHONPATH=. python -m compileall -q loom_v2
docker compose -f deploy/docker-compose.yml config --quiet
git diff --check
```

Expected: all tests pass; compile and Compose validation exit 0; no whitespace errors.

- [ ] **Step 3: Run local stack smoke test with secrets**

Create local-only files (never commit):

```bash
mkdir -p secrets
printf '%s\n' "$LOOM_CODEX_API_KEY" > secrets/codex_api_key
printf '%s\n' "$(openssl rand -hex 32)" > secrets/internal_api_secret
chmod 600 secrets/codex_api_key secrets/internal_api_secret
```

Run: `./scripts/dev-up.sh`

Expected: Observer, Driver, Slave A, Slave B, MinIO and three PostgreSQL services are running; `/api/v1/capabilities` reports both Slaves available; Driver logs contain no secret values.

- [ ] **Step 4: Verify restart semantics against the running stack**

Run:

```bash
docker compose -f deploy/docker-compose.yml restart driver
./scripts/test-e2e.sh
docker compose -f deploy/docker-compose.yml logs --tail=100 driver | rg -n 'stale|secret|thread|registered' || true
```

Expected: Driver obtains a higher epoch, Observer remains healthy, both Slaves remain ready, and a subsequent message resumes the persisted Codex thread instead of creating a new one.

## Plan self-review checklist

- [ ] Every spec section maps to at least one task: service split (2/5/6/7), registration/lease (1/2/5), direct WorkerSession (3/7), thread resume (4/7), secrets/images (6), failure/security (2/4/5/6), tests (all tasks).
- [ ] No unresolved markers or incomplete instructions remain in this plan.
- [ ] Type names and field names are consistent: `AgentRegistration`, `AgentLease`, `DriverCommand`, `DriverThreadBinding`, `driver_epoch`, `execution_epoch`, `request_id`.
- [ ] No commit steps are included because repository instructions prohibit creating commits in this session.
