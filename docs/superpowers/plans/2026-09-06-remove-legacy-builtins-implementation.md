# 删除遗留内置能力实现计划

> **目标 spec：** `docs/superpowers/specs/2026-09-06-remove-legacy-builtins-design.md`
>
> 本计划有意采用破坏性清理：不保留 `echo`、`hash`、`sort` 的兼容 alias、自动翻译或旧闭包迁移。`run_code` 只通过 `subprocess_json_v1` 执行已绑定的 capability package。

## Goal

删除 `builtin_v1` 及其 `echo`、`hash`、`sort` 演示能力，把普通 Slave 执行和确定性测试统一到真实的 `subprocess_json_v1/run_code` capability package 流程，同时保持 `orchestrator_python_v1` 和未来 executor 的现有语义。

## Architecture

- 默认 `ExecutorRegistry` 只注册 `SubprocessJSONV1Adapter`。
- `run_code` 是 executor operation；具体应用 operation 由 `CapabilityPackageVersion.operation_descriptor_ref` 命名，程序 bytes 和 IoContract 通过 ContentStore 引用。
- 新 Slave 默认只宣布通用 `run_code` 能力；没有 package 的 `run_code` dispatch 返回 `capability_package_required`。
- Driver 不再默认执行进程内 builtin。`DriverService.executor` 仅保留为显式注入的单元测试 seam；生产默认值为 `None`，无 Worker/Slave 时返回已有的 `node_target_unavailable`。
- 真实执行测试使用 embedded `SlaveService` 或 HTTP `WorkerSession`；Fake provider 通过 MCP handler 上传程序/契约、materialize package、绑定 typed hole 后再启动 Run。

## Files and verification strategy

实现遵循 RED → GREEN → refactor。每个 task 先加入/改写聚焦测试，确认预期失败，再实现最小改动；任务完成后运行该 task 的聚焦测试。

### Task 1: 建立共享 run_code 测试 fixture

**Files:**

- Create: `tests/support/__init__.py`
- Create: `tests/support/run_code_fixture.py`
- Test: `tests/slave/test_execution.py`

- [x] **Step 1: 写 fixture 使用的失败测试**

  添加一个最小真实程序 fixture：读取 JSON 输入中的数值，返回确定性的乘积或聚合 JSON。测试先直接调用 `SubprocessJSONV1Adapter.execute("run_code", ..., program=...)`，断言输出值、digest 和 `DeclaredByPackage`。

  同时添加 package helper 的契约测试，断言它生成的 `CapabilityPackageVersion` 明确包含：

  ```text
  executor_kind      = subprocess_json_v1
  executor_operation = run_code
  program_digest     = program_content_ref.version_or_digest
  io_contract_ref    != None
  ```

- [x] **Step 2: 运行聚焦测试并确认失败**

  Run: `pytest -q tests/slave/test_execution.py -k 'run_code or fixture'`

  Expected: FAIL，因为共享 fixture 尚不存在，且当前默认执行入口仍指向 `builtin_v1`。

- [x] **Step 3: 实现最小共享 fixture**

  在 `tests/support/run_code_fixture.py` 中只放 ContentStore 上传、IoContract、package、ComputeBinding 和 closure 的最小组合 helper。程序内容必须通过 ContentStore 引用传递，不把 program bytes 塞入 dispatch payload。fixture 的应用 operation 使用 `test_double` 或 `test_sum`，不再使用三个遗留名称。

- [x] **Step 4: 运行并整理 fixture 测试**

  Run: `pytest -q tests/slave/test_execution.py -k 'run_code or fixture'`

  Expected: shared fixture tests pass；后续任务复用同一个 helper，不在各测试文件复制 package 构造逻辑。

### Task 2: 删除 executor 层的 builtin 和默认本地入口

**Files:**

- Modify: `loom_v2/slave/executor.py`
- Modify: `loom_v2/driver/service.py`
- Test: `tests/slave/test_execution.py`
- Test: `tests/driver/test_interrupt.py`
- Test: `tests/driver/test_mcp.py`

- [x] **Step 1: 写 registry 和 Driver 失败测试**

  添加断言：

  - `default_registry.descriptors()` 不包含 `builtin_v1`；
  - `default_registry.get("builtin_v1")` 抛出 `unsupported_executor:builtin_v1`；
  - `SubprocessJSONV1Adapter` descriptor 仍只声明 `run_code`；
  - 没有 Worker/Slave 且没有显式 executor 注入时，Driver 不再执行本地 demo，而返回 `node_target_unavailable`。

  保留 `test_interrupt.py` 和 MCP 中显式注入的 blocking/failing executor 测试，确保它们验证的是 Driver 生命周期 seam，而不是 builtin 能力。

- [x] **Step 2: 运行聚焦测试并确认失败**

  Run: `pytest -q tests/slave/test_execution.py tests/driver/test_interrupt.py tests/driver/test_mcp.py -k 'executor or builtin or interrupt or failure'`

  Expected: FAIL，因为当前 registry 仍注册 `BuiltinV1Adapter`，Driver 默认仍引用 `execute_operation`。

- [x] **Step 3: 删除 `BuiltinV1Adapter` 和 `execute_operation`**

  从 `loom_v2/slave/executor.py` 删除 `BuiltinV1Adapter`、`builtin_v1` alias 和 `execute_operation`。默认 `ExecutorRegistry` 只实例化 `SubprocessJSONV1Adapter`；`FUTURE_EXECUTOR_KINDS` 继续只包含 HTTP/gRPC/MCP 三个未实现扩展。

- [x] **Step 4: 移除 Driver 默认本地执行回退**

  删除 `DriverService` 对 `execute_operation` 的导入和默认参数。将 `executor` 设为可选显式 seam；`_dispatch_execution` 没有 Worker/Slave 时仅在 seam 被显式提供时调用，否则抛出 `node_target_unavailable`。不新增第二个生产执行器。

- [x] **Step 5: 运行 registry/Driver 测试**

  Run: `pytest -q tests/slave/test_execution.py tests/driver/test_interrupt.py tests/driver/test_mcp.py -k 'executor or builtin or interrupt or failure'`

  Expected: registry 不再暴露 builtin，显式注入的 Driver 生命周期测试仍通过。

### Task 3: 收紧 Slave package 执行边界并更新能力发现

**Files:**

- Modify: `loom_v2/slave/service.py`
- Modify: `loom_v2/slave/app.py`
- Modify: `loom_v2/observer/app.py`
- Modify: `loom_v2/observer/repository.py`
- Modify: `loom_v2/driver/mcp.py`
- Test: `tests/slave/test_worker_api.py`
- Test: `tests/api/test_agent_registry.py`
- Test: `tests/driver/test_mcp.py`

- [x] **Step 1: 写能力快照和未绑定 package 的失败测试**

  断言新建 Slave 的默认 `supported_operations` 正好包含 `run_code`，不包含 `echo`、`hash`、`sort`；descriptor 只包含 `subprocess_json_v1`。增加 `SlaveService.run(..., operation="run_code", binding=None)` 返回 `capability_package_required` 的测试。

  更新 Driver MCP capability 查询测试，断言 executor descriptors 只有 `subprocess_json_v1` 与 Driver-side `orchestrator_python_v1`。

- [x] **Step 2: 运行聚焦测试并确认失败**

  Run: `pytest -q tests/slave/test_worker_api.py tests/api/test_agent_registry.py tests/driver/test_mcp.py -k 'capability or descriptor or run_code'`

  Expected: FAIL，因为默认 operations 仍包含三个遗留能力，Slave 无 package 时仍尝试 `builtin_v1`，Driver MCP 仍硬编码 builtin descriptor。

- [x] **Step 3: 更新 Slave 默认能力和执行分支**

  将 `SlaveService.supported_operations` 默认值改为 `{ "run_code" }`。在没有 capability package 时拒绝 `run_code`，返回 `capability_package_required`；绑定 package 时保留现有 cache、digest、IoContract、admission、terminal validation 和 resource event 流程。不要把任意应用 operation 直接加入默认集合。

- [x] **Step 4: 更新能力注册和查询输出**

  让 Slave app、Observer embedded registry 和 Driver MCP 从真实 registry/descriptors 派生结果，移除所有静态 `builtin_v1`、`echo`、`hash`、`sort` 项。保留 `orchestrator_python_v1` 作为 Driver 查询中的独立 descriptor。

- [x] **Step 5: 运行能力边界测试**

  Run: `pytest -q tests/slave/test_worker_api.py tests/api/test_agent_registry.py tests/driver/test_mcp.py -k 'capability or descriptor or run_code'`

  Expected: 能力快照只报告通用 run_code/subprocess，未绑定 package 不能执行。

### Task 4: 将 Slave/Worker 执行测试迁移到真实 package

**Files:**

- Modify: `tests/slave/test_execution.py`
- Modify: `tests/slave/test_worker_api.py`
- Modify: `tests/slave/test_slave_capability_package.py`
- Modify: `tests/integration/test_io_schema_e2e.py`
- Modify: `tests/integration/test_minio_content_store.py`

- [x] **Step 1: 替换遗留 operation 测试**

  删除直接 `execute_operation("echo")`、`execute_operation("hash")` 和裸 `sort` dispatch。使用共享 fixture provision `test_double`/`test_sum` package，再通过 `SlaveService.run` 或 `WorkerSession.dispatch` 执行。

  现有输入 schema、输出 schema、validator、attestation、digest mismatch、timeout 和 result fencing 测试保留其断言重点，但输入/输出改为 fixture 程序的真实 JSON 结果。

- [x] **Step 2: 运行 Slave/Worker 聚焦测试并确认迁移问题**

  Run: `pytest -q tests/slave tests/integration/test_io_schema_e2e.py tests/integration/test_minio_content_store.py`

  Expected: 迁移前仍有旧 operation 或旧默认入口失败；逐个修正 fixture 引用、binding descriptor 和 package scope。

- [x] **Step 3: 完成真实 package 执行迁移**

  所有成功的普通 Slave 执行都经过 `subprocess_json_v1/run_code`，程序只从 ContentStore 读取。对未绑定 package、package digest 不匹配、program 不可用和 IO contract 不完整分别保留结构化错误。

- [x] **Step 4: 运行迁移后的聚焦测试**

  Run: `pytest -q tests/slave tests/integration/test_io_schema_e2e.py tests/integration/test_minio_content_store.py`

  Expected: 所有成功结果来自真实 JSON subprocess，且 ResourceRef/digest/validation evidence 断言通过。

### Task 5: 迁移 Fake provider、Driver/API fixture 和 CLI self-test

**Files:**

- Modify: `loom_v2/coding_agents/fake.py`
- Modify: `loom_v2/cli.py`
- Modify: `tests/driver/test_service.py`
- Modify: `tests/driver/test_remote_service.py`
- Modify: `tests/driver/test_mcp.py`
- Modify: `tests/driver/test_mcp_server.py`
- Modify: `tests/driver/test_codex_protocol.py`
- Modify: `tests/api/test_readiness.py`
- Modify: `tests/api/test_observer_run.py`
- Modify: `tests/api/test_run_lifecycle.py`
- Modify: `tests/api/test_conversation.py`
- Modify: `tests/db/test_persistence.py`
- Modify: `tests/integration/test_repositories.py`
- Modify: `tests/contracts/test_decorators.py`

- [x] **Step 1: 写 Fake provider 的真实执行失败测试**

  将最小 Driver happy-path 测试改为断言：Fake provider 上传程序/IoContract，materialize 一个 `subprocess_json_v1/run_code` package，绑定 typed hole，Run 通过 embedded Slave 完成，并返回 fixture 程序的结果。测试不得只断言 patch 数量或自定义 executor 的返回值。

- [x] **Step 2: 运行 Driver/API 聚焦测试并确认失败**

  Run: `pytest -q tests/driver/test_service.py tests/driver/test_remote_service.py tests/driver/test_mcp.py tests/driver/test_mcp_server.py tests/api/test_readiness.py tests/api/test_observer_run.py tests/api/test_run_lifecycle.py tests/api/test_conversation.py`

  Expected: 迁移前 Fake provider 尚未上传 package，新的 readiness/dispatch 边界会阻止其执行。

- [x] **Step 3: 改造 Fake provider 的 MCP 流程**

  为 `FakeCodingAgentProvider` 增加与 Codex fake transport 等价的 `set_tool_handler` 保存路径，使 legacy local `start()` 和 `begin_turn()` 都能拿到 MCP handler。provider 先调用 `loom_put_content` 上传程序和 IoContract，然后 yield `open_run` 让 Driver 建立 Run，再通过 handler 执行程序/compute/typed-hole/package/binding patch、readiness、commit 和 start；每次直接 handler 调用都 yield 对应的 normalized `tool_call` 事件，避免重复执行同一 mutation。

  package operation 使用 `test_double` 或 `test_sum`；binding 的 descriptor ref 为 `executor://subprocess_json_v1/1`，package ref 和 realization digest 使用 materialization 返回/派生的值。

- [x] **Step 4: 更新 CLI self-test**

  `loom_v2/cli.py --self-test` 不再提交 `echo hello`。让 deterministic provider 通过 embedded Slave 执行真实 run_code package，并输出 terminal result。保持 Compose test profile 的入口和退出码语义不变。

- [x] **Step 5: 迁移其余 Driver/API/DB fixtures**

  对会启动执行的测试使用共享 package fixture 和 embedded/HTTP Slave；只测试 plan/readiness/persistence 而不执行的测试可以使用 `test_run_code` operation，但不得引用三个遗留名称。保留 `test_interrupt.py`、`test_dynamic_orchestration_runtime.py` 中显式注入的 executor，因为它们测试的是 Driver/runtime seam，不是遗留 builtin。

- [x] **Step 6: 运行 Driver/API/DB 聚焦测试**

  Run: `pytest -q tests/driver tests/api tests/db tests/contracts/test_decorators.py tests/integration/test_repositories.py`

  Expected: Fake/local/remote 路径均通过 package → Slave/Worker → subprocess，未启动执行的生命周期测试保持原断言。

### Task 6: 清理文档、活动 fixture 和负向引用

**Files:**

- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-08-25-loom-v2-implementation.md`
- Modify: `docs/superpowers/plans/2026-08-27-capability-gap-resolution.md`
- Modify: `docs/superpowers/specs/2026-08-24-single-user-workspace-v2-design.md`
- Modify: `docs/superpowers/specs/2026-09-02-compute-service-desk-acceptance-design.md`
- Modify: `loom_v2/driver/mcp.py` (只保留真实 descriptor)
- Modify: `loom_v2/cli.py`、`loom_v2/coding_agents/fake.py`（若前序任务仍有遗留字符串）

- [x] **Step 1: 更新活动文档**

  删除“保留 `echo/hash/sort` 向后兼容”的现行说明；把普通执行和测试基线描述改为 `subprocess_json_v1/run_code` package。历史设计若保留示例，明确标记为历史 fixture，不把它们作为当前能力契约。

- [x] **Step 2: 运行负向引用检查**

  Run:

  ```bash
  rg -n --hidden -S 'BuiltinV1Adapter|builtin_v1|loom://echo|loom://hash|loom://sort' loom_v2 tests README.md
  ```

  Expected: 无运行时、测试或 README 命中；本设计/实现计划和明确的历史说明可以保留这些字符串作为删除对象记录。

- [x] **Step 3: 检查 diff 和文档一致性**

  Run: `git diff --check`

  Expected: 无 whitespace 错误；README、MCP descriptor、Slave capabilities 和 spec/plan 对 executor 归属的描述一致。

### Task 7: 全量验证和部署 profile 验收

**Files:** 无新增文件

- [x] **Step 1: 运行完整 Python 测试**

  Run: `pytest -q`

  Expected: 全部测试通过；失败只能来自本设计明确删除的旧 fixture，不得通过恢复 builtin 兼容层规避。

- [x] **Step 2: 运行静态检查和 Compose 配置检查**

  Run:

  ```bash
  python -m compileall -q loom_v2 tests
  docker compose -f deploy/docker-compose.yml config --quiet
  docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.test.yml --profile test config --quiet
  ```

  Expected: compileall 和两份 Compose config 均成功。

- [x] **Step 3: 运行 deterministic Compose self-test**

  Run: `docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.test.yml --profile test up --build --abort-on-container-exit --exit-code-from driver`

  Expected: Fake provider 通过真实 run_code package 完成 self-test，Driver 容器以 0 退出；日志中不出现 `builtin_v1` 或三个遗留 operation。

- [x] **Step 4: 检查最终能力快照和执行 provenance**

  通过 capabilities API/MCP 查询断言：Slave 默认只报告 `run_code` 与 `subprocess_json_v1`，Driver 额外报告 `orchestrator_python_v1`；成功结果能追溯 package ref、program digest、executor descriptor digest 和 validation evidence。

## Execution notes

- 不创建 commit；所有实现变更留在工作树供审阅。
- 不为旧 operation 增加迁移或兼容分支。旧 Run/Closure 如果继续被读取，必须在 readiness 阶段显式失败，而不是执行或伪造结果。
- 不改变 `subprocess_json_v1` 的超时、隔离、JSON 协议和错误码；只改变调用它的默认路径和测试输入。
- `orchestrator_python_v1` 仍是 Driver-side executor；动态节点 package 继续通过 `subprocess_json_v1/run_code`，其 Docker 沙箱测试必须保持通过。
- 任何需要新增生产抽象的实现偏差都应先回到 spec 复核，避免用新的测试专用 adapter 替代被删除的 builtin。
