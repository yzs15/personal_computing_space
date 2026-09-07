# 删除遗留内置能力并以 `run_code` 作为执行基线

**日期：** 2026-09-06
**状态：** 已按实现计划落地
**取代范围：** 现有 `builtin_v1` 的 `echo`、`hash`、`sort` 演示能力，以及依赖这些能力的确定性测试夹具

## 1. 背景

当前 `BuiltinV1Adapter` 提供 `echo`、`hash`、`sort` 三个确定性操作。这些操作来自 Loom v2 早期垂直切片，主要用于 Fake coding-agent、单元测试、集成测试、故障注入和离线验收，并不代表真实用户能力。

当前真正可扩展的代码执行路径是 `subprocess_json_v1/run_code`：程序以 content-addressed package 形式存储，Slave provision 后在受限子进程中执行，输入和输出通过 JSON 通信。`builtin_v1` 的 `run_code` 只在 descriptor 中列出，实际执行反而明确报错并要求使用 subprocess adapter，因此继续保留 `builtin_v1` 会制造错误的能力模型。

本设计删除三个遗留操作及其空洞的 `builtin_v1` adapter，把测试和本地验收统一到真实的 `subprocess_json_v1/run_code` 能力包路径。

## 2. 目标

1. 从执行器注册表、Slave 能力快照、Driver 能力查询和测试夹具中删除 `echo`、`hash`、`sort`。
2. 删除 `BuiltinV1Adapter`、`builtin_v1` 公共别名和 `execute_operation` 默认入口。
3. 以 `subprocess_json_v1/run_code` 作为 Slave 的唯一内置执行基线；应用能力必须通过 capability package 绑定程序内容和 IoContract。
4. 让确定性测试真实覆盖内容上传、package materialization、provision、JSON 子进程执行、超时、错误、输入/输出校验和结果 digest。
5. 保留 `orchestrator_python_v1` 的 Driver-side Docker 执行，以及 `http_service_v1`、`grpc_service_v1`、`mcp_v1` 的未来扩展占位，不改变其语义。

## 3. 非目标

- 不为 `echo`、`hash`、`sort` 增加兼容别名、自动翻译或隐藏 fallback。
- 不新增另一个仅用于测试的内置 adapter，也不把 `run_code` 的实现复制到 `builtin_v1`。
- 不改变 `subprocess_json_v1` 的沙箱边界、JSONL 协议、超时配置和 `DeclaredByPackage` replay-safety 语义。
- 不把应用层 operation 名称限制为 `run_code`；应用 operation 仍可通过 package 的 `operation_descriptor_ref` 命名，执行器操作固定为 `run_code`。

## 4. 核心决策

### 4.1 `run_code` 的能力归属

`run_code` 是 `subprocess_json_v1` 的 operation，不是 `builtin_v1` 的 operation。所有需要运行程序的测试和运行时路径均使用以下组合：

```text
executor_kind:      subprocess_json_v1
executor_operation: run_code
capability_ref:     capability-package://<package>/<version>
program_content:    content://sha256/<digest>
io_contract:        content://sha256/<digest>
```

测试中的应用 operation 使用明确的 fixture 名称，例如 `loom://test_double` 或 `loom://test_sum`，其 package 仍通过 `subprocess_json_v1/run_code` 执行。这样可以同时验证“应用能力”和“通用代码执行器”之间的边界，避免把 executor operation 与应用 operation 混为一谈。

### 4.2 删除 `builtin_v1`，不保留空 adapter

`ExecutorRegistry` 的默认 adapter 只保留 `SubprocessJSONV1Adapter`。删除：

- `BuiltinV1Adapter` 类及其 `echo`、`hash`、`sort` 分支；
- `builtin_v1 = BuiltinV1Adapter` 公共别名；
- `execute_operation()` 这个默认指向 `builtin_v1` 的函数；
- 所有写死 `builtin_v1` descriptor 的 Driver MCP 返回值。

调用 `ExecutorRegistry.get("builtin_v1")` 应返回现有的 `unsupported_executor:builtin_v1`，不新增 legacy 专用错误码。旧闭包不会被静默转换。

### 4.3 Slave 只宣布通用 `run_code` 能力

`SlaveService.supported_operations` 的默认值改为 `{ "run_code" }`，执行器 descriptor 由默认 registry 提供 `subprocess_json_v1/run_code`。`run_code` 仅表示该 Slave 具备通用代码执行器；没有绑定 package 时不得直接执行：

- `SlaveService.run(..., operation="run_code", binding=None)` 返回 `capability_package_required`；
- 绑定 package 时继续走现有 cache、digest、IoContract、admission 和 terminal validation 流程；
- package 的 `executor_kind`/`executor_operation` 由 registry 校验，未知 executor 继续返回 `executor_unavailable` 或 `unsupported_executor`。

已发布 package 的应用 operation 仍可在 activation 后加入 `supported_operations`；这不改变默认只声明 `run_code` 的基线。

### 4.4 移除 Driver 的默认本地执行回退

`DriverService` 不再默认导入或调用 `execute_operation`。当没有 Worker/Slave 时：

- 生产路径返回现有的 `node_target_unavailable`；
- `executor` 构造参数可以保留为显式注入的单元测试 seam，但默认值为 `None`，不承担生产 adapter 角色；
- 需要验证真实执行的测试必须提供 `SlaveService` 或 `WorkerSession`，并使用 run_code package。

这样 Driver 不会在没有能力绑定时把任务悄悄降级到进程内 demo 操作。

### 4.5 能力发现和协议输出

所有能力发现接口必须反映删除后的真实集合：

- Slave `/worker/v1/capabilities` 的 `operations` 默认只有 `run_code`；`executor_descriptors` 只包含 `subprocess_json_v1`；
- Observer 的 embedded Slave registry 从同一 service descriptor 派生，不再写入三个遗留操作；
- Driver MCP `loom_query_capabilities` 的 executor descriptors 只列出 `subprocess_json_v1` 和 `orchestrator_python_v1`；
- 不为 `builtin_v1` 保留 descriptor、digest、alias 或兼容说明。

`orchestrator_python_v1` 动态节点仍然要求节点 package 使用 `subprocess_json_v1/run_code`，因此其 Driver-side Docker runtime 和节点执行路径保持不变。

## 5. 测试设计

### 5.1 共享 run_code fixture

新增测试辅助模块，集中构造真实 package，避免各测试手工伪造 package 字段：

1. 将一段最小 Python 程序上传到 ContentStore，例如读取 JSON 中的数值并返回确定性乘积或聚合结果；
2. 上传并校验 `io.v1` IoContract；
3. 构造 `CapabilityPackageVersion`，明确设置 `executor_kind="subprocess_json_v1"`、`executor_operation="run_code"`、程序 digest 和 operation descriptor；
4. 构造带 `capability_package_ref`、`realization_digest` 和 `executor_descriptor_digest` 的 `ComputeBinding`；
5. 提供 provision、closure 和 dispatch 的组合 helper，所有测试从同一 helper 获得引用，避免把程序 bytes 放进协议 payload。

fixture 的应用 operation 应使用 `test_double`、`test_sum` 等非遗留名称；不得以 `echo`、`hash`、`sort` 作为别名继续出现。

### 5.2 Executor 单元测试

删除 `execute_operation("echo")` 和 `execute_operation("hash")` 测试，改为直接测试 `SubprocessJSONV1Adapter`：

- 成功运行 JSON 程序并返回 `ExecutionResult`、content digest 和 `DeclaredByPackage`；
- 缺少程序、非零退出、无效 JSON、超时分别产生现有 typed error；
- descriptor 只声明 `run_code`；
- registry 默认 descriptor 集合不包含 `builtin_v1`；
- 查询 `builtin_v1` 返回 `unsupported_executor:builtin_v1`。

### 5.3 Slave 和 Worker API 测试

- 删除直接 dispatch `echo`/`sort` 的测试，改为 provision 一个 run_code package 后 dispatch `test_double` 或 `test_sum`；
- capabilities endpoint 断言默认 operations 为 `{ "run_code" }`，并断言不存在三个遗留操作和 `builtin_v1` descriptor；
- 增加未绑定 package 的 `run_code` 拒绝测试，确保不能回退到进程内执行；
- 保留并扩充现有 digest mismatch、IoContract、validator、terminal state、timeout 和 Worker transport 测试，使它们都覆盖真实 subprocess path。

### 5.4 Driver、Observer 和 API 测试

- `FakeCodingAgentProvider` 不再生成 `loom://echo` 闭包；确定性 provider 通过 MCP handler 上传 fixture 程序/IoContract、materialize package、绑定 typed hole，再启动 run_code package；
- 需要真实执行的 Driver 测试统一注入 embedded `SlaveService` 或 HTTP `WorkerSession`；不再依赖默认本地 executor；
- 只验证 turn、patch、readiness、错误 fencing 而不启动执行的测试，可使用 `loom://test_run_code` 作为 operation 名，并在需要提交/启动时提供 package；
- readiness 测试改为验证 package 缺失、executor descriptor 不匹配和未绑定 run_code package 的结构化 blocker；
- 删除所有断言 `echo/hash/sort` 结果的测试，改为断言 fixture 程序产生的实际结果和 provenance。

### 5.5 CLI 和端到端验收

`--self-test` 不再发送 `echo hello`。它应在本地 ContentStore 中构造一个最小 run_code package，通过 embedded Slave 完成 provision/dispatch，并输出真实 `ExecutionResult`。动态编排 E2E 继续使用其现有的 subprocess 节点 package；静态 fixture 中的应用 operation 改为真实测试程序名称。

### 5.6 回归约束

实现后增加以下回归断言：

- 源码和测试中不存在 `BuiltinV1Adapter`、`builtin_v1`、`loom://echo`、`loom://hash`、`loom://sort` 的运行时引用；
- 新建 Slave 的能力快照不包含三个遗留 operation；
- 所有成功的普通 Slave 执行都能在事件/结果中追溯到 package、程序 digest 和 `subprocess_json_v1` descriptor；
- `orchestrator_python_v1`、未来 executor unsupported 错误和 capability package promotion 流程没有回退到旧 adapter。

## 6. 文档和数据清理

同步更新 README、当前有效的设计/实现计划和测试说明：

- 删除“保留 `echo/hash/sort` 向后兼容”的表述；
- 把普通执行描述改为 `subprocess_json_v1/run_code` package 流程；
- 明确 `orchestrator_python_v1` 是 Driver-side executor，不能作为 Slave 的普通 adapter；
- 将早期垂直切片中仅用于历史背景的 `echo/hash/sort` 示例改成 `run_code` fixture，或标注为已废弃历史材料；
- 删除 CLI、Fake provider 和 capability query 中的遗留 operation 名称。

本设计不迁移历史 Run 或闭包中的旧 operation。部署后它们按普通能力缺失处理，readiness 不通过；平台不会重写旧内容或伪造完成结果。

## 7. 验收标准

| Gate | 断言 |
|---|---|
| G1 执行器集合 | 默认 registry 只有 `subprocess_json_v1`；`builtin_v1` 不可获取；HTTP/gRPC/MCP 仍为 unsupported extension |
| G2 能力发现 | 新建 Slave 只报告 `run_code` 和 subprocess descriptor；Driver 查询不再报告 builtin descriptor |
| G3 真实执行 | 通过 package provision + Worker/Slave dispatch 执行测试程序，返回预期 JSON、digest 和 provenance |
| G4 安全边界 | 未绑定 package 的 `run_code` 被拒绝；无本地 demo fallback；超时/非零退出/无效 JSON 错误保持结构化 |
| G5 回归路径 | IoContract、validator、fencing、reassignment、orchestrator 动态节点和 package promotion 测试全部走新基线 |
| G6 遗留清理 | 运行时源码、Fake provider、CLI、有效文档和测试中不再存在三个遗留能力的引用 |

任何 Gate 失败都阻止实现合入。实现完成后再单独编写实现计划，按 executor → fixture → Slave/Worker → Driver/API → 文档/E2E 的顺序落地。
