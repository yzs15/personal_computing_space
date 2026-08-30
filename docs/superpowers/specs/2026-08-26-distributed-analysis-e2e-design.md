# 程序化分布式任务编排与校验压力测试设计（B + A）

**日期：** 2026-08-26
**修订：** 2026-08-30（从静态 DAG 改为受约束的 Python 动态编排，并收敛抽象与安全边界）
**状态：** 设计（待评审）
**对应文档：** `docs/superpowers/specs/2026-08-24-single-user-workspace-v2-design.md`；`docs/superpowers/specs/2026-08-27-minio-content-store-design.md`；`docs/superpowers/specs/2026-08-28-io-schema-validation-design.md`

## 1. 结论与定位

分布式任务不再用预先声明的静态 DAG 表示。coding-agent 生成一个受约束的 Python **Orchestration Program**，它作为父任务闭包的内容寻址 executable realization。Driver Runtime 在隔离 executor 中执行该程序；程序在运行时 emit 子任务节点意图，Observer 校验并物化这些 DynamicNode，Driver Scheduler 选择目标 Slave 执行。

核心分工：

- **coding-agent**：生成 orchestration program、节点能力包、I/O 契约、测试与验证器；是唯一语义细化者。
- **Orchestration program**：根据输入和中间结果机械地实例化已授权能力包，不发明新语义。
- **Observer**：接受 NodeIntent、物化 DynamicNode、维护 Run/Node/Attempt/事件权威。
- **Driver Runtime**：运行 orchestration executor、调度节点、执行重指派和结果回灌。
- **Slave**：只执行被绑定和激活的能力包，回报结果与证据。

本设计不引入静态 `ClosureVersion.nodes`、`add_node` patch op 或通用 DAG 调度器。运行时生成的节点及其输入输出关系由事件与 provenance 重建，形成执行后的 lineage graph；这张图不是执行前的权威计划。

## 2. 当前基线

可直接复用：

- MinIO/S3 ContentStore：异步 `put/get/stat`，内容寻址，唯一实现。
- `io.v1`：`IoContract`、JSON Schema 子集、`NodeInputBinding`、readiness/admission/terminal 校验。
- 能力包：`program_content_ref`、`io_contract_ref`、物化、绑定、provision、`subprocess_json_v1`。
- Driver MCP：`loom_put_content`、`loom_apply_plan_patch`、`loom_inspect_plan_readiness`、`loom_commit_plan`、`loom_start_run`。
- WorkerSession HTTP 边界、attempt / execution_id / execution_epoch fencing。

需要新增：

- Driver-side `orchestrator_python_v1` capability package 与 executor。
- 受限 `OrchestrationContext` API。
- `NodeIntent` / `DynamicNode` / node 级 Attempt。
- Driver 的动态节点调度、并发控制、结果回灌和事件恢复。
- 场景 B/A 测试。

## 3. 父任务闭包与 Orchestration Capability Package

Python 程序不是闭包的全部，也不 inline 持久化在闭包中。父任务闭包仍保留 v2 的目标、成功标准、约束、权限、预算和恢复策略。Orchestration 程序及其授权策略统一作为一个 `CapabilityPackageVersion` 承载，不引入独立的 manifest 层。

```text
ProgramApplication = {
  operation_ref: "loom://orchestrate",
  io_contract_ref: ResourceRef
}

ProgramSystems = {
  executor_kind: "orchestrator_python_v1",
  package_ref: ResourceRef,
  effect_class: "Sandboxed",
  permissions: [...],
  replay_safety: "DeterministicByEventLog"
}
```

`ProgramSystems.package_ref` 指向 orchestration capability package：

```text
OrchestrationPackage = CapabilityPackageVersion {
  executor_kind: "orchestrator_python_v1",
  program_content_ref: ResourceRef,
  program_digest: Digest,
  io_contract_ref: ResourceRef,
  allowed_node_package_refs: [ResourceRef],
  max_nodes: int,
  max_live_nodes: int
}
```

约束：

- Python 程序、节点能力包、schema、IoContract 都先经 `loom_put_content` 上传。
- orchestration package 的 `io_contract_ref` 必须与父闭包 `ProgramApplication.io_contract_ref` 严格一致。
- `allowed_node_package_refs` 只能引用本 Run 物化或 Workspace 已发布的 capability package。
- 节点 operation/schema 不在 orchestration package 中重复声明；这些语义从每个 node package 的 `operation_descriptor_ref` 与 `io_contract_ref` 解析。
- `max_nodes` 和 `max_live_nodes` 是硬限制，超限时 orchestration 失败，不进入 decision_required。

### 3.1 Python 程序接口

程序是单文件 Python，运行在 Driver 管理的 Docker sandbox 中。它只能通过 `OrchestrationContext` 与 Runtime 交互：

```python
SUMMARIZE_PACKAGE_REF = {
    "resource_id": "capability-package://summarize/v1",
    "version_or_digest": "<package-digest>",
}

MERGE_PACKAGE_REF = {
    "resource_id": "capability-package://merge-summaries/v1",
    "version_or_digest": "<package-digest>",
}


async def orchestrate(ctx, input_ref):
    input_document = await ctx.read_json(input_ref)
    handles = [
        ctx.emit_node(
            package_ref=SUMMARIZE_PACKAGE_REF,
            input_refs=[partition],
        )
        for partition in input_document["partitions"]
    ]
    summaries = [await ctx.result(handle) for handle in handles]
    merge_handle = ctx.emit_node(
        package_ref=MERGE_PACKAGE_REF,
        input_refs=summaries,
    )
    final_ref = await ctx.result(merge_handle)
    return final_ref
```

第一版 API：

- `read_json(ref) -> JSON value`
- `emit_node(package_ref, input_refs) -> NodeHandle`
- `result(handle) -> ResourceRef`

禁止：

- 直接访问文件系统、网络、环境变量、数据库或 Slave HTTP endpoint；
- 直接创建或修改 Observer 状态；
- 直接生成新的 Python 源码或新的 operation/schema；
- 使用未播种的随机数、wall-clock 决策或其他不可重放状态；
- 绕过 `ctx` 返回结果。

程序成功结束的唯一机制是 `return final_ref`。失败通过结构化 `OrchestrationFailure` 异常表达，不提供第二个成功/失败结束 API。

`orchestrator_python_v1` 使用 Docker 容器和 stdin/stdout JSON 消息协议实现，不引入新的编排框架或远程执行库。

### 3.2 Driver 侧执行边界

`orchestrator_python_v1` 是 Driver-side executor，不是普通 Worker executor。它默认启用；Docker runtime 不可用时 readiness 返回 `orchestrator_runtime_unavailable`。

- 每个 orchestration 使用独立 Docker 容器，不运行在 Driver 事件循环内；
- 容器禁用网络、使用 read-only rootfs、非 root 用户，不挂载 workspace，不注入数据库、MinIO、Slave 或 Codex 凭据；
- 不添加额外 Linux capabilities，镜像版本由部署配置固定，不在运行时动态拉取；
- 只通过 stdin/stdout 的 JSON request/response 消息访问 `ctx`；
- 程序源码先通过语法解析和 package digest 校验；
- stdout/stderr 和 `ctx` 消息有输出大小限制；
- 容器资源限制由部署配置统一设置；本设计不新增 `max_program_seconds`。

这是真实 OS-level sandbox，不保留 Fake sandbox 或开发专用降级实现。测试也必须通过同一 Docker 边界运行 orchestration program。

## 4. NodeIntent 与 DynamicNode

Python 程序不能直接创建权威 Node。它只能 emit 一个声明式 `NodeIntent`：

```text
NodeIntent = {
  intent_id: OpaqueId,
  execution_id: ExecutionRef,
  package_ref: ResourceRef,
  input_refs: [ResourceRef]
}
```

`package_ref` 与 `input_refs` 是程序提供的语义字段；`intent_id` 由 Driver 按 execution 序号确定性生成，`execution_id` 由父执行上下文填充。程序不能指定这两个身份字段。

Observer 接收后执行机械物化：

```text
DynamicNode = {
  node_id,
  parent_execution_ref,
  intent_id,
  package_ref,
  package_digest,
  input_refs,
  state
}
```

物化规则：

- `node_id` 与 `intent_id` 由 Observer/Driver 生成，不接受程序指定。
- package、operation、IoContract 和 executor 描述全部来自 `NodeIntent.package_ref` 指向的已授权 capability package。
- 输入只能是 ContentStore `content://sha256/<digest>` 或上游节点成功输出的同形态引用。
- Observer 对每个静态输入执行 input schema 校验；上游输出在 terminal 已通过 schema 校验，下游 admission 仍会重读并校验。
- DynamicNode 是从已提交父闭包和授权 package 机械派生的执行事实，不产生新的父闭包 draft，也不改变已提交父闭包版本。

这保持了“coding-agent 是唯一语义细化者”的原则：Runtime 不理解业务，也不生成新语义；它只是执行 coding-agent 预先生成并授权的确定性实例化策略。

### 4.1 物化语义：确定性状态实例化，不依赖 LLM

Observer 物化 DynamicNode 是纯确定性操作，等价于：

```text
NodeIntent
  + 已提交的 OrchestrationPackage
  + 已存在的 node CapabilityPackageVersion
  + ContentStore 中的输入资源
  -> 校验
  -> DynamicNode 权威记录
  -> node_accepted 事件
```

Observer 在该过程中：

- 不调用 LLM；
- 不解释业务语义；
- 不生成新的 operation、schema、success semantics 或权限；
- 不改写 orchestration program；
- 不替程序选择 package 或 target；
- 只根据 package allowlist、digest、I/O schema、节点数量和 live 数量做机械校验。

LLM 只参与执行前的闭包细化阶段，即生成 orchestration program、节点能力包、IoContract、schema、validator 和 patch。Run start 之后的 NodeIntent 接受、DynamicNode 物化、调度和 terminal 校验都是确定性 Runtime 行为。

## 5. Readiness 与执行生命周期

### 5.1 父闭包 readiness

start 前只校验可静态确定的事实：

- orchestration package 存在、digest 匹配、语法可解析，且 Docker runtime 可用。
- `orchestrator_python_v1` descriptor 可用。
- `allowed_node_package_refs` 中每个 package 存在、scope 正确、契约可解析。
- 每个节点能力包在至少一个可用 target 上具备 `run_code` executor。
- package 的 `max_nodes` / `max_live_nodes` 不超过父闭包预算。
- 父闭包 `io_contract_ref` 可解析。
- typed hole / orchestration package binding 完整。

父闭包 ready 不代表所有未来节点已知；只表示 orchestration strategy 可以安全启动。

### 5.2 动态节点 readiness

每个 NodeIntent 被接受前单独校验：

- `package_ref` 在 orchestration package 的 allowlist 中。
- 已发射节点总数未超过 `max_nodes`。
- 当前 live 节点数未超过 `max_live_nodes`。
- input refs 存在、digest 正确并满足 node package input schema。
- 至少一个可用 target 能执行该 package。

拒绝 NodeIntent 时 orchestration 进入 failed，错误码例如：

- `orchestration_node_limit_exceeded`
- `orchestration_live_node_limit_exceeded`
- `node_package_not_allowed`
- `node_input_schema_mismatch`
- `node_target_unavailable`

### 5.3 调度与执行

Driver Scheduler 为每个 DynamicNode 选择 target：

1. 过滤不可用 Slave。
2. 过滤不支持 package executor/operation 的 Slave。
3. 应用 locality、预算、权限和已激活 package。
4. 优先选择已有 activation 的 target。
5. 若仍有多个候选，使用稳定排序或 round-robin 做确定性 tie-break。
6. 记录 scheduling decision 与 reason。

调度决策不是 Driver 内存中的私有事实。`node_accepted` 事件携带 selected target 与 reason，由 Observer 校验 target/package/epoch 后持久化；未持久化的调度决策不得派发。

程序不指定 target。若业务确实需要 locality，由父闭包约束和 node package 声明表达，不由 Python 代码直接绑定机器。

每个节点独立执行：

```text
NodeIntent
  -> Observer accepts DynamicNode
  -> Driver selects target
  -> provision package if needed
  -> create node Attempt
  -> WorkerSession dispatch
  -> Slave reads inputs from ContentStore
  -> execute package
  -> write canonical JSON output to ContentStore
  -> Observer terminal validation
  -> Driver returns result ref to orchestration program
```

节点失败时优先按父闭包 recovery policy 处理：

- replay-safe 且允许 reassignment 时，同一 node 创建新 Attempt；
- 不可恢复时 orchestration failed；
- 需要用户或 coding-agent 语义决策时立即停止 orchestration program，Run 进入 decision_required；结构化原因只进入事件和 UI，不回灌给程序解释。

## 6. 输出、事件与恢复

### 6.1 输出内容化

每个节点输出都是规范化 JSON，并写入 ContentStore：

```text
result ResourceRef = content://sha256/<digest>
```

Observer 最终接受条件：

- 所有已发射节点达到终态；
- 程序返回 final ref；
- final ref 存在且 digest 正确；
- final output 满足父闭包 output schema；
- required success criteria / validator evidence 满足；
- 总节点数、live 节点数、预算和 deadline 未超限。

### 6.2 事件溯源

动态执行通过事件恢复和审计：

```text
orchestration_started
node_requested
node_accepted
node_dispatched
node_completed
orchestration_completed
```

事件包含：

- parent execution_id / epoch；
- intent_id / node_id；
- package ref 与 package digest；
- input refs；
- selected target 与 reason；
- attempt_id；
- result ref 与 validation evidence。

`node_accepted` 携带 selected target 与 reason；`node_dispatched` 携带 attempt_id。能力包安装和激活继续复用既有 capability activation / health 事件，不新增 orchestration 专用的 `package_provisioned` 事件。

恢复策略：

- Python 程序必须确定性：给定 input ref 与已记录节点结果事件，重放得到相同 NodeIntent 序列。
- Driver 重启后可从事件日志恢复已完成节点，不重复执行 replay-safe 节点。
- 若程序被判定不可重放，orchestration failed，错误码 `orchestration_not_replayable`。

执行后的 lineage graph 可由事件重建，但它不是执行前声明的静态 DAG。

## 7. 能力包 fixture

`summarize` 与 `merge_summaries` 不是 builtin，而是普通能力包：

- `summarize`：`{"items":[number...]}` -> `{count,sum,min,max,sumsq}`。
- `merge_summaries`：`{"inputs":[Summary,Summary]}` -> `{count,sum,min,max,sumsq,mean,stddev}`。

测试中 Fake coding-agent 会：

1. 上传 input/output schema；
2. 上传每个节点的 IoContract；
3. 上传节点 Python 程序；
4. 物化节点能力包；
5. 上传 orchestration Python 程序；
6. 物化包含 program、契约、allowlist 和限额的 `orchestrator_python_v1` package；
7. 在父闭包中设置 `io_contract_ref` 与 `ProgramSystems.package_ref`；
8. commit / start。

所有程序正文、schema 和 IoContract 均不 inline 进入闭包或事件。

## 8. 场景 B：单节点动态编排压力测试

目标：先用最小动态编排压满父闭包、程序执行、NodeIntent、Observer 物化、调度、terminal 校验和结果内容化。

流程：

1. Fake agent 打开父闭包，声明多约束、预算、成功标准和 recovery policy。
2. 上传输入 JSON、schema、IoContract、summarize 程序和 orchestration 程序。
3. 物化 summarize 与 orchestration package。
4. 父闭包 readiness 通过后 commit/start。
5. orchestration 程序读取输入 document，emit 一个 summarize NodeIntent。
6. Observer 校验并接受 DynamicNode。
7. Driver 选择 Slave A，provision 并执行。
8. 输出写入 ContentStore，程序返回 final ref。
9. Slave A 下线后重放流程，验证同一 node 创建 Slave B 新 Attempt，execution_id 不变、epoch 递增。
10. 构造非法输入 schema，断言 NodeIntent 被拒绝且父 Run 不伪造 completed。

断言：

- NodeIntent 不直接写 Observer；
- DynamicNode 的 operation/schema 来自授权 package；
- readiness blocker 带 intent/node 语义；
- 改派保留父 execution 身份；
- final 输出是 `content://sha256/<digest>`；
- 事件日志可重放。

## 9. 场景 A：动态分布式统计概要

输入 document 包含两个 partition ref。

orchestration 程序：

1. `read_json(input_ref)`；
2. 对每个 partition emit `summarize` NodeIntent；
3. 等待两个结果 ref；
4. emit 一个 `merge_summaries` NodeIntent；
5. 返回 merge 输出 ref。

执行断言：

- 两个 summarize NodeIntent 是运行时生成，不在父闭包中静态声明。
- 两个节点可以并行，受 `max_live_nodes` 限制。
- Driver 记录两个 target 选择决策。
- 中间结果和最终结果均写入 ContentStore。
- reduce 输入 schema 校验通过。
- provenance 可从最终结果回溯到父闭包、orchestration program、两个 map node、merge node、package、attempt 和 Slave。
- 将输入 document 扩展为三个 partition 时，不需要修改父闭包结构或预声明图；测试可断言程序生成第三个 map node。

## 10. 测试形态

- `tests/e2e/test_dynamic_orchestration_stress.py`：场景 B。
- `tests/e2e/test_dynamic_distributed_analysis.py`：场景 A。
- Fake coding-agent 通过 MCP 工具面完成上传、物化、commit 和 start。
- 两个 SlaveService 经 WorkerSession/ASGI transport 访问。
- ContentStore 使用 moto 或本地 MinIO。
- orchestration executor 使用 Docker sandbox，不引入 Dask/Ray/Temporal。
- 测试覆盖限额、schema mismatch、target unavailable、节点失败、Driver 重启恢复和非法程序副作用拒绝。

## 11. 实施顺序

1. `orchestrator_python_v1` capability package 契约与 canonical JSON 校验。
2. Docker sandbox executor 和受限 `ctx` API。
3. `NodeIntent` 事件、Observer 接受规则与 DynamicNode 物化。
4. Driver Scheduler、有界并发、结果回灌和 attempt fencing。
5. 节点输出 ContentStore 化与父闭包 terminal 校验。
6. 场景 B。
7. 场景 A。
8. 事件重放与 Driver 重启恢复测试。

## 12. 架构影响

1. **计划表示变化**：从静态节点集合改为“父闭包 + 授权 package + 运行时 NodeIntent”。执行图变为事件重建的 lineage，而不是执行前权威 DAG。
2. **readiness 分层**：父闭包只校验 orchestration strategy；每个动态节点在 emit 时单独校验，不再要求开始前知道全部节点。
3. **Driver 职责扩大**：从一次性 dispatch 扩展为 orchestration executor 生命周期、动态调度、并发、结果回灌和恢复。
4. **Observer 权威保持不变**：Python 只能提出 NodeIntent；权威 Node/Attempt/Result 仍由 Observer 接受。
5. **能力包模型扩展**：新增 Driver-side `orchestrator_python_v1` package；节点能力包仍由 Slave 执行。
6. **安全边界前移**：orchestration program 必须运行在 Docker OS-level sandbox 和受限 API 后面，不能获得数据库、文件系统、网络或 Slave 直连能力。
7. **恢复模型更严格**：必须事件溯源并要求程序可确定性重放；否则 Driver 重启后无法安全继续。
8. **测试重点变化**：从静态图 readiness 转向 package 授权、动态节点 admission、限额、调度决策、terminal 校验和 replay。

## 13. 范围外

- 通用 workflow 引擎、分布式持久化编排框架；
- 允许程序发明新 schema、新 operation 或新权限；
- 允许程序直接选择 Slave、直接调用 Worker 或修改 Observer；
- 无上限动态 fan-out；
- 非确定性程序的 checkpoint 迁移；
- 静态 DAG 执行路径。
