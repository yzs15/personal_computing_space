# 能力缺口闭环设计（Capability Gap Resolution）

**日期：** 2026-08-26
**状态：** 设计（待评审）

## 1. 背景与目标

当前 Slave 只支持固定能力集（`echo`/`hash`/`sort`），Observer 能力注册表硬编码。任务闭包需要缺失能力（如 `matmul`）时，readiness 直接报 `capability_unavailable`，闭环中断。

本设计把能力缺口处理统一为**任务闭包细化**：coding-agent 是唯一语义细化者；当目标 Slave 没有现成的 `matmul` 能力、但具有受控的 `run_code` executor 时，coding-agent 可以为 `matmul` 生成具体程序实现，并把该实现固化为一个 `CapabilityPackage` 版本。

所有形成可执行 realization 的细化产物都使用同一种 `CapabilityPackage` 聚合：

- 当前 Run 内产生的版本默认是 `run_bound` 候选包，只能被来源 Run 引用；
- Run 结束后由用户显式决定是否将候选包提升为 `workspace_reusable` 版本；
- 只有提升并在目标 Slave 安装、测试和健康检查通过的包，才作为可发现的能力资源进入能力快照；
- Runtime/Observer 不调用 LLM、不自动生成代码、不自动提升或发布能力包。

因此不再区分“临时代码”和“能力包”两套执行模型：一次性 realization 与可复用能力的差别由同一能力包的作用域、发布状态和激活状态表达。

## 2. 核心语义

### 2.1 能力包是闭包版本的统一承载形式

`CapabilityPackage` 不是裸脚本，也不替代 `ClosureContract`、`ClosureVersion`、`CapabilityDescriptor` 或 `ComputeBinding`。它是一个聚合根，引用一个任务子闭包版本和对应 operation contract：

```text
CapabilityPackageVersion = {
  package_id,
  package_version,
  package_closure_version_ref,
  source_run_ref,
  source_closure_version_ref,
  operation_descriptor_ref,
  operation_descriptor_digest,
  program_content_ref,
  program_digest,
  effective_constraint_refs,
  provider_fillable_hole_refs,
  scope: "run_bound" | "workspace_reusable",
  publication_state: "candidate" | "published" | "abandoned",
  provenance
}
```

- `ClosureContract` 仍在 `open_run` 固化目标、成功标准、权限、预算、恢复策略和结果期待。
- 每次 `commit_plan` 仍产生不可变的完整 `ClosureVersion`。
- 每个形成或更新可执行 realization 的 committed 版本，同时物化一个不可变的 `CapabilityPackageVersion`；纯 DAG/约束 patch 不额外制造无意义的包。
- 包内容使用 `ContentResource`/`ArtifactRef` 和 digest 引用；代码正文不是普通事件、日志或数据库字段。
- 能力包定义可以保留明确标记的 `provider_fillable` 系统 typed hole（例如 OS/arch/runtime 的选择）；这不是未处理的开放变量。目标 Slave 激活时必须填洞并产生带 evidence 的目标相关 `ComputeBinding`。
- `package_closure_version_ref` 指向能力包自己的闭包版本；`source_closure_version_ref` 只保存候选包来自哪个 Run/细化版本的 provenance。针对具体 Slave 填洞后产生的目标相关闭包/绑定也必须有独立版本引用，不能覆盖包定义。
- 提升不原地修改 `run_bound` 候选包，而是从它派生新的 `workspace_reusable` 包版本，保留完整 provenance。

### 2.2 `matmul` 不会被 `run_code` 替换

`matmul` 是应用操作和成功语义；`run_code` 是实现该操作的系统 executor。细化只补充实现，不改变原应用语义：

```text
高层闭包 C：

ProgramApplication = {
  operation_ref     = "matmul",
  semantics_digest  = <matrix-multiply-semantics>,
  input_schema      = <matrix-pair-schema>,
  output_schema     = <matrix-schema>,
  success_semantics = "Y = A × B"
}

ComputeRequirement = {
  requirement_id = "r_matmul",
  key             = "capability",
  value           = "matmul",
  view            = "application",
  constraint_ref  = <matmul-contract-constraint>
}

TypedHole = ?h_matmul : ComputeRealization
```

针对当前 Run 所选目标 Slave 的执行/activation 闭包 `C'_target` 保留上述 Application 语义，并增加下列目标相关系统实现。它不是可复用能力包定义本身；可复用包仍可保留声明过的 provider hole：

```text
ProgramSystems = {
  executor_kind = "run_code",
  effect_class  = <declared-effect-class>,
  permissions   = <effective-permissions>,
  package_ref   = <CapabilityPackageVersionRef>,
  replay_safety = <declared-replay-safety>
}

ComputeSystems = {
  requirement_refs       = [<run-code/runtime requirements>],
  admission_requirements = [<target/runtime/policy checks>]
}

ComputeBinding = {
  operation_descriptor_digest = <matmul descriptor digest>,
  package_version_ref          = <run-bound package version>,
  package_digest               = <program/package digest>,
  executor_descriptor_digest  = <run_code descriptor digest>,
  target_slave                 = <exact Slave>,
  runtime_profile              = <locked runtime profile>
}
```

正确性条件是 `A(C'_target) ⊨ A(C)`：`matmul` 的输入/输出 schema、成功语义、精度、预算、权限和其他必须守恒的约束不能因使用 `run_code` 而消失。`run_code` 可用只能证明存在执行机制，不能单独证明生成程序实现了 `matmul`；后者仍需 operation contract、validator 和运行证据。

实现细化也不要求目标操作与底层 executor 一对一对应。一个 `matmul` realization 可以由多个已有能力组合而成，例如把矩阵的每一行视为向量，使用已安装的 `vector×matrix` 能力完成子计算，再由父闭包聚合结果。此时：

- 父节点的 `ProgramApplication.operation_ref` 仍为 `matmul`，父级结果契约和成功标准不变；
- 细化后的子闭包/子节点分别携带 `vector×matrix` 的 operation contract 和精确 `ComputeBinding`；
- `run_code` 可以作为受控的编排或胶水实现，但不得直接绕过 Runtime/Observer 调用 Slave；
- 子结果的 schema、精度、预算、权限和本地性约束必须按传播规则进入子闭包，并由父级 validator 验证聚合结果；
- 当前版本不引入动态 DAG fan-out。若采用分解，必须在闭包版本中显式表达有限、可校验的子节点和聚合关系；需要动态 fan-out 时另行扩展设计。

### 2.3 一个聚合、两个正交状态面

为避免把“包已发布”和“某个 Slave 已可执行”混为一谈，`CapabilityPackage` 对外保持一个概念，内部保留两个正交状态面：

```text
CapabilityPackage = {
  versions:    CapabilityPackageVersion[],
  activations: CapabilityPackageActivation[]
}

CapabilityPackageActivation = {
  package_version_ref,
  target_slave,
  activation_closure_version_ref,
  compute_binding_ref,
  evidence_refs,
  activation_state
}
```

其中 package version 是不可变、尽量与目标无关的定义；activation 是同一聚合内针对某个 Slave 的 provider 细化和运行状态，不引入另一套“临时代码/能力包”概念。

```text
发布面：candidate → published | abandoned
作用域：run_bound → workspace_reusable   # 通过派生新版本完成，不原地改

激活面（按 target Slave）：
not_installed → provisioning → ready ↔ degraded → stopped
                              ↘ failed | lost
```

- `candidate/run_bound` 可以作为来源 Run 的锁定 realization 执行，但不进入 Workspace 的通用能力快照。
- `published/workspace_reusable` 表示用户批准其跨 Run 复用，但只有某个 Slave 的 activation 为 `ready` 时，该 Slave 才报告相应能力。
- 可复用包定义尽量不固化某一台 Slave 的 machine/runtime 选择；这些目标相关细节属于 activation/refinement 产生的 `ComputeBinding`。若包本身声明了 OS/arch/runtime 限制，其他 Slave 必须按该限制匹配或诚实失败。
- 能力名只用于发现。执行身份锁定 descriptor digest、package version/digest、executor descriptor、target Slave 和 runtime profile。

## 3. 统一生命周期

### 3.1 Run 内细化与执行

```text
coding-agent 明确 ClosureContract
→ open_run
→ query_capabilities 发现 matmul 缺口，但目标 Slave 有 run_code
→ apply_plan_patch 细化 matmul 的程序与系统 realization
→ 程序正文写 ContentStore，得到 program_ref + digest
→ 物化 run_bound CapabilityPackage candidate
→ bind_compute_hole 锁定 package + run_code + target Slave
→ inspect_plan_readiness
→ commit_plan
→ start_run
→ Slave fetch/verify candidate，在受控 staging 中经 run_code 执行
→ validator/criterion evidence
→ ExecutionOutcome
```

`apply_plan_patch` 可以增加一个包物化 patch op，但不增加新的顶层 MCP：

```text
materialize_capability_package_candidate {
  node_id,
  operation_descriptor_ref,
  program_content_ref,
  expected_program_digest,
  effective_constraint_refs
}
```

该 op 与 `bind_compute_hole`、`set_node_inputs` 等操作一样进入 Driver Mutation Lane，受 version CAS、幂等 operation ID 和完整快照校验约束。不得继续用 `set_execution_payload` 把代码正文当作普通业务输入；小型执行输入仍可 inline，程序内容必须有身份、digest 和访问绑定。

### 3.2 Run 后由用户提升

Run 终态后，Browser 向用户展示本 Run 产生的候选能力包。coding-agent 可以给出说明，但不能代替用户批准。用户可选择：

```text
abandon
  → 候选包按 run_bound retention 清理，只保留允许的 digest/provenance

promote
  → Observer 校验可提升性
  → 用户批准精确 package digest、operation contract、权限与 effect summary
  → 派生 workspace_reusable PackageVersion
  → 按用户选择的精确 target 发 CapabilityProvisionCommand
  → Slave fetch → verify digest → install → package tests → health check
  → CapabilityHealthReport + ResourceEventFrame
  → Observer 验证身份、session generation、evidence 和 package version
  → activation ready
  → 更新 LiveAgentCapabilitySnapshot
```

提升是独立 mutation，不修改原 Run 的 `ClosureContract`、committed `ClosureVersion` 或 execution。Observer
以候选包 lineage（`derived_from`）和确定性 package identity 作为幂等键；因此用同一个
candidate ref 重试时返回同一个 reusable 版本，不会再次派生新版本。请求仍可带稳定
idempotency key，并创建可对账的 package/resource 事件。

### 3.3 提升准入

Run 成功不等于候选包可复用。提升前至少校验：

1. package version、program digest、operation descriptor digest 全部确定；
2. operation contract 和应用语义已经闭合，不存在无法解释的开放项；明确标记为 `provider_fillable` 的系统 Compute hole 可以保留，以便在其他 Slave 激活时重新绑定；
3. operation contract 的 input/output schema、success semantics 稳定且可参数化；
4. 不捕获当前 Run 的业务输入、Secret、raw 本机路径、临时 credential 或仅本 Attempt 可读的引用；
5. 继承的约束要么保留为包约束，要么已有可核验 discharge evidence，不得静默丢失；
6. permissions、effect class、replay safety、OS/arch/runtime profile 和 Secret slot 声明完整；
7. package tests、health check 和必要 validator 通过；
8. 用户批准的 action digest 与最终 package digest、target、权限和 effect summary 完全一致；目标相关的绑定不被错误地当成包定义的一部分。

若候选包捕获了 Run-specific 数据或权限，promotion 返回结构化 blocker；系统不自动猜测如何参数化或放宽约束。coding-agent 可以基于 blocker 创建新的细化版本，再由用户重新选择。

## 4. 能力注册与执行身份

Observer 的能力快照由资源注册表投影产生，不再硬编码：

```text
LiveAgentCapabilitySnapshot
  = Slave 自有能力
  ∪ 内置 executor（包括 run_code）
  ∪ 已 published、已安装且 activation=ready 的能力包
```

`run_bound` candidate 不进入通用快照，只在来源 RunScope 中按精确 package ref 可见。候选包执行成功也不会自动注册为新的 Workspace 能力。

能力资源由 Slave self-report 并携带 package test、health check 和 resource lifecycle evidence；Observer 只在验证 manager 身份、package/digest、session generation 和 execution epoch 后接受，不能仅凭普通 Run terminal report 伪造 `ready`。

## 5. 组件增量

| 层 | 增量 |
|---|---|
| browser | 展示 Run 产出的候选包；提供显式 promote/abandon 操作和精确批准摘要 |
| coding-agent | 将能力缺口细化为 package candidate；保留应用语义和约束；根据结构化 blocker 再细化 |
| driver | `apply_plan_patch` 支持候选包物化；readiness 校验 package/binding；不自动提升、不直接向 Slave 派发 |
| observer | package/resource 权威存储、promotion 校验、用户授权、动态能力投影、事件与幂等对账 |
| slave | 内置受描述约束的 `run_code` executor；candidate staging；可复用包安装、测试、健康检查和 activation 管理 |
| worker service | promotion 使用 `CapabilityProvisionCommand`；Slave 回报 `CapabilityHealthReport`/`ResourceEventFrame` |
| content store | 保存程序和能力包内容；数据库与协议只保存 ref/digest/version |

## 6. Readiness、Commit 与执行保证

`inspect_plan_readiness` 和 `commit_plan` 必须检查：

- 对当前选定的执行 target，所有 required typed hole（包括 package 中的 `provider_fillable` hole）已绑定为完整 `ComputeBinding`；可复用包定义本身可以保留尚未针对某个 Slave 激活的 provider hole；
- `matmul` operation contract 仍存在且与候选包 descriptor 一致；
- target Slave 的精确 `run_code` descriptor、runtime profile、health 和 capacity 可用；
- package/program ref 存在、作用域正确、digest 匹配且目标 Slave 可读取；
- ClosureContract、WorkspacePolicy 和 RefinedConstraint 单调一致；
- permissions、effect、approval、budget、deadline、success coverage 和 PlanLimits 完整；
- semantic validator 未被普通 executor success 替代。

`commit_plan` 重新校验完整快照并锁定上述引用。`start_run` 只能执行该 committed 版本；后续 package promotion 或新版本生成不能静默替换正在运行的 Node。

## 7. 执行、安全与内容边界

`run_code` 是显式、可发现、带 descriptor 的系统 executor，而不是绕过 Runtime 的后门。其 descriptor 至少声明 executor kind、输入输出 envelope、permissions、effect class、runtime profile、replay safety 和资源限制。

第一阶段共用子进程 harness：

- 包内容写入 Attempt scratch，结构化输入经 stdin 传入，结构化输出经 stdout 返回；
- 超时、进程组隔离和可用的进程级资源上限用于崩溃隔离；
- crash/timeout/non-zero exit 转为结构化错误，不影响 Slave 主进程；
- 子进程隔离不声称等于通用 sandbox；网络与文件系统强制力按部署边界诚实声明；
- 未知外部副作用默认 `non_replayable`，不得作为普通 transient 自动重试；
- 代码、Prompt、路径、stdout/stderr、Secret 和第三方原始 payload 不进入普通日志、事件和错误正文；
- 小结果发布为带 identity/schema/digest/access binding 的 `ContentResource`，大结果走 Artifact 数据面。

### Conversation 与子操作超时

第一阶段不把“暂时没有协议事件”解释为 coding-agent 空闲超时：只要对应
Codex app-server 进程和 Conversation 仍然存活，Driver 就继续等待。Driver
仅设置 Conversation 的绝对 deadline，默认 24 小时（可由
`LOOM_CODING_AGENT_DEADLINE_SECONDS` 覆盖），到期返回
`coding_agent_deadline_exceeded`。WorkerSession 的 HTTP 调用和 Slave 上的
能力执行器各自拥有独立 timeout（分别为
`LOOM_WORKER_OPERATION_TIMEOUT_SECONDS` 与
`LOOM_CAPABILITY_OPERATION_TIMEOUT_SECONDS`），不会被误报为 coding-agent
idle timeout；中断请求优先于 deadline。

### App-server TurnLifecycle 与 ProtocolHealthMonitor

Driver 将 Codex app-server 的处理拆成两个独立状态机：

- `TurnLifecycle` 只处理明确的生命周期结论：`turn/completed` 的
  completed/failed/interrupted、`turn.error`、`thread/status/changed` 的
  `systemError`、`thread/goal/get` 或 goal 通知的 `blocked`/
  `usageLimited`/`budgetLimited`、进程退出和协议解码错误。
- `ProtocolHealthMonitor` 只判断 app-server 是否仍可通信。turn 为
  `inProgress` 时按 `LOOM_CODING_AGENT_POLL_INTERVAL_SECONDS`（默认 5 秒）
  主动发送 `thread/read {includeTurns:true}` 与 `thread/goal/get`，每种
  方法最多保持一个 in-flight 请求，避免无界堆积。

任何成功的 poll 响应（即使 `updatedAt`、items 和状态完全不变），以及
`item/*`、`turn/*`、agent/reasoning delta、`thread/status/changed`、
`thread/tokenUsage/updated`、goal 等合法通知，都会刷新 protocol heartbeat。
“内容没有变化”或“长时间没有 delta”不再是失败条件，健康的长生成可以保持
静默等待。

只有 poll RPC 连续返回错误或在
`LOOM_CODING_AGENT_PROTOCOL_FAILURE_SECONDS`（默认 60 秒）内持续没有响应，
ProtocolHealthMonitor 才发出 `coding_agent_stalled`（source 为
`protocol_health`）。Driver 将当前 Run 标记为 `failed`，在
`outcome.reason` 与事件中保存失败来源、thread/turn 和失败年龄；会话状态
由该 Run 聚合为 `failed`。Conversation 的 24 小时绝对 deadline 仍由 Driver
负责，是最终兜底；`waitingOnUserInput`/`waitingOnApproval` 始终保持
`thinking`，不被当作故障。

## 8. 错误模型

错误使用统一 `DomainErrorEnvelope`，至少覆盖：

- `capability_unavailable`
- `package_candidate_invalid`
- `package_content_unavailable`
- `package_digest_mismatch`
- `package_promotion_denied`
- `package_captures_run_state`
- `capability_install_failed`
- `capability_health_check_failed`
- `capability_exec_error`
- `capability_timeout`
- `capability_memory_limit`
- `coding_agent_deadline_exceeded`
- `coding_agent_stalled`
- `coding_agent_blocked`
- `coding_agent_usage_limited`
- `coding_agent_budget_limited`
- `thread_system_error`
- `worker_operation_timeout`

每个错误包含安全的 `code/category/retryable/operation_ref` 和 allowlisted details。版本冲突重新读取后 patch；policy/approval deny、validation fail、未知副作用和 promotion blocker 不作为普通 transient 自动重试。

## 9. 测试

1. `matmul` 细化：能力快照没有 `matmul`、有 `run_code`；生成 candidate、为当前选定 target 完整绑定、readiness、commit、start，结果满足矩阵乘 validator。
2. 语义守恒与组合细化：细化前后 `operation_ref=matmul`、schema、required criteria 和有效约束不丢失；实现可以是 `run_code` 直接执行，也可以是显式组合 `vector×matrix` 等子能力，父级 validator 必须通过。
3. Candidate 隔离：Run 内可执行，但不出现在其他 Run 的通用能力快照。
4. 用户提升：候选包 → 用户精确批准 → provisioning → tests/health → activation ready → 后续 Run 可发现并绑定。
5. 拒绝提升：捕获业务输入、Secret、raw path、operation contract/应用语义不闭合、无法声明 provider 填充规则的系统 hole 或 digest 不一致时返回结构化 blocker。
6. 不可变性：promotion 不修改来源 Run 的合同、版本、execution 或结果 provenance。
7. Worker fencing：重复 provision 幂等；旧 session generation/execution epoch 的迟到 health report 被拒绝。
8. 崩溃隔离：代码异常、超时、内存不足后 Slave 仍健康，错误不泄露代码/stdout/stderr。
9. Fake provider 负责确定性领域测试；真实 WorkerSession 覆盖 package fetch、digest、provision、health report 和 resource event 的契约测试。
10. 可移植性：同一个保留 provider hole 的 package 在满足约束的 slaveA/slaveB 上分别生成不同的 target binding；包含不可移植 target 细节的包在不匹配的 Slave 上诚实失败，而不是伪造 ready。

## 10. 范围外与 v2 边界

以下内容不属于本阶段：

- Runtime/Observer 自动生成代码、自动修复代码或自动提升候选包；
- 未经用户批准把 Run 产物发布为 Workspace 能力；
- 通用 sandbox、容器隔离、cgroup 强制、netns 或全机网络隔离；
- public capability catalog、签名发布、推荐、吊销治理和能力市场；
- 多包依赖解析、远程构建农场和跨版本自动迁移；
- 自动把捕获 Run 状态的程序泛化成可复用 operation contract。

本设计中的“生成”发生在 coding-agent 的显式任务闭包细化阶段，结果先成为 `run_bound` package candidate；它不同于 v2 §12.1 排除的“Runtime 在缺能力时自动构建并发布可复用 package fallback”。真正的跨 Run 复用必须由用户显式 promotion，并重新经过 package、权限、约束和目标 Slave 准入。

## 11. 实施顺序

1. `CapabilityPackageVersion` 聚合、run-bound scope、ContentStore ref/digest 和 provenance；
2. `apply_plan_patch` 候选包物化、typed-hole binding、readiness/commit 完整校验；
3. Slave `run_code` descriptor、candidate fetch/staging 和子进程 harness；
4. 当前 Run 的 candidate 端到端执行与 validator evidence；
5. Browser 候选包列表和用户 promote/abandon API；
6. Observer promotion 校验、WorkerService capability provisioning 和动态能力投影；
7. 测试 1–9，并确认真实 Codex app-server 与 Fake provider 都只能经 Driver MCP/Runtime 触发上述流程。
