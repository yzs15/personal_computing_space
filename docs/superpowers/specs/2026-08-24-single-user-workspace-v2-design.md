# Loom v2 单用户单 Workspace 垂直切片设计

> 状态：已确认 TypedTerm/VocabularyRegistry/TermSupport 方向，待实施计划与代码验收。  
> 日期：2026-08-24  
> 权威输入：`../computility/单用户单Workspace核心架构设计v2.md`

## 1. 目标与验收范围

本项目在 `.` 建立一个物理隔离于旧 `../loom` 的 Python project，落地 v2 文档的 M0 单域管道和 M1 单域价值灯塔。首版必须可通过 Docker Compose 启动，并提供 Conversation-first Web 体验。

首版验收包含：

1. 同一 Conversation 内完成 `query_capabilities → open_run → apply_plan_patch* → inspect_plan_readiness → commit_plan → start_run → execution → ExecutionOutcome/ResourceRef → close_run`；每轮 patch 都产生可对账的 DraftClosureVersion，未 commit 草稿不得执行。
2. 事件序列可重放为 RunProjection；结果能够沿 provenance 追溯到 Run、ClosureVersion、Execution、Attempt 和 Slave。
3. M1 A′：在 Node 的 `RefinedConstraint.allow_reassignment=true` 且 replay-safe 的任务中，Slave A 下线后切换 Slave B；稳定 `TaskRef`、execution 身份、ClosureVersion 和约束保持不变，新 Attempt 和改派原因有记录。
4. M1 C′：细化 patch 明确携带 `RefinedConstraint` 到 Node；Runtime 验证只能收紧预算/允许效果/目标范围，放宽提交被拒。本切片不宣称 AP1 的全自动传播/交换性定理。
5. M1 B′：WorkspacePolicy 在 commit/admission 处阻止违规动作；可检查约束留下 validator/attestation/readiness evidence，平台不伪造 `ready`/`stopped`。
6. 默认 coding-agent 后端是 Codex app-server；Fake coding-agent 仅用于离线测试、故障注入和确定性验收。Codex 默认模型为用户指定的 `deepseek-v4-flash`。

明确不在首版范围：多用户、多 Workspace、跨域授权、动态 DAG fan-out、循环/脚本 guard、通用 sandbox/cgroup、复杂审批治理、对象存储大文件、OIDC、生产 HA、真实能力包在线构建。契约层保留 decompose/aggregate/retry 的传播字段，但 M0/M1 不执行动态 fan-out、循环或自动 reduce；不支持的形态返回结构化 `unavailable`/`invalid`，不静默猜测。`ApprovalRequest`/`DecisionRequired` 数据结构和状态投影存在，首版只覆盖默认低风险动作，完整人工审批工作流后置。

## 2. 设计原则

- **单用户单 Workspace**：首次迁移固定建立一个 user、一个 workspace、一份 WorkspacePolicy；运行期不提供第二用户/Workspace API。
- **细化权唯一**：coding-agent 是唯一语义细化者；Observer 和确定性 Runtime 不生成或修改闭包。
- **执行与细化分离**：`CommittedClosureVersion` 是执行输入；`start_run` 锁定版本后，后续再细化只产生新版本，不静默替换运行中的 Node。
- **Observer 状态权威**：Run、ClosureVersion、Execution、Attempt、RunEvent、ResourceProjection 的最终事实只由 Observer 接受和投影。
- **确定性最小面**：Runtime 只做 CAS、引用、DAG、约束、策略、READY、派发、幂等和证据校验，不调用 LLM。
- **引用与内容分离**：提交闭包只接受已绑定的 ResourceRef/ArtifactRef/NodeOutputRef 等，不接受裸 URL、跨 Slave 本地路径或未绑定内容。
- **跨边界幂等**：所有 mutation 使用稳定 operation/idempotency key；超时视为结果未知，只能用同一 key 对账。
- **本地状态与控制面分离**：每个组件有独立 PostgreSQL 实例；Slave 本地库只保存执行账本和 Replica 事实，不能自行写 Run 终态。

## 3. 总体架构

```text
Browser (Conversation-first)
        │ REST + SSE
        ▼
Observer ───────────── observer-db (PostgreSQL)
  │  control plane / Run / Task Bus / Resource / events
  │
  ├─ Driver worker session ── Driver ── driver-db (PostgreSQL)
  │                              │
  │                              ├─ fake-coding-agent（测试后端）
  │                              └─ codex app-server（默认后端，继承本机 config.toml）
  │
  ├─ Slave worker session ─── slave-a ── slave-a-db (PostgreSQL)
  │
  └─ Slave worker session ─── slave-b ── slave-b-db (PostgreSQL)
                                      
          各 Slave 挂载独立 WorkspaceReplica volume
```

部署分成两个明确 profile，避免容器内 Codex 无法读取本机 provider/loopback endpoint：

- **默认体验 profile**：`scripts/dev-up.sh` 先用 Docker Compose 启动四个隔离 PostgreSQL、Observer、两个 Slave 与 Web 静态资源，再在宿主机启动 Python Driver。Driver 直接启动本机 `codex app-server --listen stdio://` 子进程，继承运行用户的 `~/.codex/config.toml` 与环境。该 profile 默认 backend=`codex`。
- **确定性测试 profile**：Docker Compose 同时启动容器化 Driver 和 Fake coding-agent，不依赖本机 Codex、模型服务或凭据。该 profile 只用于自动化测试、故障注入和离线验收。

两种 profile 使用同一套 Driver/Observer 契约和 driver-db schema；只替换 `CodingAgentProvider`，不复制密钥到 Loom 配置。

### 3.1 Observer

Observer 是唯一控制面和 Task Bus：

- 保存固定身份、ConversationBinding/slot、WorkspacePolicy、Agent/Session；
- 保存 ClosureContract、ClosureVersion、Node、Execution、Attempt、Dispatch；
- 承载 Workspace-local GRIP projection：Resource Registry、Descriptor、Resolver、Index/query；
- 事务写 RunEvent/ResourceEvent 并维护 Projection 与 outbox；
- 校验 Driver/Slave token、Workspace、session generation、execution epoch；
- 提供 Browser REST/SSE、Driver API、Slave WorkerService；
- 不保存或重建 Message 正文，不替 coding-agent 做语义判断。

### 3.2 Driver

Driver 是每个 active Conversation 的单写者：

- 在 driver-db 保存 Conversation、Message、Turn、RunScope、草稿、能力快照、cursor 和 mutation journal；
- 绑定一个 `CodingAgentProvider`，默认 `CodexAppServerProvider`，测试使用 `FakeCodingAgentProvider`；
- 每个 active Conversation 独占一个 Driver/CodingAgent session；inactive Conversation 没有 live app-server/MCP runtime。
- 通过 Driver MCP/内部 tool facade 调用 `open_run`、patch、commit、start、observe、resolve、close；
- 判定 Node READY、创建 ReadyDispatchIntent、有界对账和结果汇报；
- 不直接向 Slave 派发，不把事件正文自动注入下一个模型 turn。

### 3.3 Coding-agent providers

统一接口：

```text
StartConversation(conversation_ref, workspace_root) -> AgentSession
SendTurn(session, user_message, tool_endpoint) -> streamed AgentEvent
Interrupt(session, expected_turn_ref)
Close(session)
```

`FakeCodingAgentProvider` 固定生成可执行的 echo/hash/sort 单 Node 闭包，用于单元测试、PostgreSQL 集成测试、故障注入和无外部网络的 M0 验收。

`CodexAppServerProvider`：

1. 默认体验 profile 在宿主机启动 `codex app-server --listen stdio://`；
2. 建立 JSONL JSON-RPC 连接，发送 `initialize`，再发送 `initialized`；
3. 以 `thread/start` 创建 Thread，默认传入 `model: "deepseek-v4-flash"`；
4. 以 `turn/start` 发送 Conversation 输入，读取 `item/*`、`turn/*` 事件；
5. 将 Driver MCP 工具作为受控工具入口暴露给 Thread；模型只能通过这些工具完成闭包细化；
6. 对 interrupt、进程退出、协议错误返回结构化 `DomainErrorEnvelope`，不把不可确认的 mutation 自动重发。

模型值可由 `LOOM_CODEX_MODEL` 覆盖，但默认必须是 `deepseek-v4-flash`。Provider 不写入或修改用户的 `~/.codex/config.toml`；Codex CLI 自己按官方配置加载 provider、base URL 和凭据。Loom 保留这个精确模型名，但不声称第三方 provider 一定提供它；模型不可用时返回 `coding_agent_unavailable`。真实后端不可用时不静默降级为 Fake，用户/测试必须显式切换后端。

官方协议依据：Codex app-server 使用 JSON-RPC 风格双向消息，stdio 是默认 transport；连接必须先 `initialize`/`initialized`，随后可 `thread/start` 与 `turn/start`。参考 [Codex App Server 官方文档](https://developers.openai.com/codex/app-server) 与本机 `codex app-server --help`。

### 3.4 Slave 与 WorkspaceReplica

每个 Slave 是独立 WorkerService 和独立 PostgreSQL 实例。它保存：

- `WorkspaceReplica` 绑定：workspace_id、目录身份、版本/digest、`ready|dirty|unavailable`；
- Attempt/Dispatch 本地幂等账本、execution epoch、ACK/terminal receipt；
- executor staging、cleanup journal、资源 supervisor 状态与 lease；
- 能力快照缓存、package digest 和 health/readiness evidence。

真实文件位于独立 volume；raw 本机路径不进入 Observer 事件、错误或 LLM 上下文。Slave 本地库不能创建 Run terminal，也不能越过 Observer 调度。

### 3.5 Python 实现边界与装饰器语法糖

实现使用 Python 3.12+、FastAPI/Starlette（HTTP/SSE）、Pydantic v2（模型与 JSON Schema）、SQLAlchemy 2 + asyncpg（四个 PostgreSQL 实例）、Alembic（migration）、pytest（单元/集成/E2E）和 asyncio subprocess（Codex app-server stdio）。这些是 Layer 1 的实现选择，不改变 Layer 0 的语言无关语义。

为方便以 Python 表达“约束附着在任务闭包上”，提供装饰器 DSL，但装饰器只接受结构化声明，不接受任意 predicate/lambda，也不把 Python 函数的闭包环境当作 Task Closure：

```python
@task_closure(
    goal="hash the supplied dataset",
    data=DataApplication(...),
    program=ProgramApplication(operation_ref="grip://loom/operation/hash"),
    compute=ComputeSpec(capability_intent="cpu", typed_holes=[compute_hole("h_compute")]),
)
@attach_constraint(
    ConstraintSpec(
        subject="compute.budget",
        predicate={"op": "le", "field": "cpu_seconds", "value": 60},
        source="workspace_policy",
        fate="preserve",
        propagation={"detail": "keep_or_tighten", "retry": "cumulative"},
    )
)
def hash_task():
    ...

closure_contract = hash_task.materialize_contract()
```

`materialize_contract()` 只读取装饰器生成的显式 metadata，输出 canonical `TaskClosure`/`Constraint` JSON；Runtime 不执行 `hash_task` 来推断语义，不扫描函数体，不运行装饰器传入的代码。所有 patch、CAS、commit、provenance 和 `A(C') ⊨ A(C)` 校验仍走 `apply_plan_patch`/`commit_plan`，装饰器不能绕过 Driver/Observer。

推荐项目边界：`loom_v2/contracts/`（Layer 0）、`loom_v2/observer/`、`loom_v2/driver/`、`loom_v2/slave/`、`loom_v2/coding_agents/`、`loom_v2/web/`、`migrations/`、`tests/`。运行时包之间只依赖 `contracts` 和版本化 HTTP/JSON Schema，不互相导入内部实现。

## 4. Layer 0 契约内核：任务闭包、细化与算力绑定

本节是语言无关的语义数据模型，先于 Python/Pydantic model、SQL 表和 HTTP DTO。实现可以用 Python dataclass/Pydantic、JSON Schema 或 PostgreSQL JSONB 表达，但必须保持这些字段的语义和不变量。

### 4.1 Task Closure 的 3×2 结构

任务闭包不是一次 RPC 参数，而是一个可持续细化、可版本化、可追溯的一等对象。主体是 `⟨D, P, C⟩`，每一元都同时有 Application Semantics 与 Systems Concerns 两个视图；机器、runtime、environment 等物理实现不直接暴露为公共闭包字段。

```text
TaskClosure = Envelope + Body + EffectivePolicySet + ResultExpectation

Body = {
  data:    SpecPart<DataApplication,   DataSystems>,
  program: SpecPart<ProgramApplication, ProgramSystems>,
  compute: SpecPart<ComputeApplication, ComputeSystems>
}

SpecPart<A, S> = {
  application: A,       // 用户工作语义：目标、输入/输出语义、成功标准
  systems: S,            // 平台义务：权限、审计、隔离、保留、可观测性
  terms: [TypedTerm],    // 可扩展、类型化的语义项；不把词汇表硬编码进闭包结构
  refs: [ResourceRef],   // 已绑定的资源/程序/数据指称，不允许裸路径或裸 URL
  constraints: [Constraint]
}
```

`application`/`systems` 仍然是 3×2 的稳定语义边界，但它们不是一个封闭的字段枚举。每个视图由一组**语义锚点**和可扩展的 `TypedTerm` 组成。锚点确保闭包可被解释、细化和追溯；新领域语义通过注册 term 加入，不需要修改 `TaskClosure` 的顶层结构。下列锚点是本版本的最小投影；具体 wire encoding 留给实现，但不能删掉这些语义面：

```text
DataApplication  = { logical_inputs, schema_digest, identity_criterion, expected_cardinality }
DataSystems      = { locality, classification, access_profile, retention_profile }
ProgramApplication = { operation_ref, semantics_digest, input_schema, output_schema, success_semantics }
ProgramSystems     = { executor_kind, effect_class, permissions, package_ref, replay_safety }
ComputeApplication = { capability_intent, requirement_refs, result_expectation }
ComputeSystems     = { requirement_refs, admission_requirements }

ComputeRequirement = {
  requirement_id: OpaqueId,
  key: VocabularyTerm,
  value: StructuredValue,
  view: "application" | "systems",
  constraint_ref: ConstraintRef?   // 约束语义的唯一归属；匹配-only 要求可为空
}
```

`capability`、`precision`、`parallelism`、`budget`、`locality`、`network`、`deadline` 不再是不可扩展的枚举，而是首批命名空间 term，例如 `loom.compute.precision.v1`、`loom.compute.parallelism.v1` 和 `loom.data.locality.v1`。它们的结构化值、比较/细化关系、传播规则和证据要求由词汇注册表定义。若某项需要传播、强制或审计，唯一语义真相是它引用的 `Constraint`；`TypedTerm`/`ComputeRequirement` 只保存该约束在特定视图中的投影，避免同一条件在 ComputeSpec 和 Constraint 中分叉。

#### TypedTerm 与词汇注册

```text
TypedTerm = {
  term_id: OpaqueId,
  kind: VocabularyTerm,              // 稳定命名空间，例如 loom.compute.precision
  schema_ref: SchemaRef,             // 版本化 schema + digest
  value: StructuredValue,
  criticality: "required" | "advisory",
  constraint_ref: ConstraintRef?,    // 该 term 若承载约束，指向唯一约束对象
  refinement_relation_ref: RelationRef?,
  provenance: [ProvenanceLink]
}

VocabularyRegistryEntry = {
  kind: VocabularyTerm,
  schema_ref: SchemaRef,
  value_schema: SchemaRef,
  refinement_relation: RelationRef,
  propagation_rules: PropagationProfile,
  evidence_kinds: [EvidenceKind],
  compatibility: CompatibilityProfile,
  registry_version: VersionRef
}

VocabularyRegistry = {
  registry_id: OpaqueId,
  version: VersionRef,
  entries: [VocabularyRegistryEntry],
  digest: Digest,
  provenance: [ProvenanceLink]
}
```

注册表是 term 的语义权威，不是某个 Slave 的私有配置。它定义 schema、版本兼容、`A(C') ⊨ A(C)` 的细化关系、传播/命运标签以及可接受的 evidence 类型。实现可以把注册表内置在 `contracts` 包或由 Observer 以版本化资源发布，但每个 ClosureVersion 必须记录所使用的 registry digest。

视图的最小锚点与扩展 term 的关系如下：

| 视图 | 稳定锚点（本版本必须可投影） | 可扩展部分 |
|---|---|---|
| Data/Application | logical inputs、schema digest、identity criterion、expected cardinality | 数据质量、分区、脱敏、采样等 `loom.data.*` terms |
| Data/Systems | locality、classification、access/retention profile | 加密、审计、驻留、传输等 `loom.data.systems.*` terms |
| Program/Application | operation ref、semantics/input/output schema、success semantics | 算法参数、确定性声明、质量目标等 `loom.program.*` terms |
| Program/Systems | executor/effect/permission、package ref、replay safety | 沙箱、设备、依赖和供应链证明等 `loom.program.systems.*` terms |
| Compute/Application | capability intent、requirement refs、result expectation | 精度、并行度、成本目标等 `loom.compute.*` terms |
| Compute/Systems | requirement refs、admission requirements、typed holes/bindings | 网络、调度、隔离、运行时证据等 `loom.compute.systems.*` terms |

因此“最小字段”不是把所有未来概念一次性塞进结构，而是保证三类主体在应用/系统两侧都有可解释锚点；新语义优先作为注册 term 增量演进。删除锚点会破坏闭包的闭合性、细化关系或 provenance；新增 term 则只需注册 schema/关系并更新支持声明。

`Envelope` 至少包含 `closure_id`（稳定任务型资源标识）、`schema_version`、`lineage`、`origin_conversation_ref`、`declared_by`、`authorization_ref`、`result_expectations` 和 `provenance`。稳定标识指向任务闭包家族；每个可执行快照另有 `closure_version`，一次 Execution 只引用一个确定版本。

### 4.2 ComputeSpec、typed hole 与 ComputeBinding

`ComputeSpec` 描述能力和约束，不等于某台机器：

```text
ComputeSpec = {
  capability_requirements: [CapabilityRequirement],
  operation_ref: OperationRef,
  requirements: [ComputeRequirement],
  typed_holes: [TypedHole<ComputeRealization>]
}

TypedHole<T> = {
  hole_id: OpaqueId,
  type: "ComputeRealization",
  constraint_refs: [ConstraintRef], // Φ_compute(hole)
  status: "unbound" | "bound",
  binding_ref: ComputeBindingRef?
}
```

`PrecisionRequirement`、`ParallelismRequirement`、预算、locality、network 和 deadline 在 `ComputeSpec` 中只出现为 `ComputeRequirement` 视图；如果它们是需要传播/强制/审计的条件，唯一的语义对象是被 `constraint_ref` 指向的 `Constraint`，不能在 `ComputeSpec` 和 Constraint 中各维护一份。只有用于能力发现的 matching-only requirement 可以没有 Constraint 引用。

`ComputeBinding` 是对 typed hole 的不透明绑定结果：

```text
ComputeBinding = {
  binding_id: OpaqueId,
  hole_id: OpaqueId,
  capability_descriptor_ref: ResourceRef,
  capability_package_ref: ResourceRef?,
  target_resource_ref: ResourceRef,       // 精确 Slave/能力资源
  realization_digest: Digest,             // 物理组合的承诺，不展开为公共字段
  constraint_evidence_refs: [EvidenceRef],
  bound_by: RefinementActorRef,
  bound_at: Timestamp
}
```

在本单域投影中，coding-agent 通过持续 `apply_plan_patch` 提出/补全 `ComputeBinding`，Driver Runtime 做确定性匹配和校验，Observer 在 `commit_plan` 时接受完整绑定；Slave 不得在 committed 之后静默修改绑定。能力包部署时 Provider/Slave 可以填能力包自身剩余的 typed hole，并以 `CapabilityHealthReport`/evidence 返回；任务闭包的公共 `ComputeBinding` 仍由 Driver/Observer 早绑定并锁定。Provider 可以在 `realization_digest` 对应的内部实现中自由选择 machine、runtime、environment、网络、存储和调度；对外只需证明 `Φ_compute(binding)` 成立并提供证据。物理执行实现由 Execution/Attempt provenance 记录，不进入高层闭包的公共结构。

约束是闭包上的一等对象，不是散落在 HTTP 参数里的布尔值：

```text
Constraint = {
  constraint_id: OpaqueId,
  source: Requester | Provider | ResourceOwner | Institution | Regulation,
  relaxability: RequesterMayRelax | AuthorityOnly | Immutable,
  enforcement: AdmissionBlock | RuntimeBlock | TerminalCheck | Attestation | RecordOnly,
  subject: [D | P | ComputeSpec | Envelope | ExecutionEnvironment],
  propagation: DetailPolicy × DecomposePolicy × AggregatePolicy × RetryPolicy,
  fate: Preserve | Discharge,
  evidence_requirements: [EvidenceRequirement],
  provenance: [ProvenanceLink]
}

ConstraintRef = { constraint_id: OpaqueId, version: VersionRef, digest: Digest }

RefinedConstraint = {
  refined_constraint_id: OpaqueId,
  parent_constraint_id: OpaqueId,
  node_id: NodeRef,
  tightening_delta: ConstraintDelta,
  allow_reassignment: Boolean,
  relation: "preserves_or_tightens",
  discharge_evidence_refs: [EvidenceRef]
}
```

`EffectivePolicySet` 是 `ClosureContract.declared_constraints + WorkspacePolicy + Resource/Operation constraints + RefinedConstraint` 的带来源归并结果；冲突返回 `policy_conflict`，不能由客户端猜测取舍。

### 4.3 ClosureContract、ClosureVersion 与 Execution

```text
ClosureContract = {
  closure_id: TaskRef,
  goal: Goal,
  required_success_criteria: [Criterion],
  allowed_effects: EffectSet,
  resource_budget: ResourceBudget,
  recovery_policy: RecoveryPolicy,
  result_expectations: [ResultExpectation],
  declared_constraints: [Constraint],
  body: TaskClosure.Body
}

ClosureVersion = {
  version_id: VersionRef,
  closure_id: TaskRef,
  parent_version: VersionRef?,
  kind: "draft" | "committed",
  snapshot: TaskClosure,
  nodes: [Node],
  edges: [DependencyEdge],
  effective_policy_set: EffectivePolicySet,
  derived_constraints: [RefinedConstraint],
  compute_bindings: [ComputeBinding],
  snapshot_digest: Digest,
  patch_cursor: PatchCursor,
  provenance: [ProvenanceLink]
}

Execution = {
  execution_id: ExecutionRef,
  closure_version_ref: VersionRef,       // start 时锁定，之后不可替换
  state: ExecutionState,
  attempts: [AttemptRef],
  execution_epoch: MonotonicInt,
  outcome: ExecutionOutcome?
}
```

`Node` 至少包含 `node_id`/`lineage`、`operation_ref`、输入 `ResourceRef`、输出 schema、`ComputeSpec`/`ComputeBinding`、Node 级 `RefinedConstraint`、guard 和精确 target。运行中的 Node 不被后续版本静默替换；语义目标改变必须结束旧 Run 并显式开新 Run。

`ClosureContract` 在 `open_run` 时固化为不可原地修改的高层锚点；`ClosureVersion` 是该锚点下的完整快照。Draft/Committed 事实分离：Draft 不进入 Observer Task Bus、Slave 或执行；只有 Committed 版本可被 `start_run` 引用。

### 4.4 apply_plan_patch 是持续、可对账的细化操作

`apply_plan_patch` 不是一次性提交完整计划，而是同一个 Run 内可跨多个 coding-agent turn 持续调用的 Draft mutation：

```text
ApplyPlanPatch = {
  operation_id: OpaqueId,
  run_id: RunRef,
  base_draft_version: VersionRef,
  base_snapshot_digest: Digest,
  patch_id: OpaqueId,
  ops: [PatchOp],
  actor: CodingAgentRef
}

PatchOp =
    add_data_ref | refine_data_semantics
  | set_program_ref | refine_program_semantics
  | set_compute_spec | add_typed_hole | bind_compute_hole
  | add_node | replace_node_lineage | add_dependency_edge
  | add_constraint | tighten_constraint | set_result_expectation
```

每次调用的确定性语义：

1. Driver single-writer lane 先检查 `operation_id` replay；相同 key 返回原 receipt。
2. Observer/Driver 以 `base_draft_version + base_snapshot_digest` 做 CAS；冲突时不产生部分效果，返回 `version_conflict`，调用方读取当前 draft 后重新生成 patch。
3. 校验通过后，将 `ops` 追加到 patch journal，产生新的 `DraftClosureVersion` 和递增 `patch_cursor`；旧 draft 快照不变。
4. 响应包含完整的新 draft digest、未绑定 typed holes、readiness blockers、约束 diff、effective policy preview 和 provenance link；Draft 不进入 Task Bus/Slave。
5. 可以在后续 turn 重复 `apply_plan_patch`，直到 `inspect_plan_readiness` 没有 blocker。只有 `commit_plan(draft_version, digest)` 才把完整快照冻结为 `CommittedClosureVersion`。
6. `commit_plan` 再次校验 `A(C') ⊨ A(C)`、引用/绑定/目标/策略/成功标准覆盖和 PlanLimits；失败不污染上一 committed 版本。

细化不变量：对每次从 `C` 到 `C'` 的 patch，应用保护集保持 `A(C') ⊨ A(C)`；每条 `preserve` 约束必须在新版本有对应下层约束，或带 `discharge evidence`，不得静默消失。单域投影固定 Application/Systems 边界，AP3b 的边界迁移不在本实现；`fate=preserve|discharge` 仍显式记录。M1 由 coding-agent 手工派生 `RefinedConstraint`，Runtime 只做确定性单调校验，不伪称已经实现 AP1 自动传播。默认 detail 传播是保持或加严；decompose/aggregate/retry 的复制、额度拆分、定向、reduce、累计/重置/计次规则作为同一 Patch/ClosureVersion 的显式字段，不能由 Runtime 猜测。

### 4.5 Closure / execution / resource 的命名与 provenance

任务型资源的稳定 `TaskRef` 指向闭包家族，不等同于 `ClosureVersion` 或 Execution。结果必须由 `ResourceRef` 表示，并记录：`task_ref → closure_version → execution → attempt → compute_binding → target_resource → evidence → result_resource`。改派只改变新的 Attempt/target provenance，不改变任务标识和已锁定闭包版本。

### 4.6 GRIP 投影、ResourceDescriptor 与 CapabilityDescriptor

宏观架构的 C1–C3 在单域中收缩为 Workspace-local 的稳定资源入口，不宣称跨信任域 GRIP 已经实现：

```text
ResourceRef = {
  resource_id: OpaqueId,          // 可映射为 grip://workspace/<ws>/resource/<id>
  version_or_digest: Version|Digest,
  access_binding: AccessBinding,
  identity_criterion: IdentityCriterion,
  provenance: [ProvenanceLink]
}

ResourceDescriptor = {
  resource_ref: ResourceRef,
  kind: capability | capability_package | content | workspace_state | service | compute | receipt,
  schemas: [SchemaDigest],
  operations: [OperationDescriptor],
  capabilities: [CapabilityDescriptor],
  effective_policy_set: EffectivePolicySet,
  bindings: [BindingDescriptor],
  management_profile: ResourceLifecycleProfile,
  declaration_evidence: [SelfReport | HealthProbe | PackageTest | BusinessAttestation],
  provenance: [ProvenanceLink]
}

CapabilityDescriptor = {
  operation_name: OperationName,
  executor_kind: ExecutorKind,
  input_schema_digest: Digest,
  output_schema_digest: Digest,
  permissions: [Permission],
  external_effect_class: Known | Idempotent | NonReplayable,
  required_target_profile: TargetProfile,
  secret_slots: [SecretSlot],
  capability_package_closure_ref: TaskRef?,
  term_support: [TermSupport]
}

TermSupport = {
  kind: VocabularyTerm,
  schema_ref: SchemaRef,
  support: [
    "parse",       // 能读取并按注册 schema 做结构校验
    "preserve",    // 细化/转交时原样保留或按关系收紧
    "match",       // 能用于能力发现和路由匹配
    "validate",    // 能确定性判断闭包/绑定是否满足该 term
    "enforce",     // 执行中或准入时能阻止违反
    "discharge"    // 能以声明的 evidence 兑现该约束
  ],
  execution_stages: ["commit" | "admission" | "execute" | "terminal"],
  evidence_kinds: [EvidenceKind],
  support_version: VersionRef
}

ResourceLifecycleProfile = {
  state_machine: StateMachine,
  readiness_evidence_levels: [Process | Endpoint | ProviderProbe | BusinessAttestation],
  lease_and_retention: LeaseRetention,
  control_operations: [OperationName],
  manager_authority: Provider | Platform | External,
  observability: Full | Partial | Opaque
}

ResourceInstance = {
  resource_ref: ResourceRef,
  owner: OwnerRef,
  manager: ManagerRef,
  lifetime: SlaveAttached | RunBound | LeaseBound | WorkspaceBound | External,
  lifecycle_profile_ref: ResourceLifecycleProfileRef,
  state: ResourceState,
  descriptor_digest: Digest
}
```

`Index` 只做能力/资源发现，`Resolver` 只把稳定 `ResourceRef` 解析到权威 Descriptor，`DESCRIBE` 只返回调用者有权看到的自描述；发现不授予访问权，解析不创建执行。M0 可以只实现 Observer 内置 registry，但必须保留这三个职责的边界。

#### Required term 的识别与支持边界

`required` 是闭包契约的一部分，不能因为某个组件“不认识”就丢弃或降级为 `advisory`。识别与执行按职责分层：

1. **VocabularyRegistry/Observer** 是共同契约权威：确认 `kind + schema_ref` 已注册、schema/value 可解析、term 的 constraint/refinement relation 合法，并在事件/provenance 中保留 registry digest。Observer 不需要理解每个领域值的业务含义，但必须能做通用结构校验、版本兼容检查和未知 required 的拒绝。
2. **Driver/coding-agent** 负责把用户意图细化成 canonical `TypedTerm`/`Constraint`，并根据 registry 做 `parse`、`validate`、`preserve` 和 `match`。coding-agent 可以提出 term，却不拥有接受、放宽或伪造 evidence 的权力。
3. **Slave** 不必理解全量词汇，只需在 `CapabilityDescriptor.term_support` 中声明自身支持的子集。目标节点若要求 `match`、`validate`、`enforce` 或 `discharge`，路由/准入必须找到具有对应 stage support 的 Slave；自报告不能被 Observer 直接伪造成已验证事实，仍需 health/package evidence。

规则固定为：

- `required + preserve` 必须在每次细化、改派和重试中保留或收紧；最终执行目标还必须具备该 term 所要求的 `enforce` 或 `discharge` 能力。
- `required` 但注册表未知、schema 不兼容、或目标能力缺少所需 stage support，返回结构化 `capability_unavailable`/`validation_failed`，不得静默执行。
- `advisory` 可以由不理解的组件按 schema/digest 原样 round-trip 保存，但该组件不得声称已匹配、验证、强制或兑现它。
- `parse/preserve` 与 `match/validate/enforce/discharge` 是可独立声明的能力；因此一个 Slave 可以安全转交未知 advisory term，却不能接收必须在执行点强制的 required term。

能力包不是裸脚本：它是带 operation 契约的可复用 Task Closure，可停在任意细化层；Slave 部署并通过 package test/health check 后，生成一个 `capability_package` ResourceInstance，随后该资源可被 `ComputeBinding` 引用。

### 4.7 结果、事件、控制和决策 envelope

```text
ResultExpectation = {
  result_kind: ResourceKind,
  delivery_path: Direct | ObserverRelay,
  persistence_owner: Observer | Slave | External,
  lifecycle_profile_ref: ResourceLifecycleProfileRef,
  retention: RetentionPolicy,
  control_authority: ManagerRef
}

RunEvent = {
  run_id: RunRef,
  sequence: MonotonicInt,
  activity: Accepted | Active | Suspended | Terminal,
  phase: String,
  outcome: Outcome?,
  execution_ref: ExecutionRef?,
  causal_operation_id: OpaqueId,
  evidence_refs: [EvidenceRef],
  provenance: [ProvenanceLink]
}

ResourceEvent = {
  resource_ref: ResourceRef,
  sequence: MonotonicInt,
  state: ResourceState,
  readiness_evidence: EvidenceRef?,
  manager_generation: MonotonicInt,
  causal_operation_id: OpaqueId
}

ApprovalRequest = {
  approval_id: OpaqueId,
  action_kind: ActionKind,
  canonical_action_digest: Digest,
  target_ref: ResourceRef,
  effect_summary: EffectSummary,
  state: Pending | Approved | Denied | Expired | Consumed | Invalidated
}

DecisionRequired = {
  decision_id: OpaqueId,
  reason: CapabilityUnavailable | ValidationFailed | PolicyDenied | UnknownExternalEffect | MixedCancellation,
  affected_nodes: [NodeRef],
  evidence_refs: [EvidenceRef],
  allowed_resolutions: [ResolutionKind]
}

WorkspacePolicy = {
  policy_id: OpaqueId,
  workspace_ref: WorkspaceRef,
  constraints: [Constraint],
  defaults_provenance: [ProvenanceLink],
  version: VersionRef
}

ReadyDispatchIntent = {
  intent_id: OpaqueId,
  execution_ref: ExecutionRef,
  closure_version_ref: VersionRef,
  node_ref: NodeRef,
  ready_epoch: MonotonicInt,
  target_resource_ref: ResourceRef,
  input_refs: [ResourceRef],
  replay_safety: Known | Idempotent | NonReplayable,
  state: Pending | Submitted | Reconciled | Blocked
}

Dispatch = {
  dispatch_id: OpaqueId,
  intent_ref: OpaqueId,
  execution_ref: ExecutionRef,
  closure_version_ref: VersionRef,
  target_resource_ref: ResourceRef,
  state: Created | Acked | Rejected | Expired
}

Assignment = {
  assignment_id: OpaqueId,
  dispatch_ref: OpaqueId,
  attempt_ref: AttemptRef,
  slave_agent_ref: AgentRef,
  session_generation: MonotonicInt,
  execution_epoch: MonotonicInt
}

ContinuationIntent = {
  intent_id: OpaqueId,
  conversation_ref: ConversationRef,
  expected_turn_ref: TurnRef,
  reason: ObserveRun | AwaitDecision | AwaitAttestation,
  budget: CodingAgentBudget,
  operation_id: OpaqueId
}
```

`DomainErrorEnvelope` 统一携带 `code/category/retryable/safe_message/operation_ref/expected_version/observed_version/details`；transport transient、capacity wait、policy deny、validation fail 和 unknown external effect 必须映射到不同 retry mode。命令收到不等于状态改变；状态只能由对应 RunEvent/ResourceEvent 投影确认。

Layer 0 与宏观 C1–C10 对齐：C1=`ResourceRef`/Resolver；C2=`ResourceDescriptor`；C3=Index/`query_capabilities`；C4=`TaskClosure`/`apply_plan_patch`/`commit_plan`；C5=带 target/operation/closure 的 dispatch envelope；C6=control-command；C7=`RunEvent`/`ResourceEvent`；C8=`DomainErrorEnvelope` + retry modes；C9=`EffectivePolicySet` + provenance；C10=`ResultExpectation` + `ResourceLifecycleProfile`。单域不激活跨信任域 GRIP、跨域命名和多 Coordinator 合流，但不删除这些契约语义。

## 5. 生命周期与状态

### 5.1 Conversation

`inactive → activating → active → deactivating → inactive`。Workspace 同时最多一个 active Conversation；只有 active Conversation 可以接收 prompt 和打开 Run；同一 active Conversation 同时最多一个 open Run。

### 5.2 Run 双层状态

细化层：`opened → refining ⇄ committed`。每次 `commit_plan` 产生完整快照 `CommittedClosureVersion vN`，旧版本不可变。

执行层：`start_run → running ↔ paused → cancelling → cancelled`，或到 `completed|partial|failed`；终态后 `close_run → closed`。Pause 只关闭新的 READY dispatch gate，不冻结 RUNNING Attempt；cancel 在没有 stopped evidence 前不伪造 `cancelled`。Execution 引用 start 时锁定的 vN；后续版本不替换已 RUNNING Node。`close_run` 依据 `ResultExpectation/ResourceLifecycleProfile` 清理 `run_bound` 资源，非 `run_bound` 资源继续进入 ResourceProjection。

### 5.3 Node/Attempt/Resource

- Node：`DRAFT → VALIDATED → READY → RUNNING → completed|failed|skipped|cancelled|superseded`，旁路 `BLOCKED_ON_APPROVAL|BLOCKED_ON_DECISION|RETRY_WAIT`。
- Attempt：`created → dispatched → running → completed|failed`，取消走 `cancelling → cancelled|completed`；重试新建 Attempt，不新建 Node。
- ResourceInstance：`provisioning → starting → ready ↔ degraded → stopping → stopped`，或 `failed|lost|expired`；`ready/stopped` 必须带证据。

Resource manager（默认是执行该资源的 Slave）产生 `ResourceEvent`；Observer 只校验 manager identity、fencing、generation 和 evidence 后接受，不能替 manager 伪造 `ready`/`stopped`。`lost`、`stopped` 和 `expired` 是不同事实。

### 5.4 M1 改派

Observer 以 WorkerLivenessPolicy fence 失联 Attempt，拒绝旧 session/epoch 的迟到报告。只有 Node 的 `RefinedConstraint.allow_reassignment=true`、输入 target-independent、executor replay-safe、Replica 条件满足且预算足够时，Driver 才提交改派意图。Observer 保持 execution_id 与闭包版本不变，创建新 Attempt 指向 Slave B，并追加 provenance（原 target、原因、时间、policy 判定）。

## 6. API 与协议

### 6.1 Browser API（Observer `/api/v1`）

- `POST /conversations`；`POST /conversations/{id}/activate`；
- `POST /conversations/{id}/messages` → `202 {message_id}`；
- `GET /conversations/{id}/stream`（SSE）；
- `GET /runs/{id}`、`GET /runs/{id}/events?cursor=`；
- `POST /runs/{id}/close`；
- `GET /capabilities`、`GET /resources`；
- `POST /slaves/{id}/availability` 仅测试/开发模式可用。

### 6.2 Driver tool facade

`open_run`、`query_capabilities`、`refresh_capabilities`、`apply_plan_patch`、`inspect_plan_readiness`、`commit_plan`、`start_run`、`pause_run`、`resume_run`、`cancel_run`、`get_run_status`、`get_events`、`resolve_decision`、`resolve_run_outcome`、`attest_criterion`、`close_run`。

所有 mutation 携带 `operation_id` 和 `expected_version`；响应包含 `receipt`、`observed_version` 和 allowlisted error details。

### 6.3 WorkerService（Observer `/worker/v1`）

- `POST /register`、`POST /heartbeat`；
- `GET /poll` 返回 DispatchCommand/CancelCommand/CleanupCommand；
- `POST /ack`、`POST /progress`、`POST /terminal`；
- `POST /resource-events`、`POST /capability-health`。

每帧带 `agent_id`、`session_generation`、`attempt_id`、`execution_epoch` 和 operation/report key。Artifact bytes 走独立受管 endpoint，不塞入 WorkerSession。

## 7. 数据与一致性

四个数据库实例分别运行版本化 migration：

| 实例 | 权威数据 |
|---|---|
| `observer-db` | users/workspaces/policies、bindings、agents/sessions、runs、`closure_contracts`、`closure_versions`、`closure_patches`、nodes/edges、constraints/refined_constraints、typed_holes/compute_bindings、executions/attempts/dispatches、run/resource events + projections、outbox、idempotency |
| `driver-db` | conversations/messages/turns、run scopes、draft snapshots、`draft_patch_journal`、capability snapshots、Codex thread/session metadata、confirmed cursors、mutation journal |
| `slave-a-db` / `slave-b-db` | replica bindings、attempt ledger、dispatch/report receipts、staging/cleanup、resource journal、capability cache、heartbeat、readiness evidence |

Observer 的跨表 mutation 在单事务内完成：校验 → 追加事件 → 更新 Projection → 写 outbox/idempotency receipt。Outbox dispatcher 使用 `FOR UPDATE SKIP LOCKED`，重复投递由 operation key 去重。Projection 可从事件序列重建并与快照校验。

Python project 的 `loom_v2/contracts/` 只承载本节 Layer 0 的版本化语义模型、JSON Schema、patch algebra、error/event envelope 和确定性校验接口；`loom_v2/observer`、`loom_v2/driver`、`loom_v2/slave` 不能互相 import 内部实现。SQL migration 可以把 `TaskClosure` 分列保存以便查询，同时保留完整 canonical snapshot/digest 供重放；数据库列名不是契约本身。

## 8. Web 体验

默认 Conversation-first：

- 主区：消息时间线、输入框、Agent 后端/模型状态和 assistant streaming；
- 右侧 Run drawer：当前 ClosureVersion、约束、readiness blockers、Node/Attempt 状态、事件 cursor、provenance、ResourceRef；
- Workspace/资源页：Slave A/B 健康、Replica 状态、能力快照和资源生命周期；
- 开发控制：Fake/Codex 后端切换、Slave A availability、重放/故障注入；生产配置隐藏故障注入；
- UI 只调用 Observer，永不持有 agent token 或 Secret 明文。

## 9. 测试与验收

### 9.1 单元

覆盖状态转移、3×2 TaskClosure schema、Python 装饰器到 canonical metadata 的编译、任意 predicate/lambda 拒绝、`TypedTerm`/`VocabularyRegistry` schema 与版本兼容、未知 required/advisory term 行为、`TermSupport` 分层能力匹配、ComputeSpec/typed hole/opaque ComputeBinding、ComputeRequirement 与 ConstraintRef 的单一真相、ClosureContract 收紧关系、policy 合并、持续 `apply_plan_patch` 的 patch algebra、CAS、PlanLimits、引用类型/作用域、criterion evidence、digest/provenance 和 error envelope。

### 9.2 集成

Docker Compose 测试 profile 启动四个 PostgreSQL 实例；各服务只使用自己的 `DATABASE_URL`。测试覆盖事务、migration、outbox、projection、worker fencing、Slave 本地 ledger 和重连对账。

### 9.3 E2E

- M0：创建/激活 Conversation，发送 prompt，Codex 或 Fake 跨多个 turn 连续调用 `apply_plan_patch`（先补 Data/Program，再补 ComputeSpec/typed hole，再绑定 ComputeBinding），读取 readiness，提交 v1，执行 echo/hash/sort，获取 ResourceRef，通过 SSE 收到 assistant 回复，显式 close。
- M1 A′：暂停/下线 A，验证旧 epoch 拒绝、同 execution 切换 B、Node 的 `RefinedConstraint` 与 provenance 保留。
- M1 C′：coding-agent 在 patch 中显式提交只读/预算 `RefinedConstraint`，Runtime 校验其单调收紧；放宽 patch 返回结构化 conflict 且旧版本不污染。
- M1 B′：policy deny 在 commit/admission 阻止；deterministic validator/pass、semantic attestation 和 readiness evidence 可查询。
- 重放与故障：duplicate ACK/terminal/close、网络超时、Driver/Codex 子进程退出、Slave 重连、未知外部副作用禁止盲重放。

### 9.4 部署验收

`scripts/dev-up.sh` 后 Compose 组件与宿主机 Driver healthcheck 全部通过；migration 完成；浏览器可访问 Conversation-first 首页；默认 Codex 后端显示配置的 `deepseek-v4-flash`。`docker compose --profile test up --build` 使用 Fake 完成离线 M0/M1。测试脚本对每个场景返回非零失败码并输出事件/provenance diff。

## 10. 安全与边界

- 密钥只由本机 Codex 配置/环境提供；不写入 Loom DB、事件、日志、Artifact 或 UI。
- Driver 不接收 user/agent token；Browser 不持有 agent token；Observer 每次写操作重新校验绑定。
- Codex/Fake coding-agent 只接收 scoped tool facade、用户消息和必要的闭包视图，不接收 `workspace_id`、`conversation_id`、Driver/Agent token 或 Secret 明文；CodingAgentBudget 的调整只影响后续 turn，不修改 ClosureContract/DAG/权限/业务 retry budget。
- 日志只记录 opaque ID、状态、错误码、耗时、bytes、digest 和 operation ref；禁止 Prompt 正文、路径、stdout/stderr、Secret/token。
- Observer 不把“收到 heartbeat”伪装为 `ready`；资源 readiness 必须带分级证据。
- Driver 本地数据库丢失时进入永久 `driver_state_lost` fence，不从 Slave 残留重建 Run；迟到 Worker/Codex 事件按 session generation/execution epoch 拒绝。
- 旧 `../loom` 与 `../computility` 内容只作参考；新 Python project 不 import 旧 `multi-agent/internal/*` 或旧运行时包。

## 11. 下一阶段 Roadmap

首版只实现固定内置 `VocabularyRegistry`、通用 `TypedTerm` round-trip/校验，以及 Slave 对当前 demo terms 的静态 `TermSupport` 声明；不在本版动态安装或执行第三方能力包。未知 `required` term 必须诚实失败，不通过 fallback 到 Fake 或忽略字段来“完成”任务。

下一阶段加入 **Term/Capability Package**：

1. 定义可签名的 package manifest（term kind/schema、细化关系、传播与 evidence 规则、运行时 adapter、兼容范围和 package tests）。
2. 安装器将 package 注册到版本化 `VocabularyRegistry`，并在 Slave 上完成隔离安装、health/package test、回滚和 capability snapshot 更新。
3. Driver/Observer 在 commit/admission 时按 registry digest 解析新 term；Slave 只有在对应 `TermSupport` 通过验证后才可接收需要该 term 的节点。
4. 增加 package 生命周期、撤销/过期、跨版本迁移和 provenance/evidence 审计；旧 closure 继续锁定原 registry/package digest，不被新安装静默改写。

该 roadmap 是能力扩展方向，不改变当前单域边界：能力包仍由 Provider/Slave 负责填充其内部 typed hole，公共 `ComputeBinding` 仍由 Driver/Observer 早绑定并锁定。

## 12. 迁移与扩展路径

首版不迁移旧数据库和旧任务。后续可在不改变 `loom_v2/contracts` 的前提下：

1. 扩展当前单 Conversation 独占的 Codex app-server 连接为受限的多 Thread 会话管理；
2. 将 WorkerService 从 HTTP long-poll 换成 mTLS 双向流；
3. 增加 Artifact object store、审批和更完整的 retry/reconciliation；
4. 扩展 capability package 和跨 Run 资源绑定。

这些扩展不能改变“coding-agent 唯一细化者、Observer 状态权威、Runtime 确定性”的边界。
