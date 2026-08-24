# Loom v2 单用户单 Workspace 垂直切片设计

> 状态：已完成设计讨论，待用户审阅。  
> 日期：2026-08-24  
> 权威输入：`../computility/单用户单Workspace核心架构设计v2.md`

## 1. 目标与验收范围

本项目在 `.` 建立一个物理隔离于旧 `../loom` 的 Go module，落地 v2 文档的 M0 单域管道和 M1 单域价值灯塔。首版必须可通过 Docker Compose 启动，并提供 Conversation-first Web 体验。

首版验收包含：

1. 同一 Conversation 内完成多轮持续 `open_run → apply_plan_patch* → inspect_plan_readiness → commit_plan → start_run → execution → ExecutionOutcome/ResourceRef → close_run`；每轮 patch 都产生可对账的 DraftClosureVersion，未 commit 草稿不得执行。
2. 事件序列可重放为 RunProjection；结果能够沿 provenance 追溯到 Run、ClosureVersion、Execution、Attempt 和 Slave。
3. M1 A′：在 `allow_reassignment=true` 且 replay-safe 的任务中，Slave A 下线后切换 Slave B；execution 身份、ClosureVersion 和约束保持不变，新 Attempt 和改派原因有记录。
4. M1 C′：高层约束自动带入细化 Node；细化只能收紧预算/允许效果/目标范围，放宽提交被拒。
5. M1 B′：WorkspacePolicy 在 commit/admission 处阻止违规动作；可检查约束留下 validator/attestation/readiness evidence，平台不伪造 `ready`/`stopped`。
6. 默认 coding-agent 后端是 Codex app-server；Fake coding-agent 仅用于离线测试、故障注入和确定性验收。Codex 默认模型为用户指定的 `deepseek-v4-flash`。

明确不在首版范围：多用户、多 Workspace、跨域授权、动态 DAG fan-out、循环/脚本 guard、通用 sandbox/cgroup、复杂审批治理、对象存储大文件、OIDC、生产 HA、真实能力包在线构建。

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

- **默认体验 profile**：`scripts/dev-up.sh` 先用 Docker Compose 启动四个隔离 PostgreSQL、Observer、两个 Slave 与 Web 静态资源，再在宿主机启动 Go Driver。Driver 直接启动本机 `codex app-server --listen stdio://` 子进程，继承运行用户的 `~/.codex/config.toml` 与环境。该 profile 默认 backend=`codex`。
- **确定性测试 profile**：Docker Compose 同时启动容器化 Driver 和 Fake coding-agent，不依赖本机 Codex、模型服务或凭据。该 profile 只用于自动化测试、故障注入和离线验收。

两种 profile 使用同一套 Driver/Observer 契约和 driver-db schema；只替换 `CodingAgentProvider`，不复制密钥到 Loom 配置。

### 3.1 Observer

Observer 是唯一控制面和 Task Bus：

- 保存固定身份、ConversationBinding/slot、WorkspacePolicy、Agent/Session；
- 保存 ClosureContract、ClosureVersion、Node、Execution、Attempt、Dispatch；
- 事务写 RunEvent/ResourceEvent 并维护 Projection 与 outbox；
- 校验 Driver/Slave token、Workspace、session generation、execution epoch；
- 提供 Browser REST/SSE、Driver API、Slave WorkerService；
- 不保存或重建 Message 正文，不替 coding-agent 做语义判断。

### 3.2 Driver

Driver 是每个 active Conversation 的单写者：

- 在 driver-db 保存 Conversation、Message、Turn、RunScope、草稿、能力快照、cursor 和 mutation journal；
- 绑定一个 `CodingAgentProvider`，默认 `CodexAppServerProvider`，测试使用 `FakeCodingAgentProvider`；
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

## 4. Layer 0 契约内核：任务闭包、细化与算力绑定

本节是语言无关的语义数据模型，先于 Go struct、SQL 表和 HTTP DTO。实现可以用 Go、JSON、Protobuf 或 PostgreSQL JSONB 表达，但必须保持这些字段的语义和不变量。

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
  refs: [ResourceRef],   // 已绑定的资源/程序/数据指称，不允许裸路径或裸 URL
  constraints: [Constraint]
}
```

三类主体的最小语义字段如下；具体 wire encoding 留给实现，但不能删掉这些语义面：

```text
DataApplication  = { logical_inputs, schema_digest, identity_criterion, expected_cardinality }
DataSystems      = { locality, classification, access_profile, retention_profile }
ProgramApplication = { operation_ref, semantics_digest, input_schema, output_schema, success_semantics }
ProgramSystems     = { executor_kind, effect_class, permissions, package_ref, replay_safety }
ComputeApplication = { capability_intent, quality/precision, parallelism_intent, result_expectation }
ComputeSystems     = { budget, locality, network_boundary, deadline, admission_requirements }
```

`Envelope` 至少包含 `closure_id`（稳定任务型资源标识）、`schema_version`、`lineage`、`origin_conversation_ref`、`declared_by`、`authorization_ref`、`result_expectations` 和 `provenance`。稳定标识指向任务闭包家族；每个可执行快照另有 `closure_version`，一次 Execution 只引用一个确定版本。

### 4.2 ComputeSpec、typed hole 与 ComputeBinding

`ComputeSpec` 描述能力和约束，不等于某台机器：

```text
ComputeSpec = {
  capability_requirements: [CapabilityRequirement],
  operation_ref: OperationRef,
  precision: PrecisionRequirement?,
  parallelism: ParallelismRequirement?,
  budget: ResourceBudget?,
  locality: DataLocalityRequirement?,
  network_boundary: NetworkRequirement?,
  deadline: Duration?,
  typed_holes: [TypedHole<ComputeRealization>]
}

TypedHole<T> = {
  hole_id: OpaqueId,
  type: "ComputeRealization",
  constraints: [Constraint],       // Φ_compute(hole)
  status: "unbound" | "bound",
  binding_ref: ComputeBindingRef?
}
```

`ComputeBinding` 是 Provider 对 hole 的不透明绑定结果：

```text
ComputeBinding = {
  binding_id: OpaqueId,
  hole_id: OpaqueId,
  capability_descriptor_ref: ResourceRef,
  capability_package_ref: ResourceRef?,
  target_resource_ref: ResourceRef,       // 精确 Slave/能力资源
  realization_digest: Digest,             // 物理组合的承诺，不展开为公共字段
  constraint_evidence_refs: [EvidenceRef],
  bound_by: AgentRef,
  bound_at: Timestamp
}
```

Provider 可以在 `realization_digest` 对应的内部实现中自由选择 machine、runtime、environment、网络、存储和调度；对外只需证明 `Φ_compute(binding)` 成立并提供证据。物理执行实现由 Execution/Attempt provenance 记录，不进入高层闭包的公共结构。

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

RefinedConstraint = {
  refined_constraint_id: OpaqueId,
  parent_constraint_id: OpaqueId,
  node_id: NodeRef,
  tightening_delta: ConstraintDelta,
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

细化不变量：对每次从 `C` 到 `C'` 的 patch，应用保护集保持 `A(C') ⊨ A(C)`；每条 `preserve` 约束必须在新版本有对应下层约束，或带 `discharge evidence`，不得静默消失。默认 detail 传播是保持或加严；decompose/aggregate/retry 的复制、额度拆分、定向、reduce、累计/重置/计次规则作为同一 Patch/ClosureVersion 的显式字段，不能由 Runtime 猜测。

### 4.5 Closure / execution / resource 的命名与 provenance

任务型资源的稳定 `TaskRef` 指向闭包家族，不等同于 `ClosureVersion` 或 Execution。结果必须由 `ResourceRef` 表示，并记录：`task_ref → closure_version → execution → attempt → compute_binding → target_resource → evidence → result_resource`。改派只改变新的 Attempt/target provenance，不改变任务标识和已锁定闭包版本。

## 5. 生命周期与状态

### 5.1 Conversation

`inactive → activating → active → deactivating → inactive`。Workspace 同时最多一个 active Conversation；只有 active Conversation 可以接收 prompt 和打开 Run。

### 5.2 Run 双层状态

细化层：`opened → refining ⇄ committed`。每次 `commit_plan` 产生完整快照 `CommittedClosureVersion vN`，旧版本不可变。

执行层：`start_run → running ↔ paused → cancelling → cancelled`，或到 `completed|partial|failed`；终态后 `close_run → closed`。Execution 引用 start 时锁定的 vN；后续版本不替换已 RUNNING Node。

### 5.3 Node/Attempt/Resource

- Node：`DRAFT → VALIDATED → READY → RUNNING → completed|failed|skipped|cancelled|superseded`，旁路 `BLOCKED_ON_APPROVAL|BLOCKED_ON_DECISION|RETRY_WAIT`。
- Attempt：`created → dispatched → running → completed|failed`，取消走 `cancelling → cancelled|completed`；重试新建 Attempt，不新建 Node。
- ResourceInstance：`provisioning → starting → ready ↔ degraded → stopping → stopped`，或 `failed|lost|expired`；`ready/stopped` 必须带证据。

### 5.4 M1 改派

Observer 以 WorkerLivenessPolicy fence 失联 Attempt，拒绝旧 session/epoch 的迟到报告。只有 ClosureVersion 的 `allow_reassignment=true`、输入 target-independent、executor replay-safe、Replica 条件满足且预算足够时，Driver 才提交改派意图。Observer 保持 execution_id 与闭包版本不变，创建新 Attempt 指向 Slave B，并追加 provenance（原 target、原因、时间、policy 判定）。

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

Go module 的 `pkg/contracts` 只承载本节 Layer 0 的版本化语义 DTO、JSON Schema、patch algebra、error/event envelope 和确定性校验接口；`internal/observer`、`internal/driver`、`internal/slave` 不能互相 import 内部包。SQL migration 可以把 `TaskClosure` 分列保存以便查询，同时保留完整 canonical snapshot/digest 供重放；数据库列名不是契约本身。

## 8. Web 体验

默认 Conversation-first：

- 主区：消息时间线、输入框、Agent 后端/模型状态和 assistant streaming；
- 右侧 Run drawer：当前 ClosureVersion、约束、readiness blockers、Node/Attempt 状态、事件 cursor、provenance、ResourceRef；
- Workspace/资源页：Slave A/B 健康、Replica 状态、能力快照和资源生命周期；
- 开发控制：Fake/Codex 后端切换、Slave A availability、重放/故障注入；生产配置隐藏故障注入；
- UI 只调用 Observer，永不持有 agent token 或 Secret 明文。

## 9. 测试与验收

### 9.1 单元

覆盖状态转移、3×2 TaskClosure schema、ComputeSpec/typed hole/opaque ComputeBinding、ClosureContract 收紧关系、policy 合并、持续 `apply_plan_patch` 的 patch algebra、CAS、PlanLimits、引用类型/作用域、criterion evidence、digest/provenance 和 error envelope。

### 9.2 集成

Docker Compose 测试 profile 启动四个 PostgreSQL 实例；各服务只使用自己的 `DATABASE_URL`。测试覆盖事务、migration、outbox、projection、worker fencing、Slave 本地 ledger 和重连对账。

### 9.3 E2E

- M0：创建/激活 Conversation，发送 prompt，Codex 或 Fake 跨多个 turn 连续调用 `apply_plan_patch`（先补 Data/Program，再补 ComputeSpec/typed hole，再绑定 ComputeBinding），读取 readiness，提交 v1，执行 echo/hash/sort，获取 ResourceRef，通过 SSE 收到 assistant 回复，显式 close。
- M1 A′：暂停/下线 A，验证旧 epoch 拒绝、同 execution 切换 B、约束和 provenance 保留。
- M1 C′：高层只读/预算约束随 patch 进入 Node；放宽 patch 返回结构化 conflict 且旧版本不污染。
- M1 B′：policy deny 在 commit/admission 阻止；deterministic validator/pass、semantic attestation 和 readiness evidence 可查询。
- 重放与故障：duplicate ACK/terminal/close、网络超时、Driver/Codex 子进程退出、Slave 重连、未知外部副作用禁止盲重放。

### 9.4 部署验收

`scripts/dev-up.sh` 后 Compose 组件与宿主机 Driver healthcheck 全部通过；migration 完成；浏览器可访问 Conversation-first 首页；默认 Codex 后端显示配置的 `deepseek-v4-flash`。`docker compose --profile test up --build` 使用 Fake 完成离线 M0/M1。测试脚本对每个场景返回非零失败码并输出事件/provenance diff。

## 10. 安全与边界

- 密钥只由本机 Codex 配置/环境提供；不写入 Loom DB、事件、日志、Artifact 或 UI。
- Driver 不接收 user/agent token；Browser 不持有 agent token；Observer 每次写操作重新校验绑定。
- 日志只记录 opaque ID、状态、错误码、耗时、bytes、digest 和 operation ref；禁止 Prompt 正文、路径、stdout/stderr、Secret/token。
- Observer 不把“收到 heartbeat”伪装为 `ready`；资源 readiness 必须带分级证据。
- 旧 `../loom` 与 `../computility` 内容只作参考；新 module 不 import 旧 `multi-agent/internal/*`。

## 11. 迁移与扩展路径

首版不迁移旧数据库和旧任务。后续可在不改变 `pkg/contracts` 的前提下：

1. 扩展当前单 Conversation 独占的 Codex app-server 连接为受限的多 Thread 会话管理；
2. 将 WorkerService 从 HTTP long-poll 换成 mTLS 双向流；
3. 增加 Artifact object store、审批和更完整的 retry/reconciliation；
4. 扩展 capability package 和跨 Run 资源绑定。

这些扩展不能改变“coding-agent 唯一细化者、Observer 状态权威、Runtime 确定性”的边界。
