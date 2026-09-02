# Run 生命周期与状态机重设计

**日期：** 2026-09-01
**状态：** 设计（重写，按单 Driver 方案修订，待评审）
**基线设计：**
- `docs/superpowers/specs/2026-09-01-message-turn-reliability-redesign.md`（turn 可靠性）
- `docs/superpowers/specs/2026-09-01-driver-service-split-design.md`（Driver 拆分）
- `docs/superpowers/specs/2026-09-01-orchestration-early-validation-design.md`（编排提前校验与失败回喂）
**适用原则：** `docs/superpowers/spec-design-guidance.md`

## 1. 背景与问题

Run 结果没有闭环回 coding agent，run 状态机本身也无法支撑闭环：

1. **结果不回喂。** `loom_start_run` 工具只返回“已启动”（`loom_v2/driver/mcp.py` 中调用 `tools.start_run`），真正的执行由 `_run_prompt` 事件循环在工具结果返回之后同步 dispatch（`loom_v2/driver/service.py:183` 的 `tool_call` 分支；`start_run` 事件分支在 `service.py:201`）。执行结果只落进 HTTP 响应与 `outcome`，不回 coding agent 上下文；失败时 `_run_prompt` 抛异常直接打断 turn，agent 既不知道失败原因，也无法修复后重跑。
2. **`decision_required` 语义混乱。** 同一个状态承载了三种互不相同的情况：
   - 执行/校验失败（可修复）→ `record_result`/`complete_orchestration` 记 `decision_required`（`loom_v2/observer/repository.py:2759`、`3657`）；
   - 成功但需 attestation → 也记 `decision_required`（`repository.py:3668`）；
   - 真正不可恢复的失败（deadline/崩溃）→ 却记 `failed`（`repository.py:2184`）。
3. **决策态是死胡同。** `decision_required` 没有任何出边：`close_run` 只允许 `{completed, failed, cancelled}`（`repository.py:2880`），`decision_required` 无法关闭；也没有 reopen → 重跑路径。guard 不一致：`cancel_run` 不挡 `decision_required`（`repository.py:2165`），`fail_run` 却挡（`repository.py:2182`）。
4. **状态无集中强制。** `RunRecord.state` 是裸字符串，`commit()`/`start()` 几乎不检查当前状态（`repository.py:2544`、`2569`），非法转移（如从 `completed` 再 commit）不被拦截。

## 2. 关键结论

- **执行层与生命周期层必须分层。** Worker/Slave 报告的执行终态（`terminal_state ∈ {completed, failed, decision_required}`）是执行结果，Run 状态是生命周期状态，两层不能互相充当。执行结果喂给状态机做转移，不直接充当 Run 状态。
- **决策态不是终态。** 引入 `awaiting_decision` 作为决策态（非终态），必须有出边（接受 / 放弃 / 修复重跑）；`completed`/`failed`/`cancelled`/`closed` 才是终态。
- **结果必须回喂。** run 的终结/决策结果必须通过工具结果直接回到 coding agent 上下文，让 agent 对照 closure contract 验证、决定接受/修复/放弃。
- **可修复失败必须可重跑。** 执行/校验失败不直接终态化，进入 `awaiting_decision(repair)`，允许 agent 修复闭包后以新 epoch 重跑。

## 3. 目标

1. 重定义 run 状态机：去掉 `decision_required`，拆分为语义明确的 `awaiting_decision`（决策态）与 `completed`/`failed`（终态）。
2. 状态转移集中定义并强制，非法转移一律拒绝；`close_run` 只允许真终态。
3. `loom_start_run` 阻塞执行并返回完整结果给 coding agent，闭环。
4. 支持 `awaiting_decision` 的三条决策路径：接受（attestation）、放弃、修复重跑（新 epoch）。
5. 持久化投影、状态 API、web UI 标签与状态机一致。

## 4. 非目标

- 不改 Worker/Slave 的执行结果报告协议；`terminal_state` 仍是执行层概念。
- 不做多 run 聚合编排、跨 run 依赖、run 间重试编排。
- 不做消息队列/调度器重写；沿用现有 receipt/coordinator/lane 模型。
- 当前部署只支持单个 Driver 消息处理 lane；不引入多 Driver 自动接管、receipt 时间 lease 或 heartbeat。
- 不为旧调用方做兼容层；已持久化的 `decision_required` 直接迁移为 `awaiting_decision`。

## 5. 设计

### 5.1 Run 状态集

| 状态 | 含义 | 是否终态 |
|---|---|---|
| `opened` | 已创建，未细化 | 否 |
| `thinking` | 细化/规划中（agent 改 draft） | 否 |
| `committed` | 闭包已提交，可启动 | 否 |
| `running` | 执行中 | 否 |
| `awaiting_decision` | 已产出结果，等待 agent/用户决策 | 否（决策态） |
| `completed` | 成功且已接受 | 是 |
| `failed` | 失败且已放弃/不可恢复 | 是 |
| `cancelled` | 用户中断 | 是 |
| `closed` | 归档 | 是 |

`awaiting_decision` 通过 `outcome.decision` 区分原因（见 5.4）：
- `repair` — 执行/校验失败，需要修复闭包后重跑；
- `attestation` — 成功但需要确认（`success_semantics` 无 validator 时的 attestation 要求）。

### 5.2 转移表

| 当前状态 | 事件 | 下一状态 | 说明 |
|---|---|---|---|
| （新） | `open_run` | `opened` | |
| `opened` | `begin_refinement` | `thinking` | 细化开始 |
| `thinking` | `commit` | `committed` | readiness ready |
| `committed` | `start` | `running` | 创建 execution；首次 `execution_epoch=1` |
| `running` | 执行成功且校验通过 | `completed` | `outcome.disposition=completed` |
| `running` | 执行/校验失败（可修复） | `awaiting_decision` | `outcome.decision=repair` |
| `running` | 成功但需 attestation | `awaiting_decision` | `outcome.decision=attestation` |
| `running`/`thinking` | 不可恢复失败 | `failed` | deadline、driver 崩溃、coding-agent 错误 |
| 非终态 | `cancel_run` | `cancelled` | 用户中断 |
| `awaiting_decision` | `resolve(accept)` | `completed` | 仅限 `decision=attestation` |
| `awaiting_decision` | `resolve(abandon)` | `failed` | 放弃 |
| `awaiting_decision` | `apply_plan_patch`（仅 `decision=repair`） | `thinking` | 修复重跑（reopen） |
| `committed` | `start`（重跑） | `running` | 新 execution，`execution_epoch+1`；首次启动 epoch 为 1 |
| `completed`/`failed`/`cancelled` | `close_run` | `closed` | 归档 |

被拒绝的转移（示例）：
- `opened → committed`（未细化直接 commit）；
- `thinking → running`（未 commit 直接 start）；
- `completed → thinking`（终态不可回退）；
- `awaiting_decision → failed`（不经 `resolve(abandon)` 不能被 turn 级异常覆盖）；
- `awaiting_decision(attestation) → thinking`（不能用 patch 绕过接受语义）；
- `awaiting_decision → closed`（必须先 resolve）；
- `failed → thinking`（`failed` 是终态；重试应在新 Run 或经 `awaiting_decision` 完成）。

### 5.3 执行结果层 → Run 状态层映射

执行层 `terminal_state` 只描述“这次执行产出了什么”，Run 状态机决定“run 现在处于哪个生命周期状态”：

| 执行层 `terminal_state` | 校验结果 | Run 状态 / disposition |
|---|---|---|
| `completed` | 通过 | `completed` |
| `completed` | schema/validator/lineage 失败 | `awaiting_decision(repair)` |
| `failed` | — | `awaiting_decision(repair)` |
| `decision_required` | — | `awaiting_decision(attestation)` |

`record_result`（`repository.py:2633`）与 `complete_orchestration`（`repository.py:3529`）按上表落状态，替代现在直接写 `decision_required` 的分支。Driver 侧 deadline 或 coding-agent 错误走 `failed`；用户显式取消统一走 `cancelled`，不与不可恢复错误混用。

### 5.4 outcome 结构

`outcome` 增加 run 层判定字段，执行层信息保留为原始回显：

```json
{
  "disposition": "completed" | "failed" | "awaiting_decision" | "cancelled",
  "decision": "repair" | "attestation" | null,
  "terminal_state": "completed" | "failed" | "decision_required",
  "terminal_error": { ... } | null,
  "resource_ref": { ... } | null,
  "value": { ... } | null,
  "digest": "...",
  "validation_evidence": [ ... ],
  "lineage": [ ... ],
  "provenance": { ... },
  "execution_id": "...",
  "execution_epoch": 1
}
```

字段不变量：

- `state=awaiting_decision` 时，`disposition=awaiting_decision` 且 `decision` 必须为 `repair` 或 `attestation`；
- `state∈{completed, failed, cancelled}` 时，`disposition` 与终态一致且 `decision=null`；`state=closed` 保留关闭前的 `disposition`；
- `terminal_state` 是本次执行的原始报告，不因 `resolve(accept)` 改写；因此 attestation 被接受后仍可为 `decision_required`；
- attestation 等待期间保留 `resource_ref`/`digest` 供 Agent 检查，`attestation_required` 不作为执行错误写入 `terminal_error`；
- repair reopen 进入 `thinking` 时清空当前 `outcome`，旧结果只保留在 `attempts`/事件历史中。

`disposition`/`decision` 由状态机写入；`terminal_state` 及以下保留 Worker/Slave 原始报告，用于投影与调试。

### 5.5 结果回喂闭环：`loom_start_run` 阻塞返回

`loom_start_run` 从“只启动”改为“阻塞执行并返回完整结果”：

1. `DriverMCP.call("loom_start_run")` 先 `start_run`（创建 execution，状态 `running`），随后调用注入的 `run_executor`（即现在的 `_dispatch_execution` / 编排 runtime / worker dispatch 逻辑），阻塞等待执行到达终态或决策态。
2. 工具结果直接携带完整 outcome：`state`、`disposition`、`decision`、`resource_ref`、`value`、`terminal_error`、`validation_evidence`、`execution_epoch`，以及 `decision_hint`（告诉 agent 下一步：验证成功标准 / 修复后重新 commit+start / `resolve_run(accept)` 确认 attestation）。
3. 执行失败（可修复）时，run 进入 `awaiting_decision(repair)`，工具调用本身仍视为成功，返回结构化 outcome；只有参数错误、非法状态或传输失败才使用 `success:false`。这样 Agent 不会把可修复的 Run 结果误判为 MCP 协议错误。
4. 事件循环不再为工具路径 dispatch：`_run_prompt`/`_run_prompt_remote` 的 `tool_call(loom_start_run)` 分支删除 `_dispatch_execution`/`_dispatch_remote_execution` 调用（工具已阻塞完成）。local、remote 和 fake 的 `start_run` 事件统一调用同一个 `execute_and_wait` 封装，禁止保留两套会重复 dispatch 的路径。

实现要点：
- `DriverMCP` 只注入一个私有 `run_executor: Callable[[run_id, prompt], Awaitable[...]]`；local 和 remote Driver 分别绑定现有 dispatch 函数，不新增 executor 层级或通用接口。
- `run_executor` 是唯一执行等待入口：local 直接复用现有执行逻辑；remote 复用现有 worker dispatch，提交 `run.result` 后读取 `run.get`；不新增 `run.await_outcome` 命令，也不在 MCP 内实现第二套轮询器。
- `loom_start_run` 阻塞直到 `state ∈ {completed, failed, awaiting_decision, cancelled}`，返回 outcome；用户取消必须唤醒等待并返回 `cancelled`。
- `loom_get_run_status` 同步返回 `outcome` 全量（含 `disposition`/`decision`），供 agent 复查。

### 5.6 决策工具与恢复路径

新增 `loom_resolve_run({decision})`，用于显式终结 `awaiting_decision`：

- `decision="accept"`：仅 `awaiting_decision(attestation)` → `completed`。不接受校验失败的结果（校验是权威闸门，不允许绕过）。
- `decision="abandon"`：任意 `awaiting_decision` → `failed`。

`accept` 后保留原始 `terminal_state=decision_required`、`resource_ref` 和 `digest`，仅更新 Run 层的 `state`/`disposition` 并追加决策事件；`decision` 清空。`abandon` 后结果引用不再作为成功输出，但可留在 attempt/event 历史中。

修复重跑路径（复用现有工具，状态机放开 `awaiting_decision → thinking`）：

```text
awaiting_decision(repair)
  → loom_apply_plan_patch（副作用触发 reopen → thinking）
  → loom_commit_plan（thinking → committed）
  → loom_start_run（committed → running，新 execution，epoch+1）
  → 终态
```

`start` 行为调整：当前 `start` 在 `execution_id` 已存在时直接返回旧值（`repository.py:2589`）。新规则为——首次从 `committed` 启动创建 execution 且 epoch 为 1；仅当状态为 `running` 时复用当前 execution；从已完成一次 execution 的 `committed`（含重跑）启动时总是创建新 `execution_id` 并原子递增 `execution_epoch`。旧 attempt 保留历史状态，任何不匹配当前 `execution_id + execution_epoch + attempt_id` 的 late result 必须被拒绝。重跑 fencing 沿用 reconcile 的 epoch 机制（`repository.py:2934`）。

`loom_close_run` 仅允许 `{completed, failed, cancelled}`；`awaiting_decision` 必须先 resolve 才能关闭。

### 5.7 集中转移强制

在 `ObserverRepository` 增加集中转移表与单一写入点：

```python
RUN_TRANSITIONS: dict[str, dict[str, str]] = {
    "opened": {"refinement_started": "thinking", "run_cancelled": "cancelled"},
    "thinking": {"committed": "committed", "run_failed": "failed", "run_cancelled": "cancelled"},
    "committed": {"execution_started": "running", "run_cancelled": "cancelled"},
    "running": {
        "run_succeeded": "completed",
        "run_needs_decision": "awaiting_decision",
        "run_failed": "failed",
        "run_cancelled": "cancelled",
    },
    "awaiting_decision": {
        "decision_accepted": "completed",
        "decision_abandoned": "failed",
        "refinement_started": "thinking",
        "run_cancelled": "cancelled",
    },
    "completed": {"run_closed": "closed"},
    "failed": {"run_closed": "closed"},
    "cancelled": {"run_closed": "closed"},
    "closed": {},
}
```

所有状态变更（`begin_refinement`/`commit`/`start`/`record_result`/`complete_orchestration`/`fail_run`/`cancel_run`/`close_run`/新增 `resolve_run`/`record_run_failure`）都经过 `_transition(run, event, **meta)`：

1. 校验 `(当前状态, 事件)` 在转移表中，否则抛 `illegal_state_transition`；
2. 追加最小转移事件 `{phase, from_state, to_state, reason, created_at}`；execution、attempt 和 outcome 的详细信息由各领域事件保留，避免重复存储；
3. 按需写 `outcome`。

`fail_run` 仅允许 `running|thinking → failed`；`cancel_run` 允许所有非终态；`close_run` 仅允许 `{completed, failed, cancelled}`。旧的逐方法 guard（`repository.py:2165`、`2182`、`2880`）删除，统一由转移表承担。

转移表之外还必须执行语义 guard：`awaiting_decision → thinking` 仅限 `outcome.decision=repair`，`resolve(accept)` 仅限 `outcome.decision=attestation`。实现前逐一迁移所有 `RunRecord.state` 写入点（包括启动恢复、动态节点汇总和失败/取消路径）；Worker/Slave 的 `terminal_state` 及动态节点执行状态不是 Run 状态，不做全局字符串替换。

### 5.8 持久化与迁移

- `runs.state` 仍是 VARCHAR，无需改列；`outcome` 仍是 JSONB。
- 新增幂等的 `migrations/008_run_lifecycle_state_machine.sql`：把存量 `state='decision_required'` 重映射为 `'awaiting_decision'`；`outcome.terminal_state` 保留原值，按 `terminal_state=failed → decision=repair`、`terminal_state=decision_required → decision=attestation` 一次性补齐 `disposition`/`decision`。迁移不采用“下次加载时再补齐”的双格式。
- receipt 不再使用时间有效期：删除 `message_receipts.claim_expires_at` 及对应模型、参数和续租逻辑。保留 `claim_token` 作为当前 Driver 的写入 fencing；新 Driver 注册并递增 `driver_epoch` 时，将上一 epoch 的 `in_flight` receipt 原子转为 `retryable` 并清空旧 token。单 Driver 未重启期间不做自动接管。
- 现有 `init_db()` 只负责建表，不能视为 migration runner。`scripts/migrate.sh` 先调用 `init_db()` 确保表存在，再通过 `observer-db` 的 `psql` 执行 008；008 使用 `IF EXISTS`/幂等更新，可重复执行，不引入新的 migration framework。
- `_conversation_status_for_state`（`repository.py:1466`）与 `ObserverStatus.status`（`mcp.py:345`）把 `decision_required` 替换为 `awaiting_decision`。
- web `app.js` `statusLabels` 增加 `awaiting_decision: 'Awaiting Decision'`（现缺失，`decision_required` 目前会回退成 idle）。

### 5.9 工具面与状态投影汇总

| 工具 | 变化 |
|---|---|
| `loom_start_run` | 阻塞执行，返回完整 outcome + `decision_hint` |
| `loom_resolve_run` | 新增，`decision ∈ {accept, abandon}` |
| `loom_get_run_status` | 返回 `outcome` 全量（含 `disposition`/`decision`） |
| `loom_close_run` | 仅限 `{completed, failed, cancelled}` |
| `loom_apply_plan_patch` | 对 `awaiting_decision` 生效并触发 reopen → `thinking` |
| `loom_commit_plan` / `loom_start_run` | 维持；`start` 重跑时新 epoch |

## 6. 测试

- **转移表**：逐条断言 5.2 合法转移通过；非法转移（`completed→thinking`、`awaiting_decision→failed` 不经 abandon、`opened→committed` 直接 commit、`awaiting_decision→closed` 直接 close）抛 `illegal_state_transition`。
- **语义拆分**：校验失败 → `awaiting_decision(repair)` 且 `outcome.decision=repair`；attestation → `awaiting_decision(attestation)`；成功 → `completed`。
- **恢复路径**：`awaiting_decision(repair) → patch → thinking → commit → committed → start（新 epoch）→ running → completed`；断言 `execution_epoch` 递增、旧 attempt 被 fencing。
- **决策**：`resolve(accept)` 仅限 attestation（repair 时被拒）；`resolve(abandon)` 生效；`close_run` 对 `awaiting_decision` 拒绝、resolve 后通过。
- **反馈闭环**：fake provider 断言 `loom_start_run` 工具结果包含 `disposition`/`resource_ref`/`terminal_error`/`decision_hint`；失败时工具调用仍为 `success:true`，run 为 `awaiting_decision(repair)` 且 agent 收到结构化 outcome。
- **投影/UI**：`awaiting_decision` 映射 `Awaiting Decision`。
- **迁移**：执行 008 后，持久化 `decision_required` 行变为 `awaiting_decision`，`outcome` 一次性补齐 `disposition`/`decision`。
- **receipt fencing**：同一 `(workspace_id, request_id)` 只允许当前 `claim_token` 写入；Driver epoch 变更后旧 receipt 可恢复为 `retryable`，无 `claim_expires_at` 和 heartbeat。
- **回归**：既有 dynamic orchestration、io validation、driver split 测试按新状态更新断言；`pytest -q` 全绿。

## 7. 风险与演进

- **阻塞工具时长**：`loom_start_run` 可能阻塞分钟级，唯一的执行 deadline 是 Driver 的 `LOOM_CODING_AGENT_DEADLINE_SECONDS`。现有 `LOOM_OBSERVER_FORWARD_TIMEOUT_SECONDS` 仅作为传输超时，部署值不得短于 Driver deadline，且不能把已接受的 receipt 改成业务失败；不新增其他 timeout。结果大小限制暂不新增，沿用现有消息上限和 `resource_ref`。
- **长 tool call 与模型上下文**：结果经 `decision_hint` 明确引导 agent 下一步，避免模型对 `awaiting_decision` 不知所措。
- **重跑 epoch/fencing**：沿用 reconcile 的 epoch 递增与 attempt fencing；`start` 从 `committed` 重跑强制新 `execution_id`，杜绝旧结果串扰。
- **迁移影响**：`decision_required → awaiting_decision` 无损；`outcome` 旧字段与 `disposition`/`decision` 一次性补齐，不做长期双写；receipt 旧 `claim_expires_at` 列一并删除。
