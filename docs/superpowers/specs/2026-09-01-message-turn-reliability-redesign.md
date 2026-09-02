# 消息投递与 Codex Turn 可靠性重设计

**日期：** 2026-09-01  
**状态：** 待评审  
**基线设计：** `docs/superpowers/specs/2026-09-01-driver-service-split-design.md`  
**适用原则：** `docs/superpowers/spec-design-guidance.md`

## 1. 目标与决策

本设计重做 Driver 拆分后的消息投递和 Codex turn 生命周期，解决以下问题：

1. 同一个 `request_id` 在重试、超时、Driver 重启或 Observer 多进程下不能重复提交 Codex turn。
2. `POST /api/v1/messages` 继续立即返回 `202`，但 accepted 请求必须有持久化结果或失败状态。
3. 同一 `conversation_ref` 的 turn 串行执行，动态工具回调不能串到其他 conversation。
4. Driver 重启后继续使用 Observer 持久化的 `thread_id`，旧 Driver 不能覆盖新 Driver 的状态。
5. 没有 `loom_open_run` 的普通对话也必须可通过 conversation API 查询。

本设计采用以下决策：

- Observer 仍是唯一对外入口和唯一权威状态存储。
- Observer 持久化消息 receipt；单个 dispatcher 的内存 task 只作为调度实现细节，不承担幂等语义。
- Driver 使用每个 conversation 的 FIFO 队列，并通过一个全局 Codex lane 保护当前单 app-server 资源模型。
- 每次 turn 使用独立 `TurnContext`，provider 不再暴露共享的可变 `tool_handler`。
- 不保留 Observer 自动嵌入 Driver 的生产兼容路径；测试通过显式依赖注入使用 fake provider。
- 不引入通用消息队列或新的服务发现系统；复用现有 FastAPI、SQLAlchemy、`ObserverRepository`、`ObserverControlClient` 和 `WorkerSession`。

## 2. 不变量

### 2.1 请求幂等

- 幂等键为 `(workspace_id, request_id)`。
- 相同幂等键必须携带相同的 `payload_digest`；摘要不同返回 `409 request_id_reused`。
- 一个 receipt 在任意时刻最多只有一个有效 execution claim。
- 重试只改变 receipt 的 delivery/claim 状态，不重新创建用户消息或 Codex user turn。

### 2.2 Turn 所有权

- 一个 `TurnContext` 独占一个 Codex turn 的进程、thread、读写锁和 MCP handler。
- 只有创建该 context 的 owner token 才能发送、打断或关闭它。
- Driver epoch 失效后，旧 context 的 Observer 写操作全部被拒绝。
- 同一个 conversation 不允许两个 active turn；不同 conversation 也必须经过全局 Codex lane。

### 2.3 结果可见性

- `202` 只表示 receipt 已接受，不表示 Driver 已成功完成。
- receipt 的终态必须是 `completed`、`failed` 或 `interrupted`；临时故障使用 `retryable`。
- conversation projection 必须能返回 receipt 产生的 user/assistant 消息，即使没有 Run。

## 3. Observer 持久化模型

新增 `message_receipts` 表，作为公网消息的生命周期和投递幂等权威：

```text
workspace_id          text                 primary key
request_id            text                 primary key
conversation_ref      text                 not null
prompt                text                 not null
payload_digest        text                 not null
state                 text                 not null
run_id                text nullable
assistant_text        text nullable
outcome               jsonb nullable
claim_token           text nullable
claim_expires_at      timestamptz nullable
attempt_count         integer not null
next_attempt_at       timestamptz nullable
created_at            timestamptz not null
updated_at            timestamptz not null
```

约束和索引：

- 主键 `(workspace_id, request_id)`。
- 索引 `(workspace_id, conversation_ref, created_at)`，用于 conversation FIFO projection。
- 索引 `(state, next_attempt_at)`，用于单 dispatcher 扫描。
- `prompt` 和 assistant 内容属于用户数据，不写日志，不复制到 provenance 或 orchestration sandbox。

receipt 状态为：

```text
accepted -> queued -> in_flight -> completed
                              ├-> retryable -> queued
                              ├-> failed
                              └-> interrupted
```

单 dispatcher 按 receipt 顺序串行投递，因此不引入独立的 delivery lease。`claim_expires_at` 表示 Driver execution claim，真正的 claim 必须由 Observer 数据库条件更新完成。

`outcome` 只保存一种终态载荷：成功时为 `{ "result": {...} }`，失败或中断时为
`{ "error": {...} }`。`assistant_text` 是 conversation 展示字段，不重复嵌入
`outcome`。

## 4. 公网消息 API

### 4.1 POST `/api/v1/messages`

请求必须包含 `request_id`、`conversation_ref` 和非空 `text`。

Observer 在同一个数据库事务中：

1. 计算规范化业务 payload digest。当前摘要输入为
   `{ "conversation_ref": ..., "text": ... }`；`request_id`、HTTP headers
   和 delivery metadata 不参与摘要。未来新增会改变 Codex turn 语义的字段时，
   必须一并纳入摘要输入。
2. 创建 receipt，初始状态为 `accepted`；或读取已有 receipt。
3. 对已有 receipt 校验 payload digest。
4. 提交事务后返回响应。

响应规则：

```json
{
  "accepted": true,
  "conversation_ref": "conversation-1",
  "request_id": "request-1",
  "status": "accepted"
}
```

- 新请求返回 `202 status=accepted`。
- 已存在且未终态的相同请求返回 `202 status=queued|in_flight`。
- 已完成、失败或中断的请求返回已有状态，不重新投递；客户端仍通过 conversation 查询详情。
- 摘要不一致返回 `409 request_id_reused`。

入口不等待 Driver、Codex 或 Slave。Observer dispatcher 在事务提交后异步处理 receipt。

### 4.2 GET conversation

`GET /api/v1/conversations/{conversation_ref}` 合并两类事实：

- receipt 产生的 conversation-level user/assistant 消息；
- Run 事件、Run summary、动态节点和执行结果。

当 receipt 已绑定 Run 时，projection 使用 Run 中带 request id 的消息，避免重复；当 receipt 没有 Run 时，直接从 `prompt` 和 `assistant_text` 生成消息。receipt 的终态和错误同时出现在 conversation 状态和对应 run summary（若存在）。

事件流继续由 Observer 生成；receipt 状态变化至少生成 accepted、in_flight、completed/failed/interrupted 事件。

### 4.3 Interrupt

中断请求只针对指定 `conversation_ref` 的 active receipt：

- 已排队但未开始的 receipt 直接转为 `interrupted`；
- 已 claim 的 receipt 交给 Driver coordinator 调用对应 `TurnContext` 的 interrupt；
- 无 active receipt 返回 `409 conversation_not_active`。

## 5. Observer Dispatcher 与内部命令

### 5.1 Dispatcher

Observer 使用一个轻量 dispatcher 扫描 `accepted` 和 `retryable` receipt：

1. 通过条件更新将 receipt 转为 `queued`；单 dispatcher 一次只处理一个 receipt。
2. 调用 active Driver 的 `POST /driver/v1/messages`，携带原始 `request_id`、`conversation_ref`、`text` 和 `payload_digest`。
3. Driver 返回或连接失败后由 receipt 状态决定是否重试；如果 Driver 已 claim，后续重复投递必须得到 `in_flight`。
4. 连接超时不直接判定执行失败；若 receipt 仍未被 claim，则转为 `retryable`，否则保留当前 claim。
5. Observer 重启后从数据库重新扫描，无需依赖旧进程的 asyncio task。

Dispatcher 使用已有 active Driver lease 和内部 token。投递超时继续使用 `LOOM_OBSERVER_FORWARD_TIMEOUT_SECONDS`，默认 3600 秒。

### 5.2 Driver 控制命令

在现有 allowlist 中新增三个显式命令：

```text
message.claim
message.update
message.release
```

`message.claim` 参数包含 `request_id`、`payload_digest`、`conversation_ref`。Observer 以条件更新实现以下结果：

```text
claimed      -> 返回 claim_token
in_flight    -> 返回当前 receipt 状态，不启动 Codex
completed    -> 返回已有结果
failed       -> 返回已有错误
retryable    -> 重新发放 claim
request_id_reused -> 409
```

`message.update` 必须携带 `claim_token`、Driver lease 和 epoch，只允许 owner 更新
receipt 的 Run/assistant/终态字段以及 `driver_threads` 中的 thread/turn 字段。
`message.release` 用于 Driver 主动放弃未开始的 claim；进程崩溃不依赖 release，而由
claim lease 和 epoch fencing 恢复。

## 6. Driver Turn Coordinator

### 6.1 队列

Driver coordinator 维护：

- 每个 `conversation_ref` 一个 FIFO receipt 队列；
- 一个全局 Codex lane，同一时间只允许一个 `TurnContext` 占用当前 app-server provider。

同一 conversation 的不同 request 按 receipt `created_at` 顺序执行。不同 conversation 可以同时入队，但必须依次获得全局 lane。该 lane 只限制 Codex turn，不限制 Driver 到 Slave 的独立 HTTP 连接。

Driver 不再使用 `_request_results` 作为公网幂等缓存。receipt claim 是唯一依据；进程内 map 只保存等待中的 asyncio Future，并可在重启时丢弃。

### 6.2 执行流程

每个 receipt 的执行顺序为：

1. Driver 调用 `message.claim`。
2. claim 成功后加入 conversation FIFO。
3. 获得全局 lane 后创建 `TurnContext`。
4. 查询或恢复 `conversation_ref` 对应的 `thread_id`。
5. 通过 `message.update` 写入 `in_flight`、thread 和 turn 标识。
6. 执行 Codex turn；动态工具只能调用该 context 的 MCP handler。
7. 每个可见 assistant 片段更新 receipt；Run 存在时同时追加 Run 消息事件。
8. 成功、失败或中断时写入 receipt 终态和 Observer thread state。
9. 关闭 context 并释放全局 lane。

Driver endpoint 返回的 HTTP body 只作为 dispatcher 诊断信息；客户端结果以 Observer receipt/conversation projection 为准。

## 7. Provider TurnContext

`CodexAppServerProvider` 改为 context-oriented API：

```text
begin_turn(conversation, request, existing_thread_id, tools, handler) -> TurnContext
send_turn(context, prompt) -> AgentEvent stream
interrupt(context)
end_turn(context)
```

`TurnContext` 至少包含：

- `request_id`、`conversation_ref`、owner token/generation；
- `thread_id`、`turn_id`；
- 当前 turn 的 dynamic tools 和 MCP handler；
- 当前 process 以及 stdout/stdin 读写锁。

要求：

- `begin_turn` 在获得全局 lane 后一次性绑定 tools/handler；等待 lane 的后续请求不能覆盖当前 context。
- `_read_lock` 只保护该 context 的 stdout reader；`_write_lock` 保护 stdin 写入，包括 interrupt。
- `end_turn(context)` 校验 owner token；旧 context 的 close 不得终止新 context 或释放新 context 的锁。
- force shutdown 只由 Driver 进程退出路径调用，不参与正常 turn 生命周期。
- provider 不再保留共享 `tool_handler` 字段供多个 turn 读取。

Codex thread resume、动态工具协议和现有模型配置保持不变；变化只限于 turn 状态隔离和所有权。

## 8. 失败、重启与 fencing

### 8.1 Driver 或 Codex 故障

- Driver/网络暂时不可用：receipt 保持 `retryable`，按 `next_attempt_at` 重投。
- Codex 协议错误、模型终态错误或预算耗尽：receipt 为 `failed`，在 `outcome.error` 保存结构化错误。
- turn 被用户打断：receipt 为 `interrupted`。
- Observer 命令因 stale epoch 失败：立即停止当前 context，不得继续向 Slave 派发或写 Run 状态。

### 8.2 Driver 重启

新 Driver 注册产生新 epoch，Observer：

1. 使旧 Driver lease 失效；
2. 将当前所有 active message claims 转为 `retryable`，使旧 Driver 的 claim token 失效；
3. 将 `starting/in_progress` thread binding 转为 `recovery_pending`；
4. 保留原 `thread_id`、receipt、Run 和事件。

新 Driver 重新 claim 后调用 `thread/resume` 和 `thread/read`。如果 Codex volume 丢失或 thread 无法恢复，写入 `codex_thread_unavailable`，不得创建新 thread 冒充恢复。

### 8.3 Observer 重启

Observer 当前只运行一个 dispatcher。Observer 重启后从数据库重新扫描，无需依赖旧进程的 asyncio task；重启期间遗留的 `queued` receipt 会重新投递，并由 Driver claim 保证幂等。多 dispatcher/多进程投递并发不在本期范围内。

## 9. 代码调整边界

### 保留并修正

- `ObserverControlClient.list_capability_packages()` / `get_capability_package()`；
- `ObserverRepository` 的 capability dict `ResourceRef` 规整；
- `ObserverDriverGateway.forward(timeout=...)`；
- Worker HTTP 直连 Slave、lease/epoch fencing 和 ContentStore 引用。

### 删除或替换

- 删除 Observer `forward_tasks` 作为去重机制；
- 删除 Driver `_request_results` 作为公网请求幂等机制；
- 删除 provider 共享 `tool_handler` 和无 owner 的 `close()`；
- 删除生产环境 Observer 内嵌 Driver、Codex、WorkerSession 和本地 SlaveService 路径；
- 删除被上述边界取代的旧参数和死代码。

不为旧公网请求格式、旧嵌入式 Driver 路径或旧 provider 调用方增加兼容层。测试使用显式 fake provider 和注入的 repository/control client。

## 10. 验证与验收

### 单元与协议测试

- capability list/get RPC、ResourceRef 字典规整和 package closure 引用；
- receipt 创建、相同/不同 payload 的 request idempotency；
- claim 的并发条件更新、lease 过期、epoch fencing 和终态不可重写；
- 单 dispatcher 超时、Driver 不可用和重试状态；
- provider context handler 隔离、读写锁、owner close 和 force shutdown；
- 同一 conversation FIFO、不同 conversation 全局 lane、interrupt 定位；
- 无 Run 对话的 conversation projection 和事件流。

### 集成与 E2E

- `POST /api/v1/messages` 始终快速返回 `202`，随后可轮询到 completed/failed/interrupted；
- 同一个 request_id 并发重试只产生一个 Codex user turn；
- Observer 重启后 accepted/queued/retryable receipt 继续投递；
- Driver 重启后同一 conversation 使用相同 thread_id，旧 epoch 无法写状态；
- 两个 conversation 的动态工具不会交叉写入 Run；
- 动态分布式分析完整执行：capability package list/get、provision、Slave dispatch、结果校验和 commit；
- Compose 配置、Docker CLI、Codex proxy base URL 和 secrets 启动检查继续通过。

## 11. 实施顺序

1. 增加 `message_receipts` 模型、迁移和 Observer Repository 状态机。
2. 实现 message claim/update/release 控制命令和 conversation projection。
3. 将 Observer 消息入口改为 receipt-first，并实现可恢复 dispatcher。
4. 引入 Driver turn coordinator 和 per-conversation FIFO。
5. 将 Codex provider 改为 `TurnContext` 所有权模型。
6. 将 DriverService 的消息执行、MCP handler 和 thread recovery 接入 receipt 状态机。
7. 删除内存幂等、共享 handler 和 Observer 嵌入式生产路径。
8. 补齐单元、协议、重启和完整 E2E 验收。

## 12. 范围外

- 多 Workspace 多活跃 Driver 共识；
- 外部 Kafka/RabbitMQ 等通用消息队列；
- Codex 硬崩溃时的 token 级续生成；
- 不同 conversation 的并行 Codex 进程池；
- Observer 代理 Driver 到 Slave 的执行数据面；
- Observer 多 dispatcher、多进程或多实例的并发投递协调。
