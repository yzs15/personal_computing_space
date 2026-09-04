# 动态节点 Worker 失联自动恢复设计

**日期：** 2026-09-03  
**状态：** 已批准，待实现计划  
**取代范围：** `docs/superpowers/specs/2026-09-02-compute-service-desk-acceptance-design.md` 中 G4 的 availability API 故障注入与 Run 级 epoch 改派语义

## 1. 背景与问题判定

当前动态编排已经具备 NodeIntent、DynamicNode、Attempt、Slave lease、能力匹配和结果 fencing，但没有形成可执行的 Worker 失联恢复闭环。

现有 strict xfail `test_disabled_slave_a_is_not_selected_for_dynamic_node` 不能证明 `DynamicOrchestrationRuntime` 忽略 Slave 可用性：

- 测试 executor 缺少当前运行时要求的 `read_json` 参数，在发射动态节点前即失败；
- 测试节点程序把 JSON 的 `true` 当作 Python 标识符，进入 Slave 后会产生 `capability_exec_error`；
- 修正这两个夹具问题后，直接使用 `ObserverRepository` 的路径会过滤已标记不可用的 `slave-a` 并选择 `slave-b`。

但生产形态仍有真实缺口：standalone Driver 只在动态编排启动时刷新一次 Slave registry；运行中 Worker 连接失败后，`DynamicOrchestrationRuntime` 会立即调用 `fail_dynamic_node()`，使节点和 Run 进入失败决策，而不会等待 lease 过期、fence 旧 Attempt、选择替代 Slave 并继续执行。

另一个相关问题是 `slave_availability` 同时承担“lease 是否存活”和“测试是否禁用”两种含义。`set_slave_availability(False)` 只修改内存字典，后续 `list_agents()` 又会根据 active lease 把它恢复为 `True`。这个布尔缓存不是可靠状态模型，不应继续修补。

本设计把问题定义为：**在运行中的 DynamicNode 所绑定的 Slave 实例失联且闭包已授权改派后，由 Driver 选择替代目标，由 Observer 原子替换 Attempt，并在不改变 Run execution 身份的前提下继续执行。**

## 2. 目标

1. 使用真实 Slave heartbeat lease 表示 Worker 存活，不再维护第二份可变 availability 状态。
2. 在部分动态节点已经完成、另一个节点所在的 Slave 容器被停止后，Run 自动把未完成节点改派到健康 Slave 并最终完成。
3. 保持 Observer 为 Run、DynamicNode、Attempt 和事件的状态权威；Driver 只负责目标选择、Worker 调用和提交改派意图。
4. 让单节点改派只 fence 该节点的旧 Attempt，不影响同一 Run 中其他健康并发节点。
5. 对 Observer 权威状态保持 fencing：旧 Attempt 的结果不能覆盖替代 Attempt；平台不承诺改派前后的业务语义等价或外部副作用安全。
6. 用真实 `docker compose stop slave-a` 完成 G4 故障注入，不在 Observer 或 Slave 增加生产级运维禁用能力。

## 3. 非目标

- 不设计生产级 `enabled`、`draining`、`disabled` 运维状态或管理 API。
- 不把 Observer 改造成任务队列或 Worker 数据面代理；Driver 仍直接调用 Slave。
- 不判断新旧目标的结果、精度、性能、环境或外部副作用是否语义等价；设置 `allow_reassignment=true` 的 coding agent 或用户承担该判断与授权责任。
- 不保留旧 availability/reconcile API 的兼容层。

## 4. 核心决策

### 4.1 重新设计 Attempt 恢复，不修补目标选择

仅在 `_select_target()` 增加条件无法解决运行中失联：目标可能在选择后、provision 中或 dispatch 中失效，且连接失败时旧执行是否仍在继续并不确定。恢复必须以 Attempt 为原子边界，并同时解决状态转换、幂等、迟到结果和并发 sibling fencing。

新增 Observer 原子命令 `node.reassign`。Driver 捕获可恢复的 Worker 失联，等待 Observer 确认旧 Slave 实例 lease 已失效，从最新 registry 选择替代目标，再提交改派。Observer 在同一事务中把旧 Attempt 标记为 `lost`，创建替代 Attempt，并记录完整 provenance。

### 4.2 Run epoch 与 Attempt fencing 分工

`execution_id + execution_epoch` 标识一次 Run execution generation：首次启动或显式重跑使用它；Driver 接管仍由独立的 `driver_epoch` fencing。单个 DynamicNode 的 Worker 改派不递增 Run 级 `execution_epoch`。

单节点迟到结果通过下列条件拒绝：

- `execution_id` 或 `execution_epoch` 不匹配当前 Run；
- `attempt_id` 不属于该 DynamicNode；
- Attempt 状态不是当前可接收结果的活动状态；
- Attempt 已是 `lost`、`failed`、`completed` 或被替代。

这使一个节点的恢复不会把其他健康节点在原 Run epoch 下产生的结果误判为 stale。

### 4.3 lease 是唯一存活事实

Slave 注册、heartbeat、release 和 TTL expiration 继续由现有 `runtime_agents` registry 管理。调度使用具体 active Slave 实例的 lease 快照，不使用独立的 `slave_availability` 字典。

Driver 在每个动态节点的首次调度边界以及每次恢复重选前读取 Observer 的最新 registry。读取动作会触发现有 TTL expiration 判定，因此停止容器后无需额外故障 API。

### 4.4 改派支持不等于语义保证

平台只承诺提供改派机制、内部状态 fencing 和完整审计。`allow_reassignment=true` 是 ClosureContract 中由 coding agent 或用户提交的显式授权，表示平台可以在机械准入条件满足时替换 Attempt。

平台不验证或保证：

- 新旧 Slave 对同一 capability 的实现、数值结果或性能等价；
- 改派后仍满足未被机器可判定约束表达的业务意图；
- 旧 Worker 在失联前没有产生 Observer 之外的外部副作用；
- package 的 `replay_safety` 声明真实、充分或适用于本次故障。

Observer 必须把授权来源、package 声明、输入引用、新旧目标和失联证据写入审计事件，使 coding agent 或用户可以评估改派造成的语义变化。平台仍执行能够机械判定的权限、能力、locality 和预算约束，但这些检查不构成语义等价证明。

## 5. 组件边界

### 5.1 Observer

Observer 负责：

- 从 `runtime_agents` 选择每个稳定 `slave_id` 的 active 实例；
- 在初次 dispatch 时把目标 `slave_id`、`instance_id` 和 agent epoch 固化到 Attempt；
- 验证旧 Attempt 所绑定的具体 Slave 实例是否已经 `expired` 或 `released`；
- 验证改派授权、能力、权限、locality、输入可访问性和 Attempt 预算；
- 原子记录旧 Attempt 丢失、替代 Attempt 创建和 `node_reassigned` 事件；
- 拒绝旧 Attempt 或旧 execution generation 的结果。

Observer 不探测 Worker HTTP，也不自行选择替代目标。

### 5.2 Driver

Driver 负责：

- 在调度边界刷新 Slave registry；
- 根据 capability、executor、locality、当前激活情况和本地 WorkerSession 可达集合选择目标；
- 直接 provision/dispatch Slave；
- 把连接或传输不可达与业务执行失败分开分类；
- Worker 失联后保持节点任务挂起，等待 lease 失效并提交 `node.reassign`；
- 使用 Observer 返回的替代 Attempt 继续执行。

Driver 不直接修改 Attempt，不把本地缓存当作 liveness 权威。

### 5.3 Slave

Slave 继续负责：

- 注册并维持自身 lease；
- 执行已授权的 provision/dispatch；
- 返回绑定 `attempt_id`、`execution_id` 和 `execution_epoch` 的 terminal report。

Slave 不增加故障注入或运维禁用接口。停止容器会自然终止 heartbeat 和 Worker HTTP。

## 6. 状态模型

### 6.1 Attempt 投影

动态节点 Attempt 使用以下字段；继续存放在现有 Run 事件/投影中，不新增数据库表：

```text
attempt_id              text
node_id                 text
target                  text              # stable slave_id
target_instance_id      text
target_agent_epoch      bigint
state                   text              # created | running | completed | failed | decision_required | lost
execution_epoch         bigint
replaces_attempt_id     text | null
replaced_by_attempt_id  text | null
terminal_error          object | null
```

`target_instance_id + target_agent_epoch` 固定本次 Attempt 实际绑定的 Worker 实例。即使相同 `slave_id` 后续重新注册，也不能让新实例冒充旧 Attempt 的执行者。

### 6.2 DynamicNode 状态

改派是同一 DynamicNode 的 Attempt 替换，不创建新 DynamicNode：

```text
accepted → dispatched → completed
                  ├── worker lost → dispatched   # replacement Attempt already created
                  └── unrecoverable → failed | decision_required
```

Observer 完成 `node.reassign` 后，节点保持 `dispatched`，因为替代 Attempt 已经进入 `created` 状态。旧 Attempt 为 `lost`，新 Attempt 为该节点唯一可接收结果的活动 Attempt。

### 6.3 改派的机械准入条件

Observer 仅在全部条件成立时接受自动改派：

1. Run 状态为 `running`，请求携带的 execution 身份与当前值一致；
2. DynamicNode 状态为 `dispatched`；
3. `lost_attempt_id` 是该节点唯一活动 Attempt；
4. 旧 Attempt 绑定的具体 Slave instance lease 已为 `expired` 或 `released`；
5. `ClosureContract.recovery_policy.allow_reassignment=true`；
6. 节点输入仍可由替代 Slave 从共享 ContentStore 解析；
7. 替代 Slave 与旧 `target` 不同，具有 active lease，支持 package 的 executor/operation，并满足机器可判定的权限和 locality；
8. 创建替代 Attempt 后仍不超过现有 `resource_budget.max_attempts`；
9. 审计事件能够记录 ClosureVersion、改派授权、package `replay_safety` 声明、输入引用和新旧目标。

任何机械条件不成立都不会静默放宽闭包约束。`replay_safety` 只作为被审计的 package 声明，不作为平台替 coding agent 或用户作出的语义安全判定；只要 `allow_reassignment=true`，它不会单独阻止改派。

## 7. `node.reassign` 控制协议

Driver 通过现有 `/internal/v1/driver/commands` 信封提交新 allowlist command `node.reassign`：

```json
{
  "request_id": "node-reassign:<lost-attempt-id>:slave-b",
  "driver_id": "driver-default",
  "instance_id": "<driver-instance>",
  "lease_id": "<opaque>",
  "driver_epoch": 3,
  "command": "node.reassign",
  "arguments": {
    "run_id": "run-...",
    "node_id": "node-...",
    "lost_attempt_id": "attempt-...",
    "expected_execution_id": "execution-...",
    "expected_execution_epoch": 1,
    "target": "slave-b",
    "reason": "worker_lease_expired"
  }
}
```

Observer 在事务中重新读取 Run 和 agent registry，不信任 Driver 提交的旧目标状态。成功响应返回 DynamicNode 与完整替代 Attempt：

```json
{
  "node": {
    "node_id": "node-...",
    "state": "dispatched"
  },
  "attempt": {
    "attempt_id": "attempt-...",
    "node_id": "node-...",
    "target": "slave-b",
    "target_instance_id": "slave-b-instance-...",
    "target_agent_epoch": 1,
    "state": "created",
    "execution_epoch": 1,
    "replaces_attempt_id": "attempt-old"
  }
}
```

### 7.1 原子效果

一次成功 command 必须同时完成：

1. 将旧 Attempt 更新为 `lost`；
2. 写入 `terminal_error={"code":"worker_lost","lease_state":"expired"}`；
3. 创建新的唯一 `attempt_id` 并绑定替代 Slave 的当前 active instance；
4. 互相写入 `replaced_by_attempt_id` 与 `replaces_attempt_id`；
5. 保持 DynamicNode 为 `dispatched`；
6. 追加一个可重放的 `node_reassigned` 事件；
7. 保存 command receipt。

事务失败不得留下只有旧 Attempt 被关闭、却没有替代 Attempt 的中间状态。

### 7.2 幂等与竞态

相同 `request_id` 重放时返回第一次创建的替代 Attempt。不同请求同时替换同一个旧 Attempt 时，只允许一个成功；其余请求返回当前替代 Attempt，而不是再创建 Attempt。

如果目标在 Driver 选择后、Observer 接受前失效，Observer 返回 `node_target_unavailable`。Driver 刷新 registry 后重选，仍受恢复时间和 Attempt 预算约束。

## 8. Driver 恢复流程

### 8.1 首次执行

1. orchestration program 发出 NodeIntent；
2. Driver 刷新 Slave registry并选择目标；
3. Observer 接受 NodeIntent；
4. Driver 请求初次 dispatch，Observer 创建绑定具体 Slave instance 的 Attempt；
5. Driver provision/dispatch Worker，并提交 terminal result。

首次目标选择和改派目标选择使用同一能力匹配函数，避免两套调度语义。

### 8.2 Worker 失联

`WorkerSession` 增加一个最小的类型化异常 `WorkerUnavailableError`，仅包装连接失败、连接中断和 HTTP transport error。现有 HTTP 业务错误、terminal `failed`、schema/validator 失败和 fencing 错误不转换为该异常。

收到 `WorkerUnavailableError` 后，单节点执行任务进入恢复循环：

1. 保留原异常和 Attempt 身份，不调用 `fail_dynamic_node()`；
2. 轮询 Observer Slave registry，使 TTL expiration 得到计算和持久化；
3. 在旧 Attempt 绑定的 Slave instance lease 仍 active 时不重复执行；
4. lease 失效后选择不同的健康目标并调用 `node.reassign`；
5. 使用响应中的替代 Attempt 重新 provision/dispatch；
6. 成功结果仍通过原 `result(handle)` 返回给 orchestration program。

恢复等待复用现有 Worker operation timeout 作为一次独立恢复窗口，不新增配置。每次替代 Attempt 都计入现有 `max_attempts`。

### 8.3 Driver 重启与编排重放

Driver 重启后从 Observer 读取 DynamicNode 和 Attempt 投影：

- `completed` 节点继续复用持久化结果；
- 存在活动 Attempt 且其目标 lease active 时，不重复 dispatch；
- 存在活动 Attempt 且其绑定实例 lease 已失效时，进入相同的 `node.reassign` 流程；
- 已有 replacement Attempt 时继续该 Attempt，不再次替换旧 Attempt。

恢复逻辑不依赖进程内 round-robin cursor 或异常对象。

## 9. 错误处理

| 事件 | 自动改派 | 处理 |
|---|---:|---|
| Worker connect/transport failure，随后旧实例 lease expired | 是 | 执行 `node.reassign` |
| Worker operation timeout，随后旧实例 lease expired | 是 | 已有 `allow_reassignment` 授权时执行 `node.reassign` |
| Worker timeout 但 lease 在恢复窗口内保持 active | 否 | 未满足 Worker 失联条件，节点进入 `awaiting_decision` |
| Worker 返回业务 `failed` | 否 | 沿用 terminal failure/repair 决策 |
| schema 或 success validator 失败 | 否 | 沿用验证失败语义 |
| package 声明 non-replay-safe | 是 | `allow_reassignment=true` 时仍可改派，并在事件中审计该声明 |
| locality 只允许旧目标 | 否 | 返回 `node_target_unavailable` 并进入决策 |
| 没有替代目标或恢复窗口耗尽 | 否 | 返回 `node_recovery_exhausted` 并进入决策 |
| 替代目标在原子提交前失效 | 重选 | 刷新 registry，不创建部分 Attempt |
| 旧 Attempt 迟到结果 | 否 | 拒绝为 `stale_attempt`，不改变 Run |

恢复循环只捕获明确的 Worker 不可达错误，不对所有 `RuntimeError` 做宽泛重试。

## 10. 并发与 fencing

设同一 Run 中 Node A 在 `slave-a`、Node B 在 `slave-b` 并发执行。Node A 改派时：

- Run 的 `execution_id` 与 `execution_epoch` 不变；
- 只有 Node A 的旧 Attempt 进入 `lost`；
- Node B 的 Attempt 保持活动，其结果仍可被 Observer 接受；
- Node A 旧 Worker 的迟到结果因 Attempt 已 `lost` 被拒绝；
- Node A 新 Attempt 的结果必须匹配当前 replacement Attempt。

`execution_epoch` 只在显式新 execution generation 时递增；它不再作为单节点改派计数器。

## 11. 事件与 provenance

`node_reassigned` 至少包含：

```json
{
  "phase": "node_reassigned",
  "run_id": "run-...",
  "execution_id": "execution-...",
  "execution_epoch": 1,
  "node_id": "node-...",
  "lost_attempt_id": "attempt-old",
  "replacement_attempt_id": "attempt-new",
  "from_target": "slave-a",
  "from_instance_id": "slave-a-instance-...",
  "from_agent_epoch": 1,
  "to_target": "slave-b",
  "to_instance_id": "slave-b-instance-...",
  "to_agent_epoch": 1,
  "reason": "worker_lease_expired",
  "source_lease_state": "expired",
  "reassignment_authorization": {
    "policy": "allow_reassignment",
    "value": true,
    "closure_version_ref": "closure-version-...",
    "user_id": "user-default",
    "origin_conversation_ref": "conversation-..."
  },
  "package_ref": {},
  "package_digest": "...",
  "package_replay_safety": "DeclaredByPackage",
  "input_refs": [],
  "created_at": "..."
}
```

事件重放必须同时重建旧 Attempt 的 `lost` 状态、replacement 关系、替代 Attempt 和 DynamicNode 的 `dispatched` 状态。事件不保存 lease secret 或内部 token。

## 12. 删除与替换

本设计不保留被新模型取代的旧路径：

- 删除 `ObserverRepository.slave_availability`；
- 删除 `RemoteObserverRepository.slave_availability`；
- 删除 `set_slave_availability()`；
- 删除 `POST /api/v1/slaves/{slave_id}/availability`；
- 删除硬编码 `slave-a → slave-b` 且递增 Run epoch 的 `reconcile()` 实现；
- 删除 `POST /api/v1/runs/{run_id}/reconcile`；
- 删除依赖上述接口的旧 E2E；
- 删除错误描述目标选择缺陷的 strict xfail。

能力查询或 UI 若需要展示可用性，直接从 active lease 派生只读结果；派生值不是可写状态。

## 13. 测试设计

### 13.1 单元测试

1. active Slave lease 被 selector 接受，expired/released lease 被排除。
2. 初次 dispatch 把具体 `instance_id` 和 agent epoch 固化到 Attempt。
3. `node.reassign` 原子产生 `lost` 旧 Attempt 和 `created` 替代 Attempt。
4. 相同 request id 重放返回同一个替代 Attempt。
5. 两个并发改派请求只产生一个替代 Attempt。
6. 旧 Attempt 迟到结果返回 `stale_attempt`。
7. Node A 改派后，Node B 在相同 Run epoch 的结果仍被接受。
8. `allow_reassignment=false`、输入不可访问、locality 或 `max_attempts` 任一不满足时拒绝改派。
9. package 声明 non-replay-safe 但 `allow_reassignment=true` 时允许改派，并完整记录声明和授权来源。
10. `WorkerSession` 只把 transport failure 分类为 `WorkerUnavailableError`。
11. fake clock 推进 lease TTL 后，Driver 恢复循环从 `slave-a` 改派到 `slave-b` 并完成。

### 13.2 Observer/Driver 集成测试

使用 ASGI transport 和两个注册 Slave 实例，验证：

- Driver 每次调度前获取最新 registry，而不是沿用启动时快照；
- `slave-a` lease 过期后，Observer 与 RemoteObserverRepository 对其状态判断一致；
- transport failure、lease expiry、`node.reassign`、替代 Worker 执行和结果提交形成完整闭环；
- Driver 在 `node.reassign` 响应丢失后重试不会重复创建 Attempt；
- Driver 重启后从事件投影继续已有 replacement Attempt。

确定性测试通过 fake clock 和明确的 Worker failure 控制时序，不把内存布尔切换描述成真实机器掉线。

### 13.3 真实 Compose G4 验收

`scripts/accept-bandgap.py --inject-failure` 使用 Python 标准库调用现有 Docker Compose，不引入依赖。故障控制器在线程中轮询 Run 事件，并在以下条件同时成立时停止容器：

- 至少一个 partition DynamicNode 已 `completed`；
- `slave-a` 上另一个 Attempt 已 `created` 或 `running`；
- 该 Attempt 尚未产生 terminal result。

验收 workload 让分配到 `slave-a` 的目标 partition 具有确定性的有限延迟，让 `slave-b` 上至少一个 partition 先完成。触发条件满足后执行：

```bash
docker compose -f deploy/docker-compose.yml stop slave-a
```

随后等待 Observer 将对应 Slave instance lease 标为 `expired`，并断言：

1. 旧 Attempt 状态为 `lost`；
2. replacement Attempt 指向 `slave-b`；
3. DynamicNode identity 不变；
4. Run `execution_id`、ClosureVersion 和 `execution_epoch` 不变；
5. `node_reassigned` 包含新旧 Attempt、目标和 lease 证据；
6. 其他健康节点结果未被错误 fence；
7. Run 最终为 `completed`；
8. 最终统计与独立 ground truth 一致且没有重复计数。

第 8 项只证明该验收 workload 在其 coding agent/用户选择的 package 和目标上得到预期结果，不形成平台对任意改派语义等价、幂等性或外部副作用的通用承诺。

验收脚本必须在 `finally` 中执行：

```bash
docker compose -f deploy/docker-compose.yml start slave-a
```

并等待 `slave-a` 重新注册为 active，防止故障验收污染后续开发或测试。

## 14. 验收标准

实现完成需同时满足：

- 不再存在可写的 Slave availability 状态或 API；
- 目标选择、初次 dispatch 和改派均基于 Observer active lease；
- `node.reassign` 具备原子性和幂等性；
- 单节点改派不递增 Run `execution_epoch`；
- 已授权改派的节点在真实容器停止后自动改派并完成；
- `allow_reassignment=false`、locality 冲突和预算耗尽会阻止自动改派；
- package replay-safety 声明和改派授权进入审计，但平台不据此承诺语义等价或副作用安全；
- 旧 Attempt 迟到结果被拒绝，健康 sibling 结果继续有效；
- 确定性测试、Observer/Driver 集成测试和真实 Compose G4 全部通过。

## 15. 演进说明

本设计刻意不预建生产级 drain/disable 控制。如果未来出现滚动维护或容量下线需求，应新增独立的调度策略状态，并定义 `schedulable = active lease && scheduling policy permits`；不能再次复用 lease 或测试故障机制表达运维意图。

当前设计已经为该演进保留清晰边界：lease 表示观察到的实例存活，Attempt 绑定具体实例，Driver 选择目标，Observer 原子授权状态转换。未来增加调度策略时只扩展候选资格，不改变本次定义的 Attempt 恢复和 fencing 语义。

同样，未来若需要平台对跨目标语义等价性作出保证，应单独设计可验证的 capability equivalence、effect evidence 和用户确认协议；本项目不预留未被当前目标使用的抽象或接口。
