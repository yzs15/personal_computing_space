# 能力包：按需动态编排、健康投影与 activation revision

**日期：** 2026-09-13
**状态：** 设计草案，Phase 3（按需、分增量实现）
**前置：** [契约与 digest](2026-09-12-http-service-capability-package-contract-and-digest-design.md)、[Slave runtime](2026-09-12-http-service-capability-package-runtime-design.md)

## 1. 范围与实施顺序

Phase 3 不是一个整体升级，拆成三个只在对应需求出现时实施的增量：

1. **Phase 3A：动态 capability export 选择。** 当动态节点需要调用含多个 export 的 package 时实施；不依赖健康上报或 fencing。
2. **Phase 3B：activation revision fencing。** 当同一 activation 可能出现并发或迟到的 lifecycle command，或准备引入后台健康上报时实施。
3. **Phase 3C：后台健康与 capability snapshot。** 只有确实需要持续感知长运行服务状态时实施；由于异步报告可能迟到，必须先实施 Phase 3B。

现有 runtime descriptor 的 execution 三元组足以完成插件发现和目标能力匹配。只有同一 execution 三元组下确实存在多个、需要策略区分的 runtime 实现时，才另行设计 descriptor digest attestation；它不是本 Phase 的必做项。

静态 `ComputeBinding`、基础 provision/dispatch/deprovision、持久 desired state 和启动 reconcile 均不依赖本 spec。

## 2. Phase 3A：动态节点精确选择 capability export

### 2.1 Contract

`NodeIntent` 和 `DynamicNode` 增加必填的 `capability_descriptor_ref: ResourceRef`。一个 Node 表达一次 capability 调用，因此只选择一个 export：

```text
emit_node(package_ref, capability_descriptor_ref, input_refs)
```

该 descriptor ref 必须精确等于所选 package 公共 `capability_exports` 中一个 export 的 descriptor。调用同一 package 的多个 export 时创建多个 Node，并使用节点依赖表达顺序；不在单个 Node 内增加一组 descriptor 或小型编排语义。

旧的二参数 `emit_node(package_ref, input_refs)` 不保留。函数节点也显式提供 descriptor ref，不依赖“包内唯一 operation”的隐式规则。

### 2.2 Observer 与 Driver 的通用边界

Observer 接受 intent 时校验：

- `package_ref` 精确解析到一个可见的 `CapabilityPackageVersion`，其 ref digest 与 `package_digest` 相等；
- `capability_descriptor_ref` 具有合法 descriptor identity，并精确属于 package 的 `capability_exports`；
- orchestration package 的 allowlist 授权该 package；
- input refs 和所选 export 的 `IoContract`/schema 合法；
- 目标 Slave 声明支持 package 的 `package_type + execution.kind + execution.version`。

Observer 不读取 package `body` 或 export 的 `runtime_binding`，也不按 function/service/module 分支。Driver 将 intent 中的 descriptor ref 写入 `ComputeBinding`，并把所选完整 export 原样转交 Slave。Worker envelope 中的短 `operation` 仅作可读标签，不参与权威选择。

### 2.3 Slave 与 runtime plugin

Slave Core 再次确认 binding descriptor 属于 package 公共 exports，然后按 execution 三元组选择 runtime plugin。只有 plugin 解释 `body` 和 `runtime_binding`；例如 `container-http-v1` 从所选 export 读取 `runtime_binding.path`。

新增普通 package contract 时注册 JSON Schema 并安装支持同一 execution 三元组的 plugin，不修改 Observer、Driver 或 Slave Core 的 package-type 分支。`container:python_orchestrator/1` 是 Driver 唯一保留的特殊编排入口；它产生的普通动态节点仍走上述通用路径。

### 2.4 身份边界

descriptor digest 已属于 capability export，并已进入 package manifest。动态节点只记录 package ref、descriptor ref、target ref 和 execution/attempt 事实；不增加 endpoint id、operation descriptor digest 或 realization digest 的旁路副本，也不复制 `runtime_binding` 或运行时地址。

## 3. Phase 3B：最小 activation revision fencing

### 3.1 适用问题

现有 agent lease/epoch 用于淘汰旧进程，但不能区分同一 Driver 任期内先后发生的 lifecycle mutation。例如旧 `provision` 或旧 `ready` 结果可能在 `deprovision` 后迟到。`activation_revision` 只解决这一顺序问题，不替代 lease、execution epoch 或 package digest。

activation 的稳定 key 是：

```text
workspace + package_version_ref + target_slave
```

`package_digest` 是该不可变 package ref 的内容断言；同一 key 出现另一 digest 时返回 identity conflict，不创建第二条 activation。`activation_revision` 是该 activation 的 desired-state 版本，不属于 activation identity。

### 3.2 分配规则

Observer 是 revision 的唯一分配者，并在一个数据库事务内同时写入新的 desired state 和递增后的 revision：

```text
首次 desired mutation              revision = 1
desired running <-> stopped        revision = previous + 1
显式 rebuild                       revision = previous + 1
同一 idempotency request 重试       revision 不变
实际 health/state transition       revision 不变
```

Driver 只转发 Observer 签发的 revision，不自行递增或提供默认值。未实施 Phase 3B 的 Phase 1/2 API 不携带占位 revision 字段。

### 3.3 Slave 接收规则

Slave 在调用 runtime plugin 前持久化已接受的 desired state、revision、package digest 和 idempotency key：

- command revision 小于当前值：返回 `stale_activation_revision`；
- command revision 等于当前值：只有 desired state、package digest 和 idempotency key 全部相同时才作为重试接受，否则返回 `activation_revision_conflict`；
- command revision 大于当前值：接受并更新 desired state，再调用 plugin。

每个 activation key 继续使用 Phase 2 的 lifecycle lock；revision 负责跨进程、跨重启和乱序到达，lock 只负责单进程内串行化。

现有 agent lease 已绑定 `instance_id + lease_id + epoch`，不再增加 `manager_instance` fencing 字段。控制面 lease 失效不自动把 `desired_state` 改为 stopped，也不默认删除或停止托管容器；是否在控制面长期失联时停服属于独立、显式的部署策略。

### 3.4 报告接收规则

同步 lifecycle 返回或 Phase 3C 的后台报告都必须携带 `activation_revision`。Observer 只接受 revision 严格等于当前 desired activation 的报告；较小或较大的 revision 均拒绝，报告不能创建或推进 desired revision。

同一 `report_id` 的重复提交幂等。相同 revision 下，实际状态可以按允许的状态迁移更新，但任何报告都不能改变 desired state、package digest 或 target。

## 4. Phase 3C：后台健康与 capability snapshot

### 4.1 健康报告

只有需要在 provision 返回后持续观察长运行服务时，Slave 才启动后台健康报告。请求使用现有 Slave lease 身份：`agent_id`、`instance_id`、`lease_id`、`epoch` 和 workspace，并携带 package ref/digest、target、`activation_revision`、实际状态与必要 evidence refs。

Observer 先验证 Slave lease，再验证：

- report target 等于该 Slave；
- package ref/digest 在 workspace 中可见且一致；
- revision 严格等于当前 desired activation；
- 报告状态满足 activation 状态机。

不在报告中复制 capability exports。Observer 从权威 package manifest 派生投影。

### 4.2 状态与投影

最小实际状态机为：

```text
provisioning -> ready -> degraded -> ready
provisioning -> failed
ready/degraded/failed -> stopped
```

Slave capability snapshot 每次从事实派生：

```text
基础 operations
+ 所有 ready activation 的 capability exports
```

多个 ready activation 暴露同名 operation 时，停止其中一个不得移除其他 activation 仍提供的 operation。Slave 只在实际状态变化时上报；Observer 对重复 `report_id` 幂等处理。

## 5. runtime descriptor 的当前边界

Phase 2 `/v1/descriptor` handshake 和 Slave 注册信息声明所支持的 `package_type + execution.kind + execution.version`。Observer/Driver 使用该三元组筛选目标，plugin descriptor 可以作为诊断 evidence，但不进入 package digest、activation identity 或 lifecycle command。

当前不要求 descriptor ref 可从 ContentStore 解析，也不要求 Observer 匹配 descriptor digest。将来只有出现以下实际需求时再单独设计 attestation：

- 同一 execution 三元组存在多个不等价的安全策略实现；
- admission policy 必须指定其中某个实现；
- descriptor 有受信任、可重算的权威正文。

## 6. 实现边界

各增量的主要改动分别为：

- Phase 3A：`NodeIntent`、`DynamicNode`、`emit_node`、Observer 通用 export 校验和 binding 创建；
- Phase 3B：Observer desired activation 事务、command/report 的必填 `activation_revision`、Slave 持久 revision 校验；
- Phase 3C：Slave 后台 health loop、Observer health endpoint 和基于 ready activation 的 snapshot 派生。

不新增 manager-instance 表、activation digest、第二套 runtime 扩展系统或 Observer 可执行 Handler。Phase 1 的 package manifest 和 package digest 不因本阶段变化。

## 7. 验收

### Phase 3A

- 多 export package 可通过 package + descriptor 精确路由；未知、不属于 package 或未授权的 descriptor 被拒绝。
- 一个 Node 只选择一个 export；多个调用由多个 Node 和依赖关系表达。
- Observer/Driver 不读取 HTTP path；短 operation 名不能绕过 binding。
- 新增测试 contract/plugin 不需要增加 Observer、Driver 或 Slave Core 的 package-type 分支。

### Phase 3B

- 同一 idempotency retry 不递增 revision；每次新的 desired-state mutation 在同一事务内递增一次。
- Slave 重启后仍拒绝旧 revision；相同 revision 的冲突命令被拒绝。
- Observer 拒绝所有不等于当前 revision 的 lifecycle/health report。
- Driver epoch 与 activation revision 分工明确，不增加 `manager_instance` token。

### Phase 3C

- `ready -> degraded -> ready/failed -> stopped` 只在实际状态变化时报告，重复 report 幂等。
- 多 activation 的 snapshot 正确保留同名 operation。
- Slave/Observer 重启后从 desired activation 和 package manifest 重新派生状态，不依赖永久追加的 operation 列表。
