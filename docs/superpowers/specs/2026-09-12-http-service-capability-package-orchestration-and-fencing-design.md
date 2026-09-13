# HTTP 服务能力包：动态编排与高级 fencing

**日期：** 2026-09-13  
**状态：** 设计草案，Phase 3（按需实现）  
**前置：** [契约与 digest](2026-09-12-http-service-capability-package-contract-and-digest-design.md)、[Slave runtime](2026-09-12-http-service-capability-package-runtime-design.md)

## 1. 适用条件

本 spec 只在服务 endpoint 需要被动态 orchestration 节点选择，或需要 Slave/manager 重启后的强 fencing 与后台健康投影时实现。静态 ComputeBinding、基础 provision/dispatch/deprovision 不依赖本 spec。

它解决三个相互关联的问题：

1. 多端点服务不能只用 package ref 表达调用入口；
2. runtime 实现变化不能被错误地当作 package 内容变化；
3. 旧 lease、旧 manager 和迟到健康报告不能覆盖新的 activation desired state。

## 2. 动态节点的精确端点选择

### 2.1 Contract 变化

`NodeIntent` 和 `DynamicNode` 增加必填的 `capability_descriptor_ref: ResourceRef`。动态编排 API 统一为：

```text
emit_node(package_ref, capability_descriptor_ref, input_refs)
```

旧的二参数 `emit_node(package_ref, input_refs)` 不保留。函数节点也显式提供 descriptor ref，不再依赖“包内唯一 operation”的隐式规则。

### 2.2 Observer/Driver 校验

Observer 接受 intent 时必须同时校验：

- `package_ref` 的 digest 与精确 `CapabilityPackageVersion.package_digest` 相等；
- `capability_descriptor_ref` 带正确 identity criterion 和 digest；
- descriptor 确实属于该 package：函数包匹配 `operation_descriptor_ref`，服务包匹配某一个 endpoint；
- orchestration package 对被调用 package 的 allowlist 授权；
- input refs 和 IoContract/schema 合法。

Driver 创建 ComputeBinding 时沿用 intent 中的完整 descriptor ref。Worker envelope 中的短 `operation` 只作可读标签，Slave 不得以它替代 descriptor 选择。

### 2.3 身份边界

descriptor digest 是 endpoint/descriptor 的身份组成部分，并已包含在 package manifest；不新增 endpoint_id、operation_descriptor_digest 或 realization_digest 的旁路副本。动态节点记录 package ref、descriptor ref、target ref 和 execution/attempt 事实，但不把运行时容器地址写入 identity。

## 3. runtime descriptor 与能力准入

### 3.1 Descriptor 语义

Slave 注册和 heartbeat 可以声明实际支持的完整组合：

```json
{
  "package_type": "service",
  "execution": {"kind": "container:http", "version": "1"},
  "descriptor_ref": "runtime://service/container:http/1",
  "descriptor_digest": "<64 lowercase hex>"
}
```

runtime descriptor 描述启动、网络、健康检查、调用限制和安全策略。它不是 package 内容身份，也不改变 package digest；它是目标 runtime 的 capability/attestation。

### 3.2 权威来源

descriptor digest 必须有可解析的权威来源（固定 registry 或 ContentStore 正文）；不能仅凭 64 位字符串通过。Observer/Driver 在选择目标和 readiness 时，对 `package_type + execution.kind + execution.version` 进行匹配，并在需要安全策略证明时再匹配 descriptor digest。

若当前部署只有一个固定的 `container:http/1` runtime plugin，可以先只校验三元组，把 plugin descriptor digest 作为 evidence；只有存在多个安全策略实现时，才把 expected descriptor digest 写入 activation/command 并强制匹配。能力声明来自 Phase 2 的 `/v1/descriptor` handshake，不新增第二套发现协议。

## 4. 健康事实与动态 capability snapshot

### 4.1 健康报告

Slave 直接提交内部 capability health report；请求 envelope 包含 Slave instance id、lease id、epoch、workspace、package ref/digest、target 和 `session_generation`（启用 generation 后）。Observer 先做 lease/epoch fencing，再验证：

- report target 等于签发 Slave；
- package ref/digest 存在且属于 workspace/source Run 可见范围；
- package digest 与 package manifest 重算结果一致；
- endpoint projection 与服务包体一致；
- generation 与当前权威 activation 一致。

Driver 在同步 provision/deprovision 返回后可以转交同一报告，但后台状态迁移不依赖 Driver 在线。Observer 按 package、target、generation 和 state 幂等接收，不能因重复报告创建重复 activation/event。

### 4.2 Snapshot 派生

Slave 的 capability snapshot 从事实重新派生，而不是永久追加 operation 名称：

```text
基础 operations
+ 所有 ready 的函数 activation operations
+ 所有 ready 的服务 activation endpoint operations
```

多个 activation 暴露同名 operation 时，停止其中一个不得删除仍由其他 ready activation 提供的 operation。只有状态变化才发 health report/event；`degraded -> ready` 也必须报告。

## 5. Activation generation 与 fencing

### 5.1 Generation 分配

`session_generation` 是 package version + target Slave 维度的单调 fencing token，由 Observer 在 desired-state mutation 前原子分配：

```text
首次 provision        generation = 1
deprovision            generation = previous + 1
再次 provision/rebuild generation = previous + 1
同一 idempotency retry generation 不变
health transition      generation 不变
```

Driver 只转发 Observer 签发的 generation，不自行递增或默认构造 `1`。generation 与 package digest 分工：前者阻止旧会话写入，后者确认执行内容。

### 5.2 接收规则

Slave 对低于当前 generation 的命令返回 `stale_session_generation`。相同 generation 只有在 package digest、desired state 和幂等键一致时作为重试接受；更高 generation 才能改变 desired state。后台 report 必须携带当前 generation，Observer 只接受严格相等的报告。

manager instance 变化时，只有新实例获得有效 lease 后才能接管或替换属于同 workspace/slave 的旧容器。lease 失效或超过 freshness deadline 后，Slave 停止接收 dispatch，并停止其托管服务；重新获得 lease 后再 reconcile。

## 6. 完整状态和错误

服务 activation 状态：

```text
provisioning -> ready -> degraded -> ready
provisioning -> failed
ready/degraded/failed -> stopped
```

状态变化必须保留 package digest、target、generation 和 evidence refs。旧的 `ready` 报告不能覆盖新 generation 的 `stopped` 或新 package digest 的 activation。

稳定错误至少包括：

```text
stale_session_generation
stale_slave_lease
runtime_descriptor_mismatch
capability_health_report_rejected
service_activation_not_ready
```

错误 details 只包含 allowlisted identity、状态和 HTTP status，不包含 credential、响应 body 或容器日志。

## 7. 与 digest 的关系

- runtime descriptor digest 只能作为 runtime capability/evidence，除非准入策略明确要求 expected digest；不得混入 package digest。
- activation identity 使用 package version ref、package digest、target 和 generation；不新增 `activation_digest`。
- health report、ValidationEvidence 和 provenance 中的 input/output digest 只绑定事实，不产生新的资源身份。
- descriptor ref 的 digest 从权威 registry/ContentStore 重算，不由 agent 或 Slave 任意填写。

## 8. Phase 3 实现边界

主要改动集中在：

- orchestration contracts/bootstrap/runtime：NodeIntent、DynamicNode、`emit_node` 和 binding 创建；
- `observer/repository.py`、`observer/app.py`：descriptor 所属校验、health report、generation 分配和 snapshot 投影；
- `slave/runtime_plugins.py`、`slave/service.py`、`slave/app.py`：沿用既有 plugin protocol 增加 runtime descriptor attestation、lease-aware reconcile、后台 health loop 和 snapshot；
- `driver/service.py`、`driver/worker.py`：转发签发的 generation 和 descriptor；
- DB：activation generation、manager instance、health evidence（仅保存审计所需字段）。

不在本阶段建立第二套 Handler/插件系统；沿用 Phase 2 的 `RuntimePluginHost` 和 Unix socket protocol。不改变 `package_digest` manifest，不把容器 name/URL/日志正文加入任何身份字段。

## 9. Phase 3 测试与验收

- 动态 node 必须携带 descriptor ref；未知 descriptor、错误 package digest、未授权 package 和错误 target 均拒绝。
- 两个以上服务 endpoint 可通过 package + descriptor 精确路由；短 operation 名不能绕过 binding。
- runtime descriptor 缺失或 digest 不匹配时，目标不进入可安装/ready 集合；仅 evidence 模式下仍能完成基础 HTTP runtime。
- 健康 `ready -> degraded -> ready/failed -> stopped` 只在状态变化时报告；重复报告幂等。
- 旧 lease、旧 epoch、旧 manager instance 和旧 generation 不能改变新 activation 或覆盖新状态。
- 多 activation 的 capability snapshot 正确保留同名 operation，停止一个不影响其他 ready activation。
- Slave/Observer 重启后 desired state、generation 和精确 package digest 可恢复并继续 fencing。
