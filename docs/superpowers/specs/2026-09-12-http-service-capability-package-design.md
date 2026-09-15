# Slave 托管的 HTTP 容器服务能力包：总览

**日期：** 2026-09-13  
**状态：** 设计草案，已拆分为分阶段 spec  
**执行契约：** `package_type=service` + `execution={kind: "container:http", version: "1"}`

## 1. 为什么拆分

原设计同时包含服务包数据契约、Docker 生命周期、动态编排协议、能力发现和高级 lease fencing。它们的依赖关系不同，一次实现会把 HTTP 服务能力包扩大成整个编排和运行时控制面的升级。

本总览只定义边界和实施顺序；可执行细节分别位于：

1. [契约与 digest 设计](2026-09-12-http-service-capability-package-contract-and-digest-design.md)：先实现服务包模型、引用校验、digest 和 HTTP v1 线协议。
2. [可插拔 Slave runtime 与生命周期设计](2026-09-12-http-service-capability-package-runtime-design.md)：一次性增加进程外 runtime plugin host；Docker/HTTP 实现由可安装插件提供。
3. [按需动态编排、健康投影与 activation revision](2026-09-12-http-service-capability-package-orchestration-and-fencing-design.md)：拆分交付动态节点 capability export 精确选择、最小 activation revision fencing 和后台健康投影。

实现必须按上述顺序推进。第二份 spec 不依赖第三份 spec；第三份内部的动态选择与 fencing 也不绑定实施，后台健康仅依赖最小 revision fencing。

## 2. 共同决策

- 首期只交付 `service + container:http/1`，不定义 gRPC、MCP、流式协议或任意 Docker 参数。
- `CapabilityPackageVersion` 的公共 envelope 包含通用 `capability_exports`。Observer 只理解该公共投影；镜像、健康检查和 HTTP path 由 contract schema 约束并由 runtime plugin 解释。
- `(package_type, execution.kind, execution.version)` 是 package contract 和 runtime provider 的唯一选择键。operator contract registry 将该键不可变地绑定到受信任 JSON Schema；不兼容 schema 变化必须提升 execution version。
- Operator 通过所有运行角色共同挂载的只读 contract 目录分发纯 JSON Schema，重启后加载；新增普通 contract 不要求重新构建主体二进制。
- package contract 不是可执行代码。新增 contract 时注册 schema 并安装声明支持同一选择键的 runtime plugin，不修改 Observer、Driver 或 Slave Core 的类型分支。
- Agent 不手写 content/package digest。Agent 通过 `loom_put_content` 上传内容并使用返回的 `ResourceRef`；package digest 由服务根据已验证 manifest 自动计算。显式 digest 只用于一致性校验。
- ContentStore 内容使用 `content://sha256/<digest>`；package 使用独立的 `loom/package/v1` 域分离 digest。
- package 坐标、生命周期、provenance、部署位置和运行时状态不是执行内容身份，不进入 `package_digest`。
- 结果的内容身份来自 `resource_ref.digest`；不再增加旁路 result digest。
- 服务容器由 Slave 管理，Driver/Observer 不直接访问容器端口。
- `container:python_orchestrator/1` 是 Driver 保留的编排执行器，用于解释动态节点计划；它不是普通能力包 runtime 的扩展模板。
- 除该保留执行器外，普通能力包都通过 runtime-neutral Worker API 转发。新增 package contract 或 Slave runtime plugin 不得要求修改 Driver 源码、增加 `package_type` 分支或解析 package-specific body。
- Docker runtime 作为 operator 安装的进程外插件运行；CapabilityPackage 不能携带插件路径、命令或 Docker 参数。
- 新设计不保留被替代的任意字典 fallback、函数包专用服务安装字段或重复 digest 字段。

## 3. 分阶段边界

### Phase 1：契约与 digest

必须交付：通用 `CapabilityExport`、按 execution 三元组注册的严格 HTTP v1 package schema、OCI digest image ref、IoContract 引用、package digest、HTTP `POST + JSON` 线协议和契约测试。

不交付：Docker 生命周期、动态节点 API、后台健康投影。

### Phase 2：Slave runtime 与生命周期

必须交付：Slave 通用 `RuntimePluginHost`、进程外 `container-http-v1` 插件、固定安全参数、专属 internal network、provision/dispatch/deprovision、持久 desired state、基础 reconcile 和基础能力投影。以后增加 runtime 时注册其 package contract schema 并安装插件，不修改 Observer、Driver 或 Slave Core 的分派源码。

Phase 2 复用现有 Driver epoch 和 Worker lease 校验，不暴露未生效的 revision 占位字段。Observer 签发的 `activation_revision`、Slave 直接健康报告和动态编排按 Phase 3 的独立增量引入。

### Phase 3：按需独立增量

按需求分别交付：

- Phase 3A：`NodeIntent`/`DynamicNode` 的 descriptor ref 和基于 `capability_exports` 的通用选择；
- Phase 3B：Observer 签发的最小 `activation_revision`，只隔离乱序 lifecycle command/report；
- Phase 3C：后台 health report 和动态 capability snapshot，实施前必须先有 Phase 3B。

runtime descriptor digest attestation、`manager_instance` fencing 和控制面失联自动停服均不属于当前 Phase 3。前者只在同一 execution 存在多个策略实现时另行设计；后者由现有 lease 身份和显式部署策略处理。

这些能力不得反向污染 Phase 1 的 package identity，也不得另建一套与 Phase 2 plugin host 重复的 runtime 扩展系统。

## 4. 跨文档验收

- 服务包可以由已上传的不可变内容引用构造，且 digest 可重复计算。
- 同一 package manifest 在 promotion、provision 和 Slave 重启后保持同一 `package_digest`。
- 不同 capability export 通过精确 descriptor 引用路由；未知或不属于该包的 descriptor 必须拒绝。Observer 不读取 HTTP path，具体入口由 runtime plugin 从 export 的 `runtime_binding` 解析。
- 被管理的能力容器无 host port、host mount、Docker socket、额外 capability 或外部网络。
- HTTP 超时、断连和 5xx 不在同一 Attempt 内自动重放。
- 函数包 `process:json_stdio/1` 和 Driver-side orchestrator 使用同一通用 export 选择，不在 Observer/Driver 中增加服务专有字段分支。
- `container:python_orchestrator/1` 的 Driver 特殊分支只负责运行编排器本身；它创建的普通动态节点仍按通用 package/export/Worker 路径执行。

## 5. 后续决策点

在实现 Phase 1 前，descriptor registry 必须明确其权威来源：descriptor ref 必须能通过既有 registry 或 ContentStore 按 digest 解析并重算，不能只接受任意 64 位字符串。operator contract registry 必须为每个 execution 三元组提供唯一、不可替换的受信任 JSON Schema；schema 变化通过新的 execution version 发布。`ResourceRef` 在进入 package manifest 前也必须采用统一的小写 digest 和固定 identity criterion，避免等价引用产生不同 package digest。

package 对象在生成 digest 后视为不可变；任何修改必须重新构造并重新计算，而不是原地修改嵌套 body。
