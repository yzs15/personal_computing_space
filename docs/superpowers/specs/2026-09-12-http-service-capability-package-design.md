# Slave 托管的 HTTP 容器服务能力包：总览

**日期：** 2026-09-13  
**状态：** 设计草案，已拆分为分阶段 spec  
**执行契约：** `package_type=service` + `execution={kind: "container:http", version: "1"}`

## 1. 为什么拆分

原设计同时包含服务包数据契约、Docker 生命周期、动态编排协议、能力发现和高级 lease fencing。它们的依赖关系不同，一次实现会把 HTTP 服务能力包扩大成整个编排和运行时控制面的升级。

本总览只定义边界和实施顺序；可执行细节分别位于：

1. [契约与 digest 设计](2026-09-12-http-service-capability-package-contract-and-digest-design.md)：先实现服务包模型、引用校验、digest 和 HTTP v1 线协议。
2. [可插拔 Slave runtime 与生命周期设计](2026-09-12-http-service-capability-package-runtime-design.md)：一次性增加进程外 runtime plugin host；Docker/HTTP 实现由可安装插件提供。
3. [动态编排与高级 fencing 设计](2026-09-12-http-service-capability-package-orchestration-and-fencing-design.md)：后置引入动态节点端点选择、runtime descriptor、后台健康报告和 generation fencing。

实现必须按上述顺序推进。第二份 spec 不依赖第三份 spec；没有明确需要动态编排或跨重启强 fencing 时，不实现第三份 spec 的内容。

## 2. 共同决策

- 首期只支持 `service + container:http/1`，不为 gRPC、MCP、流式协议或任意 Docker 参数建立通用抽象。
- `CapabilityPackageVersion` 的公共 envelope 保持通用；镜像、健康检查和端点只存在于严格类型化的服务包体。
- Agent 不手写 content/package digest。Agent 通过 `loom_put_content` 上传内容并使用返回的 `ResourceRef`；package digest 由服务根据已验证 manifest 自动计算。显式 digest 只用于一致性校验。
- ContentStore 内容使用 `content://sha256/<digest>`；package 使用独立的 `loom/package/v1` 域分离 digest。
- package 坐标、生命周期、provenance、部署位置和运行时状态不是执行内容身份，不进入 `package_digest`。
- 结果的内容身份来自 `resource_ref.digest`；不再增加旁路 result digest。
- 服务容器由 Slave 管理，Driver/Observer 不直接访问容器端口。
- Docker runtime 作为 operator 安装的进程外插件运行；CapabilityPackage 不能携带插件路径、命令或 Docker 参数。
- 新设计不保留被替代的任意字典 fallback、函数包专用服务安装字段或重复 digest 字段。

## 3. 分阶段边界

### Phase 1：契约与 digest

必须交付：严格 `ServiceCapabilityPackageBody`、严格 endpoint、OCI digest image ref、IoContract 引用、package digest、HTTP `POST + JSON` 线协议和契约测试。

不交付：Docker 生命周期、动态节点 API、后台健康投影。

### Phase 2：Slave runtime 与生命周期

必须交付：Slave 通用 `RuntimePluginHost`、进程外 `container-http-v1` 插件、固定安全参数、专属 internal network、provision/dispatch/deprovision、持久 desired state、基础 reconcile 和基础能力投影。以后增加 runtime 只安装新插件，不修改 Slave Core。

Phase 2 复用现有 Driver epoch 和 Worker lease 校验。`session_generation`、Slave 直接健康报告和动态编排属于 Phase 3。

### Phase 3：动态编排与高级 fencing

按需交付：`NodeIntent`/`DynamicNode` 的 descriptor ref、runtime descriptor attestation、后台 health report、activation generation、旧 manager 接管和动态 capability snapshot。

这些能力不得反向污染 Phase 1 的 package identity，也不得另建一套与 Phase 2 plugin host 重复的 runtime 扩展系统。

## 4. 跨文档验收

- 服务包可以由已上传的不可变内容引用构造，且 digest 可重复计算。
- 同一 package manifest 在 promotion、provision 和 Slave 重启后保持同一 `package_digest`。
- 不同 endpoint 通过精确 descriptor 引用路由；未知或不属于该包的 descriptor 必须拒绝。
- 被管理的能力容器无 host port、host mount、Docker socket、额外 capability 或外部网络。
- HTTP 超时、断连和 5xx 不在同一 Attempt 内自动重放。
- 函数包 `process:json_stdio/1` 和 Driver-side orchestrator 的现有行为不被服务专有字段污染。

## 5. 后续决策点

在实现 Phase 1 前，descriptor registry 必须明确其权威来源：descriptor ref 必须能通过既有 registry 或 ContentStore 按 digest 解析并重算，不能只接受任意 64 位字符串。`ResourceRef` 在进入 package manifest 前也必须采用统一的小写 digest 和固定 identity criterion，避免等价引用产生不同 package digest。

package 对象在生成 digest 后视为不可变；任何修改必须重新构造并重新计算，而不是原地修改嵌套 body。
