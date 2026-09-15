# HTTP 服务能力包：契约与 digest

**日期：** 2026-09-13  
**状态：** 设计草案，Phase 1  
**前置：** 无  
**后续：** [Slave runtime 与生命周期](2026-09-12-http-service-capability-package-runtime-design.md)

## 1. 目标与边界

本 spec 只定义 `service + container:http/1` 的数据契约、身份规则和 HTTP 线协议。它不定义 Docker 命令、容器网络、activation 状态机、动态编排或 Slave 后台健康管理。

服务包由一个固定 digest 的 OCI 镜像和一个或多个 HTTP JSON capability export 组成。公共 envelope 描述可选择的逻辑能力；HTTP path 等 transport 绑定由本 contract 的 JSON Schema 约束，只有 `container-http-v1` plugin 解释。

## 2. 通用 envelope 与严格 contract

公共 package envelope 由 `CapabilityPackageVersion` 承担，不使用按 package type 写死的 body union：

```text
CapabilityPackageVersion {
    package_type: str
    execution: ExecutionContract
    capability_exports: list[CapabilityExport]  // 至少一个
    body: JSON object
}

CapabilityExport {
    capability_descriptor_ref: ResourceRef
    io_contract_ref: ResourceRef
    effect_class: str
    permissions: list[str]
    replay_safety: str
    runtime_binding: JSON object
}
```

`(package_type, execution.kind, execution.version)` 是唯一 contract key。operator contract registry 将该键不可变地绑定到受信任 JSON Schema；Observer 使用项目已有 `jsonschema` 依赖，对 `{package_type, execution, capability_exports, body}` 校验，不加载或执行 contract 提供的代码。registry 将下面的 HTTP v1 schema 固定绑定到 `service + container:http/1`：

```text
ServiceContainerHttpV1Body {
    image_ref: str
    container_port: int       // 1..65535
    health_path: str
}

ServiceContainerHttpV1RuntimeBinding {
    path: str
}
```

HTTP v1 schema 对 body 和每个 export 的 `runtime_binding` 使用 `additionalProperties: false`，并要求 `runtime_binding={"path": ...}`。通用模型只允许 JSON value，不允许 Python 对象。未注册的 contract key、schema 不匹配、未知字段和缺失必填字段直接失败。一个 key 一经注册不得改绑 schema；不兼容 schema 变化必须使用新的 execution version。新增 package contract 通过注册 schema 扩展，不修改 `CapabilityPackageVersion`、Observer 或 Driver 的源码类型分支。

Operator contract 以纯 JSON 文件安装到所有 Observer、Driver 和 Slave 共同挂载的只读目录 `LOOM_PACKAGE_CONTRACT_DIR`，进程启动时按文件名排序加载，首期不做在线 reload。文件只包含 `package_type`、`execution={kind, version}` 和完整 JSON Schema；不允许 executable、Python validator 或 package 自带 schema。新增普通 contract 只需分发同一 schema 文件并重启相关进程，不重新构建主体二进制。

### 2.1 引用规则

- `image_ref` 必须匹配 `<registry>/<repository>@sha256:<64 lowercase hex>`。tag、裸 repository、Docker image ID 和命令片段不合法。
- `io_contract_ref` 必须是 `content://sha256/<digest>`，`identity_criterion` 固定为 `content_digest`。Observer 在创建、promotion 和 provision 时读取并验证 `io.v1` contract；schema ref 若存在也必须读取并验证。
- `capability_descriptor_ref` 必须带 `identity_criterion=descriptor_digest` 和 64 位十六进制 digest。
- descriptor digest 不能只做字符串形状校验。权威 descriptor registry 必须能按 `resource_id + digest` 返回不可变 descriptor；registry 对 descriptor 正文使用项目统一 JCS 序列化和 `loom/operation-descriptor/v1` 域分离。无法解析或重算不一致时拒绝 package。
- descriptor registry 的访问凭据和部署地址不进入 package body 或 package digest。
- `health_path` 和 export `runtime_binding.path` 必须是以 `/` 开头、不含 query、fragment、scheme 或 host 的 origin-form path；health path 不得与业务 path 相同。
- capability descriptor 在同一 package 内唯一，由公共 envelope 校验。JSON Schema 校验每个 HTTP path 的形状；需要跨数组比较的 path 唯一性和 health 冲突由 `container-http-v1` plugin 在 provision 前校验，Observer 不为此加入 HTTP 专用代码。

## 3. package digest

### 3.1 生成责任

Agent 不计算或手写 digest。Agent 先通过 `loom_put_content` 上传程序、IoContract、schema 或其他内容，再使用服务返回的 `ResourceRef`。创建 package 时 `package_digest` 可以省略，由 Observer/Repository 根据 manifest 计算；若显式提供，只能用于一致性校验。

### 3.2 身份边界

package manifest 仅包含：

```text
{
  "package_type": "service",
  "execution": {"kind": "container:http", "version": "1"},
  "capability_exports": <normalized CapabilityExport list>,
  "body": <schema-validated JSON object>
}
```

`package_id`、`package_version`、`package_closure_version_ref`、`source_*`、`scope`、`publication_state`、provenance、activation、target、container name 和 runtime 状态都不属于 package 内容身份。promotion 不得改变同一 execution manifest 的 digest。

嵌套 `ResourceRef` 只取规范化后的身份字段，不取 `access_binding` 或 provenance：

```text
resource_id + version_or_digest + identity_criterion
```

规范化要求：digest 一律小写；content ref 固定为 `content://sha256/<digest>` 与 `content_digest`；descriptor ref 的 criterion 固定为 `descriptor_digest`。等价引用必须生成相同 manifest。

### 3.3 算法与列表语义

对规范化 manifest 使用现成 `rfc8785` 库实现 RFC 8785/JCS，再计算：

```text
SHA-256(UTF-8("loom/package/v1") + 0x00 + JCS(manifest))
```

wire 值必须是 64 位小写十六进制。`capability_exports` 按 descriptor `resource_id + digest` 唯一并升序规范化；因此仅调整输入顺序不会改变 package identity。具有集合语义的 permission/ref 列表也必须在规范化层排序；具有顺序语义的列表必须在 contract schema 中显式声明。

package 计算 digest 后视为不可变。禁止原地修改嵌套 body；任何修改必须重新构造、重新校验并产生新的 digest。持久化、provision 和 dispatch 入口都应重新验证显式 digest 与 manifest 一致，防止可变对象绕过校验。

### 3.4 digest 收敛边界

| 对象 | 唯一身份 | 不再额外保存 |
| --- | --- | --- |
| ContentStore 内容 | `content://sha256/<digest>` | 裸 `version_or_digest` 副本 |
| CapabilityPackage | JCS manifest 的 `package_digest` | 坐标、生命周期、provenance 的 hash |
| Operation descriptor | registry 返回的 descriptor ref/digest | `operation_descriptor_digest` 旁路字段 |
| IoContract/schema | 各自 content ref digest | schema/semantics digest 副本 |
| ExecutionResult | `resource_ref.digest`，由结果值重算 | 独立 result `digest` 字段 |
| Closure | `snapshot_digest` 仅作快照完整性指纹 | 将 snapshot digest 复制到 package/binding |

`ValidationEvidence.input_digest/output_digest` 只绑定一次验证事实；它们不是新资源身份，由 Observer 根据 ref/value 重算。结果传输不得接受独立 `digest` 字段。

## 4. HTTP v1 线协议

镜像使用默认 `ENTRYPOINT/CMD`，以非 root `65534:65534` 运行，并监听 `0.0.0.0:<container_port>`。服务必须提供：

- `GET <health_path>`：任意 `2xx` 表示 ready，正文忽略；
- 每个 capability export 的 `runtime_binding.path` 对应 `POST <path>`：请求 `Content-Type: application/json`，body 为一个 JSON document；
- 成功响应为 `2xx` 和一个 JSON document；重定向不跟随。

Slave 只发送通过 IoContract 校验的业务 JSON，并设置 `Content-Type` 和 `Accept` 为 `application/json`。请求/响应正文不写入 Loom 日志；响应超过 operator 限制、非法 JSON、非 2xx 和超时转换为稳定错误码。一次请求可能已经到达服务时，当前 Attempt 不自动重放。

端口、路径、镜像、capability descriptor 和 IoContract 均属于 package manifest；容器内部 URL、名称和 host 位置不属于 package identity。contract schema 由 manifest 中的 execution 三元组确定，不再保存额外 contract ref。

## 5. 创建与校验流程

1. Operator 在 contract registry 将 HTTP v1 JSON Schema 不可变地注册为 `service + container:http/1`。
2. Agent 上传 IoContract/schema 并取得 immutable `ResourceRef`。
3. Agent 提交 execution 三元组、capability exports 和服务 body，不提交 contract ref 或自行计算的 digest。
4. Observer 解析受信任的 package contract，以 JSON Schema 校验 manifest，再通用校验 descriptor registry 和 IoContract；不执行 `service` 专用代码分支。
5. Observer 规范化 manifest 并生成 `package_digest`。
6. 显式 `package_digest` 存在时与计算值比较，不一致返回 `package_digest_mismatch`。
7. promotion/provision 只携带完整已验证 package 和计算出的 digest。

## 6. Phase 1 测试与验收

- 完整服务包 JSON round trip 后仍通过同一 contract schema；未知字段和未注册 execution 三元组均拒绝。
- contract/schema 校验拒绝 tag 镜像、非法端口/path、重复 descriptor export、缺失或错误 IoContract；plugin provision 测试拒绝重复 HTTP path 和 health 冲突，且不得先创建容器。
- descriptor registry 缺失、digest 不匹配和错误 criterion 均拒绝。
- 相同 manifest（包括 export 重排、不同 access binding/provenance）产生相同 package digest；改变 execution version、镜像 digest、health path、descriptor、IoContract、path 或安全策略会改变 digest。
- 使用 registry fixture 注册第二种 package contract 后，Observer 的同一通用校验路径可以接收它；测试不得新增按 `package_type` 分支。
- `package_digest` 省略时自动生成；显式错误 digest 和 symbolic placeholder 拒绝。
- 结果只接受与 canonical JSON 值一致的 `resource_ref.digest`，独立 result digest 拒绝。
- HTTP 请求/响应契约、重定向、大小限制和“不在同一 Attempt 自动重放”有单测。
