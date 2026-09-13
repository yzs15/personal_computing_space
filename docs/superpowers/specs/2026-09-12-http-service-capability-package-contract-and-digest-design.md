# HTTP 服务能力包：契约与 digest

**日期：** 2026-09-13  
**状态：** 设计草案，Phase 1  
**前置：** 无  
**后续：** [Slave runtime 与生命周期](2026-09-12-http-service-capability-package-runtime-design.md)

## 1. 目标与边界

本 spec 只定义 `service + container:http/1` 的数据契约、身份规则和 HTTP 线协议。它不定义 Docker 命令、容器网络、activation 状态机、动态编排或 Slave 后台健康管理。

服务包由一个固定 digest 的 OCI 镜像和一个或多个 HTTP JSON endpoint 组成。端点通过现有 `ResourceRef` 引用 operation descriptor 和 `io.v1` contract。

## 2. 严格模型

公共 package envelope 继续由 `CapabilityPackageVersion` 承担。服务专有字段只允许出现在类型化 body：

```text
ServiceCapabilityPackageBody {
    image_ref: str
    container_port: int       // 1..65535
    health_path: str
    endpoints: list[HttpServiceEndpoint]  // 至少一个
}

HttpServiceEndpoint {
    endpoint_id: str          // [a-z0-9][a-z0-9._-]{0,63}
    operation_descriptor_ref: ResourceRef
    path: str
    io_contract_ref: ResourceRef
    effect_class: str
    permissions: list[str]
    replay_safety: str
}
```

package body 使用 `extra=forbid`。未知 package type、未知 body 字段、缺失必填字段和把函数包字段放进服务包均直接失败；不保留任意 `dict[str, Any]` fallback。

### 2.1 引用规则

- `image_ref` 必须匹配 `<registry>/<repository>@sha256:<64 lowercase hex>`。tag、裸 repository、Docker image ID 和命令片段不合法。
- `io_contract_ref` 必须是 `content://sha256/<digest>`，`identity_criterion` 固定为 `content_digest`。Observer 在创建、promotion 和 provision 时读取并验证 `io.v1` contract；schema ref 若存在也必须读取并验证。
- `operation_descriptor_ref` 必须带 `identity_criterion=descriptor_digest` 和 64 位十六进制 digest。
- descriptor digest 不能只做字符串形状校验。权威 descriptor registry 必须能按 `resource_id + digest` 返回不可变 descriptor；registry 对 descriptor 正文使用项目统一 JCS 序列化和 `loom/operation-descriptor/v1` 域分离。无法解析或重算不一致时拒绝 package。
- descriptor registry 的访问凭据和部署地址不进入 package body 或 package digest。
- `health_path` 和 endpoint `path` 必须是以 `/` 开头、不含 query、fragment、scheme 或 host 的 origin-form path；health path 不得与 endpoint path 相同。
- endpoint 的 `endpoint_id`、path 和 descriptor digest 在同一 package 内唯一。

## 3. package digest

### 3.1 生成责任

Agent 不计算或手写 digest。Agent 先通过 `loom_put_content` 上传程序、IoContract、schema 或其他内容，再使用服务返回的 `ResourceRef`。创建 package 时 `package_digest` 可以省略，由 Observer/Repository 根据 manifest 计算；若显式提供，只能用于一致性校验。

### 3.2 身份边界

package manifest 仅包含：

```text
{
  "package_type": "service",
  "execution": {"kind": "container:http", "version": "1"},
  "body": <validated ServiceCapabilityPackageBody>
}
```

`package_id`、`package_version`、`package_closure_version_ref`、`source_*`、`scope`、`publication_state`、provenance、activation、target、container name 和 runtime 状态都不属于 package 内容身份。promotion 不得改变同一 execution manifest 的 digest。

嵌套 `ResourceRef` 只取规范化后的身份字段，不取 `access_binding` 或 provenance：

```text
resource_id + version_or_digest + identity_criterion
```

规范化要求：digest 一律小写；content ref 固定为 `content://sha256/<digest>` 与 `content_digest`；descriptor ref 的 criterion 固定为 `descriptor_digest`。等价引用必须生成相同 manifest。

### 3.3 算法与列表语义

对规范化 manifest 使用 RFC 8785/JCS，再计算：

```text
SHA-256(UTF-8("loom/package/v1") + 0x00 + JCS(manifest))
```

wire 值必须是 64 位小写十六进制。endpoint 列表是按 `endpoint_id` 唯一的集合，计算 manifest 前按 `endpoint_id` 升序规范化；因此仅调整输入顺序不会改变 package identity。具有集合语义的 permission/ref 列表也必须在规范化层排序；具有顺序语义的列表必须在本 spec 中显式声明。

package 计算 digest 后视为不可变。禁止原地修改嵌套 body；任何修改必须重新构造、重新校验并产生新的 digest。持久化、provision 和 dispatch 入口都应重新验证显式 digest 与 manifest 一致，防止可变对象绕过校验。

### 3.4 digest 收敛边界

| 对象 | 唯一身份 | 不再额外保存 |
| --- | --- | --- |
| ContentStore 内容 | `content://sha256/<digest>` | 裸 `version_or_digest` 副本 |
| Service CapabilityPackage | JCS manifest 的 `package_digest` | 坐标、生命周期、provenance 的 hash |
| Operation descriptor | registry 返回的 descriptor ref/digest | `operation_descriptor_digest` 旁路字段 |
| IoContract/schema | 各自 content ref digest | schema/semantics digest 副本 |
| ExecutionResult | `resource_ref.digest`，由结果值重算 | 独立 result `digest` 字段 |
| Closure | `snapshot_digest` 仅作快照完整性指纹 | 将 snapshot digest 复制到 package/binding |

`ValidationEvidence.input_digest/output_digest` 只绑定一次验证事实；它们不是新资源身份，由 Observer 根据 ref/value 重算。结果传输不得接受独立 `digest` 字段。

## 4. HTTP v1 线协议

镜像使用默认 `ENTRYPOINT/CMD`，以非 root `65534:65534` 运行，并监听 `0.0.0.0:<container_port>`。服务必须提供：

- `GET <health_path>`：任意 `2xx` 表示 ready，正文忽略；
- 每个 endpoint 的 `POST <path>`：请求 `Content-Type: application/json`，body 为一个 JSON document；
- 成功响应为 `2xx` 和一个 JSON document；重定向不跟随。

Slave 只发送通过 IoContract 校验的业务 JSON，并设置 `Content-Type` 和 `Accept` 为 `application/json`。请求/响应正文不写入 Loom 日志；响应超过 operator 限制、非法 JSON、非 2xx 和超时转换为稳定错误码。一次请求可能已经到达服务时，当前 Attempt 不自动重放。

端口、路径、镜像、endpoint descriptor 和 IoContract 均属于 package manifest；容器内部 URL、名称和 host 位置不属于 package identity。

## 5. 创建与校验流程

1. Agent 上传 IoContract/schema 并取得 immutable `ResourceRef`。
2. Agent 提交服务 package body，不提交自行计算的 digest。
3. Observer 校验 package type/execution、镜像、endpoint、descriptor registry 和 IoContract。
4. Observer 规范化 manifest 并生成 `package_digest`。
5. 显式 `package_digest` 存在时与计算值比较，不一致返回 `package_digest_mismatch`。
6. promotion/provision 只携带完整已验证 package 和计算出的 digest。

## 6. Phase 1 测试与验收

- 完整服务包 JSON round trip 后仍为严格服务包体；未知字段、函数字段和任意 execution contract 均拒绝。
- tag 镜像、非法端口/path、重复 endpoint、health 冲突、缺失或错误 IoContract 均拒绝。
- descriptor registry 缺失、digest 不匹配和错误 criterion 均拒绝。
- 相同 manifest（包括 endpoint 重排、不同 access binding/provenance）产生相同 package digest；改变镜像 digest、health path、descriptor、contract、path 或安全策略会改变 digest。
- `package_digest` 省略时自动生成；显式错误 digest 和 symbolic placeholder 拒绝。
- 结果只接受与 canonical JSON 值一致的 `resource_ref.digest`，独立 result digest 拒绝。
- HTTP 请求/响应契约、重定向、大小限制和“不在同一 Attempt 自动重放”有单测。
