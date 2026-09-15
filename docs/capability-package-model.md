# Capability Package 模型

**日期：** 2026-09-14
**状态：** Phase 1/2/3 实现说明

本文说明当前仓库中的 Capability Package 公共模型、contract 选择、digest 和 runtime 扩展边界。详细设计见：

- [HTTP 服务能力包总览](superpowers/specs/2026-09-12-http-service-capability-package-design.md)
- [契约与 digest](superpowers/specs/2026-09-12-http-service-capability-package-contract-and-digest-design.md)
- [可插拔 Slave runtime](superpowers/specs/2026-09-12-http-service-capability-package-runtime-design.md)

## 1. 公共模型

`CapabilityPackageVersion` 不再使用按 package type 写死的 body union。所有普通能力包共享同一个 runtime-neutral envelope：

```text
CapabilityPackageVersion {
    package_type: str
    execution: { kind: str, version: str }
    capability_exports: list[CapabilityExport]
    body: JSON object
    package_digest: sha256

    package_id / package_version
    package_closure_version_ref
    source_run_ref / source_closure_version_ref
    scope / publication_state / provenance
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

公共层只理解 export、不可变引用和生命周期字段。`body` 与 `runtime_binding` 的含义由 package contract 校验、由对应 runtime provider 解释。

## 2. 唯一选择键

Package contract、Slave 内置 executor 和进程外 runtime plugin 都使用同一个键：

```text
(package_type, execution.kind, execution.version)
```

`package_type` 与执行方式不是可以互相替代的字段。例如：

```text
function + process:json_stdio/1
function + container:python_orchestrator/1
service  + container:http/1
```

同一三元组只能绑定一个 contract schema 和一个 runtime provider。不兼容的 schema 或协议变化必须提升 `execution.version`，不能覆盖已有绑定。

## 3. Package contract registry

Contract 是受信任的 JSON Schema，不是 package 携带的可执行代码。内置 contract 包括：

- `function + process:json_stdio/1`
- `function + container:python_orchestrator/1`
- `service + container:http/1`

Operator 还可以将 contract JSON 文件安装到 `LOOM_PACKAGE_CONTRACT_DIR`（默认 `/opt/loom/package-contracts`），重启进程后加载。文件格式为：

```json
{
  "package_type": "example",
  "execution": {"kind": "vendor:runtime", "version": "1"},
  "schema": {"$schema": "https://json-schema.org/draft/2020-12/schema"}
}
```

Schema 必须校验完整的执行 manifest：

```text
{package_type, execution, capability_exports, body}
```

未知三元组、缺失字段、未知字段或 schema 不匹配都会拒绝。Registry 是 append-only；重复注册相同 schema 幂等，改绑为不同 schema 失败。

这使新增普通 package contract 不需要给 `CapabilityPackageVersion`、Observer 或 Driver 增加类型分支。

## 4. 引用与 digest

Agent 使用 `loom_put_content` 上传程序、JSON Schema、IoContract 和 operation descriptor，并直接使用返回的 `ResourceRef`。Agent 不计算 content、descriptor 或 package digest。

引用规则如下：

- 普通内容：`content://sha256/<lowercase digest>` 和 `content_digest`；
- operation descriptor：`descriptor_digest`，正文采用 JCS 和 `loom/operation-descriptor/v1` 域分离计算；
- IoContract：必须是可读取并通过 `io.v1` 校验的 content ref；
- package：对规范化 execution manifest 使用项目的 `rfc8785` 库进行 JCS 序列化，再以 `loom/package/v1` 域分离计算 SHA-256。

Package digest 只覆盖 `package_type`、`execution`、规范化后的 `capability_exports` 和 `body`。坐标、scope、publication state、provenance、activation、Slave 和容器状态不进入 package identity。

Export 按完整 descriptor identity 排序，permissions 按集合语义排序；`ResourceRef.access_binding` 和 provenance 不参与 digest。因此 export 顺序或部署位置变化不会改变同一 manifest 的 digest。

## 5. Phase 1 HTTP service contract

`service + container:http/1` 的 body 为：

```json
{
  "image_ref": "registry.example/org/service@sha256:<64 lowercase hex>",
  "container_port": 8080,
  "health_path": "/healthz"
}
```

每个 export 的 runtime binding 为：

```json
{"path": "/v1/operation"}
```

Schema 拒绝 tag image、非法端口、非 origin-form path 和未知字段。跨 export 的重复 path 与 health path 冲突由 `container-http-v1` plugin 在任何 Docker mutation 前拒绝。

服务线协议固定为：

- `GET <health_path>` 的任意 2xx 表示 ready；
- `POST <runtime_binding.path>` 发送和接收 JSON document；
- 不跟随重定向；
- timeout、连接错误、5xx、非法 JSON 和超限响应不在同一 Attempt 内自动重放。

## 6. Phase 2 runtime plugin

Slave Core 只提供一次性的通用 `RuntimePluginHost`。Operator 将 plugin bundle 放入 `LOOM_RUNTIME_PLUGIN_DIR`（默认 `/opt/loom/runtime-plugins`）并重启 Slave。Bundle 使用 `plugin.toml` 声明：

```toml
plugin_id = "container-http-v1"
protocol_version = "loom.runtime-plugin/1"
command = ["bin/runtime-plugin"]

[[supports]]
package_type = "service"
execution_kind = "container:http"
execution_version = "1"
```

Host 只按三元组路由，不读取 package-specific body。声明未注册 contract、重复 provider、manifest 越界或 descriptor handshake 不一致都会导致插件加载失败。

仓库附带的 `runtime_plugins/container_http_v1/` 是完整 bundle：具体 Docker/HTTP runtime 和协议 server 都位于该目录，不导入 `loom_v2.slave`，也不实现第二套 package digest。替换或新增普通 runtime 只需安装另一个 bundle 并重启 Slave。

Core 与插件通过私有 Unix socket 上的 HTTP/JSON 通信。Core 负责 auth、lease、workspace、package digest、export 选择、IoContract、activation 持久化和结果验证；插件负责解释其 contract 的 body/runtime binding 以及具体 runtime 生命周期。

`container-http-v1` 插件通过 Docker CLI/socket 管理由 dockerd 承载的 sibling container。能力容器不是 Slave 的 POSIX 子进程，也不获得 Loom credential、Docker socket、host port、volume、device 或额外 capability。插件固定使用 internal network、只读 rootfs、非 root 用户、cap-drop、no-new-privileges 和资源限制。

## 7. Driver 边界

`container:python_orchestrator/1` 是 Driver 保留的唯一特殊执行类型：Driver 解释其编排程序并生成通用动态节点请求。

其他普通能力包统一通过 runtime-neutral Worker API 完成 provision、dispatch 和 deprovision。Driver 不解析其 body，不按 `package_type` 分派。新增普通 contract/runtime 时，Operator 注册 schema 并安装声明相同三元组的 plugin；Observer、Driver 和 Slave Core 均无需增加该类型的源码分支。

## 8. Phase 3 动态编排与健康投影

动态节点统一使用三参数 `emit_node(package_ref, capability_descriptor_ref, input_refs)`；Observer、Driver 和 Slave 都按 package 公共 `capability_exports` 的完整 descriptor identity 选择单个 export。多个 export 调用由多个 Node 表达，不把 runtime binding 或 endpoint 地址复制到 Node identity。

activation 使用 Observer 分配的单调 `activation_revision`。Slave 在持久化 desired state、package digest 和幂等键后才调用 runtime；低 revision 和相同 revision 冲突命令会被拒绝。健康报告必须严格匹配当前 revision，重复 `report_id` 幂等。

长运行 runtime 由 Slave 后台 health loop 检查，状态从 `ready/degraded/failed/stopped` 事实投影到 Observer 和 Slave capability snapshot。快照每次从基础 operation 与 ready activation 的 exports 重算，多 activation 暴露同名 operation 时停止一个不会删除另一个。

## 9. 当前边界

Phase 1/2/3 已覆盖 package contract、digest、HTTP runtime、动态 descriptor 选择、activation revision fencing、后台健康与 capability snapshot。runtime descriptor digest attestation 仅在同一 execution 确有多个策略实现时另行设计。这些运行时事实均不属于 package identity，也不引入第二套 runtime 扩展系统。
