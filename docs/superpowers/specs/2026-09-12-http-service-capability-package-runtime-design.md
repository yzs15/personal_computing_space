# HTTP 服务能力包：可插拔 Slave runtime 与生命周期

**日期：** 2026-09-13  
**状态：** 设计草案，Phase 2  
**前置：** [契约与 digest](2026-09-12-http-service-capability-package-contract-and-digest-design.md)  
**后续：** [按需动态编排、健康投影与 activation revision](2026-09-12-http-service-capability-package-orchestration-and-fencing-design.md)

## 1. 目标与边界

在不把 Docker/HTTP 实现编译进 Slave 主体的前提下，让指定 Slave 安装、调用、停止和恢复一个 `service + container:http/1` package。

Slave 主体只新增一次通用的进程外 runtime plugin host。以后新增或升级 runtime 时，operator 放入插件 bundle 并重启 Slave，不需要修改或重新构建 Slave 主体代码。首个插件是 `container-http-v1`；本阶段不为未出现的 runtime 设计额外扩展点。

本阶段不修改 `NodeIntent`、`DynamicNode` 或 `emit_node`，不引入 runtime attestation、Slave 后台直报、Observer 签发的 `activation_revision` 或运行中热插拔。这些能力按 Phase 3 的独立增量实施。

## 2. 进程模型

Docker 创建的能力容器不是 Slave 的 POSIX 子进程，而是由 dockerd 管理的 sibling container。Slave 启动和监管的是 runtime plugin 子进程；插件通过 Docker CLI/socket 管理服务容器。

```text
Worker API
    |
Slave Core
    |-- auth / lease / workspace / digest / schema / provenance
    |
    `-- RuntimePluginHost
            |
            `-- Unix socket HTTP
                    |
             container-http-v1 plugin process
                    |
                 Docker CLI/socket
                    |
                  dockerd
                    |
             managed service containers
```

现有 `ExecutorRegistry` 和 `ProcessJSONStdioV1Adapter` 继续只处理一次性函数：

```text
function + process:json_stdio/1 -> ExecutorRegistry
service  + container:http/1     -> RuntimePluginHost -> container-http-v1
```

`container:python_orchestrator/1` 仍只属于 Driver；`module` 暂无执行实现。Slave 不创建通用 Handler 类层次，也不把长期容器生命周期塞入 `ExecutorAdapter.invoke()`。

Driver 对 `container:python_orchestrator/1` 的特殊处理只解释编排程序并生成通用动态节点请求。普通能力包的 provision、dispatch 和 deprovision 一律走 runtime-neutral Worker API；inspect/reconcile 留在 Slave Core 与 runtime plugin 之间。新增 package contract 或 runtime plugin 不得增加 Driver 的 package-type/body 分支。

## 3. 插件 bundle 与发现

### 3.1 Bundle 布局

operator 将插件安装到只读目录：

```text
/opt/loom/runtime-plugins/container_http_v1/
  plugin.toml
  bin/runtime-plugin
  runtime.py
  server.py
```

`plugin.toml` 至少包含：

```toml
plugin_id = "container-http-v1"
protocol_version = "loom.runtime-plugin/1"
command = ["bin/runtime-plugin"]

[[supports]]
package_type = "service"
execution_kind = "container:http"
execution_version = "1"
```

插件目录和命令只能来自 operator 配置的插件根目录。Bundle 自带具体 runtime 与协议 server，不导入 `loom_v2.slave`，也不复算 package digest；它只消费 Core 已验证的 JSON manifest。CapabilityPackage、用户请求和 agent 均不能提供 executable、插件路径、socket 或 Docker 参数。拥有 Docker socket 的插件与 Slave 基础设施同等受信任，不属于普通能力代码。

### 3.2 启动发现

Slave 启动时执行一次确定性发现：

1. 按目录名排序读取直接子目录中的 `plugin.toml`；不递归搜索。
2. 拒绝未知字段、绝对/越界 command、不可执行文件和不支持的 protocol version。
3. `(package_type, execution_kind, execution_version)` 只能由一个插件提供；冲突时 Slave 启动失败，不按目录顺序覆盖。该键也必须存在于 operator contract registry，且绑定 schema 不可变。
4. 使用参数数组启动插件，不经过 shell；为每个实例分配仅 Core 和该插件可访问的私有 Unix socket 和最小环境变量。
5. 等待 `/healthz` 和 `/v1/descriptor`，验证返回能力与 manifest 一致后才宣布支持该 execution contract。

首期不做目录 watch 或在线 reload。新增、删除、升级插件后重启 Slave，使加载边界清晰且易于回滚。

### 3.3 子进程监管

`RuntimePluginHost` 为每个插件维护一个进程。插件异常退出时，当前调用失败为 `runtime_plugin_unavailable`；Host 以有限指数退避重启。重启成功后，Host 根据 Slave 持久的 desired activation 调用 `reconcile`。

shutdown 时 Host 先停止接收新调用，等待有限 drain deadline，再终止插件进程。终止插件不隐式停止 desired-running 的服务容器；下次启动通过 reconcile 重新接管。插件 stdout 不作为协议通道，避免日志污染 framing；协议只走 Unix socket，stdout/stderr 进入受限 operator 日志。

## 4. Plugin Protocol v1

使用 Unix domain socket 上的 HTTP/JSON；Slave 已依赖 `httpx`，无需增加 RPC 框架。所有业务请求包含唯一 `request_id`、workspace、target、package ref/digest 和 deadline；生命周期 mutation 还包含幂等键。Core 是 package digest 的唯一计算和校验权威，插件只消费 Core 已验证的 manifest/digest，并用它核对 Docker image、labels 和 activation，不维护第二套 package digest 实现。

```text
GET  /healthz
GET  /v1/descriptor
POST /v1/provision
POST /v1/invoke
POST /v1/inspect
POST /v1/deprovision
POST /v1/reconcile
```

`/v1/descriptor` 返回：

```json
{
  "plugin_id": "container-http-v1",
  "protocol_version": "loom.runtime-plugin/1",
  "supports": [{
    "package_type": "service",
    "execution": {"kind": "container:http", "version": "1"}
  }],
  "runtime_descriptor_ref": "runtime://service/container:http/1",
  "runtime_descriptor_digest": "<digest>"
}
```

response envelope 固定为：

```text
success: {request_id, ok: true, result}
failure: {request_id, ok: false, error: {code, retryable, safe_message, details}}
```

details 只允许 identity、状态、HTTP status 和受限计数，不返回 credential、响应 body、完整 Docker 命令或容器日志。Host 校验 response schema、request id 和大小上限；protocol violation 会隔离插件并返回 `runtime_plugin_protocol_error`。

## 5. Core 与插件职责

| Slave Core | Runtime plugin |
| --- | --- |
| internal auth、Driver epoch、Worker lease、workspace/target | Docker/runtime/network probe |
| package/ref/digest、execution contract 和通用 export 唯一权威校验 | 使用 Core 签发的 package digest 核对 image/labels，并按受支持 execution contract 解释 body/runtime binding |
| 通用 activation desired state 与幂等记录 | image inspect/pull 和 RepoDigest 校验 |
| ComputeBinding、descriptor 是否属于 `capability_exports` | HTTP path 等 runtime binding、容器 create/start/inspect/stop/remove |
| input/output schema、success validator | health endpoint 和业务 HTTP 调用 |
| Attempt、ExecutionResult、ValidationEvidence、provenance | 返回结构化 runtime state/evidence |

插件不直接写 Slave 数据库、Observer 或 Driver。Slave Core 持久化 package payload 和 desired state，并在重启后通过 `reconcile` 重放期望状态。插件可以使用自己的临时工作目录，但不得把它作为 activation 权威。

## 6. container-http-v1 插件

### 6.1 容器与网络

operator 为插件配置专属 internal Docker network。插件进程运行在 Slave 容器内，因此使用该网络访问 sibling service container。服务不发布宿主机端口。

容器参数由插件固定生成，package 不能覆盖：

```text
--detach --pull=never --restart=unless-stopped
--network=<operator-provided internal network>
--read-only --user=65534:65534
--cap-drop=ALL --security-opt=no-new-privileges
--pids-limit=<limit> --memory=<limit> --cpus=<limit>
--tmpfs=/tmp:rw,noexec,nosuid,size=<limit>
```

禁止 host network/PID/IPC、`--publish`、bind mount、named volume、device、额外 capability、Docker socket 和外部网络。Phase 2 把 socket 挂到承载 Slave Core 与插件进程的同一控制容器，因此这里只是代码职责隔离，不是 OS 权限隔离；Core 不使用 socket，被管理的能力容器也不会继承它。需要把 socket 从 Core 的权限边界彻底移除时，将同一插件协议部署为独立 sidecar。

插件使用节点预配置的 registry credential 拉取镜像，但 credential 不进入 package、activation、协议 response、事件或日志。能力容器不接收 Loom token、S3 credential、数据库连接或其他 Loom 环境变量。

### 6.2 Provision

Core 先校验身份与契约并写入 `desired_state=running`，再调用插件：

1. 插件确认请求由私有 Host 通道发出，并校验自己声明支持 package 的 type 和 execution；package digest 和 contract schema 已由 Core 验证。插件在任何 Docker mutation 前校验 HTTP path 唯一且不与 health path 冲突。
2. probe Docker CLI/socket 和 internal network。
3. inspect 本地镜像；缺失时精确 pull `image_ref`，随后验证 RepoDigest。
4. 根据 workspace、Slave id 和 package digest 派生容器名。
5. 检查同名受管容器的 labels/image digest；不匹配时只删除精确确认属于本插件的容器。
6. 使用固定安全参数 create/start。
7. 在 startup deadline 内轮询 health path。
8. 返回 `ready`、container identity 的受限证据和 `idempotent` 标志。

同一 target、package version 和 digest 的重复请求必须收敛到同一容器。相同 logical coordinate 携带不同 digest 返回 identity conflict，不替换运行中的容器。

### 6.3 Invoke

Core 校验 activation、binding、input ref 和 input schema，并确认 binding 的完整 descriptor ref 精确属于 package `capability_exports`，再把选中的完整 export 与业务 JSON 发给插件。Core 不读取 `runtime_binding`。插件重新检查 package contract、容器 labels/image digest 和 ready 状态，从 export 的 `runtime_binding.path` 解析入口，调用内部 `POST <path>`，并返回 HTTP status 与解析后的 JSON value。

Core 负责 output schema、success validator、结果 ContentStore 写入、ValidationEvidence 和 provenance。请求可能已经到达服务后，timeout、连接中断和 5xx 不在同一 Attempt 内自动重放。

服务调用可以并发；Host 和插件均支持 correlation request id。每个 activation 的并发上限由插件实现 semaphore。未发送前容量不足返回 `capability_service_busy`。

### 6.4 Inspect、reconcile 与 deprovision

`inspect` 返回容器是否存在、image/labels 是否匹配和 health 状态，不返回原始日志。

`reconcile` 接收 Core 提供的完整 desired activation 列表：desired running 且精确容器健康则接管，缺失则重建；desired stopped 则确保容器不存在。插件不能自行发现并接管其他 workspace/Slave 或无受管 labels 的容器。

`deprovision` 只停止并删除精确 package digest、target 和 labels 匹配的容器，不删除 image cache。重复调用幂等。Core 在插件成功后持久化 `activation_state=stopped` 并从 ready 能力投影移除。

## 7. 通用 activation 持久化

Slave Core 的 runtime-neutral activation 表至少包含：

```text
activation_key       # workspace + package version ref + package digest + target
runtime_plugin_id
workspace_id / slave_id
package_version_ref / package_digest
package_payload      # 完整、已验证的不可变 package JSON
desired_state        # running | stopped
activation_state     # provisioning | ready | degraded | failed | stopped
runtime_handle       # opaque、非身份字段
last_idempotency_key / last_error_code
created_at / updated_at
```

每个 activation key 使用一个异步 lock 串行化 lifecycle mutation；不同 activation 可以并行。数据库先写 desired state，再调用插件。插件返回的 `runtime_handle` 只能用于后续 inspect/invoke 提示，Core 仍以 package digest、target 和插件重新检查的 labels 为准。

Phase 2 使用现有 Driver epoch 和 Worker lease，不引入 activation 级 revision。lease 失效不改变持久 desired state；是否在控制面长期失联后停止托管容器由独立、显式的部署策略决定。乱序 lifecycle command/report 的隔离在 Phase 3B 使用 Observer 签发的 `activation_revision` 实现。

## 8. API 和错误

外部 Worker API 保持 runtime-neutral：

```text
POST /worker/v1/provision
POST /worker/v1/dispatch
GET  /worker/v1/capabilities
POST /worker/v1/deprovision
```

新增 public deactivate 控制流仍为：

```text
Observer -> Driver -> Worker -> RuntimePluginHost -> plugin /v1/deprovision
```

稳定的 Host 错误：

```text
runtime_plugin_not_found
runtime_plugin_conflict
runtime_plugin_unavailable
runtime_plugin_timeout
runtime_plugin_protocol_error
runtime_plugin_capability_mismatch
```

`container-http-v1` 保留服务错误：image unavailable/digest mismatch、container start/health failed、activation not ready、endpoint mismatch、busy、timeout、HTTP error、invalid JSON 和 response too large。

## 9. 配置和部署

Slave Core 只新增通用插件配置：

```text
LOOM_PACKAGE_CONTRACT_DIR             default /opt/loom/package-contracts
LOOM_RUNTIME_PLUGIN_DIR              default /opt/loom/runtime-plugins
LOOM_RUNTIME_PLUGIN_SOCKET_DIR       default /run/loom/runtime-plugins
LOOM_RUNTIME_PLUGIN_STARTUP_TIMEOUT  default 10
LOOM_RUNTIME_PLUGIN_CALL_TIMEOUT     bounded by operation deadline
```

`container-http-v1` 的 operator 配置通过 Host allowlist 传入插件，不由 package 控制：network、memory、CPU、PIDs、tmpfs、startup/request/Docker timeout、response bytes 和 concurrency。

首期部署构建或挂载一个包含 `plugin.toml`、自包含 executable 和 Docker CLI 的只读 bundle，并只向 Slave 容器挂载 Docker socket。以后可把同一 protocol 的插件迁到独立 sidecar，使 Docker socket 不再暴露给 Slave 容器；这不是 Phase 2 的必要实现。

## 10. 实现边界

- `slave/runtime_plugins.py`：bundle 发现、manifest 校验、进程监管、Unix socket client 和 provider 路由；
- `slave/service.py`：保持 package/ref/schema/activation 权威，经 Host 调用 provider；
- `slave/app.py`：startup/shutdown 管理 Host，Worker API 不出现 Docker 分支；
- `runtime_plugins/container_http_v1/`：独立插件 bundle，包含 Docker/HTTP 实现；
- `db/models.py`：runtime-neutral activation row；
- `driver/worker.py`、`driver/service.py`：转发 runtime-neutral provision/dispatch/deprovision；除 `container:python_orchestrator/1` 编排入口外，不按 package type 分派或解析 package body；
- `observer/repository.py`、`observer/app.py`：通过受信任 contract registry 和通用 capability exports 处理 promotion/provision/deactivate，不按 package type 分派；
- Compose/deployment：挂载插件 bundle、socket 目录、Docker socket和 internal network。

删除 generic provision 中的 `program_content_ref`、对 `package.function_body` 的无条件访问，以及 Observer/Driver/Slave Core 中按 `service` 判断的路由分支。Slave Core 不导入 `container_http_v1` 模块，也不包含 Docker command、HTTP endpoint 或容器 label 的实现细节。

## 11. 测试与验收

- manifest 未知字段、越界 command、重复 provider、协议版本错误、package contract 不匹配和 descriptor 不匹配均拒绝。
- fake plugin process 验证启动、health handshake、超时、崩溃重启、协议错误隔离和 reconcile。
- 不安装插件时函数执行正常；放入 bundle 并重启后才宣布 `service + container:http/1`。
- 新增一个 fake package contract/runtime plugin 后，Driver 无源码改动即可完成 generic provision/dispatch/deprovision；测试应检查 Driver 不出现该 package 的类型或 body 分支。
- Slave Core 测试不 import Docker plugin；插件替换不需要修改 Core 测试 fixture 或 Worker API。
- recording Docker runner 验证 inspect-hit、pull-miss、digest mismatch、固定安全参数和无 host port/mount/socket。
- provision 幂等、同名 identity conflict、重复 path/health 冲突的 mutation 前拒绝、export 路由、schema/validator、response limit 和无隐式重放。
- activation DB 重启恢复、desired stopped 不重建、deprovision 精确删除且不删除 image。
- Compose E2E 使用真实 plugin process、dockerd 和测试镜像，覆盖两个 endpoint、Slave/plugin 重启恢复和 deactivate。
