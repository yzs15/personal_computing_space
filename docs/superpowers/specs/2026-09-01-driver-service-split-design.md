# Driver 独立服务与 Codex 容器化设计

**日期：** 2026-09-01  
**状态：** 待评审  
**对应文档：** `docs/superpowers/specs/2026-08-26-distributed-analysis-e2e-design.md`

**可靠性修订：** 消息投递、request receipt、turn 所有权和恢复状态机由
`docs/superpowers/specs/2026-09-01-message-turn-reliability-redesign.md`
进一步定义；该修订覆盖本文 4.1、5.2 和 5.3 中对应的消息异步与幂等语义。

## 1. 目标与决策

本设计将 Driver 从 Observer 进程中移出，形成独立的容器化控制服务。Driver 镜像内安装固定版本的 Codex CLI 和 Docker CLI，通过 Docker Compose secrets 获取模型凭据，通过 Docker socket 启动 orchestration sandbox，并通过 Worker HTTP 直连 Slave。

本设计默认单 Workspace、单活跃 Driver，不保留 Observer 内嵌 Driver 的旧运行路径。Observer 继续是唯一对外入口和唯一状态权威；Driver 负责 coding-agent 会话、编排运行时、动态调度和执行面通信。

必须满足：

1. Observer 不需要依赖 Driver 或 Slave 才能启动并提供健康检查。
2. Driver 重启后恢复同一 `conversation_ref` 对应的 Codex `thread_id`。
3. Driver 与 Slave 直连执行，Observer 不代理 provision/dispatch 数据面请求。
4. Run、Node、Attempt、lease、thread binding 和事件仍由 Observer 持久化。
5. Codex API key 只通过 Docker secret 进入 Driver，不挂载宿主机 `~/.codex`。
6. Driver 旧 epoch 的请求不能修改 Observer 状态，也不能向 Slave 派发任务。

设计原则：不默认保留旧接口兼容层；优先复用现有 FastAPI、httpx、SQLAlchemy、`WorkerSession`、`CodexAppServerProvider` 和 ContentStore；删除被新边界取代的 Observer 内嵌 Driver、Observer 侧 WorkerSession 及未使用的 `driver-db`。

## 2. 服务边界

```text
Client / Browser / MCP
          │ public HTTP
          ▼
      Observer :18080
          │ internal authenticated RPC
          ▼
      Driver :8090
       │       │              │
       │       │              └── MinIO/S3
       │       ├── Worker HTTP ──▶ Slave A/B
       │       └── codex app-server (stdio)
       │
       └── Docker socket ──▶ orchestration sandbox
```

### 2.1 Observer

Observer 只承担：

- 公网 API、MCP 入口和静态 UI；
- Run、Closure、Node、Attempt、事件、能力包和 ContentStore 引用的权威读写；
- Driver/Slave agent registry、lease 和 epoch fencing；
- 将消息和中断请求转发给当前 active Driver；
- 对 Driver 回传的结果执行最终校验并持久化。

Observer 不再：

- 创建 `DriverService` 或 coding-agent provider；
- 持有 Codex CLI、Codex secret 或 Docker socket；
- 创建本地 `SlaveService` 镜像或持有 `WorkerSession`；
- 代理 Driver→Slave 的 provision/dispatch 数据面。

### 2.2 Driver

Driver 负责：

- 向 Observer 注册并维护 Workspace lease；
- 暴露内部消息和中断 endpoint；
- 启动和恢复 Codex app-server，维护 conversation/thread 绑定；
- 承载 Driver MCP、闭包细化和 coding-agent turn 生命周期；
- 运行 `orchestrator_python_v1` Docker sandbox；
- 根据 Observer 授权和 Slave capability 选择目标；
- 通过 `WorkerSession` 直连 Slave，执行 provision/dispatch；
- 将消息、agent signal、调度决策、节点结果和 turn 状态提交 Observer。

Driver 不直接访问 Observer 数据库，不直接修改 Observer 内存对象；所有权威状态操作经过带 lease fencing 的内部 API。

### 2.3 Slave

Slave 继续只执行已绑定和激活的能力包。Slave 启动后向 Observer 注册 endpoint、executor 和 term support，定期 heartbeat；它不依赖 Driver 数据库，也不接触 Codex secret。

## 3. Agent 注册与 lease

### 3.1 Registry

Observer 新增 `runtime_agents` 表：

```text
agent_id             text
role                 text              # driver | slave
instance_id          text
workspace_id         text
endpoint_url         text
protocol_version     text
capabilities         jsonb
epoch                bigint
lease_state          text              # active | expired | released
last_seen_at         timestamptz
created_at           timestamptz
updated_at           timestamptz
```

主键为 `(workspace_id, agent_id, instance_id)`；同一 Workspace 的 Driver 额外受 active lease 唯一约束。Driver 和 Slave 使用稳定 `agent_id`、每次启动生成 `instance_id`。

### 3.2 注册协议

```text
POST /internal/v1/agents/register
POST /internal/v1/agents/{agent_id}/heartbeat
POST /internal/v1/agents/{agent_id}/release
GET  /internal/v1/agents/slaves?workspace_id=<id>
```

注册请求必须包含 `role`、`agent_id`、`instance_id`、`workspace_id`、`endpoint_url`、`protocol_version` 和能力描述。请求使用 Compose secret 注入的内部 API token。

Driver 注册成功后 Observer：

1. 使同一 Workspace 的旧 Driver lease 失效；
2. 递增 `driver_epoch`；
3. 返回 `lease_id`、`driver_epoch`、heartbeat interval、当前 thread bindings 和可恢复 Run 摘要；
4. 持久化 `driver_registered` 事件。

所有 Driver→Observer 命令都携带 `driver_id`、`instance_id`、`lease_id` 和 `driver_epoch`。旧 lease 或 epoch 返回 `409 stale_driver_epoch`。

Heartbeat 超时只使 lease 进入 `expired`，不删除 thread binding 和 Run 事件。新 Driver 可重新注册并接管。

### 3.3 启动依赖

Observer 只等待自己的 PostgreSQL 和 MinIO 初始化完成。Driver、Slave 使用注册重试，不要求 Observer 在 Compose 中等待它们；Driver 也不要求 Compose 等待 Slave，WorkerSession 在运行时使用 endpoint 健康检查和有界重试。

## 4. Observer 公网网关与 Driver 内部 API

### 4.1 公网入口

Observer 保留：

```text
POST /api/v1/messages
POST /api/v1/conversations/{conversation_ref}/interrupt
GET  /api/v1/conversations/{conversation_ref}/stream
```

`/api/v1/messages` 必须包含 `request_id`、`conversation_ref` 和 `text`。Observer 根据 Workspace active Driver endpoint 转发：

```text
POST /driver/v1/messages
POST /driver/v1/conversations/{conversation_ref}/interrupt
```

Observer 不保存 Driver 的进程对象。没有 active Driver、Driver endpoint 超时或 lease 已失效时，返回结构化 `driver_unavailable`。

事件流仍由 Observer 从事件日志生成，不由 Driver 代理流式输出。

### 4.2 Driver→Observer 控制协议

Driver 使用固定命令集合的内部 JSON RPC：

```text
POST /internal/v1/driver/commands
```

请求信封：

```json
{
  "request_id": "<idempotency key>",
  "driver_id": "driver-default",
  "instance_id": "<instance>",
  "lease_id": "<opaque>",
  "driver_epoch": 3,
  "command": "run.get",
  "arguments": {}
}
```

Observer 只允许以下命令前缀，不执行任意方法名：

- `run.open`、`run.get`、`run.patch`、`run.commit`、`run.start`、`run.close`、`run.cancel`；
- `run.readiness`、`run.recovery.list`、`run.recovery.mark`；
- `message.append`、`agent_signal.record`；
- `thread.bind`、`thread.get`、`turn.state`；
- `capability.list`、`capability.get`、`capability.health`；
- `node.accept`、`node.dispatch`、`node.result`、`node.fail`。

每个命令由显式 handler 映射到现有 `ObserverRepository` 方法，并在 handler 前检查 lease/epoch、Workspace scope 和 request idempotency。大对象只传 `ResourceRef`，内容通过 Driver/Slave 各自配置的 ContentStore 读取。

### 4.3 Driver→Slave 执行面

Driver 直接调用注册得到的 Slave endpoint：

```text
POST /worker/v1/provision
POST /worker/v1/dispatch
```

请求必须携带 `driver_id`、`driver_epoch`、`workspace_id`、`attempt_id`、`execution_id` 和 `execution_epoch`。Slave 校验内部 token、目标 Workspace、attempt 和 epoch；结果先返回 Driver，再由 Driver 通过 Observer 控制协议提交最终状态。

能力包 promotion 由 Observer 完成语义发布，provision 由 active Driver 直连目标 Slave 完成，health report 再回写 Observer。

## 5. Codex thread 持久化与恢复

### 5.1 Thread 表

Observer 新增 `driver_threads` 表：

```text
workspace_id         text
conversation_ref     text
thread_id            text
model                text
workspace_root       text
last_turn_id         text nullable
turn_state           text              # idle | starting | in_progress | completed | interrupted | recovery_pending
active_request_id    text nullable
driver_epoch         bigint
updated_at           timestamptz
```

主键为 `(workspace_id, conversation_ref)`，`thread_id` 在 Workspace 内唯一。

### 5.2 首次 turn

1. Driver 查询 `thread.get`；不存在时调用 Codex `thread/start`。
2. Driver 使用 `thread.bind` 将 `conversation_ref → thread_id` 写入 Observer。
3. Driver 在 `turn.state=starting` 下记录 `request_id`。
4. Driver 调用 `turn/start`，收到 turn id 后写入 `in_progress`。
5. 每个终态写入 `completed`、`interrupted` 或 `failed`，并追加对应事件。

`request_id` 在 Observer 持久化 receipt 中具有幂等语义。网关重试或 Driver
重试同一请求时，不得再次向 Codex 提交相同用户消息；同一个 `request_id`
产生的重复请求称为 delivery attempt，不视为新的 conversation turn。

### 5.3 Driver 重启

新 Driver 注册后的 thread resume 流程保持不变；消息 receipt 的 claim、重投和
旧 epoch fencing 详见可靠性修订 spec。

新 Driver 注册后：

1. 读取所有 thread bindings 和 `recovery_pending` turn；
2. 启动本地 Codex app-server；
3. 对每个 `thread_id` 调用 `thread/resume`，传入相同 model、cwd、runtime workspace roots 和 dynamic tools；
4. 调用 `thread/read` 检查最近 turn；
5. 将恢复结果写回 Observer，并继续接受同一 `conversation_ref` 的新请求。

Codex `CODEX_HOME` 使用 Driver named volume 保存本地 rollout/cache 状态，但 `thread_id` 的权威绑定仍在 Observer。该 volume 是本地 app-server 恢复 thread 的必要状态；若 volume 丢失且 app-server 无法根据绑定恢复，记录结构化 `codex_thread_unavailable`，不创建新 thread 冒充恢复。

正常重启可以等待当前 turn 完成后再释放 lease。硬崩溃无法保证 token 级续生成；新 Driver 保持同一 thread，检查未完成 turn，将其标记为 `interrupted/retryable`，后续用户消息继续进入原 thread。

## 6. Driver 镜像、secret 与 Compose

### 6.1 镜像

新增独立 `Dockerfile.driver`：

- Python 项目依赖；
- Docker CLI；
- Node.js/npm；
- 通过 `CODEX_VERSION` build arg 固定安装 `@openai/codex`；
- Driver FastAPI 入口和启动脚本。

Observer 镜像删除 Docker CLI，不安装 Codex CLI。

### 6.2 Secrets

Compose 定义内部认证 secret；Codex 上游默认使用宿主机代理地址，不要求在 Compose 中填写 Codex API key：

```yaml
secrets:
  internal_api_secret:
    file: ${LOOM_INTERNAL_API_SECRET_FILE:-../secrets/internal_api_secret}
```

Driver 只读挂载内部认证 secret：

```text
/run/secrets/internal_api_secret
```

启动脚本从 secret 文件读取值并生成 Driver 专用 `CODEX_HOME/config.toml`。模型 provider、`deepseek-v4-flash` 和 `LOOM_CODEX_BASE_URL` 使用环境变量配置，默认 base URL 为 `http://host.docker.internal:8787`。仅当上游明确要求认证时，才通过可选的 `LOOM_CODEX_API_KEY_FILE` 提供 Codex key；不配置 key 时不会生成 `env_key` 或导出对应环境变量。任何 secret 值都不写入日志、镜像、Observer API 或 orchestration sandbox。

### 6.3 Driver 服务

```yaml
driver:
  build:
    context: .
    dockerfile: Dockerfile.driver
    args:
      CODEX_VERSION: ${CODEX_VERSION}
  command: ["uvicorn", "loom_v2.driver.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8090"]
  environment:
    LOOM_SERVICE_NAME: driver
    LOOM_OBSERVER_URL: http://observer:8080
    LOOM_WORKSPACE_ROOT: /workspace
    LOOM_CODEX_MODEL: deepseek-v4-flash
    LOOM_CODEX_BASE_URL: ${LOOM_CODEX_BASE_URL}
    CODEX_HOME: /var/lib/loom/codex
  secrets: [internal_api_secret]
  volumes:
    - driver-codex-state:/var/lib/loom/codex
    - ${LOOM_WORKSPACE_HOST_PATH:-.}:/workspace
    - /var/run/docker.sock:/var/run/docker.sock
```

Driver 使用 Compose 网络访问 Observer、Slave 和 MinIO；orchestration sandbox 继续使用 `--network none`，不继承 Driver 的 secret、workspace 或 Docker socket。

### 6.4 服务删除与调整

- 删除 `driver-db` 服务及 `driver-db-data` volume；
- 删除 Observer 的 `LOOM_CODING_AGENT_*`、`LOOM_CODEX_MODEL` 和 Slave URL 配置；
- 删除 Observer→Slave/Driver 的硬 `depends_on`；
- Observer 和 Driver 只保留各自必要的数据库、MinIO 和内部 endpoint 配置；
- `scripts/dev-up.sh` 改为校验 secret 文件并启动完整 Compose 栈，不在宿主机创建 Observer/Driver 进程。

## 7. 故障与安全语义

- Observer 重启：从 PostgreSQL 恢复 agent leases、thread bindings、Run 事件和动态节点投影；新 Driver 重新注册并 resume thread。
- Driver 重启：旧 epoch 自动失效，新 epoch 接管；已完成节点不重复执行，未完成 orchestration 按事件重放。
- Slave 重启：Driver 发现 Worker HTTP 失败后通过 Observer 记录 target unavailable，并按 recovery policy 重指派；旧 attempt 被 fencing。
- Observer 不可用：Driver 不写本地伪权威状态，控制命令失败并按请求幂等键重试；Worker dispatch 不在没有 Observer accepted attempt 时启动。
- Driver→Observer 和 Driver→Slave 使用内部 secret；公网只暴露 Observer。
- Docker socket 只挂载 Driver；Driver 容器可创建 sandbox，因此本地部署必须视为受信任控制服务。
- 不允许把 Codex secret、`CODEX_HOME` 内容或用户 workspace 内容写入事件和 provenance。

## 8. 验证与验收

### 8.1 单元与协议测试

- Agent register/heartbeat/release、epoch takeover 和 stale fencing；
- Observer 在没有 Driver/Slave 时可启动；
- 网关转发、Driver unavailable、request idempotency；
- `thread/start` 首次绑定和 `thread/resume` 恢复参数；
- turn 状态转换及硬崩溃后的 retryable 语义；
- Driver direct WorkerSession 的 provision/dispatch/auth/fencing；
- Observer command allowlist 拒绝未知命令；
- secret 值不出现在日志、响应和 sandbox 环境。

### 8.2 集成与 E2E

- Compose 启动顺序不要求 Observer 等待 Slave/Driver；
- 两个 Slave 注册后 Driver 获取 capability 并直连执行；
- Driver 容器重启后同一 `conversation_ref` 使用相同 `thread_id`；
- Driver 重启期间消息重试不产生重复 Codex user turn；
- 动态 orchestration 的 NodeIntent、provision、dispatch、结果回写和 replay 全部通过拆分后的边界运行；
- Codex CLI 不存在、模型 provider 不可达或 secret 缺失时返回结构化错误。

## 9. 实施顺序

1. 增加 `runtime_agents`、`driver_threads`、lease/epoch 和迁移；
2. 实现 Observer agent registry、内部 command handler 和 Driver gateway client；
3. 实现 Driver FastAPI 入口与 `ObserverControlClient`；
4. 将 Driver MCP、DriverService、WorkerSession 和 orchestration runtime 移入 Driver 进程；
5. 将 Slave 注册、capability discovery 和 provision 路径改为 Driver 直连；
6. 扩展 Codex provider 的 thread bind/resume、turn metadata 和 request idempotency；
7. 更新 Dockerfile、Compose secrets、Driver named volume 和本地启动脚本；
8. 删除 Observer 内嵌 Driver/Worker/Slave 路径及 `driver-db`；
9. 执行单元、协议、集成、E2E、Compose config 和安全验收测试；
10. 更新 README 与运行手册。

## 10. 范围外

- 多 Workspace 或同一 Workspace 多活跃 Driver；
- Driver 集群共识、跨节点自动扩缩容和外部服务发现；
- Codex 硬崩溃时的 token 级续生成；
- Observer 代理 Driver→Slave 的执行数据面；
- 将 Docker socket 暴露给 Observer、Slave 或 orchestration program；
- 允许 Driver/程序绕过 Observer 创建 Node、Attempt 或修改闭包语义。
