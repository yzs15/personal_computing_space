# Loom v2 重构审计结论与收敛计划

> 审计输入：`/tmp/loom_v2-audit-2026-09-15-122257.md`
> 审计日期：2026-09-15
> 能力包基线：`06a46f1 feat: complete capability package phase 3 and compose tests`
> 文档性质：对审计意见的取舍和实施顺序，不是立即执行的代码变更清单。
> 本轮复核：2026-09-16，按“官方库优先”重新检查现有未提交重构和能力包 Phase 3 基线。

## 1. 结论

审计报告发现的主要问题基本真实，但报告把“重复确实存在”和“应立即替换成某个库”混在了一起。本次复核仍以减少行为分叉、明确 composition root 和迁移所有权为目标；同时按照仓库设计规范，把已经有成熟官方生态方案的边界列为优先重构项，不为没有明确收益的部分引入依赖。

最终取舍如下：

| 类别 | 判定 | 处理方式 |
| --- | --- | --- |
| schema 初始化、手写 DDL、SQL migration 并存 | 靠谱，且官方方案明确 | 迁移到 SQLAlchemy 官方生态的 Alembic；保留 role-local 数据库边界和现有 revision 行为，删除自定义 migration runner |
| 纯 helper、HTTP、内部鉴权、配置读取重复 | 靠谱 | 优先收敛，复用已有 `httpx`、FastAPI dependency、Pydantic Settings |
| FastAPI `on_event`、手动认证、raw dict endpoint | 靠谱，属于当前代码中的直接维护成本 | 使用 FastAPI `lifespan`、`Depends`/`APIRouter` 和已有 Pydantic v2 request/response model |
| package/ref 校验、projection、activation、进程生命周期重复 | 部分靠谱 | 只抽最小纯函数/窄接口，保留 I/O、角色差异和安全边界 |
| Driver local/remote 大一统处理器 | 问题靠谱，方案过宽 | 先做 local/remote 行为对照；只保留有真实调用方的窄 planning protocol，再按行为测试逐步合并 |
| MCP、Docker 等立即换库 | 不采纳直接替换 | 先冻结协议/安全 golden tests，再对官方 SDK 做隔离 PoC；不能因为“官方”而跳过行为等价验证 |
| boto3、jsonschema、rfc8785、asyncio/tomllib/argparse | 不需要替换 | 当前已经是合适的官方 SDK 或标准库；只消除外围重复封装 |
| 所有字符串改 Enum、所有事件改 discriminated union、所有进程统一一个 runner | 过度设计 | 仅集中内部常量或按高价值事件/流程增量迁移 |

本计划默认允许破坏性重构，不额外维护旧调用方兼容层；如果某个部署接口必须兼容，应在对应任务中单独写出理由和测试。

## 1.1 按设计规范复核后的调整

本次复核依据 `docs/superpowers/spec-design-guidance.md`，对原审计建议做了以下收敛：

- Alembic 需要从“候选实现”升级为数据库迁移的首选官方方案；MCP SDK、Docker SDK 仍需通过 PoC 后再决定，不把未验证的库写成必选依赖。
- 不建立覆盖所有角色、协议和进程类型的“大一统”基类；每个新 protocol 都必须有现实调用方和行为测试。
- 明确删除被新能力包模型替代的 `function_body`、服务专用 endpoint 字段、旁路 digest、`create_all`/重复 DDL 等旧生产路径；不保留无理由的兼容层。
- 能力包的可插拔 runtime 是已经落地的长期架构边界，重构只做实现收敛，不再设计第二套 plugin/handler 发现机制。
- “不采纳”部分只保留与本次重复实现、协议稳定性和安全边界直接相关的判断；低收益库替换不进入前五阶段实施。

## 1.2 最新能力包提交后的重构基线

截至 `06a46f1`，能力包 Phase 1/2/3 的主要契约已经进入代码，重构计划不能再按旧的函数包/服务包模型描述目标：

- `CapabilityPackageVersion` 使用通用 `capability_exports` + `body` envelope；`(package_type, execution.kind, execution.version)` 是 contract/plugin 选择键。
- `PackageContractRegistry` 使用受信任 JSON Schema 校验 manifest；`container-http-v1` 通过 `RuntimePluginHost` 进程外运行，普通 package 不要求修改 Driver/Observer/Slave Core。
- `CapabilityPackageVersion` 在构造时通过现有 JCS/digest 实现计算 `package_digest`；package identity 只包含规范化 manifest，不包含坐标、provenance、activation 或运行时位置。
- `put_typed_content`、`digest_json` 和现有 ContentStore 完整性检查已经是共享入口；后续 helper 只能复用或补齐其边界，不能再复制一套 digest/上传实现。
- `NodeIntent`/`DynamicNode` 已要求完整 `capability_descriptor_ref`，并按 package export 精确选择；一个 Node 只表达一个 export 调用。
- activation 乱序隔离使用 `activation_revision`；它不是 package/activation identity，也不替代 Driver epoch 或 Slave lease。后台 health report 携带并校验同一 revision。
- `slave_replica.digest`、`message_receipts.payload_digest` 和独立 result digest 已被移除；后续重构不得重新增加这些旁路字段。

因此，能力包相关重构只处理 registry 显式注入、同步 helper 去重、projection/调用边界和测试整理，不重做 package envelope、plugin protocol、descriptor 选择或 activation revision 设计。

## 2. 逐项审核结果

### 2.1 直接采纳

#### A. 数据库迁移和索引必须单轨

此前 Observer、Slave 同时存在 `Base.metadata.create_all`、手写 `ALTER TABLE`/`CREATE INDEX` 和 `migrations/*.sql`，会造成新实例和升级实例走不同路径，且模型、索引和 migration 容易漂移。

采纳内容：

- 迁移到 SQLAlchemy 官方生态的 Alembic，作为唯一 schema evolution owner；`loom_v2.db.migrations` 只保留为注入应用连接、role 和并发锁的薄 adapter，不再拥有 migration 语义。
- 保留 Observer/Slave role-local 数据库边界和最终 schema 语义。为两个 role 提供明确的 Alembic migration context/version table；不把两个数据库强行合并成一个 schema。
- 首发前确认没有需要保留的外部数据库后，删除旧 runner/legacy bridge，直接以最终 schema 建立不可变的 release baseline；正式发布后的 schema 变更只新增 Alembic revision。
- PostgreSQL 生产启动只执行按 role 的 Alembic upgrade；删除自定义 SQL 分号解析、运行时 `create_all`、运行时散落 `ALTER TABLE` 和重复 index DDL。revision 内仍可使用 `op.execute` 保留现有 SQL 语义，直到各 revision 有足够覆盖再改为 Alembic operations。
- migration 执行需要显式的 PostgreSQL 并发保护（例如 migration transaction 内的 advisory lock 或等价的单实例启动约束）；Alembic 本身不应被误解为自动解决并发升级。
- 模型中的 index 作为 ORM 元数据保留，但由 Alembic revision 负责创建和变更。

SQLite 仅作为 hermetic test backend，显式保留 `metadata.create_all` 测试路径，不作为生产升级机制。`scripts/migrate.sh` 应调用同一 Alembic entrypoint，而不是通过 `psql` 重放单个 migration。

#### B. 纯工具函数收敛

以下重复是低风险、可验证的纯逻辑重复，应集中到 contracts/capability 或 refs utility：

- `_select_slave_agents`
- `_operation_name`
- `_package_body_ref`
- `_input_binding_for`
- content ref/digest 格式判断和 canonical ref 构造

共享 helper 不负责打开 ContentStore、不负责网络访问，也不负责改变状态。迁移完成后删除旧副本，并用参数化测试覆盖 local/remote/Slave 三个调用方。

#### C. 内部 HTTP client 和错误映射收敛

Observer gateway、Driver control/worker、Slave app 都重复创建 `httpx.AsyncClient`、拼接 internal token、设置 timeout、检查状态码和解包错误。这个重复会导致 timeout 和错误 envelope 不一致。

采纳内容：

- 增加窄的 `InternalHttpClient`（名称可调整），在组件生命周期内复用一个 `httpx.AsyncClient`。
- 统一 base URL、认证 header、timeout、transport 注入和非 2xx 错误映射。
- 保留 ASGI transport 注入点，不能为了抽象破坏现有测试。
- 先不引入 tenacity；只有在确定存在需要统一重试的调用族后，才为该调用族增加有限退避策略。

#### D. 内部鉴权改为共享 FastAPI dependency

Driver、Slave、Observer 对 `X-Loom-Internal-Token`/Bearer fallback 有重复校验。应提供一个共享 dependency，在 router 层挂载，使用 `hmac.compare_digest`，并保留现有 401 错误语义。认证依赖只负责认证，不把业务授权、节点选择或状态检查塞进去。

#### E. 运行时配置统一通过 Settings 注入

已有 `pydantic-settings` 的 `Settings`，但 service、worker、Codex、control client、repository、package registry 和 Slave app 仍各自调用 `os.getenv`。这会产生默认值分叉，也使测试难以控制配置。

采纳内容：

- composition root 创建一次 `Settings`，显式传给 app/service/client/registry。
- 删除业务模块中的重复环境变量读取；保留 bootstrap 读取 secret file 的必要边界。
- package contract registry 改为显式接收 contract directory，不在 import 时读取环境变量。
- 配置字段校验留在 Settings/Pydantic model，不增加新的配置框架。

#### F. Deployment TOML 使用标准读取 + Pydantic 校验

`tomllib` 负责语法读取，Pydantic model 负责字段、类型和结构验证，足以替代 deployment config 中的手写检查。无需引入 Dynaconf/OmegaConf。错误信息和安全字段（尤其 secret）须保持不泄漏。

#### G. Compose 测试的一次性初始化生命周期

审计指出 one-shot initializer 与 `--abort-on-container-exit` 竞争的问题；该问题已在前一提交中修复。本计划只保留原则：测试覆盖层必须等待初始化完成并满足健康条件，再启动依赖服务；不得把 initializer 的正常退出误判为整个测试栈退出。

#### G.1. Driver/Slave endpoint 的重复校验

Driver 的 provision/deprovision endpoint，以及 Slave 的 epoch、workspace、target 校验，存在重复的请求解析和错误包装。问题判断成立，但不需要新的框架：新 endpoint 使用 Pydantic request model，重复的认证/上下文校验使用 dependency，业务动作仍留在对应 service。只有在两个 endpoint 的字段和副作用完全一致时才共享 helper，不能为了消除几行重复而合并不同的生命周期语义。

#### G.2. FastAPI 生命周期和依赖注入应直接采用官方用法

当前 Observer、Driver、Slave 仍使用已被 FastAPI 标记为 deprecated 的 `@app.on_event("startup"/"shutdown")`，并在 endpoint 函数体内手动执行 `require_internal(request)`。这不是协议差异，而是框架能力没有贯彻到底。

采纳内容：

- 用 `@asynccontextmanager` lifespan 统一初始化和关闭数据库、共享 `httpx.AsyncClient`、dispatcher、runtime plugin host、agent heartbeat task 和 provider；关闭顺序保持现有资源依赖关系。
- 用 `APIRouter` 将内部路由挂载 `Depends(InternalAuth(...))`，公共 health、静态资源和用户 API 不被误加内部认证。endpoint 不再携带只为鉴权服务的 `Request` 参数。
- 依赖只做 token 认证；workspace、lease、driver epoch、target 和 capability admission 仍由 service/业务校验负责。
- lifespan 迁移必须保留 TestClient/ASGI transport 的启动、取消和关闭测试，不把后台 task 放到 import 副作用中。

这是优先级高于新建通用生命周期框架的官方库重构；不引入额外 DI 容器。

#### G.3. API 边界优先使用现有 Pydantic v2 model

内部 endpoint 仍有大量 `payload: dict[str, Any]`，然后在函数体中手写必填字段、类型、workspace 和嵌套对象解析。项目已经依赖 Pydantic v2，且已有 `AgentRegistration`、`DriverCommand`、`CapabilityProvisionCommand`、`CapabilityDeprovisionCommand`、`TaskClosure` 等模型。

采纳顺序：

1. 先覆盖 Driver/Slave 的 provision、deprovision、dispatch，以及 Observer 的 agent register/heartbeat/release、capability-health 和 driver-command envelope。
2. 再覆盖稳定的 public run/message/content request；任意用户输入的 `body`、closure payload 和 package body 继续使用明确的 `dict[str, Any]` 字段，不为动态数据制造伪静态模型。
3. 为稳定 response 增加 `response_model`，错误 envelope 保持现有 HTTP status 和 domain error code。

request model 只负责结构和基础约束；数据库读写、digest 完整性、lease fencing 和运行时插件 admission 不能塞进 Pydantic validator。

### 2.2 部分采纳，限制抽象范围

#### H. Driver local/remote 执行链路

本地 Observer repository 与远端 Control API 路径确实重复 turn、event、attempt、provision、result 流程，但一次性引入大而全的 `TurnEventProcessor`/`ExecutionDispatcher` 容易把传输差异和业务状态混在一起。

此前设想的 `RunRepository` protocol 没有形成生产调用方，因此不保留该抽象。当前的 `ObserverRepository` 是本地实现，`RemoteObserverRepository` 是把相应操作翻译成 Observer RPC 的远端实现；两者继续通过行为对照测试固定共同语义。Driver planning 工具使用有真实调用方的窄 `PlanRepository` protocol，SQL session、HTTP client、Control API 命令名和 ContentStore 细节不进入该 protocol。

分阶段处理：

1. 先写 local/remote 行为 golden tests，固定状态、事件和错误 envelope。
2. 仅为有真实调用方的 Driver 工具定义最小 protocol；不要要求两个 repository 实现所有私有 helper。
3. 抽共享的纯流程（snapshot 解析、attempt/provision/result 规范化），将 HTTP/本地 I/O 留在 adapter。
4. 只有重复仍然稳定且测试证明等价时，才抽窄的 execution dispatcher。

#### I. `execute_driver_command` 分发器

超长 `elif` 分发器确实降低可读性和测试隔离性。2026-09-17 收敛决定：先共享命令白名单、抽取资源 scope admission，删除 handler 内重复的 package ref 解析；保留现有业务分支。逐命令 registry 暂缓，避免仅为转发已有 repository 方法新增一批 handler。幂等性、lease fencing 和审计事件仍保留在原执行边界。

#### J. Capability package 校验

Observer、Slave、Driver 存在相近的 package/export/IO/schema 校验。最新实现已经把 package contract 收敛到 `PackageContractRegistry` 和通用 `capability_exports`；这里不再引入 `FunctionCapabilityPackageBody`、`ServiceCapabilityPackageBody` 或按 `package_type` 增长的类型分支。可共享同步校验、descriptor/ref helper 和错误码映射；ContentStore 读取、digest 完整性验证和执行时 admission 仍由拥有该 I/O 的层负责。不要把异步存储访问塞进 Pydantic validator，也不要把所有运行时环境检查做成模型校验。

#### K. Run projection 序列化

相似的 run 字典投影确实存在，但 Observer public API、Driver remote read model、MCP response 的字段边界并不相同。增加一个明确的 projection builder（例如 `RunProjection`）用于共享公共字段；每个边界保留自己的 envelope 和敏感字段过滤，不追求一个“万能字典”。

#### L. Capability promote/deactivate

target 选择、worker 调用、health report 和错误映射可抽成窄的 activation command executor；Observer 的 desired/actual 状态变更、`activation_revision` fencing 和持久化仍由 Observer service 负责。revision 只隔离同一 activation 的乱序 lifecycle command/report，不替代 Driver epoch、Slave lease 或 package digest。不得把 Observer 状态机和 Slave 执行混成一个 service。

#### M. 进程生命周期 helper

各处的 timeout、graceful terminate、forced kill、wait 和 stdout/stderr 收集有真实重复。可以抽一个只覆盖这些公共动作的 `AsyncProcessRunner` 或函数组；

- capability subprocess、runtime plugin、Codex app-server、Docker orchestration 仍使用各自 adapter；
- `RuntimePluginHost` 已经拥有插件崩溃重启和 reconcile 协调逻辑；后续 helper 不能复制这套策略，只能在确认行为等价后复用底层 terminate/wait 动作；
- Docker sandbox 参数、process group 和输出上限等安全约束必须显式传入并继续测试；
- 不把所有进程类型包装成可配置的通用工作流引擎。

#### N. ResourceRef/digest helper

content ref 的解析、构造和 SHA-256 格式检查重复，但 digest 不是可以删除的概念：它仍用于 content identity、package realization 和 fencing。最新实现中，普通 content ref 使用 `content://sha256/<digest>` + `content_digest`；内容承载的 descriptor ref 还可能同时保留 content object digest 与 `descriptor_digest` 的 domain-separated `version_or_digest`，两者不能被 helper 错误合并。可在 `ResourceRef`/refs utility 提供同步 helper（`is_content_ref`、`require_sha256`、canonical content ref 构造），而实际 bytes 读取和 digest 对比继续由 ContentStore/执行层完成。不得让 `ResourceRef` 隐式访问存储。

#### O. Agent registration/heartbeat

Driver 和 Slave 都有注册、重试、heartbeat、release 生命周期，重复判断成立。可定义最小 `AgentLifecycleManager`，由统一 control client 提供传输；Driver/Slave 的角色字段、能力快照和 release 语义仍由调用方提供。使用 FastAPI lifespan/`AsyncExitStack` 管理任务，禁止在 import 时启动后台循环。

#### P. Observer embedded mode

生产 Observer 不应因为 legacy embedded 模式而 import/构造 SlaveService、Worker 或第二个 MCP composition root。先盘点测试对 embedded 模式的依赖，把它移到 test/dev composition factory；生产 app 只接收已组装的 repository/control client。迁移完成后删除生产路径中的 embedded 分支，而不是永久保留双模式。

#### Q. Event、状态和 API 模型

raw event dict、execution kind/state 字符串散落是维护问题，但一次性把全部事件替换成 discriminated union 或把所有外部字符串改成 Enum 风险过高。

- 先统一事件构造入口和序列化，再覆盖动态节点、activation、run lifecycle 等高价值事件。
- 仅集中内部稳定常量（execution kind、activation/run state、terminal state）；外部 JSON 协议继续使用既有字符串。
- 新增/修改 endpoint 优先使用 Pydantic request/response model；不为旧的稳定 response 强行增加宽泛模型。

#### R. Capability package 的 Pydantic 与 JSON Schema

两套模型当前责任不同：Pydantic 用于运行时/API，JSON Schema 用于外部 package contract。不能简单删除一套。后续可从 Pydantic 生成不涉及自定义关键字的基础 schema，并保留 package contract 对自定义 policy 的显式声明；registry 改为显式注入目录。

#### S. Deployment wrapper 与 healthcheck

多个 `scripts/deploy_*.py` 是薄 wrapper，deployment healthcheck 还使用 `urllib`。这属于低风险整理，但不是 capability/runtime 的阻塞问题。前五阶段不改变部署协议；稳定后可提供一个统一 CLI 入口，并删除被替代的旧 wrapper。只有部署方明确要求保留旧命令时，才为其保留薄 shim，并为该兼容要求单独写测试。healthcheck 需要共享 timeout/error mapping 时再改用已有 `httpx`，不得借此重写 SSH/tar 传输或引入新的部署框架。

### 2.3 本轮不采纳的建议

下列建议不是“永远不能做”，而是当前收益不足或会扩大安全/协议变更面；先不写入实施任务：

| 审计建议 | 不采纳理由 | 重新评估条件 |
| --- | --- | --- |
| 立即用官方 MCP SDK/FastMCP 替换手写 MCP/JSON-RPC | Loom 有 conversation scope、internal auth、dynamic tools 和现有错误 envelope；SDK 迁移可能改变 `tools/list`/`tools/call` 兼容性 | 先冻结 golden protocol tests，再做隔离 PoC 和兼容性评估 |
| 用 Docker SDK/aiodocker 替代 Docker CLI | 当前 CLI 参数直接表达 sandbox、网络、内存、PID 和 tmpfs 安全边界；SDK 替换会扩大验证范围 | SDK 能逐项表达并测试所有安全参数，且解决明确的可观测性/生命周期痛点 |
| 用 aioboto3/aiobotocore 替代 boto3 + `asyncio.to_thread` | 当前实现可用，异步库收益不足且增加依赖和行为变化 | 线程模型成为已测量的瓶颈，且异步客户端能保持错误/重试语义 |
| 用 Fabric/asyncssh 替代 SSH/tar subprocess | 与 capability/runtime 重构无关，现有实现简单可控 | 部署协议或并发需求实际超过 subprocess 方案 |
| 用 Typer/click 替代 argparse、用 sse-starlette 替代简单 SSE | 低收益样板替换，当前协议已有测试 | CLI/SSE 需求出现稳定的子命令、重连、heartbeat 或流控需求 |
| 用 transitions/python-statemachine 替代手写状态机 | 当前状态数量有限，手写转移表更容易核对协议 | 状态数量和并发转移显著增加，且有模型能降低错误而非增加间接层 |
| 所有工具字符串、target、state 都改成 Enum | 容易改变 JSON contract，并不能消除跨边界校验 | 只在内部模块边界证明能减少错误时局部采用 |
| 直接合并 Pydantic model 与 JSON Schema | 两者职责不同，合并会丢失 package contract policy | 先验证生成 schema 能完整表达现有 contract，再逐项收敛 |
| 为所有重试立即引入 tenacity | 不是当前主要重复源，可能掩盖不同错误的重试语义 | 存在至少一组共享且已定义幂等/退避策略的调用 |

### 2.4 官方库优先复核清单

下面的判断以当前 `pyproject.toml` 和代码实际边界为准。官方库优先并不等于把所有自研代码替换掉；领域协议、沙箱和 digest 语义仍需要本项目自己的适配层。

| 边界 | 当前实现 | 本轮结论 |
| --- | --- | --- |
| SQLAlchemy migration | `loom_v2/db/migrations.py` 曾是自定义 SQL runner；现在只保留连接/role/legacy bridge adapter | **直接使用 Alembic**。这是唯一应新增的高优先级官方依赖；revision bridge 已落地，旧 SQL 文件仅作兼容资料 |
| FastAPI 生命周期/依赖 | `on_event`、endpoint 内手动鉴权 | **直接重构**为 lifespan、`Depends` 和内部 `APIRouter`，不新增框架 |
| Pydantic API/config | 已有 Pydantic v2，但 endpoint 和 `deployment/config.py` 仍有大量手写校验 | **直接重构** request/response 边界和 deployment 基础字段；拓扑、secret 文件和运行时 admission 保留业务校验 |
| HTTP | 已有 `httpx` 和 `InternalHttpClient` | **保留**。共享 client 是合理领域适配层，不再引入 aiohttp/requests；按 API 差异保留错误映射 |
| S3 | 官方 AWS `boto3` + `asyncio.to_thread` | **保留**。`aioboto3` 不是 AWS 官方 SDK，除非线程瓶颈有测量证据，不替换 |
| JSON Schema/JCS | `jsonschema`、`rfc8785` | **保留**。能力包 contract 和 digest 是稳定协议，不改为普通 JSON 排序或自定义 schema 引擎 |
| 进程/文件/TOML/CLI | `asyncio.subprocess`、`tomllib`、`argparse` | **保留标准库**。可以抽窄 helper，但不引入 workflow engine、Dynaconf 或 Typer 作为“官方库”替代 |
| MCP | 手写最小 JSON-RPC/MCP transport，带 conversation scope 和自定义错误 envelope | **先做官方 MCP SDK adapter PoC**；golden protocol、scope、dynamic tools、错误和 auth 全通过后才考虑替换 dispatch 内核 |
| Docker orchestration | `docker run` subprocess，显式 sandbox/resource/stdin/stdout 限制 | **先做 Docker 官方 Python SDK PoC**；它未在项目依赖中，且同步 API、流式 stdin/stdout、所有安全 flag 必须逐项等价。Compose 仍使用 Docker CLI |
| retry/backoff | 各调用族有不同幂等和 fencing 语义 | **暂不引入 tenacity**；先定义调用族 policy，再决定是否局部采用 |
| 状态机/SSE/SSH | 显式 transition table、简单 `StreamingResponse`、OpenSSH/tar subprocess | **不为换库而换库**；只有状态规模、SSE 重连或部署并发需求实际增长时单独评估 |

因此，本轮真正的“官方库优先”落点只有三项直接重构：Alembic、FastAPI 官方生命周期/依赖、现有 Pydantic v2 的边界模型；MCP 和 Docker 是兼容性 PoC，不是立即替换任务。

## 3. 分阶段实施计划

每个阶段遵循 RED → GREEN → refactor：先加入 characterization/golden tests，再替换一个实现，最后删除旧副本。阶段完成标准是全量测试通过，且新旧路径不再并行作为生产入口。

### Phase 1：纯 helper、配置、HTTP 和鉴权

范围：

- 新建 contracts/ref 与 capability utility，迁移纯函数和 content ref 格式 helper。
- 将业务模块的 `os.getenv` 读取收敛到 `Settings` 注入；bootstrap 的 secret-file 读取保持独立。
- 实现生命周期复用的 `InternalHttpClient`，保留可注入 transport。
- 实现共享 `require_internal_api` dependency，统一 token 比较和 401 envelope。
- deployment TOML 改为 `tomllib` + Pydantic model。
- 将三套 FastAPI app 的 `on_event` 改为官方 lifespan；用内部 `APIRouter` + `Depends` 挂载认证。
- 为 Driver/Slave internal command 和 capability endpoints 增加窄的 Pydantic request/response model。

验收：纯函数参数化测试覆盖 Observer/Driver/Slave；HTTP 错误、timeout、ASGI transport、401、lifespan cancel/close 测试通过；测试可以用一份 Settings 覆盖所有运行时默认值；同一 endpoint 不再同时保留手动鉴权和 dependency 两条生产路径。

当前进度：Phase 1 已完成。三个 FastAPI app 已迁移到 lifespan/Depends；Observer 内部路由已集中到共享 `APIRouter` 鉴权依赖；内部 HTTP client、鉴权、Settings 注入、refs/helper 与 internal request model 已收敛，并保留既有错误码和 transport 测试缝。Slave 三个 command endpoint 已共享 driver identity/epoch 校验，deployment TOML 已改为 `tomllib` + Pydantic 模型，删除了重复的字符串、端口和机器解析 helper。稳定 response 的动态业务字段继续使用明确的 `dict[str, Any]`，不为动态 envelope 制造伪静态模型。

### Phase 2：package 校验、projection、activation 边界

范围：

- 仅共享 package/export/io/ref 同步校验 helper；沿用现有 `PackageContractRegistry`、`capability_exports`、execution 三元组和 runtime plugin protocol，不重做 package model。I/O 和 digest 完整性仍在 ContentStore 所属层。
- 引入窄的 run projection builder，分别适配 Observer、Driver、MCP 边界。
- 提取 activation command executor 的 target/worker/report/error 流程。
- 完成 capability package/public run 边界的 Pydantic request/response model；Phase 1 已覆盖的 internal command model 不再重复建模。

验收：package mismatch、descriptor/ref mismatch、schema 错误和 digest mismatch 的错误码不变；public API 不泄漏内部字段；promote/deactivate 的状态与 `activation_revision` fencing golden tests 通过。

当前进度：Phase 2 已完成。同步校验和 refs helper、显式 package registry、窄的 `observer.activation.execute_activation_targets` 已落地；稳定的 public content/run/message/patch/commit/start/resolve/capability-action request model 已补齐。Observer public API 与 Driver RPC 已复用同一 `run_record_payload`，只用一个 `include_snapshots` 开关表达边界差异；MCP 的决策提示视图继续独立，不为动态 JSON-RPC envelope 制造伪静态模型。package mismatch、descriptor/ref mismatch、schema/digest 错误，以及 activation revision 的乱序 fencing 已由 API、Slave 和 runtime plugin 测试覆盖。生产 Driver gateway 的远程转发仍保留在 gateway 边界，embedded helper 只负责本地 worker/Slave 执行。

### Phase 3：数据库 schema 单轨（Alembic 迁移，已实现并验证）

范围：

- 已将 Alembic 加入生产依赖，建立 Observer/Slave role-local migration context 和 version table。
- 已将最终 schema 收敛为单一 `001_release_baseline` Alembic revision；不再保留内部测试时期的 legacy SQL 和 bridge。
- 已加入 PostgreSQL advisory lock、空库初始化和重复执行路径；baseline、role 隔离和 Alembic 32 字符 revision 限制有单元测试。`tests/db/test_postgres_migrations.py` 使用独立 PostgreSQL 16 容器，对两个 role 分别验证 DDL 失败时 schema/version table 一起回滚、失败后重试、重复执行，以及三个独立进程竞争同一个 advisory lock 后成功升级。
- `loom_v2.db.migrations` 现在只是注入应用连接、role 和 advisory lock 的 Alembic adapter，不再包含 legacy bridge 或历史 revision 映射；生产 PostgreSQL 仍只走 Alembic，SQLite 测试路径显式保留 `create_all`。
- 由于项目尚未对外部署，旧 `migrations/*.sql` 和历史 Alembic revisions 已删除，并将最终 schema 收敛为 `001_release_baseline`。该 baseline 发布后不可修改，后续版本必须追加新 revision。

验收：空库初始化和重复执行由 Alembic 覆盖，role-local version table 与 baseline 已通过临时 PostgreSQL 16 容器验证；Compose 生产路径由 role app/显式 migration entrypoint 执行 Alembic upgrade，应用不会使用 ORM metadata 隐式升级 PostgreSQL schema；旧 runner 和 legacy bridge 已删除。

### Phase 4：Driver local/remote 收敛

范围：

- 先以行为测试确认 ObserverRepository 与 RemoteObserverRepository 的共同语义；仅保留有调用方的窄 protocol。
- 抽 snapshot/attempt/provision/result 的共享纯流程；HTTP 与本地访问保留 adapter。
- 收敛 `execute_driver_command` 的共享白名单和资源 scope admission；逐命令 handler registry 暂缓，理由见 I。
- 只有重复被 golden tests 证明等价后，才引入窄的 execution dispatcher。

验收：local/remote 对相同输入产生相同状态、事件顺序、审计字段和错误 envelope；不再在 turn loop 中按 transport 分支。

当前进度：保留有真实调用方的窄 `PlanRepository` protocol，供 `DriverTools` 的 patch/commit/start/readiness 操作使用；删除没有生产调用方的 `RunRepository` protocol。`ObserverRepository` 与 `RemoteObserverRepository` 继续保留各自 transport 实现，行为对照测试固定两端语义，但尚未抽取完整 turn-loop dispatcher。执行等待部分已提取为 `_wait_for_execution_or_cancel`，只共享任务等待、权威取消和清理动作，local/remote 的状态读取与错误映射仍留在各自 adapter。

2026-09-17 增量：Driver/Observer 共享 `DRIVER_COMMANDS`；Observer 通过 `_admit_driver_arguments` 统一校验 Run/package workspace 并规范化 package ref，删除后续 handler 的重复解析。补齐 `run.result` 的 workspace admission，普通结果和 orchestration 结果均有跨 workspace 拒绝测试。`tests/driver/test_repository_parity.py` 已覆盖 opened/thinking/failed 生命周期、幂等消息、结果决策、orchestration completion、错误语义和 epoch fencing；Driver/API 回归 166 项通过。完整 turn loop 仍保留 transport adapter 差异，尚未合并成一个 dispatcher，Phase 4 仍处于行为固定和窄接口收敛阶段。

### Phase 5：composition、生命周期和事件模型

范围：

- 将 Observer embedded 组装移到 test/dev factory，清理生产 import 依赖。
- 在 Phase 1 lifespan 基础上，在不抹平角色差异的前提下引入最小 AgentLifecycleManager；它只复用注册/heartbeat/release 的调度，不重复创建 app 生命周期。
- 抽窄的 AsyncProcessRunner；只迁移 capability subprocess、Codex 等已证明相同的公共终止动作，保留 `RuntimePluginHost` 的重启/reconcile 策略和 Docker orchestration adapter。
- 先规范关键事件构造，再按收益引入 discriminated union；集中内部状态常量。

验收：生产 Observer 不构造 SlaveService；注册/heartbeat/release 的取消和重试行为有生命周期测试；进程超时、kill、输出上限和 Docker sandbox 测试通过。

### Phase 6：有证据才进行的独立评估

前五阶段完成后，如果某个库替换仍有明确性能、协议或运维收益，再单独创建短 RFC/PoC。PoC 必须先通过现有协议或安全 golden tests，并给出收益指标和回滚方案；没有明确收益的项目不进入本计划，也不引入依赖。

## 4. 不在本次重构中改变的契约

- RFC 8785/JCS canonicalization 和 SHA-256 content identity 语义不变；只删除重复 helper。
- `CapabilityPackageVersion` 的通用 `capability_exports` envelope、execution 三元组 contract registry、`RuntimePluginHost`/`container-http-v1` plugin protocol 保留现有边界；不恢复旧的 `function_body`、服务专用 `endpoints` 或 package-type 路由分支。
- `container:python_orchestrator/1` 仍是 Driver 保留的特殊执行类型；除此之外新增普通能力包不要求修改 Driver 主体代码。
- `NodeIntent`/`DynamicNode` 的 descriptor ref 必须精确属于 package 的公共 exports；一个 Node 只表达一个 export 调用，不新增旁路 endpoint/operation digest。
- activation 使用 Observer 签发的 `activation_revision`；不重新引入 `session_generation`、`manager_instance` 或 activation digest。
- package digest 仍只来自规范化 manifest；不重新引入 `slave_replica.digest`、`message_receipts.payload_digest` 或独立 result digest。
- Docker sandbox 的网络、资源、PID、tmpfs 和输出上限保持显式配置并继续测试。
- Observer/Driver/Slave 的内部 token 认证和错误 envelope 只做实现收敛，不降低认证要求。
- Compose 的 production/test profile 入口、initializer 完成条件和 smoke/E2E 验收保持有效。

## 5. 完成定义

本计划全部完成并不等于“所有审计建议都换成库”。完成定义是：

1. 每个被采纳的重复点只有一个生产实现，旧副本已删除。
2. 每个新增抽象都有调用方、测试和明确边界；没有为未来假设预留空泛插件层。
3. schema、digest、安全 sandbox、runtime plugin 和外部 JSON 协议的行为由测试固定。
4. 迁移失败时能定位到具体阶段，不依赖隐式 import、副作用初始化或未声明的环境变量。
