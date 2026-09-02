# 真实负载验收设计：材料带隙统计报告（算力服务台场景）

**日期：** 2026-09-02
**状态：** 设计（待评审）
**对应文档：** `docs/superpowers/specs/2026-08-26-distributed-analysis-e2e-design.md`；`docs/superpowers/specs/2026-09-01-run-lifecycle-redesign.md`

## 1. 背景与问题

现有 230 个测试全部运行在确定性 Fake coding-agent profile 上。系统最核心的差异化主张——**coding-agent 是唯一语义细化者**——在真实 Codex 后端下从未被端到端验证过。同时，现有"应用场景"（echo/hash/sort、map-reduce 统计、matmul）都是系统自己的验收夹具，不是真实用户任务。

本设计定义一次**判别性验收**：用真实 Codex 后端 + 一份非玩具的领域数据（材料带隙统计报告），走通自然语言 → 闭包细化 → 能力缺口闭环 → 动态分布式执行 → 掉线改派 → 结果回溯到输入 的完整链路。它把系统从"架构原型"的 claim 推向"真实工具"的 claim：跑通，则核心主张有实证；跑不通，则暴露真实后端路径上的缺口。

本设计不引入新依赖、不新增抽象层，全部复用现有组件与契约。

## 2. 关键结论

- 验收核心是 **G1（真实 agent 细化）**：一次自然语言消息驱动整套 MCP 工具面，不允许在测试里手工调用 `loom_*` 工具。
- 场景采用与现有动态 map-reduce 相同的 `partition → summarize → merge` 模式，但增加两层此前未覆盖的内容：**领域解析能力缺口**（`df_xml_parse` 不预装）与 **运行中掉线改派**。
- 验收断言对行为、统计正确性与 provenance 可走通负责，不对 agent 生成的程序文本负责（真实 agent 存在非确定性）。

## 3. 目标

1. 证明真实 Codex 后端能从一句自然语言目标自动细化出可提交、可启动、正确的动态编排闭包。
2. 证明能力缺口闭环：缺失的 `df_xml_parse` 由 agent 物化为 `run_bound` 候选包，Run 终态后由用户显式提升为 `workspace_reusable` 并激活。
3. 证明动态分布式执行正确（报告统计 == 独立计算的 ground truth）。
4. 证明掉线改派：Slave A 下线后，未完成节点改派到 Slave B，execution 身份不变、epoch 递增、无重复节点、Run 仍完成。
5. 证明 provenance：从最终报告可回溯到输入、包版本、attempt/execution，且事件日志重放可重建同一 DynamicNode 集与输出 digest。

## 4. 非目标

- 不验证多用户/多 Workspace、多 Coordinator 联邦或跨资源提供者互操作（当前范围外）。
- 不验证真实 DFT 计算正确性：输入是结构真实的合成样本，验收对象是平台路径与统计正确性，不是物理/量化计算本身。
- 不新增新依赖、新执行器或新抽象层；`df_xml_parse` 走现有 `subprocess_json_v1` + capability package 模型。

## 5. 场景设计

### 5.1 输入样本

- 500 份合成 DFT 风格输出文件（XML/文本），按材料族（如 `perovskite`/`rutile`/`zincblende`/`wurtzite`/`rocksalt`）分布，确定性随机种子生成，字段含 `family`、`e_gap`、`functional`、`kpoints` 等。
- 每份文件经 `loom_put_content` 上传为独立内容对象；父输入文档为 `{"partitions": [{"files": [ref...]}, ...]}`，按两个"集群"切分（partition 1 → slave-a，partition 2 → slave-b）。
- ground truth 由验收脚本独立计算（不经过 Loom），供 G3 比对。

### 5.2 总体流程

用户向 `POST /api/v1/messages` 发送一句自然语言目标（材料带隙统计报告需求，含预算与 deadline）→ Observer 持久化 receipt 并异步转发 Driver → 真实 Codex turn 运行 → agent 经 MCP 工具面完成 `loom_put_content`（schema/IoContract/程序）、`loom_open_run`、`loom_apply_plan_patch`、`loom_commit_plan`、`loom_start_run` → Driver 执行 `orchestrator_python_v1` 编排程序，运行时 emit `summarize` NodeIntent，Observer 物化 DynamicNode → Slaves 执行 → 最终报告写入 ContentStore。

### 5.3 Agent 细化路径（G1，判别性核心）

- Driver 使用默认真实后端（`CodexAppServerProvider`，模型 `deepseek-v4-flash`），**不注入 Fake provider**。
- 验收脚本只发送一条自然语言消息，随后轮询 `GET /api/v1/conversations/{ref}` 观察 `idle → thinking → executing → completed` 状态迁移。
- 通过条件：agent 自主完成上传、开闭包、物化三个包（`df_xml_parse`/`summarize_bandgap`/`merge_bandgap_report`）与编排包、补 `set_execution_payload`、使 readiness 达到 `ready`、commit、start。证据 = 持久化对话 turn + Run 事件中的工具调用序列。

### 5.4 能力缺口闭环（G2）

- 前置：两个 Slave 的能力快照中**不含** `df_xml_parse`（只含基础 `subprocess_json_v1`/`run_code` 执行器与既有包）；不补该包时，闭包 readiness 必须报 `capability_unavailable`。
- Agent 在细化期间物化 `df_xml_parse` 为 `run_bound` 候选包并被 Run 引用执行。
- Run 终态后，验收脚本以用户身份 `GET /api/v1/capability-packages` 列出候选，`POST /api/v1/capability-packages/{ref}/promote` 提升，随后断言该包进入能力快照、目标 Slave 激活并返回健康/激活证据。
- 对照路径：同一候选走 `.../abandon` 时不得进入能力快照（验证"提升是用户决策、非自动发布"）。

### 5.5 动态编排与能力包

- 编排包使用 `executor_kind="orchestrator_python_v1"`，`allowed_node_package_refs` 仅含 `summarize_bandgap` 与 `merge_bandgap_report`，`max_nodes`/`max_live_nodes` 设上限。
- 编排程序读取父输入，对每个 partition emit summarize 节点，等待结果后 emit merge 节点，返回报告 ref；中间与最终结果均写入 ContentStore（`content://sha256/<digest>`）。
- 非法输入（缺字段/类型错）被 readiness/admission 拒绝，父 Run 不伪造 `completed`（复用现有 admission 语义）。

### 5.6 机器掉线注入与改派（G4）

- 在部分 summarize 节点完成后（通过事件轮询确认），`POST /api/v1/slaves/slave-a/availability {"available": false}`，随后触发 reconcile。
- 断言：剩余/排队节点在 slave-b 产生新 Attempt，`execution_id` 与既有执行一致、epoch 递增，`selected_target` 更新；事件日志记录改派原因；最终无重复节点、无重复统计（幂等由 execution 身份保证）。
- 掉线注入点选在"已完成 ≥1 个 partition、仍有节点未完成"处，确保既验证已完成节点的结果回灌，也验证未完成节点的改派。

### 5.7 结果回溯到输入的证明（G5）

- 从最终报告 `resource_ref` 出发，沿事件日志走：report ← merge 节点 ← summarize 节点 ← parse 相关包 ← partition 文件 ref 链，逐级校验输出 digest。
- 断言事件日志重放（Driver 重启恢复路径）重建出相同的 DynamicNode 集、attempt/execution 序列与输出 digest。
- 断言报告内每个统计数字可关联到具体输入文件与包版本；报告整体统计与 ground truth 一致（G3 的 provenance 化形态）。

## 6. 验收标准（Pass/Fail Gates）

| Gate | 断言 |
|---|---|
| G1 真实 agent 细化 | 仅一条自然语言消息 → 闭包 `ready` → commit → start；无手工 MCP 调用；对话/Run 事件含完整工具调用序列 |
| G2 能力缺口闭环 | 前置 readiness 因缺 `df_xml_parse` 失败；agent 物化候选；用户 promote 后进入快照且激活；abandon 不进入快照 |
| G3 统计正确性 | 报告 per-family/overall/outliers 与独立 ground truth 完全一致 |
| G4 掉线改派 | slave-a 下线后节点改派 slave-b，execution 身份不变、epoch 递增、无重复节点、Run 仍 `completed` |
| G5 结果回溯 | 报告 → 输入全链 provenance 可走通；事件重放重建相同节点集与输出 digest |
| G6 回归 | 确定性 profile 全量测试保持通过；本场景的确定性变体测试纳入且通过 |

任一门失败，验收脚本以非零码退出并在输出中标出失败 Gate 与对应事件片段。

## 7. 测试形态与执行

- **确定性基线变体**：新增 `tests/e2e/test_bandgap_report_deterministic.py`（Fake agent），复用现有动态分析测试模式，但加入 parse/summarize/merge 三层与掉线注入，纳入 CI 常态运行。
- **真实后端验收**：新增 `scripts/accept-bandgap.sh`——复用 `scripts/dev-up.sh` 启动完整 Compose 栈 + 宿主机 Driver（真实 Codex），种入样本语料，发送一条自然语言消息，轮询会话状态，注入掉线，逐 Gate 校验，任一失败即非零退出。
- 两个 profile 共用同一套组件契约与 Observer/Driver/Slave 边界，不新增配置维度。

## 8. 当前基线复用

- 组件：`DriverMCP`、`DriverService`、`WorkerSession`、`DockerOrchestrationExecutor`、`ObserverRepository`、`ContentStore`、`SlaveService`。
- 契约与工具：`io.v1`（IoContract/JSON Schema 子集/NodeInputBinding）、`loom_put_content`、`loom_open_run`、`loom_apply_plan_patch`、`loom_commit_plan`、`loom_start_run`、`materialize_capability_package_candidate`。
- 既有测试模式：`tests/e2e/test_dynamic_distributed_analysis.py`（动态 map-reduce）、`tests/e2e/test_dynamic_orchestration_stress.py`（admission）、`tests/e2e/test_reassignment.py`（改派语义）。
- 接口：`GET /api/v1/capability-packages`、`POST /api/v1/capability-packages/{ref}/promote|abandon`、`POST /api/v1/slaves/{id}/availability`、`GET /api/v1/conversations/{ref}`。

## 9. 风险与演进

- **真实 agent 非确定性**：每次细化产出的程序文本/包结构可能不同。Gate 只断言行为（统计正确、provenance 可走通、包可提升），不断言程序文本；如必要，可在验收脚本中对比两次运行的稳定性。
- **长时运行**：受 24h Codex deadline 约束，验收用小语料（500 文件、2 partition）以分钟级完成；语料规模与 deadline 走既有 `LOOM_*` 环境配置，不新增配置。
- **演进**：本设计是一份可复用的"真实负载验收模板"；后续其他真实负载（合规批处理、跨站点数据）复用同一套 Gate 结构与组件边界，不需要新的执行模型。
