# 分布式数据分析与校验压力测试设计（A + B）

**日期：** 2026-08-26
**修订：** 2026-08-27（对齐源码演进：ContentStore 落地、能力包主线、Driver 显式驱动）

## 1. 背景与目标

当前测试面只覆盖单节点、单操作（`echo`/`hash`/`sort`）的执行路径，细化/校验链路也只有最小断言。本设计引入两套端到端确定性测试，分别压满两条链：

- **B（地基，先实现）**：复杂单节点任务——多约束、多 typed hole、能力缺口用能力包补齐、readiness blockers、binding 锁定、reassignment，并复用内容资源路径（数据从 store 拉、结果写回 store）。目标是压满“细化与决策”链路。
- **A（在 B 之上）**：三节点 DAG 的分布式统计概要——`map-a`/`map-b` 并行跑在两个 Slave 上、`reduce` 归并，跨节点 provenance，内容资源全程引用，且 `summarize`/`merge_summaries` 均以**能力包**实现（缺失能力 → 生成程序包 → 激活 → 执行）。目标是压满“能力缺口 + 分布执行 + 资源”链路。

## 2. 现状基线（截至 2026-08-27 源码）

- **ContentStore 已实现**：`loom_v2/observer/content_store.py`，文件系统后端，内容寻址，`put/get/exists`。
- **能力包主线已起步**：`CapabilityPackageVersion`/激活/`provision`、`run_code` executor、`materialize_capability_package_candidate` patch op 已存在；独立设计见 `docs/superpowers/specs/2026-08-26-capability-gap-resolution-design.md`，实施计划见 `docs/superpowers/plans/2026-08-27-capability-gap-resolution.md`。
- **Driver 已改为显式驱动**：coding-agent 是唯一细化者，显式 `open_run`/`commit_plan`/`start_run`；Driver 不再代 commit、不再自动执行（`loom_v2/driver/service.py`）。
- **Node/DAG 仍未实现**：`TaskClosure` 无 `nodes` 字段、无 `add_node` patch op；readiness 仍按全局 `typed_holes`；`attempts` 仍单条。
- **尚无 `summarize`/`merge_summaries` 算子**（builtin 或能力包均无）。

## 3. 设计

### 3.1 ContentStore（文件层访问格式，对齐已落地接口）

资源命名与身份：

- 内容身份：内容寻址，`version_or_digest = sha256(bytes)`；资源不可变，改名/改内容只产生新版本，不原地覆盖。
- 引用形式：`ResourceRef(resource_id="content://sha256/<digest>", version_or_digest=<digest>, identity_criterion="content_digest")`。
- 闭包与派发信封中只出现 `ResourceRef`，不出现路径、不出现字节；本地路径只存在于 `access_binding`，不得进入公共闭包身份。

接口（provider 无关；当前实现为同步文件后端，后续接 MinIO 时包一层适配）：

- `put(content: bytes, *, media_type, resource_id) -> ResourceRef`：按内容写盘，返回带 digest 的引用。
- `get(ref, expected_digest) -> bytes`：按引用读，读回后校验 digest，不符报 `content_digest_mismatch`。
- `exists(ref) -> bool`：只查不读。

实现：

- 当前：文件系统后端（`ContentStore`，默认 `/tmp/loom-v2-content`）；A/B 测试中两个 Slave 与 Observer 共享同一实例（模拟共享存储）。
- 后续：`S3ContentStore`（MinIO 容器），bucket 按 workspace 隔离、key 用 digest（内容寻址），同一接口；接入时闭包/派发格式零改动。
- `stat`/`resolve` 不进入本版接口，留作资源注册表阶段的扩展点。

### 3.2 Node/DAG 进闭包（待实现）

- `TaskClosure` 增加 `nodes: list[ClosureNode]`。
- `ClosureNode` 字段：`node_id`、`operation_ref`、`inputs: list[node_id]`（即依赖边）、`binding: ComputeBinding | None`（节点自包含的内联绑定，可携带 `capability_package_ref`）、`payload: dict[str, Any]`（静态参数，不含运行时数据）。
- patch op 新增 `add_node`（`kind` 枚举追加）；`inputs` 隐含依赖关系，不单独引入 edge op。
- **与能力包主线对齐**：引用能力包设计“当前版本不引入动态 DAG fan-out；若采用分解，必须在闭包版本中显式表达有限、可校验的子节点和聚合关系”。本设计就是“显式有限子节点 + 聚合”的最小落地。
- **兼容性**：闭包无 `nodes` 时退化为现有隐式单节点路径（`program.operation_ref` + 第一个 `compute_bindings`），现有测试零改动。

### 3.3 按节点派发、provision 与结果资源（待实现）

- Driver 按拓扑序遍历 `nodes`，每个节点独立 `attempt_id`，独立 dispatch。
- 派发信封只带 `ResourceRef`（节点输入 = 依赖节点结果的 `result-<digest>` 引用，或数据源的 `content://` 引用），不携带值。
- **每个绑定到能力包的节点，先经 `WorkerSession.provision` 在目标 Slave 上安装/激活包**（复用现有 provision 流程：程序正文走共享 store 或 `program_bytes_b64` 数据面，Slave 再哈希校验），再 dispatch。
- Slave 执行前先 `get(ref)` 解析输入，再调用纯算子（算子只吃解析后的值）。
- **每个节点结果 `put()` 回 store**，成为 `result-<digest>` 资源；下游节点按 ref 拉取。最终 Run 的 `ResourceRef` = 最后一个节点（reduce）的结果。
- `SlaveService` 构造时注入 `ContentStore`（已实现）。

### 3.4 readiness 按节点（待实现）

- `_evaluate_readiness` 改为按每个 node 检查：`node.binding` 非空、绑定目标 Slave 能力（含能力包激活所需 `run_code`）、Slave 可用、能力包 scope/digest/content/provider-fillable 校验、依赖节点存在。
- blockers 携带 `node_id`，保持结构化。
- 隐式单节点路径沿用现有全局检查逻辑（已含能力包各类 blocker）。

### 3.5 attempts / provenance 按节点（待实现）

- `RunRecord.attempts` 改为按 `node_id` 索引，每个节点一条 attempt，含 `attempt_id`、`target`、`node_id`、`operation`、`result_ref`、`state`。
- 形成完整 provenance 链：`closure_version → execution → 每节点的 attempt → binding → package → target → result`。
- 最终 `ExecutionOutcome` 的 `resource_ref` = reduce 节点结果。

### 3.6 算子：以能力包实现，而非 builtin

`summarize`（map）与 `merge_summaries`（reduce）**不是 builtin 算子**，而是应用能力：

- 输入/输出语义：
  - `summarize`：输入 `{items: [number...]}`，输出局部概要 `{count, sum, min, max, sumsq}`。
  - `merge_summaries`：输入多个局部概要，输出全局 `{count, sum, min, max, sumsq, mean, stddev}`。
- 实现方式：coding-agent 在细化期发现目标 Slave 缺该能力时，生成对应程序正文（`run_code`，`subprocess_json_v1` executor），经 `materialize_capability_package_candidate` 物化为 `CapabilityPackageVersion`（程序正文只进 ContentStore，闭包只存 `program_content_ref` + digest），再 `bind_compute_hole` 绑定到具体 Slave。
- 语义归属：`summarize`/`merge_summaries` 是应用操作与成功语义；`run_code` 只是实现机制，正确性由 operation descriptor、validator 与运行证据保证（沿用能力包设计 §2.2）。

## 4. 场景 A：分布式统计概要（能力包 + DAG）

- 数据：`dataset-part-0`、`dataset-part-1` 两个内容资源（测试 fixture 通过 `put()` 注册）。
- DAG：
  - `map-a`：`summarize`，绑定 `slave-a`（summarize 包激活于 slave-a），输入 `dataset-part-0`；
  - `map-b`：`summarize`，绑定 `slave-b`（summarize 包激活于 slave-b），输入 `dataset-part-1`；
  - `reduce`：`merge_summaries`，绑定 `slave-a`（merge_summaries 包激活于 slave-a），输入 `[map-a, map-b]`。
- Fake provider 确定性事件序列：
  1. `open_run`（contract 带成功标准）；
  2. `query_capabilities`（发现缺 `summarize`/`merge_summaries`）；
  3. `add_typed_hole` ×3（map-a/map-b/reduce 各自 hole）；
  4. `materialize_capability_package_candidate` ×2（summarize 包、merge_summaries 包，程序正文进 ContentStore）；
  5. `add_node` ×3；
  6. `bind_compute_hole` ×3（每个节点绑定到对应包 + 目标 Slave）；
  7. `inspect_plan_readiness`（先 blocked 后 ready）；
  8. `commit_plan` → `start_run`。
- 执行：Driver 对 slave-a/b 各 provision summarize 包（两次激活）、对 slave-a provision merge_summaries 包，再按拓扑序派发三个节点。
- 断言：
  - `map-a`、`map-b` 落到不同 Slave，各有一条独立 attempt 与 provenance；
  - 全局统计正确（count/sum/min/max/mean/stddev，与 fixture 数据核对）；
  - 中间结果（两个局部概要）以 `result-<digest>` 资源存在于 store，可 `get()` 解析；
  - readiness 在全部绑定/物化完成前 blocked，完成后 ready；
  - summarize 包在两个 Slave 上均有 `ready` 激活、merge_summaries 包在 slave-a 上有激活；
  - 最终 `ResourceRef` 正确。

## 5. 场景 B：严格约束的单节点任务（能力包补齐 + 校验压力）

- 任务：**一个显式 node**（与 A 同一套节点机制）对全量数据 `summarize`，绑定 `slave-a`（summarize 包补齐）；数据从 store 拉、结果写回 store。隐式单节点路径仅用于旧测试兼容，不用于 B。
- `ClosureContract` 声明多个约束：CPU 预算 `le 60`、deadline、locality。
- 细化期：
  - 两个 typed hole（`h_compute` + `h_data_io`），先只绑一个 → readiness 报 `typed_hole_unbound`；
  - 经 `query_capabilities` 发现 `summarize` 缺失 → 物化 summarize 能力包并绑定 `h_compute`；
  - `h_data_io` 改绑到有能力的目标（验证 query_capabilities + 改绑决策）；
  - 一次非法放松 `tighten_constraint` 被拒（`constraint_not_monotonic`），且不污染旧 draft 版本。
- 执行期：`allow_reassignment=true`，`slave-a` 下线 → `reconcile` 生成 `slave-b` 新 attempt，`execution_id`、闭包版本、约束保持不变（复用现有 reassignment 语义）。
- 断言：
  - readiness blockers 按序出现且带 `node_id`；
  - 非法 patch 不污染 draft（版本/digest 不变）；
  - binding 锁定后不可静默修改；
  - 能力包物化 → 绑定 → readiness 通过 → provision 激活 → 执行完整闭环；
  - 改派保留 execution 身份与条件；
  - 最终结果 digest 正确。

## 6. 测试形态

- 两套 Fake provider（按 prompt 分支：A 场景发 DAG+包序列，B 场景发校验压力序列），全部事件走 MCP/Driver 工具面，不绕过 Observer。
- 进程内两个 `SlaveService`，共享同一 `ContentStore`，经 `WorkerSession`（ASGI transport）dispatch 与 provision。
- executor 使用 `SubprocessJSONV1Adapter`（`run_code`）执行能力包程序，程序正文为确定性小脚本（输入 JSON 行 → 输出 JSON 行）。
- 新增两个 e2e 测试文件（`tests/e2e/test_distributed_analysis.py`、`tests/e2e/test_validation_stress.py`）。
- 现有测试保持通过（隐式单节点兼容路径）。

## 7. 实施顺序与协调

0. **已完成（基线）**：ContentStore、能力包契约/物化/provision（与 `2026-08-27-capability-gap-resolution.md` 并行推进，先合入其步骤 1–4 作为本设计的依赖）。
1. Node/DAG 进闭包：`ClosureNode`/`TaskClosure.nodes` + `add_node` patch op + readiness 按节点 + attempts 按节点。
2. 按节点派发：拓扑序、每节点独立 attempt、每节点 provision/激活、结果写回 store。
3. `summarize`/`merge_summaries` 能力包 fixture（程序正文 + descriptor + 绑定）。
4. 场景 B 测试。
5. 场景 A 测试。
6. （可选）`S3ContentStore`（MinIO）实现，不阻塞本次。

## 8. 范围外（YAGNI）

- 自动 fan-out、通用调度器、backpressure、自动 retry；
- 真实文件系统对外 API（先以共享 ContentStore + 接口约定承接，后续接 MinIO）；
- 运行时 partition 算子（本设计采用天然分片的数据集，两个源资源直接喂 map）；
- 将 `summarize`/`merge_summaries` 做成 builtin 算子。

