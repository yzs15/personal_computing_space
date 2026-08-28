# 能力 I/O 契约校验设计（Capability I/O Schema Validation）

**日期：** 2026-08-28
**状态：** 设计（待评审）
**对应文档：** `docs/superpowers/specs/2026-08-24-single-user-workspace-v2-design.md`（权威输入：`../computility/单用户单Workspace核心架构设计v2.md`）

## 1. 背景与问题

当前 `ProgramApplication.input_schema` / `output_schema` 只是自由描述字符串（例如 `"scores:number[]"`），没有任何执行点做机器校验。执行载荷 `metadata.execution_payload` 是自由 JSON，能力包程序是自由 Python 正文。系统对“程序期待什么、载荷给的是什么”没有统一契约，只能依赖 coding-agent 的自觉保持一致。

实测暴露的具体问题：coding-agent 偶发把 `set_execution_payload` 写成 `value={"payload": {"scores":[...]}}`（多包一层），而它自己生成的程序读 `d["scores"]`。因为没有任何结构校验点，这个不一致在 `readiness`/`commit` 阶段全部通过，直到执行器收到顶层为 `payload` 的 JSON、程序抛 `KeyError`、Slave 返回 `capability_exec_error` 才暴露，Run 被标记失败。

v2 文档已经为 I/O 契约留好了语义锚点，但本实现尚未落地为可校验结构：

- `ProgramApplication = { operation_ref, semantics_digest, input_schema, output_schema, success_semantics }`（v2 §4.1）。
- `CapabilityDescriptor` 已声明 `input_schema_digest` / `output_schema_digest`（v2 §4.6）。
- `TermSupport.support` 含 `parse` / `validate`，`execution_stages` 含 `commit` / `admission` / `execute` / `terminal`（v2 §4.6）。
- 职责分层第 1 条：“Observer 不需要理解每个领域值的业务含义，但必须能做通用结构校验、版本兼容检查和未知 required 的拒绝。”（v2 §4.6）
- 验收第 5 条：“可检查约束留下 validator/attestation/readiness evidence，平台不伪造 `ready`/`stopped`。”（v2 §1）

## 2. 目标

1. 用机器可读的类型结构（JSON Schema 作为 wire 格式）表达 operation 的输入/输出契约，内容寻址。
2. 在执行前（`readiness` / `commit` / Slave admission）校验 `execution_payload` 对 `input_schema`，把“多包一层、缺字段、类型错误”拦截为结构化 blocker（`payload_schema_mismatch`），而不是运行期 `capability_exec_error`。
3. 执行后校验 executor 输出对 `output_schema` / `success_semantics`，生成 criterion/validator evidence；不匹配不得进入 `completed` outcome（`output_schema_mismatch`）。
4. 能力包必须携带 I/O 契约（`io_contract`），程序与契约对账，形成“程序接口 == 闭包输入契约 == 实际载荷”的单一事实来源。
5. 全程确定性校验，不调用 LLM；Observer 是校验与 evidence 的接受权威，Slave 只按声明做结构校验。

## 3. 非目标

- 不自动生成或改写程序正文；不校验程序语义正确性（“算的是不是平均分”仍需运行证据/测试）。
- 不引入完整 RPC 框架编译管线（protobuf/thrift）；JSON Schema 是 wire 契约，protobuf/thrift 可作为“约定类型”来源在后续导出为 JSON Schema，不在本设计实现。
- 不改 `TypedTerm` / `VocabularyRegistry` 的既有语义；I/O schema 作为 `ProgramApplication` 的独立锚点增量演进。
- 不做跨 Slave 的 schema 协商协议；沿用 `CapabilityDescriptor` 的 input/output schema digest 作为能力侧契约。
- 不在本设计实现动态 DAG fan-out、循环或自动 reduce。

## 4. 设计原则（对应 v2 §2）

- **确定性最小面**：schema 校验是纯函数、无副作用、不调用 LLM；Runtime 只做 CAS、引用、结构校验、READY 与 evidence 校验。
- **Observer 状态权威**：结构校验结果、`payload_schema_mismatch` / `output_schema_mismatch` 以及 criterion evidence 的最终接受只在 Observer。
- **细化权唯一**：I/O schema 由 coding-agent 在细化期提出，Runtime 只做机械校验，不代填、不猜测、不放宽。
- **引用与内容分离**：schema 按内容寻址（digest），闭包/包只存 digest 与 ref；schema 正文进 ContentStore，不散落在事件或日志。
- **平台不伪造 ready/completed**：未通过输入校验不得 ready，未通过输出校验不得 completed。

## 5. I/O 契约表示

新增一等模型 `IoContract`，作为 operation 输入/输出契约的单一事实来源：

```text
IoContract = {
  schema_version: "io.v1",
  input_schema: JsonSchema,        // 执行载荷（stdin JSON）的结构约束
  output_schema: JsonSchema,       // executor 返回 JSON 的结构约束
  success_semantics: { op, field, value, ... }?   // 可机检的成功谓词（沿用 v2 锚点）
  input_schema_digest: Digest,
  output_schema_digest: Digest,
  io_contract_digest: Digest       // sha256(规范化 JSON，exclude digest 自身)
}
```

- **wire 格式**：JSON Schema（draft 2020-12 子集：`type` / `required` / `properties` / `items` / `enum` / `const` / `numericMinimum` / `numericMaximum` / `stringFormat`）。单用户单 Workspace 下此子集足够，且与 JSONB / JSON 载荷天然兼容。
- **内容寻址**：`io_contract_digest` 是权威身份；闭包与能力包都只引用 digest，schema 正文按需进 ContentStore。
- **兼容约定**：`ProgramApplication.input_schema` / `output_schema` 现有字符串字段保留为描述性注解；当字段是 JSON 对象（或通过 `input_schema_digest` / `output_schema_digest` 指向已注册 schema）时进入校验路径。无 schema 时行为保持不变（退化为现状，不强制）。

## 6. 校验点与生命周期

```text
open_run
  → apply_plan_patch（声明/物化 io_contract；set_execution_payload）
  → inspect_plan_readiness ── 输入校验：payload_schema_mismatch 在此拦截
  → commit_plan ── 复用同一 readiness，未通过不得提交
  → start_run ── 复用同一 readiness，未通过不得启动
  → dispatch（Slave admission 再校验一次，防止 stale/篡改载荷）
  → execute（subprocess_json_v1 读 stdin JSON、写 stdout JSON）
  → terminal（Slave 用 output_schema/success_semantics 校验结果，附 evidence）
  → record_result（Observer 复核输出契约后才接受 completed）
```

### 6.1 执行前校验（readiness / commit / admission）

`_evaluate_readiness` 在既有 typed-hole/binding 校验之外增加：

1. 若闭包声明了 `input_schema`（对象形式或 digest 可解析）且存在 `execution_payload`：
   - 校验 `metadata.execution_payload` 对 `input_schema`；
   - 失败 → blocker `{ "code": "payload_schema_mismatch", "hole_id"?, "schema_digest", "errors": [ ... ] }`。
   - 本例中 `input_schema.required=["scores"]` 会让 `{"payload":{"scores":[...]}}` 在 readiness 阶段被直接拦截。
2. `bind_compute_hole` 时若 binding 引用能力包：校验包声明 `io_contract.input_schema` 与闭包 `input_schema` 一致/可参数化；不匹配 → blocker `io_contract_mismatch`。
3. `commit` / `start` 复用同一 `_evaluate_readiness`，保证“未通过结构校验不得 ready / commit / start”（对应 v2 “平台不伪造 ready”）。

### 6.2 执行后校验（terminal / evidence）

1. Slave 在 executor 返回 `value` 后，用包声明的 `output_schema` 校验输出 JSON：
   - 通过 → 生成 `criterion_evidence`（evidence_ref 记录 schema digest、校验结果、结果 digest）；
   - 不通过 → 结构化 `output_schema_mismatch`（带 errors），不把结果作为成功 outcome 汇报。
2. `success_semantics` 中可机检的谓词（如 `{op:"equals", field:"average", value:87.6}`）作为 criterion 一并校验，证据并入同一 evidence。
3. Observer 在 `record_result` 接受前按 `output_schema` / `success_semantics` 复核，证据进 RunEvent 与 evidence 投影（对应 v2 §4.7 与验收 5）。

### 6.3 程序与契约对账（单一事实来源）

- `materialize_capability_package_candidate` 要求可执行 realization 声明 `io_contract`（输入/输出 schema）；缺失 → 结构化拒绝 `io_contract_required`（可对历史包宽限，新包强制）。
- 包的程序正文若带 `__loom_contract__`（程序自声明接口）则与 `io_contract` 做等值校验；不一致 → `program_contract_mismatch`。
- 最小闭环：闭包 `input_schema` → 包 `io_contract.input_schema` → 实际 `execution_payload` 三方对账，任一处不一致都在执行前拦截。

## 7. 错误码与事件

新增结构化 blocker / 错误码：

```text
payload_schema_mismatch      // 执行载荷不满足 input_schema（readiness/commit/admission）
output_schema_mismatch       // executor 输出不满足 output_schema（terminal）
io_contract_mismatch         // 绑定包契约与闭包输入契约不一致（readiness）
program_contract_mismatch    // 程序自声明接口与 io_contract 不一致（materialize）
io_contract_required         // 可执行 realization 未声明 io_contract（materialize）
```

RunEvent 新增 phase：`io_schema_validated`（通过）与 `io_schema_rejected`（拒绝，含 schema digest 与 errors）；通过/拒绝都写证据 ref。

## 8. 契约 / 数据模型 / 工具变更

### 8.1 数据模型

- `ProgramApplication`：`input_schema` / `output_schema` 允许 JSON 对象（JSON Schema）或描述字符串；新增 `input_schema_digest` / `output_schema_digest`（默认空）。
- 新增 `IoContract` 模型（见 §5），内容寻址。
- `CapabilityPackageVersion`：新增 `io_contract_ref` / `input_schema_digest` / `output_schema_digest`，与 v2 `CapabilityDescriptor` 的 `input_schema_digest` / `output_schema_digest` 对齐。
- 新增 migration：上述字段的 JSONB 列。

### 8.2 MCP / 工具描述

- `materialize_capability_package_candidate`：增加 `input_schema` / `output_schema` / `success_semantics`（或 `io_contract_ref`）入参；工具描述说明“可执行 realization 必须声明 I/O 契约”。
- `set_execution_payload`：工具描述明确“`value` 即顶层执行载荷，必须满足 `input_schema`，不得再包一层 `payload`”。
- `loom_inspect_plan_readiness`：返回新增 schema blockers，让代理能自纠。

### 8.3 校验器实现

- 新增 `loom_v2/contracts/io_schema.py`：纯函数 `validate(schema, value) -> list[ValidationError]`（实现 JSON Schema 子集）；Observer 与 Slave 复用同一实现。
- schema 正文入 ContentStore，`GET /api/v1/content/{digest}` 可回读；事件/闭包只存 digest。

## 9. 与 v2 文档对应关系

| v2 文档锚点 | 本设计落地 |
| --- | --- |
| `ProgramApplication.input_schema / output_schema / success_semantics`（§4.1） | 升级为机器可校验的 `IoContract`，参与 readiness/terminal 校验 |
| `CapabilityDescriptor.input_schema_digest / output_schema_digest`（§4.6） | `CapabilityPackageVersion` 增加同名词段，与能力描述对齐 |
| `TermSupport.support=["parse","validate"]`、`execution_stages=[commit,admission,execute,terminal]`（§4.6） | I/O 结构校验即 `validate` 在 commit/admission/terminal 的落地 |
| “Observer 必须能做通用结构校验”（§4.6 职责分层 1） | §6.1/§6.2 的 Observer 校验与复核 |
| “平台不伪造 ready/completed，留下 validator/attestation evidence”（§1 验收 5、§4.7） | `payload_schema_mismatch` 阻止 ready；`output_schema_mismatch` 阻止 completed；证据进 RunEvent |
| “Runtime 只做确定性校验，不调用 LLM”（§2） | 校验器为纯函数，Observer/Slave 复用 |

## 10. 测试计划

- **校验器单测**（`tests/contracts/test_io_schema.py`）：type/required/properties/items/enum/numeric bounds；多包一层 `{"payload":{...}}` 必须报 `required` 缺 `scores`。
- **readiness/commit 测试**（`tests/api/test_readiness.py`）：声明 `input_schema` 的闭包在载荷不匹配时 blocker 含 `payload_schema_mismatch`；`commit`/`start` 被拒；补齐载荷后通过。
- **terminal 测试**（`tests/slave/test_execution.py`）：输出满足 `output_schema` → 带 criterion evidence；不满足 → `output_schema_mismatch`、不产生成功 outcome。
- **包物化测试**（`tests/api/test_capability_packages.py`）：未声明 `io_contract` → `io_contract_required`；`__loom_contract__` 与契约不一致 → `program_contract_mismatch`；绑定契约不匹配 → `io_contract_mismatch`。
- **E2E 数据分析任务**：scores 平均分——正常载荷一次通过；多包一层载荷在 readiness 被拦截，而不是 `capability_exec_error`。

## 11. 验收标准

1. 声明 `input_schema` 的闭包在载荷不满足时无法 `ready` / `commit` / `start`，返回结构化 `payload_schema_mismatch`。
2. executor 输出必须通过 `output_schema` / `success_semantics`，失败不进入 `completed` outcome，且留下 criterion/validator evidence。
3. 新物化的可执行能力包必须声明 `io_contract`；程序接口与契约对账，绑定包与闭包输入契约一致。
4. 校验全程无 LLM；Observer 与 Slave 复用同一纯函数校验器。
5. 默认 Codex 路径 E2E（数据分析任务）在提示词不做特殊说明时，多包一层载荷被执行前拦截而非运行期失败；全量测试通过。
