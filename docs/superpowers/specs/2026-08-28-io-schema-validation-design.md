# 能力 I/O 契约校验设计（Capability I/O Schema Validation）

**日期：** 2026-08-28
**状态：** 设计（按审核意见修订，待评审）
**对应文档：** `docs/superpowers/specs/2026-08-24-single-user-workspace-v2-design.md`（权威输入：`../computility/单用户单Workspace核心架构设计v2.md`）；`docs/superpowers/specs/2026-08-27-minio-content-store-design.md`（内容存储唯一实现为 MinIO/S3）

## 1. 背景与问题

当前 `ProgramApplication.input_schema` / `output_schema` 只是自由描述字符串（例如 `"scores:number[]"`），没有任何执行点做机器校验。执行输入也没有绑定到 `DataApplication` 的资源引用，能力包程序是自由 Python 正文。系统对“程序期待什么、载荷给的是什么”没有统一契约，只能依赖 coding-agent 的自觉保持一致。

实测暴露的具体问题：coding-agent 偶发把 `set_execution_payload` 写成 `value={"payload": {"scores":[...]}}`（多包一层），而它自己生成的程序读 `d["scores"]`。因为没有任何结构校验点，这个不一致在 `readiness`/`commit` 阶段全部通过，直到执行器收到顶层为 `payload` 的 JSON、程序抛 `KeyError`、Slave 返回 `capability_exec_error` 才暴露，Run 被标记失败。

v2 文档已经为 I/O 契约留好了语义锚点，但本实现尚未落地为可校验结构：

- `ProgramApplication = { operation_ref, semantics_digest, input_schema, output_schema, success_semantics }`（v2 §4.1）。
- `CapabilityDescriptor` 已声明 `input_schema_digest` / `output_schema_digest`（v2 §4.6）。
- `TermSupport.support` 含 `parse` / `validate`，`execution_stages` 含 `commit` / `admission` / `execute` / `terminal`（v2 §4.6）。
- 职责分层第 1 条：“Observer 不需要理解每个领域值的业务含义，但必须能做通用结构校验、版本兼容检查和未知 required 的拒绝。”（v2 §4.6）
- 验收第 5 条：“可检查约束留下 validator/attestation/readiness evidence，平台不伪造 `ready`/`stopped`。”（v2 §1）

## 2. 目标

1. 用机器可读的类型结构（JSON Schema 作为 wire 格式）表达 operation 的输入/输出契约，内容寻址。
2. 在执行前（`readiness` / `commit` / Slave admission）校验已绑定输入 `ResourceRef` 对 `input_schema_ref`，把“多包一层、缺字段、类型错误”拦截为结构化 blocker（`payload_schema_mismatch`），而不是运行期 `capability_exec_error`。
3. 执行后校验 executor 输出对 `output_schema_ref`，并按闭包提供的 validator plugin 或 coding-agent attestation 处理 `success_semantics`；不匹配不得进入 `completed` outcome（`output_schema_mismatch`）。
4. 闭包必须携带 I/O 契约引用（`io_contract_ref`），能力包复制并锁定同一契约，形成“闭包契约 == 能力包契约 == 已绑定输入资源”的单一事实来源。
5. 结构校验全程确定性且不调用 LLM；无 validator plugin 的业务成功标准交由 coding-agent attestation；Observer 是校验与 evidence 的接受权威，Slave 只按声明做结构校验。

## 3. 非目标

- 不自动生成或改写程序正文；不校验程序语义正确性（“算的是不是平均分”仍需运行证据/测试）。
- 不引入完整 RPC 框架编译管线（protobuf/thrift）；JSON Schema 是 wire 契约，protobuf/thrift 可作为“约定类型”来源在后续导出为 JSON Schema，不在本设计实现。
- 不改 `TypedTerm` / `VocabularyRegistry` 的既有语义；I/O schema 作为 `ProgramApplication` 的独立锚点增量演进。
- 不做跨 Slave 的 schema 协商协议；沿用 `CapabilityDescriptor` 的 input/output schema digest 作为能力侧契约。
- 不在本设计实现动态 DAG fan-out、循环或自动 reduce。

## 4. 设计原则（对应 v2 §2）

- **确定性最小面**：I/O schema 校验是纯函数、无副作用、不调用 LLM；Runtime 只做 CAS、引用、结构校验、READY 与 evidence 校验。业务成功语义没有 plugin 时由 coding-agent attestation，不伪装成确定性校验。
- **Observer 状态权威**：结构校验结果、`payload_schema_mismatch` / `output_schema_mismatch` 以及 criterion evidence 的最终接受只在 Observer。
- **细化权唯一**：I/O schema 由 coding-agent 在细化期提出，Runtime 只做机械校验，不代填、不猜测、不放宽。
- **引用与内容分离**：schema 按内容寻址（digest），闭包/包只存 digest 与 ref；schema 正文进 ContentStore，不进入事件、日志或闭包快照。I/O schema 不支持 inline 形态。
- **平台不伪造 ready/completed**：未通过输入校验不得 ready，未通过输出校验不得 completed。

## 5. I/O 契约表示

新增一等模型 `IoContract`，作为 operation 输入/输出契约的单一事实来源。完整契约正文作为规范化内容存入 ContentStore；闭包和能力包只保存指向该正文的 `io_contract_ref`。

```text
IoContract = {
  schema_version: "io.v1",
  input_schema_ref: ResourceRef | null,   // content://sha256/<digest>
  output_schema_ref: ResourceRef | null,  // content://sha256/<digest>
  success_semantics: StructuredValue | null,
  success_validator_ref: ResourceRef | null,
}
```

- **wire 格式**：JSON Schema 2020-12 的最小子集：`type` / `required` / `properties` / `items` / `enum` / `const` / `minimum` / `maximum`。本版本不实现 `format` assertion；不支持的关键字拒绝，不静默忽略。
- **内容寻址**：schema 正文和 IoContract 正文都必须写入 MinIO/S3 ContentStore。schema 与 IoContract 均使用 UTF-8 的 RFC 8785/JCS 规范化 JSON；`io_contract_ref.version_or_digest` 是契约身份，`input_schema_ref.version_or_digest` / `output_schema_ref.version_or_digest` 是对应 schema 身份；`ResourceRef.access_binding` 不参与任何契约 digest。
- `io_contract_ref` 指向 `content://sha256/<digest>` 的规范化 IoContract 正文；schema 引用的 media type 为 `application/schema+json`，IoContract 引用的 media type 为 `application/vnd.loom.io-contract+json`。
- **身份不变量**：`input_schema_ref=null` 表示没有 JSON 输入，非空则节点必须绑定输入；`output_schema_ref=null` 表示没有 JSON 输出。所有引用必须是 `content://sha256/<digest>`，引用中的 digest 必须与 ContentStore 正文重算结果一致；不一致返回结构化 conflict。
- **写入规则**：schema 和 IoContract 必须先通过 ContentStore 写入接口取得引用，再在 patch 中引用；Observer 校验引用与正文 digest 一致，持久化模型中不存在 inline schema。
- **契约字段**：可执行 Node 必须声明 `io_contract_ref`。v2 中 `ProgramApplication.input_schema` / `output_schema` / `success_semantics` 保留为应用视图，由 `io_contract_ref` 解析得到，不再在 `ProgramApplication` 中重复持久化。无输入/无 JSON 输出统一用对应 schema ref 为 `null` 表示。

`ProgramApplication` 的三个应用视图字段不能作为第二套可执行契约。若闭包在可执行路径上独立携带非空的 `input_schema` / `output_schema` / `success_semantics`，readiness 返回 `inline_io_contract_forbidden`，并列出携带的字段；系统不做自动同步、合并或降级解释。

`ProgramApplication.success_semantics` 是 `IoContract.success_semantics` 的只读应用视图；校验和身份只取 IoContract 正文及其 `io_contract_ref`。

criterion 的稳定 ID 仍由 `ClosureContract.required_success_criteria` 承载；该列表引用本次 I/O `ValidationEvidence`，`IoContract` 不重复定义 `criterion_id`。

### 5.1 成功验证 plugin

`success_semantics` 只描述应用成功标准，不由通用 Runtime 解释。需要机器判定时，闭包必须同时提供确定性的 `success_validator_ref`：

`success_semantics` 是由 validator plugin 解释的 opaque `StructuredValue`；其具体字段和比较规则不属于 Observer/Slave 的通用语义。闭包通过 `success_validator_ref: ResourceRef` 指定 plugin，能力包必须原样锁定该引用。plugin 使用固定 `validator.v1` 接口：输入为已通过 output schema 的 JSON，输出为 `pass | fail + ValidationError[]`。

没有 plugin 时，output schema 仍可校验，但成功标准只能进入 `attestation_required`，由 coding-agent 读取证据后调用 `attest_criterion`；Observer/Slave 不猜测业务语义。

## 6. 校验点与生命周期

```text
open_run
  → apply_plan_patch（声明/物化 io_contract；绑定输入 ResourceRef）
  → inspect_plan_readiness ── 输入校验：payload_schema_mismatch 在此拦截
  → commit_plan ── 复用同一 readiness，未通过不得提交
  → start_run ── 复用同一 readiness，未通过不得启动
  → dispatch（Slave admission 再校验一次，防止 stale/篡改输入引用）
  → execute（subprocess_json_v1 读 stdin JSON、写 stdout JSON）
  → terminal（Slave 用 output_schema_ref 和闭包 validator plugin 校验结果，附 evidence）
  → record_result（Observer 复核输出契约后才接受 completed）
```

### 6.1 执行前校验（readiness / commit / admission）

`_evaluate_readiness` 在既有 typed-hole/binding 校验之外增加（注意：MinIO 设计已把 `_evaluate_readiness` 改为 `async def`，以便 await `ContentStore.stat/get`；输入校验因此运行在异步 readiness 内）：

1. 若闭包的 IoContract 声明了 `input_schema_ref`：
   - 节点必须有已绑定的输入 `ResourceRef`；缺失 → blocker `payload_missing`；
   - Observer 通过 ContentStore 取回内容并校验其 digest，再校验内容对 `input_schema_ref` 指向的 schema；
   - 失败 → blocker `{ "code": "payload_schema_mismatch", "schema_digest", "input_ref", "errors": [ ... ] }`；
   - 本例中，`input_schema_ref` 指向的 schema 若声明 `required=["scores"]`，就会让 `{"payload":{"scores":[...]}}` 在 readiness 阶段被直接拦截。
2. `bind_compute_hole` 时若 binding 引用能力包：能力包的 `io_contract_ref.version_or_digest` 必须与闭包一致，并且 input/output schema、success semantics、success validator ref 全部严格相等；不匹配 → blocker `io_contract_mismatch`。
3. `commit` / `start` 复用同一 `_evaluate_readiness`，保证“未通过结构校验不得 ready / commit / start”（对应 v2 “平台不伪造 ready”）。

### 6.2 执行后校验（terminal / evidence）

1. Slave 在 executor 返回 `value` 后，解析包的 `io_contract_ref`，再按其中的 `output_schema_ref` 取回并校验输出 JSON：
   - 通过 → 生成 `ValidationEvidence`（记录 schema/validator 引用、输入输出 digest）；
   - 不通过 → 结构化 `output_schema_mismatch`（带 errors），Attempt 进入 `failed`，不产生成功 outcome。
2. 存在 `success_validator_ref` 时，由声明的 validator plugin 校验 `success_semantics`；没有 plugin 时产生 `attestation_required`，不得由通用 Runtime 自动解释。
3. Observer 在 `record_result` 接受前按同一 schema/plugin 引用复核；校验失败时接受失败 Attempt/ExecutionOutcome，并将 Run 投影为 `decision_required(validation_failed)`，不进入 `completed`。验证通过只产生 evidence，Run 是否 completed 仍按 v2 的 required criteria 全量重算。

`ValidationEvidence` 的简化结构如下（本阶段不引入额外签名体系）：

```text
ValidationEvidence = {
  evidence_id,
  attempt_id,
  execution_epoch,
  validator_ref: ResourceRef | null,
  schema_ref: ResourceRef | null,
  input_digest: Digest | null,
  output_digest: Digest | null,
  result: "pass" | "fail",
  errors: ValidationError[],
  issuer: "slave" | "observer",
  created_at
}
```

Observer 必须校验 attempt/execution epoch、schema/plugin 引用和输入输出 digest；旧 epoch 或其他 Attempt 的 evidence 一律拒绝。`execution_id` 从 Attempt 归属得到，不在 evidence 中重复保存；引用中的 digest 是唯一身份。

### 6.3 程序与契约对账（单一事实来源）

- `materialize_capability_package_candidate` 要求可执行 realization 声明 `io_contract_ref`；缺失 → 结构化拒绝 `io_contract_required`。本设计不保留历史包兼容逻辑，所有进入本版本执行面的能力包都必须满足 `io.v1`。
- 程序正文不再通过 AST、import 或执行进行接口自声明检查；能力包的 `io_contract_ref` 是唯一契约来源，package test/validator 负责提供程序符合契约的运行证据。
- 最小闭环：闭包 `io_contract_ref` → 包 `io_contract_ref` → 已绑定输入 `ResourceRef` 三方对账，任一处不一致都在执行前拦截。

## 7. 错误码与事件

新增结构化 blocker / 错误码：

```text
payload_schema_mismatch      // 执行载荷不满足 input_schema（readiness/commit/admission）
payload_missing              // 已声明 input_schema，但节点没有绑定输入 ResourceRef
output_schema_mismatch       // executor 输出不满足 output_schema（terminal）
io_contract_mismatch         // 绑定包契约与闭包 IoContract 不一致（readiness）
io_contract_required         // 可执行 realization 未声明 io_contract（materialize）
attestation_required         // 没有闭包提供的 success validator，需要 coding-agent 语义证言
inline_io_contract_forbidden // ProgramApplication 携带独立 inline I/O 语义字段
```

错误统一使用 `DomainErrorEnvelope`：至少包含 `code`、`category`、`retryable`、`stage`、`safe_message`、`operation_ref` 和 allowlisted `details`。schema 失败 details 只允许 schema digest、JSON path、keyword、expected/observed 类型和稳定排序的 errors，不携带 payload 正文。

RunEvent 在既有 `activity/outcome/phase/sequence` envelope 中新增 phase：`io_schema_validated`（通过）与 `io_schema_rejected`（拒绝）；每个事件带稳定 operation/idempotency ref，重复检查不得产生重复业务状态。通过/拒绝都写 `ValidationEvidence` ref。

## 8. 契约 / 数据模型 / 工具变更

### 8.1 数据模型

- `ProgramApplication`：保留 v2 的 `input_schema` / `output_schema` / `success_semantics` 语义字段作为 `IoContract` 的只读应用视图，持久化只新增 `io_contract_ref`；不再把描述字符串作为可执行契约。
- 新增 `IoContract` 模型（见 §5），内容寻址。
- `CapabilityPackageVersion`：新增 `io_contract_ref`；与 v2 `CapabilityDescriptor` 的 schema digest 通过解析 IoContract 得到，不在包中重复保存。
- `NodeInputBinding`：`node_id`、`input_ref: ResourceRef`、`provenance`；内容 digest 从 `input_ref` 得到，schema digest 从 IoContract 得到，不在 binding 中重复保存。执行输入不再存为裸 `metadata.execution_payload`。
- 新增 migration：上述字段的 JSONB 列。

### 8.2 MCP / 工具描述

- 新增通用 `loom_put_content` 数据面操作：上传规范化 JSON Schema 或 IoContract 正文到 ContentStore，按 `media_type` 区分内容；正文不嵌入 patch、事件或日志。
- `materialize_capability_package_candidate`：接收 `io_contract_ref`；工具描述说明“可执行 realization 必须声明 I/O 契约”。schema 和 IoContract 正文必须预先通过 `loom_put_content` 写入接口取得 ref。
- `set_execution_payload`：保留工具名以减少 MCP 变更，但参数改为 `input_ref: ResourceRef`；Observer 校验资源存在、digest、schema 和作用域，不接受裸 JSON `value`。
- `loom_inspect_plan_readiness`：返回新增 schema blockers，让代理能自纠。

### 8.3 校验器实现

- 不自行实现 JSON Schema 引擎。运行时依赖 Python `jsonschema` 4.x 的 `Draft202012Validator`；JSON 文本解析使用标准库 `json.loads`/`json.load`；固定的 Loom wire/domain 模型继续使用现有 Pydantic v2。`jsonschema` 负责标准关键字的确定性校验，Pydantic 不替代动态 I/O schema。
- 新增 `loom_v2/contracts/io_schema.py` 作为策略适配层，暴露纯函数 `validate(schema, value) -> list[ValidationError]`（实现 JSON Schema 2020-12 的 `json-schema-2020-12-loom-v1` 子集）；Observer 与 Slave 只依赖这个适配层，不直接散落调用第三方库。
- 适配层在调用 `Draft202012Validator` 前先递归检查允许关键字白名单；`jsonschema` 默认会忽略未知关键字，因此 Loom 必须把未知/未实现关键字转换为结构化 schema 错误并拒绝。当前明确拒绝 `$ref`、`format` 以及白名单之外的关键字，不启用远程引用解析，不做类型 coercion。
- 适配层先调用 `Draft202012Validator.check_schema(schema)` 校验 schema 本身，再调用 `iter_errors(value)` 校验实例；错误按绝对 JSON path、keyword 和 message 的稳定键排序，转换为 Loom 的 `ValidationError`，不得携带 payload 正文。
- `jsonschema` 的默认行为保持纯函数、无副作用；schema 正文仍先经 `json.loads` 解码、JCS 规范化并写入 ContentStore，校验时只接受已验证 digest 的内容。
- `pyproject.toml` 增加运行时依赖 `jsonschema>=4.23,<5`；`jsonschema` 的传递依赖不作为 Loom wire API 暴露。
- schema 与 IoContract 正文入 MinIO/S3 版 ContentStore（异步 `put`，身份由内容推出，`GET /api/v1/content/{digest}` 可回读）；事件/闭包只存 `ResourceRef`，`access_binding` 不携带部署位置。
- 本阶段不扩展 validator 的资源限制/复杂度治理；只允许文档列出的关键字、禁止 `$ref` 和类型 coercion，后续再单独设计复杂 schema 的限额与沙箱。

### 8.4 契约兼容与校验失败状态

- `io.v1` 采用严格兼容：closure 与 package 的 `io_contract_ref.version_or_digest`、input/output schema ref、success semantics 和 success validator ref 必须完全相等；不实现未定义的“可参数化”或通用 schema 子集关系。
- `output_schema_mismatch` 或 validator plugin 返回 fail 时，Attempt 写入失败的 `ValidationEvidence` 并进入 `failed`；ExecutionOutcome 为 `failed`，不发布成功 ResourceRef。Run 投影进入 `decision_required(validation_failed)`，由 coding-agent 通过 `resolve_decision` 细化新版本或显式结束，不自动重试。
- 无 `success_validator_ref` 时，输出结构通过只能产生 schema evidence；required semantic criterion 保持 `attestation_required`，由 coding-agent 调用 `attest_criterion` 后再按全部 criteria 重算 Run 终态。

## 9. 与 v2 文档对应关系

| v2 文档锚点 | 本设计落地 |
| --- | --- |
| `ProgramApplication.input_schema / output_schema / success_semantics`（§4.1） | 通过 `io_contract_ref` 指向内容寻址契约，参与 readiness/terminal 校验 |
| `CapabilityDescriptor.input_schema_digest / output_schema_digest`（§4.6） | 从能力包引用的 IoContract 解析得到，不在 `CapabilityPackageVersion` 中重复保存 |
| `TermSupport.support=["parse","validate"]`、`execution_stages=[commit,admission,execute,terminal]`（§4.6） | I/O 结构校验即 `validate` 在 commit/admission/terminal 的落地 |
| “Observer 必须能做通用结构校验”（§4.6 职责分层 1） | §6.1/§6.2 的 Observer 校验与复核 |
| “平台不伪造 ready/completed，留下 validator/attestation evidence”（§1 验收 5、§4.7） | `payload_schema_mismatch`/`payload_missing` 阻止 ready；`output_schema_mismatch` 阻止 completed；无 plugin 时转 `attestation_required`；证据进 RunEvent |
| “Runtime 只做确定性校验，不调用 LLM”（§2） | 校验器为纯函数，Observer/Slave 复用 |

## 10. 测试计划

- **校验器单测**（`tests/contracts/test_io_schema.py`）：先验证标准库 JSON 解析与 `Draft202012Validator.check_schema` 的 schema 元校验，再覆盖 type/required/properties/items/enum/minimum/maximum；多包一层 `{"payload":{...}}` 必须报 `required` 缺 `scores`；未知关键字、`$ref`、`format` 必须被适配层拒绝而不是静默忽略；错误顺序稳定且不包含 payload 正文；schema 和 IoContract 的 JCS 规范化序列化/digest 一致性（与 MinIO 版 ContentStore 的 digest 身份一致）。
- **readiness/commit 测试**（`tests/api/test_readiness.py`）：声明 `input_schema_ref` 但未绑定输入时返回 `payload_missing`；输入不匹配时返回 `payload_schema_mismatch`；`commit`/`start` 被拒；绑定正确的 `ResourceRef` 后通过。
- **terminal 测试**（`tests/slave/test_execution.py`）：输出满足 `output_schema` → 带 `ValidationEvidence`；不满足 → `output_schema_mismatch`、Attempt/ExecutionOutcome 为 failed、Run 为 `decision_required(validation_failed)`；无 validator plugin 时返回 `attestation_required`。
- **包物化测试**（`tests/api/test_capability_packages.py`）：未声明 `io_contract_ref` → `io_contract_required`；绑定契约（input/output/success/plugin）不匹配 → `io_contract_mismatch`；package test 失败时拒绝发布。
- **E2E 数据分析任务**：scores 平均分——正常载荷一次通过；多包一层载荷在 readiness 被拦截，而不是 `capability_exec_error`。

## 11. 验收标准

1. 声明 `input_schema_ref` 的闭包在未绑定输入或输入不满足时无法 `ready` / `commit` / `start`，分别返回 `payload_missing` / `payload_schema_mismatch`。
2. executor 输出必须通过 `output_schema_ref`；失败产生 `output_schema_mismatch`、失败 Attempt/ExecutionOutcome 和 `ValidationEvidence`，不进入 `completed`。有 `success_validator_ref` 时还必须通过 plugin；没有 plugin 时必须由 coding-agent 完成 attestation。
3. 新物化的可执行能力包必须声明 `io_contract_ref`；package test/validator 证明程序符合契约，绑定包与闭包的 input/output/success/plugin 契约严格一致。
4. 校验全程无 LLM；Observer 与 Slave 复用同一纯函数校验器。
5. 默认 Codex 路径 E2E（数据分析任务）在提示词不做特殊说明时，多包一层载荷被执行前拦截而非运行期失败；全量测试通过。
