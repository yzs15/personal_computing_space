# 编排程序 Pyright 预检设计

**日期：** 2026-09-02  
**状态：** 设计（已收窄，待评审）  
**对应文档：** `docs/superpowers/specs/2026-08-26-distributed-analysis-e2e-design.md`（动态编排）；`docs/superpowers/specs/2026-08-28-io-schema-validation-design.md`（I/O 契约与 readiness 校验）

## 1. 背景与问题

编排程序中的未定义名称（例如把 JSON 的 `null` 直接写进 Python）以及明显的参数类型错误，通常要等 Driver 启动 Docker 编排容器后才暴露。这样会浪费一次完整执行，并把本可立即修复的错误变成运行期失败。

当前动态编排已经具备 Docker 沙箱、受限 `OrchestrationContext`、ResourceRef 校验和结构化运行结果。本设计只增加一次确定性的静态预检，不改变现有执行边界或动态节点调度模型。

## 2. 目标

1. 对 `orchestrator_python_v1` 程序在 readiness 阶段执行一次 Pyright 检查。
2. 检查未定义名称和可静态确定的类型错误，重点覆盖 DSL 调用；典型错误包括 `null`、错误的 `emit_node` 参数和错误的 `result` 参数。
3. 编排入口必须显式标注类型，使 `ctx` API 的类型检查真正生效：

   ```python
   async def orchestrate(
       ctx: "OrchestrationContext",
       input_ref: "ResourceRef",
   ) -> "ResourceRef":
       ...
   ```

   类型名称使用字符串，运行容器无需导入宿主机模块。未标注入口不提供兼容路径，直接产生结构化 blocker。
4. Pyright 诊断进入现有 readiness blocker 流程；`inspect_readiness`、`commit` 和 `start` 使用同一结果，未通过时不得启动 Run。
5. 保留当前 Docker 沙箱、受限 context、AST/import/危险调用检查，以及执行期 ResourceRef 和协议校验。

## 3. 非目标

- 不做业务语义、数据流、算法正确性或输入 payload 内容推断；`read_json()` 返回值不从 I/O schema 生成 Python 类型。
- 不用 Pyright 替代 Docker 安全边界或 Driver 的最终运行时校验。
- 不新增全局强类型 JSON Schema 政策，不禁止合法的自由表单，也不把 payload 中的业务字段提升为平台规则。
- 不自动修改程序、不自动重试、不在 readiness 阶段调用 LLM。
- 不保留旧的未标注 `orchestrate` 入口兼容层。

## 4. 执行边界

编排程序仍在现有 Docker OS-level sandbox 中执行：无网络、只读 rootfs、非 root 用户、丢弃 capabilities、固定镜像和资源限制。现有 `_BOOTSTRAP` 的受限 builtins、模块 allowlist、危险调用拒绝和 stdout 协议隔离继续有效。

Pyright 只在 Observer readiness 宿主进程中分析源代码，不执行程序，不读取数据库、ContentStore 凭据或部署环境。删除或放宽现有安全限制不属于本设计。

## 5. Pyright 预检

### 5.1 调用方式

在 `_evaluate_orchestration_package` 已完成程序内容 digest、媒体类型和 UTF-8 校验后：

1. 将源代码写入临时检查目录。
2. 在同一目录生成 `loom_orchestration_types.pyi` 和固定的 `pyrightconfig.json`。
3. 生成只用于检查的副本，在模块 docstring 与已有 `from __future__` 导入之后插入类型桩导入：

   ```python
   from loom_orchestration_types import OrchestrationContext, ResourceRef
   ```

   原始程序不被修改，也不执行该副本。
4. 使用项目依赖中锁定的官方 `pyright` Python 发行包调用 Pyright CLI 一次，通过 `pyright --outputjson` 读取诊断；Observer 镜像随应用安装该依赖，readiness 不做运行时探测、自动下载或降级。
5. 单次检查超时固定为 10 秒；子进程工作目录固定为临时检查目录，且只能读取该目录中的检查副本、类型桩和配置。超时、进程不可用或输出不是合法 JSON 时返回 `orchestration_program_typecheck_unavailable`，不允许 readiness 通过。
6. 临时目录和子进程在成功、失败、超时路径都清理。

每次 readiness 评估执行一次检查，不持久化 Pyright 结果，也不增加跨请求缓存协议。

### 5.2 类型桩

类型桩采用运行时 JSON wire 形态，而不是宿主机的 Pydantic 类。`ResourceRef` 只表示“字符串键的 mapping”这一静态类别，使 Pyright 能拒绝字符串、数字等明显错误参数；字段完整性、content URI、digest 和资源可用性仍由现有运行时校验：

```python
from collections.abc import Mapping
from typing import Any, Protocol, TypeAlias

ResourceRef: TypeAlias = Mapping[str, object]

class OrchestrationContext(Protocol):
    async def read_json(self, ref: ResourceRef) -> Any: ...
    def emit_node(self, package_ref: ResourceRef, input_refs: list[ResourceRef]) -> str: ...
    async def result(self, handle: str) -> ResourceRef: ...
```

`read_json` 的返回值保持 `Any`，因为 payload 的领域结构由现有 I/O schema 校验负责；本设计不把业务 schema 复制到 Python 类型桩，也不承诺识别从 payload 中取出的值是否为 `ResourceRef`。

### 5.3 诊断范围与映射

Pyright 配置使用 basic 类型检查模式，并只将以下诊断作为阻塞条件：

- `reportUndefinedVariable` → `orchestration_program_unresolved_name`；
- `reportArgumentType`、`reportCallIssue`、`reportAssignmentType`、`reportReturnType`、`reportAttributeAccessIssue`、`reportGeneralTypeIssues`、`reportInvalidTypeForm` 和 `reportUnboundVariable` → `orchestration_program_type_error`。

诊断详情只包含稳定的规则名、原始程序位置和消息，不携带程序正文或 payload。每条诊断至少包含 `file`、`line`、`column`、`end_line`、`end_column`、`severity`、`rule` 和 Pyright 原始 `message`。`file` 统一为 `orchestration.py`，不暴露临时目录路径；检查副本因插入类型导入产生的行号偏移在映射 blocker 时扣除，类型桩自身的诊断不对外返回。样式、未使用变量、未知类型传播和缺少第三方类型桩不作为 blocker；模块是否合法仍由现有 Driver 执行边界检查。

入口签名不是顶层 `async def orchestrate(ctx, input_ref)`，或任一注解不是字符串字面量 `"OrchestrationContext"`、`"ResourceRef"`，返回 `orchestration_program_type_error`，并附 `missing_entry_annotation` 或 `invalid_entry_annotation` 规则；这是让 context 调用类型检查生效的必要前置条件，也避免检查副本中的导入掩盖运行时名称错误。

### 5.4 返回 coding-agent 的错误格式

Pyright blocker 必须以结构化错误返回给 coding-agent，不得把 blocker 列表拼接进异常字符串或只返回单一 `code`。`loom_inspect_plan_readiness` 直接返回完整 `blockers`；`loom_commit_plan` 和 `loom_start_run` 失败时，Driver MCP 的 JSON-RPC tool result 使用 `success: false`，并在错误内容中保留同一组 blocker：

```json
{
  "success": false,
  "error": {
    "code": "readiness_blocked",
    "blockers": [
      {
        "code": "orchestration_program_unresolved_name",
        "diagnostics": [
          {
            "file": "orchestration.py",
            "line": 7,
            "column": 38,
            "end_line": 7,
            "end_column": 42,
            "severity": "error",
            "rule": "reportUndefinedVariable",
            "message": "null is not defined"
          }
        ]
      }
    ]
  }
}
```

多条诊断按原始程序的行、列稳定排序，完整消息不得降级为 `str(exception)`；coding-agent 可以据此直接定位并重写程序。

## 6. Readiness 与运行时流程

`inspect_readiness`、`commit` 和 `start` 继续调用同一个 `_evaluate_readiness`：

```text
load package/content
  → digest/media/UTF-8 校验
  → orchestrate 入口签名校验
  → Pyright 单次预检
  → 现有 package、I/O contract、allowlist、target、预算校验
  → ready / blockers
```

Pyright blocker 使 Run 保持不可提交或不可启动。`loom_commit_plan` 和 `loom_start_run` 的错误通过上述结构化动态工具通道返回给 coding-agent；系统不自动改写源代码。

通过 readiness 不代表程序一定成功。Driver 执行时仍保留 AST、import/危险调用、协议消息、ResourceRef、超时和 Docker 进程校验。执行期错误按现有 Run lifecycle 记录为结构化失败或 repair decision，不在本设计中新增自动重试。

## 7. 错误码

```text
orchestration_program_invalid          # UTF-8/语法错误
orchestration_program_type_error       # 入口标注或 Pyright 类型错误
orchestration_program_unresolved_name  # Pyright 未定义名称
orchestration_program_typecheck_unavailable # CLI、超时或输出协议错误
```

## 8. 测试

- 单元测试覆盖：合法的带标注入口、`null` 未定义名、错误的 `emit_node` 参数、错误的 `result` 参数、缺少入口标注、语法错误和 Pyright 不可用。
- readiness/API 测试确认上述 blocker 会阻止 `commit`/`start`，且不会创建执行 Attempt；MCP tool result 保留完整的诊断位置、规则和原始消息，不退化为异常字符串。
- 回归测试更新现有动态编排样例，为 `orchestrate` 增加字符串类型标注；dict 形式的 package/resource ref 必须通过类型检查，从 `read_json()` 取得的 `Any` 不新增业务类型约束。
- 现有 Docker 沙箱安全测试和动态节点 e2e 继续运行，不因 Pyright 预检删除或放宽安全限制。

## 9. 取舍

- 使用官方 `pyright` Python 发行包和 CLI，不在 spec 中保留 `pyright`/`basedpyright` 二选一；版本固定为 `1.1.411`。
- 只检查能导致程序无法正常调用的未定义名和类型错误；不把 Pyright 变成完整 lint 或业务静态分析器。
- 类型桩只描述稳定的 context/wire API；payload 结构和 content-ref 可解析性仍由现有 schema、Observer 和 Driver 运行时校验负责。
