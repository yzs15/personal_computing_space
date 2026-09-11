# Capability Package 模型讨论结论

**日期：** 2026-09-07  
**状态：** 讨论结论与后续设计方向

本文整理关于 Loom v2 CapabilityPackage 的讨论，区分当前仓库已经实现的行为和后续建议，避免把设计方向误读为现有功能。

## 1. 核心结论

CapabilityPackage 可以继续作为函数能力、服务能力、模块能力等不同能力形态的统一资源抽象。

但是，CapabilityPackage 本身应该保持简单，只定义所有能力包共有的包头和生命周期信息。不同类型能力包的内部结构由各自的类型实现负责，不应该把当前单函数模型扩展成一个试图支撑任意能力的万能类。

可以概括为：

~~~text
CapabilityPackage = 通用包头/资源封装 + 类型化包体
~~~

function、service、module 等是能力包的形态；执行机制由能力包中的 `execution` 执行契约描述，当前实现使用 `process:json_stdio`，并为 `container:http`、`container:grpc`、`container:mcp` 预留扩展。

## 2. 当前实现的实际模型

### 2.1 当前包更接近“单操作能力函数”

当前 CapabilityPackageVersion（loom_v2/contracts/types.py）包含以下单数概念：

- operation_descriptor_ref：能力对外提供的应用操作，例如 matmul、summarize；
- io_contract_ref：该操作的输入/输出契约；
- program_content_ref：程序正文的 ContentStore 引用；
- `execution.kind`：新模型中的执行契约标识，例如 `process:json_stdio`；
- `execution.version`：执行契约版本，例如 `1`；
- `executor_kind`、`executor_operation` 不属于当前包格式。

因此当前一个包版本主要表达：

~~~text
一个应用 operation
+ 一个 I/O contract
+ 一个程序 artifact
+ 一个执行器调用方式
~~~

它不是“只能有一个输入字段”。输入可以是复杂 JSON，包含多个字段或多个 content reference；但对外仍然是一次调用对应一个输入契约。

### 2.2 当前 Slave 的调用路径是单次调用模型

SlaveService.run(attempt_id, operation, payload) 接收一个 operation 和一个 payload，完成一次执行并返回一个结果。

对当前的 `process:json_stdio` 而言，ProcessJSONStdioV1Adapter 会：

1. 启动隔离的 Python 子进程；
2. 通过 stdin 传入 JSON；
3. 从 stdout 读取 JSON；
4. 将结果包装为执行结果，并进行输出契约/validator 校验。

程序使用 python -I -c 运行，当前没有 Python 模块导入 API、导出函数表、依赖清单或长期驻留对象状态。因此它不像 Python library/module，而更像：

~~~text
f(input) -> output
~~~

### 2.3 当前不是完整的 FaaS，也不是 service

执行面具有 FaaS/函数风格，但控制面比普通函数平台更丰富。能力包还包含：

- 不可变版本和 package_digest；
- run_bound / workspace_reusable 作用域；
- candidate / published / abandoned 发布状态；
- 针对具体 Slave 的 activation 和 health report；
- ComputeBinding、约束、权限和 replay policy；
- I/O schema、success validator 和运行证据；
- 完整 provenance。

所以当前更准确的描述是：

> 带契约、版本、发布和激活生命周期管理的单操作可执行能力包。

它还不是 service-level 模型，因为当前没有包级多端点注册、端点路由、长期服务进程、共享服务状态或 service-specific lifecycle。

### 2.4 现有示例也按单操作拆包

examples/distributed-statistics 将 summarize、merge-summaries 和 orchestrate 分别物化为能力包。即使 orchestrate 能够编排多个子节点，它对外仍然是一个 orchestrate operation，不是一个同时提供多个公共端点的服务。

## 3. 执行器相关术语

当前代码在 `loom_v2/slave/executor.py` 中已经形成了执行器注册表这一层抽象：

~~~text
ExecutorRegistry
    execution.kind -> ExecutorAdapter
~~~

建议使用以下术语：

| 概念 | 含义 | 当前/计划示例 |
| --- | --- | --- |
| CapabilityPackage | 对外可治理、可绑定、可激活的能力资源 | 函数包、服务包 |
| `package_type` | 包体结构的 discriminator，不直接表示执行方式 | `function`、`service` |
| `package_version` | 具体能力包的业务/实现版本 | `3` |
| `execution.kind` | 执行契约引用的执行后端/适配器类型 | `process:json_stdio`、`container:http` |
| `execution.version` | 执行契约/适配器版本 | `1` |
| `ExecutorAdapter` | 在 Slave 上实现某种执行机制的适配器 | `ProcessJSONStdioV1Adapter` |
| `execution` | 能力包的执行契约 | `{kind: process:json_stdio, version: 1}` |
| Slave / Worker | 承载执行器的执行节点或执行机 | slave-a、slave-b |
| runtime_profile | 目标运行环境和机器相关信息 | OS、架构、运行时、资源限制 |
| protocol | 调用时采用的线协议 | JSON stdio、HTTP、gRPC、MCP |

因此：

- `process:json_stdio` 是执行契约中的执行后端/执行适配器类型，不是执行机；
- ProcessJSONStdioV1Adapter 是该类型的具体实现；
- Slave 才是实际承载执行的执行节点；
- `container:http`、`container:grpc`、`container:mcp` 是未来扩展点，当前尚未实现完整服务运行时。

应用 operation 和执行器 operation 也要区分：

~~~text
operation_descriptor_ref = "matmul"
execution.kind           = "process:json_stdio"
execution.version        = "1"
~~~

含义是：能力对外提供 `matmul`，但通过进程级 JSON stdio 执行契约实现。`run_code` 仅是当前 Slave API 的调用动作，不是包头字段或应用能力类型。

## 4. 建议的统一包结构

建议将能力包建模为通用 envelope，而不是让基础类拥有所有类型的字段：

~~~text
CapabilityPackageVersion {
    header: CapabilityPackageHeader
    body:   TypedCapabilityPackageBody
}
~~~

### 4.1 通用包头

包头只保留跨能力类型都成立的信息，例如：

~~~text
CapabilityPackageHeader {
    package_type
    package_id
    package_version
    execution {
        kind
        version
    }
    package_digest
    scope
    publication_state
    provenance
}
~~~

`package_type` 是必要的 discriminator，用于选择包体解析器和类型处理器。当前选定的最小公共字段形式为：

~~~json
{
  "package_type": "function",
  "package_version": "3",
  "execution": {
    "kind": "process:json_stdio",
    "version": "1"
  }
}
~~~

包头不应包含某一种能力专有的 `program_content_ref`、端点表或模块导出符号；这些属于对应类型的包体。

### 4.2 类型化包体

当前单函数实现可以成为一种具体包体：

~~~text
FunctionCapabilityPackageBody {
    operation_descriptor_ref
    io_contract_ref
    program_content_ref
}
~~~

服务包可以有不同结构：

~~~text
ServiceCapabilityPackageBody {
    service_runtime_ref
    endpoints
    protocol
    health_contract
    lifecycle
}
~~~

Python 模块包也可以独立定义：

~~~text
PythonModuleCapabilityPackageBody {
    module_content_ref
    exported_symbols
    dependency_manifest
}
~~~

这些包体之间不需要共享全部字段，也不需要强行抽象成同一个大基类。

## 5. Python 继承与线上格式

Python 代码中可以使用继承或 tagged union：

~~~text
CapabilityPackage
├── FunctionCapabilityPackage
├── ServiceCapabilityPackage
└── PythonModuleCapabilityPackage
~~~

但是在线上协议和持久化格式中，应该显式保存 package_type，而不是依赖 Python 类名或反序列化时猜测类型：

~~~json
{
  "package_type": "service",
  "package_version": "3",
  "execution": {
    "kind": "container:http",
    "version": "1"
  },
  "body": {"endpoints": ["..."]}
}
~~~

实现层可以通过 CapabilityPackageRegistry 将 package_type 路由到对应的 parser、validator、provisioner 和 dispatcher。

## 6. 执行模型与包类型必须正交

一个能力包是什么形态，和它由什么执行器承载，是两个不同问题：

~~~text
function + process:json_stdio
function + builtin:function
service  + container:http
service  + container:grpc
service  + container:mcp
~~~

因此不建议只用执行器类型充当 `package_type`。执行契约使用一个 `execution` 对象表达，不再同时暴露 `executor_kind`、`executor_operation` 和 `execution_model`：

~~~text
execution {
    kind
    version
}
~~~

这些信息会影响 provision、启动、健康检查、并发、路由、停止和绑定校验，应该进入受校验的契约，并参与包身份计算，而不是被当作无语义的附加字典。具体执行器所需的额外配置由对应类型的包体或执行器契约定义，不在公共包头中预先泛化。

## 7. 平台需要的最小公共接口

虽然包体可以完全不同，但平台仍需要一组有限的公共操作：

~~~text
parse(package)
describe(package)
validate(package)
provision(package)
dispatch(package, request)
health(package)
~~~

具体实现分别由 FunctionPackageHandler、ServicePackageHandler 等处理。

Observer 不需要理解所有包体内部结构，但能力发现和路由需要一个小的标准化描述投影，例如：

~~~text
CapabilityDescriptor {
    package_ref
    package_type
    operations
    input/output schema summaries
    target requirements
}
~~~

这个 descriptor 只是发现、匹配和准入所需的投影，不应反向要求基础 CapabilityPackage 拥有所有能力类型的完整字段。

## 8. 对后续 service-level 扩展的含义

如果实现服务能力包，服务包体应当自己描述端点：

~~~text
ServiceCapabilityPackageBody {
    endpoints: [
        {
            endpoint_id
            operation_descriptor_ref
            io_contract_ref
            handler_ref
        }
    ]
}
~~~

服务包激活时启动或连接一个长期运行的服务，调用时按 endpoint 路由。ComputeBinding 需要绑定 package 以及具体 endpoint，而不是只绑定 package。

单函数能力可以视为端点数量为一的特殊服务包，或者继续保持独立的 `function` 包体；两者都不需要改变通用包头。

## 9. 设计边界

本方向明确以下边界：

- 不把 CapabilityPackage 设计成所有能力类型共享的巨大 schema；
- 不把 process:json_stdio 等执行契约类型当作能力包类型；
- 不用任意 metadata 代替会影响执行语义的正式契约；
- 不因为服务包未来可能有多个端点，就让当前单函数包体预先承担服务字段；
- 不要求所有能力包都采用 Python 程序或 JSON stdio；
- 继续复用通用的 digest、provenance、scope、publication 和 activation 治理能力。

最终目标是：

> 用一个简单、稳定的 CapabilityPackage 作为资源边界，用类型化包体表达不同能力形态，用执行器注册表承载不同运行机制。
