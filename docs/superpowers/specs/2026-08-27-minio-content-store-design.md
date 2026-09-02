# MinIO/S3 内容存储设计（唯一实现，替换本地文件系统）

**日期：** 2026-08-27
**修订：** 2026-08-28（补充对象不可变性/完整性校验，剥离 `ResourceRef` 的部署位置语义）

## 1. 背景与目标

当前内容存储是 `ContentStore` 的本地文件系统实现（[`loom_v2/observer/content_store.py`](../../loom_v2/observer/content_store.py)）：内容寻址、每个 blob 一个文件（文件名 = sha256 digest），默认根目录 `/tmp/loom-v2-content`；跨容器靠 compose 共享 volume `loom-content`，或 provision 时经 `program_bytes_b64` 数据面携带。

问题：

- `/tmp` 易失，容器重建丢内容；
- 强依赖“单机共享目录”假设，无法水平扩展；
- 本地路径会进入 `ResourceRef.access_binding`，机器相关的路径语义不干净；
- 跨容器内容搬运靠 base64 数据面，不是持久存储；
- 旧接口是同步文件系统形态（`put(content, *, media_type, resource_id)`），与异步代码基（FastAPI/asyncpg/httpx）不匹配。

目标：**MinIO（S3 兼容对象存储）成为内容存储的唯一实现，彻底移除本地文件系统实现；接口按 S3 最自然的方式重设计（异步、内容寻址、digest 身份），不迁就旧签名**。闭包、派发信封、`ResourceRef` 格式、能力包协议语义不变（它们由契约决定，不属于本存储接口）；MinIO 容器进入 docker-compose，Observer 与两个 Slave 共享同一 bucket、按 digest 存取。

## 2. 现状基线

- 旧 `ContentStore` 接口：`put(content, *, media_type, resource_id) -> ResourceRef`、`get(ref, expected_digest) -> bytes`、`exists(ref) -> bool`（同步、文件系统）。
- 使用点：`ObserverRepository.materialize_capability_package_candidate`（`put`）、readiness（`exists`）、`GET /api/v1/content/{digest}`（`get`）；`SlaveService.provision`（`exists`）、执行（`get`）；`WorkerSession.provision` 的 `program_bytes_b64` 数据面。
- 依赖现状：`pyproject.toml` 未含 boto3/moto；venv 未安装。
- 部署现状：`deploy/docker-compose.yml` 中 observer/slave-a/slave-b 共享 `loom-content` volume 挂到 `/tmp/loom-v2-content`；`ContentStore` 构造默认本地目录、无环境配置。
- 契约约束（不受本次重设计影响）：`ResourceRef(resource_id="content://sha256/<digest>", version_or_digest=<digest>, identity_criterion="content_digest")`；`CapabilityPackageVersion.package_digest` 计算剥离 `access_binding`。

## 3. 设计

### 3.1 接口重设计（S3 原生形态，异步）

单一实现类 `ContentStore`（S3 后端），接口按“内容寻址对象存储”最自然的方式设计，全部异步：

```python
class ContentStore:
    def __init__(self, *, endpoint_url, bucket, access_key, secret_key,
                 region="us-east-1", prefix=""): ...

    async def put(self, content: bytes, *, media_type: str = "application/octet-stream") -> ResourceRef: ...
    async def get(self, ref: ResourceRef | str, *, expected_digest: str | None = None) -> bytes: ...
    async def exists(self, ref: ResourceRef | str) -> bool: ...
    async def stat(self, ref: ResourceRef | str) -> ContentStat | None: ...
```

设计要点：

- **异步**：调用方全部在 async 上下文（FastAPI 路由、async repository/slave），S3 是网络 I/O；接口 `async def` 与代码基一致。实现用 boto3（同步 SDK）+ `asyncio.to_thread` 包一层，不引入额外异步 SDK 依赖（aioboto3 作为备选，不默认）。
- **内容寻址与不可变性**：`put` 由内容决定身份——`digest = sha256(content)`、`key = <prefix>/<digest>`。同 digest 的重试必须返回同一个引用，但不能依赖 S3 覆盖写实现幂等：首次写入使用条件创建；若对象已存在，必须先校验对象 metadata 中的 SHA-256、长度和 media type，确认一致后才返回成功；任何不一致都返回完整性冲突，禁止覆盖既有对象。并发写入遇到条件创建冲突时，重新 `HEAD` 并执行同样校验。
- **`put` 不再接收 `resource_id`**（旧参数移除）：身份完全由内容推出，调用方无法伪造/指定名字。
- **`get` 读回后本地重算 sha256 校验**：正文必须与 `ref.version_or_digest` 一致；若同时提供 `expected_digest`，它也必须与正文和引用 digest 一致，否则抛 `content_digest_mismatch`。`expected_digest` 用于跨检查（如核对 `package.program_digest`），不能替代正文校验。
- **`exists` 用 HEAD**：`head_object`，404/NoSuchKey 返回 False，不下载内容。
- **新增 `stat`**：返回 `ContentStat{size, declared_digest, media_type, integrity_verified}` 或 None。`HEAD` 只能读取对象长度、Content-Type 和 `x-amz-meta-sha256` 声明，不能把 key 名称本身当作已验证的正文 digest；`integrity_verified` 只有在 metadata/长度与请求 digest 一致时为 true，完整正文校验仍由 `get` 完成。readiness 用它做“存在 + 声明完整性 + 大小”检查，比 `get` 便宜。
- **`ResourceRef` 语义身份与后端位置分离**：`resource_id="content://sha256/<digest>"`、`version_or_digest=<digest>`、`identity_criterion="content_digest"` 保持不变。`access_binding` 不再持久化 bucket、key、endpoint_url 等部署相关位置；ContentStore 根据自身配置从 digest 解析 key，最多保留用于展示的 media type。`access_binding` 整体都不参与 ClosureVersion/TaskClosure canonical digest、package digest 或 provenance 身份计算；若实现需要返回临时解析位置，必须标记为 ephemeral，且不携带 secret。
- 只有一个实现、一个类，不设抽象基类/Protocol（moto 直接 mock 同一类，无需第二实现）。

### 3.2 实现

- boto3 `client("s3", endpoint_url=..., aws_access_key_id=..., aws_secret_access_key=..., region_name=...)`，惰性创建（首次调用时），后续复用。
- `put`：先计算 digest 并 `HEAD`；对象不存在时使用条件创建（`If-None-Match: *` 或等价的 MinIO 条件写入），同时写入 `ContentType=media_type`、`ContentLength` 和 `x-amz-meta-sha256=digest`。若条件写入因并发冲突失败，重新 `HEAD` 并校验 metadata/长度；已存在对象不允许覆盖。
- `get`：在同一个 `asyncio.to_thread` 同步函数中完成 `get_object`、响应体读取和关闭，再在事件循环中重算正文 SHA-256；正文、引用 digest、`expected_digest` 任一不一致都抛 `content_digest_mismatch`。
- `exists` / `stat`：使用 `head_object` 处理 404/NoSuchKey；`stat` 返回 HEAD 的长度、Content-Type、`x-amz-meta-sha256` 及其与引用 digest 的一致性，不把对象 key 当作正文校验结果。
- 可选：构造时校验 bucket 存在，不存在则 `create_bucket`（单用户简化；生产可用预置策略）。

### 3.3 配置（Settings + env）

`Settings` 新增（`LOOM_` 前缀）：

- `s3_endpoint_url: str`（本地开发 `http://localhost:9000`，容器内 `http://minio:9000`）。
- `s3_bucket: str = "loom-content"`。
- `s3_access_key: str`、`s3_secret_key: str`（来自 MinIO 初始化或环境注入）。
- `s3_region: str = "us-east-1"`、`s3_prefix: str = ""`。

约束：

- **无 filesystem 回退选项**；`ContentStore` 仅有一个 S3 实现。
- S3 凭据不写入 Loom 日志、事件、闭包或 UI；`access_binding` 不携带 secret。
- 未配置 `s3_endpoint_url`/凭据时构造 `ContentStore` 直接报配置错误。

### 3.4 调用点改造（同步 → await）

- `ObserverRepository.materialize_capability_package_candidate`：`program_ref = await self.content_store.put(...)`。
- `ObserverRepository._evaluate_readiness`：由同步改为 `async def`（其内部 `exists`/`stat` 需 await），`apply_patch`/`commit`/`start` 相应 await；包内容检查用 `await self.content_store.stat(ref)`。
- `SlaveService.run`：`program = await self.content_store.get(...)`。
- `SlaveService.provision`：`await self.content_store.exists/get(...)`。
- `observer.app` 的 `GET /api/v1/content/{digest}`：`body = await app.state.repo.content_store.get(digest)`。

### 3.5 部署（docker-compose）

- 新增 `minio` 服务：镜像 `minio/minio`，命令 `server /data --console-address :9001`，volume `minio-data:/data`，端口 `9000/9001`，healthcheck 用 `curl -f http://localhost:9000/minio/health/live`。
- `observer`、`slave-a`、`slave-b` 注入：`LOOM_S3_ENDPOINT_URL=http://minio:9000`、`LOOM_S3_BUCKET=loom-content`、访问/密钥。
- **移除 `loom-content` volume 及 `/tmp/loom-v2-content` 挂载**（不再需要）。
- 测试 profile 可加 MinIO（环境允许时）用于真实集成；否则用 moto 做 hermetic 单元测试。

### 3.6 WorkerSession.provision 数据面

- 共享 S3 后，Slave 直接按 `program_content_ref.version_or_digest` 从 S3 读取程序正文并本地校验，**移除 `program_bytes_b64` 数据面携带**。
- provision 流程简化为：命令+包引用 → Slave `await stat/get` 校验 digest → 激活。

### 3.7 兼容与迁移

- `ResourceRef`、闭包、派发信封、能力包协议语义不变（接口重设计不触碰契约层）。
- 本地 `/tmp/loom-v2-content` 已有内容**不迁移、不保留**（一次性数据，MinIO 上线后重新写入）。
- 现有引用旧 `ContentStore` 的测试改为 S3 后端（moto），并适配异步接口（`await`）。

## 4. 测试

- **单元（hermetic，moto）**：`ContentStore`（S3 后端）用 `moto` 模拟 S3，覆盖：
  - `put` 幂等（同内容重复 put 返回同一 digest/引用）、条件创建和既有对象不可覆盖、内容决定身份（无 resource_id 参数）；
  - `get` 命中、篡改内容抛 `content_digest_mismatch`、`expected_digest` 跨检查；
  - `exists`、`stat`（大小 + declared digest + media type + integrity 状态，缺失返回 None）；
  - `content://sha256/<digest>` 身份、bucket/key 布局与 prefix；
  - `ResourceRef.access_binding` 中不含部署 endpoint/bucket/key，且后端位置变化不影响 ClosureVersion/TaskClosure canonical digest；
  - 异步调用形态（pytest-asyncio）。
- **复用现有能力包测试**：把 `ContentStore(tmp_path)` 替换为 moto 包装的 `ContentStore`（同一类、异步接口），覆盖 materialize → readiness → provision → 执行闭环。
- **集成（环境允许）**：compose 起真实 MinIO，observer + slave 直连，跑 capability-gap e2e（缺失能力 → 包物化 → S3 存程序 → provision 激活 → 执行）。当前环境 Docker 可用但镜像拉取可能受限，如受限则标注环境限制并在本地 venv 跑 moto 版本。
- **回归**：现有 `tests/contracts/test_capability_package.py`、`tests/slave/test_slave_capability_package.py` 适配异步接口后保持通过。

## 5. 实施顺序

1. 依赖：`pyproject.toml` 增加 `boto3`（运行时）、`moto`（test）；venv 安装。
2. `Settings` 新增 S3 配置项。
3. 重写 `ContentStore` 为 S3 后端异步实现（`put/get/exists/stat`）+ moto 单元测试；删除 filesystem 实现与顶层 re-export。
4. 调用点改造：repository（含 `_evaluate_readiness` 异步化）、slave、observer 内容端点。
5. `WorkerSession.provision` 移除 `program_bytes_b64` 数据面。
6. docker-compose：加 MinIO 服务、注入 env、移除 `loom-content` volume。
7. 集成验证（真实 MinIO 或 moto 回退），更新 README。

## 6. 范围外（YAGNI）

- 资源注册表 / `resolve`（留待注册表阶段）；
- 大文件分片、生命周期策略、版本控制、跨 region 复制、加密；
- 旧 `/tmp` 内容自动迁移；
- filesystem 实现及其回退路径（彻底移除）；
- 抽象基类/多实现（仅 moto 用于测试同一类）；
- aioboto3（备选，不默认引入）。
