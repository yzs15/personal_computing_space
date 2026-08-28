# MinIO Content Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task with verification checkpoints.

**Goal:** Replace the local filesystem content store with one asynchronous, content-addressed MinIO/S3 implementation that preserves immutable digest semantics and keeps backend location out of closure identity.

**Architecture:** `ContentStore` lives at the package boundary and uses a configured S3 client through `asyncio.to_thread`; it never falls back to a filesystem. Objects are created conditionally under `<prefix>/<digest>`, carry SHA-256 metadata, and are verified on read. `ResourceRef` contains only semantic content identity (plus optional media type); bucket/endpoint/key come from each process's store configuration and are excluded from canonical closure/package identity. Observer and Slaves use the same configured bucket, while WorkerSession sends only control references.

**Tech Stack:** Python 3.12+, boto3, moto, asyncio, FastAPI, SQLAlchemy async, PostgreSQL, Docker Compose, MinIO.

---

### Task 1: Add S3 dependencies, settings, and hermetic test fixture

**Files:**
- Modify: `pyproject.toml`
- Modify: `loom_v2/settings.py`
- Create: `tests/conftest.py`
- Test: `tests/content/test_content_store.py`

- [x] **Step 1: Add runtime and test dependencies.** Add `boto3>=1.35,<2` to project dependencies and `moto[s3]>=5,<6` to test extras.
- [x] **Step 2: Add S3 settings.** Add `s3_endpoint_url`, `s3_bucket`, `s3_access_key`, `s3_secret_key`, `s3_region`, and `s3_prefix` with `LOOM_` names; endpoint and credentials have no implicit filesystem fallback.
- [x] **Step 3: Add an autouse moto fixture.** Start `mock_aws()` for each test and set `LOOM_S3_ENDPOINT_URL`, `LOOM_S3_BUCKET`, `LOOM_S3_ACCESS_KEY`, and `LOOM_S3_SECRET_KEY` to deterministic test values; the S3 store creates the test bucket lazily.
- [x] **Step 4: Write failing async tests for the public store contract.** Cover content-addressed refs, no `resource_id` argument, repeated put idempotency, missing configuration rejection, `get`, `exists`, `stat`, expected digest mismatch, and the absence of endpoint/bucket/key in returned `access_binding`.
- [x] **Step 5: Run `pytest -q tests/content/test_content_store.py` and confirm failure because the S3 implementation and `ContentStat` do not exist.**

### Task 2: Implement the immutable asynchronous S3 ContentStore

**Files:**
- Modify: `loom_v2/content_store.py`
- Delete: `loom_v2/observer/content_store.py`
- Modify: `tests/content/test_content_store.py`

- [x] **Step 1: Implement `ContentStat` and configuration validation.** Define `ContentStat(size, declared_digest, media_type, integrity_verified)` and reject missing endpoint, bucket, access key, or secret key with a configuration error.
- [x] **Step 2: Implement lazy boto3 client and bucket initialization.** Create one client per store, ensure the configured bucket exists on first operation, and never log credentials.
- [x] **Step 3: Implement conditional immutable `put`.** Compute SHA-256, derive `<prefix>/<digest>`, `HEAD` existing objects, validate metadata/length/media type, and otherwise issue conditional `put_object` with `IfNoneMatch="*"`, `ContentType`, `ContentLength`, and `x-amz-meta-sha256`; a concurrent precondition failure must re-`HEAD` and validate rather than overwrite.
- [x] **Step 4: Implement `get` with full verification.** Run `get_object`, body read, and body close inside one `asyncio.to_thread` function; compare body digest with the ref and optional expected digest and raise `content_digest_mismatch` on any disagreement.
- [x] **Step 5: Implement `exists` and `stat` error mapping.** Return false/None only for 404/NoSuchKey; preserve access-denied, timeout, and server errors. `stat` reports metadata and whether it agrees with the ref but does not claim body verification.
- [x] **Step 6: Remove the filesystem implementation and update the package export/import path.** All production callers import `loom_v2.content_store.ContentStore`; no filesystem compatibility class remains.
- [x] **Step 7: Run the focused content-store tests and confirm they pass, including a test that a pre-existing object cannot be overwritten.**

### Task 3: Migrate Observer repository and HTTP content access to async S3

**Files:**
- Modify: `loom_v2/observer/repository.py`
- Modify: `loom_v2/observer/app.py`
- Modify: `loom_v2/contracts/types.py`
- Test: `tests/api/test_capability_packages.py`
- Test: `tests/contracts/test_capability_package.py`

- [x] **Step 1: Construct the store from Settings.** Pass all S3 settings into `ObserverRepository` from `observer.app`; keep explicit `content_store` injection for tests.
- [x] **Step 2: Await all repository content operations.** Convert `_evaluate_readiness` to async and await `put`, `exists`, and `stat` in materialization, patch, commit, start, and readiness paths.
- [x] **Step 3: Normalize semantic refs.** Ensure materialized `ResourceRef` contains `content://sha256/<digest>`, digest, identity criterion, and no endpoint/bucket/key access binding; ensure closure canonical digest ignores `access_binding`.
- [x] **Step 4: Update the content endpoint.** Await `content_store.get`, validate the requested digest as `sha256/<digest>`, and map missing content to 404 without exposing backend credentials or locations.
- [x] **Step 5: Adapt capability-package tests to async S3 stores and add a canonical-digest invariance assertion.**
- [x] **Step 6: Run repository/API/content tests and confirm materialize → readiness → commit still passes.**

### Task 4: Migrate Slave and WorkerSession to direct S3 references

**Files:**
- Modify: `loom_v2/slave/service.py`
- Modify: `loom_v2/slave/app.py`
- Modify: `loom_v2/observer/worker.py`
- Modify: `loom_v2/observer/app.py`
- Test: `tests/slave/test_slave_capability_package.py`
- Test: `tests/slave/test_worker_api.py`

- [x] **Step 1: Construct each Slave ContentStore from its process Settings.** Preserve explicit injection for hermetic tests.
- [x] **Step 2: Await `exists`, `stat`, and `get` in provisioning and execution.** Require package/program digest validation before activation or execution.
- [x] **Step 3: Remove `program_bytes_b64` from WorkerSession and Slave HTTP handling.** Provision requests carry package/program refs and digests only; the target Slave resolves content from its configured S3 store.
- [x] **Step 4: Update worker/slave tests to use a shared moto S3 backend and assert no base64 payload is sent.**
- [x] **Step 5: Run all Slave and WorkerSession tests and confirm the HTTP execution boundary remains green.**

### Task 5: Add MinIO to Docker Compose and runtime documentation

**Files:**
- Modify: `deploy/docker-compose.yml`
- Modify: `deploy/docker-compose.test.yml`
- Modify: `scripts/dev-up.sh`
- Modify: `README.md`

- [x] **Step 1: Add the MinIO service.** Use `minio/minio`, persistent `minio-data:/data`, server and console ports, root credentials from environment, and a healthcheck that is available in the image.
- [x] **Step 2: Inject S3 settings into Observer and both Slaves.** Keep the shared bucket/credentials strategy selected by the user, remove `loom-content` volume mounts, and make services depend on MinIO readiness.
- [x] **Step 3: Add a bucket initialization/readiness command.** Ensure `loom-content` exists before application services accept work.
- [x] **Step 4: Update dev-up and README.** Document MinIO as the only content backend, semantic refs, immutable writes, and the required `LOOM_S3_*` variables.
- [x] **Step 5: Render production and test Compose files and verify no filesystem content volume or `program_bytes_b64` path remains.**

### Task 6: End-to-end verification and deployment

**Files:**
- Modify: `tests/integration/test_repositories.py`
- Create: `tests/integration/test_minio_content_store.py`

- [x] **Step 1: Add an integration test for Observer → MinIO → Slave.** Materialize a candidate, verify readiness, provision it through WorkerSession, execute it, and confirm the program digest is readable from both configured stores.
- [x] **Step 2: Add failure tests for tampered metadata/body and missing content.** Assert structured digest/not-found errors and no false readiness.
- [x] **Step 3: Start the Compose MinIO service and run the real capability-gap flow when the image is available; otherwise record the hermetic moto result explicitly.**
- [x] **Step 4: Run `pytest -q`, `python -m compileall -q loom_v2`, both Compose config commands, and `git diff --check`.**
- [x] **Step 5: Restart Observer on port 18080 with `LOOM_S3_*`, check `/healthz` and `/api/v1/runtime`, and run a real Codex echo smoke through the S3-backed execution path.**

## Scope decisions carried from the approved design

- Observer, Slave A, and Slave B use the same configured MinIO bucket and credential strategy.
- Existing `/tmp/loom-v2-content` objects are not migrated.
- Lifecycle/GC, encryption, multipart uploads, and multi-region replication remain outside this implementation.
