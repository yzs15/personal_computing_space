# Loom v2

Python implementation of the single-user/single-Workspace task-closure runtime.

The canonical contract lives in `loom_v2/contracts`; the Observer is the state
authority, Driver is the single conversation writer, and each Slave owns an
isolated WorkspaceReplica and PostgreSQL ledger. The default runtime backend is
the local Codex app-server with model `deepseek-v4-flash`; Fake is selected
explicitly by the deterministic test profile.

Run the unit tests with:

```bash
.venv/bin/pytest -q
```

Validate the Compose definitions with:

```bash
docker compose -f deploy/docker-compose.yml config
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.test.yml --profile test config
```

The deterministic test profile uses Fake coding-agent:

```bash
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.test.yml --profile test up --build --abort-on-container-exit --exit-code-from driver
```

For the local experience, create Compose secret files and start the complete
stack with `scripts/dev-up.sh`:

```bash
mkdir -p secrets
openssl rand -hex 32 > secrets/internal_api_secret
chmod 600 secrets/internal_api_secret
./scripts/dev-up.sh
```

Observer is the only public API entry point at `http://localhost:18080`; Driver
and both Slaves stay on the private Compose network. Driver owns the Codex CLI, Docker CLI and Docker socket. Codex state is
kept in the `driver-codex-state` named volume. The default Codex base URL is
`http://host.docker.internal:8787`; override `LOOM_CODEX_BASE_URL` for another
OpenAI-compatible endpoint. A Codex API key is optional and is only needed when
the configured upstream requires authentication. Driver's generated
`config.toml` follows the host Codex provider shape (`model_provider = "proxy"`,
`wire_api = "responses"`, `model_reasoning_effort = "xhigh"`,
`approvals_reviewer = "guardian_subagent"`, and
`sandbox_mode = "danger-full-access"`). The host model catalog is mounted from
`LOOM_CODEX_MODEL_CATALOG_PATH` (default `../../.codex/model_catalog.json`) so
custom model metadata such as `deepseek-v4-flash` remains available. Set
`CODEX_VERSION` when rebuilding the Driver image.

The Driver treats a live Codex app-server conversation as active even when
the app-server is temporarily quiet.  A single conversation has a 24-hour
absolute deadline by default (`LOOM_CODING_AGENT_DEADLINE_SECONDS`); expiry is
reported as the retryable `coding_agent_deadline_exceeded`.  Child operations
are bounded independently: WorkerSession HTTP calls use
`LOOM_WORKER_OPERATION_TIMEOUT_SECONDS` (default 90 seconds), while the
`subprocess_json_v1` capability executor uses
`LOOM_CAPABILITY_OPERATION_TIMEOUT_SECONDS` (default 30 seconds).  These child
timeouts do not turn into coding-agent idle timeouts.  When Observer forwards
`POST /api/v1/messages` to the Driver it accepts the request asynchronously
and returns `202` immediately; the Driver turn keeps running in the background
up to `LOOM_OBSERVER_FORWARD_TIMEOUT_SECONDS` (default 86400 seconds) while the
client polls the conversation projection for the outcome.

While a turn is `inProgress`, the Codex provider also polls
`thread/read(includeTurns=true)` and `thread/goal/get` (every
`LOOM_CODING_AGENT_POLL_INTERVAL_SECONDS`, default 5 seconds).  The provider
separates turn lifecycle from protocol health: every successful poll response,
delta, item/turn notification, token-usage update, and thread-status update is
a heartbeat even when the returned snapshot is unchanged.  Quiet long
generation is therefore allowed.  Only a continuous RPC failure or missing
poll response longer than `LOOM_CODING_AGENT_PROTOCOL_FAILURE_SECONDS`
(default 60 seconds) is reported as `coding_agent_stalled`; the Driver's
24-hour absolute deadline remains the final safety bound.  `waitingOnUserInput`
and `waitingOnApproval` are normal thinking states.

Content is stored only in MinIO/S3. Observer and both Slaves resolve the same
immutable `content://sha256/<digest>` references from their configured bucket;
no filesystem volume or `program_bytes_b64` transfer is used. The Compose stack
starts MinIO on private S3 and console endpoints, initializes the
`loom-content` bucket, and injects `LOOM_S3_ENDPOINT_URL`, `LOOM_S3_BUCKET`,
`LOOM_S3_ACCESS_KEY`, `LOOM_S3_SECRET_KEY`, `LOOM_S3_REGION`, and optional
`LOOM_S3_PREFIX`. Writes are conditional and existing objects are never
overwritten; reads verify the SHA-256 digest.

There is one public content upload operation: `loom_put_content` (also
`POST /api/v1/content`). Upload JSON Schema and `io.v1` contract documents
before refining a closure, then place only their `ResourceRef` values in the
closure. `set_execution_payload` likewise accepts only an input
`ResourceRef`/`NodeInputBinding`; raw payloads and inline program bodies are
not executable inputs. The I/O validator uses a strict, local JSON Schema
2020-12 subset (`type`, `required`, `properties`, `items`, `enum`, `const`,
`minimum`, `maximum`) and rejects unsupported keywords instead of ignoring
them.

`open_run` is a coding-agent decision: after clarifying the user's goal, the
agent calls it through the conversation-scoped Driver MCP surface. The Driver
injects and verifies the conversation/user/Workspace identity, while Observer
persists the immutable high-level `ClosureContract`. Driver does not infer or
silently open a Run from a prompt, and it does not auto-commit or auto-start a
Run that the agent has only opened/refined.

Codex app-server receives the Driver tools through its native per-thread
`dynamicTools` registration and handles `item/tool/call` requests. A standard
MCP JSON-RPC HTTP transport is also available at `POST /mcp`; send the
conversation scope in `X-Loom-Conversation-Ref` and use `tools/list` or
`tools/call`.

After `start_run`, Driver dispatches the locked closure over `WorkerSession` to
`POST /worker/v1/dispatch` on the selected Slave. The Compose deployment uses
private service-to-service HTTP; the envelope carries attempt/execution IDs and epoch, and
returns a dispatch acknowledgement plus terminal report. Each Slave has its
own PostgreSQL ledger and exposes `/worker/v1/capabilities`.

The Conversation-first UI persists user and assistant messages in Observer run
events. `GET /api/v1/conversations` lists saved conversations and
`GET /api/v1/conversations/{conversation_ref}` restores their message/run
history after a browser refresh. Codex `agentMessage` events and Fake test
messages use the same normalized `assistant_text` path.

`POST /api/v1/messages` is receipt-first and always returns quickly with
`202 {"accepted": true, "conversation_ref": ..., "request_id": ..., "status": ...}`.
The Observer's single recoverable dispatcher delivers the persisted receipt to
the active Driver; `status` progresses through `accepted`, `queued`, and
`in_flight` before `completed`, `failed`, or `interrupted`. Repeating the same
`(workspace_id, request_id)` with a different payload returns `409
request_id_reused`; a matching retry never creates another Codex turn. Poll
`GET /api/v1/conversations/{conversation_ref}` (or its stream endpoint) for the
authoritative receipt and assistant result. The Driver serializes each
conversation FIFO and uses one global Codex lane.

Each conversation exposes a derived status (`idle`, `thinking`, `executing`,
`completed`, `awaiting_decision`, `interrupted`, or `failed`) in both
conversation projections. The
workspace refreshes the selected conversation while a turn is active and shows
an Interrupt button. `POST /api/v1/conversations/{conversation_ref}/interrupt`
requests cancellation of the active Codex turn; it leaves persisted user,
assistant, draft, and event history intact, marks the run `cancelled`, and
reports the conversation as `interrupted`. If no turn is active, the endpoint
returns HTTP 409 with `conversation_not_active`.

Capability-gap refinement is implemented through immutable, content-addressed
`CapabilityPackageVersion` candidates. A coding-agent can materialize a
`run_bound/candidate` package, bind it to an exact Slave and execute it through
the `subprocess_json_v1` adapter. Candidates never enter the Workspace
capability snapshot automatically. After a terminal Run, the user can inspect
`GET /api/v1/capability-packages` and explicitly promote or abandon a candidate;
promotion derives a `workspace_reusable/published` version and optional
provisioning returns a health report and activation evidence. Unknown required
terms still fail with a structured capability error; installing new term
support remains a later roadmap item.

Dynamic distributed runs use a content-addressed `orchestrator_python_v1`
capability package. The Driver executes its single-file `orchestrate(ctx,
input_ref)` program in a per-run Docker sandbox (`--network none`, read-only
rootfs, non-root UID, dropped capabilities). The restricted context exposes
only `read_json`, `emit_node`, and `result`; every emitted `NodeIntent` is
validated against the package allowlist and materialized by Observer as a
`DynamicNode`. Child outputs are canonical JSON objects in ContentStore and
are replayed from persisted node events after a Driver restart. Configure the
sandbox image and limits with `LOOM_ORCHESTRATOR_IMAGE`,
`LOOM_ORCHESTRATOR_MEMORY`, `LOOM_ORCHESTRATOR_CPUS`,
`LOOM_ORCHESTRATOR_PIDS_LIMIT`, `LOOM_ORCHESTRATOR_MAX_PROGRAM_BYTES`, and
`LOOM_ORCHESTRATOR_MAX_MESSAGE_BYTES`, plus the wall-clock
`LOOM_ORCHESTRATOR_TIMEOUT_SECONDS`. The production Driver image includes the
Docker CLI and requires access to the host Docker socket for sandbox launches.
Observer has no Docker or Codex runtime dependency and starts even when Driver
or Slaves are not registered.

Before an orchestration package can be committed or started, Observer runs the
official pinned `pyright[nodejs]==1.1.411` CLI once against an isolated copy. The entry
point must use quoted annotations for `OrchestrationContext` and `ResourceRef`:

```python
async def orchestrate(ctx: "OrchestrationContext", input_ref: "ResourceRef") -> "ResourceRef":
    ...
```

Undefined names and selected type errors are returned as a
`readiness_blocked` error with complete `orchestration.py` diagnostics (rule,
message, and one-based source positions), so the coding agent can repair the
source directly. The source is never rewritten or executed during readiness;
Driver still performs its Docker AST and runtime checks.
