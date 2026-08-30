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

For the local experience, start the four isolated PostgreSQL instances and both
containerized Slaves with `scripts/dev-up.sh`; the Observer runs on the host so
it can access the local Codex app-server. The default host port is 18080
(`LOOM_OBSERVER_PORT` can override it). The default runtime setting remains
Codex app-server with model `deepseek-v4-flash`; it reads the host Codex CLI
configuration and never copies credentials into Loom. If the local app-server or
model is unavailable, the UI reports `coding_agent_unavailable` rather than
silently switching to Fake.

The host Driver treats a live Codex app-server conversation as active even when
the app-server is temporarily quiet.  A single conversation has a 24-hour
absolute deadline by default (`LOOM_CODING_AGENT_DEADLINE_SECONDS`); expiry is
reported as the retryable `coding_agent_deadline_exceeded`.  Child operations
are bounded independently: WorkerSession HTTP calls use
`LOOM_WORKER_OPERATION_TIMEOUT_SECONDS` (default 90 seconds), while the
`subprocess_json_v1` capability executor uses
`LOOM_CAPABILITY_OPERATION_TIMEOUT_SECONDS` (default 30 seconds).  These child
timeouts do not turn into coding-agent idle timeouts.

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
starts MinIO on ports 9000 (S3) and 9001 (console), initializes the
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
`POST /worker/v1/dispatch` on the selected Slave. The development deployment
uses loopback HTTP; the envelope carries attempt/execution IDs and epoch, and
returns a dispatch acknowledgement plus terminal report. Each Slave has its
own PostgreSQL ledger and exposes `/worker/v1/capabilities`.

The Conversation-first UI persists user and assistant messages in Observer run
events. `GET /api/v1/conversations` lists saved conversations and
`GET /api/v1/conversations/{conversation_ref}` restores their message/run
history after a browser refresh. Codex `agentMessage` events and Fake test
messages use the same normalized `assistant_text` path.

Each conversation exposes a derived status (`idle`, `thinking`, `executing`,
`completed`, `decision_required`, `interrupted`, or `failed`) in both
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
