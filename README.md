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

For the local experience, start the four isolated PostgreSQL instances, Observer,
and both Slaves with `scripts/dev-up.sh`. The default runtime setting remains
Codex app-server with model `deepseek-v4-flash`; it reads the host Codex CLI
configuration and never copies credentials into Loom. If the local app-server or
model is unavailable, the UI reports `coding_agent_unavailable` rather than
silently switching to Fake.

The host Driver allows up to 90 seconds per Codex turn by default
(`LOOM_CODING_AGENT_TIMEOUT_SECONDS` can override this) and reports a retryable
`coding_agent_timeout` if the local upstream does not finish in that window.

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
`completed`, `interrupted`, or `failed`) in both conversation projections. The
workspace refreshes the selected conversation while a turn is active and shows
an Interrupt button. `POST /api/v1/conversations/{conversation_ref}/interrupt`
requests cancellation of the active Codex turn; it leaves persisted user,
assistant, draft, and event history intact, marks the run `cancelled`, and
reports the conversation as `interrupted`. If no turn is active, the endpoint
returns HTTP 409 with `conversation_not_active`.

The next-stage Term/Capability Package work is intentionally roadmap-only in
this version; an unknown required term fails with a structured capability error.
