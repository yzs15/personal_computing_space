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

The next-stage Term/Capability Package work is intentionally roadmap-only in
this version; an unknown required term fails with a structured capability error.
