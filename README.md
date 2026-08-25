# Loom v2

Python implementation of the single-user/single-Workspace task-closure runtime.

The canonical contract lives in `loom_v2/contracts`; the Observer is the state
authority, Driver is the single conversation writer, and each Slave owns an
isolated WorkspaceReplica and PostgreSQL ledger. The default test backend is
Fake; the host experience can use the local Codex app-server with model
`deepseek-v4-flash`.

Run the unit tests with:

```bash
.venv/bin/pytest -q
```
