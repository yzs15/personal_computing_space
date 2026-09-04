---
name: personal-computing-space
description: Use when a coding agent needs to submit, monitor, continue, resolve, or cancel a distributed task through a deployed Personal Computing Space (Loom) Observer.
---

# Personal Computing Space

Use the Observer as the only external entry point. Act as a user of the
workspace; let the Driver's coding agent refine the closure, invoke its internal
tools, orchestrate work, and dispatch nodes to Slaves.

## Boundary

- Submit work with `POST /api/v1/messages`.
- Observe work with `/api/v1/conversations/*` and `/api/v1/runs/*` reads.
- Use `POST /api/v1/conversations/{conversation_ref}/interrupt` only to cancel.
- Resolve `awaiting_decision` only after presenting the evidence and obtaining
  the user's explicit `accept` or `abandon` decision.
- Never call Observer `/mcp`, Driver `/driver/v1/*`, Observer `/internal/v1/*`,
  or Slave `/worker/v1/*`. Those are internal control surfaces.
- Never use direct Run mutation endpoints to open, patch, commit, or start a
  Run. The Driver's coding agent owns those decisions.

## Workflow

1. Check `GET /healthz` and `GET /api/v1/runtime`. Stop and report
   `driver_unavailable` rather than bypassing the Observer.
2. Reuse one stable `conversation_ref` for follow-up turns. Generate a new
   opaque `request_id` for each semantic message.
3. Write a task message that states the goal, inputs visible to the workspace,
   required output, distribution requirement, constraints, and acceptance
   criteria. Do not include secrets.
4. Submit the message and treat HTTP `202` as receipt acceptance, not task
   completion.
5. Poll `GET /api/v1/conversations/{conversation_ref}` with a bounded interval.
   Correlate the user message by `request_id`; inspect its status, assistant
   reply, Run outcome, dynamic nodes, and evidence.
6. On `completed`, verify the requested acceptance criteria from the outcome.
   On `awaiting_decision`, follow the decision rules in the protocol reference.
   On `failed` or `interrupted`, report the structured reason without hiding it.
7. For corrections or repair, send a new message in the same conversation with
   a new `request_id`; do not mutate internal Run state directly.

Retries after an uncertain HTTP result must reuse the same `request_id` with
the exact same `conversation_ref` and text. Reusing it with different content is
an error.

Read [references/observer-workflow.md](references/observer-workflow.md) before
calling the Observer. It contains request shapes, polling rules, decision
handling, result retrieval, and a distributed-task prompt template.
