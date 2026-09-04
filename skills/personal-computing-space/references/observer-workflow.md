# Observer Distributed Task Workflow

This reference describes the external user plane. The Observer base URL is
called `OBSERVER_URL`; the local Compose default is
`http://localhost:18080`.

## 1. Check Readiness

Call:

```http
GET {OBSERVER_URL}/healthz
GET {OBSERVER_URL}/api/v1/runtime
```

Continue only when both return HTTP 200. `/api/v1/runtime` identifies the active
Driver and coding-agent backend. HTTP 503 with `driver_unavailable` means the
task cannot currently be planned or executed; report it or retry later. Do not
fall back to Driver or Slave endpoints.

## 2. Prepare the Message

Keep `conversation_ref` stable across a task and its follow-ups. Use a fresh,
opaque `request_id` for every new message, preferably a UUID.

Describe the task in user terms rather than prescribing internal `loom_*`
calls. Include only applicable fields:

```text
Goal: <what must be produced>
Inputs: <workspace-relative paths or immutable content references>
Output: <required artifact and format>
Distribution: <work that should be partitioned across available Slaves>
Constraints: <resource, correctness, side-effect, or recovery limits>
Acceptance criteria: <observable checks for success>
```

Use paths that are visible inside the configured workspace. Never send API
keys, passwords, tokens, private keys, or host-only paths in the message.

If an input must be uploaded rather than read from the workspace, use the
Observer data-plane endpoint:

```http
POST {OBSERVER_URL}/api/v1/content
Content-Type: application/json

{
  "media_type": "application/json",
  "content": {"example": "value"}
}
```

The response is an immutable `ResourceRef`. Mention that reference in the task
message. Do not upload executable programs to direct the internal planner; ask
the Driver's coding agent to materialize any required capability.

## 3. Submit Asynchronously

```http
POST {OBSERVER_URL}/api/v1/messages
Content-Type: application/json

{
  "workspace_id": "workspace-default",
  "conversation_ref": "conversation-<opaque-id>",
  "request_id": "request-<opaque-id>",
  "text": "<task message>"
}
```

The production response is HTTP 202:

```json
{
  "accepted": true,
  "conversation_ref": "conversation-<opaque-id>",
  "request_id": "request-<opaque-id>",
  "status": "accepted"
}
```

`accepted` means the Observer persisted the receipt. It does not mean a Run
started or completed. Receipt states can move through `accepted`, `queued`,
`in_flight`, and `retryable` before reaching `completed`, `failed`, or
`interrupted`.

### Idempotent Retry

If the client cannot tell whether submission succeeded, repeat the same request
with exactly the same `workspace_id`, `conversation_ref`, `request_id`, and
`text`. Never reuse a `request_id` for edited text or another conversation;
Observer returns HTTP 409 `request_id_reused`.

## 4. Poll the Conversation Projection

Poll every 2–5 seconds unless the caller supplies another reasonable interval:

```http
GET {OBSERVER_URL}/api/v1/conversations/{conversation_ref}
```

The projection contains:

- `status`: latest conversation status;
- `messages`: user and assistant messages, with `request_id` correlation;
- `runs`: Run state, outcome, dynamic nodes, packages, and activations;
- `events`: receipt and Run lifecycle evidence.

Find the user message whose `request_id` matches the submitted request. Its
status is authoritative for that turn; do not rely only on the conversation's
latest aggregate status when multiple turns exist.

Interpret statuses as follows:

| Status | Action |
| --- | --- |
| `queued` | Keep polling; Observer owns delivery and retry. |
| `thinking` | Keep polling; Driver's coding-agent turn is active. |
| `executing` | Keep polling; inspect Run/node progress if useful. |
| `completed` | Verify the outcome and acceptance criteria. |
| `awaiting_decision` | Inspect the latest Run outcome; follow section 5. |
| `failed` | Report the exact structured error and evidence. |
| `interrupted` | Report cancellation; do not resubmit automatically. |

Use an explicit caller deadline. Reaching the client deadline does not prove
the server-side turn stopped. Return the `conversation_ref` and `request_id` so
the caller can resume polling later.

## 5. Handle Decisions and Repair

For the latest Run, inspect `outcome.decision`:

- `attestation`: present the result reference, validation evidence, and risks to
  the user. Only after an explicit user decision call:

  ```http
  POST {OBSERVER_URL}/api/v1/runs/{run_id}/resolve
  Content-Type: application/json

  {"decision": "accept"}
  ```

  Use `abandon` instead when the user rejects the result.
- `repair`: do not call `accept`. Send a new corrective message using the same
  `conversation_ref` and a new `request_id`, including the exact failure and the
  requested correction. The Driver's coding agent owns reopening and patching.

Never infer acceptance from silence. Never resolve a decision merely to make a
workflow appear successful.

## 6. Retrieve and Verify Results

Read the latest matching Run from `runs`. Verify at least:

- terminal status and `outcome.disposition`;
- required output fields and domain acceptance criteria;
- `dynamic_nodes` when the task required distributed execution;
- structured validation evidence and terminal errors;
- content digest when the outcome returns a content-addressed reference.

For `content://sha256/<digest>`, retrieve the immutable object with:

```http
GET {OBSERVER_URL}/api/v1/content/{digest}
```

Do not report success solely because an assistant message says the task
completed. Prefer the persisted Run outcome and evidence.

## 7. Cancel Only When Requested

```http
POST {OBSERVER_URL}/api/v1/conversations/{conversation_ref}/interrupt
```

Use this only for an explicit cancellation request or a caller policy that was
agreed in advance. HTTP 409 `conversation_not_active` means there is no active
turn to cancel.

## Prohibited External Interfaces

External coding agents must not call:

- Observer `/mcp`;
- Driver `/driver/v1/*`;
- Observer `/internal/v1/*`;
- Slave `/worker/v1/*`;
- direct Run open, patch, commit, or start endpoints.

These interfaces expose internal planning, control, or worker protocols. Their
presence in source code or tests does not make them part of the external agent
contract.
