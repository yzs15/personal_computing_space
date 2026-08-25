# Conversation Persistence and Agent Reply Design

## Goal

Make the Conversation-first experience durable and readable: a real Codex
`agentMessage` must appear as the assistant reply, and refreshing the browser
must restore the conversation from Observer state rather than losing the DOM
timeline.

## Scope and compatibility

This is an M0/M1 usability correction for the current single-user/single-
Workspace deployment. The existing Observer PostgreSQL `runs.events` JSON
column remains the persistence boundary, so the live database requires no
destructive migration. A future driver-db split can project the same message
events into dedicated Conversation/Message tables without changing the browser
contract.

Fake and Codex use the same message path. Fake remains test-only; it emits the
same normalized `assistant_text` event shape used by Codex.

## Data flow

1. `POST /api/v1/messages` receives a `conversation_ref` and creates a run.
2. Observer records a user message event on that run.
3. Coding-agent providers emit normalized `AgentEvent` values. Codex maps
   completed `item` notifications whose type is `agentMessage` to
   `assistant_text`; Driver records each assistant text event and includes the
   concatenated text in the response.
4. Existing plan, commit, execution, and resource events continue to be
   persisted on the same run.
5. `GET /api/v1/conversations` returns distinct conversation summaries.
   `GET /api/v1/conversations/{conversation_ref}` returns ordered message
   events and run summaries. The stream endpoint reads persisted events, so it
   also works after an Observer process restart.
6. The browser loads the runtime status and conversation list on startup,
   selects the most recent conversation, renders its history, and sends later
   prompts with that stable reference. A New conversation action creates an
   in-memory reference; it becomes durable when its first message is sent.

## Message event shape

Message events are allowlisted JSON entries in `RunRecord.events`:

```json
{
  "phase": "message",
  "message_id": "message-opaque-id",
  "role": "user | assistant",
  "content": "opaque user or assistant text",
  "run_id": "run-opaque-id"
}
```

The UI renders `role` and `content` only. It never exposes agent process
metadata, credentials, or raw workspace paths.

## Error behavior

The message route continues to return structured 503 errors for unavailable or
timed-out Codex turns. The browser renders the safe error code instead of the
misleading generic “refining” line. A successful turn with no assistant text
renders a completion notice and remains recoverable from the run history.

## Testing and rollout

- Repository tests verify message events persist and reload through a fresh
  repository instance.
- API tests verify conversation summaries/history and normalized assistant
  output for the Fake provider.
- Codex protocol tests verify `item/completed` `agentMessage` mapping without
  requiring a live model.
- Static UI tests verify conversation controls and history API wiring.
- Run the full pytest suite, Compose config checks, then restart the host
  Observer and perform one real Codex request before handoff.
