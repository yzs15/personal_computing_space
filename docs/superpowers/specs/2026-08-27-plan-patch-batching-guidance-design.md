# Plan Patch Batching Guidance Design

## Goal

Reduce unnecessary `loom_apply_plan_patch` calls by explicitly guiding the
coding agent to send independent refinement operations in one atomic `ops`
array, without changing the Observer transaction model or adding server-side
staging.

## Scope

The change is limited to the dynamic MCP tool description exposed by
`DriverMCP.tool_specs()`:

- Explain that one call may contain multiple ordered operations.
- Tell the agent that the complete `ops` array is applied atomically and
  creates one draft version, receipt, and `draft_patched` event.
- Recommend splitting calls only when an intermediate readiness result or a
  prior operation's result is required.

The existing `ops` schema, operation ordering, CAS checks, idempotency, and
readiness response remain unchanged.

## Non-goals

- No debounce or server-side patch staging.
- No new MCP tool or batch identifier.
- No change to version history or audit granularity for calls the agent still
  chooses to split.
- No modification to Fake or Codex protocol routing.

## Implementation

Modify `loom_v2/driver/mcp.py` at the `loom_apply_plan_patch` tool description.
Add a regression assertion in `tests/driver/test_mcp.py` that the description
mentions batching, atomic application, and the readiness-dependent split rule.

## Success criteria

1. The generated dynamic tool schema contains actionable batching guidance.
2. Existing MCP and repository behavior remains unchanged.
3. The MCP test module and the complete test suite pass.
