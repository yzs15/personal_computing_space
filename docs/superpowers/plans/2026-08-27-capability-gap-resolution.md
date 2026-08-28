# Capability Gap Resolution Implementation Plan

## Goal

Implement the capability-gap design in `docs/superpowers/specs/2026-08-26-capability-gap-resolution-design.md` while preserving the existing single-user Workspace APIs and tests.

## Steps

1. **Capability contracts and content identity**
   - Add immutable Pydantic contracts for `CapabilityPackageVersion`, `CapabilityPackageActivation`, `CapabilityPackage`, provision commands, health reports, and resource events.
   - Add a small content store abstraction that writes program/package bytes by digest and returns `ResourceRef` values.
   - Add focused contract/content-store tests first, then implement the minimum behavior.

2. **Unified Executor Adapter registry**
   - Replace Slave operation `if` branches with an adapter registry and descriptors.
   - Implement `builtin_v1` and constrained `subprocess_json_v1` adapters; keep `http_service_v1`, `grpc_service_v1`, and `mcp_v1` as explicit unsupported extension points.
   - Add tests for descriptor identity, built-ins, subprocess execution, and unsupported adapters.

3. **Observer package candidate, persistence, and promotion**
   - Add package state to the repository and durable `RunRow`/migration fields where needed.
   - Implement `materialize_capability_package_candidate` as a versioned, idempotent patch operation.
   - Add readiness/commit checks for exact bindings, package scope/digest, provider-fillable holes, and target capability descriptors.
   - Implement post-run abandon/promotion as a derived `workspace_reusable` version with idempotency and event provenance.
   - Add repository/API tests for candidate visibility, blockers, and promotion invariants.

4. **Slave package cache, provisioning, and WorkerSession**
   - Add package cache and activation state to `SlaveService`.
   - Implement provision/fetch/verify/install/test/health-report flow and target-specific activation bindings.
   - Add WorkerSession provision and dispatch envelopes; route execution through the adapter registry and package activation.
   - Add slave and worker API tests, including digest mismatch and typed-hole/provider-fillable validation.

5. **Driver MCP and browser/API surface**
   - Extend Driver MCP patch schemas and scoped calls for package materialization and status.
   - Add Observer endpoints to list candidate packages and explicitly abandon/promote them.
   - Ensure the UI can show package candidates, approval summaries, and activation status without automatic promotion.

6. **Verification and documentation**
   - Add an end-to-end capability-gap test: missing application capability, `run_code` refinement, exact binding, execution, and optional promotion.
   - Update README/roadmap with the implemented scope and future adapter roadmap.
   - Run pytest, compileall, and both Docker Compose config checks; report any environment-only limitations.

7. **Conversation deadline and child-operation timeouts**
   - Treat a live app-server Conversation as active while it is quiet; remove the
     fixed idle turn timeout.
   - Enforce a configurable absolute Conversation deadline (24 hours by default).
   - Keep WorkerSession and capability executor timeouts independently configurable
   and return distinct deadline/capability timeout codes.

8. **App-server TurnLifecycle and protocol health**
   - Keep explicit turn/system/goal/process/protocol errors in a TurnLifecycle
     state machine; never infer failure from unchanged content.
   - Run a bounded poll scheduler for `thread/read(includeTurns=true)` and
     `thread/goal/get` (one in-flight request per method).
   - Treat every successful poll response and every valid item/turn/delta,
     token-usage, thread-status, or goal notification as a protocol heartbeat.
   - Report `coding_agent_stalled` only after continuous poll RPC errors or
     missing responses exceed `LOOM_CODING_AGENT_PROTOCOL_FAILURE_SECONDS`
     (default 60 seconds); keep the Driver-owned 24-hour absolute deadline as
     the final safety bound.

## Execution notes

- Follow test-driven development for each step (RED → GREEN → refactor).
- Keep program bodies in the content store; protocol/database records contain refs and digests only.
- Preserve backward compatibility for existing `echo`/`hash`/`sort` flows and existing untracked user files.
