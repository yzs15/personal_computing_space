# Orchestration Pyright Preflight Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox ( - [ ] ) syntax for tracking.

**Goal:** Add one official Pyright preflight for every orchestration readiness evaluation and return precise diagnostics to the coding agent.

**Architecture:** Observer performs a non-executing Pyright check on an isolated temporary copy after verifying immutable program content. A shared entry-signature validator enforces quoted OrchestrationContext/ResourceRef annotations, while Driver keeps its runtime AST and sandbox checks. Readiness failures use DomainErrorEnvelope so local and remote MCP calls expose the same structured blocker payload.

**Tech Stack:** Python 3.12+, official `pyright[nodejs]==1.1.411` PyPI package, asyncio subprocesses, Pydantic v2, FastAPI, pytest/pytest-asyncio, Docker Compose.

The repository instructions prohibit creating commits during this work; implementation leaves changes in the working tree for review.

---

### Task 1: Build the isolated Pyright preflight module

**Files:**
- Create: loom_v2/observer/orchestration_preflight.py
- Modify: pyproject.toml
- Test: tests/observer/test_orchestration_preflight.py

- [x] Step 1: Write failing preflight tests

  Add tests for a valid annotated program, an undefined null name, a bad emit_node argument, a missing entry annotation, an unavailable executable, and a ten-second timeout. Assert the normalized diagnostic fields file, line, column, end_line, end_column, severity, rule, and message.

  Example fixtures:

      VALID = (
          'async def orchestrate(ctx: "OrchestrationContext", '
          'input_ref: "ResourceRef") -> "ResourceRef":\n'
          '    handle = ctx.emit_node({"resource_id": "pkg"}, [input_ref])\n'
          '    return await ctx.result(handle)\n'
      )
      UNDEFINED = VALID.replace('"pkg"', 'null')
      BAD_ARGUMENT = VALID.replace(
          'ctx.emit_node({"resource_id": "pkg"}, [input_ref])',
          'ctx.emit_node(1, [input_ref])',
      )

- [x] Step 2: Run the focused tests and verify they fail

  Run: pytest -q tests/observer/test_orchestration_preflight.py

  Expected: FAIL because the preflight module and dependency are absent.

- [x] Step 3: Add and verify the exact dependency

  Add the official `pyright[nodejs]==1.1.411` dependency to pyproject.toml so the pinned CLI has a build-time Node runtime and never downloads one during readiness. Build the Observer environment and verify `pyright --version` prints 1.1.411. Do not add basedpyright or a version range.

- [x] Step 4: Implement the preflight API

  Expose:

      def validate_entry_signature(source: str) -> list[dict[str, object]]: ...
      async def check_program(source: str, *, timeout_seconds: float = 10.0) -> list[dict[str, object]]: ...

  validate_entry_signature parses only module-level AST nodes, requires exactly one async def orchestrate(ctx, input_ref), and requires string-literal annotations OrchestrationContext and ResourceRef for parameters and return value. It returns stable missing_entry_annotation or invalid_entry_annotation details.

  check_program creates orchestration.py, loom_orchestration_types.pyi, and pyrightconfig.json in TemporaryDirectory. The check copy imports the type stub after the module docstring and future imports; the original source is never modified or executed. Invoke pyright --outputjson --project pyrightconfig.json orchestration.py with cwd set to the temporary directory and a ten-second timeout. Parse generalDiagnostics, convert positions to one-based values, normalize file to orchestration.py, remove the synthetic import line offset, and filter the configured undefined/type rules. Always remove the temporary directory and terminate the child process.

- [x] Step 5: Run the focused tests and verify they pass

  Run: pytest -q tests/observer/test_orchestration_preflight.py

  Expected: all preflight and failure-mode tests pass.

### Task 2: Gate readiness and direct Driver execution

**Files:**
- Modify: loom_v2/observer/repository.py:1936
- Modify: loom_v2/driver/orchestrator.py:279
- Modify: tests/api/test_readiness.py
- Modify: tests/e2e/test_dynamic_distributed_analysis.py:135
- Modify: tests/e2e/test_dynamic_orchestration_stress.py:97

- [x] Step 1: Add readiness regression tests

  Create orchestration packages in ContentStore and assert that null, bad DSL arguments, missing annotations, and invalid syntax produce their documented blockers. Assert each diagnostic retains the exact normalized file, position, rule, and message. Assert failed readiness prevents commit/start from creating an execution attempt.

- [x] Step 2: Run the readiness tests and verify they fail

  Run: pytest -q tests/api/test_readiness.py -k orchestration

  Expected: FAIL because readiness currently only performs content and syntax checks.

- [x] Step 3: Integrate preflight into Observer readiness

  In _evaluate_orchestration_package, fetch digest-verified program bytes once, decode UTF-8, call validate_entry_signature, then await check_program. Append blockers without changing existing package, contract, allowlist, target, budget, media, digest, or Docker availability checks. Do not execute program source in Observer.

- [x] Step 4: Enforce the same signature in DockerOrchestrationExecutor

  Call validate_entry_signature from _validate_program after decoding and before launching Docker. Convert signature failures to orchestration_program_type_error while retaining the current AST/import/side-effect checks.

- [x] Step 5: Annotate the existing orchestration fixtures

  Change both generated orchestration functions to:

      async def orchestrate(
          ctx: "OrchestrationContext",
          input_ref: "ResourceRef",
      ) -> "ResourceRef":

  Keep package/resource references as dicts; the Mapping[str, object] alias must accept them.

- [x] Step 6: Run the readiness and orchestration tests

  Run: pytest -q tests/observer/test_orchestration_preflight.py tests/api/test_readiness.py tests/driver/test_orchestrator.py tests/e2e/test_dynamic_distributed_analysis.py tests/e2e/test_dynamic_orchestration_stress.py

  Expected: all selected tests pass, with Docker integration tests skipped only when the configured image is unavailable.

### Task 3: Preserve precise blockers through MCP

**Files:**
- Modify: loom_v2/observer/repository.py:2128
- Modify: loom_v2/observer/app.py:221
- Modify: loom_v2/driver/control_client.py:55
- Modify: loom_v2/coding_agents/codex.py:667
- Modify: loom_v2/driver/service.py:218
- Test: tests/driver/test_codex_protocol.py
- Test: tests/driver/test_control_client.py
- Test: tests/api/test_readiness.py

- [x] Step 1: Write failing structured-error tests

  Assert that local and remote readiness failures reach the coding agent as:

      {
          "success": False,
          "error": {
              "code": "readiness_blocked",
              "blockers": [
                  {
                      "code": "orchestration_program_unresolved_name",
                      "diagnostics": [
                          {
                              "file": "orchestration.py",
                              "line": 7,
                              "column": 38,
                              "end_line": 7,
                              "end_column": 42,
                              "severity": "error",
                              "rule": "reportUndefinedVariable",
                              "message": "null is not defined",
                          }
                      ],
                  }
              ],
          },
      }

  Also assert generic errors retain the existing fallback code.

- [x] Step 2: Run the transport tests and verify they fail

  Run: pytest -q tests/driver/test_codex_protocol.py tests/driver/test_control_client.py tests/api/test_readiness.py -k structured

  Expected: FAIL because current code converts errors to str(exc).

- [x] Step 3: Make repository and HTTP errors structured

  Change _readiness_error to raise DomainError with a DomainErrorEnvelope whose code is readiness_blocked and whose details contain the complete blockers list. Serialize DomainError envelopes as the HTTP detail object in the internal driver-command, commit, and start routes while preserving status codes.

- [x] Step 4: Reconstruct remote envelopes

  In ObserverControlClient._request, parse a dict HTTP detail containing code, category, and details into DomainError. Keep string details mapped to the existing RuntimeError behavior for unrelated failures.

- [x] Step 5: Preserve the envelope in Codex JSON-RPC

  Catch DomainError separately in _handle_dynamic_tool_call and send success false with the envelope and complete blocker diagnostics. Update local and remote DriverService readiness event paths to pass the structured reason rather than readiness_blocked alone.

- [x] Step 6: Run the structured-error tests

  Run: pytest -q tests/driver/test_codex_protocol.py tests/driver/test_control_client.py tests/api/test_readiness.py -k structured

  Expected: all structured-error tests pass.

### Task 4: Update deployment and agent guidance

**Files:**
- Modify: README.md
- Modify: loom_v2/driver/mcp.py
- Modify: tests/driver/test_mcp.py
- Modify: tests/deploy/test_compose_config.py

- [x] Step 1: Document the required signature and diagnostic payload

  Explain the quoted entry annotations, the exact diagnostic fields, and the repair/retry flow. State that no automatic source rewrite occurs.

- [x] Step 2: Update MCP tool descriptions

  Document that readiness, commit, and start failures return readiness_blocked with the full blocker list and Pyright diagnostics.

- [x] Step 3: Verify the Observer image

  Keep dependency installation through the existing Dockerfile pip install . path. Assert that the built Observer environment contains pyright==1.1.411; do not add Node/npm or basedpyright.

- [x] Step 4: Run deployment and tool tests

  Run: pytest -q tests/driver/test_mcp.py tests/deploy/test_compose_config.py

  Expected: all tool-description and Compose tests pass.

### Task 5: Full verification

**Files:** no additional files

- [x] Step 1: Run the complete Python suite

  Run: pytest -q

  Expected: all existing and new tests pass.

- [x] Step 2: Verify formatting and Compose rendering

  Run: git diff --check && docker compose -f deploy/docker-compose.yml config --quiet

  Expected: both commands exit successfully.

- [ ] Step 3: Run the real dynamic orchestration flow

  Run: ./scripts/test-e2e.sh

  Expected: the two-Slave flow reaches its terminal state, while an intentionally invalid null program returns the complete structured blocker to the coding agent without starting an execution.
