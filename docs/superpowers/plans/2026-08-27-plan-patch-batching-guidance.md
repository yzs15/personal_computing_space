# Plan Patch Batching Guidance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Guide the coding agent to combine independent closure-refinement operations into one atomic `loom_apply_plan_patch` call.

**Architecture:** Keep the existing MCP schema and Observer transaction semantics unchanged. Only enrich the dynamic tool description and lock the guidance down with a schema-level regression test; no buffering, staging, or new batch protocol is introduced.

**Tech Stack:** Python 3.12+, FastAPI/Starlette, Pydantic v2, pytest/pytest-asyncio, Codex app-server dynamic tool schemas.

---

### Task 1: Add and verify batching guidance to the dynamic tool description

**Files:**
- Modify: `loom_v2/driver/mcp.py:197-207` — update the `loom_apply_plan_patch` description only.
- Test: `tests/driver/test_mcp.py:40-52` — extend the dynamic tool schema test with guidance assertions.

- [ ] **Step 1: Write the failing test**

Extend `test_driver_mcp_exposes_dynamic_tool_specs` after the existing schema assertions:

```python
    description = patch_spec["description"].lower()
    assert "batch" in description
    assert "atomic" in description
    assert "readiness" in description
```

These assertions verify that the model receives all three pieces of guidance:
batch independent operations, atomic application, and splitting only when
readiness feedback is needed.

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cd .
.venv/bin/pytest -q tests/driver/test_mcp.py::test_driver_mcp_exposes_dynamic_tool_specs
```

Expected: FAIL because the current description only says “Apply one
deterministic closure refinement patch” and does not mention batching or
atomicity.

- [ ] **Step 3: Write the minimal implementation**

Replace the current description in `loom_v2/driver/mcp.py`:

```python
"description": (
    "Apply deterministic closure refinement operations after open_run. "
    "Batch independent operations in one ordered `ops` array whenever possible: "
    "the complete array is applied atomically and creates one draft version, "
    "receipt, and draft_patched event. Split into multiple calls only when an "
    "intermediate readiness result or a prior operation's result is required."
),
```

Do not change the input schema, operation handling, repository calls, or
versioning behavior.

- [ ] **Step 4: Run the focused tests to verify the change**

Run:

```bash
cd .
.venv/bin/pytest -q tests/driver/test_mcp.py
```

Expected: all tests in `tests/driver/test_mcp.py` pass.

- [ ] **Step 5: Run the complete test suite**

Run:

```bash
cd .
.venv/bin/pytest -q
```

Expected: the complete suite passes with zero failures; existing deprecation
warnings may remain.

- [ ] **Step 6: Commit the implementation**

```bash
cd .
git add loom_v2/driver/mcp.py tests/driver/test_mcp.py
git commit -m "feat: guide coding agent to batch plan patches"
```

