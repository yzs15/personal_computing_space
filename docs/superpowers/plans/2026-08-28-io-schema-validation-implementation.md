# Capability I/O Schema Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (or superpowers:subagent-driven-development) to implement this plan task-by-task with verification checkpoints.

**Goal:** Implement the approved `io.v1` contract so JSON inputs and outputs are validated deterministically with a strict JSON Schema 2020-12 subset before a Run can become ready or completed.

**Architecture:** `jsonschema.Draft202012Validator` remains the standards engine, while `loom_v2/contracts/io_schema.py` owns Loom's allowlist, error envelope, stable ordering, and no-remote-reference policy. Pydantic models represent fixed Loom contracts; schema and IoContract JSON are canonicalized and stored in the existing MinIO/S3 ContentStore. Observer performs authoritative readiness and terminal checks, and Slave performs admission/terminal checks with the same pure validator.

**Tech Stack:** Python 3.12+, `json` (syntax parsing), `jsonschema>=4.23,<5`, Pydantic v2, FastAPI, SQLAlchemy async, MinIO/S3 ContentStore, pytest/pytest-asyncio/moto.

---

### Task 1: Add the contract models and dependency boundary

**Files:**
- Modify: `pyproject.toml`
- Modify: `loom_v2/contracts/types.py`
- Modify: `loom_v2/contracts/__init__.py`
- Modify: `loom_v2/db/models.py` (only if a JSONB migration is required by the selected persistence backend)
- Test: `tests/contracts/test_io_contract.py`

- [x] **Step 1: Write failing model tests.** Add tests that construct an `IoContract` with nullable input/output `ResourceRef`, reject extra fields, and verify `CapabilityPackageVersion.io_contract_ref` and `NodeInputBinding.input_ref` round-trip through `model_dump(mode="json")`.

- [x] **Step 2: Run the model tests to verify the failure.**

  Run: `pytest -q tests/contracts/test_io_contract.py`

  Expected: FAIL because `IoContract` and `NodeInputBinding` do not yet exist and `CapabilityPackageVersion` has no `io_contract_ref` field.

- [x] **Step 3: Add the dependency and models.** Add `"jsonschema>=4.23,<5"` to `[project].dependencies`. Define `IoContract` with exactly `schema_version`, `input_schema_ref`, `output_schema_ref`, `success_semantics`, and `success_validator_ref`; define `NodeInputBinding` with `node_id`, `input_ref`, and `provenance`. Change the executable package model to carry `io_contract_ref: ResourceRef`, and expose `ProgramApplication.input_schema`, `output_schema`, and `success_semantics` as derived/read-only views rather than independent executable fields. Export the new models from `loom_v2.contracts`.

- [x] **Step 4: Run the model tests to verify they pass.**

  Run: `pytest -q tests/contracts/test_io_contract.py`

  Expected: PASS.

- [x] **Step 5: Run the existing contract suite.**

  Run: `pytest -q tests/contracts`

  Expected: PASS, or a targeted list of compatibility failures to resolve before continuing; no test should silently accept an inline schema or an unknown model field.

### Task 2: Build the strict JSON Schema adapter with TDD

**Files:**
- Create: `loom_v2/contracts/io_schema.py`
- Test: `tests/contracts/test_io_schema.py`

- [x] **Step 1: Write the failing validator tests.** Cover:

  ```python
  import json
  import pytest

  from loom_v2.contracts.io_schema import validate

  SCORE_SCHEMA = {
      "type": "object",
      "required": ["scores"],
      "properties": {
          "scores": {
              "type": "array",
              "items": {"type": "number", "minimum": 0, "maximum": 100},
          }
      },
  }

  def test_valid_value_has_no_errors():
      value = json.loads('{"scores":[80, 90]}')
      assert validate(SCORE_SCHEMA, value) == []

  def test_extra_payload_wrapper_reports_required_scores():
      errors = validate(SCORE_SCHEMA, {"payload": {"scores": [80]}})
      assert [(error.path, error.keyword) for error in errors] == [("$", "required")]
      assert "payload" not in errors[0].message

  def test_type_items_enum_and_range_errors_are_structured():
      schema = {"type": "array", "items": {"type": "number", "enum": [1, 2], "minimum": 1, "maximum": 2}}
      errors = validate(schema, ["bad", 3])
      assert all(error.keyword in {"type", "enum", "minimum", "maximum"} for error in errors)
      assert all(hasattr(error, "path") and hasattr(error, "expected") for error in errors)

  @pytest.mark.parametrize("schema", [
      {"type": "object", "additionalProperties": False},
      {"$ref": "#/definitions/x"},
      {"type": "string", "format": "email"},
  ])
  def test_unsupported_keywords_are_rejected_instead_of_ignored(schema):
      with pytest.raises(ValueError, match="schema_unsupported_keyword"):
          validate(schema, {})

  def test_invalid_schema_is_rejected_by_draft_2020_12_meta_validation():
      with pytest.raises(ValueError, match="schema_invalid"):
          validate({"type": "not-a-json-schema-type"}, {})

  def test_error_order_is_stable_and_payload_is_not_in_error_text():
      schema = {"type": "object", "required": ["a", "b"], "properties": {"a": {"type": "string"}, "b": {"type": "number"}}}
      first = validate(schema, {"secret": "do-not-leak"})
      second = validate(schema, {"secret": "do-not-leak"})
      assert [error.model_dump() for error in first] == [error.model_dump() for error in second]
      assert "do-not-leak" not in repr(first)
  ```

- [x] **Step 2: Run the focused tests and verify the red state.**

  Run: `pytest -q tests/contracts/test_io_schema.py`

  Expected: FAIL during collection because `loom_v2.contracts.io_schema` is not implemented yet. Once the dependency is installed, the remaining failure must be the missing adapter API, not a malformed test.

- [x] **Step 3: Implement the minimal adapter.** Define an immutable Pydantic `ValidationError(path, keyword, message, expected, observed)` model and a `validate(schema, value)` function. Recursively enforce the allowlist `type`, `required`, `properties`, `items`, `enum`, `const`, `minimum`, and `maximum`; reject `$ref`, `format`, and every other keyword with `ValueError("schema_unsupported_keyword")`. Call `Draft202012Validator.check_schema(schema)` and map `SchemaError` to `ValueError("schema_invalid")`. Use `iter_errors`, convert absolute paths to `$`-prefixed JSON paths, omit instance values from messages, and sort by `(path, keyword, message)`.

- [x] **Step 4: Run the focused tests and verify green.**

  Run: `pytest -q tests/contracts/test_io_schema.py`

  Expected: PASS with deterministic error order and no payload text in errors.

- [x] **Step 5: Run the full contract suite.**

  Run: `pytest -q tests/contracts`

  Expected: PASS.

### Task 3: Canonical content upload and the single `loom_put_content` entry point

**Files:**
- Modify: `loom_v2/content_store.py`
- Modify: `loom_v2/observer/app.py`
- Modify: `loom_v2/driver/mcp.py`
- Modify: `loom_v2/driver/mcp_server.py`
- Modify: `loom_v2/observer/repository.py`
- Test: `tests/content/test_content_store.py`
- Test: `tests/driver/test_mcp.py`
- Test: `tests/api/test_content.py`

- [x] **Step 1: Write failing upload and canonicalization tests.** Verify that JSON media types are parsed with `json.loads`, canonicalized before hashing, stored with `application/schema+json`, `application/vnd.loom.io-contract+json`, or the declared program/artifact media type, and return a `content://sha256/<digest>` `ResourceRef`. Verify repeated puts return the same ref and no endpoint/bucket/key appears in the ref.

- [x] **Step 2: Run the focused tests and verify red.**

  Run: `pytest -q tests/content/test_content_store.py tests/driver/test_mcp.py tests/api/test_content.py`

  Expected: FAIL because `loom_put_content` is not registered and the API endpoint is missing.

- [x] **Step 3: Implement the one public upload path.** Add a repository method that accepts bytes plus `media_type`, parses and validates JSON Schema/IoContract media types, applies the existing JCS canonicalization helper, and delegates immutable storage to `ContentStore.put`. Add the corresponding Driver MCP tool and an HTTP endpoint that returns only the semantic `ResourceRef`. Do not add `loom_put_schema`, `loom_put_program`, or other specialized public put tools; `ContentStore.put` remains an internal Python method.

- [x] **Step 4: Run the focused content/MCP/API tests.**

  Run: `pytest -q tests/content/test_content_store.py tests/driver/test_mcp.py tests/api/test_content.py`

  Expected: PASS.

### Task 4: Enforce IoContract and input bindings during readiness, commit, and start

**Files:**
- Modify: `loom_v2/observer/repository.py`
- Modify: `loom_v2/driver/tools.py`
- Modify: `loom_v2/driver/mcp.py`
- Modify: `loom_v2/contracts/types.py`
- Test: `tests/api/test_readiness.py`
- Test: `tests/api/test_runtime.py`

- [x] **Step 1: Write failing lifecycle tests.** Add cases for an input schema with no `NodeInputBinding` (`payload_missing`), an input ref whose stored JSON is `{"payload": {"scores": [...]}}` (`payload_schema_mismatch`), a correct input ref that reaches ready, and commit/start rejection while blockers remain.

- [x] **Step 2: Run the focused readiness tests and verify red.**

  Run: `pytest -q tests/api/test_readiness.py tests/api/test_runtime.py`

  Expected: FAIL because readiness currently ignores IoContract content and still accepts `metadata.execution_payload`.

- [x] **Step 3: Implement one asynchronous readiness path.** Resolve and digest-check the IoContract and schema refs through `ContentStore.get`, locate the node's `NodeInputBinding`, reject missing refs, run `io_schema.validate`, and emit allowlisted blocker details. Make `apply_patch`, `inspect_readiness`, `commit`, and `start` all call this same `_evaluate_readiness`; change `set_execution_payload` to accept only `input_ref` and remove the bare JSON value path.

- [x] **Step 4: Run readiness, commit, and start tests.**

  Run: `pytest -q tests/api/test_readiness.py tests/api/test_runtime.py`

  Expected: PASS, including no false `ready` state for missing or mismatched inputs.

### Task 5: Require and compare IoContract on capability package materialization/binding

**Files:**
- Modify: `loom_v2/observer/repository.py`
- Modify: `loom_v2/contracts/types.py`
- Test: `tests/api/test_capability_packages.py`
- Test: `tests/contracts/test_capability_package.py`

- [x] **Step 1: Write failing package tests.** Verify materialization without `io_contract_ref` returns `io_contract_required`; verify closure/package input schema ref, output schema ref, success semantics, and validator ref must be exactly equal; verify program bytes are no longer accepted by `materialize_capability_package_candidate`, only a pre-uploaded `program_content_ref` is accepted.

- [x] **Step 2: Run package tests and verify red.**

  Run: `pytest -q tests/api/test_capability_packages.py tests/contracts/test_capability_package.py`

  Expected: FAIL because materialization still accepts inline program content and does not persist/compare the IoContract reference.

- [x] **Step 3: Implement strict package checks.** Require `io_contract_ref`, load and validate its `io.v1` body, remove inline program handling, set the package field to the exact ref, and compare all IoContract fields in `bind_compute_hole`/readiness. Keep package identity content-addressed and exclude deployment bindings.

- [x] **Step 4: Run package tests and the existing capability flow.**

  Run: `pytest -q tests/api/test_capability_packages.py tests/contracts/test_capability_package.py tests/driver/test_fake_flow.py`

  Expected: PASS.

### Task 6: Validate Slave terminal output and Observer evidence acceptance

**Files:**
- Modify: `loom_v2/slave/service.py`
- Modify: `loom_v2/slave/executor.py`
- Modify: `loom_v2/slave/app.py`
- Modify: `loom_v2/observer/worker.py`
- Modify: `loom_v2/observer/repository.py`
- Modify: `loom_v2/observer/app.py`
- Test: `tests/slave/test_execution.py`
- Test: `tests/api/test_observer_run.py`

- [x] **Step 1: Write failing terminal/evidence tests.** Cover output schema pass with `ValidationEvidence`, output schema mismatch producing a failed Attempt/ExecutionOutcome and `decision_required(validation_failed)`, validator plugin pass/fail, and no plugin producing `attestation_required` rather than a guessed success result.

- [x] **Step 2: Run terminal tests and verify red.**

  Run: `pytest -q tests/slave/test_execution.py tests/api/test_observer_run.py`

  Expected: FAIL because Slave currently returns an unvalidated `ExecutionResult` and `record_result` unconditionally marks the Run completed.

- [x] **Step 3: Implement terminal validation and evidence.** After executor JSON decoding, resolve the package IoContract, validate `output_schema_ref`, run the fixed `validator.v1` plugin when present, and create a `ValidationEvidence` containing only refs/digests/errors. Reject stale attempt/epoch/evidence in Observer; on validation failure persist failed Attempt/ExecutionOutcome and project the Run to `decision_required(validation_failed)`.

- [x] **Step 4: Run Slave/Observer tests.**

  Run: `pytest -q tests/slave/test_execution.py tests/api/test_observer_run.py`

  Expected: PASS.

### Task 7: Remove legacy payload paths and complete integration verification

**Files:**
- Modify: `loom_v2/observer/repository.py`
- Modify: `loom_v2/driver/service.py`
- Modify: `loom_v2/slave/service.py`
- Modify: `README.md`
- Test: `tests/integration/test_repositories.py`
- Test: `tests/integration/test_minio_content_store.py`
- Test: `tests/integration/test_io_schema_e2e.py`
- Test: `tests/e2e/test_reassignment.py`

- [x] **Step 1: Write the end-to-end regression test.** Upload a scores input and `io.v1` contract through `loom_put_content`, open/refine/bind/commit/start a run, execute the package, and assert the wrapped `{"payload": ...}` input is rejected at readiness while a correctly shaped input reaches terminal evidence.

- [x] **Step 2: Run the E2E test and verify red.**

  Run: `pytest -q tests/integration/test_repositories.py tests/integration/test_minio_content_store.py tests/e2e/test_reassignment.py`

  Expected: FAIL until all lifecycle boundaries use `NodeInputBinding` and terminal evidence.

- [x] **Step 3: Remove legacy production paths.** Delete reads/writes of `metadata.execution_payload`, inline `program` materialization, and any direct third-party `jsonschema` calls outside `io_schema.py`; update tool descriptions and README to document the single upload entry point and the strict subset.

- [x] **Step 4: Run the complete verification suite.**

  Run: `pytest -q && python -m compileall -q loom_v2 && git diff --check`

  Expected: all tests pass, compileall exits 0, and diff check is clean. Run the MinIO Compose integration only when services are available.

### Task 8: Post-review hardening

This task records the review fixes added while closing the plan.

**Files:**
- Modify: `loom_v2/observer/app.py`
- Modify: `loom_v2/observer/repository.py`
- Modify: `loom_v2/observer/worker.py`
- Modify: `loom_v2/slave/app.py`
- Modify: `loom_v2/slave/service.py`
- Test: `tests/api/test_readiness.py`
- Test: `tests/api/test_worker_fencing.py`
- Test: `tests/db/test_persistence.py`
- Test: `tests/api/test_observer_run.py`
- Test: `tests/slave/test_execution.py`

- [x] Recover stale `thinking`/`running` Runs at Observer startup as `failed(observer_restarted)`, mark their active attempts failed, and append `run_recovered` events so UI state cannot remain active after an ungraceful process loss.
- [x] At Slave admission, re-read a bound `NodeInputBinding` from ContentStore and validate it against the package/closure IoContract instead of trusting mutable Driver payload.
- [x] Fence Worker dispatch and terminal reports with `attempt_id`, `execution_id`, and `execution_epoch`; reject stale attempts and stale execution identities.
- [x] Remove mutable payload from dispatch envelopes whenever a `NodeInputBinding` exists.
- [x] Make Observer terminal acceptance authoritative: require `execution_id`, current execution epoch, live attempt, value/digest/resource-ref consistency, recompute the canonical result digest, and independently rerun `success_validator_ref` when present.
- [x] Remove the legacy Observer-owned `/worker/v1/terminal` endpoint.
- [x] Reject independently persisted `ProgramApplication.input_schema` / `output_schema` / `success_semantics` in readiness with `inline_io_contract_forbidden`; these remain v2 application semantics, while `io_contract_ref` is the sole executable contract source.
- [x] Resolve capability packages from the complete `ComputeBinding.capability_package_ref` in both dispatch branches, so readiness and dispatch share one `ResourceRef`/digest identity rule instead of two lookup semantics.
- [x] Add regression coverage for inline I/O fields, stale recovery, admission re-read, Worker fencing, Observer digest/validator authority, legacy terminal endpoint removal, and epoch reassignment.

Verification:

Run: `pytest -q tests/api/test_readiness.py tests/api/test_worker_fencing.py tests/db/test_persistence.py tests/api/test_observer_run.py tests/slave/test_execution.py`

Expected: PASS. Full-suite verification remains `pytest -q && python -m compileall -q loom_v2 && git diff --check`.

Completed on 2026-08-29: focused suite passed (34 tests), full suite passed (145 tests), `compileall` passed, and `git diff --check` passed.

## Scope decisions carried from the approved design

- `jsonschema` is the validation engine; Loom owns only policy adaptation and error normalization.
- JSON syntax parsing uses the standard library; Pydantic remains for fixed protocol/domain models.
- Only `loom_put_content` is an external upload operation. Specialized `put` MCP tools are intentionally not added.
- `$ref`, `format`, unknown keywords, remote schema loading, coercion, validator resource limits, and historical package compatibility remain out of scope for this implementation.
