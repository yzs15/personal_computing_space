from __future__ import annotations

import asyncio
import json

import pytest

from loom_v2.observer import orchestration_preflight


VALID = (
    'async def orchestrate(ctx: "OrchestrationContext", '
    'input_ref: "ResourceRef") -> "ResourceRef":\n'
    '    handle = ctx.emit_node({"resource_id": "pkg"}, [input_ref])\n'
    '    return await ctx.result(handle)\n'
)


def test_validate_entry_signature_accepts_quoted_annotations():
    assert orchestration_preflight.validate_entry_signature(VALID) == []


def test_validate_entry_signature_reports_missing_annotation():
    source = VALID.replace('ctx: "OrchestrationContext"', "ctx")
    blockers = orchestration_preflight.validate_entry_signature(source)
    assert blockers == [
        {
            "code": "missing_entry_annotation",
            "message": 'parameter "ctx" must use annotation "OrchestrationContext"',
        }
    ]


def test_validate_entry_signature_reports_invalid_annotation():
    source = VALID.replace('"ResourceRef"', "ResourceRef", 1)
    blockers = orchestration_preflight.validate_entry_signature(source)
    assert blockers == [
        {
            "code": "invalid_entry_annotation",
            "message": 'parameter "input_ref" must use string annotation "ResourceRef"',
        }
    ]


@pytest.mark.asyncio
async def test_check_program_reports_undefined_name_with_normalized_position():
    source = VALID.replace('"pkg"', "null")
    diagnostics = await orchestration_preflight.check_program(source)
    assert diagnostics
    diagnostic = next(item for item in diagnostics if item["rule"] == "reportUndefinedVariable")
    assert diagnostic == {
        "file": "orchestration.py",
        "line": 2,
        "column": 44,
        "end_line": 2,
        "end_column": 48,
        "severity": "error",
        "rule": "reportUndefinedVariable",
        "message": "\"null\" is not defined",
    }


@pytest.mark.asyncio
async def test_check_program_reports_emit_node_argument_type():
    source = VALID.replace('ctx.emit_node({"resource_id": "pkg"}, [input_ref])', "ctx.emit_node(1, [input_ref])")
    diagnostics = await orchestration_preflight.check_program(source)
    diagnostic = next(item for item in diagnostics if item["rule"] == "reportArgumentType")
    assert diagnostic["file"] == "orchestration.py"
    assert diagnostic["severity"] == "error"
    assert diagnostic["line"] == 2
    assert "cannot be assigned" in diagnostic["message"]


@pytest.mark.asyncio
async def test_check_program_reports_result_argument_type():
    source = VALID.replace("ctx.result(handle)", "ctx.result(1)")
    diagnostics = await orchestration_preflight.check_program(source)
    diagnostic = next(item for item in diagnostics if item["rule"] == "reportArgumentType")
    assert diagnostic["line"] == 3
    assert "cannot be assigned" in diagnostic["message"]


@pytest.mark.asyncio
async def test_check_program_reports_unavailable_pyright(monkeypatch):
    monkeypatch.setattr(orchestration_preflight.shutil, "which", lambda name: None)
    with pytest.raises(orchestration_preflight.OrchestrationPreflightError) as caught:
        await orchestration_preflight.check_program(VALID)
    assert caught.value.code == "orchestration_program_typecheck_unavailable"


@pytest.mark.asyncio
async def test_check_program_timeout_terminates_child(monkeypatch):
    class HungProcess:
        returncode = None

        async def communicate(self):
            await asyncio.sleep(10)
            return b"", b""

        terminate_called = False

        def terminate(self):
            self.terminate_called = True

        async def wait(self):
            return 0

    process = HungProcess()

    async def create_process(*args, **kwargs):
        return process

    monkeypatch.setattr(orchestration_preflight.shutil, "which", lambda name: "/usr/bin/pyright")
    monkeypatch.setattr(orchestration_preflight.asyncio, "create_subprocess_exec", create_process)
    with pytest.raises(orchestration_preflight.OrchestrationPreflightError) as caught:
        await orchestration_preflight.check_program(VALID, timeout_seconds=0.01)
    assert caught.value.code == "orchestration_program_typecheck_unavailable"
    assert process.terminate_called
