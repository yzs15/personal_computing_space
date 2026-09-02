from __future__ import annotations

import ast
import asyncio
import json
import shutil
import tempfile
from pathlib import Path
from typing import Any


_TYPECHECK_RULES = frozenset(
    {
        "reportUndefinedVariable",
        "reportArgumentType",
        "reportCallIssue",
        "reportAssignmentType",
        "reportReturnType",
        "reportAttributeAccessIssue",
        "reportGeneralTypeIssues",
        "reportInvalidTypeForm",
        "reportUnboundVariable",
    }
)
_PYRIGHT_IMPORT = "from loom_orchestration_types import OrchestrationContext, OrchestrationFailure, ResourceRef\n"
_TYPE_STUB = '''from collections.abc import Mapping
from typing import Any, Protocol, TypeAlias

ResourceRef: TypeAlias = Mapping[str, object]

class OrchestrationContext(Protocol):
    async def read_json(self, ref: ResourceRef) -> Any: ...
    def emit_node(self, package_ref: ResourceRef, input_refs: list[ResourceRef]) -> str: ...
    async def result(self, handle: str) -> ResourceRef: ...

class OrchestrationFailure(Exception):
    def __init__(self, reason: object) -> None: ...
'''
_PYRIGHT_CONFIG = {
    "include": ["orchestration.py"],
    "typeCheckingMode": "basic",
    "reportMissingImports": "none",
    "reportMissingTypeStubs": "none",
    "reportUnknownVariableType": "none",
    "reportUnknownMemberType": "none",
    "reportUnknownArgumentType": "none",
    "reportUnknownParameterType": "none",
    "reportMissingParameterType": "none",
    "reportMissingTypeArgument": "none",
    "reportUnusedImport": "none",
    "reportUnusedVariable": "none",
    "reportUnusedFunction": "none",
}


class OrchestrationPreflightError(RuntimeError):
    def __init__(self, code: str, *, message: str | None = None, details: dict[str, Any] | None = None) -> None:
        self.code = code
        self.details = details or {}
        super().__init__(message or code)


def _annotation_name(annotation: ast.expr | None) -> tuple[str | None, bool]:
    if annotation is None:
        return None, False
    if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
        return annotation.value, True
    if isinstance(annotation, ast.Name):
        return annotation.id, False
    return None, False


def validate_entry_signature(source: str) -> list[dict[str, object]]:
    """Validate the exact, import-free orchestration entry signature."""
    try:
        tree = ast.parse(source, filename="orchestration.py")
    except SyntaxError:
        return [{"code": "invalid_entry_annotation", "message": "orchestration entry cannot be parsed"}]

    entries = [node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "orchestrate"]
    if len(entries) != 1:
        return [{"code": "missing_entry_annotation", "message": "exactly one async def orchestrate is required"}]
    entry = entries[0]
    positional = [*entry.args.posonlyargs, *entry.args.args]
    if (
        entry.args.posonlyargs
        or len(entry.args.args) != 2
        or len(positional) != 2
        or entry.args.vararg is not None
        or entry.args.kwarg is not None
        or entry.args.kwonlyargs
    ):
        return [{"code": "invalid_entry_annotation", "message": "orchestrate must accept exactly ctx and input_ref"}]
    expected = (("ctx", "OrchestrationContext"), ("input_ref", "ResourceRef"))
    blockers: list[dict[str, object]] = []
    for argument, (name, type_name) in zip(positional, expected, strict=True):
        if argument.arg != name:
            blockers.append({"code": "invalid_entry_annotation", "message": f'parameter "{argument.arg}" must be named "{name}"'})
            continue
        annotation, is_string = _annotation_name(argument.annotation)
        if argument.annotation is None:
            blockers.append({"code": "missing_entry_annotation", "message": f'parameter "{name}" must use annotation "{type_name}"'})
        elif annotation != type_name or not is_string:
            blockers.append({"code": "invalid_entry_annotation", "message": f'parameter "{name}" must use string annotation "{type_name}"'})
    return_annotation, is_string = _annotation_name(entry.returns)
    if entry.returns is None:
        blockers.append({"code": "missing_entry_annotation", "message": 'return value must use annotation "ResourceRef"'})
    elif return_annotation != "ResourceRef" or not is_string:
        blockers.append({"code": "invalid_entry_annotation", "message": 'return value must use string annotation "ResourceRef"'})
    return blockers


def _insert_type_import(source: str) -> tuple[str, int]:
    lines = source.splitlines(keepends=True)
    try:
        tree = ast.parse(source, filename="orchestration.py")
    except SyntaxError:
        return source, 0
    insert_after = 0
    body = list(tree.body)
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str):
        insert_after = body[0].end_lineno or body[0].lineno
    for node in body:
        if isinstance(node, ast.ImportFrom) and node.module == "__future__":
            insert_after = max(insert_after, node.end_lineno or node.lineno)
        elif insert_after and (node.lineno or 0) <= insert_after:
            continue
        elif insert_after:
            break
    checked = "".join(lines[:insert_after] + [_PYRIGHT_IMPORT] + lines[insert_after:])
    return checked, insert_after


def _normalize_diagnostics(payload: dict[str, Any], *, inserted_lines: int) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for item in payload.get("generalDiagnostics", []):
        if not isinstance(item, dict):
            continue
        diagnostic_file = item.get("file")
        if diagnostic_file is not None and Path(str(diagnostic_file)).name != "orchestration.py":
            continue
        rule = item.get("rule")
        if rule not in _TYPECHECK_RULES:
            continue
        location = item.get("range")
        if not isinstance(location, dict):
            continue
        start_value = location.get("start")
        end_value = location.get("end")
        start: dict[str, Any] = start_value if isinstance(start_value, dict) else {}
        end: dict[str, Any] = end_value if isinstance(end_value, dict) else {}
        start_line = int(start.get("line", 0)) + 1
        end_line = int(end.get("line", start.get("line", 0))) + 1
        if start_line == inserted_lines + 1 and end_line == start_line:
            continue
        if start_line > inserted_lines:
            start_line -= 1
        if end_line > inserted_lines:
            end_line -= 1
        normalized.append(
            {
                "file": "orchestration.py",
                "line": start_line,
                "column": int(start.get("character", 0)) + 1,
                "end_line": end_line,
                "end_column": int(end.get("character", 0)) + 1,
                "severity": str(item.get("severity", "error")),
                "rule": str(rule),
                "message": str(item.get("message", "")),
            }
        )
    normalized.sort(key=lambda item: (int(str(item["line"])), int(str(item["column"])), str(item["rule"]), str(item["message"])))
    return normalized


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    try:
        process.terminate()
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(process.wait(), timeout=1.0)
    except (TimeoutError, asyncio.TimeoutError):
        try:
            process.kill()
        except ProcessLookupError:
            return
        try:
            await process.wait()
        except Exception:
            pass


async def check_program(source: str, *, timeout_seconds: float = 10.0) -> list[dict[str, object]]:
    """Run the pinned Pyright CLI against an isolated orchestration copy."""
    executable = shutil.which("pyright")
    if executable is None:
        raise OrchestrationPreflightError("orchestration_program_typecheck_unavailable", message="pyright executable is unavailable")
    checked_source, inserted_lines = _insert_type_import(source)
    process: asyncio.subprocess.Process | None = None
    with tempfile.TemporaryDirectory(prefix="loom-orchestration-check-") as directory:
        root = Path(directory)
        (root / "orchestration.py").write_text(checked_source, encoding="utf-8")
        (root / "loom_orchestration_types.pyi").write_text(_TYPE_STUB, encoding="utf-8")
        (root / "pyrightconfig.json").write_text(json.dumps(_PYRIGHT_CONFIG), encoding="utf-8")
        try:
            process = await asyncio.create_subprocess_exec(
                executable,
                "--outputjson",
                "--project",
                "pyrightconfig.json",
                "orchestration.py",
                cwd=directory,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
        except asyncio.CancelledError:
            if process is not None:
                await _terminate_process(process)
            raise
        except (OSError, TimeoutError, asyncio.TimeoutError) as exc:
            if process is not None:
                await _terminate_process(process)
            raise OrchestrationPreflightError("orchestration_program_typecheck_unavailable") from exc
        if not stdout:
            raise OrchestrationPreflightError("orchestration_program_typecheck_unavailable")
        try:
            payload = json.loads(stdout.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise OrchestrationPreflightError("orchestration_program_typecheck_unavailable") from exc
        if not isinstance(payload, dict):
            raise OrchestrationPreflightError("orchestration_program_typecheck_unavailable")
        return _normalize_diagnostics(payload, inserted_lines=inserted_lines)
