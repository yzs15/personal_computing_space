from __future__ import annotations

from typing import Any


def is_monotonic_tightening(parent: dict[str, Any], child: dict[str, Any]) -> bool:
    if parent.get("field") != child.get("field"):
        return False
    op = parent.get("op")
    if op == "le" and child.get("op") == "le":
        return child.get("value") <= parent.get("value")
    if op == "ge" and child.get("op") == "ge":
        return child.get("value") >= parent.get("value")
    if op == "allow" and child.get("op") == "allow":
        return set(child.get("value", [])) <= set(parent.get("value", []))
    return parent == child
