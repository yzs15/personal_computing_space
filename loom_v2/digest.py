"""Shared canonical JSON digest primitives.

All content identities in Loom use one canonical serializer.  A short domain
prefix keeps digests from unrelated protocol objects from being interpreted as
the same identity while retaining ordinary SHA-256 hex wire values.
"""

from __future__ import annotations

import hashlib
from typing import Any

import rfc8785


def canonical_json_bytes(value: Any) -> bytes:
    try:
        return rfc8785.dumps(value)
    except (TypeError, ValueError, rfc8785.CanonicalizationError) as exc:
        raise ValueError("json_canonicalization_error") from exc


def digest_json(value: Any, *, domain: str) -> str:
    domain_bytes = domain.encode("utf-8")
    return hashlib.sha256(domain_bytes + b"\0" + canonical_json_bytes(value)).hexdigest()


def digest_bytes(value: bytes, *, domain: str = "") -> str:
    prefix = domain.encode("utf-8") + b"\0" if domain else b""
    return hashlib.sha256(prefix + value).hexdigest()


__all__ = ["canonical_json_bytes", "digest_json", "digest_bytes"]
