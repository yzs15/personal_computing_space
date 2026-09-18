from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from typing import Any

from loom_v2.coding_agents.fake import FakeCodingAgentProvider
from loom_v2.contracts.types import ResourceRef
from loom_v2.driver.service import DriverService
from loom_v2.observer.repository import ObserverRepository
from loom_v2.content_store import ContentStat, ContentStore, canonical_json_bytes
from loom_v2.slave.service import SlaveService
from loom_v2.testing.observer import seed_embedded_slaves


@dataclass
class _SelfTestContentStore:
    """Small process-local store used by ``--self-test``.

    The production services use the S3-backed ``ContentStore``.  The CLI
    self-test must remain standalone, so it supplies the same async contract
    without requiring MinIO or another external service.
    """

    values: dict[str, tuple[bytes, str]]

    async def put(self, content: Any, *, media_type: str) -> ResourceRef:
        body = content if isinstance(content, bytes) else content.encode("utf-8") if isinstance(content, str) else canonical_json_bytes(content)
        digest = ContentStore.digest(body)
        self.values.setdefault(digest, (body, media_type))
        return ResourceRef(resource_id=f"content://sha256/{digest}", identity_criterion="content_digest")

    async def get(self, ref: ResourceRef, *, expected_digest: str | None = None) -> bytes:
        digest = (expected_digest or ref.digest or ref.resource_id.rsplit("/", 1)[-1]).lower()
        body = self.values[digest][0]
        if expected_digest is not None and ContentStore.digest(body) != expected_digest.lower():
            raise ValueError("content_digest_mismatch")
        return body

    async def stat(self, ref: ResourceRef) -> ContentStat | None:
        digest = (ref.digest or ref.resource_id.rsplit("/", 1)[-1]).lower()
        value = self.values.get(digest)
        if value is None:
            return None
        body, media_type = value
        return ContentStat(size=len(body), declared_digest=digest, media_type=media_type, integrity_verified=ContentStore.digest(body) == digest)


async def _self_test() -> None:
    store = _SelfTestContentStore({})
    repository = ObserverRepository(content_store=store)
    slave = SlaveService("slave-a", content_store=store)
    seed_embedded_slaves(repository, {"slave-a": slave})
    result = await DriverService(repository, FakeCodingAgentProvider(), slaves={"slave-a": slave}).run_prompt("cli-self-test", "run the packaged test program")
    print(json.dumps(result, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser(prog="loom-v2")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        asyncio.run(_self_test())
        return
    parser.error("use --self-test for the deterministic local check")


if __name__ == "__main__":
    main()
