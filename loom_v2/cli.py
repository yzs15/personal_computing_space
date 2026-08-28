from __future__ import annotations

import argparse
import asyncio
import json
import os

from loom_v2.coding_agents.fake import FakeCodingAgentProvider
from loom_v2.driver.service import DriverService
from loom_v2.observer.repository import ObserverRepository
from loom_v2.content_store import ContentStore


async def _self_test() -> None:
    # The self-test is a deterministic control-plane check and does not touch
    # content.  Give its repository an explicit local S3 configuration so it
    # remains runnable without starting the Compose stack first.
    store = ContentStore(
        endpoint_url=os.getenv("LOOM_S3_ENDPOINT_URL", "http://127.0.0.1:9000"),
        bucket=os.getenv("LOOM_S3_BUCKET", "loom-content"),
        access_key=os.getenv("LOOM_S3_ACCESS_KEY", "loom"),
        secret_key=os.getenv("LOOM_S3_SECRET_KEY", "loom-content-secret"),
        region=os.getenv("LOOM_S3_REGION", "us-east-1"),
        prefix=os.getenv("LOOM_S3_PREFIX", ""),
    )
    result = await DriverService(ObserverRepository(content_store=store), FakeCodingAgentProvider()).run_prompt("cli-self-test", "echo hello")
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
