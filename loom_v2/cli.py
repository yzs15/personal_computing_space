from __future__ import annotations

import argparse
import asyncio
import json

from loom_v2.coding_agents.fake import FakeCodingAgentProvider
from loom_v2.driver.service import DriverService
from loom_v2.observer.repository import ObserverRepository


async def _self_test() -> None:
    result = await DriverService(ObserverRepository(), FakeCodingAgentProvider()).run_prompt("cli-self-test", "echo hello")
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
