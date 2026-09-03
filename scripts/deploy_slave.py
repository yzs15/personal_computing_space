#!/usr/bin/env python3
"""Deploy one Slave and its private PostgreSQL."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from loom_v2.deployment.cli import main


if __name__ == "__main__":
    raise SystemExit(main(forced_target="slave"))

