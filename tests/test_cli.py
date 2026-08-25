import subprocess
import sys


def test_cli_self_test_runs_fake_vertical_slice():
    result = subprocess.run([sys.executable, "-m", "loom_v2.cli", "--self-test"], check=True, capture_output=True, text=True)
    assert '"state": "running"' in result.stdout
