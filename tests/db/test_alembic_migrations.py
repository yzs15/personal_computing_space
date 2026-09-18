from __future__ import annotations

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _scripts() -> ScriptDirectory:
    config = Config()
    config.set_main_option("script_location", str(PROJECT_ROOT / "migrations" / "alembic"))
    return ScriptDirectory.from_config(config)


def test_alembic_has_one_linear_head_and_safe_revision_names() -> None:
    scripts = _scripts()
    assert scripts.get_heads() == ["001_release_baseline"]

    revisions = list(scripts.walk_revisions(base="base", head="001_release_baseline"))
    assert [script.revision for script in revisions] == ["001_release_baseline"]
    assert all(len(script.revision) <= 32 for script in revisions)
