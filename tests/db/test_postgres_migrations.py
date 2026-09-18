"""Migration transaction and process-lock tests against disposable PostgreSQL."""

import asyncio
import os
import subprocess
import sys
import time
from uuid import uuid4

import pytest
from alembic import op
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from loom_v2.db.migrations import apply_migrations


pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres_container():
    container = subprocess.check_output(
        ["docker", "run", "--detach", "--rm", "--publish", "127.0.0.1::5432",
         "--env", "POSTGRES_PASSWORD=migration-test", "postgres:16-alpine"],
        text=True,
    ).strip()
    try:
        for _ in range(100):
            ready = subprocess.run(
                ["docker", "exec", container, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"],
                capture_output=True, timeout=5,
            )
            if ready.returncode == 0:
                break
            time.sleep(0.1)
        else:
            pytest.fail("test PostgreSQL did not become ready")
        port = subprocess.check_output(
            ["docker", "port", container, "5432/tcp"], text=True,
        ).strip().rsplit(":", 1)[1]
        yield container, port
    finally:
        subprocess.run(["docker", "stop", "--time", "1", container], check=True, capture_output=True)


@pytest.fixture
async def database(postgres_container):
    container, port = postgres_container
    name = f"migration_{uuid4().hex}"
    subprocess.run(["docker", "exec", container, "createdb", "-U", "postgres", name], check=True)
    url = f"postgresql+asyncpg://postgres:migration-test@127.0.0.1:{port}/{name}"
    engine = create_async_engine(url)
    try:
        yield engine, url
    finally:
        await engine.dispose()


@pytest.mark.parametrize("role,own_table,other_table", [
    ("observer", "runs", "slave_attempts"),
    ("slave", "slave_attempts", "runs"),
])
async def test_failed_migration_rolls_back_schema_and_version(database, monkeypatch, role, own_table, other_table):
    engine, _ = database
    execute = op.execute
    calls = 0

    def fail_after_first_ddl(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected_migration_failure")
        return execute(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(op, "execute", fail_after_first_ddl)
        with pytest.raises(RuntimeError, match="injected_migration_failure"):
            async with engine.begin() as connection:
                await apply_migrations(connection, role)
    async with engine.begin() as connection:
        tables = (await connection.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        ))).scalars().all()
        assert tables == []
        await apply_migrations(connection, role)
    async with engine.begin() as connection:
        await apply_migrations(connection, role)
        assert (await connection.execute(text(f"SELECT to_regclass('{own_table}')"))).scalar() == own_table
        assert (await connection.execute(text(f"SELECT to_regclass('{other_table}')"))).scalar() is None
        assert (await connection.execute(text(
            f"SELECT version_num FROM loom_{role}_alembic_version"
        ))).scalar_one() == "001_release_baseline"


@pytest.mark.parametrize("role", ["observer", "slave"])
async def test_concurrent_processes_upgrade_once(database, role):
    engine, url = database
    script = """
import asyncio, os
from sqlalchemy.ext.asyncio import create_async_engine
from loom_v2.db.migrations import apply_migrations
async def main():
    engine = create_async_engine(os.environ['MIGRATION_TEST_URL'])
    try:
        async with engine.begin() as connection:
            await apply_migrations(connection, os.environ['MIGRATION_TEST_ROLE'])
    finally:
        await engine.dispose()
asyncio.run(main())
"""
    processes = []
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": f"loom:alembic:{role}"},
            )
            for _ in range(3):
                processes.append(await asyncio.create_subprocess_exec(
                    sys.executable, "-c", script,
                    env={**os.environ, "MIGRATION_TEST_URL": url, "MIGRATION_TEST_ROLE": role},
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                ))
            async with asyncio.timeout(30):
                while True:
                    blocked = (await connection.execute(text(
                        "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted"
                    ))).scalar_one()
                    if blocked == 3:
                        break
                    await asyncio.sleep(0.05)
        for process in processes:
            _, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
            assert process.returncode == 0, stderr.decode()
        async with engine.connect() as connection:
            assert (await connection.execute(text(
                f"SELECT version_num FROM loom_{role}_alembic_version"
            ))).scalars().all() == ["001_release_baseline"]
    finally:
        for process in processes:
            if process.returncode is None:
                process.kill()
                await process.wait()
