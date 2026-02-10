"""PostgreSQL harness for DB profile tests."""

from __future__ import annotations

import os
import re
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

try:
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import event
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
except Exception:
    raise unittest.SkipTest("DB test dependencies are not available")

from tests._env import require_db_or_skip

_BOOTSTRAPPED = False
_ENGINE: AsyncEngine | None = None

_PROD_MARKERS = ("prod", "production", "heroku")
_CLOUD_MARKERS = (
    "amazonaws",
    "rds.",
    "render",
    "neon",
    "supabase",
    "railway",
)
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


def _normalize_sync_url(raw_url: str) -> str:
    if raw_url.startswith("postgres://"):
        return raw_url.replace("postgres://", "postgresql://", 1)
    if raw_url.startswith("postgresql+asyncpg://"):
        return raw_url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)
    return raw_url


def _normalize_async_url(raw_url: str) -> str:
    if raw_url.startswith("postgres://"):
        return raw_url.replace("postgres://", "postgresql+asyncpg://", 1)
    if raw_url.startswith("postgresql://"):
        return raw_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    if raw_url.startswith("postgresql+psycopg://"):
        return raw_url.replace("postgresql+psycopg://", "postgresql+asyncpg://", 1)
    return raw_url


def _extract_db_name(parsed) -> str:
    raw = (parsed.path or "").strip("/")
    if not raw:
        return ""
    return raw.split("/")[-1]


def _has_test_marker(url: str, db_name: str) -> bool:
    lowered = f"{url} {db_name}".lower()
    if re.search(r"(^|[^a-z])test([^a-z]|$)", lowered):
        return True
    return "_test" in lowered or "-test" in lowered


def _assert_safe_test_db_url(raw_url: str) -> None:
    lowered = raw_url.lower()
    parsed = urlparse(raw_url)
    scheme = parsed.scheme.lower()
    if not scheme.startswith("postgres"):
        raise AssertionError("DB suite supports PostgreSQL TEST_DATABASE_URL only")

    if any(marker in lowered for marker in _PROD_MARKERS):
        raise AssertionError("Refusing to run DB tests against a production-like URL")
    host = (parsed.hostname or "").lower()
    db_name = _extract_db_name(parsed).lower()
    has_test_marker = _has_test_marker(raw_url, db_name)
    is_local = (not host) or host in _LOCAL_HOSTS
    is_cloud = any(marker in host for marker in _CLOUD_MARKERS)

    if is_cloud and not has_test_marker:
        raise AssertionError(
            "Cloud-hosted DB URL must include an explicit test marker"
        )
    if (not is_local) and not has_test_marker:
        raise AssertionError(
            "Non-local DB URL must include an explicit test marker"
        )


def _run_alembic_upgrade_head(sync_url: str) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    cfg = Config(str(repo_root / "alembic.ini"))
    cfg.set_main_option("script_location", str(repo_root / "alembic"))
    cfg.set_main_option("sqlalchemy.url", sync_url)
    command.upgrade(cfg, "head")


async def ensure_bootstrap() -> None:
    global _BOOTSTRAPPED, _ENGINE
    require_db_or_skip()
    if _BOOTSTRAPPED and _ENGINE is not None:
        return
    if _BOOTSTRAPPED and _ENGINE is None:
        _BOOTSTRAPPED = False

    raw_url = os.getenv("TEST_DATABASE_URL", "").strip()
    _assert_safe_test_db_url(raw_url)

    sync_url = _normalize_sync_url(raw_url)
    async_url = _normalize_async_url(raw_url)

    # Programmatic Alembic bootstrap once per DB suite run.
    _run_alembic_upgrade_head(sync_url)
    _ENGINE = create_async_engine(async_url, pool_pre_ping=True)
    _BOOTSTRAPPED = True


async def dispose_bootstrap() -> None:
    global _BOOTSTRAPPED, _ENGINE
    if _ENGINE is not None:
        await _ENGINE.dispose()
    _ENGINE = None
    _BOOTSTRAPPED = False


@asynccontextmanager
async def session_ctx():
    await ensure_bootstrap()
    assert _ENGINE is not None

    async with _ENGINE.connect() as conn:
        outer_txn = await conn.begin()
        session = AsyncSession(bind=conn, expire_on_commit=False)
        await session.begin_nested()

        def _restart_savepoint(sync_session, transaction):
            parent = getattr(transaction, "_parent", None)
            if transaction.nested and parent is not None and not parent.nested:
                sync_session.begin_nested()

        event.listen(
            session.sync_session, "after_transaction_end", _restart_savepoint
        )
        try:
            yield session
        finally:
            event.remove(
                session.sync_session, "after_transaction_end", _restart_savepoint
            )
            await session.close()
            if outer_txn.is_active:
                await outer_txn.rollback()


class DBTestCase(unittest.IsolatedAsyncioTestCase):
    """Base testcase that provides isolated DB session in self.session."""

    async def asyncSetUp(self) -> None:
        self._session_cm = session_ctx()
        self.session = await self._session_cm.__aenter__()

    async def asyncTearDown(self) -> None:
        await self._session_cm.__aexit__(None, None, None)
