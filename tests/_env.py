"""Environment helpers for test profiles."""

from __future__ import annotations

import os
import unittest


def is_db_enabled() -> bool:
    return os.getenv("RUN_DB_TESTS", "").strip() == "1"


def is_slow_enabled() -> bool:
    return os.getenv("RUN_SLOW_TESTS", "").strip() == "1"


def require_db_or_skip() -> None:
    if not is_db_enabled():
        raise unittest.SkipTest("RUN_DB_TESTS is not enabled")
    if not os.getenv("TEST_DATABASE_URL", "").strip():
        raise unittest.SkipTest("TEST_DATABASE_URL is not configured")


def require_slow_or_skip() -> None:
    if not is_slow_enabled():
        raise unittest.SkipTest("RUN_SLOW_TESTS is not enabled")

