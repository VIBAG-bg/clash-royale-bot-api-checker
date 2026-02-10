import os
import unittest
from uuid import uuid4
from unittest.mock import patch

from tests._env import require_db_or_skip


class DBHarnessGateTests(unittest.TestCase):
    def test_require_db_or_skip_requires_run_flag(self) -> None:
        with patch.dict(
            os.environ,
            {
                "RUN_DB_TESTS": "0",
            },
            clear=False,
        ):
            with self.assertRaises(unittest.SkipTest):
                require_db_or_skip()


class DBHarnessRuntimeSmokeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        require_db_or_skip()

    def _load_runtime_helpers(self):
        try:
            from sqlalchemy import text
            from tests._db_harness import session_ctx
        except unittest.SkipTest as exc:
            self.skipTest(str(exc))
        except Exception:
            self.skipTest("sqlalchemy DB dependencies are not available")
        return text, session_ctx

    async def test_session_ctx_can_execute_select_one(self) -> None:
        text, session_ctx = self._load_runtime_helpers()
        async with session_ctx() as session:
            result = await session.execute(text("SELECT 1"))
            self.assertEqual(1, int(result.scalar_one()))

    async def test_nested_commit_survives_and_outer_rollback_cleans_state(self) -> None:
        text, session_ctx = self._load_runtime_helpers()
        key = f"db_harness_smoke_{uuid4().hex}"
        insert_stmt = text(
            'INSERT INTO app_state ("key", value, updated_at) '
            "VALUES (:key, CAST(:value AS jsonb), NOW())"
        )
        count_stmt = text(
            'SELECT COUNT(*) FROM app_state WHERE "key" = :key'
        )

        async with session_ctx() as session:
            await session.execute(
                insert_stmt,
                {"key": key, "value": '{"source":"smoke"}'},
            )
            await session.commit()
            in_tx_count = await session.execute(count_stmt, {"key": key})
            self.assertEqual(1, int(in_tx_count.scalar_one()))

        async with session_ctx() as session:
            after_ctx_count = await session.execute(count_stmt, {"key": key})
            self.assertEqual(0, int(after_ctx_count.scalar_one()))

