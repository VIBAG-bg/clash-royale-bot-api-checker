from datetime import date, datetime, timezone
import unittest

try:
    import sqlalchemy  # noqa: F401
except Exception:
    raise unittest.SkipTest("sqlalchemy not available")

from db import get_warning_count, increment_user_warning, try_mark_reminder_posted
from tests._db_harness import DBTestCase


class DBIdempotencyTests(DBTestCase):
    async def test_try_mark_reminder_posted_is_idempotent(self) -> None:
        first = await try_mark_reminder_posted(
            chat_id=-1001,
            reminder_date=date(2026, 2, 10),
            season_id=1,
            section_index=0,
            period="warday",
            day_number=2,
            session=self.session,
        )
        second = await try_mark_reminder_posted(
            chat_id=-1001,
            reminder_date=date(2026, 2, 10),
            season_id=1,
            section_index=0,
            period="warday",
            day_number=2,
            session=self.session,
        )
        self.assertTrue(first)
        self.assertFalse(second)

    async def test_increment_user_warning_accumulates(self) -> None:
        now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)
        count_1 = await increment_user_warning(
            -1002, 777, now=now, session=self.session
        )
        count_2 = await increment_user_warning(
            -1002, 777, now=now, session=self.session
        )
        self.assertEqual(1, count_1)
        self.assertEqual(2, count_2)
        stored = await get_warning_count(-1002, 777, session=self.session)
        self.assertEqual(2, stored)
