from datetime import datetime, timedelta, timezone
import unittest

try:
    from sqlalchemy import text
except Exception:
    raise unittest.SkipTest("sqlalchemy not available")

from db import (
    clear_user_penalty,
    get_warning_count,
    increment_user_warning,
    record_rate_counter,
    reset_user_warnings,
    set_user_penalty,
)
from tests._db_harness import DBTestCase


class DBModerationCoreTests(DBTestCase):
    async def test_increment_user_warning_increases_and_persists(self) -> None:
        chat_id = -200100
        user_id = 5001
        now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)

        count_1 = await increment_user_warning(
            chat_id, user_id, now=now, session=self.session
        )
        count_2 = await increment_user_warning(
            chat_id, user_id, now=now + timedelta(minutes=1), session=self.session
        )

        self.assertEqual(1, count_1)
        self.assertEqual(2, count_2)
        self.assertEqual(
            2, await get_warning_count(chat_id, user_id, session=self.session)
        )

        row = (
            await self.session.execute(
                text(
                    'SELECT "count" AS warning_count, last_warned_at '
                    "FROM user_warnings "
                    "WHERE chat_id = :chat_id AND user_id = :user_id"
                ),
                {"chat_id": chat_id, "user_id": user_id},
            )
        ).first()
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(2, int(row._mapping["warning_count"]))
        self.assertIsNotNone(row._mapping["last_warned_at"])

    async def test_reset_user_warnings_sets_zero(self) -> None:
        chat_id = -200101
        user_id = 5002
        now = datetime(2026, 2, 10, 13, 0, tzinfo=timezone.utc)

        await increment_user_warning(chat_id, user_id, now=now, session=self.session)
        await increment_user_warning(
            chat_id, user_id, now=now + timedelta(seconds=10), session=self.session
        )
        await reset_user_warnings(chat_id, user_id, session=self.session)

        self.assertEqual(
            0, await get_warning_count(chat_id, user_id, session=self.session)
        )
        row = (
            await self.session.execute(
                text(
                    'SELECT "count" AS warning_count FROM user_warnings '
                    "WHERE chat_id = :chat_id AND user_id = :user_id"
                ),
                {"chat_id": chat_id, "user_id": user_id},
            )
        ).first()
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(0, int(row._mapping["warning_count"]))

    async def test_set_and_clear_user_penalty(self) -> None:
        chat_id = -200102
        user_id = 5003
        penalty = "mute"
        until = datetime(2026, 2, 10, 14, 30, tzinfo=timezone.utc)

        await set_user_penalty(
            chat_id, user_id, penalty, until, session=self.session
        )
        active_row = (
            await self.session.execute(
                text(
                    "SELECT penalty, until "
                    "FROM user_penalties "
                    "WHERE chat_id = :chat_id AND user_id = :user_id "
                    "AND penalty = :penalty"
                ),
                {
                    "chat_id": chat_id,
                    "user_id": user_id,
                    "penalty": penalty,
                },
            )
        ).first()
        self.assertIsNotNone(active_row)
        assert active_row is not None
        self.assertEqual(penalty, active_row.penalty)
        self.assertEqual(until, active_row.until)

        await clear_user_penalty(chat_id, user_id, penalty, session=self.session)
        cleared_row = (
            await self.session.execute(
                text(
                    "SELECT 1 "
                    "FROM user_penalties "
                    "WHERE chat_id = :chat_id AND user_id = :user_id "
                    "AND penalty = :penalty"
                ),
                {
                    "chat_id": chat_id,
                    "user_id": user_id,
                    "penalty": penalty,
                },
            )
        ).first()
        self.assertIsNone(cleared_row)

    async def test_record_rate_counter_window_and_expiry(self) -> None:
        chat_id = -200103
        user_id = 5004
        base = datetime(2026, 2, 10, 15, 0, tzinfo=timezone.utc)

        c1 = await record_rate_counter(
            chat_id,
            user_id,
            window_seconds=10,
            now=base,
            session=self.session,
        )
        c2 = await record_rate_counter(
            chat_id,
            user_id,
            window_seconds=10,
            now=base + timedelta(seconds=5),
            session=self.session,
        )
        c3 = await record_rate_counter(
            chat_id,
            user_id,
            window_seconds=10,
            now=base + timedelta(seconds=15),
            session=self.session,
        )

        self.assertEqual(1, c1)
        self.assertEqual(2, c2)
        self.assertEqual(1, c3)

        row = (
            await self.session.execute(
                text(
                    'SELECT "count" AS rate_count, window_start '
                    "FROM rate_counters "
                    "WHERE chat_id = :chat_id AND user_id = :user_id"
                ),
                {"chat_id": chat_id, "user_id": user_id},
            )
        ).first()
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(1, int(row._mapping["rate_count"]))
        self.assertEqual(base + timedelta(seconds=15), row._mapping["window_start"])
