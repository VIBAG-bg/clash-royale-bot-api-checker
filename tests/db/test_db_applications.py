from datetime import datetime, timedelta, timezone
import unittest

try:
    import sqlalchemy  # noqa: F401
except Exception:
    raise unittest.SkipTest("sqlalchemy not available")

from db import (
    create_application,
    get_application_by_id,
    list_invited_applications,
    list_pending_applications,
    mark_application_invited,
    mark_application_joined,
    reset_expired_invite,
)
from tests._db_harness import DBTestCase


class DBApplicationsTests(DBTestCase):
    async def test_application_lifecycle_invited_pending_joined(self) -> None:
        app = await create_application(
            telegram_user_id=123,
            telegram_username="user1",
            telegram_display_name="User 1",
            player_name="Player 1",
            player_tag="#P1",
            session=self.session,
        )
        self.assertEqual("pending", app["status"])

        pending = await list_pending_applications(limit=10, session=self.session)
        self.assertEqual(1, len(pending))

        now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)
        await mark_application_invited(
            app["id"],
            now=now,
            invite_expires_at=now + timedelta(minutes=20),
            session=self.session,
        )
        invited_rows = await list_invited_applications(session=self.session)
        self.assertEqual(1, len(invited_rows))
        invited = await get_application_by_id(app["id"], session=self.session)
        self.assertEqual("invited", invited["status"])
        self.assertEqual(1, invited["notify_attempts"])

        await reset_expired_invite(
            app["id"], now=now + timedelta(minutes=30), exhausted=False, session=self.session
        )
        reset = await get_application_by_id(app["id"], session=self.session)
        self.assertEqual("pending", reset["status"])
        self.assertIsNone(reset["invite_expires_at"])

        await mark_application_invited(
            app["id"],
            now=now,
            invite_expires_at=now + timedelta(minutes=20),
            session=self.session,
        )
        await mark_application_joined(app["id"], now=now, session=self.session)
        joined = await get_application_by_id(app["id"], session=self.session)
        self.assertEqual("joined", joined["status"])

    async def test_application_reset_to_expired_when_exhausted(self) -> None:
        app = await create_application(
            telegram_user_id=456,
            telegram_username="user2",
            telegram_display_name="User 2",
            player_name="Player 2",
            player_tag="#P2",
            session=self.session,
        )
        now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)
        await mark_application_invited(
            app["id"],
            now=now,
            invite_expires_at=now + timedelta(minutes=20),
            session=self.session,
        )
        await reset_expired_invite(
            app["id"], now=now + timedelta(minutes=30), exhausted=True, session=self.session
        )
        expired = await get_application_by_id(app["id"], session=self.session)
        self.assertEqual("expired", expired["status"])
