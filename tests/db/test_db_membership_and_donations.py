from datetime import date, datetime, timezone
import unittest

try:
    import sqlalchemy  # noqa: F401
except Exception:
    raise unittest.SkipTest("sqlalchemy not available")

from db import (
    get_current_member_tags,
    get_donation_week_start_date,
    get_donation_weekly_sums_for_window,
    get_last_seen_map,
)
from tests._db_harness import DBTestCase
from tests._seed import seed_donations, seed_members


class DBMembershipAndDonationsTests(DBTestCase):
    async def test_get_current_member_tags_and_last_seen_map(self) -> None:
        clan_tag = "#CLAN"
        old_date = date(2026, 2, 8)
        latest_date = date(2026, 2, 10)
        await seed_members(
            self.session,
            clan_tag=clan_tag,
            snapshot_date=old_date,
            members=[
                {"player_tag": "#A", "player_name": "A"},
            ],
        )
        await seed_members(
            self.session,
            clan_tag=clan_tag,
            snapshot_date=latest_date,
            members=[
                {
                    "player_tag": "#B",
                    "player_name": "B",
                    "last_seen": datetime(2026, 2, 9, 22, tzinfo=timezone.utc),
                },
                {"player_tag": "#C", "player_name": "C", "last_seen": None},
            ],
        )
        tags = await get_current_member_tags(clan_tag, session=self.session)
        last_seen = await get_last_seen_map(clan_tag, session=self.session)
        self.assertEqual({"#B", "#C"}, tags)
        self.assertIn("#B", last_seen)
        self.assertIn("#C", last_seen)
        self.assertIsNone(last_seen["#C"])

    def test_get_donation_week_start_date(self) -> None:
        dt = datetime(2026, 2, 11, 5, 0, tzinfo=timezone.utc)  # Wed
        week_start = get_donation_week_start_date(dt)
        self.assertEqual(date(2026, 2, 8), week_start)  # Sunday

    async def test_get_donation_weekly_sums_for_window(self) -> None:
        clan_tag = "#CLAN"
        latest_date = date(2026, 2, 10)
        await seed_members(
            self.session,
            clan_tag=clan_tag,
            snapshot_date=latest_date,
            members=[
                {"player_tag": "#A", "player_name": "Alice"},
                {"player_tag": "#B", "player_name": "Bob"},
            ],
        )
        await seed_donations(
            self.session,
            clan_tag=clan_tag,
            week_start_date=date(2026, 2, 1),
            rows=[
                {"player_tag": "#A", "player_name": "Alice", "donations_week_total": 50},
                {"player_tag": "#B", "player_name": "Bob", "donations_week_total": 20},
            ],
        )
        await seed_donations(
            self.session,
            clan_tag=clan_tag,
            week_start_date=date(2026, 2, 8),
            rows=[
                {"player_tag": "#A", "player_name": "Alice", "donations_week_total": 70},
                {"player_tag": "#B", "player_name": "Bob", "donations_week_total": 30},
            ],
        )
        rows, coverage = await get_donation_weekly_sums_for_window(
            clan_tag,
            window_weeks=8,
            session=self.session,
        )
        by_tag = {row["player_tag"]: row for row in rows}
        self.assertEqual(2, coverage)
        self.assertEqual(120, by_tag["#A"]["donations_sum"])
        self.assertEqual(50, by_tag["#B"]["donations_sum"])
        self.assertEqual(2, by_tag["#A"]["weeks_present"])
