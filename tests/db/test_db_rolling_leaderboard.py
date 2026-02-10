from datetime import date
import unittest

try:
    import sqlalchemy  # noqa: F401
except Exception:
    raise unittest.SkipTest("sqlalchemy not available")

from db import get_rolling_leaderboard
from tests._db_harness import DBTestCase
from tests._seed import seed_members, seed_war_week


class DBRollingLeaderboardTests(DBTestCase):
    async def test_get_rolling_leaderboard_sorts_weakest_first(self) -> None:
        clan_tag = "#CLAN"
        weeks = [(100, 0), (100, 1)]

        await seed_members(
            self.session,
            clan_tag=clan_tag,
            snapshot_date=date(2026, 2, 10),
            members=[
                {"player_tag": "#A", "player_name": "A"},
                {"player_tag": "#B", "player_name": "B"},
                {"player_tag": "#C", "player_name": "C"},
            ],
        )

        await seed_war_week(
            self.session,
            clan_tag=clan_tag,
            season_id=100,
            section_index=0,
            participants=[
                {"player_tag": "#A", "player_name": "A", "decks_used": 0, "fame": 0},
                {"player_tag": "#B", "player_name": "B", "decks_used": 8, "fame": 800},
                {"player_tag": "#C", "player_name": "C", "decks_used": 4, "fame": 400},
            ],
        )
        await seed_war_week(
            self.session,
            clan_tag=clan_tag,
            season_id=100,
            section_index=1,
            participants=[
                {"player_tag": "#A", "player_name": "A", "decks_used": 1, "fame": 100},
                {"player_tag": "#B", "player_name": "B", "decks_used": 8, "fame": 900},
                {"player_tag": "#C", "player_name": "C", "decks_used": 4, "fame": 500},
            ],
        )

        inactive, active = await get_rolling_leaderboard(
            weeks,
            clan_tag,
            inactive_limit=3,
            active_limit=3,
            session=self.session,
        )
        self.assertEqual(["#A", "#C", "#B"], [row["player_tag"] for row in inactive])
        self.assertEqual(["#B", "#C", "#A"], [row["player_tag"] for row in active])

    async def test_get_rolling_leaderboard_empty_without_members(self) -> None:
        inactive, active = await get_rolling_leaderboard(
            [(1, 0)],
            "#EMPTY",
            inactive_limit=3,
            active_limit=3,
            session=self.session,
        )
        self.assertEqual([], inactive)
        self.assertEqual([], active)
