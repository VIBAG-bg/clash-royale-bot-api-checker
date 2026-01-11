import unittest
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

try:
    import sqlalchemy  # noqa: F401
except Exception:
    raise unittest.SkipTest("sqlalchemy not available")

from config import NEW_MEMBER_WEEKS_PLAYED
from i18n import t
from reports import (
    build_current_war_report,
    build_kick_shortlist_report,
    build_rolling_report,
    build_weekly_report,
)


class _FakeResult:
    def __init__(self, rows=None, first_value=None):
        self._rows = rows or []
        self._first_value = first_value

    def all(self):
        return self._rows

    def first(self):
        return self._first_value


class _FakeSession:
    def __init__(self, results):
        self._results = list(results)

    async def execute(self, *args, **kwargs):
        if not self._results:
            raise AssertionError("Unexpected session.execute call")
        return self._results.pop(0)


class WarReportsTests(unittest.IsolatedAsyncioTestCase):
    async def test_weekly_report_basic(self) -> None:
        inactive = [
            {
                "player_tag": "#A",
                "player_name": "Alice",
                "decks_used": 0,
                "fame": 0,
            }
        ]
        active = [
            {
                "player_tag": "#B",
                "player_name": "Bob",
                "decks_used": 16,
                "fame": 200,
            }
        ]
        with patch(
            "reports.get_week_leaderboard",
            new=AsyncMock(return_value=(inactive, active)),
        ), patch(
            "reports.get_current_member_tags",
            new=AsyncMock(return_value={"#A", "#B"}),
        ), patch(
            "reports._build_top_donors_wtd_block",
            new=AsyncMock(return_value=["DONORS"]),
        ):
            report = await build_weekly_report(
                1, 0, "#CLAN", lang="en"
            )
        self.assertIn(t("weekly_report_title", "en", season=1, week=1), report)
        self.assertIn("Alice", report)
        self.assertIn("DONORS", report)

    async def test_rolling_report_basic(self) -> None:
        inactive = [
            {
                "player_tag": "#A",
                "player_name": "Alice",
                "decks_used": 1,
                "fame": 10,
            }
        ]
        active = [
            {
                "player_tag": "#B",
                "player_name": "Bob",
                "decks_used": 16,
                "fame": 200,
            }
        ]
        weeks = [(1, 0), (1, 1)]
        with patch(
            "reports.get_rolling_leaderboard",
            new=AsyncMock(return_value=(inactive, active)),
        ), patch(
            "reports.get_current_member_tags",
            new=AsyncMock(return_value={"#A", "#B"}),
        ), patch(
            "reports._build_top_donors_window_block",
            new=AsyncMock(return_value=["WINDOW_DONORS"]),
        ), patch(
            "reports.DONATION_WEEKS_WINDOW", 8
        ):
            report = await build_rolling_report(
                weeks, "#CLAN", lang="en"
            )
        self.assertIn(t("rolling_report_title", "en", weeks=2), report)
        self.assertIn(t("rolling_report_weeks", "en", weeks="1/1, 1/2"), report)
        self.assertIn("WINDOW_DONORS", report)

    async def test_current_war_report_training(self) -> None:
        last_completed = SimpleNamespace(
            season_id=1, section_index=2, is_colosseum=False
        )
        session = _FakeSession([_FakeResult(first_value=last_completed)])

        @asynccontextmanager
        async def session_ctx():
            yield session

        training_state = {
            "period_type": "training",
            "is_colosseum": False,
            "updated_at": None,
        }
        with patch(
            "reports._resolve_active_week_key",
            new=AsyncMock(return_value=(1, 0)),
        ), patch(
            "reports.get_river_race_state_for_week",
            new=AsyncMock(return_value=training_state),
        ), patch(
            "reports.get_latest_river_race_state",
            new=AsyncMock(return_value=None),
        ), patch(
            "reports.get_session",
            new=session_ctx,
        ):
            report = await build_current_war_report("#CLAN", lang="en")
        expected_line = t(
            "current_war_training_last_completed",
            "en",
            season=1,
            week=3,
            war_type=t("current_war_river_race", "en"),
        )
        self.assertIn(t("current_war_training_msg_1", "en"), report)
        self.assertIn(expected_line, report)

    async def test_current_war_report_war_day(self) -> None:
        totals = SimpleNamespace(decks_sum=32, fame_sum=400, member_count=2)
        top_rows = [
            SimpleNamespace(
                player_tag="#A", player_name="Alice", decks_used=16, fame=200
            )
        ]
        bottom_rows = [
            SimpleNamespace(
                player_tag="#B", player_name="Bob", decks_used=0, fame=0
            )
        ]
        session = _FakeSession(
            [
                _FakeResult(first_value=totals),
                _FakeResult(rows=top_rows),
                _FakeResult(rows=bottom_rows),
            ]
        )

        @asynccontextmanager
        async def session_ctx():
            yield session

        war_state = {
            "period_type": "war_day",
            "period_index": 0,
            "is_colosseum": False,
            "updated_at": None,
        }
        with patch(
            "reports._resolve_active_week_key",
            new=AsyncMock(return_value=(1, 0)),
        ), patch(
            "reports.get_river_race_state_for_week",
            new=AsyncMock(return_value=war_state),
        ), patch(
            "reports.get_latest_river_race_state",
            new=AsyncMock(return_value=None),
        ), patch(
            "reports.get_current_member_tags",
            new=AsyncMock(return_value={"#A", "#B"}),
        ), patch(
            "reports.get_session",
            new=session_ctx,
        ):
            report = await build_current_war_report("#CLAN", lang="en")
        self.assertIn(t("current_war_top_header", "en"), report)
        self.assertIn(t("current_war_bottom_header", "en"), report)
        self.assertIn("Alice", report)

    async def test_kick_shortlist_no_weeks(self) -> None:
        report = await build_kick_shortlist_report(
            [], None, "#CLAN", lang="en"
        )
        self.assertIn(t("kick_shortlist_none", "en"), report)
        self.assertIn(t("kick_wtd_note", "en"), report)

    async def test_kick_shortlist_new_member(self) -> None:
        inactive = [
            {
                "player_tag": "#A",
                "player_name": "Alice",
                "decks_used": 0,
                "fame": 0,
            }
        ]

        @asynccontextmanager
        async def session_ctx():
            yield object()

        with patch(
            "reports.get_session",
            new=session_ctx,
        ), patch(
            "reports.get_rolling_leaderboard",
            new=AsyncMock(return_value=(inactive, [])),
        ), patch(
            "reports.get_participation_week_counts",
            new=AsyncMock(return_value={"#A": 1}),
        ), patch(
            "reports.get_week_decks_map",
            new=AsyncMock(return_value={"#A": 0}),
        ), patch(
            "reports._collect_wtd_donations",
            new=AsyncMock(return_value={}),
        ), patch(
            "reports.get_last_seen_map",
            new=AsyncMock(return_value={}),
        ), patch(
            "reports.get_current_members_snapshot",
            new=AsyncMock(return_value=[]),
        ):
            report = await build_kick_shortlist_report(
                [(1, 0)], (1, 0), "#CLAN", lang="en"
            )
        self.assertIn(
            t(
                "kick_new_members_header",
                "en",
                weeks=NEW_MEMBER_WEEKS_PLAYED,
            ),
            report,
        )
        self.assertIn("Alice", report)
