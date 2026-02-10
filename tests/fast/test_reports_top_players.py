import unittest
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

try:
    import sqlalchemy  # noqa: F401
except Exception:
    raise unittest.SkipTest("sqlalchemy not available")

from i18n import t
from reports import build_top_players_report


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeSession:
    def __init__(self, rows):
        self._rows = rows

    async def execute(self, *args, **kwargs):
        return _FakeResult(self._rows)


class TopPlayersReportTests(unittest.IsolatedAsyncioTestCase):
    async def test_tenure_filter_applied_before_aggregation(self) -> None:
        session = _FakeSession([])

        @asynccontextmanager
        async def session_ctx():
            yield session

        weeks = [(50, 2), (50, 1)]
        war_stats = {
            "#A": {"weeks_played": 6},
            "#B": {"weeks_played": 2},
            "#C": {"weeks_played": 7},
        }
        summary_rows = [
            {"player_tag": "#A", "player_name": "Player A", "decks_used": 30, "fame": 3000},
            {"player_tag": "#C", "player_name": "Player C", "decks_used": 28, "fame": 2800},
        ]

        with patch("reports.get_session", new=session_ctx), patch(
            "reports.get_last_completed_weeks",
            new=AsyncMock(return_value=weeks),
        ), patch(
            "reports.get_current_member_tags",
            new=AsyncMock(return_value={"#A", "#B", "#C"}),
        ), patch(
            "reports.get_war_stats_for_weeks",
            new=AsyncMock(return_value=war_stats),
        ), patch(
            "reports.get_rolling_summary",
            new=AsyncMock(return_value=summary_rows),
        ) as rolling_mock:
            report = await build_top_players_report(
                "#CLAN",
                lang="en",
                min_tenure_weeks=6,
                limit=5,
            )

        rolling_mock.assert_awaited_once_with(
            weeks, player_tags={"#A", "#C"}
        )
        self.assertIn(t("top_filter_line", "en", weeks=6), report)
        self.assertIn("Player A", report)
        self.assertIn("Player C", report)
        self.assertNotIn("Player B", report)

    async def test_aggregation_ordering_across_weeks(self) -> None:
        rows = [
            SimpleNamespace(season_id=51, section_index=3),
            SimpleNamespace(season_id=51, section_index=2),
        ]
        session = _FakeSession(rows)

        @asynccontextmanager
        async def session_ctx():
            yield session

        war_stats = {
            "#A": {"weeks_played": 10},
            "#B": {"weeks_played": 10},
            "#C": {"weeks_played": 10},
        }
        summary_rows = [
            {"player_tag": "#A", "player_name": "Alpha", "decks_used": 20, "fame": 100},
            {"player_tag": "#B", "player_name": "Bravo", "decks_used": 20, "fame": 150},
            {"player_tag": "#C", "player_name": "Charlie", "decks_used": 18, "fame": 200},
        ]

        with patch("reports.get_session", new=session_ctx), patch(
            "reports.get_last_completed_weeks",
            new=AsyncMock(return_value=[]),
        ) as fallback_mock, patch(
            "reports.get_current_member_tags",
            new=AsyncMock(return_value={"#A", "#B", "#C"}),
        ), patch(
            "reports.get_war_stats_for_weeks",
            new=AsyncMock(return_value=war_stats),
        ), patch(
            "reports.get_rolling_summary",
            new=AsyncMock(return_value=summary_rows),
        ):
            report = await build_top_players_report(
                "#CLAN",
                lang="en",
                limit=3,
                min_tenure_weeks=6,
            )

        fallback_mock.assert_not_awaited()
        lines = report.splitlines()
        decks_header = t("top_decks_header", "en", n=3)
        fame_header = t("top_fame_header", "en", n=3)

        decks_start = lines.index(decks_header) + 1
        fame_start = lines.index(fame_header) + 1
        decks_lines = lines[decks_start : decks_start + 3]
        fame_lines = lines[fame_start : fame_start + 3]

        self.assertIn("Bravo", decks_lines[0])
        self.assertIn("Alpha", decks_lines[1])
        self.assertIn("Charlie", decks_lines[2])
        self.assertIn("Charlie", fame_lines[0])
        self.assertIn("Bravo", fame_lines[1])
        self.assertIn("Alpha", fame_lines[2])

