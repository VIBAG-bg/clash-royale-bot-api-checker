import unittest
from contextlib import asynccontextmanager
from datetime import date
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

try:
    import sqlalchemy  # noqa: F401
except Exception:
    raise unittest.SkipTest("sqlalchemy not available")

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

    def scalar_one_or_none(self):
        return self._first_value


class _FakeSession:
    def __init__(self, results):
        self._results = list(results)

    async def execute(self, *args, **kwargs):
        if not self._results:
            raise AssertionError("Unexpected session.execute call")
        return self._results.pop(0)


class WarReportsTests(unittest.IsolatedAsyncioTestCase):
    def _extract_short_section(self, report: str, header: str) -> list[str]:
        lines = report.splitlines()
        if header not in lines:
            return []
        section_headers = {
            t("kick_short_section_candidates", "en"),
            t("kick_short_section_not_applicable", "en"),
            t("kick_short_section_revived", "en"),
            t("kick_short_section_new_members", "en"),
        }
        start = lines.index(header) + 1
        end = len(lines)
        for idx in range(start, len(lines)):
            if lines[idx] in section_headers:
                end = idx
                break
        return lines[start:end]

    def _count_player_lines(self, lines: list[str]) -> int:
        return len([line for line in lines if re.match(r"^\d+\) ", line)])

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
        @asynccontextmanager
        async def session_ctx():
            yield _FakeSession([_FakeResult(rows=[])])

        with patch("reports.get_session", new=session_ctx), patch(
            "reports._collect_wtd_donations",
            new=AsyncMock(return_value={}),
        ), patch(
            "reports.get_last_seen_map",
            new=AsyncMock(return_value={}),
        ):
            report = await build_kick_shortlist_report(
                [], None, "#CLAN", lang="en"
            )
        self.assertIn(t("kick_shortlist_none", "en"), report)

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
            yield _FakeSession(
                [
                    _FakeResult(rows=[]),
                ]
            )

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
            t("kick_short_section_new_members", "en"),
            report,
        )
        self.assertIn("Alice", report)

    async def test_kick_shortlist_candidates_capped_to_five(self) -> None:
        inactive = [
            {
                "player_tag": f"#P{i}",
                "player_name": f"P{i}",
                "decks_used": 0,
                "fame": 0,
            }
            for i in range(1, 7)
        ]
        tags = {row["player_tag"] for row in inactive}
        session = _FakeSession(
            [
                _FakeResult(
                    rows=[SimpleNamespace(season_id=128, section_index=3)]
                ),
                _FakeResult(first_value=date(2026, 1, 3)),
                _FakeResult(
                    rows=[
                        SimpleNamespace(
                            snapshot_date=date(2026, 1, 3), player_tag=tag
                        )
                        for tag in tags
                    ]
                ),
                _FakeResult(
                    rows=[
                        SimpleNamespace(
                            player_tag=tag,
                            season_id=128,
                            section_index=3,
                            decks_used=0,
                            fame=0,
                        )
                        for tag in tags
                    ]
                ),
            ]
        )

        @asynccontextmanager
        async def session_ctx():
            yield session

        with patch("reports.get_session", new=session_ctx), patch(
            "reports.get_first_snapshot_date_for_week",
            new=AsyncMock(return_value=date(2026, 1, 1)),
        ), patch(
            "reports.get_rolling_leaderboard",
            new=AsyncMock(return_value=(inactive, [])),
        ), patch(
            "reports.get_participation_week_counts",
            new=AsyncMock(return_value={tag: 4 for tag in tags}),
        ), patch(
            "reports.get_week_decks_map",
            new=AsyncMock(return_value={tag: 0 for tag in tags}),
        ), patch(
            "reports._collect_wtd_donations",
            new=AsyncMock(return_value={}),
        ), patch(
            "reports.get_last_seen_map",
            new=AsyncMock(return_value={}),
        ):
            report = await build_kick_shortlist_report(
                [(128, 3)], (128, 3), "#CLAN", lang="en"
            )

        candidate_lines = self._extract_short_section(
            report, t("kick_short_section_candidates", "en")
        )
        self.assertEqual(5, self._count_player_lines(candidate_lines))
        self.assertIn("P1", report)
        self.assertIn("P5", report)
        self.assertNotIn("P6", report)

    async def test_kick_shortlist_tops_up_with_weakest_rolling(self) -> None:
        inactive = [
            {
                "player_tag": "#A",
                "player_name": "A",
                "decks_used": 0,
                "fame": 0,
            },
            {
                "player_tag": "#B",
                "player_name": "B",
                "decks_used": 1,
                "fame": 10,
            },
            {
                "player_tag": "#C",
                "player_name": "C",
                "decks_used": 2,
                "fame": 20,
            },
        ]
        tags = {"#A", "#B", "#C"}
        session = _FakeSession(
            [
                _FakeResult(
                    rows=[SimpleNamespace(season_id=128, section_index=3)]
                ),
                _FakeResult(first_value=date(2026, 1, 3)),
                _FakeResult(
                    rows=[
                        SimpleNamespace(
                            snapshot_date=date(2026, 1, 3), player_tag="#A"
                        )
                    ]
                ),
                _FakeResult(
                    rows=[
                        SimpleNamespace(
                            player_tag=tag,
                            season_id=128,
                            section_index=3,
                            decks_used=0,
                            fame=0,
                        )
                        for tag in tags
                    ]
                ),
            ]
        )

        @asynccontextmanager
        async def session_ctx():
            yield session

        with patch("reports.get_session", new=session_ctx), patch(
            "reports.get_first_snapshot_date_for_week",
            new=AsyncMock(return_value=date(2026, 1, 1)),
        ), patch(
            "reports.get_rolling_leaderboard",
            new=AsyncMock(return_value=(inactive, [])),
        ), patch(
            "reports.get_participation_week_counts",
            new=AsyncMock(return_value={tag: 4 for tag in tags}),
        ), patch(
            "reports.get_week_decks_map",
            new=AsyncMock(return_value={tag: 0 for tag in tags}),
        ), patch(
            "reports._collect_wtd_donations",
            new=AsyncMock(return_value={}),
        ), patch(
            "reports.get_last_seen_map",
            new=AsyncMock(return_value={}),
        ):
            report = await build_kick_shortlist_report(
                [(128, 3)], (128, 3), "#CLAN", lang="en"
            )

        candidate_lines = self._extract_short_section(
            report, t("kick_short_section_candidates", "en")
        )
        self.assertEqual(3, self._count_player_lines(candidate_lines))
        self.assertIn("A", "\n".join(candidate_lines))
        self.assertIn("B", "\n".join(candidate_lines))
        self.assertIn("C", "\n".join(candidate_lines))
        self.assertIn(t("kick_short_status_fallback_weakest", "en"), report)
        self.assertNotIn(t("kick_short_section_not_applicable", "en"), report)

    async def test_kick_shortlist_keeps_four_without_top_up(self) -> None:
        inactive = [
            {
                "player_tag": f"#P{i}",
                "player_name": f"P{i}",
                "decks_used": 0,
                "fame": 0,
            }
            for i in range(1, 5)
        ]
        tags = {row["player_tag"] for row in inactive}
        session = _FakeSession(
            [
                _FakeResult(
                    rows=[SimpleNamespace(season_id=128, section_index=3)]
                ),
                _FakeResult(first_value=date(2026, 1, 3)),
                _FakeResult(
                    rows=[
                        SimpleNamespace(
                            snapshot_date=date(2026, 1, 3), player_tag=tag
                        )
                        for tag in tags
                    ]
                ),
                _FakeResult(
                    rows=[
                        SimpleNamespace(
                            player_tag=tag,
                            season_id=128,
                            section_index=3,
                            decks_used=0,
                            fame=0,
                        )
                        for tag in tags
                    ]
                ),
            ]
        )

        @asynccontextmanager
        async def session_ctx():
            yield session

        with patch("reports.get_session", new=session_ctx), patch(
            "reports.get_first_snapshot_date_for_week",
            new=AsyncMock(return_value=date(2026, 1, 1)),
        ), patch(
            "reports.get_rolling_leaderboard",
            new=AsyncMock(return_value=(inactive, [])),
        ), patch(
            "reports.get_participation_week_counts",
            new=AsyncMock(return_value={tag: 4 for tag in tags}),
        ), patch(
            "reports.get_week_decks_map",
            new=AsyncMock(return_value={tag: 0 for tag in tags}),
        ), patch(
            "reports._collect_wtd_donations",
            new=AsyncMock(return_value={}),
        ), patch(
            "reports.get_last_seen_map",
            new=AsyncMock(return_value={}),
        ):
            report = await build_kick_shortlist_report(
                [(128, 3)], (128, 3), "#CLAN", lang="en"
            )

        candidate_lines = self._extract_short_section(
            report, t("kick_short_section_candidates", "en")
        )
        self.assertEqual(4, self._count_player_lines(candidate_lines))
        self.assertNotIn(t("kick_short_status_fallback_weakest", "en"), report)
        self.assertNotIn(t("kick_short_status_fallback_revived", "en"), report)

    async def test_kick_shortlist_tops_up_from_revived_when_needed(self) -> None:
        inactive = [
            {
                "player_tag": "#A",
                "player_name": "A",
                "decks_used": 0,
                "fame": 0,
            },
            {
                "player_tag": "#D",
                "player_name": "D",
                "decks_used": 0,
                "fame": 0,
            },
            {
                "player_tag": "#E",
                "player_name": "E",
                "decks_used": 0,
                "fame": 0,
            },
        ]
        tags = {"#A", "#D", "#E"}
        session = _FakeSession(
            [
                _FakeResult(
                    rows=[SimpleNamespace(season_id=128, section_index=3)]
                ),
                _FakeResult(first_value=date(2026, 1, 3)),
                _FakeResult(
                    rows=[
                        SimpleNamespace(
                            snapshot_date=date(2026, 1, 3), player_tag="#A"
                        )
                    ]
                ),
                _FakeResult(
                    rows=[
                        SimpleNamespace(
                            player_tag=tag,
                            season_id=128,
                            section_index=3,
                            decks_used=0,
                            fame=0,
                        )
                        for tag in tags
                    ]
                ),
            ]
        )

        @asynccontextmanager
        async def session_ctx():
            yield session

        with patch("reports.get_session", new=session_ctx), patch(
            "reports.get_first_snapshot_date_for_week",
            new=AsyncMock(return_value=date(2026, 1, 1)),
        ), patch(
            "reports.get_rolling_leaderboard",
            new=AsyncMock(return_value=(inactive, [])),
        ), patch(
            "reports.get_participation_week_counts",
            new=AsyncMock(return_value={tag: 4 for tag in tags}),
        ), patch(
            "reports.get_week_decks_map",
            new=AsyncMock(return_value={"#A": 0, "#D": 8, "#E": 9}),
        ), patch(
            "reports._collect_wtd_donations",
            new=AsyncMock(return_value={}),
        ), patch(
            "reports.get_last_seen_map",
            new=AsyncMock(return_value={}),
        ):
            report = await build_kick_shortlist_report(
                [(128, 3)], (128, 3), "#CLAN", lang="en"
            )

        candidate_lines = self._extract_short_section(
            report, t("kick_short_section_candidates", "en")
        )
        self.assertEqual(3, self._count_player_lines(candidate_lines))
        self.assertIn("A", "\n".join(candidate_lines))
        self.assertIn("D", "\n".join(candidate_lines))
        self.assertIn("E", "\n".join(candidate_lines))
        self.assertIn(t("kick_short_status_fallback_revived", "en"), report)

        revived_lines = self._extract_short_section(
            report, t("kick_short_section_revived", "en")
        )
        self.assertFalse(revived_lines)

    async def test_kick_shortlist_tops_up_from_new_members_when_needed(self) -> None:
        inactive = [
            {
                "player_tag": "#A",
                "player_name": "A",
                "decks_used": 0,
                "fame": 0,
            },
            {
                "player_tag": "#B",
                "player_name": "B",
                "decks_used": 0,
                "fame": 0,
            },
            {
                "player_tag": "#N2",
                "player_name": "N2",
                "decks_used": 0,
                "fame": 0,
            },
            {
                "player_tag": "#N1",
                "player_name": "N1",
                "decks_used": 0,
                "fame": 0,
            },
        ]
        tags = {"#A", "#B", "#N2", "#N1"}
        session = _FakeSession(
            [
                _FakeResult(
                    rows=[SimpleNamespace(season_id=128, section_index=3)]
                ),
                _FakeResult(first_value=date(2026, 1, 3)),
                _FakeResult(
                    rows=[
                        SimpleNamespace(
                            snapshot_date=date(2026, 1, 3), player_tag="#A"
                        )
                    ]
                ),
                _FakeResult(
                    rows=[
                        SimpleNamespace(
                            player_tag="#A",
                            season_id=128,
                            section_index=3,
                            decks_used=0,
                            fame=0,
                        )
                    ]
                ),
            ]
        )

        @asynccontextmanager
        async def session_ctx():
            yield session

        with patch("reports.get_session", new=session_ctx), patch(
            "reports.get_first_snapshot_date_for_week",
            new=AsyncMock(return_value=date(2026, 1, 1)),
        ), patch(
            "reports.get_rolling_leaderboard",
            new=AsyncMock(return_value=(inactive, [])),
        ), patch(
            "reports.get_participation_week_counts",
            new=AsyncMock(
                return_value={"#A": 4, "#B": 3, "#N2": 2, "#N1": 1}
            ),
        ), patch(
            "reports.get_week_decks_map",
            new=AsyncMock(return_value={tag: 0 for tag in tags}),
        ), patch(
            "reports._collect_wtd_donations",
            new=AsyncMock(return_value={}),
        ), patch(
            "reports.get_last_seen_map",
            new=AsyncMock(return_value={}),
        ):
            report = await build_kick_shortlist_report(
                [(128, 3)], (128, 3), "#CLAN", lang="en"
            )

        candidate_lines = self._extract_short_section(
            report, t("kick_short_section_candidates", "en")
        )
        self.assertEqual(3, self._count_player_lines(candidate_lines))
        self.assertIn("A", "\n".join(candidate_lines))
        self.assertIn("B", "\n".join(candidate_lines))
        self.assertIn("N2", "\n".join(candidate_lines))
        self.assertIn(
            t("kick_short_status_fallback_new_member_zero", "en"), report
        )

        new_member_lines = self._extract_short_section(
            report, t("kick_short_section_new_members", "en")
        )
        self.assertIn("N1", "\n".join(new_member_lines))
        self.assertNotIn("N2", "\n".join(new_member_lines))

    async def test_kick_shortlist_tops_up_from_pass_as_last_fallback(self) -> None:
        inactive = [
            {
                "player_tag": "#A",
                "player_name": "A",
                "decks_used": 0,
                "fame": 0,
            },
            {
                "player_tag": "#B",
                "player_name": "B",
                "decks_used": 0,
                "fame": 0,
            },
            {
                "player_tag": "#C",
                "player_name": "C",
                "decks_used": 0,
                "fame": 0,
            },
        ]
        tags = {"#A", "#B", "#C"}
        session = _FakeSession(
            [
                _FakeResult(
                    rows=[SimpleNamespace(season_id=128, section_index=3)]
                ),
                _FakeResult(first_value=date(2026, 1, 3)),
                _FakeResult(
                    rows=[
                        SimpleNamespace(
                            snapshot_date=date(2026, 1, 3), player_tag=tag
                        )
                        for tag in tags
                    ]
                ),
                _FakeResult(
                    rows=[
                        SimpleNamespace(
                            player_tag="#A",
                            season_id=128,
                            section_index=3,
                            decks_used=0,
                            fame=0,
                        ),
                        SimpleNamespace(
                            player_tag="#B",
                            season_id=128,
                            section_index=3,
                            decks_used=8,
                            fame=1500,
                        ),
                        SimpleNamespace(
                            player_tag="#C",
                            season_id=128,
                            section_index=3,
                            decks_used=8,
                            fame=1500,
                        ),
                    ]
                ),
            ]
        )

        @asynccontextmanager
        async def session_ctx():
            yield session

        with patch("reports.get_session", new=session_ctx), patch(
            "reports.get_first_snapshot_date_for_week",
            new=AsyncMock(return_value=date(2026, 1, 1)),
        ), patch(
            "reports.get_rolling_leaderboard",
            new=AsyncMock(return_value=(inactive, [])),
        ), patch(
            "reports.get_participation_week_counts",
            new=AsyncMock(return_value={tag: 4 for tag in tags}),
        ), patch(
            "reports.get_week_decks_map",
            new=AsyncMock(return_value={tag: 0 for tag in tags}),
        ), patch(
            "reports._collect_wtd_donations",
            new=AsyncMock(return_value={}),
        ), patch(
            "reports.get_last_seen_map",
            new=AsyncMock(return_value={}),
        ):
            report = await build_kick_shortlist_report(
                [(128, 3)], (128, 3), "#CLAN", lang="en"
            )

        candidate_lines = self._extract_short_section(
            report, t("kick_short_section_candidates", "en")
        )
        self.assertEqual(3, self._count_player_lines(candidate_lines))
        self.assertIn("A", "\n".join(candidate_lines))
        self.assertIn("B", "\n".join(candidate_lines))
        self.assertIn("C", "\n".join(candidate_lines))
        self.assertIn(
            t("kick_short_status_fallback_pass_weakest", "en"), report
        )

    async def test_kick_shortlist_emergency_nearest_when_zero_candidates(self) -> None:
        inactive = [
            {
                "player_tag": "#P1",
                "player_name": "P1",
                "decks_used": 8,
                "fame": 1700,
            },
            {
                "player_tag": "#P2",
                "player_name": "P2",
                "decks_used": 10,
                "fame": 2100,
            },
        ]
        tags = {"#P1", "#P2"}
        session = _FakeSession(
            [
                _FakeResult(
                    rows=[SimpleNamespace(season_id=128, section_index=3)]
                ),
                _FakeResult(first_value=date(2026, 1, 3)),
                _FakeResult(
                    rows=[
                        SimpleNamespace(
                            snapshot_date=date(2026, 1, 3), player_tag=tag
                        )
                        for tag in tags
                    ]
                ),
                _FakeResult(
                    rows=[
                        SimpleNamespace(
                            player_tag="#P1",
                            season_id=128,
                            section_index=3,
                            decks_used=8,
                            fame=1700,
                        ),
                        SimpleNamespace(
                            player_tag="#P2",
                            season_id=128,
                            section_index=3,
                            decks_used=10,
                            fame=2100,
                        ),
                    ]
                ),
            ]
        )

        @asynccontextmanager
        async def session_ctx():
            yield session

        with patch("reports.get_session", new=session_ctx), patch(
            "reports.get_first_snapshot_date_for_week",
            new=AsyncMock(return_value=date(2026, 1, 1)),
        ), patch(
            "reports.get_rolling_leaderboard",
            new=AsyncMock(return_value=(inactive, [])),
        ), patch(
            "reports.get_participation_week_counts",
            new=AsyncMock(return_value={tag: 4 for tag in tags}),
        ), patch(
            "reports.get_week_decks_map",
            new=AsyncMock(return_value={"#P1": 0, "#P2": 0}),
        ), patch(
            "reports._collect_wtd_donations",
            new=AsyncMock(return_value={}),
        ), patch(
            "reports.get_last_seen_map",
            new=AsyncMock(return_value={}),
        ):
            report = await build_kick_shortlist_report(
                [(128, 3)], (128, 3), "#CLAN", lang="en"
            )

        candidate_lines = self._extract_short_section(
            report, t("kick_short_section_candidates", "en")
        )
        self.assertGreaterEqual(self._count_player_lines(candidate_lines), 1)
        self.assertIn("P1", "\n".join(candidate_lines))
        self.assertIn(t("kick_short_status_fallback_nearest", "en"), report)
        self.assertIn(
            t(
                "kick_short_nearest_rule_line",
                "en",
                decks_req=8,
                fame_req=1500,
                decks=8,
                fame=1700,
                decks_delta=0,
                fame_delta=200,
            ),
            report,
        )
