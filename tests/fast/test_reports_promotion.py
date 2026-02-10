import unittest
from unittest.mock import AsyncMock, patch

try:
    import sqlalchemy  # noqa: F401
except Exception:
    raise unittest.SkipTest("sqlalchemy not available")

from i18n import t
from reports import build_promotion_candidates_report


class PromotionReportTests(unittest.IsolatedAsyncioTestCase):
    async def test_filters_protected_and_new_members(self) -> None:
        weeks = [(120, 1)]
        member_rows = [
            {
                "player_tag": "#PROT",
                "player_name": "ProtectedPlayer",
                "role": "member",
            },
            {"player_tag": "#NEW", "player_name": "Newbie", "role": "member"},
            {"player_tag": "#ELIG", "player_name": "Eligible", "role": "member"},
        ]
        war_stats = {
            "#PROT": {"weeks_played": 8, "active_weeks": 8, "avg_decks": 12.0, "avg_fame": 1200.0},
            "#NEW": {"weeks_played": 0, "active_weeks": 0, "avg_decks": 1.0, "avg_fame": 100.0},
            "#ELIG": {"weeks_played": 8, "active_weeks": 8, "avg_decks": 12.0, "avg_fame": 1300.0},
        }
        donations_rows = [
            {"player_tag": "#ELIG", "donations_sum": 200, "weeks_present": 4},
            {"player_tag": "#NEW", "donations_sum": 10, "weeks_present": 1},
        ]

        with patch.multiple(
            "reports",
            PROMOTE_MIN_WEEKS_PLAYED_ELDER=1,
            PROMOTE_MIN_ACTIVE_WEEKS_ELDER=1,
            PROMOTE_MIN_AVG_DECKS_ELDER=1,
            PROMOTE_ELDER_LIMIT=5,
            PROMOTE_COLEADER_LIMIT=1,
        ), patch(
            "reports.PROTECTED_TAGS_NORMALIZED",
            {"#PROT"},
        ), patch(
            "reports.get_last_completed_weeks",
            new=AsyncMock(return_value=weeks),
        ), patch(
            "reports.get_current_members_snapshot",
            new=AsyncMock(return_value=member_rows),
        ), patch(
            "reports.get_war_stats_for_weeks",
            new=AsyncMock(return_value=war_stats),
        ), patch(
            "reports.get_alltime_weeks_played",
            new=AsyncMock(return_value={"#ELIG": 20, "#NEW": 1}),
        ), patch(
            "reports.get_donation_weekly_sums_for_window",
            new=AsyncMock(return_value=(donations_rows, 4)),
        ):
            report = await build_promotion_candidates_report("#CLAN", lang="en")

        self.assertIn("Eligible", report)
        self.assertNotIn("ProtectedPlayer", report)
        self.assertNotIn("Newbie", report)
        self.assertIn(t("promotion_title", "en"), report)
        self.assertIn(t("promotion_notes_header", "en"), report)

    async def test_score_ordering_stable_on_ties(self) -> None:
        weeks = [(121, 0)]
        member_rows = [
            {"player_tag": "#B", "player_name": "Beta", "role": "member"},
            {"player_tag": "#A", "player_name": "Alpha", "role": "member"},
        ]
        war_stats = {
            "#A": {"weeks_played": 8, "active_weeks": 6, "avg_decks": 11.0, "avg_fame": 1000.0},
            "#B": {"weeks_played": 8, "active_weeks": 6, "avg_decks": 11.0, "avg_fame": 1000.0},
        }
        donations_rows = [
            {"player_tag": "#A", "donations_sum": 120, "weeks_present": 4},
            {"player_tag": "#B", "donations_sum": 120, "weeks_present": 4},
        ]

        with patch.multiple(
            "reports",
            PROMOTE_MIN_WEEKS_PLAYED_ELDER=1,
            PROMOTE_MIN_ACTIVE_WEEKS_ELDER=1,
            PROMOTE_MIN_AVG_DECKS_ELDER=1,
            PROMOTE_ELDER_LIMIT=2,
            PROMOTE_COLEADER_LIMIT=0,
        ), patch(
            "reports.PROTECTED_TAGS_NORMALIZED",
            set(),
        ), patch(
            "reports.get_last_completed_weeks",
            new=AsyncMock(return_value=weeks),
        ), patch(
            "reports.get_current_members_snapshot",
            new=AsyncMock(return_value=member_rows),
        ), patch(
            "reports.get_war_stats_for_weeks",
            new=AsyncMock(return_value=war_stats),
        ), patch(
            "reports.get_alltime_weeks_played",
            new=AsyncMock(return_value={"#A": 20, "#B": 20}),
        ), patch(
            "reports.get_donation_weekly_sums_for_window",
            new=AsyncMock(return_value=(donations_rows, 4)),
        ):
            report = await build_promotion_candidates_report("#CLAN", lang="en")

        self.assertIn(
            t("promotion_elder_header", "en", count=2),
            report,
        )
        self.assertIn(t("promotion_co_leader_header", "en"), report)
        self.assertLess(report.index("Alpha"), report.index("Beta"))
