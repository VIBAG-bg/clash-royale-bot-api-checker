import unittest
from unittest.mock import AsyncMock, patch

try:
    import sqlalchemy  # noqa: F401
except Exception:
    raise unittest.SkipTest("sqlalchemy not available")

from i18n import t
from reports import build_donations_report


class DonationsReportTests(unittest.IsolatedAsyncioTestCase):
    async def test_donations_report_with_data(self) -> None:
        member_rows = [
            {"player_tag": "#A", "player_name": "Alice", "donations": 50},
            {"player_tag": "#B", "player_name": "Bob", "donations": 30},
            {"player_tag": "#C", "player_name": "Cara", "donations": None},
        ]
        weekly_rows = [
            {
                "player_tag": "#A",
                "player_name": "Alice",
                "donations_sum": 200,
                "weeks_present": 8,
            },
            {
                "player_tag": "#B",
                "player_name": "Bob",
                "donations_sum": 100,
                "weeks_present": 7,
            },
        ]
        with patch(
            "reports.get_current_members_with_wtd_donations",
            new=AsyncMock(return_value=member_rows),
        ), patch(
            "reports.get_donation_weekly_sums_for_window",
            new=AsyncMock(return_value=(weekly_rows, 8)),
        ), patch(
            "reports.DONATION_WEEKS_WINDOW", 8
        ):
            report = await build_donations_report(
                "#CLAN", "ClanName", lang="en"
            )
        self.assertIn(t("donations_title", "en"), report)
        self.assertIn(
            t(
                "donations_top_line",
                "en",
                index=1,
                name="Alice",
                cards="50",
            ),
            report,
        )
        self.assertIn(
            t("donations_window_header", "en", weeks=8), report
        )
        self.assertIn(
            t(
                "donations_window_line",
                "en",
                index=1,
                name="Alice",
                cards=200,
                weeks=8,
                window=8,
            ),
            report,
        )
        self.assertIn(
            t("donations_totals_window_header", "en", weeks=8),
            report,
        )

    async def test_donations_report_no_data(self) -> None:
        with patch(
            "reports.get_current_members_with_wtd_donations",
            new=AsyncMock(return_value=[]),
        ), patch(
            "reports.get_donation_weekly_sums_for_window",
            new=AsyncMock(return_value=([], 0)),
        ), patch(
            "reports.DONATION_WEEKS_WINDOW", 8
        ):
            report = await build_donations_report(
                "#CLAN", None, lang="en"
            )
        self.assertIn(t("donations_none", "en"), report)
        self.assertIn(t("donations_window_none", "en"), report)
