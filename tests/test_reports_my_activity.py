import unittest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

try:
    import sqlalchemy  # noqa: F401
except Exception:
    raise unittest.SkipTest("sqlalchemy not available")

from i18n import t
from reports import build_my_activity_report


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


class MyActivityReportTests(unittest.IsolatedAsyncioTestCase):
    async def test_my_activity_report_no_data(self) -> None:
        session = _FakeSession([])

        @asynccontextmanager
        async def session_ctx():
            yield session

        with patch(
            "reports._resolve_active_week_key",
            new=AsyncMock(return_value=None),
        ), patch(
            "reports.get_river_race_state_for_week",
            new=AsyncMock(return_value=None),
        ), patch(
            "reports.get_latest_river_race_state",
            new=AsyncMock(return_value=None),
        ), patch(
            "reports.get_current_member_tags",
            new=AsyncMock(return_value=set()),
        ), patch(
            "reports.get_last_weeks_from_db",
            new=AsyncMock(return_value=[]),
        ), patch(
            "reports.get_current_wtd_donations",
            new=AsyncMock(return_value={}),
        ), patch(
            "reports.get_donations_weekly_sums",
            new=AsyncMock(return_value={}),
        ), patch(
            "reports.get_last_seen_map",
            new=AsyncMock(return_value={}),
        ), patch(
            "reports.get_clan_wtd_donation_average",
            new=AsyncMock(return_value=None),
        ), patch(
            "reports.get_session",
            new=session_ctx,
        ):
            report = await build_my_activity_report(
                "#AAA", "Player", "#CLAN", lang="en"
            )
        self.assertIn(t("my_activity_title", "en"), report)
        self.assertIn(
            t("my_activity_player_line", "en", player="Player"), report
        )
        self.assertIn(
            t("my_activity_tag_line", "en", tag="#AAA"), report
        )
        self.assertIn(t("my_activity_summary_header", "en"), report)

    async def test_my_activity_report_training_minimal(self) -> None:
        session = _FakeSession([_FakeResult(first_value=None)])

        @asynccontextmanager
        async def session_ctx():
            yield session

        training_state = {"period_type": "training", "is_colosseum": False}
        with patch(
            "reports._resolve_active_week_key",
            new=AsyncMock(return_value=(0, 0)),
        ), patch(
            "reports.get_river_race_state_for_week",
            new=AsyncMock(return_value=training_state),
        ), patch(
            "reports.get_latest_river_race_state",
            new=AsyncMock(return_value=None),
        ), patch(
            "reports.get_current_member_tags",
            new=AsyncMock(return_value=set()),
        ), patch(
            "reports.get_last_weeks_from_db",
            new=AsyncMock(return_value=[]),
        ), patch(
            "reports.get_current_wtd_donations",
            new=AsyncMock(return_value={}),
        ), patch(
            "reports.get_donations_weekly_sums",
            new=AsyncMock(return_value={}),
        ), patch(
            "reports.get_last_seen_map",
            new=AsyncMock(return_value={}),
        ), patch(
            "reports.get_clan_wtd_donation_average",
            new=AsyncMock(return_value=None),
        ), patch(
            "reports.get_session",
            new=session_ctx,
        ):
            report = await build_my_activity_report(
                "#AAA", "Player", "#CLAN", lang="en"
            )
        self.assertIn(t("my_activity_training_notice", "en"), report)
        self.assertIn(t("my_activity_summary_header", "en"), report)
