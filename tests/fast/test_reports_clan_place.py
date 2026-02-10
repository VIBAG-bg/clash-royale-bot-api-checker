import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

try:
    import sqlalchemy  # noqa: F401
except Exception:
    raise unittest.SkipTest("sqlalchemy not available")

from i18n import t
from reports import build_clan_place_report


class ClanPlaceReportTests(unittest.IsolatedAsyncioTestCase):
    async def test_fresh_snapshot_reused_without_capture(self) -> None:
        snapshot = {
            "snapshot_ts": datetime.now(timezone.utc),
            "our_rank": 2,
            "our_fame": 3000,
            "total_clans": 5,
            "period_type": "riverRace",
            "top5_json": [
                {"rank": 1, "tag": "#AAA", "name": "Alpha", "fame": 3500},
                {"rank": 2, "tag": "#CLAN", "name": "Our Clan", "fame": 3000},
            ],
        }
        with patch(
            "reports._resolve_active_week_key",
            new=AsyncMock(return_value=(101, 1)),
        ), patch(
            "reports.get_latest_river_race_place_snapshot",
            new=AsyncMock(return_value=snapshot),
        ), patch(
            "reports.capture_clan_place_snapshot",
            new=AsyncMock(),
        ) as capture_mock, patch(
            "reports.get_river_race_state_for_week",
            new=AsyncMock(return_value={"is_colosseum": False}),
        ):
            report = await build_clan_place_report("#clan", lang="en")

        capture_mock.assert_not_awaited()
        self.assertIn(t("clan_place_title", "en"), report)
        self.assertIn(
            t(
                "clan_place_week_line",
                "en",
                season=101,
                week=2,
                period=t("current_war_river_race", "en"),
            ),
            report,
        )
        self.assertIn(
            t("clan_place_rank_line", "en", rank=2, total=5),
            report,
        )

    async def test_missing_snapshot_uses_capture_fallback(self) -> None:
        fallback_snapshot = {
            "our_rank": 1,
            "our_fame": 4200,
            "total_clans": 4,
            "period_type": "colosseum",
            "top5_json": [
                {"rank": 1, "tag": "#CLAN", "name": "Our Clan", "fame": 4200},
            ],
        }
        with patch(
            "reports._resolve_active_week_key",
            new=AsyncMock(return_value=(202, 0)),
        ), patch(
            "reports.get_latest_river_race_place_snapshot",
            new=AsyncMock(return_value=None),
        ), patch(
            "reports.capture_clan_place_snapshot",
            new=AsyncMock(return_value=fallback_snapshot),
        ) as capture_mock, patch(
            "reports.get_river_race_state_for_week",
            new=AsyncMock(return_value=None),
        ):
            report = await build_clan_place_report("clan", lang="en")

        capture_mock.assert_awaited_once_with("#CLAN")
        self.assertIn(
            t(
                "clan_place_week_line",
                "en",
                season=202,
                week=1,
                period=t("current_war_colosseum", "en"),
            ),
            report,
        )
        self.assertIn(
            t("clan_place_rank_line", "en", rank=1, total=4),
            report,
        )

