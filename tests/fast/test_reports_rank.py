import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

try:
    import sqlalchemy  # noqa: F401
except Exception:
    raise unittest.SkipTest("sqlalchemy not available")

from i18n import t
from reports import build_rank_report, collect_clan_rank_snapshot


class RankReportTests(unittest.IsolatedAsyncioTestCase):
    async def test_collect_rank_snapshot_fresh_uses_db_without_api(self) -> None:
        fresh = {
            "clan_tag": "#CLAN",
            "location_id": 57000000,
            "snapshot_at": datetime.now(timezone.utc),
            "war_rank": 11,
        }
        api_factory = AsyncMock()
        with patch("reports.RANKING_SNAPSHOT_ENABLED", True), patch(
            "reports.RANKING_LOCATION_ID",
            57000000,
        ), patch(
            "reports.get_latest_clan_rank_snapshot",
            new=AsyncMock(return_value=fresh),
        ) as latest_mock, patch(
            "reports.get_api_client",
            new=api_factory,
        ):
            snapshot = await collect_clan_rank_snapshot("#clan")
        self.assertIs(fresh, snapshot)
        latest_mock.assert_awaited_once_with("#CLAN", 57000000)
        api_factory.assert_not_awaited()

    async def test_collect_rank_snapshot_stale_falls_back_to_api(self) -> None:
        stale = {
            "clan_tag": "#CLAN",
            "location_id": 57000000,
            "snapshot_at": datetime.now(timezone.utc) - timedelta(days=2),
            "war_rank": 22,
        }
        final = {
            "clan_tag": "#CLAN",
            "location_id": 57000000,
            "snapshot_at": datetime.now(timezone.utc),
            "war_rank": 7,
        }
        api_client = MagicMock()
        api_client.get_clan = AsyncMock(
            return_value={
                "name": "Clan Name",
                "clanScore": 12345,
                "clanWarTrophies": 4321,
                "members": 50,
                "location": {"localizedName": "Earth"},
            }
        )
        api_client.get_location_clan_rankings = AsyncMock(
            return_value=[
                {
                    "tag": "#CLAN",
                    "rank": 9,
                    "previousRank": 10,
                    "name": "Clan Name",
                    "clanScore": 12345,
                }
            ]
        )
        api_client.get_location_clanwar_rankings = AsyncMock(
            return_value=[
                {
                    "tag": "#CLAN",
                    "rank": 7,
                    "previousRank": 8,
                    "name": "Clan Name",
                    "clanWarTrophies": 4321,
                }
            ]
        )

        with patch("reports.RANKING_SNAPSHOT_ENABLED", True), patch(
            "reports.RANKING_LOCATION_ID",
            57000000,
        ), patch(
            "reports.get_latest_clan_rank_snapshot",
            new=AsyncMock(side_effect=[stale, stale, final]),
        ) as latest_mock, patch(
            "reports.get_api_client",
            new=AsyncMock(return_value=api_client),
        ) as api_factory, patch(
            "reports.insert_clan_rank_snapshot",
            new=AsyncMock(),
        ) as insert_mock:
            snapshot = await collect_clan_rank_snapshot("#clan")

        self.assertIs(final, snapshot)
        api_factory.assert_awaited_once()
        api_client.get_clan.assert_awaited_once()
        api_client.get_location_clan_rankings.assert_awaited_once()
        api_client.get_location_clanwar_rankings.assert_awaited_once()
        insert_mock.assert_awaited_once()
        self.assertEqual(3, latest_mock.await_count)

    async def test_build_rank_report_formats_header_and_clan_line(self) -> None:
        snapshot = {
            "clan_tag": "#CLAN",
            "location_id": 57000000,
            "location_name": "International",
            "ladder_rank": 123,
            "ladder_clan_score": 45678,
            "war_rank": 88,
            "war_clan_score": 4321,
            "clan_war_trophies": 4321,
            "neighbors_ladder_json": [],
            "neighbors_war_json": [],
            "ladder_points_to_overtake_above": None,
            "war_points_to_overtake_above": None,
            "raw_source": {"clan_name": "Clan Name"},
        }
        with patch(
            "reports.collect_clan_rank_snapshot",
            new=AsyncMock(return_value=snapshot),
        ) as collect_mock, patch(
            "reports.get_clan_rank_snapshot_at_or_before",
            new=AsyncMock(return_value=None),
        ):
            report = await build_rank_report("#clan", lang="en")

        self.assertIn(t("rank_title", "en"), report)
        self.assertIn(
            t("rank_clan_line", "en", name="Clan Name", tag="#CLAN"),
            report,
        )
        self.assertIn(
            t("rank_location_line", "en", location="International"),
            report,
        )
        collect_mock.assert_awaited_once_with("#clan", force=False)

