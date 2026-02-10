from __future__ import annotations

import unittest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

try:
    import sqlalchemy  # noqa: F401
    from sqlalchemy import text
except Exception:
    raise unittest.SkipTest("sqlalchemy not available")

try:
    import riverrace_import as rr
except Exception:
    raise unittest.SkipTest("riverrace_import dependencies not available")

from tests._db_harness import session_ctx
from tests._env import require_db_or_skip


def _session_provider(session):
    @asynccontextmanager
    async def _ctx():
        yield session

    return _ctx


def _build_week_item(
    *,
    season_id: int,
    section_index: int,
    clan_tag: str,
    clan_fame: int,
    participants: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "seasonId": season_id,
        "sectionIndex": section_index,
        "periodType": "completed",
        "standings": [
            {
                "clan": {
                    "tag": clan_tag,
                    "fame": clan_fame,
                    "participants": participants,
                }
            }
        ],
    }


class RiverRaceImportDBTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        require_db_or_skip()

    async def _count_state_rows(self, session, clan_tag: str) -> int:
        result = await session.execute(
            text(
                "SELECT COUNT(*) FROM river_race_state WHERE clan_tag = :clan_tag"
            ),
            {"clan_tag": clan_tag},
        )
        return int(result.scalar_one())

    async def _count_participation_rows(
        self, session, season_id: int, section_index: int
    ) -> int:
        result = await session.execute(
            text(
                "SELECT COUNT(*) FROM player_participation "
                "WHERE season_id = :season_id AND section_index = :section_index"
            ),
            {"season_id": season_id, "section_index": section_index},
        )
        return int(result.scalar_one())

    async def _count_participation_rows_all_weeks(self, session, player_prefix: str) -> int:
        result = await session.execute(
            text(
                "SELECT COUNT(*) FROM player_participation "
                "WHERE player_tag LIKE :prefix"
            ),
            {"prefix": f"{player_prefix}%"},
        )
        return int(result.scalar_one())

    async def test_import_riverrace_log_is_idempotent_for_same_week(self) -> None:
        clan_tag = "#DBRR_IDEMP"
        participants = [
            {
                "tag": "#DBRR_P1",
                "name": "P1",
                "fame": 1200,
                "repairPoints": 10,
                "boatAttacks": 2,
                "decksUsed": 8,
                "decksUsedToday": 4,
            },
            {
                "tag": "#DBRR_P2",
                "name": "P2",
                "fame": 900,
                "repairPoints": 0,
                "boatAttacks": 1,
                "decksUsed": 6,
                "decksUsedToday": 3,
            },
            {
                "tag": "#DBRR_P3",
                "name": "P3",
                "fame": 500,
                "repairPoints": 5,
                "boatAttacks": 0,
                "decksUsed": 4,
                "decksUsedToday": 2,
            },
        ]
        week_item = _build_week_item(
            season_id=701,
            section_index=0,
            clan_tag=clan_tag,
            clan_fame=2600,
            participants=participants,
        )
        client = MagicMock()
        client.get_river_race_log = AsyncMock(
            return_value={"items": [week_item]}
        )

        async with session_ctx() as session:
            with patch(
                "riverrace_import.get_api_client",
                new=AsyncMock(return_value=client),
            ), patch(
                "riverrace_import.get_session",
                new=_session_provider(session),
            ), patch(
                "riverrace_import.get_colosseum_index_map",
                new=AsyncMock(return_value={}),
            ):
                imported_1 = await rr.import_riverrace_log(weeks=1, clan_tag=clan_tag)
                state_rows_after_first = await self._count_state_rows(
                    session, clan_tag
                )
                part_rows_after_first = await self._count_participation_rows(
                    session, 701, 0
                )

                imported_2 = await rr.import_riverrace_log(weeks=1, clan_tag=clan_tag)
                state_rows_after_second = await self._count_state_rows(
                    session, clan_tag
                )
                part_rows_after_second = await self._count_participation_rows(
                    session, 701, 0
                )

        self.assertEqual((1, 3), imported_1)
        self.assertEqual((1, 3), imported_2)
        self.assertEqual(1, state_rows_after_first)
        self.assertEqual(1, state_rows_after_second)
        self.assertEqual(3, part_rows_after_first)
        self.assertEqual(3, part_rows_after_second)

    async def test_import_second_week_adds_new_rows_only(self) -> None:
        clan_tag = "#DBRR_MULTI"
        player_prefix = "#DBRR_M"
        participants_week_1 = [
            {
                "tag": f"{player_prefix}1",
                "name": "M1",
                "fame": 1000,
                "repairPoints": 5,
                "boatAttacks": 1,
                "decksUsed": 8,
                "decksUsedToday": 4,
            },
            {
                "tag": f"{player_prefix}2",
                "name": "M2",
                "fame": 800,
                "repairPoints": 2,
                "boatAttacks": 1,
                "decksUsed": 7,
                "decksUsedToday": 3,
            },
            {
                "tag": f"{player_prefix}3",
                "name": "M3",
                "fame": 600,
                "repairPoints": 1,
                "boatAttacks": 0,
                "decksUsed": 5,
                "decksUsedToday": 2,
            },
        ]
        participants_week_2 = [
            {
                "tag": f"{player_prefix}1",
                "name": "M1",
                "fame": 1100,
                "repairPoints": 6,
                "boatAttacks": 2,
                "decksUsed": 8,
                "decksUsedToday": 4,
            },
            {
                "tag": f"{player_prefix}2",
                "name": "M2",
                "fame": 850,
                "repairPoints": 2,
                "boatAttacks": 1,
                "decksUsed": 7,
                "decksUsedToday": 3,
            },
            {
                "tag": f"{player_prefix}3",
                "name": "M3",
                "fame": 650,
                "repairPoints": 2,
                "boatAttacks": 0,
                "decksUsed": 6,
                "decksUsedToday": 3,
            },
        ]
        week_1 = _build_week_item(
            season_id=702,
            section_index=0,
            clan_tag=clan_tag,
            clan_fame=2400,
            participants=participants_week_1,
        )
        week_2 = _build_week_item(
            season_id=702,
            section_index=1,
            clan_tag=clan_tag,
            clan_fame=2600,
            participants=participants_week_2,
        )

        client = MagicMock()
        client.get_river_race_log = AsyncMock(
            side_effect=[{"items": [week_1]}, {"items": [week_2]}]
        )

        async with session_ctx() as session:
            with patch(
                "riverrace_import.get_api_client",
                new=AsyncMock(return_value=client),
            ), patch(
                "riverrace_import.get_session",
                new=_session_provider(session),
            ), patch(
                "riverrace_import.get_colosseum_index_map",
                new=AsyncMock(return_value={}),
            ):
                imported_1 = await rr.import_riverrace_log(weeks=1, clan_tag=clan_tag)
                week_1_rows_before = await self._count_participation_rows(
                    session, 702, 0
                )
                total_rows_before = await self._count_participation_rows_all_weeks(
                    session, player_prefix
                )

                imported_2 = await rr.import_riverrace_log(weeks=1, clan_tag=clan_tag)
                week_1_rows_after = await self._count_participation_rows(
                    session, 702, 0
                )
                week_2_rows_after = await self._count_participation_rows(
                    session, 702, 1
                )
                total_rows_after = await self._count_participation_rows_all_weeks(
                    session, player_prefix
                )
                state_rows_after = await self._count_state_rows(session, clan_tag)

        self.assertEqual((1, 3), imported_1)
        self.assertEqual((1, 3), imported_2)
        self.assertEqual(3, week_1_rows_before)
        self.assertEqual(3, total_rows_before)
        self.assertEqual(3, week_1_rows_after)
        self.assertEqual(3, week_2_rows_after)
        self.assertEqual(6, total_rows_after)
        self.assertEqual(2, state_rows_after)

