from contextlib import asynccontextmanager
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

try:
    from cr_api import ClashRoyaleAPIError
    import riverrace_import as rr
except Exception:
    raise unittest.SkipTest("riverrace_import dependencies not available")


class TagsMatchTests(unittest.TestCase):
    def test_hash_and_plain_match(self) -> None:
        self.assertTrue(rr._tags_match("#ABC", "ABC"))

    def test_case_insensitive_match(self) -> None:
        self.assertTrue(rr._tags_match("#aBc", "AbC"))

    def test_whitespace_is_trimmed(self) -> None:
        self.assertTrue(rr._tags_match("  #ABC  ", "abc"))


class FindClanEntryTests(unittest.TestCase):
    def test_returns_entry_when_present(self) -> None:
        expected = {"tag": "#AAA", "name": "Alpha"}
        standings = [{"clan": {"tag": "#BBB"}}, {"clan": expected}]
        found = rr._find_clan_entry(standings, "AAA")
        self.assertIs(expected, found)

    def test_returns_none_when_missing(self) -> None:
        standings = [{"clan": {"tag": "#BBB"}}, {"clan": {"tag": "#CCC"}}]
        self.assertIsNone(rr._find_clan_entry(standings, "#AAA"))

    def test_multiple_entries_choose_first_matching(self) -> None:
        first = {"tag": "#AAA", "name": "First"}
        second = {"tag": "#aaa", "name": "Second"}
        standings = [
            {"clan": {"tag": "#BBB"}},
            {"clan": first},
            {"clan": second},
        ]
        found = rr._find_clan_entry(standings, "AAA")
        self.assertIs(first, found)


class ResolveIsColosseumTests(unittest.TestCase):
    def test_colosseum_marker_true(self) -> None:
        item = {"isColosseum": 1}
        self.assertTrue(rr._resolve_is_colosseum(item, 100, 3, {}))

    def test_non_colosseum_false(self) -> None:
        item = {"isColosseum": 0}
        self.assertFalse(rr._resolve_is_colosseum(item, 100, 3, {100: 3}))


class ImportFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_last_completed_weeks_falls_back_to_db_on_api_error(self) -> None:
        client = AsyncMock()
        client.get_river_race_log = AsyncMock(
            side_effect=ClashRoyaleAPIError(503, "unavailable")
        )
        db_fallback = AsyncMock(return_value=[])

        with patch("riverrace_import.get_api_client", new=AsyncMock(return_value=client)), patch(
            "riverrace_import.get_last_completed_weeks_from_db",
            new=db_fallback,
        ):
            weeks = await rr.get_last_completed_weeks(3, clan_tag="#CLAN")

        self.assertEqual([], weeks)
        db_fallback.assert_awaited_once_with("#CLAN", limit=3)

    async def test_import_riverrace_log_empty_items_returns_zero_without_writes(self) -> None:
        client = AsyncMock()
        client.get_river_race_log = AsyncMock(return_value={"items": []})
        save_state = AsyncMock()
        save_participation = AsyncMock()
        get_session = MagicMock()

        with patch("riverrace_import.get_api_client", new=AsyncMock(return_value=client)), patch(
            "riverrace_import.get_session",
            new=get_session,
        ), patch(
            "riverrace_import.save_river_race_state",
            new=save_state,
        ), patch(
            "riverrace_import.save_player_participation",
            new=save_participation,
        ):
            imported_weeks, imported_players = await rr.import_riverrace_log(
                weeks=5,
                clan_tag="#CLAN",
            )

        self.assertEqual(0, imported_weeks)
        self.assertEqual(0, imported_players)
        get_session.assert_not_called()
        save_state.assert_not_awaited()
        save_participation.assert_not_awaited()

    async def test_import_riverrace_log_partial_items_safe_no_writes(self) -> None:
        client = AsyncMock()
        client.get_river_race_log = AsyncMock(
            return_value={
                "items": [
                    {"seasonId": 1},
                    {
                        "seasonId": 2,
                        "sectionIndex": 0,
                        "standings": [
                            {
                                "clan": {
                                    "tag": "#OTHER",
                                    "participants": [
                                        {
                                            "tag": "#P1",
                                            "name": "Player 1",
                                        }
                                    ],
                                }
                            }
                        ],
                    },
                ]
            }
        )
        save_state = AsyncMock()
        save_participation = AsyncMock()
        colosseum_map = AsyncMock(return_value={})

        class _FakeSession:
            def __init__(self) -> None:
                self.commit = AsyncMock()
                self.rollback = AsyncMock()

        session = _FakeSession()

        @asynccontextmanager
        async def _session_ctx():
            yield session

        get_session = MagicMock(side_effect=lambda: _session_ctx())

        with patch("riverrace_import.get_api_client", new=AsyncMock(return_value=client)), patch(
            "riverrace_import.get_session",
            new=get_session,
        ), patch(
            "riverrace_import.get_colosseum_index_map",
            new=colosseum_map,
        ), patch(
            "riverrace_import.save_river_race_state",
            new=save_state,
        ), patch(
            "riverrace_import.save_player_participation",
            new=save_participation,
        ):
            imported_weeks, imported_players = await rr.import_riverrace_log(
                weeks=10,
                clan_tag="#CLAN",
            )

        self.assertEqual(0, imported_weeks)
        self.assertEqual(0, imported_players)
        get_session.assert_called_once()
        colosseum_map.assert_awaited_once_with(session=session)
        session.commit.assert_awaited_once()
        session.rollback.assert_not_awaited()
        save_state.assert_not_awaited()
        save_participation.assert_not_awaited()

