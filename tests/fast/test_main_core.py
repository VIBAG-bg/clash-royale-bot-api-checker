import unittest
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

try:
    import main
except Exception:
    raise unittest.SkipTest("main module dependencies not available")

from cr_api import ClashRoyaleAPIError
from tests._fakes_aiogram import FakeBot
from tests._time_freeze import freeze_utc


class MainParserTests(unittest.IsolatedAsyncioTestCase):
    def test_parse_cr_timestamp(self) -> None:
        parsed = main._parse_cr_timestamp("20260210T120000.000Z")
        self.assertEqual(datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc), parsed)
        parsed_short = main._parse_cr_timestamp("20260210T120000Z")
        self.assertEqual(datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc), parsed_short)
        self.assertIsNone(main._parse_cr_timestamp("bad"))
        self.assertIsNone(main._parse_cr_timestamp(None))

    def test_parse_reminder_time(self) -> None:
        self.assertEqual((9, 5), main._parse_reminder_time("09:05"))
        self.assertIsNone(main._parse_reminder_time("24:00"))
        self.assertIsNone(main._parse_reminder_time("09"))
        self.assertIsNone(main._parse_reminder_time("bad"))

    async def test_resolve_war_day_number_from_snapshot(self) -> None:
        now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)
        with freeze_utc(now, modules=("main",)):
            day_number, source, _ = await main._resolve_war_day_number(
                period_index=0,
                season_id=1,
                section_index=0,
                period_type="warday",
                first_snapshot_date=date(2026, 2, 8),
                log_items=None,
            )
        self.assertEqual(3, day_number)
        self.assertEqual("db", source)

    async def test_resolve_war_day_number_snapshot_too_old(self) -> None:
        now = datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc)
        with freeze_utc(now, modules=("main",)):
            day_number, source, _ = await main._resolve_war_day_number(
                period_index=0,
                season_id=1,
                section_index=0,
                period_type="warday",
                first_snapshot_date=date(2026, 2, 1),
                log_items=None,
            )
        self.assertIsNone(day_number)
        self.assertEqual("none", source)

    async def test_resolve_war_day_number_from_period_index(self) -> None:
        now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)
        with freeze_utc(now, modules=("main",)):
            day_number, source, _ = await main._resolve_war_day_number(
                period_index=2,
                season_id=1,
                section_index=0,
                period_type="warday",
                first_snapshot_date=None,
                log_items=[],
            )
        self.assertEqual(3, day_number)
        self.assertEqual("periodIndex", source)

    async def test_resolve_war_day_number_from_log_anchor(self) -> None:
        now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)
        log_items = [
            {
                "standings": [
                    {
                        "clan": {
                            "tag": "#CLAN",
                            "finishTime": "20260206T100000.000Z",
                        }
                    }
                ]
            }
        ]
        with freeze_utc(now, modules=("main",)), patch("main.CLAN_TAG", "#CLAN"), patch(
            "main.TRAINING_DAYS_FALLBACK", 3
        ):
            day_number, source, _ = await main._resolve_war_day_number(
                period_index=None,
                season_id=1,
                section_index=0,
                period_type="warday",
                first_snapshot_date=None,
                log_items=log_items,
            )
        self.assertEqual(2, day_number)
        self.assertIn(source, ("finishTime", "createdDate"))


class MainReminderTests(unittest.IsolatedAsyncioTestCase):
    async def test_daily_reminder_disabled(self) -> None:
        with patch("main.REMINDER_ENABLED", False):
            status = await main.maybe_post_daily_war_reminder(
                FakeBot(), return_status=True
            )
        self.assertEqual({"status": "disabled"}, status)

    async def test_daily_reminder_api_error_403_notifies(self) -> None:
        bot = FakeBot()
        api_client = SimpleNamespace(
            get_current_river_race=AsyncMock(
                side_effect=ClashRoyaleAPIError(403, "forbidden")
            )
        )
        with patch("main.REMINDER_ENABLED", True), patch(
            "main.get_api_client", new=AsyncMock(return_value=api_client)
        ), patch("main.CLAN_TAG", "#CLAN"), patch(
            "main._notify_cr_api_forbidden",
            new=AsyncMock(),
        ) as notify_mock:
            status = await main.maybe_post_daily_war_reminder(
                bot, return_status=True
            )
        self.assertEqual("api_error", status["status"])
        notify_mock.assert_awaited_once()

    async def test_daily_reminder_already_posted(self) -> None:
        bot = FakeBot()
        api_client = SimpleNamespace(
            get_current_river_race=AsyncMock(
                return_value={"periodType": "warDay", "seasonId": 1, "sectionIndex": 0}
            ),
            get_river_race_log=AsyncMock(return_value=[]),
        )

        @asynccontextmanager
        async def session_ctx():
            yield object()

        with patch("main.REMINDER_ENABLED", True), patch(
            "main.get_api_client", new=AsyncMock(return_value=api_client)
        ), patch("main.CLAN_TAG", "#CLAN"), patch(
            "main.get_session", new=session_ctx
        ), patch(
            "main._resolve_active_week",
            new=AsyncMock(return_value=(1, 0, "currentriverrace")),
        ), patch(
            "main.get_river_race_state_for_week",
            new=AsyncMock(return_value={"is_colosseum": False}),
        ), patch(
            "main.get_first_snapshot_date_for_week",
            new=AsyncMock(return_value=date(2026, 2, 10)),
        ), patch(
            "main._resolve_war_day_number",
            new=AsyncMock(return_value=(1, "db", {})),
        ), patch(
            "main.get_app_state",
            new=AsyncMock(
                return_value={
                    "season_id": 1,
                    "section_index": 0,
                    "period_type": "warday",
                    "day_number": 1,
                }
            ),
        ):
            status = await main.maybe_post_daily_war_reminder(
                bot, return_status=True
            )
        self.assertEqual("already_posted", status["status"])

    async def test_daily_reminder_posted_success(self) -> None:
        bot = FakeBot()
        bot.send_message = AsyncMock()
        api_client = SimpleNamespace(
            get_current_river_race=AsyncMock(
                return_value={"periodType": "warDay", "seasonId": 1, "sectionIndex": 0}
            ),
            get_river_race_log=AsyncMock(return_value=[]),
        )

        @asynccontextmanager
        async def session_ctx():
            yield object()

        with patch("main.REMINDER_ENABLED", True), patch(
            "main.get_api_client", new=AsyncMock(return_value=api_client)
        ), patch("main.CLAN_TAG", "#CLAN"), patch(
            "main.get_session", new=session_ctx
        ), patch(
            "main._resolve_active_week",
            new=AsyncMock(return_value=(1, 0, "currentriverrace")),
        ), patch(
            "main.get_river_race_state_for_week",
            new=AsyncMock(return_value={"is_colosseum": False}),
        ), patch(
            "main.get_first_snapshot_date_for_week",
            new=AsyncMock(return_value=date(2026, 2, 10)),
        ), patch(
            "main._resolve_war_day_number",
            new=AsyncMock(return_value=(2, "db", {})),
        ), patch(
            "main.get_app_state",
            new=AsyncMock(return_value=None),
        ), patch(
            "main.get_enabled_clan_chats",
            new=AsyncMock(return_value=[-1001, -1002]),
        ), patch(
            "main.try_mark_reminder_posted",
            new=AsyncMock(return_value=True),
        ), patch(
            "main.set_app_state",
            new=AsyncMock(),
        ) as set_state_mock:
            status = await main.maybe_post_daily_war_reminder(
                bot, return_status=True
            )
        self.assertEqual("posted", status["status"])
        self.assertEqual(2, status["sent_count"])
        self.assertEqual(2, bot.send_message.await_count)
        set_state_mock.assert_awaited_once()

