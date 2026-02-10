import asyncio
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

try:
    import main
except Exception:
    raise unittest.SkipTest("main module dependencies not available")

from tests._fakes_aiogram import FakeBot
from tests._time_freeze import freeze_utc


class MainAutoInviteTests(unittest.IsolatedAsyncioTestCase):
    async def test_dm_success_and_mark_invited_success(self) -> None:
        now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)
        bot = FakeBot()
        api_client = SimpleNamespace(
            get_clan_members=AsyncMock(return_value=[{"tag": "#ALREADY"}])
        )
        candidate = {
            "id": 101,
            "telegram_user_id": 7001,
            "player_tag": "#PENDING1",
            "notify_attempts": 0,
        }

        with freeze_utc(now, modules=("main",)), patch(
            "main.AUTO_INVITE_ENABLED",
            True,
        ), patch(
            "main.CLAN_TAG",
            "#CLAN",
        ), patch(
            "main.AUTO_INVITE_INVITE_MINUTES",
            20,
        ), patch(
            "main.get_api_client",
            new=AsyncMock(return_value=api_client),
        ), patch(
            "main.list_invited_applications",
            new=AsyncMock(return_value=[]),
        ), patch(
            "main.list_invite_candidates",
            new=AsyncMock(return_value=[candidate]),
        ), patch(
            "main.mark_application_invited",
            new=AsyncMock(),
        ) as mark_invited, patch(
            "main.mark_application_joined",
            new=AsyncMock(),
        ), patch(
            "main.reset_expired_invite",
            new=AsyncMock(),
        ), patch(
            "main.log_mod_action",
            new=AsyncMock(),
        ) as log_mod_action, patch(
            "main._send_modlog",
            new=AsyncMock(),
        ) as send_modlog:
            await main.maybe_auto_invite(bot)

        bot.send_message.assert_awaited_once()
        mark_invited.assert_awaited_once()
        log_mod_action.assert_awaited_once()
        send_modlog.assert_awaited_once()

        _, kwargs = mark_invited.await_args
        self.assertEqual(now, kwargs["now"])
        self.assertEqual(now + timedelta(minutes=20), kwargs["invite_expires_at"])

    async def test_dm_success_mark_invited_fails_logs_error_path(self) -> None:
        now = datetime(2026, 2, 10, 13, 0, tzinfo=timezone.utc)
        bot = FakeBot()
        api_client = SimpleNamespace(get_clan_members=AsyncMock(return_value=[]))
        candidate = {
            "id": 102,
            "telegram_user_id": 7002,
            "player_tag": "#PENDING2",
            "notify_attempts": 0,
        }
        logger_error = MagicMock()

        with freeze_utc(now, modules=("main",)), patch(
            "main.AUTO_INVITE_ENABLED",
            True,
        ), patch(
            "main.CLAN_TAG",
            "#CLAN",
        ), patch(
            "main.get_api_client",
            new=AsyncMock(return_value=api_client),
        ), patch(
            "main.list_invited_applications",
            new=AsyncMock(return_value=[]),
        ), patch(
            "main.list_invite_candidates",
            new=AsyncMock(return_value=[candidate]),
        ), patch(
            "main.mark_application_invited",
            new=AsyncMock(
                side_effect=[RuntimeError("db write failed"), asyncio.CancelledError()]
            ),
        ), patch(
            "main.log_mod_action",
            new=AsyncMock(),
        ) as log_mod_action, patch(
            "main._send_modlog",
            new=AsyncMock(),
        ) as send_modlog, patch.object(main.logger, "error", logger_error):
            await main.auto_invite_task(bot)

        self.assertGreaterEqual(bot.send_message.await_count, 1)
        self.assertGreaterEqual(logger_error.call_count, 1)
        log_mod_action.assert_not_awaited()
        send_modlog.assert_not_awaited()

    async def test_dm_fails_is_handled_without_crash(self) -> None:
        bot = FakeBot()
        bot.send_message = AsyncMock(side_effect=RuntimeError("dm blocked"))
        api_client = SimpleNamespace(get_clan_members=AsyncMock(return_value=[]))
        candidate = {
            "id": 103,
            "telegram_user_id": 7003,
            "player_tag": "#PENDING3",
            "notify_attempts": 0,
        }

        with patch("main.AUTO_INVITE_ENABLED", True), patch(
            "main.CLAN_TAG",
            "#CLAN",
        ), patch(
            "main.get_api_client",
            new=AsyncMock(return_value=api_client),
        ), patch(
            "main.list_invited_applications",
            new=AsyncMock(return_value=[]),
        ), patch(
            "main.list_invite_candidates",
            new=AsyncMock(return_value=[candidate]),
        ), patch(
            "main.mark_application_invited",
            new=AsyncMock(),
        ) as mark_invited, patch(
            "main.log_mod_action",
            new=AsyncMock(),
        ) as log_mod_action, patch(
            "main._send_modlog",
            new=AsyncMock(),
        ) as send_modlog:
            await main.maybe_auto_invite(bot)

        mark_invited.assert_not_awaited()
        log_mod_action.assert_not_awaited()
        send_modlog.assert_not_awaited()

