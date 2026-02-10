import asyncio
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

try:
    import main
except Exception:
    raise unittest.SkipTest("main module dependencies not available")

from tests._fakes_aiogram import FakeBot, FakeUser
from tests._time_freeze import freeze_utc


class MainScheduledUnmuteTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_due_item_unmutes_and_clears_penalty(self) -> None:
        now = datetime(2026, 2, 10, 14, 0, tzinfo=timezone.utc)
        bot = FakeBot()
        bot.get_chat_member = AsyncMock(
            return_value=SimpleNamespace(
                status=main.ChatMemberStatus.MEMBER,
                user=FakeUser(id=8001, username="player_1", full_name="Player One"),
            )
        )
        due = [{"id": 1, "chat_id": -1009001, "user_id": 8001}]

        with freeze_utc(now, modules=("main",)), patch(
            "main.list_due_scheduled_unmutes",
            new=AsyncMock(return_value=due),
        ), patch(
            "main.clear_user_penalty",
            new=AsyncMock(),
        ) as clear_penalty, patch(
            "main.mark_scheduled_unmute_sent",
            new=AsyncMock(),
        ) as mark_sent, patch(
            "main.get_app_state",
            new=AsyncMock(return_value=None),
        ), patch(
            "main.delete_app_state",
            new=AsyncMock(),
        ) as delete_state, patch(
            "main._restore_invite_only_admin",
            new=AsyncMock(return_value=True),
        ) as restore_admin, patch(
            "main.asyncio.sleep",
            new=AsyncMock(side_effect=asyncio.CancelledError()),
        ):
            await main.scheduled_unmute_task(bot)

        bot.restrict_chat_member.assert_awaited_once()
        clear_penalty.assert_awaited_once_with(-1009001, 8001, "mute")
        bot.send_message.assert_awaited_once()
        mark_sent.assert_awaited_once_with(1, sent_at=now)
        restore_admin.assert_not_awaited()
        delete_state.assert_not_awaited()

    async def test_no_due_items_no_actions(self) -> None:
        bot = FakeBot()

        with patch(
            "main.list_due_scheduled_unmutes",
            new=AsyncMock(return_value=[]),
        ), patch(
            "main.clear_user_penalty",
            new=AsyncMock(),
        ) as clear_penalty, patch(
            "main.mark_scheduled_unmute_sent",
            new=AsyncMock(),
        ) as mark_sent, patch(
            "main.asyncio.sleep",
            new=AsyncMock(side_effect=asyncio.CancelledError()),
        ):
            await main.scheduled_unmute_task(bot)

        bot.get_chat_member.assert_not_awaited()
        bot.restrict_chat_member.assert_not_awaited()
        bot.send_message.assert_not_awaited()
        clear_penalty.assert_not_awaited()
        mark_sent.assert_not_awaited()

