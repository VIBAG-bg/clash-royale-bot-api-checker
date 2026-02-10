import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

try:
    from aiogram.enums import ChatType, MessageEntityType
except Exception:
    raise unittest.SkipTest("aiogram not available")

try:
    from bot import handlers as h
except Exception:
    raise unittest.SkipTest("bot.handlers dependencies not available")

from tests._fakes_aiogram import FakeBot, FakeChat, FakeEntity, FakeMessage, FakeUser

_UNSET = object()


def _default_settings() -> dict[str, int | bool]:
    return {
        "raid_mode": False,
        "flood_window_seconds": 10,
        "flood_max_messages": 6,
        "flood_mute_minutes": 10,
        "new_user_link_block_hours": 72,
    }


class EvaluateModerationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        h._FLOOD_RATE_CACHE.clear()
        self.bot = FakeBot()
        self.chat = FakeChat(id=-100100, type=ChatType.SUPERGROUP)
        self.user = FakeUser(id=42, username="tester")

    def _message(
        self,
        *,
        text: str | None = "hello",
        entities: list[FakeEntity] | None = None,
        from_user: object = _UNSET,
    ) -> FakeMessage:
        user = self.user if from_user is _UNSET else from_user
        return FakeMessage(
            bot=self.bot,
            chat=self.chat,
            from_user=user,
            text=text,
            entities=entities,
            message_id=77,
        )

    async def test_no_user_bypass(self) -> None:
        message = self._message(from_user=None)
        result = await h.evaluate_moderation(message)
        self.assertFalse(result["should_check"])
        self.assertEqual("no_user", result["reason"])

    async def test_moderation_disabled(self) -> None:
        message = self._message()
        with patch("bot.handlers.MODERATION_ENABLED", False):
            result = await h.evaluate_moderation(message)
        self.assertFalse(result["should_check"])
        self.assertEqual("disabled", result["reason"])

    async def test_disabled_by_command_state(self) -> None:
        message = self._message()
        with patch(
            "bot.handlers.get_app_state",
            new=AsyncMock(return_value={"enabled": False}),
        ):
            result = await h.evaluate_moderation(message)
        self.assertFalse(result["should_check"])
        self.assertEqual("disabled_by_command", result["reason"])

    async def test_no_settings_bypass(self) -> None:
        message = self._message()
        with patch("bot.handlers.get_app_state", new=AsyncMock(return_value=None)), patch(
            "bot.handlers.get_chat_settings",
            new=AsyncMock(return_value=None),
        ):
            result = await h.evaluate_moderation(message)
        self.assertFalse(result["should_check"])
        self.assertEqual("no_settings", result["reason"])

    async def test_admin_bypass(self) -> None:
        message = self._message()
        with patch("bot.handlers.get_app_state", new=AsyncMock(return_value=None)), patch(
            "bot.handlers.get_chat_settings",
            new=AsyncMock(return_value=_default_settings()),
        ), patch(
            "bot.handlers._is_admin_user",
            new=AsyncMock(return_value=True),
        ):
            result = await h.evaluate_moderation(message)
        self.assertFalse(result["should_check"])
        self.assertEqual("admin", result["reason"])

    async def test_admin_check_failed_bypass(self) -> None:
        message = self._message()
        with patch("bot.handlers.get_app_state", new=AsyncMock(return_value=None)), patch(
            "bot.handlers.get_chat_settings",
            new=AsyncMock(return_value=_default_settings()),
        ), patch(
            "bot.handlers._is_admin_user",
            new=AsyncMock(side_effect=RuntimeError("boom")),
        ):
            result = await h.evaluate_moderation(message)
        self.assertFalse(result["should_check"])
        self.assertEqual("admin_check_failed", result["reason"])

    async def test_link_violation_for_unverified_recent_user(self) -> None:
        message = self._message(
            entities=[FakeEntity(type=MessageEntityType.URL, offset=0, length=10)]
        )
        with patch("bot.handlers.get_app_state", new=AsyncMock(return_value=None)), patch(
            "bot.handlers.get_chat_settings",
            new=AsyncMock(return_value=_default_settings()),
        ), patch(
            "bot.handlers._is_admin_user",
            new=AsyncMock(return_value=False),
        ), patch(
            "bot.handlers.is_user_verified",
            new=AsyncMock(return_value=False),
        ), patch(
            "bot.handlers._is_recent_user",
            new=AsyncMock(return_value=True),
        ):
            result = await h.evaluate_moderation(message)
        self.assertTrue(result["should_check"])
        self.assertEqual("link", result["violation"])
        self.assertTrue(result["should_delete"])

    async def test_link_allowed_for_verified_non_recent_user(self) -> None:
        message = self._message(
            entities=[FakeEntity(type=MessageEntityType.URL, offset=0, length=10)]
        )
        with patch("bot.handlers.get_app_state", new=AsyncMock(return_value=None)), patch(
            "bot.handlers.get_chat_settings",
            new=AsyncMock(return_value=_default_settings()),
        ), patch(
            "bot.handlers._is_admin_user",
            new=AsyncMock(return_value=False),
        ), patch(
            "bot.handlers.is_user_verified",
            new=AsyncMock(return_value=True),
        ), patch(
            "bot.handlers._is_recent_user",
            new=AsyncMock(return_value=False),
        ):
            result = await h.evaluate_moderation(message)
        self.assertEqual("flood", result["violation"])
        self.assertFalse(result["should_delete"])

    async def test_raid_mode_blocks_links_even_for_verified(self) -> None:
        message = self._message(
            entities=[FakeEntity(type=MessageEntityType.URL, offset=0, length=10)]
        )
        settings = _default_settings()
        settings["raid_mode"] = True
        with patch("bot.handlers.get_app_state", new=AsyncMock(return_value=None)), patch(
            "bot.handlers.get_chat_settings",
            new=AsyncMock(return_value=settings),
        ), patch(
            "bot.handlers._is_admin_user",
            new=AsyncMock(return_value=False),
        ), patch(
            "bot.handlers.is_user_verified",
            new=AsyncMock(return_value=True),
        ), patch(
            "bot.handlers._is_recent_user",
            new=AsyncMock(return_value=False),
        ), patch("bot.handlers.RAID_LINK_BLOCK_ALL", True):
            result = await h.evaluate_moderation(message)
        self.assertEqual("link", result["violation"])
        self.assertTrue(result["should_delete"])

    async def test_non_link_message_defaults_to_flood(self) -> None:
        message = self._message(text="just text")
        with patch("bot.handlers.get_app_state", new=AsyncMock(return_value=None)), patch(
            "bot.handlers.get_chat_settings",
            new=AsyncMock(return_value=_default_settings()),
        ), patch(
            "bot.handlers._is_admin_user",
            new=AsyncMock(return_value=False),
        ):
            result = await h.evaluate_moderation(message)
        self.assertEqual("flood", result["violation"])
        self.assertEqual("flood", result["reason"])

    async def test_mod_debug_includes_link_diagnostics(self) -> None:
        message = self._message(
            entities=[FakeEntity(type=MessageEntityType.URL, offset=0, length=10)]
        )
        with patch("bot.handlers.get_app_state", new=AsyncMock(return_value=None)), patch(
            "bot.handlers.get_chat_settings",
            new=AsyncMock(return_value=_default_settings()),
        ), patch(
            "bot.handlers._is_admin_user",
            new=AsyncMock(return_value=False),
        ), patch(
            "bot.handlers.is_user_verified",
            new=AsyncMock(return_value=False),
        ), patch(
            "bot.handlers._is_recent_user",
            new=AsyncMock(return_value=True),
        ):
            result = await h.evaluate_moderation(message, mod_debug=True)
        debug = result["debug"]
        self.assertIn("verified", debug)
        self.assertIn("recent", debug)
        self.assertIn("block_links", debug)


class ApplyModerationDecisionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        h._FLOOD_RATE_CACHE.clear()
        self.bot = FakeBot()
        self.chat = FakeChat(id=-100101, type=ChatType.SUPERGROUP)
        self.user = FakeUser(id=99, username="warned")
        self.message = FakeMessage(
            bot=self.bot,
            chat=self.chat,
            from_user=self.user,
            text="message",
            message_id=10,
        )
        self.now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)

    async def test_skip_when_should_check_false(self) -> None:
        with patch("bot.handlers._get_lang_for_message", new=AsyncMock(return_value="en")):
            await h.apply_moderation_decision(
                self.message,
                {"should_check": False},
                now=self.now,
            )
        self.message.answer.assert_not_awaited()

    async def test_link_violation_warn_without_mute(self) -> None:
        decision = {"should_check": True, "violation": "link", "should_delete": True}
        with patch("bot.handlers._get_lang_for_message", new=AsyncMock(return_value="en")), patch(
            "bot.handlers._format_user",
            return_value="@warned",
        ), patch(
            "bot.handlers._delete_message_safe",
            new=AsyncMock(),
        ) as delete_mock, patch(
            "bot.handlers.increment_user_warning",
            new=AsyncMock(return_value=2),
        ), patch(
            "bot.handlers.log_mod_action",
            new=AsyncMock(),
        ), patch(
            "bot.handlers.send_modlog",
            new=AsyncMock(),
        ), patch(
            "bot.handlers._mute_user",
            new=AsyncMock(),
        ) as mute_mock:
            await h.apply_moderation_decision(self.message, decision, now=self.now)
        delete_mock.assert_awaited_once()
        mute_mock.assert_not_awaited()
        self.assertEqual(1, self.message.answer.await_count)

    async def test_link_violation_warn_and_mute_on_threshold(self) -> None:
        decision = {
            "should_check": True,
            "violation": "link",
            "should_delete": True,
            "flood_mute": 15,
        }
        with patch("bot.handlers._get_lang_for_message", new=AsyncMock(return_value="en")), patch(
            "bot.handlers._format_user",
            return_value="@warned",
        ), patch(
            "bot.handlers._delete_message_safe",
            new=AsyncMock(),
        ), patch(
            "bot.handlers.increment_user_warning",
            new=AsyncMock(return_value=3),
        ), patch(
            "bot.handlers.log_mod_action",
            new=AsyncMock(),
        ), patch(
            "bot.handlers.send_modlog",
            new=AsyncMock(),
        ), patch(
            "bot.handlers._mute_user",
            new=AsyncMock(),
        ) as mute_mock:
            await h.apply_moderation_decision(self.message, decision, now=self.now)
        mute_mock.assert_awaited_once()
        self.assertEqual(2, self.message.answer.await_count)

    async def test_flood_new_window_below_threshold(self) -> None:
        decision = {
            "should_check": True,
            "violation": "flood",
            "flood_window": 10,
            "flood_max": 6,
            "flood_mute": 10,
        }
        with patch("bot.handlers._get_lang_for_message", new=AsyncMock(return_value="en")), patch(
            "bot.handlers._format_user",
            return_value="@warned",
        ), patch(
            "bot.handlers.record_rate_counter",
            new=AsyncMock(return_value=2),
        ) as rate_mock, patch(
            "bot.handlers.increment_user_warning",
            new=AsyncMock(return_value=1),
        ) as warn_mock:
            await h.apply_moderation_decision(self.message, decision, now=self.now)
        rate_mock.assert_awaited_once()
        warn_mock.assert_not_awaited()

    async def test_flood_existing_cache_under_threshold(self) -> None:
        decision = {
            "should_check": True,
            "violation": "flood",
            "flood_window": 10,
            "flood_max": 6,
            "flood_mute": 10,
        }
        key = (self.chat.id, self.user.id)
        h._FLOOD_RATE_CACHE[key] = {
            "window_start": self.now,
            "last_db_count": 2,
            "pending": 1,
        }
        with patch("bot.handlers._get_lang_for_message", new=AsyncMock(return_value="en")), patch(
            "bot.handlers._format_user",
            return_value="@warned",
        ), patch(
            "bot.handlers.record_rate_counter",
            new=AsyncMock(return_value=3),
        ) as rate_mock, patch(
            "bot.handlers.increment_user_warning",
            new=AsyncMock(return_value=1),
        ) as warn_mock:
            await h.apply_moderation_decision(self.message, decision, now=self.now)
        rate_mock.assert_not_awaited()
        warn_mock.assert_not_awaited()
        self.assertEqual(2, h._FLOOD_RATE_CACHE[key]["pending"])

    async def test_flood_existing_cache_over_threshold_warns(self) -> None:
        decision = {
            "should_check": True,
            "violation": "flood",
            "flood_window": 10,
            "flood_max": 6,
            "flood_mute": 10,
        }
        key = (self.chat.id, self.user.id)
        h._FLOOD_RATE_CACHE[key] = {
            "window_start": self.now,
            "last_db_count": 5,
            "pending": 1,
        }
        with patch("bot.handlers._get_lang_for_message", new=AsyncMock(return_value="en")), patch(
            "bot.handlers._format_user",
            return_value="@warned",
        ), patch(
            "bot.handlers.record_rate_counter",
            new=AsyncMock(return_value=7),
        ) as rate_mock, patch(
            "bot.handlers.increment_user_warning",
            new=AsyncMock(return_value=2),
        ) as warn_mock, patch(
            "bot.handlers.log_mod_action",
            new=AsyncMock(),
        ), patch(
            "bot.handlers.send_modlog",
            new=AsyncMock(),
        ):
            await h.apply_moderation_decision(self.message, decision, now=self.now)
        rate_mock.assert_awaited_once()
        warn_mock.assert_awaited_once()
        self.assertEqual(1, self.message.answer.await_count)

    async def test_flood_warn_three_triggers_mute(self) -> None:
        decision = {
            "should_check": True,
            "violation": "flood",
            "flood_window": 10,
            "flood_max": 6,
            "flood_mute": 30,
        }
        key = (self.chat.id, self.user.id)
        h._FLOOD_RATE_CACHE[key] = {
            "window_start": self.now,
            "last_db_count": 6,
            "pending": 1,
        }
        with patch("bot.handlers._get_lang_for_message", new=AsyncMock(return_value="en")), patch(
            "bot.handlers._format_user",
            return_value="@warned",
        ), patch(
            "bot.handlers.record_rate_counter",
            new=AsyncMock(return_value=9),
        ), patch(
            "bot.handlers.increment_user_warning",
            new=AsyncMock(return_value=3),
        ), patch(
            "bot.handlers.log_mod_action",
            new=AsyncMock(),
        ), patch(
            "bot.handlers.send_modlog",
            new=AsyncMock(),
        ), patch(
            "bot.handlers._mute_user",
            new=AsyncMock(),
        ) as mute_mock:
            await h.apply_moderation_decision(self.message, decision, now=self.now)
        mute_mock.assert_awaited_once()
        self.assertEqual(2, self.message.answer.await_count)

    async def test_bot_user_is_ignored(self) -> None:
        bot_user = FakeUser(id=12, username="bot", is_bot=True)
        message = FakeMessage(
            bot=self.bot,
            chat=self.chat,
            from_user=bot_user,
            text="hello",
        )
        with patch("bot.handlers._get_lang_for_message", new=AsyncMock(return_value="en")):
            await h.apply_moderation_decision(
                message,
                {"should_check": True, "violation": "link", "should_delete": True},
                now=self.now,
            )
        message.answer.assert_not_awaited()
