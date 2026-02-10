import unittest
from unittest.mock import AsyncMock, patch

try:
    from aiogram.enums import ChatType
except Exception:
    raise unittest.SkipTest("aiogram not available")

try:
    import moderation_middleware as mm
    from bot import handlers as handlers_module
except Exception:
    raise unittest.SkipTest("middleware dependencies not available")

from tests._fakes_aiogram import FakeBot, FakeChat, FakeMessage, FakeUser


class ModerationMiddlewareTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.middleware = mm.ModerationPolicyMiddleware()
        self.bot = FakeBot()
        self.user = FakeUser(id=11, username="u")
        self.group_chat = FakeChat(id=-100100, type=ChatType.SUPERGROUP)

    def _message(
        self,
        *,
        text: str | None = "text",
        chat_type=ChatType.SUPERGROUP,
        from_user: FakeUser | None = None,
    ) -> FakeMessage:
        return FakeMessage(
            bot=self.bot,
            chat=FakeChat(id=self.group_chat.id, type=chat_type),
            from_user=self.user if from_user is None else from_user,
            text=text,
        )

    async def test_bypass_when_middleware_disabled(self) -> None:
        handler = AsyncMock(return_value="ok")
        event = self._message()
        with patch("moderation_middleware.MODERATION_MW_ENABLED", False), patch.object(
            mm, "Message", FakeMessage
        ):
            result = await self.middleware(handler, event, {})
        self.assertEqual("ok", result)
        handler.assert_awaited_once()

    async def test_bypass_for_non_message_event(self) -> None:
        handler = AsyncMock(return_value="ok")
        with patch("moderation_middleware.MODERATION_MW_ENABLED", True):
            result = await self.middleware(handler, object(), {})
        self.assertEqual("ok", result)
        handler.assert_awaited_once()

    async def test_bypass_for_non_group_chat(self) -> None:
        handler = AsyncMock(return_value="ok")
        event = self._message(chat_type=ChatType.PRIVATE)
        with patch("moderation_middleware.MODERATION_MW_ENABLED", True), patch.object(
            mm, "Message", FakeMessage
        ):
            result = await self.middleware(handler, event, {})
        self.assertEqual("ok", result)
        handler.assert_awaited_once()

    async def test_bypass_for_command_message(self) -> None:
        handler = AsyncMock(return_value="ok")
        event = self._message(text="/start")
        with patch("moderation_middleware.MODERATION_MW_ENABLED", True), patch.object(
            mm, "Message", FakeMessage
        ):
            result = await self.middleware(handler, event, {})
        self.assertEqual("ok", result)
        handler.assert_awaited_once()

    async def test_bypass_for_missing_user(self) -> None:
        handler = AsyncMock(return_value="ok")
        event = self._message(from_user=None)
        with patch("moderation_middleware.MODERATION_MW_ENABLED", True), patch.object(
            mm, "Message", FakeMessage
        ):
            result = await self.middleware(handler, event, {})
        self.assertEqual("ok", result)
        handler.assert_awaited_once()

    async def test_bypass_when_pending_captcha_cached(self) -> None:
        handler = AsyncMock(return_value="ok")
        event = self._message()
        with patch("moderation_middleware.MODERATION_MW_ENABLED", True), patch(
            "moderation_middleware.ENABLE_CAPTCHA", True
        ), patch.object(mm, "Message", FakeMessage), patch(
            "moderation_middleware.get_pending_challenge",
            new=AsyncMock(),
        ) as pending_mock:
            result = await self.middleware(
                handler, event, {"pending_captcha_challenge": {"id": 1}}
            )
        self.assertEqual("ok", result)
        pending_mock.assert_not_awaited()

    async def test_bypass_when_pending_check_raises(self) -> None:
        handler = AsyncMock(return_value="ok")
        event = self._message()
        with patch("moderation_middleware.MODERATION_MW_ENABLED", True), patch(
            "moderation_middleware.ENABLE_CAPTCHA", True
        ), patch.object(mm, "Message", FakeMessage), patch(
            "moderation_middleware.get_pending_challenge",
            new=AsyncMock(side_effect=RuntimeError("boom")),
        ):
            result = await self.middleware(handler, event, {})
        self.assertEqual("ok", result)
        handler.assert_awaited_once()

    async def test_dry_run_calls_evaluate_but_not_apply(self) -> None:
        handler = AsyncMock(return_value="ok")
        event = self._message()
        evaluate_mock = AsyncMock(
            return_value={"violation": "link", "reason": "link", "debug": {}}
        )
        apply_mock = AsyncMock()
        with patch("moderation_middleware.MODERATION_MW_ENABLED", True), patch(
            "moderation_middleware.MODERATION_MW_DRY_RUN", True
        ), patch("moderation_middleware.ENABLE_CAPTCHA", False), patch.object(
            mm, "Message", FakeMessage
        ), patch(
            "moderation_middleware.get_app_state",
            new=AsyncMock(return_value={"enabled": True}),
        ), patch.object(
            handlers_module,
            "evaluate_moderation",
            new=evaluate_mock,
        ), patch.object(
            handlers_module,
            "apply_moderation_decision",
            new=apply_mock,
        ):
            result = await self.middleware(handler, event, {})
        self.assertEqual("ok", result)
        evaluate_mock.assert_awaited_once()
        apply_mock.assert_not_awaited()

    async def test_enforce_calls_apply_for_violation(self) -> None:
        handler = AsyncMock(return_value="ok")
        event = self._message()
        evaluate_mock = AsyncMock(return_value={"violation": "link", "debug": {}})
        apply_mock = AsyncMock()
        with patch("moderation_middleware.MODERATION_MW_ENABLED", True), patch(
            "moderation_middleware.MODERATION_MW_DRY_RUN", False
        ), patch("moderation_middleware.ENABLE_CAPTCHA", False), patch.object(
            mm, "Message", FakeMessage
        ), patch(
            "moderation_middleware.get_app_state",
            new=AsyncMock(return_value=None),
        ), patch.object(
            handlers_module,
            "evaluate_moderation",
            new=evaluate_mock,
        ), patch.object(
            handlers_module,
            "apply_moderation_decision",
            new=apply_mock,
        ):
            await self.middleware(handler, event, {})
        apply_mock.assert_awaited_once()

    async def test_enforce_skips_apply_for_none_violation(self) -> None:
        handler = AsyncMock(return_value="ok")
        event = self._message()
        evaluate_mock = AsyncMock(return_value={"violation": "none", "debug": {}})
        apply_mock = AsyncMock()
        with patch("moderation_middleware.MODERATION_MW_ENABLED", True), patch(
            "moderation_middleware.MODERATION_MW_DRY_RUN", False
        ), patch("moderation_middleware.ENABLE_CAPTCHA", False), patch.object(
            mm, "Message", FakeMessage
        ), patch(
            "moderation_middleware.get_app_state",
            new=AsyncMock(return_value=None),
        ), patch.object(
            handlers_module,
            "evaluate_moderation",
            new=evaluate_mock,
        ), patch.object(
            handlers_module,
            "apply_moderation_decision",
            new=apply_mock,
        ):
            await self.middleware(handler, event, {})
        apply_mock.assert_not_awaited()

    async def test_handler_still_runs_on_evaluate_exception(self) -> None:
        handler = AsyncMock(return_value="ok")
        event = self._message()
        with patch("moderation_middleware.MODERATION_MW_ENABLED", True), patch(
            "moderation_middleware.MODERATION_MW_DRY_RUN", False
        ), patch("moderation_middleware.ENABLE_CAPTCHA", False), patch.object(
            mm, "Message", FakeMessage
        ), patch(
            "moderation_middleware.get_app_state",
            new=AsyncMock(return_value=None),
        ), patch.object(
            handlers_module,
            "evaluate_moderation",
            new=AsyncMock(side_effect=RuntimeError("boom")),
        ):
            result = await self.middleware(handler, event, {})
        self.assertEqual("ok", result)
        handler.assert_awaited_once()

