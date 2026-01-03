"""Moderation policy middleware (dry-run evaluation)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram.enums import ChatType
from aiogram.types import Message

from config import ENABLE_CAPTCHA, MODERATION_MW_DRY_RUN, MODERATION_MW_ENABLED
from db import get_app_state, get_pending_challenge

logger = logging.getLogger(__name__)


class ModerationPolicyMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Any, dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: dict[str, Any],
    ) -> Any:
        if not MODERATION_MW_ENABLED:
            return await handler(event, data)
        if not isinstance(event, Message):
            return await handler(event, data)

        message = event
        if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
            return await handler(event, data)

        text = message.text or ""
        caption = message.caption or ""
        if text.startswith("/") or caption.startswith("/"):
            return await handler(event, data)

        if message.from_user is None:
            return await handler(event, data)

        if ENABLE_CAPTCHA:
            try:
                pending = await get_pending_challenge(
                    message.chat.id, message.from_user.id
                )
            except Exception as e:
                logger.warning(
                    "[MOD_MW] bypass: pending check failed chat=%s user=%s err=%s",
                    message.chat.id,
                    message.from_user.id,
                    type(e).__name__,
                )
                return await handler(event, data)
            if pending:
                return await handler(event, data)

        mod_debug = False
        try:
            state = await get_app_state(f"mod_debug:{message.chat.id}")
            mod_debug = bool(state and state.get("enabled") is True)
        except Exception:
            mod_debug = False

        try:
            from bot import handlers as h

            decision = await h.evaluate_moderation(
                message, now=datetime.now(timezone.utc), mod_debug=mod_debug
            )
            if MODERATION_MW_DRY_RUN:
                if mod_debug:
                    logger.warning(
                        "[MOD_MW] dry_run decision: chat=%s user=%s action=%s details=%s",
                        message.chat.id,
                        message.from_user.id,
                        decision.get("reason"),
                        decision.get("debug"),
                    )
                return await handler(event, data)

            if decision.get("violation") != "none":
                await h.apply_moderation_decision(
                    message,
                    decision,
                    now=datetime.now(timezone.utc),
                )
        except Exception as e:
            if mod_debug:
                logger.warning(
                    "[MOD_MW] enforce error: chat=%s user=%s err=%s",
                    message.chat.id,
                    message.from_user.id,
                    type(e).__name__,
                )
                logger.debug("Middleware error detail: %s", e, exc_info=True)

        return await handler(event, data)
