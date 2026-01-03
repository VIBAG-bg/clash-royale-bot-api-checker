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
            except Exception:
                pending = None
            if pending:
                return await handler(event, data)

        mod_debug = False
        try:
            state = await get_app_state(f"mod_debug:{message.chat.id}")
            mod_debug = bool(state and state.get("enabled") is True)
        except Exception:
            mod_debug = False

        if MODERATION_MW_DRY_RUN:
            try:
                from bot.handlers import evaluate_moderation

                decision = await evaluate_moderation(
                    message, now=datetime.now(timezone.utc), mod_debug=mod_debug
                )
                if mod_debug:
                    logger.warning(
                        "[MOD_MW] dry_run decision: chat=%s user=%s action=%s details=%s",
                        message.chat.id,
                        message.from_user.id,
                        decision.get("reason"),
                        decision.get("debug"),
                    )
            except Exception as e:
                if mod_debug:
                    logger.warning(
                        "[MOD_MW] dry_run error: chat=%s user=%s err=%s",
                        message.chat.id,
                        message.from_user.id,
                        type(e).__name__,
                    )
                    logger.debug("Middleware error detail: %s", e, exc_info=True)
            return await handler(event, data)

        try:
            from bot import handlers as h

            if not h.MODERATION_ENABLED:
                return await handler(event, data)
            if message.from_user is None or message.from_user.is_bot:
                return await handler(event, data)
            try:
                if await h._is_admin_user(message, message.from_user.id):
                    return await handler(event, data)
            except Exception:
                pass

            settings = await h.get_chat_settings(
                message.chat.id,
                defaults={
                    "raid_mode": h.RAID_MODE_DEFAULT,
                    "flood_window_seconds": h.FLOOD_WINDOW_SECONDS,
                    "flood_max_messages": h.FLOOD_MAX_MESSAGES,
                    "flood_mute_minutes": h.FLOOD_MUTE_MINUTES,
                    "new_user_link_block_hours": h.NEW_USER_LINK_BLOCK_HOURS,
                },
            )
            if not settings:
                return await handler(event, data)
            raid_mode = bool(settings.get("raid_mode"))
            flood_window = int(
                settings.get("flood_window_seconds", h.FLOOD_WINDOW_SECONDS)
            )
            flood_max = int(
                settings.get("flood_max_messages", h.FLOOD_MAX_MESSAGES)
            )
            flood_mute = int(
                settings.get("flood_mute_minutes", h.FLOOD_MUTE_MINUTES)
            )
            link_block_hours = int(
                settings.get(
                    "new_user_link_block_hours", h.NEW_USER_LINK_BLOCK_HOURS
                )
            )

            if h._message_has_link(message):
                verified = await h.is_user_verified(
                    message.chat.id, message.from_user.id
                )
                recent = await h._is_recent_user(
                    message.chat.id,
                    message.from_user.id,
                    now=datetime.now(timezone.utc),
                    hours=link_block_hours,
                )
                block_links = (not verified) or recent
                if raid_mode and h.RAID_LINK_BLOCK_ALL:
                    block_links = True
                if block_links:
                    await h._delete_message_safe(message)
                    warn_count = await h.increment_user_warning(
                        message.chat.id, message.from_user.id, now=datetime.now(timezone.utc)
                    )
                    await message.answer(
                        f"⚠️ Warning {h._warn_step(warn_count)}/3{h._warn_suffix(warn_count)} — "
                        f"links are not allowed here. User: {h._format_user(message.from_user)}",
                        parse_mode=None,
                        disable_web_page_preview=True,
                    )
                    await h.log_mod_action(
                        chat_id=message.chat.id,
                        target_user_id=message.from_user.id,
                        admin_user_id=0,
                        action="link_block",
                        reason="link",
                        message_id=message.message_id,
                    )
                    await h.send_modlog(
                        message.bot,
                        f"[MOD] link blocked: chat={message.chat.id} "
                        f"user={message.from_user.id} warnings={warn_count}",
                    )
                    if warn_count >= 3:
                        await h._mute_user(
                            message,
                            message.from_user.id,
                            minutes=flood_mute,
                            reason="link warnings",
                        )
                        await message.answer(
                            f"🔇 Auto-mute for links — 3/3 warnings. "
                            f"Muted: {h._format_user(message.from_user)}. "
                            f"Duration: {flood_mute} min. Reason: link warnings.",
                            parse_mode=None,
                            disable_web_page_preview=True,
                        )
                    return await handler(event, data)

            if raid_mode:
                flood_max = h.RAID_FLOOD_MAX_MESSAGES

            count = await h.record_rate_counter(
                message.chat.id,
                message.from_user.id,
                window_seconds=flood_window,
                now=datetime.now(timezone.utc),
            )
            if count > flood_max:
                warn_count = await h.increment_user_warning(
                    message.chat.id, message.from_user.id, now=datetime.now(timezone.utc)
                )
                await h.log_mod_action(
                    chat_id=message.chat.id,
                    target_user_id=message.from_user.id,
                    admin_user_id=0,
                    action="flood",
                    reason="flood",
                    message_id=message.message_id,
                )
                await h.send_modlog(
                    message.bot,
                    f"[MOD] flood: chat={message.chat.id} "
                    f"user={message.from_user.id} count={count}",
                )
                await message.answer(
                    f"⚠️ Warning {h._warn_step(warn_count)}/3{h._warn_suffix(warn_count)} — "
                    f"flood/spam detected (>{flood_max} msgs/{flood_window}s). "
                    f"User: {h._format_user(message.from_user)}.",
                    parse_mode=None,
                    disable_web_page_preview=True,
                )
                if warn_count >= 3:
                    await h._mute_user(
                        message,
                        message.from_user.id,
                        minutes=flood_mute,
                        reason="flood warnings",
                    )
                    await message.answer(
                        f"🔇 Auto-mute for flood — 3/3 warnings. "
                        f"Muted: {h._format_user(message.from_user)}. "
                        f"Duration: {flood_mute} min. Reason: flood warnings.",
                        parse_mode=None,
                        disable_web_page_preview=True,
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
