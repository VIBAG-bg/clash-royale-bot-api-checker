"""Telegram bot command handlers using aiogram v3."""

import inspect
import logging
from datetime import datetime, timedelta, timezone

from aiogram import Bot, F, Router
from aiogram.enums import ChatMemberStatus, ChatType, MessageEntityType
from aiogram.filters import BaseFilter, Command
from aiogram.types import (
    CallbackQuery,
    ChatMemberUpdated,
    ChatPermissions,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from config import (
    ADMIN_TELEGRAM_IDS,
    ADMIN_USER_IDS,
    APPLY_CHAT_FORWARD_TO,
    APPLY_COOLDOWN_HOURS,
    APPLY_ENABLED,
    APPLY_MAX_PENDING,
    APP_NOTIFY_COOLDOWN_HOURS,
    AUTO_INVITE_INVITE_MINUTES,
    BOT_USERNAME,
    CAPTCHA_EXPIRE_MINUTES,
    CAPTCHA_MAX_ATTEMPTS,
    CAPTCHA_REMIND_COOLDOWN_SECONDS,
    CLAN_DEEP_LINK,
    CLAN_TAG,
    CLAN_ROYALEAPI_URL,
    ENABLE_CAPTCHA,
    FLOOD_MAX_MESSAGES,
    FLOOD_MUTE_MINUTES,
    FLOOD_WINDOW_SECONDS,
    INACTIVE_LAST_SEEN_LIMIT,
    LAST_SEEN_RED_DAYS,
    LAST_SEEN_YELLOW_DAYS,
    MODERATION_ENABLED,
    MODERATION_MW_DRY_RUN,
    MODERATION_MW_ENABLED,
    MODLOG_CHAT_ID,
    NEW_USER_LINK_BLOCK_HOURS,
    RAID_FLOOD_MAX_MESSAGES,
    RAID_LINK_BLOCK_ALL,
    RAID_MODE_DEFAULT,
    WARN_MUTE_AFTER,
    WARN_MUTE_MINUTES,
    WARN_RESET_AFTER_MUTE,
    WELCOME_RULES_MESSAGE_LINK,
)
from i18n import DEFAULT_LANG, t
from cr_api import ClashRoyaleAPIError, get_api_client
from db import (
    count_pending_applications,
    create_fresh_captcha_challenge,
    create_application,
    delete_verified_user,
    delete_user_link,
    delete_user_link_request,
    expire_active_challenges,
    get_chat_settings,
    get_app_state,
    get_application_by_id,
    get_captcha_question,
    get_current_member_tags,
    get_current_members_snapshot,
    get_user_language,
    get_first_seen_time,
    get_last_rejected_time_for_user,
    get_latest_challenge,
    get_pending_application_for_user,
    get_or_create_pending_challenge,
    get_pending_challenge,
    get_top_absent_members,
    increment_challenge_attempts,
    record_rate_counter,
    increment_user_warning,
    reset_user_warnings,
    is_user_verified,
    log_mod_action,
    schedule_unmute_notification,
    mark_challenge_expired,
    mark_challenge_failed,
    mark_challenge_passed,
    mark_pending_challenges_passed,
    list_pending_applications,
    get_player_name_for_tag,
    get_challenge_by_id,
    get_user_link,
    get_user_link_request,
    get_user_links_by_tags,
    list_mod_actions,
    list_mod_actions_for_user,
    search_player_candidates,
    set_chat_raid_mode,
    set_application_status,
    set_user_penalty,
    clear_user_penalty,
    set_user_verified,
    set_user_language,
    touch_last_reminded_at,
    update_application_tag,
    mark_application_invited,
    update_challenge_message_id,
    upsert_clan_chat,
    upsert_user_link,
    upsert_user_link_request,
    delete_app_state,
    set_app_state,
    list_invited_applications,
)
from reports import (
    build_clan_info_report,
    build_clan_place_report,
    build_current_war_report,
    build_donations_report,
    build_kick_debug_report,
    build_kick_newbie_report,
    build_kick_shortlist_report,
    build_my_activity_report,
    build_rank_report,
    build_promotion_candidates_report,
    build_rolling_report,
    build_tg_list_report,
    build_top_players_report,
    build_weekly_report,
)
from riverrace_import import get_last_completed_week, get_last_completed_weeks

logger = logging.getLogger(__name__)
logger.info("MODLOG_CHAT_ID loaded as %r", MODLOG_CHAT_ID)

# Create router for handlers
router = Router(name="main_handlers")
moderation_router = Router(name="moderation_router")

from moderation_middleware import ModerationPolicyMiddleware

moderation_router.message.middleware(ModerationPolicyMiddleware())

HEADER_LINE = "\u2550" * 30
DIVIDER_LINE = "---------------------------"
_FLOOD_RATE_CACHE: dict[tuple[int, int], dict[str, object]] = {}


def _format_help_commands(commands: list[dict[str, object]]) -> list[str]:
    lines: list[str] = []
    for index, cmd in enumerate(commands, 1):
        usage = cmd["usage"][0] if cmd["usage"] else cmd["name"]
        lines.append(f"{index}) {cmd['name']} — {cmd['what']}")
        lines.append(f"   Usage: {usage}")
    return lines


async def _get_lang_for_message(message: Message) -> str:
    if message.from_user is None:
        return DEFAULT_LANG
    chat_id = message.chat.id if message.chat else None
    return await get_user_language(chat_id, message.from_user.id)


async def _get_lang_for_query(query: CallbackQuery) -> str:
    if query.from_user is None:
        return DEFAULT_LANG
    chat_id = query.message.chat.id if query.message else None
    return await get_user_language(chat_id, query.from_user.id)


def _build_language_keyboard(target_user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=t("lang_button_uk", DEFAULT_LANG),
                    callback_data=f"lang_select:{target_user_id}:uk",
                ),
                InlineKeyboardButton(
                    text=t("lang_button_ru", DEFAULT_LANG),
                    callback_data=f"lang_select:{target_user_id}:ru",
                ),
                InlineKeyboardButton(
                    text=t("lang_button_en", DEFAULT_LANG),
                    callback_data=f"lang_select:{target_user_id}:en",
                ),
            ]
        ]
    )


def _require_clan_tag() -> str | None:
    if CLAN_TAG:
        return CLAN_TAG
    return None


def _normalize_tag(tag: str) -> str:
    raw = tag.strip()
    if not raw:
        return raw
    if not raw.startswith("#"):
        raw = f"#{raw}"
    return raw.upper()


def _apply_state_key(user_id: int) -> str:
    return f"apply_state:{user_id}"


def _app_notify_state_key(app_id: int) -> str:
    return f"app_notify:{app_id}"


def _mod_debug_state_key(chat_id: int) -> str:
    return f"mod_debug:{chat_id}"


def _moderation_state_key(chat_id: int) -> str:
    return f"moderation:{chat_id}"


def _is_debug_admin(user_id: int) -> bool:
    return user_id in ADMIN_TELEGRAM_IDS


async def _is_mod_debug(chat_id: int) -> bool:
    state = await get_app_state(_mod_debug_state_key(chat_id))
    if not state:
        return False
    return bool(state.get("enabled") is True)


def _parse_debug_day(text: str | None) -> int:
    if not text:
        return 1
    parts = text.split()
    if len(parts) < 2:
        return 1
    try:
        day = int(parts[1])
    except ValueError:
        return 1
    if day < 1:
        return 1
    if day > 4:
        return 4
    return day


def _format_dt(value: datetime | None) -> str:
    if not isinstance(value, datetime):
        return "n/a"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _format_user_label(user: object, lang: str = DEFAULT_LANG) -> str:
    if not hasattr(user, "full_name"):
        return t("unknown", lang)
    label = user.full_name
    username = getattr(user, "username", None)
    if username:
        label = f"{label} (@{username})"
    return label


def _format_user(user: object, lang: str = DEFAULT_LANG) -> str:
    username = getattr(user, "username", None)
    user_id = getattr(user, "id", None)
    full_name = getattr(user, "full_name", t("unknown", lang))
    if username and user_id:
        return f"@{username} ({user_id})"
    if user_id:
        return f"{full_name} ({user_id})"
    return full_name


WARN_THRESHOLD = 3


def _warn_step(total: int) -> int:
    return min(total, WARN_THRESHOLD)


def _warn_suffix(total: int) -> str:
    return f" (total: {total})" if total > WARN_THRESHOLD else ""


def _build_user_mention(user: object, lang: str = DEFAULT_LANG) -> str:
    username = getattr(user, "username", None)
    if username:
        return f"@{username}"
    user_id = getattr(user, "id", None)
    if user_id:
        return f"tg://user?id={user_id}"
    return t("user_generic", lang)


def _format_application_summary(app: dict[str, object], lang: str) -> str:
    tag = app.get("player_tag") or t("na", lang)
    user_display = (
        app.get("telegram_username")
        or app.get("telegram_display_name")
        or t("user_label", lang)
    )
    created_at = _format_dt(app.get("created_at"))
    return t(
        "app_summary_line",
        lang,
        name=app.get("player_name"),
        tag=tag,
        user=user_display,
        created_at=created_at,
    )


def _parse_optional_tag(value: str) -> tuple[bool, str | None]:
    raw = value.strip()
    if not raw:
        return False, None
    if raw.lower() in ("skip", "no"):
        return True, None
    tag = _normalize_tag(raw)
    body = tag.lstrip("#")
    if not body or not body.isalnum():
        return False, None
    return True, tag


async def _notify_application(bot: Bot, text: str) -> None:
    if APPLY_CHAT_FORWARD_TO == 0:
        return
    try:
        await bot.send_message(
            APPLY_CHAT_FORWARD_TO,
            text,
            parse_mode=None,
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.warning("Failed to notify application chat: %s", e)


def _message_has_link(message: Message) -> bool:
    entities = []
    if message.entities:
        entities.extend(message.entities)
    if message.caption_entities:
        entities.extend(message.caption_entities)
    for entity in entities:
        if entity.type in (MessageEntityType.URL, MessageEntityType.TEXT_LINK):
            return True
    text = message.text or message.caption or ""
    if not text:
        return False
    lowered = text.lower()
    if "http" in lowered or "https" in lowered or "t.me/" in lowered:
        return True
    for token in ("bit.ly", "tinyurl.com", "t.co", "goo.gl", "discord.gg"):
        if token in lowered:
            return True
    return False


async def evaluate_moderation(
    message: Message,
    *,
    now: datetime | None = None,
    mod_debug: bool = False,
) -> dict[str, object]:
    # MUST REMAIN SIDE-EFFECT FREE
    if now is None:
        now = datetime.now(timezone.utc)
    if message.from_user is None or message.from_user.is_bot:
        return {
            "should_check": False,
            "violation": "none",
            "should_delete": False,
            "reason": "no_user",
            "debug": {},
        }
    if not MODERATION_ENABLED:
        return {
            "should_check": False,
            "violation": "none",
            "should_delete": False,
            "reason": "disabled",
            "debug": {},
        }
    try:
        state = await get_app_state(_moderation_state_key(message.chat.id))
        if state and state.get("enabled") is False:
            return {
                "should_check": False,
                "violation": "none",
                "should_delete": False,
                "reason": "disabled_by_command",
                "debug": {},
            }
    except Exception:
        pass
    settings = await get_chat_settings(
        message.chat.id,
        defaults={
            "raid_mode": RAID_MODE_DEFAULT,
            "flood_window_seconds": FLOOD_WINDOW_SECONDS,
            "flood_max_messages": FLOOD_MAX_MESSAGES,
            "flood_mute_minutes": FLOOD_MUTE_MINUTES,
            "new_user_link_block_hours": NEW_USER_LINK_BLOCK_HOURS,
        },
    )
    if not settings:
        return {
            "should_check": False,
            "violation": "none",
            "should_delete": False,
            "reason": "no_settings",
            "debug": {},
        }
    try:
        if await _is_admin_user(message, message.from_user.id):
            return {
                "should_check": False,
                "violation": "none",
                "should_delete": False,
                "reason": "admin",
                "debug": {},
            }
    except Exception:
        return {
            "should_check": False,
            "violation": "none",
            "should_delete": False,
            "reason": "admin_check_failed",
            "debug": {},
        }
    raid_mode = bool(settings.get("raid_mode"))
    flood_window = int(settings.get("flood_window_seconds", FLOOD_WINDOW_SECONDS))
    flood_max = int(settings.get("flood_max_messages", FLOOD_MAX_MESSAGES))
    flood_mute = int(settings.get("flood_mute_minutes", FLOOD_MUTE_MINUTES))
    link_block_hours = int(
        settings.get("new_user_link_block_hours", NEW_USER_LINK_BLOCK_HOURS)
    )
    if raid_mode:
        flood_max = RAID_FLOOD_MAX_MESSAGES
    violation = "none"
    should_delete = False
    reason = "none"
    debug: dict[str, object] = {
        "raid_mode": raid_mode,
        "flood_window": flood_window,
        "flood_max": flood_max,
        "flood_mute": flood_mute,
        "link_block_hours": link_block_hours,
    }
    if _message_has_link(message):
        verified = await is_user_verified(message.chat.id, message.from_user.id)
        recent = await _is_recent_user(
            message.chat.id,
            message.from_user.id,
            now=now,
            hours=link_block_hours,
        )
        block_links = (not verified) or recent
        if raid_mode and RAID_LINK_BLOCK_ALL:
            block_links = True
        if mod_debug:
            debug.update(
                {
                    "verified": verified,
                    "recent": recent,
                    "block_links": block_links,
                }
            )
        if block_links:
            violation = "link"
            should_delete = True
            reason = "link"
    if violation == "none":
        violation = "flood"
        reason = "flood"
    return {
        "should_check": True,
        "violation": violation,
        "should_delete": should_delete,
        "reason": reason,
        "flood_window": flood_window,
        "flood_max": flood_max,
        "flood_mute": flood_mute,
        "debug": debug,
    }


async def apply_moderation_decision(
    message: Message,
    decision: dict[str, object],
    *,
    now: datetime | None = None,
) -> None:
    # ALL ENFORCEMENT MUST LIVE HERE
    if now is None:
        now = datetime.now(timezone.utc)
    if message.from_user is None or message.from_user.is_bot:
        return
    lang = await _get_lang_for_message(message)
    user_label = _format_user(message.from_user, lang)
    if not decision.get("should_check"):
        return
    violation = decision.get("violation")
    flood_window = int(decision.get("flood_window") or FLOOD_WINDOW_SECONDS)
    flood_max = int(decision.get("flood_max") or FLOOD_MAX_MESSAGES)
    flood_mute = int(decision.get("flood_mute") or FLOOD_MUTE_MINUTES)

    if violation == "link":
        if decision.get("should_delete"):
            await _delete_message_safe(message)
        warn_count = await increment_user_warning(
            message.chat.id, message.from_user.id, now=now
        )
        await message.answer(
            t(
                "mod_warn_link",
                lang,
                step=_warn_step(warn_count),
                suffix=_warn_suffix(warn_count),
                user=user_label,
            ),
            parse_mode=None,
            disable_web_page_preview=True,
        )
        await log_mod_action(
            chat_id=message.chat.id,
            target_user_id=message.from_user.id,
            admin_user_id=0,
            action="link_block",
            reason="link",
            message_id=message.message_id,
        )
        await send_modlog(
            message.bot,
            t(
                "modlog_link_blocked",
                DEFAULT_LANG,
                chat_id=message.chat.id,
                user_id=message.from_user.id,
                warnings=warn_count,
            ),
        )
        if warn_count >= 3:
            await _mute_user(
                message,
                message.from_user.id,
                minutes=flood_mute,
                reason="link warnings",
            )
            await message.answer(
                t(
                    "mod_auto_mute_link",
                    lang,
                    user=user_label,
                    minutes=flood_mute,
                ),
                parse_mode=None,
                disable_web_page_preview=True,
            )
        return

    if violation == "flood":
        cache_key = (message.chat.id, message.from_user.id)
        entry = _FLOOD_RATE_CACHE.get(cache_key)
        if entry:
            window_start = entry.get("window_start")
            if isinstance(window_start, datetime):
                if window_start.tzinfo is None:
                    window_start = window_start.replace(tzinfo=timezone.utc)
                if (now - window_start).total_seconds() > flood_window:
                    entry = None
        if entry is None:
            count = await record_rate_counter(
                message.chat.id,
                message.from_user.id,
                window_seconds=flood_window,
                now=now,
                increment=1,
            )
            _FLOOD_RATE_CACHE[cache_key] = {
                "window_start": now,
                "last_db_count": count,
                "pending": 0,
            }
        else:
            pending = int(entry.get("pending") or 0) + 1
            last_db_count = int(entry.get("last_db_count") or 0)
            total = last_db_count + pending
            if total <= flood_max:
                entry["pending"] = pending
                return
            count = await record_rate_counter(
                message.chat.id,
                message.from_user.id,
                window_seconds=flood_window,
                now=now,
                increment=pending,
            )
            entry["last_db_count"] = count
            entry["pending"] = 0
        if count <= flood_max:
            return
        warn_count = await increment_user_warning(
            message.chat.id, message.from_user.id, now=now
        )
        await log_mod_action(
            chat_id=message.chat.id,
            target_user_id=message.from_user.id,
            admin_user_id=0,
            action="flood",
            reason="flood",
            message_id=message.message_id,
        )
        await send_modlog(
            message.bot,
            t(
                "modlog_flood",
                DEFAULT_LANG,
                chat_id=message.chat.id,
                user_id=message.from_user.id,
                count=count,
            ),
        )
        await message.answer(
            t(
                "mod_warn_flood",
                lang,
                step=_warn_step(warn_count),
                suffix=_warn_suffix(warn_count),
                flood_max=flood_max,
                flood_window=flood_window,
                user=user_label,
            ),
            parse_mode=None,
            disable_web_page_preview=True,
        )
        if warn_count >= 3:
            await _mute_user(
                message,
                message.from_user.id,
                minutes=flood_mute,
                reason="flood warnings",
            )
            await message.answer(
                t(
                    "mod_auto_mute_flood",
                    lang,
                    user=user_label,
                    minutes=flood_mute,
                ),
                parse_mode=None,
                disable_web_page_preview=True,
            )


def is_bot_command_message(message: Message) -> bool:
    text = message.text or ""
    caption = message.caption or ""
    if text.startswith("/") or caption.startswith("/"):
        return True
    for entity in message.entities or []:
        if entity.type == MessageEntityType.BOT_COMMAND and entity.offset == 0:
            return True
    for entity in message.caption_entities or []:
        if entity.type == MessageEntityType.BOT_COMMAND and entity.offset == 0:
            return True
    return False


def _extract_command_name(message: Message) -> str:
    text = message.text or message.caption or ""
    if not text:
        return "/?"
    return text.split()[0]


# Verification checklist:
# - /help works in group.
# - /mod_debug_on then normal text logs [MOD] HIT.
# - Link from non-admin logs link block + delete attempt.
# - Pending user message logs [CAPTCHA] pending handler HIT.
# - /mod_debug_off to reduce logs.
async def is_user_pending_captcha(chat_id: int, user_id: int, data=None) -> bool:
    if not ENABLE_CAPTCHA:
        return False
    if data is not None and "pending_captcha_challenge" in data:
        return bool(data.get("pending_captcha_challenge"))
    challenge = await get_pending_challenge(chat_id, user_id)
    if data is not None:
        data["pending_captcha_challenge"] = challenge
    return bool(challenge)


class PendingCaptchaFilter(BaseFilter):
    async def __call__(self, message: Message, data=None) -> bool:
        if message.from_user is None:
            return False
        return await is_user_pending_captcha(
            message.chat.id, message.from_user.id, data=data
        )


class NotPendingCaptchaFilter(BaseFilter):
    async def __call__(self, message: Message, data=None) -> bool:
        if message.from_user is None:
            return True
        return not await is_user_pending_captcha(
            message.chat.id, message.from_user.id, data=data
        )


async def _is_recent_user(
    chat_id: int, user_id: int, *, now: datetime, hours: int
) -> bool:
    if hours <= 0:
        return False
    first_seen = await get_first_seen_time(chat_id, user_id)
    if not first_seen:
        return True
    if first_seen.tzinfo is None:
        first_seen = first_seen.replace(tzinfo=timezone.utc)
    return now - first_seen < timedelta(hours=hours)


async def _delete_message_safe(message: Message) -> None:
    try:
        await message.delete()
        logger.warning(
            "[MOD] delete ok: chat=%s user=%s msg_id=%s",
            message.chat.id,
            message.from_user.id if message.from_user else None,
            message.message_id,
        )
    except Exception as e:
        logger.warning(
            "[MOD] delete failed: chat=%s user=%s msg_id=%s err=%s",
            message.chat.id,
            message.from_user.id if message.from_user else None,
            message.message_id,
            type(e).__name__,
            exc_info=True,
        )
        await send_modlog(
            message.bot,
            t(
                "modlog_delete_failed",
                DEFAULT_LANG,
                chat_id=message.chat.id,
                user_id=(
                    message.from_user.id if message.from_user else t("na", DEFAULT_LANG)
                ),
                message_id=message.message_id,
                error=e,
            ),
        )


async def _mute_user(
    message: Message, user_id: int, *, minutes: int, reason: str
) -> None:
    until = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    try:
        await _apply_mute_restriction(message, user_id=user_id, until=until)
        logger.warning(
            "[MOD] mute ok: chat=%s user=%s until=%s reason=%s",
            message.chat.id,
            user_id,
            until.isoformat(),
            reason,
        )
    except Exception as e:
        logger.warning("Failed to mute user: %s", e, exc_info=True)
        await send_modlog(
            message.bot,
            t(
                "modlog_mute_failed",
                DEFAULT_LANG,
                chat_id=message.chat.id,
                user_id=user_id,
                error=e,
            ),
        )
        return
    await set_user_penalty(
        message.chat.id, user_id, "mute", until=until
    )
    try:
        await schedule_unmute_notification(
            chat_id=message.chat.id,
            user_id=user_id,
            unmute_at=until,
            reason=reason,
        )
    except Exception as e:
        logger.warning(
            "Failed to schedule unmute notification: %s", e, exc_info=True
        )
    await log_mod_action(
        chat_id=message.chat.id,
        target_user_id=user_id,
        admin_user_id=0,
        action="mute",
        reason=reason,
        message_id=message.message_id,
    )
    await send_modlog(
        message.bot,
        t(
            "modlog_mute",
            DEFAULT_LANG,
            chat_id=message.chat.id,
            user_id=user_id,
            until=until.isoformat(),
            reason=reason,
        ),
    )

async def send_modlog(bot: Bot, text: str) -> None:
    if MODLOG_CHAT_ID == 0:
        return
    try:
        await bot.send_message(
            MODLOG_CHAT_ID,
            text,
            parse_mode=None,
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.exception(
            "Failed to send modlog (chat_id=%s): %s", MODLOG_CHAT_ID, e
        )


def _build_captcha_keyboard(
    challenge_id: int, question: dict[str, object]
) -> InlineKeyboardMarkup:
    options = [
        question.get("option_a"),
        question.get("option_b"),
        question.get("option_c"),
        question.get("option_d"),
    ]
    buttons = [
        [
            InlineKeyboardButton(
                text=str(options[0]),
                callback_data=f"cap:{challenge_id}:0",
            ),
            InlineKeyboardButton(
                text=str(options[1]),
                callback_data=f"cap:{challenge_id}:1",
            ),
        ],
        [
            InlineKeyboardButton(
                text=str(options[2]),
                callback_data=f"cap:{challenge_id}:2",
            ),
            InlineKeyboardButton(
                text=str(options[3]),
                callback_data=f"cap:{challenge_id}:3",
            ),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def _send_captcha_message(
    bot: Bot,
    chat_id: int,
    *,
    challenge_id: int,
    question: dict[str, object],
    mention: str | None = None,
) -> int | None:
    prefix = f"{mention}\n" if mention else ""
    text = (
        f"{prefix}"
        f"{t('captcha_prompt', DEFAULT_LANG, question=question.get('question_text'))}"
    )
    keyboard = _build_captcha_keyboard(challenge_id, question)
    try:
        sent = await bot.send_message(
            chat_id,
            text,
            reply_markup=keyboard,
            parse_mode=None,
        )
        return sent.message_id
    except Exception as e:
        logger.warning("Failed to send captcha message: %s", e, exc_info=True)
        return None


async def _send_welcome_message(
    bot: Bot,
    chat_id: int,
    user_display: str,
) -> None:
    lang = DEFAULT_LANG
    buttons: list[list[InlineKeyboardButton]] = []
    if WELCOME_RULES_MESSAGE_LINK:
        buttons.append(
            [InlineKeyboardButton(text=t("btn_rules", DEFAULT_LANG), url=WELCOME_RULES_MESSAGE_LINK)]
        )
    if BOT_USERNAME:
        username = BOT_USERNAME.lstrip("@")
        buttons.append(
            [
                InlineKeyboardButton(
                    text=t("btn_link_account", DEFAULT_LANG),
                    url=f"https://t.me/{username}?start=link",
                ),
                InlineKeyboardButton(
                    text=t("btn_apply", DEFAULT_LANG),
                    url=f"https://t.me/{username}?start=apply",
                ),
            ]
        )
    keyboard = (
        InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
    )
    text = (
        f"{t('welcome_message', lang, user=user_display)} "
        f"{t('welcome_message_help', lang)}"
    )
    await bot.send_message(
        chat_id,
        text,
        reply_markup=keyboard,
        parse_mode=None,
    )


async def _get_bot_username(message: Message) -> str | None:
    if BOT_USERNAME:
        return BOT_USERNAME.lstrip("@")
    try:
        me = await message.bot.get_me()
    except Exception:
        return None
    return me.username if me else None


async def _is_admin_user(message: Message, user_id: int) -> bool:
    if user_id in ADMIN_USER_IDS:
        return True
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return False
    member = await message.bot.get_chat_member(message.chat.id, user_id)
    return member.status == ChatMemberStatus.CREATOR


def _admin_restore_state_key(chat_id: int, user_id: int) -> str:
    return f"admin_restore:{chat_id}:{user_id}"


_PROMOTE_RIGHTS_KEYS: set[str] | None = None


def _filter_promote_kwargs(bot: Bot, rights: dict[str, bool]) -> dict[str, bool]:
    global _PROMOTE_RIGHTS_KEYS
    if _PROMOTE_RIGHTS_KEYS is None:
        try:
            params = inspect.signature(bot.promote_chat_member).parameters
            if any(
                param.kind == inspect.Parameter.VAR_KEYWORD
                for param in params.values()
            ):
                _PROMOTE_RIGHTS_KEYS = set(rights.keys())
            else:
                _PROMOTE_RIGHTS_KEYS = {
                    name
                    for name in params
                    if name not in ("chat_id", "user_id")
                }
        except Exception:
            _PROMOTE_RIGHTS_KEYS = set(rights.keys())
    return {key: value for key, value in rights.items() if key in _PROMOTE_RIGHTS_KEYS}


def _build_admin_rights(invite_only: bool) -> dict[str, bool]:
    return {
        "can_manage_chat": False,
        "can_change_info": False,
        "can_post_messages": False,
        "can_edit_messages": False,
        "can_delete_messages": False,
        "can_invite_users": invite_only,
        "can_restrict_members": False,
        "can_pin_messages": False,
        "can_promote_members": False,
        "can_manage_video_chats": False,
        "can_manage_topics": False,
        "is_anonymous": False,
    }


def _normalize_admin_title(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[:16]


async def _restore_invite_only_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    rights = _filter_promote_kwargs(bot, _build_admin_rights(invite_only=True))
    try:
        await bot.promote_chat_member(chat_id, user_id, **rights)
    except Exception as e:
        logger.warning(
            "Failed to restore admin rights: chat=%s user=%s err=%s",
            chat_id,
            user_id,
            type(e).__name__,
            exc_info=True,
        )
        return False
    link = None
    try:
        link = await get_user_link(user_id)
    except Exception as e:
        logger.warning(
            "Failed to load user link for admin title: chat=%s user=%s err=%s",
            chat_id,
            user_id,
            type(e).__name__,
            exc_info=True,
        )
    title = _normalize_admin_title(link.get("player_name") if link else None)
    if title and hasattr(bot, "set_chat_administrator_custom_title"):
        try:
            await bot.set_chat_administrator_custom_title(chat_id, user_id, title)
        except Exception as e:
            logger.warning(
                "Failed to set admin title: chat=%s user=%s err=%s",
                chat_id,
                user_id,
                type(e).__name__,
                exc_info=True,
            )
    return True


async def _demote_admin_for_mute(message: Message, user_id: int) -> bool:
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return False
    if user_id in ADMIN_USER_IDS:
        return False
    member = await message.bot.get_chat_member(message.chat.id, user_id)
    if member.status != ChatMemberStatus.ADMINISTRATOR:
        return False
    rights = _filter_promote_kwargs(
        message.bot, _build_admin_rights(invite_only=False)
    )
    await message.bot.promote_chat_member(message.chat.id, user_id, **rights)
    return True


async def _apply_mute_restriction(
    message: Message, *, user_id: int, until: datetime
) -> bool:
    demoted = False
    try:
        demoted = await _demote_admin_for_mute(message, user_id)
    except Exception as e:
        logger.warning(
            "Failed to demote admin before mute: chat=%s user=%s err=%s",
            message.chat.id,
            user_id,
            type(e).__name__,
            exc_info=True,
        )
    try:
        await message.bot.restrict_chat_member(
            message.chat.id,
            user_id,
            permissions=ChatPermissions(
                can_send_messages=False,
                can_send_media_messages=False,
                can_send_other_messages=False,
                can_add_web_page_previews=False,
            ),
            until_date=until,
        )
    except Exception:
        if demoted:
            try:
                await _restore_invite_only_admin(
                    message.bot, message.chat.id, user_id
                )
            except Exception:
                pass
        raise
    if demoted:
        try:
            await set_app_state(
                _admin_restore_state_key(message.chat.id, user_id),
                {"restore_admin": True, "muted_until": until.isoformat()},
            )
        except Exception as e:
            logger.warning(
                "Failed to store admin restore state: chat=%s user=%s err=%s",
                message.chat.id,
                user_id,
                type(e).__name__,
                exc_info=True,
            )
    return True


async def _restore_admin_after_mute(bot: Bot, chat_id: int, user_id: int) -> None:
    state_key = _admin_restore_state_key(chat_id, user_id)
    try:
        state = await get_app_state(state_key)
    except Exception as e:
        logger.warning(
            "Failed to read admin restore state: chat=%s user=%s err=%s",
            chat_id,
            user_id,
            type(e).__name__,
            exc_info=True,
        )
        return
    if not state or not state.get("restore_admin"):
        return
    restored = await _restore_invite_only_admin(bot, chat_id, user_id)
    if restored:
        try:
            await delete_app_state(state_key)
        except Exception as e:
            logger.warning(
                "Failed to clear admin restore state: chat=%s user=%s err=%s",
                chat_id,
                user_id,
                type(e).__name__,
                exc_info=True,
            )


async def _send_link_button(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    username = await _get_bot_username(message)
    if not username:
        await message.answer(t("link_unavailable", lang), parse_mode=None)
        return
    url = f"https://t.me/{username}?start=link"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=t("btn_link_my_account", lang), url=url)]]
    )
    prefix = ""
    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        prefix = t("link_open_private", lang) + "\n"
    text = prefix + t("link_tap_button", lang)
    await message.answer(
        text,
        reply_markup=keyboard,
        parse_mode=None,
    )


async def _send_war_activity_chart(
    message: Message,
    *,
    clan_tag: str,
    player_tag: str,
    title: str,
    lang: str,
) -> None:
    from aiogram.types import BufferedInputFile

    from charts import render_my_activity_decks_chart
    from db import (
        get_current_member_tags,
        get_last_completed_weeks_from_db,
        get_player_weekly_activity,
        get_rolling_summary,
    )

    weeks_desc = await get_last_completed_weeks_from_db(clan_tag, limit=8)
    if not weeks_desc:
        return
    weeks = list(reversed(weeks_desc))
    weekly_rows = await get_player_weekly_activity(player_tag, weeks)
    weeks_available = len(weekly_rows)
    player_fame_total = sum(fame for _, _, _, fame in weekly_rows)
    week_map = {
        (season, section): (decks, fame)
        for season, section, decks, fame in weekly_rows
    }
    week_labels = [f"{season}/{section + 1}" for season, section in weeks]
    player_decks = []
    player_fame = []
    for season, section in weeks:
        decks, fame = week_map.get((season, section), (0, 0))
        player_decks.append(decks)
        player_fame.append(fame)
    clan_avg_decks = None
    clan_avg_fame = None
    member_tags = await get_current_member_tags(clan_tag)
    if member_tags:
        rolling = await get_rolling_summary(weeks, player_tags=member_tags)
        total_decks = sum(int(row.get("decks_used", 0)) for row in rolling)
        total_fame = sum(int(row.get("fame", 0)) for row in rolling)
        denominator = max(1, len(weeks) * len(member_tags))
        clan_avg_decks = total_decks / denominator
        clan_avg_fame = total_fame / denominator
    clan_avg_fame_line = None
    if clan_avg_fame is not None and weeks_available:
        player_avg_fame = player_fame_total / weeks_available
        if abs(player_avg_fame - clan_avg_fame) <= 500:
            clan_avg_fame_line = clan_avg_fame

    png_bytes = render_my_activity_decks_chart(
        title=title,
        week_labels=week_labels,
        player_decks=player_decks,
        player_fame=player_fame,
        clan_avg_decks=clan_avg_decks,
        clan_avg_fame=clan_avg_fame_line,
        x_label=t("chart.axis.week", lang),
        y_left_label=t("chart.axis.decks", lang),
        y_right_label=t("chart.axis.fame", lang),
        legend_you_decks=t("chart.legend.you.decks", lang),
        legend_you_fame=t("chart.legend.you.fame", lang),
        legend_clan_avg_decks=t("chart.legend.clan_avg.decks", lang),
        legend_clan_avg_fame=t("chart.legend.clan_avg.fame", lang),
    )
    await message.answer_photo(
        BufferedInputFile(png_bytes, filename="activity.png"),
        parse_mode=None,
    )


async def _handle_link_candidates(
    *,
    message: Message,
    target_user_id: int,
    nickname: str,
    source: str,
    origin_chat_id: int | None,
) -> None:
    lang = await _get_lang_for_message(message)
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return

    candidates = await search_player_candidates(clan_tag, nickname)

    if not candidates:
        await message.answer(
            t("link_no_player_found", lang),
            parse_mode=None,
        )
        return

    if len(candidates) == 1:
        candidate = candidates[0]
        await upsert_user_link(
            telegram_user_id=target_user_id,
            player_tag=_normalize_tag(candidate["player_tag"]),
            player_name=candidate["player_name"],
            source=source,
        )
        await delete_user_link_request(target_user_id)
        await message.answer(
            t(
                "link_success",
                lang,
                name=candidate["player_name"],
                tag=_normalize_tag(candidate["player_tag"]),
            ),
            parse_mode=None,
        )
        return

    buttons: list[list[InlineKeyboardButton]] = []
    lines = [t("link_multiple_found", lang)]
    for candidate in candidates:
        tag = _normalize_tag(candidate["player_tag"])
        status = (
            t("status_in_clan", lang)
            if candidate.get("in_clan")
            else t("status_not_in_clan", lang)
        )
        label = t(
            "link_candidate_label",
            lang,
            name=candidate["player_name"],
            tag=tag,
            status=status,
        )
        data = f"link_select:{target_user_id}:{tag.lstrip('#')}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=data)])
        lines.append(
            t(
                "link_candidate_line",
                lang,
                index=len(buttons),
                name=candidate["player_name"],
                tag=tag,
                status=status,
            )
        )

    await upsert_user_link_request(
        telegram_user_id=target_user_id,
        status="awaiting_choice",
        origin_chat_id=origin_chat_id,
    )
    await message.answer(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode=None,
    )


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    """Handle /start command - Welcome message and bot information."""
    args = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            args = parts[1].strip().lower()
    lang = await _get_lang_for_message(message)
    if args == "link":
        if message.chat.type != ChatType.PRIVATE:
            await _send_link_button(message)
            return
        if message.from_user is None:
            await message.answer(
                t("unable_identify_account", lang), parse_mode=None
            )
            return
        existing = await get_user_link(message.from_user.id)
        if existing:
            await message.answer(
                t(
                    "already_linked",
                    lang,
                    name=existing["player_name"],
                    tag=existing["player_tag"],
                ),
                parse_mode=None,
            )
            return
        await upsert_user_link_request(
            telegram_user_id=message.from_user.id,
            status="awaiting_name",
            origin_chat_id=None,
        )
        await message.answer(
            t("send_nickname_exact", lang),
            parse_mode=None,
        )
        return

    if args == "apply":
        if message.chat.type != ChatType.PRIVATE:
            await message.answer(t("apply_open_dm", lang), parse_mode=None)
            return
        if not APPLY_ENABLED:
            await message.answer(
                t("apply_disabled", lang), parse_mode=None
            )
            return
        if message.from_user is None:
            await message.answer(
                t("unable_identify_account", lang), parse_mode=None
            )
            return
        clan_tag = _require_clan_tag()
        if not clan_tag:
            await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
            return

        pending_app = await get_pending_application_for_user(message.from_user.id)
        if pending_app:
            await message.answer(
                t(
                    "apply_already_pending",
                    lang,
                    summary=_format_application_summary(pending_app, lang),
                ),
                parse_mode=None,
            )
            return

        last_rejected = await get_last_rejected_time_for_user(message.from_user.id)
        if last_rejected and APPLY_COOLDOWN_HOURS > 0:
            now = datetime.now(timezone.utc)
            wait_until = last_rejected + timedelta(hours=APPLY_COOLDOWN_HOURS)
            if wait_until > now:
                remaining = wait_until - now
                hours = int(remaining.total_seconds() // 3600)
                minutes = int((remaining.total_seconds() % 3600) // 60)
                await message.answer(
                    t(
                        "apply_wait_before",
                        lang,
                        hours=hours,
                        minutes=minutes,
                    ),
                    parse_mode=None,
                )
                return

        pending_count = await count_pending_applications()
        if pending_count >= APPLY_MAX_PENDING:
            await message.answer(
                t("apply_queue_full", lang),
                parse_mode=None,
            )
            return

        try:
            api_client = await get_api_client()
            clan_data = await api_client.get_clan(clan_tag)
            members = clan_data.get("members") if isinstance(clan_data, dict) else None
            max_members = (
                clan_data.get("maxMembers", 50) if isinstance(clan_data, dict) else 50
            )
            if isinstance(members, int) and members < int(max_members or 50):
                await message.answer(
                    t("apply_free_slots", lang, clan_tag=clan_tag),
                    parse_mode=None,
                )
                return
        except ClashRoyaleAPIError as e:
            logger.warning("Apply clan info fetch failed: %s", e)
        except Exception as e:
            logger.warning("Apply clan info fetch failed: %s", e)

        state_key = _apply_state_key(message.from_user.id)
        state = await get_app_state(state_key)
        if state and state.get("status") == "awaiting_tag":
            await message.answer(
                t("apply_send_tag", lang), parse_mode=None
            )
            return

        await set_app_state(
            state_key,
            {"status": "awaiting_name", "started_at": datetime.now(timezone.utc).isoformat()},
        )
        await message.answer(
            t("send_nickname_exact", lang),
            parse_mode=None,
        )
        return

    await message.answer(t("start_welcome", lang), parse_mode=None)


@router.message(Command("language"))
async def cmd_language(message: Message) -> None:
    if message.from_user is None:
        lang = DEFAULT_LANG
        await message.answer(
            t("unable_identify_account", lang), parse_mode=None
        )
        return
    await message.answer(
        t("language_prompt", DEFAULT_LANG),
        reply_markup=_build_language_keyboard(message.from_user.id),
        parse_mode=None,
    )


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    """Show help for available commands."""
    lang = await _get_lang_for_message(message)
    is_admin = False
    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        if message.from_user is not None:
            try:
                is_admin = await _is_admin_user(message, message.from_user.id)
            except Exception:
                is_admin = False

    general_lines = [
        t("help_cmd_help", lang),
        t("help_cmd_start", lang),
        t("help_cmd_ping", lang),
        t("help_cmd_war", lang),
        t("help_cmd_war8", lang),
        t("help_cmd_top", lang),
        t("help_cmd_rank", lang),
        t("help_cmd_war_all", lang),
        t("help_cmd_current_war", lang),
        t("help_cmd_my_activity", lang),
        t("help_cmd_activity", lang),
        t("help_cmd_donations", lang),
        t("help_cmd_list_for_kick", lang),
        t("help_cmd_kick_newbie", lang),
        t("help_cmd_tg", lang),
        t("help_cmd_inactive", lang),
        t("help_cmd_promote_candidates", lang),
        t("help_cmd_clan_place", lang),
        t("help_cmd_clan", lang),
        t("help_cmd_info", lang),
        t("help_cmd_language", lang),
    ]

    admin_lines = [
        t("help_admin_bind", lang),
        t("help_admin_admin_link_name", lang),
        t("help_admin_unlink", lang),
        t("help_admin_apps", lang),
        t("help_admin_app", lang),
        t("help_admin_app_approve", lang),
        t("help_admin_app_reject", lang),
        t("help_admin_app_notify", lang),
        t("help_admin_captcha_send", lang),
        t("help_admin_captcha_status", lang),
        t("help_admin_captcha_reset", lang),
        t("help_admin_captcha_verify", lang),
        t("help_admin_captcha_unverify", lang),
        t("help_admin_riverside", lang),
        t("help_admin_coliseum", lang),
        t("help_admin_kick_report", lang),
        t("help_admin_modlog_test", lang),
    ]

    moderation_lines = [
        t("help_mod_warn", lang),
        t("help_mod_warns", lang),
        t("help_mod_mute", lang),
        t("help_mod_unmute", lang),
        t("help_mod_ban", lang),
        t("help_mod_unban", lang),
        t("help_mod_purge", lang),
        t("help_mod_raid_on", lang),
        t("help_mod_raid_off", lang),
        t("help_mod_raid_status", lang),
        t("help_mod_modlog", lang),
    ]

    lines = [

        HEADER_LINE,
        t("help_title", lang),
        HEADER_LINE,
        t("help_general_header", lang),
        DIVIDER_LINE,
        *general_lines,
    ]
    if is_admin:
        lines.extend(
            [
                "",
                t("help_admin_header", lang),
                DIVIDER_LINE,
                *admin_lines,
                "",
                t("help_moderation_header", lang),
                DIVIDER_LINE,
                *moderation_lines,
            ]
        )
    lines.extend(
        [
            HEADER_LINE,
            t("help_tip", lang),
            HEADER_LINE,
        ]
    )
    await message.answer("\n".join(lines), parse_mode=None)

@router.message(Command("apps"))
async def cmd_apps(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer(t("not_allowed", lang), parse_mode=None)
        return
    limit = 10
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            try:
                limit = int(parts[1])
            except ValueError:
                limit = 10
    if limit < 1:
        limit = 1
    if limit > 50:
        limit = 50

    apps = await list_pending_applications(limit=limit)
    invited_all = await list_invited_applications()
    now = datetime.now(timezone.utc)
    invited = []
    for app in invited_all:
        invite_expires_at = app.get("invite_expires_at")
        if not isinstance(invite_expires_at, datetime):
            continue
        if invite_expires_at.tzinfo is None:
            invite_expires_at = invite_expires_at.replace(tzinfo=timezone.utc)
        if invite_expires_at < now:
            continue
        invited.append(app)
    invited = invited[:limit]

    if not apps and not invited:
        await message.answer(
            t("apps_none", lang), parse_mode=None
        )
        return

    lines = []
    if apps:
        lines.append(t("apps_pending_header", lang, count=len(apps)))
        for app in apps:
            tag = app.get("player_tag") or t("na", lang)
            user = app.get("telegram_username") or app.get("telegram_display_name") or t("user_label", lang)
            created_at = _format_dt(app.get("created_at"))
            lines.append(
                t(
                    "apps_pending_line",
                    lang,
                    app_id=app.get("id"),
                    name=app.get("player_name"),
                    tag=tag,
                    user=user,
                    created_at=created_at,
                )
            )
    if invited:
        if lines:
            lines.append("")
        lines.append(t("apps_invited_header", lang, count=len(invited)))
        for app in invited:
            tag = app.get("player_tag") or t("na", lang)
            user = app.get("telegram_username") or app.get("telegram_display_name") or t("user_label", lang)
            notified_at = _format_dt(app.get("last_notified_at"))
            expires_at = _format_dt(app.get("invite_expires_at"))
            lines.append(
                t(
                    "apps_invited_line",
                    lang,
                    app_id=app.get("id"),
                    name=app.get("player_name"),
                    tag=tag,
                    user=user,
                    notified_at=notified_at,
                    expires_at=expires_at,
                )
            )
    lines.append(t("apps_note", lang))
    await message.answer("\n".join(lines), parse_mode=None)


@router.message(Command("app"))
async def cmd_app(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer(t("not_allowed", lang), parse_mode=None)
        return
    if not message.text:
        await message.answer(t("usage_app", lang), parse_mode=None)
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer(t("usage_app", lang), parse_mode=None)
        return
    try:
        app_id = int(parts[1])
    except ValueError:
        await message.answer(t("invalid_application_id", lang), parse_mode=None)
        return
    app = await get_application_by_id(app_id)
    if not app:
        await message.answer(t("application_not_found", lang), parse_mode=None)
        return
    username = app.get("telegram_username")
    username_display = f"@{username}" if username else t("na", lang)
    user_display = app.get("telegram_display_name") or t("user_label", lang)
    lines = [
        t("app_detail_header", lang, app_id=app_id),
        t("app_detail_status", lang, status=app.get("status")),
        t("app_detail_player", lang, name=app.get("player_name")),
        t("app_detail_tag", lang, tag=app.get("player_tag") or t("na", lang)),
        t(
            "app_detail_user",
            lang,
            user=user_display,
            username=username_display,
            user_id=app.get("telegram_user_id"),
        ),
        t(
            "app_detail_created",
            lang,
            created_at=_format_dt(app.get("created_at")),
        ),
        t(
            "app_detail_updated",
            lang,
            updated_at=_format_dt(app.get("updated_at")),
        ),
    ]
    await message.answer("\n".join(lines), parse_mode=None)


@router.message(Command("app_approve"))
async def cmd_app_approve(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer(t("not_allowed", lang), parse_mode=None)
        return
    if not message.text:
        await message.answer(t("usage_app_approve", lang), parse_mode=None)
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer(t("usage_app_approve", lang), parse_mode=None)
        return
    try:
        app_id = int(parts[1])
    except ValueError:
        await message.answer(t("invalid_application_id", lang), parse_mode=None)
        return
    app = await get_application_by_id(app_id)
    if not app:
        await message.answer(t("application_not_found", lang), parse_mode=None)
        return
    if app.get("status") == "approved":
        await message.answer(t("application_already_approved", lang), parse_mode=None)
        return
    updated = await set_application_status(app_id, "approved")
    if not updated:
        await message.answer(t("unable_approve_application", lang), parse_mode=None)
        return
    await message.answer(t("application_approved", lang), parse_mode=None)
    try:
        target_user_id = app.get("telegram_user_id")
        if target_user_id:
            target_lang = await get_user_language(None, int(target_user_id))
            await message.bot.send_message(
                target_user_id,
                t("application_user_approved", target_lang),
                parse_mode=None,
            )
    except Exception as e:
        logger.warning("Failed to notify applicant: %s", e)
    await _notify_application(
        message.bot,
        t(
            "app_admin_notify_approved",
            DEFAULT_LANG,
            app_id=app_id,
            admin_id=message.from_user.id,
        ),
    )


@router.message(Command("app_reject"))
async def cmd_app_reject(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer(t("not_allowed", lang), parse_mode=None)
        return
    if not message.text:
        await message.answer(t("usage_app_reject", lang), parse_mode=None)
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 2:
        await message.answer(t("usage_app_reject", lang), parse_mode=None)
        return
    try:
        app_id = int(parts[1])
    except ValueError:
        await message.answer(t("invalid_application_id", lang), parse_mode=None)
        return
    reason = parts[2].strip() if len(parts) > 2 else ""
    app = await get_application_by_id(app_id)
    if not app:
        await message.answer(t("application_not_found", lang), parse_mode=None)
        return
    if app.get("status") == "rejected":
        await message.answer(t("application_already_rejected", lang), parse_mode=None)
        return
    updated = await set_application_status(app_id, "rejected")
    if not updated:
        await message.answer(t("unable_reject_application", lang), parse_mode=None)
        return
    await message.answer(t("application_rejected", lang), parse_mode=None)
    try:
        target_user_id = app.get("telegram_user_id")
        if target_user_id:
            target_lang = await get_user_language(None, int(target_user_id))
            text = t("application_user_rejected", target_lang)
            if reason:
                text = text + "\n" + t(
                    "application_user_rejected_reason",
                    target_lang,
                    reason=reason,
                )
            await message.bot.send_message(
                target_user_id,
                text,
                parse_mode=None,
            )
    except Exception as e:
        logger.warning("Failed to notify applicant: %s", e)
    await _notify_application(
        message.bot,
        t(
            "app_admin_notify_rejected",
            DEFAULT_LANG,
            app_id=app_id,
            admin_id=message.from_user.id,
        ),
    )


@router.message(Command("app_notify"))
async def cmd_app_notify(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer(t("not_allowed", lang), parse_mode=None)
        return
    if not message.text:
        await message.answer(t("usage_app_notify", lang), parse_mode=None)
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 2:
        await message.answer(t("usage_app_notify", lang), parse_mode=None)
        return
    try:
        app_id = int(parts[1])
    except ValueError:
        await message.answer(t("invalid_application_id", lang), parse_mode=None)
        return
    reason = parts[2].strip() if len(parts) > 2 else ""

    app = await get_application_by_id(app_id)
    if not app:
        await message.answer(t("application_not_found", lang), parse_mode=None)
        await log_mod_action(
            chat_id=message.chat.id,
            target_user_id=0,
            admin_user_id=message.from_user.id,
            action="app_notify_not_found",
            reason=f"id={app_id}",
        )
        await send_modlog(
            message.bot,
            t(
                "modlog_app_notify_not_found",
                DEFAULT_LANG,
                app_id=app_id,
                admin_id=message.from_user.id,
            ),
        )
        return
    if app.get("status") != "pending":
        await message.answer(t("application_not_pending", lang), parse_mode=None)
        await log_mod_action(
            chat_id=message.chat.id,
            target_user_id=int(app.get("telegram_user_id") or 0),
            admin_user_id=message.from_user.id,
            action="app_notify_not_pending",
            reason=f"status={app.get('status')}",
        )
        await send_modlog(
            message.bot,
            t(
                "modlog_app_notify_not_pending",
                DEFAULT_LANG,
                app_id=app_id,
                status=app.get("status"),
                admin_id=message.from_user.id,
            ),
        )
        return

    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return
    clan_tag = _normalize_tag(clan_tag)

    now = datetime.now(timezone.utc)
    last_notified = app.get("last_notified_at")
    if isinstance(last_notified, datetime):
        if last_notified.tzinfo is None:
            last_notified = last_notified.replace(tzinfo=timezone.utc)
        if now - last_notified < timedelta(hours=APP_NOTIFY_COOLDOWN_HOURS):
            remaining = timedelta(hours=APP_NOTIFY_COOLDOWN_HOURS) - (
                now - last_notified
            )
            hours = int(remaining.total_seconds() // 3600)
            minutes = int((remaining.total_seconds() % 3600) // 60)
            await message.answer(
                t(
                    "application_notify_cooldown",
                    lang,
                    hours=hours,
                    minutes=minutes,
                ),
                parse_mode=None,
            )
            await log_mod_action(
                chat_id=message.chat.id,
                target_user_id=int(app.get("telegram_user_id") or 0),
                admin_user_id=message.from_user.id,
                action="app_notify_cooldown",
                reason=f"remaining={hours}h{minutes}m",
            )
            await send_modlog(
                message.bot,
                t(
                    "modlog_app_notify_cooldown",
                    DEFAULT_LANG,
                    app_id=app_id,
                    user_id=app.get("telegram_user_id"),
                    hours=hours,
                    minutes=minutes,
                ),
            )
            return

    telegram_user_id = app.get("telegram_user_id")
    if not telegram_user_id:
        await message.answer(t("application_no_user_id", lang), parse_mode=None)
        await log_mod_action(
            chat_id=message.chat.id,
            target_user_id=0,
            admin_user_id=message.from_user.id,
            action="app_notify_failed",
            reason="missing telegram_user_id",
        )
        await send_modlog(
            message.bot,
            t(
                "modlog_app_notify_failed_no_user",
                DEFAULT_LANG,
                app_id=app_id,
            ),
        )
        return

    text = t("application_invite_message", lang, clan_tag=clan_tag)
    deep_link = CLAN_DEEP_LINK or f"clashroyale://clanInfo?id={clan_tag.lstrip('#')}"
    text = "\n".join(
        [
            text,
            t("clan_link_open_in_game", lang, link=deep_link),
            t("clan_link_fallback_tag", lang, tag=clan_tag),
        ]
    )
    keyboard = None
    web_url = CLAN_ROYALEAPI_URL or f"https://royaleapi.com/clan/{clan_tag.lstrip('#')}"
    if web_url:
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=t("clan_link_button_open", lang),
                        url=web_url,
                    )
                ]
            ]
        )
    try:
        await message.bot.send_message(
            int(telegram_user_id),
            text,
            parse_mode=None,
            reply_markup=keyboard,
        )
    except Exception as e:
        logger.warning("Failed to notify applicant %s: %s", app_id, e)
        await message.answer(
            t("application_notify_failed_dm", lang),
            parse_mode=None,
        )
        await log_mod_action(
            chat_id=message.chat.id,
            target_user_id=int(telegram_user_id),
            admin_user_id=message.from_user.id,
            action="app_notify_failed",
            reason=f"{type(e).__name__}",
        )
        await send_modlog(
            message.bot,
            t(
                "modlog_app_notify_failed",
                DEFAULT_LANG,
                app_id=app_id,
                user_id=telegram_user_id,
                error=type(e).__name__,
            ),
        )
        return

    invite_expires_at = now + timedelta(minutes=AUTO_INVITE_INVITE_MINUTES)

    try:
        await mark_application_invited(
            app_id,
            now=now,
            invite_expires_at=invite_expires_at,
        )
    except Exception as e:
        logger.warning(
            "DM sent but DB update failed for app %s: %s",
            app_id,
            e,
        )
        await message.answer(
            t("application_notify_db_failed", lang),
            parse_mode=None,
        )
        await log_mod_action(
            chat_id=message.chat.id,
            target_user_id=int(telegram_user_id),
            admin_user_id=message.from_user.id,
            action="app_notify_db_failed",
            reason=type(e).__name__,
        )
        await send_modlog(
            message.bot,
            t(
                "modlog_app_notify_db_failed",
                DEFAULT_LANG,
                app_id=app_id,
                user_id=telegram_user_id,
                error=type(e).__name__,
            ),
        )
        return
    await log_mod_action(
        chat_id=message.chat.id,
        target_user_id=int(telegram_user_id),
        admin_user_id=message.from_user.id,
        action="app_notify",
        reason=reason or None,
    )
    await send_modlog(
        message.bot,
        t(
            "modlog_app_notify_sent",
            DEFAULT_LANG,
            app_id=app_id,
            user_id=telegram_user_id,
            tag=app.get("player_tag") or t("na", DEFAULT_LANG),
            admin_id=message.from_user.id,
        ),
    )
    await message.answer(
        t("application_notify_sent", lang), parse_mode=None
    )


@router.message(Command("warn"))
async def cmd_warn(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_verify_permissions", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(t("use_command_in_group", lang), parse_mode=None)
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer(t("reply_to_user_message", lang), parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer(t("not_allowed", lang), parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return

    target = message.reply_to_message.from_user
    reason = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            reason = parts[1].strip()

    now = datetime.now(timezone.utc)
    warn_count = await increment_user_warning(
        message.chat.id, target.id, now=now
    )
    await log_mod_action(
        chat_id=message.chat.id,
        target_user_id=target.id,
        admin_user_id=message.from_user.id,
        action="warn",
        reason=reason or None,
        message_id=message.message_id,
    )
    await send_modlog(
        message.bot,
        t(
            "modlog_warn",
            DEFAULT_LANG,
            chat_id=message.chat.id,
            user_id=target.id,
            count=warn_count,
            reason=reason or t("na", DEFAULT_LANG),
        ),
    )
    if warn_count >= WARN_MUTE_AFTER:
        until = datetime.now(timezone.utc) + timedelta(minutes=WARN_MUTE_MINUTES)
        try:
            await _apply_mute_restriction(message, user_id=target.id, until=until)
        except Exception as e:
            logger.warning("Failed to auto-mute user: %s", e, exc_info=True)
            await send_modlog(
                message.bot,
                t(
                    "modlog_auto_mute_failed",
                    DEFAULT_LANG,
                    chat_id=message.chat.id,
                    user_id=target.id,
                    error=e,
                ),
            )
            await message.answer(
                t("warn_issued", lang, count=warn_count),
                parse_mode=None,
            )
            return

        await set_user_penalty(
            message.chat.id, target.id, "mute", until=until
        )
        try:
            await schedule_unmute_notification(
                chat_id=message.chat.id,
                user_id=target.id,
                unmute_at=until,
                reason="warn threshold",
            )
        except Exception as e:
            logger.warning(
                "Failed to schedule unmute notification: %s", e, exc_info=True
            )
        await log_mod_action(
            chat_id=message.chat.id,
            target_user_id=target.id,
            admin_user_id=message.from_user.id,
            action="auto_mute_warn_threshold",
            reason=f"warns={warn_count}",
            message_id=message.message_id,
        )
        await send_modlog(
            message.bot,
            t(
                "modlog_auto_mute_warn_threshold",
                DEFAULT_LANG,
                chat_id=message.chat.id,
                user_id=target.id,
                until=until.isoformat(),
                count=warn_count,
            ),
        )
        if WARN_RESET_AFTER_MUTE:
            await reset_user_warnings(message.chat.id, target.id)
        await message.answer(
            t("warn_issued_auto_mute", lang, count=warn_count),
            parse_mode=None,
        )
        return

    await message.answer(
        t("warn_issued", lang, count=warn_count), parse_mode=None
    )


@router.message(Command("warns"))
async def cmd_warns(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_verify_permissions", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(t("use_command_in_group", lang), parse_mode=None)
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer(t("reply_to_user_message", lang), parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer(t("not_allowed", lang), parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return

    limit = 5
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            try:
                limit = int(parts[1])
            except ValueError:
                limit = 5
    if limit < 1:
        limit = 1
    if limit > 10:
        limit = 10

    target = message.reply_to_message.from_user
    actions = await list_mod_actions_for_user(
        message.chat.id,
        target.id,
        ["warn", "auto_mute_warn_threshold"],
        limit=limit,
    )
    if not actions:
        await message.answer(t("warns_none", lang), parse_mode=None)
        return
    lines = [
        t("warns_header", lang, count=len(actions), user=target.full_name)
    ]
    for action in actions:
        created_at = _format_dt(action.get("created_at"))
        reason = action.get("reason") or t("na", lang)
        lines.append(
            t(
                "warns_line",
                lang,
                action=action.get("action"),
                created_at=created_at,
                admin_id=action.get("admin_user_id"),
                reason=reason,
            )
        )
    await message.answer("\n".join(lines), parse_mode=None)


@router.message(Command("mute"))
async def cmd_mute(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_verify_permissions", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(t("use_command_in_group", lang), parse_mode=None)
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer(t("reply_to_user_message", lang), parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer(t("not_allowed", lang), parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.answer(t("usage_mute", lang), parse_mode=None)
        return
    try:
        minutes = int(parts[1])
    except ValueError:
        await message.answer(t("invalid_minutes", lang), parse_mode=None)
        return
    if minutes <= 0:
        await message.answer(t("minutes_positive", lang), parse_mode=None)
        return
    reason = parts[2].strip() if len(parts) > 2 else ""
    target = message.reply_to_message.from_user

    until = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    try:
        await _apply_mute_restriction(message, user_id=target.id, until=until)
    except Exception as e:
        logger.warning("Failed to mute user: %s", e, exc_info=True)
        await message.answer(t("mute_failed", lang), parse_mode=None)
        return

    await set_user_penalty(message.chat.id, target.id, "mute", until=until)
    try:
        await schedule_unmute_notification(
            chat_id=message.chat.id,
            user_id=target.id,
            unmute_at=until,
            reason=reason or None,
        )
    except Exception as e:
        logger.warning(
            "Failed to schedule unmute notification: %s", e, exc_info=True
        )
    await log_mod_action(
        chat_id=message.chat.id,
        target_user_id=target.id,
        admin_user_id=message.from_user.id,
        action="mute",
        reason=reason or None,
        message_id=message.message_id,
    )
    await send_modlog(
        message.bot,
        t(
            "modlog_mute",
            DEFAULT_LANG,
            chat_id=message.chat.id,
            user_id=target.id,
            until=until.isoformat(),
            reason=reason or t("na", DEFAULT_LANG),
        ),
    )
    await message.answer(t("mute_done", lang), parse_mode=None)


@router.message(Command("unmute"))
async def cmd_unmute(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_verify_permissions", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(t("use_command_in_group", lang), parse_mode=None)
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer(t("reply_to_user_message", lang), parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer(t("not_allowed", lang), parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return

    target = message.reply_to_message.from_user
    try:
        await message.bot.restrict_chat_member(
            message.chat.id,
            target.id,
            permissions=ChatPermissions(
                can_send_messages=True,
                can_send_media_messages=True,
                can_send_other_messages=True,
                can_add_web_page_previews=True,
            ),
        )
    except Exception as e:
        logger.warning("Failed to unmute user: %s", e, exc_info=True)
        await message.answer(t("unmute_failed", lang), parse_mode=None)
        return

    await clear_user_penalty(message.chat.id, target.id, "mute")
    await _restore_admin_after_mute(message.bot, message.chat.id, target.id)
    await log_mod_action(
        chat_id=message.chat.id,
        target_user_id=target.id,
        admin_user_id=message.from_user.id,
        action="unmute",
        reason=None,
        message_id=message.message_id,
    )
    await send_modlog(
        message.bot,
        t(
            "modlog_unmute",
            DEFAULT_LANG,
            chat_id=message.chat.id,
            user_id=target.id,
        ),
    )
    await message.answer(t("unmute_done", lang), parse_mode=None)


@router.message(Command("ban"))
async def cmd_ban(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_verify_permissions", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(t("use_command_in_group", lang), parse_mode=None)
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer(t("reply_to_user_message", lang), parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer(t("not_allowed", lang), parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return

    reason = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            reason = parts[1].strip()
    target = message.reply_to_message.from_user

    try:
        await message.bot.ban_chat_member(message.chat.id, target.id)
    except Exception as e:
        logger.warning("Failed to ban user: %s", e, exc_info=True)
        await message.answer(t("ban_failed", lang), parse_mode=None)
        return

    await set_user_penalty(message.chat.id, target.id, "ban", until=None)
    await log_mod_action(
        chat_id=message.chat.id,
        target_user_id=target.id,
        admin_user_id=message.from_user.id,
        action="ban",
        reason=reason or None,
        message_id=message.message_id,
    )
    await send_modlog(
        message.bot,
        t(
            "modlog_ban",
            DEFAULT_LANG,
            chat_id=message.chat.id,
            user_id=target.id,
            reason=reason or t("na", DEFAULT_LANG),
        ),
    )
    await message.answer(t("ban_done", lang), parse_mode=None)


@router.message(Command("unban"))
async def cmd_unban(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_verify_permissions", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(t("use_command_in_group", lang), parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer(t("not_allowed", lang), parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer(t("usage_unban", lang), parse_mode=None)
        return
    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer(t("invalid_user_id", lang), parse_mode=None)
        return

    try:
        await message.bot.unban_chat_member(message.chat.id, user_id)
    except Exception as e:
        logger.warning("Failed to unban user: %s", e, exc_info=True)
        await message.answer(t("unban_failed", lang), parse_mode=None)
        return

    await clear_user_penalty(message.chat.id, user_id, "ban")
    await log_mod_action(
        chat_id=message.chat.id,
        target_user_id=user_id,
        admin_user_id=message.from_user.id,
        action="unban",
        reason=None,
        message_id=message.message_id,
    )
    await send_modlog(
        message.bot,
        t(
            "modlog_unban",
            DEFAULT_LANG,
            chat_id=message.chat.id,
            user_id=user_id,
        ),
    )
    await message.answer(t("unban_done", lang), parse_mode=None)


@router.message(Command("purge"))
async def cmd_purge(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_verify_permissions", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(t("use_command_in_group", lang), parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer(t("not_allowed", lang), parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer(t("usage_purge", lang), parse_mode=None)
        return
    try:
        count = int(parts[1])
    except ValueError:
        await message.answer(t("invalid_number", lang), parse_mode=None)
        return
    if count < 1:
        await message.answer(t("number_positive", lang), parse_mode=None)
        return
    if count > 100:
        count = 100

    deleted = 0
    start_id = message.message_id
    for msg_id in range(start_id, start_id - count, -1):
        try:
            await message.bot.delete_message(message.chat.id, msg_id)
            deleted += 1
        except Exception:
            continue

    await log_mod_action(
        chat_id=message.chat.id,
        target_user_id=0,
        admin_user_id=message.from_user.id,
        action="purge",
        reason=f"{deleted} messages",
        message_id=message.message_id,
    )
    await send_modlog(
        message.bot,
        t(
            "modlog_purge",
            DEFAULT_LANG,
            chat_id=message.chat.id,
            admin_id=message.from_user.id,
            deleted=deleted,
        ),
    )
    await message.answer(t("purge_done", lang, count=deleted), parse_mode=None)


@router.message(Command("raid_on"))
async def cmd_raid_on(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_verify_permissions", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(t("use_command_in_group", lang), parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer(t("not_allowed", lang), parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return

    await set_chat_raid_mode(message.chat.id, True)
    await log_mod_action(
        chat_id=message.chat.id,
        target_user_id=0,
        admin_user_id=message.from_user.id,
        action="raid_on",
        reason=None,
        message_id=message.message_id,
    )
    await send_modlog(
        message.bot,
        t(
            "modlog_raid_on",
            DEFAULT_LANG,
            chat_id=message.chat.id,
            admin_id=message.from_user.id,
        ),
    )
    await message.answer(t("raid_mode_enabled", lang), parse_mode=None)


@router.message(Command("raid_off"))
async def cmd_raid_off(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_verify_permissions", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(t("use_command_in_group", lang), parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer(t("not_allowed", lang), parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return

    await set_chat_raid_mode(message.chat.id, False)
    await log_mod_action(
        chat_id=message.chat.id,
        target_user_id=0,
        admin_user_id=message.from_user.id,
        action="raid_off",
        reason=None,
        message_id=message.message_id,
    )
    await send_modlog(
        message.bot,
        t(
            "modlog_raid_off",
            DEFAULT_LANG,
            chat_id=message.chat.id,
            admin_id=message.from_user.id,
        ),
    )
    await message.answer(t("raid_mode_disabled", lang), parse_mode=None)


@router.message(Command("raid_status"))
async def cmd_raid_status(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_verify_permissions", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(t("use_command_in_group", lang), parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer(t("not_allowed", lang), parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return

    settings = await get_chat_settings(
        message.chat.id,
        defaults={
            "raid_mode": RAID_MODE_DEFAULT,
            "flood_window_seconds": FLOOD_WINDOW_SECONDS,
            "flood_max_messages": FLOOD_MAX_MESSAGES,
            "flood_mute_minutes": FLOOD_MUTE_MINUTES,
            "new_user_link_block_hours": NEW_USER_LINK_BLOCK_HOURS,
        },
    )
    lines = [
        t("raid_status_title", lang),
        t(
            "raid_status_raid_mode",
            lang,
            value=settings.get("raid_mode"),
        ),
        t(
            "raid_status_flood_window",
            lang,
            value=settings.get("flood_window_seconds"),
        ),
        t(
            "raid_status_flood_max",
            lang,
            value=settings.get("flood_max_messages"),
        ),
        t(
            "raid_status_flood_mute",
            lang,
            value=settings.get("flood_mute_minutes"),
        ),
        t(
            "raid_status_link_block",
            lang,
            value=settings.get("new_user_link_block_hours"),
        ),
    ]
    await message.answer("\n".join(lines), parse_mode=None)


@router.message(Command("mod_debug_on"))
async def cmd_mod_debug_on(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_verify_permissions", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(t("use_command_in_group", lang), parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer(t("not_allowed", lang), parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return

    await set_app_state(
        _mod_debug_state_key(message.chat.id),
        {"enabled": True, "updated_at": datetime.now(timezone.utc).isoformat()},
    )
    await message.answer(t("mod_debug_enabled", lang), parse_mode=None)


@router.message(Command("mod_debug_off"))
async def cmd_mod_debug_off(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_verify_permissions", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(t("use_command_in_group", lang), parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer(t("not_allowed", lang), parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return

    await delete_app_state(_mod_debug_state_key(message.chat.id))
    await message.answer(t("mod_debug_disabled", lang), parse_mode=None)


@router.message(Command("moderation_on"))
async def cmd_moderation_on(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_verify_permissions", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(t("use_command_in_group", lang), parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer(t("not_allowed", lang), parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return

    await set_app_state(
        _moderation_state_key(message.chat.id),
        {"enabled": True, "updated_at": datetime.now(timezone.utc).isoformat()},
    )
    await message.answer(t("moderation_enabled", lang), parse_mode=None)


@router.message(Command("moderation_off"))
async def cmd_moderation_off(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_verify_permissions", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(t("use_command_in_group", lang), parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer(t("not_allowed", lang), parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return

    await set_app_state(
        _moderation_state_key(message.chat.id),
        {"enabled": False, "updated_at": datetime.now(timezone.utc).isoformat()},
    )
    await message.answer(t("moderation_disabled", lang), parse_mode=None)


@router.message(Command("modlog"))
async def cmd_modlog(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_verify_permissions", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(t("use_command_in_group", lang), parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer(t("not_allowed", lang), parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return

    limit = 10
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            try:
                limit = int(parts[1])
            except ValueError:
                limit = 10
    if limit < 1:
        limit = 1
    if limit > 50:
        limit = 50
    actions = await list_mod_actions(message.chat.id, limit=limit)
    if not actions:
        await message.answer(t("modlog_none", lang), parse_mode=None)
        return
    lines = [
        t("modlog_header", lang, count=len(actions))
    ]
    for entry in actions:
        created_at = _format_dt(entry.get("created_at"))
        lines.append(
            t(
                "modlog_line",
                lang,
                entry_id=entry.get("id"),
                action=entry.get("action"),
                user_id=entry.get("target_user_id"),
                admin_id=entry.get("admin_user_id"),
                created_at=created_at,
            )
        )
    await message.answer("\n".join(lines), parse_mode=None)


@moderation_router.chat_member()
async def handle_member_join(event: ChatMemberUpdated) -> None:
    user = event.new_chat_member.user
    old_status = event.old_chat_member.status
    new_status = event.new_chat_member.status
    old_is_member = getattr(event.old_chat_member, "is_member", None)
    new_is_member = getattr(event.new_chat_member, "is_member", None)
    logger.info(
        "CAPTCHA chat_member update: chat_id=%s user_id=%s old=%s new=%s old_is_member=%s new_is_member=%s",
        event.chat.id,
        user.id,
        old_status,
        new_status,
        old_is_member,
        new_is_member,
    )
    if not ENABLE_CAPTCHA:
        logger.info("CAPTCHA skip: reason=disabled chat_id=%s", event.chat.id)
        return
    if event.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        logger.info("CAPTCHA skip: reason=chat_type chat_id=%s", event.chat.id)
        return
    if user.is_bot:
        logger.info(
            "CAPTCHA skip: reason=user_is_bot chat_id=%s user_id=%s",
            event.chat.id,
            user.id,
        )
        return
    try:
        if await _is_admin_user(event, user.id):
            logger.info(
                "CAPTCHA skip: reason=user_is_admin chat_id=%s user_id=%s",
                event.chat.id,
                user.id,
            )
            return
    except Exception:
        pass
    is_join = (
        old_status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED)
        and new_status in (ChatMemberStatus.MEMBER, ChatMemberStatus.RESTRICTED)
    ) or (
        old_status == ChatMemberStatus.RESTRICTED
        and new_status == ChatMemberStatus.RESTRICTED
        and old_is_member is False
        and new_is_member is True
    )
    if not is_join:
        logger.info(
            "CAPTCHA skip: reason=not_join chat_id=%s user_id=%s",
            event.chat.id,
            user.id,
        )
        return

    try:
        if await is_user_verified(event.chat.id, user.id):
            logger.info(
                "CAPTCHA skip: reason=verified chat_id=%s user_id=%s",
                event.chat.id,
                user.id,
            )
            return
    except Exception as e:
        logger.error("Failed to check verification: %s", e, exc_info=True)
        return

    try:
        await event.bot.restrict_chat_member(
            event.chat.id,
            user.id,
            permissions=ChatPermissions(
                can_send_messages=False,
                can_send_media_messages=False,
                can_send_other_messages=False,
                can_add_web_page_previews=False,
            ),
        )
        logger.info("Restricted new member %s in chat %s", user.id, event.chat.id)
    except Exception as e:
        logger.error("Failed to restrict member %s: %s", user.id, e, exc_info=True)
        await send_modlog(
            event.bot,
            t(
                "modlog_captcha_restrict_failed",
                DEFAULT_LANG,
                chat_id=event.chat.id,
                user_id=user.id,
                error=e,
            ),
        )
        return

    try:
        challenge, question = await get_or_create_pending_challenge(
            event.chat.id, user.id, CAPTCHA_EXPIRE_MINUTES
        )
    except Exception as e:
        logger.error("Failed to create challenge: %s", e, exc_info=True)
        return

    if not challenge:
        return
    if challenge.get("status") == "failed":
        await event.bot.send_message(
            event.chat.id,
            t("captcha_too_many_attempts", DEFAULT_LANG),
            parse_mode=None,
        )
        username = f"@{user.username}" if user.username else t("na", DEFAULT_LANG)
        await send_modlog(
            event.bot,
            t(
                "modlog_captcha_join_gate_failed",
                DEFAULT_LANG,
                chat_id=event.chat.id,
                user_id=user.id,
                username=username,
                name=user.full_name,
                status_old=old_status,
                status_new=new_status,
                challenge_id=challenge.get("id"),
            ),
        )
        return
    if not question:
        question = await get_captcha_question(challenge["question_id"])
    if not question:
        logger.info(
            "CAPTCHA skip: reason=missing_question chat_id=%s user_id=%s",
            event.chat.id,
            user.id,
        )
        return
    now = datetime.now(timezone.utc)
    last_reminded_at = challenge.get("last_reminded_at")
    if challenge.get("message_id") and isinstance(last_reminded_at, datetime):
        if (now - last_reminded_at).total_seconds() < CAPTCHA_REMIND_COOLDOWN_SECONDS:
            logger.info(
                "CAPTCHA skip: reason=cooldown chat_id=%s user_id=%s",
                event.chat.id,
                user.id,
            )
            return

    message_id = await _send_captcha_message(
        event.bot,
        event.chat.id,
        challenge_id=challenge["id"],
        question=question,
        mention=_build_user_mention(user),
    )
    if message_id:
        await update_challenge_message_id(challenge["id"], message_id)
        await touch_last_reminded_at(challenge["id"], now)
        logger.info(
            "Captcha sent to user %s in chat %s", user.id, event.chat.id
        )
        username = f"@{user.username}" if user.username else t("na", DEFAULT_LANG)
        await send_modlog(
            event.bot,
            t(
                "modlog_captcha_join_gate_sent",
                DEFAULT_LANG,
                chat_id=event.chat.id,
                user_id=user.id,
                username=username,
                name=user.full_name,
                status_old=old_status,
                status_new=new_status,
                challenge_id=challenge["id"],
                message_id=message_id,
            ),
        )
    else:
        username = f"@{user.username}" if user.username else t("na", DEFAULT_LANG)
        await send_modlog(
            event.bot,
            t(
                "modlog_captcha_join_gate_send_failed",
                DEFAULT_LANG,
                chat_id=event.chat.id,
                user_id=user.id,
                username=username,
                name=user.full_name,
                status_old=old_status,
                status_new=new_status,
                challenge_id=challenge["id"],
            ),
        )


@moderation_router.message(
    F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}),
    ~(F.text.startswith("/") | F.caption.startswith("/")),
    PendingCaptchaFilter(),
)
async def handle_pending_user_message(message: Message) -> None:
    if not ENABLE_CAPTCHA:
        return
    if message.from_user is None or message.from_user.is_bot:
        return
    if await _is_mod_debug(message.chat.id):
        logger.warning(
            "[CAPTCHA] pending handler HIT chat=%s user=%s msg_id=%s",
            message.chat.id,
            message.from_user.id,
            message.message_id,
        )
    challenge = await get_pending_challenge(message.chat.id, message.from_user.id)
    if not challenge:
        if await _is_mod_debug(message.chat.id):
            logger.warning(
                "[CAPTCHA] pending handler no challenge chat=%s user=%s msg_id=%s",
                message.chat.id,
                message.from_user.id,
                message.message_id,
            )
        return
    try:
        await message.delete()
        logger.info(
            "Deleted message from pending user %s in chat %s",
            message.from_user.id,
            message.chat.id,
        )
    except Exception as e:
        logger.warning("Failed to delete message: %s", e, exc_info=True)
        await send_modlog(
            message.bot,
            t(
                "modlog_captcha_delete_failed",
                DEFAULT_LANG,
                chat_id=message.chat.id,
                user_id=message.from_user.id,
                message_id=message.message_id,
                error=e,
            ),
        )

    now = datetime.now(timezone.utc)
    last_reminded_at = challenge.get("last_reminded_at")
    if isinstance(last_reminded_at, datetime):
        if (now - last_reminded_at).total_seconds() < CAPTCHA_REMIND_COOLDOWN_SECONDS:
            return

    reminder_text = t(
        "captcha_reminder", DEFAULT_LANG, user=message.from_user.full_name
    )
    await message.bot.send_message(
        message.chat.id, reminder_text, parse_mode=None
    )
    await touch_last_reminded_at(challenge["id"], now)
    await send_modlog(
        message.bot,
        t(
            "modlog_captcha_pending_deleted",
            DEFAULT_LANG,
            chat_id=message.chat.id,
            user_id=message.from_user.id,
            message_id=message.message_id,
        ),
    )

    if not challenge.get("message_id"):
        question = await get_captcha_question(challenge["question_id"])
        if question:
            message_id = await _send_captcha_message(
                message.bot,
                message.chat.id,
                challenge_id=challenge["id"],
                question=question,
                mention=_build_user_mention(message.from_user),
            )
            if message_id:
                await update_challenge_message_id(challenge["id"], message_id)


@moderation_router.message(
    F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}),
    ~(F.text.startswith("/") | F.caption.startswith("/")),
    NotPendingCaptchaFilter(),
)
async def handle_moderation_message(message: Message) -> None:
    # DO NOT ADD POLICY LOGIC HERE. Add new rules only to evaluate_moderation().
    if MODERATION_MW_ENABLED and not MODERATION_MW_DRY_RUN:
        return
    mod_debug = await _is_mod_debug(message.chat.id)
    if mod_debug:
        logger.warning(
            "[MOD] HIT chat=%s type=%s msg_id=%s from=%s text=%r entities=%s",
            message.chat.id,
            message.chat.type,
            message.message_id,
            message.from_user.id if message.from_user else None,
            (message.text or message.caption or "")[:200],
            [entity.type for entity in (message.entities or [])],
        )
    now = datetime.now(timezone.utc)
    decision = await evaluate_moderation(
        message, now=now, mod_debug=mod_debug
    )
    await apply_moderation_decision(message, decision, now=now)


@moderation_router.callback_query(F.data.startswith("cap:"))
async def handle_captcha_callback(query: CallbackQuery) -> None:
    lang = DEFAULT_LANG
    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 3:
        await query.answer(t("captcha_invalid", lang), show_alert=False)
        return
    try:
        challenge_id = int(parts[1])
        choice = int(parts[2])
    except ValueError:
        await query.answer(t("captcha_invalid", lang), show_alert=False)
        return

    challenge = await get_challenge_by_id(challenge_id)
    if not challenge:
        await query.answer(t("captcha_not_found", lang), show_alert=False)
        return
    if query.from_user is None:
        await query.answer(t("not_allowed", lang), show_alert=False)
        return
    if challenge["user_id"] != query.from_user.id:
        await query.answer(t("captcha_not_for_you", lang), show_alert=False)
        return

    now = datetime.now(timezone.utc)
    expires_at = challenge.get("expires_at")
    if isinstance(expires_at, datetime) and expires_at < now:
        await mark_challenge_expired(challenge_id)
        await query.answer(t("captcha_expired", lang), show_alert=False)
        return

    if challenge["status"] != "pending":
        await query.answer(t("captcha_not_active", lang), show_alert=False)
        return

    question = await get_captcha_question(challenge["question_id"])
    if not question:
        await query.answer(t("captcha_missing", lang), show_alert=False)
        return

    if int(choice) == int(question["correct_option"]):
        await mark_challenge_passed(challenge_id)
        await set_user_verified(challenge["chat_id"], challenge["user_id"])
        try:
            await query.bot.restrict_chat_member(
                challenge["chat_id"],
                challenge["user_id"],
                permissions=ChatPermissions(
                    can_send_messages=True,
                    can_send_media_messages=True,
                    can_send_other_messages=True,
                    can_add_web_page_previews=True,
                ),
            )
        except Exception as e:
            logger.error("Failed to unrestrict member: %s", e, exc_info=True)
            await send_modlog(
                query.bot,
                t(
                    "modlog_captcha_unrestrict_failed",
                    DEFAULT_LANG,
                    chat_id=challenge["chat_id"],
                    user_id=challenge["user_id"],
                    error=e,
                ),
            )

        if query.message:
            try:
                await query.message.delete()
            except Exception:
                pass
        await _send_welcome_message(
            query.bot,
            challenge["chat_id"],
            query.from_user.full_name,
        )
        await query.bot.send_message(
            challenge["chat_id"],
            t("language_prompt", DEFAULT_LANG),
            reply_markup=_build_language_keyboard(challenge["user_id"]),
            parse_mode=None,
        )
        await query.answer(t("captcha_verified", lang), show_alert=False)
        logger.info(
            "Captcha passed for user %s in chat %s",
            challenge["user_id"],
            challenge["chat_id"],
        )
        await send_modlog(
            query.bot,
            t(
                "modlog_captcha_passed",
                DEFAULT_LANG,
                chat_id=challenge["chat_id"],
                user_id=challenge["user_id"],
                challenge_id=challenge_id,
            ),
        )
        return

    attempts = await increment_challenge_attempts(challenge_id)
    if attempts >= CAPTCHA_MAX_ATTEMPTS:
        now = datetime.now(timezone.utc)
        await mark_challenge_failed(
            challenge_id,
            now + timedelta(seconds=30),
        )
        if query.message:
            await query.message.answer(
                t("captcha_wrong_new", lang), parse_mode=None
            )
        new_challenge, new_question = await create_fresh_captcha_challenge(
            challenge["chat_id"],
            challenge["user_id"],
            CAPTCHA_EXPIRE_MINUTES,
        )
        if new_challenge and new_question:
            message_id = await _send_captcha_message(
                query.bot,
                challenge["chat_id"],
                challenge_id=new_challenge["id"],
                question=new_question,
                mention=_build_user_mention(query.from_user),
            )
            if message_id:
                await update_challenge_message_id(
                    new_challenge["id"], message_id
                )
                await touch_last_reminded_at(new_challenge["id"], now)
        await query.answer(t("captcha_wrong_short", lang), show_alert=False)
        logger.info(
            "Captcha failed for user %s in chat %s (new challenge created)",
            challenge["user_id"],
            challenge["chat_id"],
        )
        await send_modlog(
            query.bot,
            t(
                "modlog_captcha_failed",
                DEFAULT_LANG,
                chat_id=challenge["chat_id"],
                user_id=challenge["user_id"],
                challenge_id=challenge_id,
                expires_at=_format_dt(now + timedelta(seconds=30)),
            ),
        )
        return

    await query.answer(t("captcha_wrong_try_again", lang), show_alert=False)
    await send_modlog(
        query.bot,
        t(
            "modlog_captcha_wrong",
            DEFAULT_LANG,
            chat_id=challenge["chat_id"],
            user_id=challenge["user_id"],
            challenge_id=challenge_id,
            attempts=attempts,
        ),
    )

async def _send_debug_reminder(
    message: Message,
    *,
    war_type: str,
    day: int,
    banner_url: str,
    banner_url_day4: str,
    templates: dict[int, str],
    lang: str,
) -> None:
    caption = templates.get(day)
    if not caption:
        await message.answer(
            t("debug_reminder_unavailable", lang), parse_mode=None
        )
        return
    await message.answer(
        t("debug_reminder_sending", lang, war_type=war_type, day=day),
        parse_mode=None,
    )
    if day in (1, 4):
        try:
            await message.bot.send_photo(
                message.chat.id,
                photo=banner_url_day4 if day == 4 else banner_url,
                caption=caption,
                parse_mode=None,
            )
            logger.info(
                "Debug reminder photo sent: user=%s chat=%s type=%s day=%s",
                message.from_user.id if message.from_user else None,
                message.chat.id,
                war_type,
                day,
            )
        except Exception as e:
            logger.warning(
                "Debug reminder photo failed: user=%s chat=%s type=%s day=%s error=%s",
                message.from_user.id if message.from_user else None,
                message.chat.id,
                war_type,
                day,
                e,
                exc_info=True,
            )
            await message.answer(caption, parse_mode=None)
    else:
        await message.answer(caption, parse_mode=None)
        logger.info(
            "Debug reminder sent: user=%s chat=%s type=%s day=%s",
            message.from_user.id if message.from_user else None,
            message.chat.id,
            war_type,
            day,
        )


@router.message(Command("ping"))
async def cmd_ping(message: Message) -> None:
    """Handle /ping command - Check bot responsiveness and API status."""
    lang = await _get_lang_for_message(message)
    start_time = datetime.now(timezone.utc)

    api_status = t("ping_api_connected", lang)
    clan_name = t("unknown", lang)

    try:
        api_client = await get_api_client()
        clan_tag = _require_clan_tag()
        if not clan_tag:
            raise ValueError("CLAN_TAG is not configured")
        clan_data = await api_client.get_clan(clan_tag)
        if isinstance(clan_data, dict):
            clan_name = clan_data.get("name", t("unknown", lang))
    except ClashRoyaleAPIError as e:
        api_status = t("ping_api_error", lang, error=e.message)
        logger.warning("API check failed: %s", e)
    except Exception as e:
        api_status = t("ping_api_error", lang, error=e)
        logger.error("Unexpected error during API check: %s", e)

    response_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
    response_time_text = f"{response_time:.0f}"
    server_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    response_text = t(
        "ping_response",
        lang,
        response_time=response_time_text,
        api_status=api_status,
        clan_name=clan_name,
        server_time=server_time,
    )

    await message.answer(response_text, parse_mode=None)


@router.message(Command("bind"))
async def cmd_bind(message: Message) -> None:
    """Bind the current group chat for weekly war reports."""
    lang = await _get_lang_for_message(message)
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(
            t("bind_group_only", lang), parse_mode=None
        )
        return
    if message.from_user is None:
        await message.answer(
            t("unable_verify_permissions_user", lang), parse_mode=None
        )
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer(t("bind_admin_only", lang), parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return
    await upsert_clan_chat(clan_tag, message.chat.id, enabled=True)
    await message.answer(t("bind_success", lang), parse_mode=None)


@router.message(Command("war"))
async def cmd_war(message: Message) -> None:
    """Show the weekly war report for the last completed week."""
    lang = await _get_lang_for_message(message)
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return
    week = await get_last_completed_week(clan_tag)
    if not week:
        await message.answer(t("war_no_completed_weeks", lang), parse_mode=None)
        return
    season_id, section_index = week
    report = await build_weekly_report(
        season_id, section_index, clan_tag, lang=lang
    )
    await message.answer(report, parse_mode=None)


@router.message(Command("war8"))
async def cmd_war8(message: Message) -> None:
    """Show the rolling war report for the last 8 completed weeks."""
    lang = await _get_lang_for_message(message)
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return
    weeks = await get_last_completed_weeks(8, clan_tag)
    if not weeks:
        await message.answer(t("war_no_completed_weeks", lang), parse_mode=None)
        return
    report = await build_rolling_report(weeks, clan_tag, lang=lang)
    await message.answer(report, parse_mode=None)


@router.message(Command("top"))
async def cmd_top(message: Message) -> None:
    """Show top players by decks and fame for the last 10 completed weeks."""
    lang = await _get_lang_for_message(message)
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return
    n = 10
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            try:
                n = int(parts[1].strip())
            except Exception:
                n = 10
    n = max(1, min(50, n))
    report = await build_top_players_report(
        clan_tag, lang=lang, limit=n, window_weeks=10, min_tenure_weeks=6
    )
    await message.answer(
        report, parse_mode=None, disable_web_page_preview=True
    )


@router.message(Command("war_all"))
async def cmd_war_all(message: Message) -> None:
    """Send weekly, rolling, and kick shortlist reports together."""
    lang = await _get_lang_for_message(message)
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return
    last_week = await get_last_completed_week(clan_tag)
    if not last_week:
        await message.answer(t("war_no_completed_weeks", lang), parse_mode=None)
        return
    weeks = await get_last_completed_weeks(8, clan_tag)
    if not weeks:
        weeks = [last_week]

    weekly_report = await build_weekly_report(
        last_week[0], last_week[1], clan_tag, lang=lang
    )
    rolling_report = await build_rolling_report(weeks, clan_tag, lang=lang)
    kick_report = await build_kick_shortlist_report(
        weeks, last_week, clan_tag, lang=lang
    )

    await message.answer(weekly_report, parse_mode=None)
    await message.answer(rolling_report, parse_mode=None)
    await message.answer(kick_report, parse_mode=None)


@router.message(Command("list_for_kick"))
async def cmd_list_for_kick(message: Message) -> None:
    """Show kick shortlist based on the last 8 completed weeks."""
    lang = await _get_lang_for_message(message)
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return
    weeks = await get_last_completed_weeks(8, clan_tag)
    last_week = await get_last_completed_week(clan_tag)
    if not weeks or not last_week:
        await message.answer(t("war_no_completed_weeks", lang), parse_mode=None)
        return
    report = await build_kick_shortlist_report(
        weeks, last_week, clan_tag, lang=lang
    )
    await message.answer(report, parse_mode=None)


@router.message(Command("kick_report"))
async def cmd_kick_report(message: Message) -> None:
    """Show detailed kick shortlist diagnostics (admin-only)."""
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_verify_permissions", lang), parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer(t("not_allowed", lang), parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return
    weeks = await get_last_completed_weeks(8, clan_tag)
    last_week = await get_last_completed_week(clan_tag)
    if not weeks or not last_week:
        await message.answer(t("war_no_completed_weeks", lang), parse_mode=None)
        return
    report = await build_kick_debug_report(
        weeks, last_week, clan_tag, lang=lang
    )
    await message.answer(report, parse_mode=None)


@router.message(Command("kick_newbie"))
async def cmd_kick_newbie(message: Message) -> None:
    """Show kick shortlist for newbies (1-2 full war weeks)."""
    lang = await _get_lang_for_message(message)
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return
    report = await build_kick_newbie_report(
        clan_tag, lang=lang, limit=10
    )
    await message.answer(report, parse_mode=None)


@router.message(Command("tg"))
async def cmd_tg(message: Message) -> None:
    """Show clan members with Telegram usernames (if known)."""
    lang = await _get_lang_for_message(message)
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return
    members = await get_current_members_snapshot(clan_tag)
    if not members:
        await message.answer(t("tg_no_snapshot", lang), parse_mode=None)
        return
    tags: set[str] = set()
    for row in members:
        raw_tag = row.get("player_tag")
        if not raw_tag:
            continue
        tag = str(raw_tag).strip().upper()
        if tag and not tag.startswith("#"):
            tag = f"#{tag}"
        if tag:
            tags.add(tag)
    if not tags:
        await message.answer(t("tg_no_snapshot", lang), parse_mode=None)
        return
    links = await get_user_links_by_tags(tags)
    if not links:
        await message.answer(t("tg_no_users", lang), parse_mode=None)
        return
    entries: list[dict[str, str]] = []
    for row in members:
        raw_tag = row.get("player_tag")
        if not raw_tag:
            continue
        tag = str(raw_tag).strip().upper()
        if tag and not tag.startswith("#"):
            tag = f"#{tag}"
        user_id = links.get(tag)
        if not user_id:
            continue
        username = None
        try:
            chat = await message.bot.get_chat(user_id)
            username = getattr(chat, "username", None)
        except Exception:
            username = None
        entries.append(
            {
                "name": row.get("player_name") or t("unknown", lang),
                "username": (
                    username.lstrip("@")
                    if username
                    else t("tg_username_id", lang, id=user_id)
                ),
            }
        )
    if not entries:
        await message.answer(t("tg_no_users", lang), parse_mode=None)
        return
    entries.sort(key=lambda row: row["name"].lower())
    report = await build_tg_list_report(
        clan_tag, lang=lang, entries=entries
    )
    await message.answer(report, parse_mode=None)


@router.message(Command("inactive"))
async def cmd_inactive(message: Message) -> None:
    """Handle /inactive command - Show players with low River Race participation."""
    lang = await _get_lang_for_message(message)
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return
    try:
        current_members = await get_current_member_tags(clan_tag)
        if not current_members:
            await message.answer(
                t("inactive_no_snapshot", lang),
                parse_mode=None,
            )
            return

        absent_members = await get_top_absent_members(
            clan_tag, INACTIVE_LAST_SEEN_LIMIT
        )
        if not absent_members:
            await message.answer(
                t("inactive_no_activity", lang),
                parse_mode=None,
            )
            return

        response_lines = [t("inactive_header", lang), ""]
        for index, member in enumerate(absent_members, 1):
            name = member.get("player_name") or t("unknown", lang)
            days_absent = member.get("days_absent")
            if days_absent is None:
                days_text = t("na", lang)
                flag = ""
            else:
                days_text = t("inactive_days_ago", lang, days=days_absent)
                if days_absent >= LAST_SEEN_RED_DAYS:
                    flag = "🔴"
                elif days_absent >= LAST_SEEN_YELLOW_DAYS:
                    flag = "🟡"
                else:
                    flag = ""
            prefix = f"{flag} " if flag else ""
            response_lines.append(
                t(
                    "inactive_line",
                    lang,
                    index=index,
                    prefix=prefix,
                    name=name,
                    days_text=days_text,
                )
            )

        await message.answer("\n".join(response_lines), parse_mode=None)
    except Exception as e:
        logger.error("Error in /inactive command: %s", e, exc_info=True)
        await message.answer(
            t("inactive_error", lang),
            parse_mode=None,
        )


@router.message(Command("current_war"))
async def cmd_current_war(message: Message) -> None:
    """Show current war snapshot from the database."""
    lang = await _get_lang_for_message(message)
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return
    report = await build_current_war_report(clan_tag, lang=lang)
    await message.answer(report, parse_mode=None)


@router.message(Command("info"))
async def cmd_info(message: Message) -> None:
    """Show clan info from the official Clash Royale API."""
    lang = await _get_lang_for_message(message)
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return
    report = await build_clan_info_report(clan_tag, lang=lang)
    await message.answer(report, parse_mode=None)


@router.message(Command("clan"))
async def cmd_clan(message: Message) -> None:
    """Show clan tag and deep link for Clash Royale."""
    lang = await _get_lang_for_message(message)
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return
    clan_tag_hash = _normalize_tag(clan_tag)
    clan_tag_no_hash = clan_tag_hash.lstrip("#")
    deep_link = CLAN_DEEP_LINK or f"clashroyale://clanInfo?id={clan_tag_no_hash}"
    web_url = CLAN_ROYALEAPI_URL or f"https://royaleapi.com/clan/{clan_tag_no_hash}"
    lines = [
        t("clan_link_title", lang),
        t("clan_link_tag_line", lang, tag=clan_tag_hash),
        t("clan_link_open_in_game", lang, link=deep_link),
        t("clan_link_fallback_tag", lang, tag=clan_tag_hash),
    ]
    if web_url:
        lines.append(t("clan_link_open_web", lang, url=web_url))
    keyboard = None
    if web_url:
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=t("clan_link_button_open", lang),
                        url=web_url,
                    )
                ]
            ]
        )
    await message.answer(
        "\n".join(lines),
        parse_mode=None,
        reply_markup=keyboard,
    )


@router.message(Command("clan_place"))
async def cmd_clan_place(message: Message) -> None:
    """Show current clan place in River Race."""
    lang = await _get_lang_for_message(message)
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return
    report = await build_clan_place_report(clan_tag, lang=lang)
    await message.answer(report, parse_mode=None)


@router.message(Command("rank"))
async def cmd_rank(message: Message) -> None:
    """Show clan ranking snapshot for current location."""
    lang = await _get_lang_for_message(message)
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return
    report = await build_rank_report(clan_tag, lang=lang)
    await message.answer(
        report,
        parse_mode=None,
        disable_web_page_preview=True,
    )


@router.message(Command("debug_reminder"))
async def cmd_debug_reminder(message: Message) -> None:
    """Debug: run daily reminder logic immediately for this chat."""
    lang = await _get_lang_for_message(message)
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer(t("not_allowed", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(t("use_command_in_group", lang), parse_mode=None)
        return
    try:
        from main import maybe_post_daily_war_reminder

        summary = await maybe_post_daily_war_reminder(
            message.bot, debug_chat_id=message.chat.id
        )
        await message.answer(
            t("debug_reminder_triggered", lang), parse_mode=None
        )
        if lang == "en" and isinstance(summary, dict):
            period_type = summary.get("period_type")
            war_type = (
                t("debug_war_type_coliseum", "en")
                if period_type == "colosseum"
                else t("debug_war_type_riverside", "en")
            )
            text = t(
                "debug_reminder_summary",
                "en",
                season=summary.get("season_id", "n/a"),
                section=summary.get("section_index", "n/a"),
                day=summary.get("day_number", "n/a"),
                war_type=war_type,
                resolved_by=summary.get("resolved_by", "n/a"),
                snapshot=summary.get("snapshot", "n/a"),
                override=summary.get("override", "none"),
            )
            await message.answer(text, parse_mode=None)
    except Exception as e:
        logger.error("Debug reminder failed: %s", e, exc_info=True)
        await message.answer(t("debug_reminder_failed", lang), parse_mode=None)


@router.message(Command("riverside"))
async def cmd_riverside(message: Message) -> None:
    """Debug: send Clan War reminder to this chat only."""
    lang = await _get_lang_for_message(message)
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer(t("not_allowed", lang), parse_mode=None)
        return
    day = _parse_debug_day(message.text)
    templates = {
        1: t("riverside_day1", lang),
        2: t("riverside_day2", lang),
        3: t("riverside_day3", lang),
        4: t("riverside_day4", lang),
    }
    await _send_debug_reminder(
        message,
        war_type=t("debug_war_type_riverside", lang),
        day=day,
        banner_url="https://i.ibb.co/VyGjscj/image.png",
        banner_url_day4="https://i.ibb.co/0jvgVSgq/image-1.jpg",
        templates=templates,
        lang=lang,
    )


@router.message(Command("coliseum"))
async def cmd_coliseum(message: Message) -> None:
    """Debug: send Colosseum reminder to this chat only."""
    lang = await _get_lang_for_message(message)
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer(t("not_allowed", lang), parse_mode=None)
        return
    day = _parse_debug_day(message.text)
    templates = {
        1: t("coliseum_day1", lang),
        2: t("coliseum_day2", lang),
        3: t("coliseum_day3", lang),
        4: t("coliseum_day4", lang),
    }
    await _send_debug_reminder(
        message,
        war_type=t("debug_war_type_coliseum", lang),
        day=day,
        banner_url="https://i.ibb.co/Cs4Sjpzw/image.png",
        banner_url_day4="https://i.ibb.co/R4YLyPzR/image.jpg",
        templates=templates,
        lang=lang,
    )


@router.message(Command("captcha_send"))
async def cmd_captcha_send(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer(t("not_allowed", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(
            t("reply_to_user_message_in_group", lang), parse_mode=None
        )
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer(t("reply_to_user_message", lang), parse_mode=None)
        return

    target = message.reply_to_message.from_user
    if target.is_bot:
        await message.answer(t("captcha_send_bot", lang), parse_mode=None)
        return
    if _is_debug_admin(target.id):
        await message.answer(t("captcha_send_admin", lang), parse_mode=None)
        return

    try:
        if await _is_admin_user(message, target.id):
            await message.answer(t("captcha_send_admin", lang), parse_mode=None)
            return
    except Exception:
        pass

    chat_id = message.chat.id
    await send_modlog(
        message.bot,
        t(
            "modlog_captcha_send",
            DEFAULT_LANG,
            admin_id=message.from_user.id,
            user_id=target.id,
            chat_id=chat_id,
        ),
    )
    try:
        if await is_user_verified(chat_id, target.id):
            await message.answer(
                t("captcha_user_already_verified", lang),
                parse_mode=None,
            )
            return
    except Exception as e:
        logger.error("Failed to check verification: %s", e, exc_info=True)

    try:
        await message.bot.restrict_chat_member(
            chat_id,
            target.id,
            permissions=ChatPermissions(
                can_send_messages=False,
                can_send_media_messages=False,
                can_send_other_messages=False,
                can_add_web_page_previews=False,
            ),
        )
    except Exception as e:
        logger.warning("Failed to restrict user: %s", e, exc_info=True)
        await send_modlog(
            message.bot,
            t(
                "modlog_captcha_restrict_failed",
                DEFAULT_LANG,
                chat_id=chat_id,
                user_id=target.id,
                error=e,
            ),
        )

    try:
        challenge, question = await get_or_create_pending_challenge(
            chat_id, target.id, CAPTCHA_EXPIRE_MINUTES
        )
    except Exception as e:
        logger.error("Failed to create captcha challenge: %s", e, exc_info=True)
        await message.answer(t("captcha_create_failed", lang), parse_mode=None)
        return

    if not challenge:
        await message.answer(t("captcha_create_failed", lang), parse_mode=None)
        return
    if not question:
        question = await get_captcha_question(challenge["question_id"])
    if not question:
        await message.answer(
            t("captcha_question_unavailable", lang), parse_mode=None
        )
        return

    message_id = await _send_captcha_message(
        message.bot,
        chat_id,
        challenge_id=challenge["id"],
        question=question,
        mention=_build_user_mention(target),
    )
    if message_id:
        await update_challenge_message_id(challenge["id"], message_id)
        await touch_last_reminded_at(challenge["id"], datetime.now(timezone.utc))
        await message.answer(
            t(
                "captcha_sent",
                lang,
                user=_format_user_label(target),
                challenge_id=challenge["id"],
                message_id=message_id,
            ),
            parse_mode=None,
        )
        return

    await message.answer(
        t("captcha_send_failed", lang), parse_mode=None
    )


@router.message(Command("captcha_status"))
async def cmd_captcha_status(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer(t("not_allowed", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(
            t("reply_to_user_message_in_group", lang), parse_mode=None
        )
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer(t("reply_to_user_message", lang), parse_mode=None)
        return
    target = message.reply_to_message.from_user
    chat_id = message.chat.id
    await send_modlog(
        message.bot,
        t(
            "modlog_captcha_status",
            DEFAULT_LANG,
            admin_id=message.from_user.id,
            user_id=target.id,
            chat_id=chat_id,
        ),
    )

    try:
        is_verified = await is_user_verified(chat_id, target.id)
        challenge = await get_pending_challenge(chat_id, target.id)
        if not challenge:
            challenge = await get_latest_challenge(chat_id, target.id)
        question_text = None
        if challenge:
            question = await get_captcha_question(challenge["question_id"])
            if question:
                question_text = question.get("question_text") or ""
        if question_text:
            if len(question_text) > 120:
                question_text = f"{question_text[:117]}..."
        else:
            question_text = t("na", lang)
    except Exception as e:
        logger.error("Failed to fetch captcha status: %s", e, exc_info=True)
        await message.answer(
            t("captcha_status_unavailable", lang), parse_mode=None
        )
        return

    lines = [
        t("captcha_status_title", lang),
        t("captcha_status_chat", lang, chat_id=chat_id),
        t(
            "captcha_status_user",
            lang,
            user=_format_user_label(target),
            user_id=target.id,
        ),
        t(
            "captcha_status_verified",
            lang,
            status=t("yes" if is_verified else "no", lang),
        ),
    ]
    if not challenge:
        lines.append(t("captcha_status_challenge_none", lang))
        await message.answer("\n".join(lines), parse_mode=None)
        return

    lines.extend(
        [
            t(
                "captcha_status_challenge",
                lang,
                challenge_id=challenge.get("id"),
                status=challenge.get("status"),
                attempts=challenge.get("attempts"),
                created_at=_format_dt(challenge.get("created_at")),
            ),
            t(
                "captcha_status_details",
                lang,
                expires_at=_format_dt(challenge.get("expires_at")),
                message_id=challenge.get("message_id") or t("na", lang),
                last_reminded_at=_format_dt(challenge.get("last_reminded_at")),
                question_id=challenge.get("question_id"),
            ),
            t("captcha_status_question", lang, question=question_text),
        ]
    )
    await message.answer("\n".join(lines), parse_mode=None)


@router.message(Command("captcha_reset"))
async def cmd_captcha_reset(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer(t("not_allowed", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(
            t("reply_to_user_message_in_group", lang), parse_mode=None
        )
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer(t("reply_to_user_message", lang), parse_mode=None)
        return
    target = message.reply_to_message.from_user
    chat_id = message.chat.id
    await send_modlog(
        message.bot,
        t(
            "modlog_captcha_reset",
            DEFAULT_LANG,
            admin_id=message.from_user.id,
            user_id=target.id,
            chat_id=chat_id,
        ),
    )

    try:
        await expire_active_challenges(chat_id, target.id)
        await delete_verified_user(chat_id, target.id)
    except Exception as e:
        logger.error("Failed to reset captcha state: %s", e, exc_info=True)

    try:
        await message.bot.restrict_chat_member(
            chat_id,
            target.id,
            permissions=ChatPermissions(
                can_send_messages=False,
                can_send_media_messages=False,
                can_send_other_messages=False,
                can_add_web_page_previews=False,
            ),
        )
    except Exception as e:
        logger.warning("Failed to restrict user: %s", e, exc_info=True)
        await send_modlog(
            message.bot,
            t(
                "modlog_captcha_restrict_failed",
                DEFAULT_LANG,
                chat_id=chat_id,
                user_id=target.id,
                error=e,
            ),
        )

    try:
        challenge, question = await get_or_create_pending_challenge(
            chat_id, target.id, CAPTCHA_EXPIRE_MINUTES
        )
    except Exception as e:
        logger.error("Failed to create captcha challenge: %s", e, exc_info=True)
        await message.answer(t("captcha_create_new_failed", lang), parse_mode=None)
        return

    if not challenge:
        await message.answer(t("captcha_create_new_failed", lang), parse_mode=None)
        return

    if not question:
        question = await get_captcha_question(challenge["question_id"])
    message_id = None
    if question:
        message_id = await _send_captcha_message(
            message.bot,
            chat_id,
            challenge_id=challenge["id"],
            question=question,
            mention=_build_user_mention(target),
        )
    if message_id:
        await update_challenge_message_id(challenge["id"], message_id)
        await message.answer(
            t("captcha_reset_sent", lang), parse_mode=None
        )
        return

    await message.answer(
        t("captcha_reset_send_failed", lang), parse_mode=None
    )


@router.message(Command("captcha_verify"))
async def cmd_captcha_verify(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer(t("not_allowed", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(
            t("reply_to_user_message_in_group", lang), parse_mode=None
        )
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer(t("reply_to_user_message", lang), parse_mode=None)
        return
    target = message.reply_to_message.from_user
    chat_id = message.chat.id
    await send_modlog(
        message.bot,
        t(
            "modlog_captcha_verify",
            DEFAULT_LANG,
            admin_id=message.from_user.id,
            user_id=target.id,
            chat_id=chat_id,
        ),
    )

    try:
        await set_user_verified(chat_id, target.id)
        await mark_pending_challenges_passed(chat_id, target.id)
    except Exception as e:
        logger.error("Failed to mark user verified: %s", e, exc_info=True)
        await message.answer(t("captcha_verify_failed", lang), parse_mode=None)
        return

    try:
        await message.bot.restrict_chat_member(
            chat_id,
            target.id,
            permissions=ChatPermissions(
                can_send_messages=True,
                can_send_media_messages=True,
                can_send_other_messages=True,
                can_add_web_page_previews=True,
            ),
        )
    except Exception as e:
        logger.warning("Failed to unrestrict user: %s", e, exc_info=True)
        await send_modlog(
            message.bot,
            t(
                "modlog_captcha_unrestrict_failed",
                DEFAULT_LANG,
                chat_id=chat_id,
                user_id=target.id,
                error=e,
            ),
        )

    await message.answer(t("captcha_verify_done", lang), parse_mode=None)


@router.message(Command("captcha_unverify"))
async def cmd_captcha_unverify(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer(t("not_allowed", lang), parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(
            t("reply_to_user_message_in_group", lang), parse_mode=None
        )
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer(t("reply_to_user_message", lang), parse_mode=None)
        return
    target = message.reply_to_message.from_user
    chat_id = message.chat.id
    await send_modlog(
        message.bot,
        t(
            "modlog_captcha_unverify",
            DEFAULT_LANG,
            admin_id=message.from_user.id,
            user_id=target.id,
            chat_id=chat_id,
        ),
    )

    try:
        await delete_verified_user(chat_id, target.id)
    except Exception as e:
        logger.error("Failed to remove verified flag: %s", e, exc_info=True)
        await message.answer(
            t("captcha_unverify_failed", lang), parse_mode=None
        )
        return

    await message.answer(t("captcha_unverify_done", lang), parse_mode=None)


@router.message(Command("modlog_test"))
async def cmd_modlog_test(message: Message) -> None:
    lang = await _get_lang_for_message(message)
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer(t("not_allowed", lang), parse_mode=None)
        return
    if MODLOG_CHAT_ID == 0:
        await message.answer(t("modlog_not_configured", lang), parse_mode=None)
        return
    try:
        await message.bot.send_message(
            MODLOG_CHAT_ID,
            t("modlog_test_message", DEFAULT_LANG),
            parse_mode=None,
            disable_web_page_preview=True,
        )
    except Exception as e:
        await message.answer(
            t(
                "modlog_test_failed",
                lang,
                error_type=type(e).__name__,
                error=str(e),
            ),
            parse_mode=None,
        )
        return

    await message.answer(t("modlog_test_sent", lang), parse_mode=None)


@router.message(Command("donations"))
async def cmd_donations(message: Message) -> None:
    """Show donation leaderboards for the clan."""
    lang = await _get_lang_for_message(message)
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return
    clan_name = t("unknown", lang)
    try:
        api_client = await get_api_client()
        clan_data = await api_client.get_clan(clan_tag)
        if isinstance(clan_data, dict):
            clan_name = clan_data.get("name") or clan_name
    except ClashRoyaleAPIError as e:
        logger.warning("Failed to fetch clan name: %s", e)
    except Exception as e:
        logger.warning("Failed to fetch clan name: %s", e)
    report = await build_donations_report(
        clan_tag, clan_name, lang=lang
    )
    await message.answer(report, parse_mode=None)


@router.message(Command("promote_candidates"))
async def cmd_promote_candidates(message: Message) -> None:
    """Show promotion recommendations."""
    lang = await _get_lang_for_message(message)
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return
    report = await build_promotion_candidates_report(clan_tag, lang=lang)
    await message.answer(report, parse_mode=None)


@router.message(Command("my_activity"))
async def cmd_my_activity(message: Message) -> None:
    """Show the current user's war activity report."""
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_identify_account", lang), parse_mode=None)
        return

    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return

    args = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            args = parts[1].strip()

    existing = await get_user_link(message.from_user.id)
    if existing:
        await message.answer(
            t(
                "my_activity_linked",
                lang,
                name=existing["player_name"],
                tag=existing["player_tag"],
            ),
            parse_mode=None,
        )
        report = await build_my_activity_report(
            existing["player_tag"],
            existing["player_name"],
            clan_tag,
            lang=lang,
        )
        await message.answer(report, parse_mode=None)
        try:
            await _send_war_activity_chart(
                message,
                clan_tag=clan_tag,
                player_tag=existing["player_tag"],
                title=t("chart.war_activity.title", lang),
                lang=lang,
            )
        except Exception as e:
            logger.warning(
                "Failed to send my_activity chart: %s", e, exc_info=True
            )
        return

    if message.chat.type == ChatType.PRIVATE:
        await upsert_user_link_request(
            telegram_user_id=message.from_user.id,
            status="awaiting_name",
            origin_chat_id=None,
        )
        if args:
            await _handle_link_candidates(
                message=message,
                target_user_id=message.from_user.id,
                nickname=args,
                source="self",
                origin_chat_id=None,
            )
        else:
            await message.answer(
                t("my_activity_not_linked", lang),
                parse_mode=None,
            )
        return

    await _send_link_button(message)


@router.message(Command("activity"))
async def cmd_activity(message: Message) -> None:
    """Show a player's activity report by nickname, @username, or reply."""
    lang = await _get_lang_for_message(message)
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer(t("clan_tag_not_configured", lang), parse_mode=None)
        return

    args = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            args = parts[1].strip()

    if args.startswith("@"):
        try:
            chat = await message.bot.get_chat(args)
        except Exception:
            chat = None
        if not chat or chat.type != ChatType.PRIVATE:
            await message.answer(
                t("activity_username_not_found", lang),
                parse_mode=None,
            )
            return
        link = await get_user_link(chat.id)
        if not link:
            await message.answer(
                t("activity_user_not_linked", lang),
                parse_mode=None,
            )
            return
        report = await build_my_activity_report(
            link["player_tag"], link["player_name"], clan_tag, lang=lang
        )
        await message.answer(report, parse_mode=None)
        try:
            await _send_war_activity_chart(
                message,
                clan_tag=clan_tag,
                player_tag=link["player_tag"],
                title=t(
                    "chart.war_activity.title_named",
                    lang,
                    name=link["player_name"],
                ),
                lang=lang,
            )
        except Exception as e:
            logger.warning(
                "Failed to send activity chart: %s", e, exc_info=True
            )
        return

    if not args and message.reply_to_message and message.reply_to_message.from_user:
        target_id = message.reply_to_message.from_user.id
        link = await get_user_link(target_id)
        if not link:
            await message.answer(
                t("activity_user_not_linked", lang),
                parse_mode=None,
            )
            return
        report = await build_my_activity_report(
            link["player_tag"], link["player_name"], clan_tag, lang=lang
        )
        await message.answer(report, parse_mode=None)
        try:
            await _send_war_activity_chart(
                message,
                clan_tag=clan_tag,
                player_tag=link["player_tag"],
                title=t(
                    "chart.war_activity.title_named",
                    lang,
                    name=link["player_name"],
                ),
                lang=lang,
            )
        except Exception as e:
            logger.warning(
                "Failed to send activity chart: %s", e, exc_info=True
            )
        return

    if not args:
        await message.answer(
            t("activity_usage", lang),
            parse_mode=None,
        )
        return

    candidates = await search_player_candidates(clan_tag, args)
    if not candidates:
        await message.answer(
            t("activity_no_player_found", lang),
            parse_mode=None,
        )
        return

    if len(candidates) > 1:
        lines = [t("activity_multiple_found", lang)]
        for index, candidate in enumerate(candidates, 1):
            tag = _normalize_tag(candidate["player_tag"])
            status = (
                t("status_in_clan", lang)
                if candidate.get("in_clan")
                else t("status_not_in_clan", lang)
            )
            lines.append(
                t(
                    "activity_candidate_line",
                    lang,
                    index=index,
                    name=candidate["player_name"],
                    tag=tag,
                    status=status,
                )
            )
        await message.answer("\n".join(lines), parse_mode=None)
        return

    candidate = candidates[0]
    player_tag = _normalize_tag(candidate["player_tag"])
    report = await build_my_activity_report(
        player_tag,
        candidate["player_name"],
        clan_tag,
        lang=lang,
    )
    await message.answer(report, parse_mode=None)
    try:
        await _send_war_activity_chart(
            message,
            clan_tag=clan_tag,
            player_tag=player_tag,
            title=t(
                "chart.war_activity.title_named",
                lang,
                name=candidate["player_name"],
            ),
            lang=lang,
        )
    except Exception as e:
        logger.warning(
            "Failed to send activity chart: %s", e, exc_info=True
        )


@router.message(Command("admin_link_name"))
async def cmd_admin_link_name(message: Message) -> None:
    """Link a user account by nickname (admin-only, reply required)."""
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_verify_permissions", lang), parse_mode=None)
        return

    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer(
            t("admin_link_reply_prompt", lang),
            parse_mode=None,
        )
        return

    try:
        is_admin = await _is_admin_user(message, message.from_user.id)
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return

    if not is_admin:
        await message.answer(t("no_permission", lang), parse_mode=None)
        return

    args = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            args = parts[1].strip()

    if not args:
        await message.answer(t("admin_link_missing_nickname", lang), parse_mode=None)
        return

    await _handle_link_candidates(
        message=message,
        target_user_id=message.reply_to_message.from_user.id,
        nickname=args,
        source="admin",
        origin_chat_id=message.chat.id,
    )


@router.message(Command("unlink"))
async def cmd_unlink(message: Message) -> None:
    """Unlink a user account (admin-only, reply required)."""
    lang = await _get_lang_for_message(message)
    if message.from_user is None:
        await message.answer(t("unable_verify_permissions", lang), parse_mode=None)
        return

    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer(
            t("unlink_reply_prompt", lang),
            parse_mode=None,
        )
        return

    try:
        is_admin = await _is_admin_user(message, message.from_user.id)
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer(t("unable_verify_admin_status", lang), parse_mode=None)
        return

    if not is_admin:
        await message.answer(
            t("no_permission", lang),
            parse_mode=None,
        )
        return

    target_id = message.reply_to_message.from_user.id
    existing = await get_user_link(target_id)
    if not existing:
        await message.answer(t("unlink_no_account", lang), parse_mode=None)
        return

    await delete_user_link(target_id)
    await delete_user_link_request(target_id)
    await message.answer(
        t(
            "unlink_done",
            lang,
            name=existing["player_name"],
            tag=existing["player_tag"],
        ),
        parse_mode=None,
    )


@router.message(F.text & (F.chat.type == ChatType.PRIVATE))
async def handle_private_text(message: Message) -> None:
    if message.text is None or message.text.startswith("/"):
        return
    if message.from_user is None:
        return
    lang = await _get_lang_for_message(message)

    state_key = _apply_state_key(message.from_user.id)
    apply_state = await get_app_state(state_key)
    if apply_state:
        status = apply_state.get("status")
        if status == "awaiting_name":
            nickname = message.text.strip()
            if not nickname:
                await message.answer(
                    t("apply_nickname_empty", lang),
                    parse_mode=None,
                )
                return
            if len(nickname) > 32:
                await message.answer(
                    t("apply_nickname_too_long", lang),
                    parse_mode=None,
                )
                return

            pending_app = await get_pending_application_for_user(message.from_user.id)
            if pending_app:
                await delete_app_state(state_key)
                await message.answer(
                    t(
                        "apply_already_pending",
                        lang,
                        summary=_format_application_summary(pending_app),
                    ),
                    parse_mode=None,
                )
                return

            app = await create_application(
                telegram_user_id=message.from_user.id,
                telegram_username=message.from_user.username,
                telegram_display_name=message.from_user.full_name,
                player_name=nickname,
                player_tag=None,
            )
            await set_app_state(
                state_key,
                {
                    "status": "awaiting_tag",
                    "application_id": app["id"],
                    "player_name": nickname,
                },
            )
            user_label = message.from_user.full_name
            if message.from_user.username:
                user_label = f"{user_label} (@{message.from_user.username})"
            await _notify_application(
                message.bot,
                t(
                    "app_admin_notify_new",
                    DEFAULT_LANG,
                    app_id=app["id"],
                    player_name=nickname,
                    player_tag=t("na", DEFAULT_LANG),
                    user_label=user_label,
                    user_id=message.from_user.id,
                ),
            )
            await message.answer(
                t("apply_send_tag_optional", lang),
                parse_mode=None,
            )
            return

        if status == "awaiting_tag":
            app_id = apply_state.get("application_id")
            if not app_id:
                await delete_app_state(state_key)
                await message.answer(
                    t("apply_restart", lang),
                    parse_mode=None,
                )
                return
            ok, tag = _parse_optional_tag(message.text)
            if not ok:
                await message.answer(
                    t("apply_invalid_tag", lang),
                    parse_mode=None,
                )
                return
            if tag:
                await update_application_tag(app_id, tag)
            await delete_app_state(state_key)
            await message.answer(
                t("apply_received", lang),
                parse_mode=None,
            )
            return

    request = await get_user_link_request(message.from_user.id)
    if not request:
        return

    status = request.get("status")
    if status == "awaiting_name":
        await _handle_link_candidates(
            message=message,
            target_user_id=message.from_user.id,
            nickname=message.text,
            source="self",
            origin_chat_id=None,
        )
    elif status == "awaiting_choice":
        await message.answer(
            t("link_select_prompt", lang), parse_mode=None
        )


@router.callback_query(F.data.startswith("link_select:"))
async def handle_link_select(query: CallbackQuery) -> None:
    lang = await _get_lang_for_query(query)
    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 3:
        await query.answer(t("invalid_selection", lang), show_alert=True)
        return

    try:
        target_user_id = int(parts[1])
    except ValueError:
        await query.answer(t("invalid_selection", lang), show_alert=True)
        return

    tag = _normalize_tag(parts[2])

    request = await get_user_link_request(target_user_id)
    if not request or request.get("status") != "awaiting_choice":
        await query.answer(t("link_request_expired", lang), show_alert=True)
        return

    if query.from_user is None:
        await query.answer(t("unable_verify_account", lang), show_alert=True)
        return

    if query.from_user.id != target_user_id:
        authorized = False
        if query.message is not None:
            try:
                authorized = await _is_admin_user(query.message, query.from_user.id)
            except Exception:
                authorized = False
        if not authorized and query.from_user.id not in ADMIN_USER_IDS:
            await query.answer(t("link_not_allowed", lang), show_alert=True)
            return

    clan_tag = _require_clan_tag()
    if not clan_tag:
        await query.answer(t("clan_tag_not_configured", lang), show_alert=True)
        return

    player_name = await get_player_name_for_tag(tag, clan_tag)
    if not player_name:
        player_name = t("unknown", lang)

    source = "self"
    if request.get("origin_chat_id") is not None and query.from_user.id != target_user_id:
        source = "admin"

    await upsert_user_link(
        telegram_user_id=target_user_id,
        player_tag=tag,
        player_name=player_name,
        source=source,
    )
    await delete_user_link_request(target_user_id)

    if query.message is not None:
        await query.message.answer(
            t(
                "link_success",
                lang,
                name=player_name,
                tag=tag,
            ),
            parse_mode=None,
        )
    else:
        target_lang = await get_user_language(None, target_user_id)
        await query.bot.send_message(
            target_user_id,
            t(
                "link_success",
                target_lang,
                name=player_name,
                tag=tag,
            ),
            parse_mode=None,
        )

    await query.answer(t("link_confirm", lang))


@router.callback_query(F.data.startswith("lang_select:"))
async def handle_lang_select(query: CallbackQuery) -> None:
    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 3:
        await query.answer(t("invalid_selection", DEFAULT_LANG), show_alert=True)
        return
    try:
        target_user_id = int(parts[1])
    except ValueError:
        await query.answer(t("invalid_selection", DEFAULT_LANG), show_alert=True)
        return
    lang_code = parts[2]
    if query.from_user is None or query.from_user.id != target_user_id:
        await query.answer(t("lang_not_for_you", DEFAULT_LANG), show_alert=True)
        return
    await set_user_language(target_user_id, lang_code)
    confirm = (
        f"{t('lang_set_confirm', lang_code)} "
        f"{t('lang_set_change_hint', lang_code)}"
    )
    if query.message is not None:
        try:
            await query.message.edit_text(confirm, parse_mode=None)
        except Exception:
            pass
    await query.answer(confirm, show_alert=False)


@router.message(
    F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}),
    ~(F.text.startswith("/") | F.caption.startswith("/")),
)
async def trace_catch_all(message: Message) -> None:
    if message.from_user is None or message.from_user.is_bot:
        return
    if not await _is_mod_debug(message.chat.id):
        return
    logger.warning(
        "[TRACE] catch-all consumed: chat=%s msg_id=%s from=%s text=%r",
        message.chat.id,
        message.message_id,
        message.from_user.id,
        (message.text or message.caption or "")[:200],
    )
