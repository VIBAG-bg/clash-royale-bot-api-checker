"""Telegram bot command handlers using aiogram v3."""

import logging
from datetime import datetime, timedelta, timezone

from aiogram import Bot, F, Router
from aiogram.enums import ChatMemberStatus, ChatType, MessageEntityType
from aiogram.filters import Command
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
    CLAN_TAG,
    ENABLE_CAPTCHA,
    FLOOD_MAX_MESSAGES,
    FLOOD_MUTE_MINUTES,
    FLOOD_WINDOW_SECONDS,
    INACTIVE_LAST_SEEN_LIMIT,
    LAST_SEEN_RED_DAYS,
    LAST_SEEN_YELLOW_DAYS,
    MODERATION_ENABLED,
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
    get_first_seen_time,
    get_last_rejected_time_for_user,
    get_latest_challenge,
    get_pending_application_for_user,
    get_or_create_pending_challenge,
    get_pending_challenge,
    get_top_absent_members,
    get_warning_info,
    increment_challenge_attempts,
    record_rate_counter,
    increment_user_warning,
    reset_user_warnings,
    is_user_verified,
    log_mod_action,
    mark_challenge_expired,
    mark_challenge_failed,
    mark_challenge_passed,
    mark_pending_challenges_passed,
    list_pending_applications,
    get_player_name_for_tag,
    get_challenge_by_id,
    get_user_link,
    get_user_link_request,
    list_mod_actions,
    list_mod_actions_for_user,
    search_player_candidates,
    set_chat_raid_mode,
    set_application_status,
    set_user_penalty,
    clear_user_penalty,
    set_user_verified,
    touch_last_reminded_at,
    update_application_tag,
    mark_application_invited,
    update_challenge_message_id,
    upsert_clan_chat,
    upsert_user_link,
    upsert_user_link_request,
    delete_app_state,
    set_app_state,
)
from reports import (
    build_clan_info_report,
    build_current_war_report,
    build_donations_report,
    build_kick_shortlist_report,
    build_my_activity_report,
    build_promotion_candidates_report,
    build_rolling_report,
    build_weekly_report,
)
from riverrace_import import get_last_completed_week, get_last_completed_weeks

logger = logging.getLogger(__name__)
logger.info("MODLOG_CHAT_ID loaded as %r", MODLOG_CHAT_ID)

# Create router for handlers
router = Router(name="main_handlers")
moderation_router = Router(name="moderation_router")

HEADER_LINE = "══════════════════════════════"
DIVIDER_LINE = "---------------------------"


def _format_help_commands(commands: list[dict[str, object]]) -> list[str]:
    lines: list[str] = []
    for index, cmd in enumerate(commands, 1):
        usage = cmd["usage"][0] if cmd["usage"] else cmd["name"]
        lines.append(f"{index}) {cmd['name']} — {cmd['what']}")
        lines.append(f"   Usage: {usage}")
    return lines


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


def _format_user_label(user: object) -> str:
    if not hasattr(user, "full_name"):
        return "Unknown"
    label = user.full_name
    username = getattr(user, "username", None)
    if username:
        label = f"{label} (@{username})"
    return label


def _build_user_mention(user: object) -> str:
    username = getattr(user, "username", None)
    if username:
        return f"@{username}"
    user_id = getattr(user, "id", None)
    if user_id:
        return f"tg://user?id={user_id}"
    return "user"


def _format_application_summary(app: dict[str, object]) -> str:
    tag = app.get("player_tag") or "n/a"
    user_display = app.get("telegram_username") or app.get("telegram_display_name") or "user"
    created_at = _format_dt(app.get("created_at"))
    return f"{app.get('player_name')} | {tag} | {user_display} | {created_at}"


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
            f"[MOD] ERROR: delete failed: chat={message.chat.id} "
            f"user={message.from_user.id if message.from_user else 'n/a'} "
            f"msg_id={message.message_id} err={e}",
        )


async def _mute_user(
    message: Message, user_id: int, *, minutes: int, reason: str
) -> None:
    until = datetime.now(timezone.utc) + timedelta(minutes=minutes)
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
            f"[MOD] ERROR: mute failed: chat={message.chat.id} "
            f"user={user_id} err={e}",
        )
        return
    await set_user_penalty(
        message.chat.id, user_id, "mute", until=until
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
        f"[MOD] mute: chat={message.chat.id} user={user_id} "
        f"until={until.isoformat()} reason={reason}",
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
        "🛡 Please verify to chat.\n"
        f"{question.get('question_text')}\n"
        "Choose the correct answer:"
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
    buttons: list[list[InlineKeyboardButton]] = []
    if WELCOME_RULES_MESSAGE_LINK:
        buttons.append(
            [InlineKeyboardButton(text="📌 Rules", url=WELCOME_RULES_MESSAGE_LINK)]
        )
    if BOT_USERNAME:
        username = BOT_USERNAME.lstrip("@")
        buttons.append(
            [
                InlineKeyboardButton(
                    text="🔗 Link account",
                    url=f"https://t.me/{username}?start=link",
                ),
                InlineKeyboardButton(
                    text="📝 Apply",
                    url=f"https://t.me/{username}?start=apply",
                ),
            ]
        )
    keyboard = (
        InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
    )
    await bot.send_message(
        chat_id,
        f"Welcome, {user_display}! You can now chat.",
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
    return member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR)


async def _send_link_button(message: Message) -> None:
    username = await _get_bot_username(message)
    if not username:
        await message.answer("Unable to generate link right now.", parse_mode=None)
        return
    url = f"https://t.me/{username}?start=link"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🔗 Link my account", url=url)]]
    )
    prefix = ""
    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        prefix = "Open bot in private to link.\n"
    await message.answer(
        f"{prefix}Tap the button below to link your account.",
        reply_markup=keyboard,
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
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer("CLAN_TAG is not configured.", parse_mode=None)
        return

    candidates = await search_player_candidates(clan_tag, nickname)

    if not candidates:
        await message.answer(
            "No player found with that nickname in clan data. Please check spelling and send again.",
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
            f"✅ Linked: {candidate['player_name']} ({_normalize_tag(candidate['player_tag'])}). Now use /my_activity.",
            parse_mode=None,
        )
        return

    buttons: list[list[InlineKeyboardButton]] = []
    lines = ["Multiple matches found. Choose yours:"]
    for candidate in candidates:
        tag = _normalize_tag(candidate["player_tag"])
        status = "IN CLAN" if candidate.get("in_clan") else "NOT IN CLAN"
        label = f"{candidate['player_name']} - {tag} - {status}"
        data = f"link_select:{target_user_id}:{tag.lstrip('#')}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=data)])
        lines.append(f"{len(buttons)}) {candidate['player_name']} — {tag} — {status}")

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
    if args == "link":
        if message.chat.type != ChatType.PRIVATE:
            await _send_link_button(message)
            return
        if message.from_user is None:
            await message.answer("Unable to identify your account.", parse_mode=None)
            return
        existing = await get_user_link(message.from_user.id)
        if existing:
            await message.answer(
                f"You are already linked to {existing['player_name']} ({existing['player_tag']}). Use /my_activity.",
                parse_mode=None,
            )
            return
        await upsert_user_link_request(
            telegram_user_id=message.from_user.id,
            status="awaiting_name",
            origin_chat_id=None,
        )
        await message.answer(
            "Send your in-game nickname exactly as it appears.",
            parse_mode=None,
        )
        return

    if args == "apply":
        if message.chat.type != ChatType.PRIVATE:
            await message.answer("Please open bot in DM to apply.", parse_mode=None)
            return
        if not APPLY_ENABLED:
            await message.answer(
                "Applications are disabled right now.", parse_mode=None
            )
            return
        if message.from_user is None:
            await message.answer("Unable to identify your account.", parse_mode=None)
            return
        clan_tag = _require_clan_tag()
        if not clan_tag:
            await message.answer("CLAN_TAG is not configured.", parse_mode=None)
            return

        pending_app = await get_pending_application_for_user(message.from_user.id)
        if pending_app:
            await message.answer(
                "You already have a pending application:\n"
                f"{_format_application_summary(pending_app)}",
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
                    f"Please wait {hours}h {minutes}m before applying again.",
                    parse_mode=None,
                )
                return

        pending_count = await count_pending_applications()
        if pending_count >= APPLY_MAX_PENDING:
            await message.answer(
                "Application queue is full right now. Please try later.",
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
                    f"Clan has free slots now. Join using clan tag {clan_tag}.",
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
                "Please send your player tag (or 'skip').", parse_mode=None
            )
            return

        await set_app_state(
            state_key,
            {"status": "awaiting_name", "started_at": datetime.now(timezone.utc).isoformat()},
        )
        await message.answer(
            "Send your in-game nickname exactly as it appears.",
            parse_mode=None,
        )
        return

    welcome_text = (
        "Welcome to the Clash Royale Clan Monitor Bot!\n\n"
        "This bot monitors your clan's River Race participation "
        "and tracks player activity.\n\n"
        "Available commands:\n"
        "/start - Show this welcome message\n"
        "/ping - Check if the bot is responsive\n"
        "/inactive - Show players with low River Race participation\n"
        "/war - Weekly war report\n"
        "/war8 - Rolling 8-week report\n"
        "/list_for_kick - Kick shortlist\n"
        "/current_war - Current war snapshot\n"
        "/my_activity - Your activity report\n"
        "/activity - Show activity by nickname or @username\n"
        "/donations - Donations leaderboard"
    )
    await message.answer(welcome_text, parse_mode=None)


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    """Show help for available commands."""
    is_admin = False
    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        if message.from_user is not None:
            try:
                member = await message.bot.get_chat_member(
                    message.chat.id, message.from_user.id
                )
                is_admin = member.status in (
                    ChatMemberStatus.ADMINISTRATOR,
                    ChatMemberStatus.CREATOR,
                )
            except Exception:
                is_admin = False

    general_lines = [
        "/help - show this help",
        "/start - welcome (/start link, /start apply)",
        "/ping - health check",
        "/war - weekly report",
        "/war8 - last 8 weeks report",
        "/war_all - war + war8 + kick list",
        "/current_war - current week snapshot",
        "/my_activity - your activity",
        "/activity <nickname>|@username|reply - activity by name/@/reply",
        "/donations - donations leaderboard",
        "/list_for_kick - kick shortlist",
        "/inactive - last seen list",
        "/promote_candidates - promotions",
        "/info - clan info",
    ]

    admin_lines = [
        "/bind - bind this chat",
        "/admin_link_name <nickname> - link user (reply)",
        "/unlink - unlink user (reply)",
        "/apps [N] - list applications",
        "/app <id> - application details",
        "/app_approve <id> - approve",
        "/app_reject <id> <reason> - reject",
        "/app_notify <id> - notify slot",
        "/captcha_send - send captcha (reply)",
        "/captcha_status - captcha status (reply)",
        "/captcha_reset - reset captcha (reply)",
        "/captcha_verify - verify user (reply)",
        "/captcha_unverify - remove verify (reply)",
        "/riverside <day> - test war reminder",
        "/coliseum <day> - test colosseum reminder",
        "/modlog_test - test modlog",
    ]

    moderation_lines = [
        "/warn <reason> - warn user (reply)",
        "/warns [N] - last warnings (reply)",
        "/mute <min> <reason> - mute user (reply)",
        "/unmute - unmute user (reply)",
        "/ban <reason> - ban user (reply)",
        "/unban <id> - unban by id",
        "/purge <N> - delete last N messages",
        "/raid_on - enable raid mode",
        "/raid_off - disable raid mode",
        "/raid_status - show raid settings",
        "/modlog [N] - recent mod actions",
    ]

    lines = [

        HEADER_LINE,
        "🤖 Black Poison Bot - Help",
        HEADER_LINE,
        "📌 GENERAL COMMANDS",
        DIVIDER_LINE,
        *general_lines,
    ]
    if is_admin:
        lines.extend(
            [
                "",
                "🛡 ADMIN COMMANDS",
                DIVIDER_LINE,
                *admin_lines,
                "",
                "🧰 MODERATION COMMANDS",
                DIVIDER_LINE,
                *moderation_lines,
            ]
        )
    lines.extend(
        [
            HEADER_LINE,
            "ℹ️ Tip: Use /my_activity in DM to link your account.",
            HEADER_LINE,
        ]
    )
    await message.answer("\n".join(lines), parse_mode=None)

@router.message(Command("apps"))
async def cmd_apps(message: Message) -> None:
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer("Not allowed.", parse_mode=None)
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
    if not apps:
        await message.answer("No pending applications.", parse_mode=None)
        return

    lines = [f"Pending applications (latest {len(apps)}):"]
    for app in apps:
        tag = app.get("player_tag") or "n/a"
        user = app.get("telegram_username") or app.get("telegram_display_name") or "user"
        created_at = _format_dt(app.get("created_at"))
        lines.append(
            f"{app.get('id')}) {app.get('player_name')} | {tag} | {user} | {created_at}"
        )
    await message.answer("\n".join(lines), parse_mode=None)


@router.message(Command("app"))
async def cmd_app(message: Message) -> None:
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer("Not allowed.", parse_mode=None)
        return
    if not message.text:
        await message.answer("Usage: /app <id>", parse_mode=None)
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Usage: /app <id>", parse_mode=None)
        return
    try:
        app_id = int(parts[1])
    except ValueError:
        await message.answer("Invalid application id.", parse_mode=None)
        return
    app = await get_application_by_id(app_id)
    if not app:
        await message.answer("Application not found.", parse_mode=None)
        return
    username = app.get("telegram_username")
    username_display = f"@{username}" if username else "n/a"
    lines = [
        f"Application {app_id}",
        f"Status: {app.get('status')}",
        f"Player: {app.get('player_name')}",
        f"Tag: {app.get('player_tag') or 'n/a'}",
        (
            "User: "
            f"{app.get('telegram_display_name') or 'user'} "
            f"({username_display}) id={app.get('telegram_user_id')}"
        ),
        f"Created: {_format_dt(app.get('created_at'))}",
        f"Updated: {_format_dt(app.get('updated_at'))}",
    ]
    await message.answer("\n".join(lines), parse_mode=None)


@router.message(Command("app_approve"))
async def cmd_app_approve(message: Message) -> None:
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer("Not allowed.", parse_mode=None)
        return
    if not message.text:
        await message.answer("Usage: /app_approve <id>", parse_mode=None)
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Usage: /app_approve <id>", parse_mode=None)
        return
    try:
        app_id = int(parts[1])
    except ValueError:
        await message.answer("Invalid application id.", parse_mode=None)
        return
    app = await get_application_by_id(app_id)
    if not app:
        await message.answer("Application not found.", parse_mode=None)
        return
    if app.get("status") == "approved":
        await message.answer("Application already approved.", parse_mode=None)
        return
    updated = await set_application_status(app_id, "approved")
    if not updated:
        await message.answer("Unable to approve application.", parse_mode=None)
        return
    await message.answer("Application approved.", parse_mode=None)
    try:
        await message.bot.send_message(
            app["telegram_user_id"],
            "✅ Your application was approved.",
            parse_mode=None,
        )
    except Exception as e:
        logger.warning("Failed to notify applicant: %s", e)
    await _notify_application(
        message.bot,
        f"Application {app_id} approved by admin {message.from_user.id}.",
    )


@router.message(Command("app_reject"))
async def cmd_app_reject(message: Message) -> None:
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer("Not allowed.", parse_mode=None)
        return
    if not message.text:
        await message.answer("Usage: /app_reject <id> [reason]", parse_mode=None)
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 2:
        await message.answer("Usage: /app_reject <id> [reason]", parse_mode=None)
        return
    try:
        app_id = int(parts[1])
    except ValueError:
        await message.answer("Invalid application id.", parse_mode=None)
        return
    reason = parts[2].strip() if len(parts) > 2 else ""
    app = await get_application_by_id(app_id)
    if not app:
        await message.answer("Application not found.", parse_mode=None)
        return
    if app.get("status") == "rejected":
        await message.answer("Application already rejected.", parse_mode=None)
        return
    updated = await set_application_status(app_id, "rejected")
    if not updated:
        await message.answer("Unable to reject application.", parse_mode=None)
        return
    await message.answer("Application rejected.", parse_mode=None)
    try:
        text = "Your application was rejected."
        if reason:
            text = f"{text}\nReason: {reason}"
        await message.bot.send_message(
            app["telegram_user_id"],
            text,
            parse_mode=None,
        )
    except Exception as e:
        logger.warning("Failed to notify applicant: %s", e)
    await _notify_application(
        message.bot,
        f"Application {app_id} rejected by admin {message.from_user.id}.",
    )


@router.message(Command("app_notify"))
async def cmd_app_notify(message: Message) -> None:
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer("Not allowed.", parse_mode=None)
        return
    if not message.text:
        await message.answer("Usage: /app_notify <id> [reason]", parse_mode=None)
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 2:
        await message.answer("Usage: /app_notify <id> [reason]", parse_mode=None)
        return
    try:
        app_id = int(parts[1])
    except ValueError:
        await message.answer("Invalid application id.", parse_mode=None)
        return
    reason = parts[2].strip() if len(parts) > 2 else ""

    app = await get_application_by_id(app_id)
    if not app:
        await message.answer("Application not found.", parse_mode=None)
        await log_mod_action(
            chat_id=message.chat.id,
            target_user_id=0,
            admin_user_id=message.from_user.id,
            action="app_notify_not_found",
            reason=f"id={app_id}",
        )
        await send_modlog(
            message.bot,
            f"[APP_NOTIFY] not found: id={app_id} admin={message.from_user.id}",
        )
        return
    if app.get("status") != "pending":
        await message.answer("Application is not pending.", parse_mode=None)
        await log_mod_action(
            chat_id=message.chat.id,
            target_user_id=int(app.get("telegram_user_id") or 0),
            admin_user_id=message.from_user.id,
            action="app_notify_not_pending",
            reason=f"status={app.get('status')}",
        )
        await send_modlog(
            message.bot,
            f"[APP_NOTIFY] not pending: id={app_id} status={app.get('status')} admin={message.from_user.id}",
        )
        return

    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer("CLAN_TAG is not configured.", parse_mode=None)
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
                f"Already notified recently. Try again in {hours}h {minutes}m.",
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
                f"[APP_NOTIFY] cooldown: id={app_id} user={app.get('telegram_user_id')} remaining={hours}h{minutes}m",
            )
            return

    telegram_user_id = app.get("telegram_user_id")
    if not telegram_user_id:
        await message.answer("No telegram_user_id on application.", parse_mode=None)
        await log_mod_action(
            chat_id=message.chat.id,
            target_user_id=0,
            admin_user_id=message.from_user.id,
            action="app_notify_failed",
            reason="missing telegram_user_id",
        )
        await send_modlog(
            message.bot,
            f"[APP_NOTIFY] failed: id={app_id} reason=no_user_id",
        )
        return

    text = (
        "Hi! A slot has opened in Black Poison.\n"
        f"You can join now using clan tag: {clan_tag}\n"
        "Please join as soon as possible."
    )
    try:
        await message.bot.send_message(
            int(telegram_user_id),
            text,
            parse_mode=None,
        )
    except Exception as e:
        logger.warning("Failed to notify applicant %s: %s", app_id, e)
        await message.answer(
            "⚠️ Failed to notify user (DM unavailable).",
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
            f"[APP_NOTIFY] failed: id={app_id} user={telegram_user_id} error={type(e).__name__}",
        )
        return

    invite_expires_at = now + timedelta(minutes=AUTO_INVITE_INVITE_MINUTES)
    await mark_application_invited(
        app_id, now=now, invite_expires_at=invite_expires_at
    )
    await log_mod_action(
        chat_id=message.chat.id,
        target_user_id=int(telegram_user_id),
        admin_user_id=message.from_user.id,
        action="app_notify",
        reason=reason or None,
    )
    await send_modlog(
        message.bot,
        f"[APP_NOTIFY] sent: id={app_id} user={telegram_user_id} "
        f"tag={app.get('player_tag') or 'n/a'} admin={message.from_user.id}",
    )
    await message.answer("✅ Applicant notified successfully.", parse_mode=None)


@router.message(Command("warn"))
async def cmd_warn(message: Message) -> None:
    if message.from_user is None:
        await message.answer("Unable to verify permissions.", parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Use this command in a group.", parse_mode=None)
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer("Reply to a user's message.", parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer("Not allowed.", parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer("Unable to verify admin status.", parse_mode=None)
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
        f"[MOD] warn: chat={message.chat.id} user={target.id} "
        f"count={warn_count} reason={reason or 'n/a'}",
    )
    if warn_count >= WARN_MUTE_AFTER:
        until = datetime.now(timezone.utc) + timedelta(minutes=WARN_MUTE_MINUTES)
        try:
            await message.bot.restrict_chat_member(
                message.chat.id,
                target.id,
                permissions=ChatPermissions(
                    can_send_messages=False,
                    can_send_media_messages=False,
                    can_send_other_messages=False,
                    can_add_web_page_previews=False,
                ),
                until_date=until,
            )
        except Exception as e:
            logger.warning("Failed to auto-mute user: %s", e, exc_info=True)
            await send_modlog(
                message.bot,
                f"[MOD] auto_mute failed: chat={message.chat.id} "
                f"user={target.id} err={e}",
            )
            await message.answer(
                f"Warning issued. Total warnings: {warn_count}.",
                parse_mode=None,
            )
            return

        await set_user_penalty(
            message.chat.id, target.id, "mute", until=until
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
            f"[MOD] auto_mute_warn_threshold: chat={message.chat.id} "
            f"user={target.id} until={until.isoformat()} warns={warn_count}",
        )
        if WARN_RESET_AFTER_MUTE:
            await reset_user_warnings(message.chat.id, target.id)
        await message.answer(
            f"Warning issued. Total warnings: {warn_count}. Auto-muted.",
            parse_mode=None,
        )
        return

    await message.answer(
        f"Warning issued. Total warnings: {warn_count}.", parse_mode=None
    )


@router.message(Command("warns"))
async def cmd_warns(message: Message) -> None:
    if message.from_user is None:
        await message.answer("Unable to verify permissions.", parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Use this command in a group.", parse_mode=None)
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer("Reply to a user's message.", parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer("Not allowed.", parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer("Unable to verify admin status.", parse_mode=None)
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
        await message.answer("No warnings found.", parse_mode=None)
        return
    lines = [f"Last {len(actions)} warnings for {target.full_name}:"]
    for action in actions:
        created_at = _format_dt(action.get("created_at"))
        reason = action.get("reason") or "n/a"
        lines.append(
            f"{action.get('action')} at {created_at} by {action.get('admin_user_id')} - {reason}"
        )
    await message.answer("\n".join(lines), parse_mode=None)


@router.message(Command("mute"))
async def cmd_mute(message: Message) -> None:
    if message.from_user is None:
        await message.answer("Unable to verify permissions.", parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Use this command in a group.", parse_mode=None)
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer("Reply to a user's message.", parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer("Not allowed.", parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer("Unable to verify admin status.", parse_mode=None)
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.answer("Usage: /mute <minutes> [reason]", parse_mode=None)
        return
    try:
        minutes = int(parts[1])
    except ValueError:
        await message.answer("Invalid minutes.", parse_mode=None)
        return
    if minutes <= 0:
        await message.answer("Minutes must be positive.", parse_mode=None)
        return
    reason = parts[2].strip() if len(parts) > 2 else ""
    target = message.reply_to_message.from_user

    until = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    try:
        await message.bot.restrict_chat_member(
            message.chat.id,
            target.id,
            permissions=ChatPermissions(
                can_send_messages=False,
                can_send_media_messages=False,
                can_send_other_messages=False,
                can_add_web_page_previews=False,
            ),
            until_date=until,
        )
    except Exception as e:
        logger.warning("Failed to mute user: %s", e, exc_info=True)
        await message.answer("Failed to mute user.", parse_mode=None)
        return

    await set_user_penalty(message.chat.id, target.id, "mute", until=until)
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
        f"[MOD] mute: chat={message.chat.id} user={target.id} "
        f"until={until.isoformat()} reason={reason or 'n/a'}",
    )
    await message.answer("User muted.", parse_mode=None)


@router.message(Command("unmute"))
async def cmd_unmute(message: Message) -> None:
    if message.from_user is None:
        await message.answer("Unable to verify permissions.", parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Use this command in a group.", parse_mode=None)
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer("Reply to a user's message.", parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer("Not allowed.", parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer("Unable to verify admin status.", parse_mode=None)
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
        await message.answer("Failed to unmute user.", parse_mode=None)
        return

    await clear_user_penalty(message.chat.id, target.id, "mute")
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
        f"[MOD] unmute: chat={message.chat.id} user={target.id}",
    )
    await message.answer("User unmuted.", parse_mode=None)


@router.message(Command("ban"))
async def cmd_ban(message: Message) -> None:
    if message.from_user is None:
        await message.answer("Unable to verify permissions.", parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Use this command in a group.", parse_mode=None)
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer("Reply to a user's message.", parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer("Not allowed.", parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer("Unable to verify admin status.", parse_mode=None)
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
        await message.answer("Failed to ban user.", parse_mode=None)
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
        f"[MOD] ban: chat={message.chat.id} user={target.id} "
        f"reason={reason or 'n/a'}",
    )
    await message.answer("User banned.", parse_mode=None)


@router.message(Command("unban"))
async def cmd_unban(message: Message) -> None:
    if message.from_user is None:
        await message.answer("Unable to verify permissions.", parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Use this command in a group.", parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer("Not allowed.", parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer("Unable to verify admin status.", parse_mode=None)
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Usage: /unban <user_id>", parse_mode=None)
        return
    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer("Invalid user id.", parse_mode=None)
        return

    try:
        await message.bot.unban_chat_member(message.chat.id, user_id)
    except Exception as e:
        logger.warning("Failed to unban user: %s", e, exc_info=True)
        await message.answer("Failed to unban user.", parse_mode=None)
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
        f"[MOD] unban: chat={message.chat.id} user={user_id}",
    )
    await message.answer("User unbanned.", parse_mode=None)


@router.message(Command("purge"))
async def cmd_purge(message: Message) -> None:
    if message.from_user is None:
        await message.answer("Unable to verify permissions.", parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Use this command in a group.", parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer("Not allowed.", parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer("Unable to verify admin status.", parse_mode=None)
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Usage: /purge <N>", parse_mode=None)
        return
    try:
        count = int(parts[1])
    except ValueError:
        await message.answer("Invalid number.", parse_mode=None)
        return
    if count < 1:
        await message.answer("Number must be positive.", parse_mode=None)
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
        f"[MOD] purge: chat={message.chat.id} "
        f"admin={message.from_user.id} deleted={deleted}",
    )
    await message.answer(f"Purged {deleted} messages.", parse_mode=None)


@router.message(Command("raid_on"))
async def cmd_raid_on(message: Message) -> None:
    if message.from_user is None:
        await message.answer("Unable to verify permissions.", parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Use this command in a group.", parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer("Not allowed.", parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer("Unable to verify admin status.", parse_mode=None)
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
        f"[MOD] raid_on: chat={message.chat.id} admin={message.from_user.id}",
    )
    await message.answer("Raid mode enabled.", parse_mode=None)


@router.message(Command("raid_off"))
async def cmd_raid_off(message: Message) -> None:
    if message.from_user is None:
        await message.answer("Unable to verify permissions.", parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Use this command in a group.", parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer("Not allowed.", parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer("Unable to verify admin status.", parse_mode=None)
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
        f"[MOD] raid_off: chat={message.chat.id} admin={message.from_user.id}",
    )
    await message.answer("Raid mode disabled.", parse_mode=None)


@router.message(Command("raid_status"))
async def cmd_raid_status(message: Message) -> None:
    if message.from_user is None:
        await message.answer("Unable to verify permissions.", parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Use this command in a group.", parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer("Not allowed.", parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer("Unable to verify admin status.", parse_mode=None)
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
        "Raid status",
        f"raid_mode: {settings.get('raid_mode')}",
        f"flood_window_seconds: {settings.get('flood_window_seconds')}",
        f"flood_max_messages: {settings.get('flood_max_messages')}",
        f"flood_mute_minutes: {settings.get('flood_mute_minutes')}",
        f"new_user_link_block_hours: {settings.get('new_user_link_block_hours')}",
    ]
    await message.answer("\n".join(lines), parse_mode=None)


@router.message(Command("mod_debug_on"))
async def cmd_mod_debug_on(message: Message) -> None:
    if message.from_user is None:
        await message.answer("Unable to verify permissions.", parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Use this command in a group.", parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer("Not allowed.", parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer("Unable to verify admin status.", parse_mode=None)
        return

    await set_app_state(
        _mod_debug_state_key(message.chat.id),
        {"enabled": True, "updated_at": datetime.now(timezone.utc).isoformat()},
    )
    await message.answer("Mod debug enabled for this chat.", parse_mode=None)


@router.message(Command("mod_debug_off"))
async def cmd_mod_debug_off(message: Message) -> None:
    if message.from_user is None:
        await message.answer("Unable to verify permissions.", parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Use this command in a group.", parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer("Not allowed.", parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer("Unable to verify admin status.", parse_mode=None)
        return

    await delete_app_state(_mod_debug_state_key(message.chat.id))
    await message.answer("Mod debug disabled for this chat.", parse_mode=None)


@router.message(Command("modlog"))
async def cmd_modlog(message: Message) -> None:
    if message.from_user is None:
        await message.answer("Unable to verify permissions.", parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Use this command in a group.", parse_mode=None)
        return
    try:
        if not await _is_admin_user(message, message.from_user.id):
            await message.answer("Not allowed.", parse_mode=None)
            return
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer("Unable to verify admin status.", parse_mode=None)
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
        await message.answer("No moderation actions found.", parse_mode=None)
        return
    lines = [f"Last {len(actions)} moderation actions:"]
    for entry in actions:
        created_at = _format_dt(entry.get("created_at"))
        lines.append(
            f"{entry.get('id')}) {entry.get('action')} "
            f"user={entry.get('target_user_id')} "
            f"admin={entry.get('admin_user_id')} "
            f"at {created_at}"
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
    if event.new_chat_member.status in (
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.CREATOR,
    ):
        logger.info(
            "CAPTCHA skip: reason=user_is_admin chat_id=%s user_id=%s",
            event.chat.id,
            user.id,
        )
        return
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
            f"[CAPTCHA] ERROR: restrict failed: chat={event.chat.id} "
            f"user={user.id} err={e}",
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
            "Too many attempts, try again later.",
            parse_mode=None,
        )
        username = f"@{user.username}" if user.username else "n/a"
        await send_modlog(
            event.bot,
            "[CAPTCHA] join gate: "
            f"chat={event.chat.id} user={user.id} {username} "
            f"name={user.full_name} status_old={old_status} "
            f"status_new={new_status} -> restricted, "
            f"captcha_failed challenge={challenge.get('id')}",
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
        username = f"@{user.username}" if user.username else "n/a"
        await send_modlog(
            event.bot,
            "[CAPTCHA] join gate: "
            f"chat={event.chat.id} user={user.id} {username} "
            f"name={user.full_name} status_old={old_status} "
            f"status_new={new_status} -> restricted, "
            f"captcha_sent challenge={challenge['id']} msg={message_id}",
        )
    else:
        username = f"@{user.username}" if user.username else "n/a"
        await send_modlog(
            event.bot,
            "[CAPTCHA] join gate: "
            f"chat={event.chat.id} user={user.id} {username} "
            f"name={user.full_name} status_old={old_status} "
            f"status_new={new_status} -> restricted, "
            f"captcha_sent=no challenge={challenge['id']}",
        )


@moderation_router.message(
    F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP})
)
async def handle_pending_user_message(message: Message) -> None:
    if not ENABLE_CAPTCHA:
        return
    if message.from_user is None or message.from_user.is_bot:
        return
    if is_bot_command_message(message):
        if await _is_mod_debug(message.chat.id):
            logger.warning(
                "[MOD] bypass command %s from user=%s chat=%s",
                _extract_command_name(message),
                message.from_user.id,
                message.chat.id,
            )
        return
    challenge = await get_pending_challenge(message.chat.id, message.from_user.id)
    if not challenge:
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
            f"[CAPTCHA] ERROR: delete failed: chat={message.chat.id} "
            f"user={message.from_user.id} msg_id={message.message_id} err={e}",
        )

    now = datetime.now(timezone.utc)
    last_reminded_at = challenge.get("last_reminded_at")
    if isinstance(last_reminded_at, datetime):
        if (now - last_reminded_at).total_seconds() < CAPTCHA_REMIND_COOLDOWN_SECONDS:
            return

    reminder_text = (
        f"{message.from_user.full_name}, please solve the captcha to chat."
    )
    await message.bot.send_message(
        message.chat.id, reminder_text, parse_mode=None
    )
    await touch_last_reminded_at(challenge["id"], now)
    await send_modlog(
        message.bot,
        f"[CAPTCHA] pending msg deleted: chat={message.chat.id} "
        f"user={message.from_user.id} msg_id={message.message_id}",
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
    F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP})
)
async def handle_moderation_message(message: Message) -> None:
    logger.warning(
        "[MOD] HIT chat=%s type=%s msg_id=%s from=%s text=%r entities=%s",
        message.chat.id,
        message.chat.type,
        message.message_id,
        message.from_user.id if message.from_user else None,
        (message.text or message.caption or "")[:200],
        [entity.type for entity in (message.entities or [])],
    )
    if is_bot_command_message(message):
        if await _is_mod_debug(message.chat.id):
            logger.warning(
                "[MOD] bypass command %s from user=%s chat=%s",
                _extract_command_name(message),
                message.from_user.id if message.from_user else None,
                message.chat.id,
            )
        return
    if not MODERATION_ENABLED:
        logger.warning(
            "[MOD] skip: disabled chat=%s msg_id=%s",
            message.chat.id,
            message.message_id,
        )
        return
    if message.from_user is None or message.from_user.is_bot:
        logger.warning(
            "[MOD] skip: no_user_or_bot chat=%s msg_id=%s",
            message.chat.id,
            message.message_id,
        )
        return
    mod_debug = await _is_mod_debug(message.chat.id)
    if ENABLE_CAPTCHA:
        pending = await get_pending_challenge(message.chat.id, message.from_user.id)
        if pending:
            logger.warning(
                "[MOD] skip: pending_captcha chat=%s user=%s",
                message.chat.id,
                message.from_user.id,
            )
            return
    try:
        if await _is_admin_user(message, message.from_user.id):
            logger.warning(
                "[MOD] skip: admin chat=%s user=%s",
                message.chat.id,
                message.from_user.id,
            )
            return
    except Exception:
        logger.warning(
            "[MOD] skip: admin_check_failed chat=%s user=%s",
            message.chat.id,
            message.from_user.id,
        )
        pass

    now = datetime.now(timezone.utc)
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
        logger.warning("[MOD] skip: no_settings chat=%s", message.chat.id)
        return
    raid_mode = bool(settings.get("raid_mode"))
    flood_window = int(settings.get("flood_window_seconds", FLOOD_WINDOW_SECONDS))
    flood_max = int(settings.get("flood_max_messages", FLOOD_MAX_MESSAGES))
    flood_mute = int(settings.get("flood_mute_minutes", FLOOD_MUTE_MINUTES))
    link_block_hours = int(
        settings.get("new_user_link_block_hours", NEW_USER_LINK_BLOCK_HOURS)
    )
    if mod_debug:
        logger.warning(
            "[MOD] settings chat=%s raid=%s flood_window=%s flood_max=%s flood_mute=%s link_block_hours=%s",
            message.chat.id,
            raid_mode,
            flood_window,
            flood_max,
            flood_mute,
            link_block_hours,
        )
    if mod_debug and not (message.text or message.caption):
        logger.warning(
            "[MOD] note: no_text_or_caption chat=%s msg_id=%s",
            message.chat.id,
            message.message_id,
        )

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
        if block_links:
            await _delete_message_safe(message)
            warn_count = await increment_user_warning(
                message.chat.id, message.from_user.id, now=now
            )
            logger.warning(
                "[MOD] link_block: chat=%s user=%s warnings=%s verified=%s recent=%s raid=%s",
                message.chat.id,
                message.from_user.id,
                warn_count,
                verified,
                recent,
                raid_mode,
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
                f"[MOD] link blocked: chat={message.chat.id} "
                f"user={message.from_user.id} warnings={warn_count}",
            )
            if warn_count >= 3:
                await _mute_user(
                    message,
                    message.from_user.id,
                    minutes=flood_mute,
                    reason="link warnings",
                )
            return

    if raid_mode:
        flood_max = RAID_FLOOD_MAX_MESSAGES

    count = await record_rate_counter(
        message.chat.id,
        message.from_user.id,
        window_seconds=flood_window,
        now=now,
    )
    if count > flood_max:
        await _delete_message_safe(message)
        logger.warning(
            "[MOD] flood_detected: chat=%s user=%s count=%s limit=%s raid=%s",
            message.chat.id,
            message.from_user.id,
            count,
            flood_max,
            raid_mode,
        )
        warn_info = await get_warning_info(
            message.chat.id, message.from_user.id
        )
        recent_warn = False
        if warn_info and isinstance(warn_info.get("last_warned_at"), datetime):
            last_warned = warn_info["last_warned_at"]
            if last_warned.tzinfo is None:
                last_warned = last_warned.replace(tzinfo=timezone.utc)
            recent_warn = now - last_warned < timedelta(hours=1)
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
            f"[MOD] flood: chat={message.chat.id} "
            f"user={message.from_user.id} count={count}",
        )
        if recent_warn and warn_count >= 2:
            logger.warning(
                "[MOD] auto_mute: chat=%s user=%s warn_count=%s",
                message.chat.id,
                message.from_user.id,
                warn_count,
            )
            await _mute_user(
                message,
                message.from_user.id,
                minutes=flood_mute,
                reason="flood repeat",
            )


@moderation_router.callback_query(F.data.startswith("cap:"))
async def handle_captcha_callback(query: CallbackQuery) -> None:
    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 3:
        await query.answer("Invalid captcha.", show_alert=False)
        return
    try:
        challenge_id = int(parts[1])
        choice = int(parts[2])
    except ValueError:
        await query.answer("Invalid captcha.", show_alert=False)
        return

    challenge = await get_challenge_by_id(challenge_id)
    if not challenge:
        await query.answer("Captcha not found.", show_alert=False)
        return
    if query.from_user is None:
        await query.answer("Not allowed.", show_alert=False)
        return
    if challenge["user_id"] != query.from_user.id:
        await query.answer("Not for you.", show_alert=False)
        return

    now = datetime.now(timezone.utc)
    expires_at = challenge.get("expires_at")
    if isinstance(expires_at, datetime) and expires_at < now:
        await mark_challenge_expired(challenge_id)
        await query.answer("Captcha expired. Please try again.", show_alert=False)
        return

    if challenge["status"] != "pending":
        await query.answer("Captcha not active.", show_alert=False)
        return

    question = await get_captcha_question(challenge["question_id"])
    if not question:
        await query.answer("Captcha missing.", show_alert=False)
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
                f"[CAPTCHA] ERROR: unrestrict failed: chat={challenge['chat_id']} "
                f"user={challenge['user_id']} err={e}",
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
        await query.answer("✅ Verified", show_alert=False)
        logger.info(
            "Captcha passed for user %s in chat %s",
            challenge["user_id"],
            challenge["chat_id"],
        )
        await send_modlog(
            query.bot,
            f"[CAPTCHA] passed: chat={challenge['chat_id']} "
            f"user={challenge['user_id']} challenge={challenge_id} -> unrestrict ok",
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
                "Неправильно. Другая капча 👇", parse_mode=None
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
        await query.answer("Неправильно.", show_alert=False)
        logger.info(
            "Captcha failed for user %s in chat %s (new challenge created)",
            challenge["user_id"],
            challenge["chat_id"],
        )
        await send_modlog(
            query.bot,
            f"[CAPTCHA] failed: chat={challenge['chat_id']} "
            f"user={challenge['user_id']} challenge={challenge_id} "
            f"expires_at={_format_dt(now + timedelta(seconds=30))}",
        )
        return

    await query.answer("Wrong answer. Try again.", show_alert=False)
    await send_modlog(
        query.bot,
        f"[CAPTCHA] wrong: chat={challenge['chat_id']} "
        f"user={challenge['user_id']} challenge={challenge_id} attempts={attempts}",
    )

async def _send_debug_reminder(
    message: Message,
    *,
    war_type: str,
    day: int,
    banner_url: str,
    banner_url_day4: str,
    templates: dict[int, str],
) -> None:
    caption = templates.get(day)
    if not caption:
        await message.answer("Unable to build reminder message.", parse_mode=None)
        return
    await message.answer(
        f"Debug: sending {war_type} Day {day} to this chat only.",
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
    start_time = datetime.now(timezone.utc)

    api_status = "✅ Connected"
    clan_name = "Unknown"

    try:
        api_client = await get_api_client()
        clan_tag = _require_clan_tag()
        if not clan_tag:
            raise ValueError("CLAN_TAG is not configured")
        clan_data = await api_client.get_clan(clan_tag)
        if isinstance(clan_data, dict):
            clan_name = clan_data.get("name", "Unknown")
    except ClashRoyaleAPIError as e:
        api_status = f"⚠️ Error: {e.message}"
        logger.warning("API check failed: %s", e)
    except Exception as e:
        api_status = f"⚠️ Error: {e}"
        logger.error("Unexpected error during API check: %s", e)

    response_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

    response_text = (
        "Pong!\n\n"
        f"Response time: {response_time:.0f}ms\n"
        f"Clash Royale API: {api_status}\n"
        f"Monitoring clan: {clan_name}\n"
        f"Server time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC"
    )

    await message.answer(response_text, parse_mode=None)


@router.message(Command("bind"))
async def cmd_bind(message: Message) -> None:
    """Bind the current group chat for weekly war reports."""
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer(
            "This command can only be used in a group chat.", parse_mode=None
        )
        return
    if message.from_user is None:
        await message.answer("Unable to verify permissions for this user.", parse_mode=None)
        return
    try:
        member = await message.bot.get_chat_member(
            chat_id=message.chat.id, user_id=message.from_user.id
        )
    except Exception as e:
        logger.error("Failed to verify chat member: %s", e, exc_info=True)
        await message.answer("Unable to verify permissions right now.", parse_mode=None)
        return
    if member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR):
        await message.answer("Only chat admins can bind this group.", parse_mode=None)
        return
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer("CLAN_TAG is not configured.", parse_mode=None)
        return
    await upsert_clan_chat(clan_tag, message.chat.id, enabled=True)
    await message.answer("Chat bound for weekly war reports.", parse_mode=None)


@router.message(Command("war"))
async def cmd_war(message: Message) -> None:
    """Show the weekly war report for the last completed week."""
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer("CLAN_TAG is not configured.", parse_mode=None)
        return
    week = await get_last_completed_week(clan_tag)
    if not week:
        await message.answer("No completed war weeks found yet.", parse_mode=None)
        return
    season_id, section_index = week
    report = await build_weekly_report(season_id, section_index, clan_tag)
    await message.answer(report, parse_mode=None)


@router.message(Command("war8"))
async def cmd_war8(message: Message) -> None:
    """Show the rolling war report for the last 8 completed weeks."""
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer("CLAN_TAG is not configured.", parse_mode=None)
        return
    weeks = await get_last_completed_weeks(8, clan_tag)
    if not weeks:
        await message.answer("No completed war weeks found yet.", parse_mode=None)
        return
    report = await build_rolling_report(weeks, clan_tag)
    await message.answer(report, parse_mode=None)


@router.message(Command("war_all"))
async def cmd_war_all(message: Message) -> None:
    """Send weekly, rolling, and kick shortlist reports together."""
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer("CLAN_TAG is not configured.", parse_mode=None)
        return
    last_week = await get_last_completed_week(clan_tag)
    if not last_week:
        await message.answer("No completed war weeks found yet.", parse_mode=None)
        return
    weeks = await get_last_completed_weeks(8, clan_tag)
    if not weeks:
        weeks = [last_week]

    weekly_report = await build_weekly_report(last_week[0], last_week[1], clan_tag)
    rolling_report = await build_rolling_report(weeks, clan_tag)
    kick_report = await build_kick_shortlist_report(weeks, last_week, clan_tag)

    await message.answer(weekly_report, parse_mode=None)
    await message.answer(rolling_report, parse_mode=None)
    await message.answer(kick_report, parse_mode=None)


@router.message(Command("list_for_kick"))
async def cmd_list_for_kick(message: Message) -> None:
    """Show kick shortlist based on the last 8 completed weeks."""
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer("CLAN_TAG is not configured.", parse_mode=None)
        return
    weeks = await get_last_completed_weeks(8, clan_tag)
    last_week = await get_last_completed_week(clan_tag)
    if not weeks or not last_week:
        await message.answer("No completed war weeks found yet.", parse_mode=None)
        return
    report = await build_kick_shortlist_report(weeks, last_week, clan_tag)
    await message.answer(report, parse_mode=None)


@router.message(Command("inactive"))
async def cmd_inactive(message: Message) -> None:
    """Handle /inactive command - Show players with low River Race participation."""
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer("CLAN_TAG is not configured.", parse_mode=None)
        return
    try:
        current_members = await get_current_member_tags(clan_tag)
        if not current_members:
            await message.answer(
                "No clan membership snapshot available yet.\n"
                "Please wait for the next update cycle.",
                parse_mode=None,
            )
            return

        absent_members = await get_top_absent_members(
            clan_tag, INACTIVE_LAST_SEEN_LIMIT
        )
        if not absent_members:
            await message.answer(
                "No member activity data available yet.\n"
                "Please wait for the next update cycle.",
                parse_mode=None,
            )
            return

        response_lines = ["😴 INACTIVITY — last seen in-game", ""]
        for index, member in enumerate(absent_members, 1):
            name = member.get("player_name") or "Unknown"
            days_absent = member.get("days_absent")
            if days_absent is None:
                days_text = "n/a"
                flag = ""
            else:
                days_text = f"{days_absent}d ago"
                if days_absent >= LAST_SEEN_RED_DAYS:
                    flag = "🔴"
                elif days_absent >= LAST_SEEN_YELLOW_DAYS:
                    flag = "🟡"
                else:
                    flag = ""
            prefix = f"{flag} " if flag else ""
            response_lines.append(
                f"{index}) {prefix}{name} — last seen {days_text}"
            )

        await message.answer("\n".join(response_lines), parse_mode=None)
    except Exception as e:
        logger.error("Error in /inactive command: %s", e, exc_info=True)
        await message.answer(
            "An error occurred while fetching inactive players.\n"
            "Please try again later.",
            parse_mode=None,
        )


@router.message(Command("current_war"))
async def cmd_current_war(message: Message) -> None:
    """Show current war snapshot from the database."""
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer("CLAN_TAG is not configured.", parse_mode=None)
        return
    report = await build_current_war_report(clan_tag)
    await message.answer(report, parse_mode=None)


@router.message(Command("info"))
async def cmd_info(message: Message) -> None:
    """Show clan info from the official Clash Royale API."""
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer("CLAN_TAG is not configured.", parse_mode=None)
        return
    report = await build_clan_info_report(clan_tag)
    await message.answer(report, parse_mode=None)


@router.message(Command("riverside"))
async def cmd_riverside(message: Message) -> None:
    """Debug: send Clan War reminder to this chat only."""
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer("Not allowed.", parse_mode=None)
        return
    day = _parse_debug_day(message.text)
    templates = {
        1: "🏁 Clan War has begun!\nDay 1 is live.\n⚔️ Use your attacks and bring fame to the clan.",
        2: "⏳ Clan War – Day 2\nNew war day is open.\n💪 Don’t forget to play your battles.",
        3: "🔥 Clan War – Day 3\nWe’re close to the finish.\n⚔️ Every attack matters.",
        4: "🚨 Final Day of Clan War!\n⚔️ Finish your attacks today.\n📊 Results and activity report after war ends.",
    }
    await _send_debug_reminder(
        message,
        war_type="Riverside",
        day=day,
        banner_url="https://i.ibb.co/VyGjscj/image.png",
        banner_url_day4="https://i.ibb.co/0jvgVSgq/image-1.jpg",
        templates=templates,
    )


@router.message(Command("coliseum"))
async def cmd_coliseum(message: Message) -> None:
    """Debug: send Colosseum reminder to this chat only."""
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer("Not allowed.", parse_mode=None)
        return
    day = _parse_debug_day(message.text)
    templates = {
        1: "🏛 COLISEUM WAR HAS STARTED\nDay 1 is live.\n❗ Participation is mandatory.\n⚔️ Play your attacks.",
        2: "🏛 Coliseum – Day 2\n⚔️ All attacks matter.\n❗ Participation is mandatory.",
        3: "🏛 Coliseum – Day 3\n🔥 Stay active.\n❗ Participation is mandatory.",
        4: "🚨 FINAL DAY – COLISEUM\n⚔️ Finish your attacks today.\n📊 Inactive players will be reviewed after war.",
    }
    await _send_debug_reminder(
        message,
        war_type="Coliseum",
        day=day,
        banner_url="https://i.ibb.co/Cs4Sjpzw/image.png",
        banner_url_day4="https://i.ibb.co/R4YLyPzR/image.jpg",
        templates=templates,
    )


@router.message(Command("captcha_send"))
async def cmd_captcha_send(message: Message) -> None:
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer("Not allowed.", parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Reply to a user's message in a group.", parse_mode=None)
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer("Reply to a user's message.", parse_mode=None)
        return

    target = message.reply_to_message.from_user
    if target.is_bot:
        await message.answer("Cannot send captcha to a bot.", parse_mode=None)
        return
    if _is_debug_admin(target.id):
        await message.answer("Cannot send captcha to an admin.", parse_mode=None)
        return

    try:
        member = await message.bot.get_chat_member(message.chat.id, target.id)
        if member.status in (
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.CREATOR,
        ):
            await message.answer(
                "Cannot send captcha to a chat admin.", parse_mode=None
            )
            return
    except Exception:
        pass

    chat_id = message.chat.id
    await send_modlog(
        message.bot,
        f"[ADMIN] captcha_send by {message.from_user.id} -> target {target.id} chat={chat_id}",
    )
    try:
        if await is_user_verified(chat_id, target.id):
            await message.answer(
                "User is already verified. Use /captcha_unverify first.",
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
            f"[CAPTCHA] ERROR: restrict failed: chat={chat_id} "
            f"user={target.id} err={e}",
        )

    try:
        challenge, question = await get_or_create_pending_challenge(
            chat_id, target.id, CAPTCHA_EXPIRE_MINUTES
        )
    except Exception as e:
        logger.error("Failed to create captcha challenge: %s", e, exc_info=True)
        await message.answer("Unable to create a captcha.", parse_mode=None)
        return

    if not challenge:
        await message.answer("Unable to create a captcha.", parse_mode=None)
        return
    if not question:
        question = await get_captcha_question(challenge["question_id"])
    if not question:
        await message.answer("Captcha question unavailable.", parse_mode=None)
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
            f"Captcha sent to {_format_user_label(target)} "
            f"(challenge_id={challenge['id']}, msg_id={message_id}).",
            parse_mode=None,
        )
        return

    await message.answer(
        "Captcha created, but failed to send message.", parse_mode=None
    )


@router.message(Command("captcha_status"))
async def cmd_captcha_status(message: Message) -> None:
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer("Not allowed.", parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Reply to a user's message in a group.", parse_mode=None)
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer("Reply to a user's message.", parse_mode=None)
        return
    target = message.reply_to_message.from_user
    chat_id = message.chat.id
    await send_modlog(
        message.bot,
        f"[ADMIN] captcha_status by {message.from_user.id} -> target {target.id} chat={chat_id}",
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
            question_text = "n/a"
    except Exception as e:
        logger.error("Failed to fetch captcha status: %s", e, exc_info=True)
        await message.answer("Unable to fetch captcha status right now.", parse_mode=None)
        return

    lines = [
        "Captcha status",
        f"Chat: {chat_id}",
        f"User: {_format_user_label(target)} | id={target.id}",
        f"Verified: {'yes' if is_verified else 'no'}",
    ]
    if not challenge:
        lines.append("Challenge: none")
        await message.answer("\n".join(lines), parse_mode=None)
        return

    lines.extend(
        [
            (
                "Challenge: "
                f"id={challenge.get('id')} "
                f"status={challenge.get('status')} "
                f"attempts={challenge.get('attempts')} "
                f"created_at={_format_dt(challenge.get('created_at'))}"
            ),
            (
                "Details: "
                f"expires_at={_format_dt(challenge.get('expires_at'))} "
                f"message_id={challenge.get('message_id') or 'n/a'} "
                f"last_reminded_at={_format_dt(challenge.get('last_reminded_at'))} "
                f"question_id={challenge.get('question_id')}"
            ),
            f"Question: {question_text}",
        ]
    )
    await message.answer("\n".join(lines), parse_mode=None)


@router.message(Command("captcha_reset"))
async def cmd_captcha_reset(message: Message) -> None:
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer("Not allowed.", parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Reply to a user's message in a group.", parse_mode=None)
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer("Reply to a user's message.", parse_mode=None)
        return
    target = message.reply_to_message.from_user
    chat_id = message.chat.id
    await send_modlog(
        message.bot,
        f"[ADMIN] captcha_reset by {message.from_user.id} -> target {target.id} chat={chat_id}",
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
            f"[CAPTCHA] ERROR: restrict failed: chat={chat_id} "
            f"user={target.id} err={e}",
        )

    try:
        challenge, question = await get_or_create_pending_challenge(
            chat_id, target.id, CAPTCHA_EXPIRE_MINUTES
        )
    except Exception as e:
        logger.error("Failed to create captcha challenge: %s", e, exc_info=True)
        await message.answer("Unable to create a new captcha.", parse_mode=None)
        return

    if not challenge:
        await message.answer("Unable to create a new captcha.", parse_mode=None)
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
            "Captcha reset done. New captcha sent.", parse_mode=None
        )
        return

    await message.answer(
        "Captcha reset done, but failed to send captcha.", parse_mode=None
    )


@router.message(Command("captcha_verify"))
async def cmd_captcha_verify(message: Message) -> None:
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer("Not allowed.", parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Reply to a user's message in a group.", parse_mode=None)
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer("Reply to a user's message.", parse_mode=None)
        return
    target = message.reply_to_message.from_user
    chat_id = message.chat.id
    await send_modlog(
        message.bot,
        f"[ADMIN] captcha_verify by {message.from_user.id} -> target {target.id} chat={chat_id}",
    )

    try:
        await set_user_verified(chat_id, target.id)
        await mark_pending_challenges_passed(chat_id, target.id)
    except Exception as e:
        logger.error("Failed to mark user verified: %s", e, exc_info=True)
        await message.answer("Unable to verify user right now.", parse_mode=None)
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
            f"[CAPTCHA] ERROR: unrestrict failed: chat={chat_id} "
            f"user={target.id} err={e}",
        )

    await message.answer("User verified by admin.", parse_mode=None)


@router.message(Command("captcha_unverify"))
async def cmd_captcha_unverify(message: Message) -> None:
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer("Not allowed.", parse_mode=None)
        return
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Reply to a user's message in a group.", parse_mode=None)
        return
    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer("Reply to a user's message.", parse_mode=None)
        return
    target = message.reply_to_message.from_user
    chat_id = message.chat.id
    await send_modlog(
        message.bot,
        f"[ADMIN] captcha_unverify by {message.from_user.id} -> target {target.id} chat={chat_id}",
    )

    try:
        await delete_verified_user(chat_id, target.id)
    except Exception as e:
        logger.error("Failed to remove verified flag: %s", e, exc_info=True)
        await message.answer(
            "Unable to remove verified flag right now.", parse_mode=None
        )
        return

    await message.answer("Verified flag removed.", parse_mode=None)


@router.message(Command("modlog_test"))
async def cmd_modlog_test(message: Message) -> None:
    if message.from_user is None or not _is_debug_admin(message.from_user.id):
        await message.answer("Not allowed.", parse_mode=None)
        return
    if MODLOG_CHAT_ID == 0:
        await message.answer("MODLOG_CHAT_ID is not set.", parse_mode=None)
        return
    try:
        await message.bot.send_message(
            MODLOG_CHAT_ID,
            "MODLOG TEST ok",
            parse_mode=None,
            disable_web_page_preview=True,
        )
    except Exception as e:
        await message.answer(
            f"failed: {type(e).__name__}: {e}", parse_mode=None
        )
        return

    await message.answer("sent ok", parse_mode=None)


@router.message(Command("donations"))
async def cmd_donations(message: Message) -> None:
    """Show donation leaderboards for the clan."""
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer("CLAN_TAG is not configured.", parse_mode=None)
        return
    clan_name = "Unknown"
    try:
        api_client = await get_api_client()
        clan_data = await api_client.get_clan(clan_tag)
        if isinstance(clan_data, dict):
            clan_name = clan_data.get("name") or clan_name
    except ClashRoyaleAPIError as e:
        logger.warning("Failed to fetch clan name: %s", e)
    except Exception as e:
        logger.warning("Failed to fetch clan name: %s", e)
    report = await build_donations_report(clan_tag, clan_name)
    await message.answer(report, parse_mode=None)


@router.message(Command("promote_candidates"))
async def cmd_promote_candidates(message: Message) -> None:
    """Show promotion recommendations."""
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer("CLAN_TAG is not configured.", parse_mode=None)
        return
    report = await build_promotion_candidates_report(clan_tag)
    await message.answer(report, parse_mode=None)


@router.message(Command("my_activity"))
async def cmd_my_activity(message: Message) -> None:
    """Show the current user's war activity report."""
    if message.from_user is None:
        await message.answer("Unable to identify your account.", parse_mode=None)
        return

    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer("CLAN_TAG is not configured.", parse_mode=None)
        return

    args = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            args = parts[1].strip()

    existing = await get_user_link(message.from_user.id)
    if existing:
        await message.answer(
            f"✅ You are linked to: {existing['player_name']} ({existing['player_tag']})",
            parse_mode=None,
        )
        report = await build_my_activity_report(
            existing["player_tag"], existing["player_name"], clan_tag
        )
        await message.answer(report, parse_mode=None)
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
                "You are not linked yet. Send your in-game nickname exactly as it appears.",
                parse_mode=None,
            )
        return

    await _send_link_button(message)


@router.message(Command("activity"))
async def cmd_activity(message: Message) -> None:
    """Show a player's activity report by nickname, @username, or reply."""
    clan_tag = _require_clan_tag()
    if not clan_tag:
        await message.answer("CLAN_TAG is not configured.", parse_mode=None)
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
                "Unable to resolve that Telegram username.",
                parse_mode=None,
            )
            return
        link = await get_user_link(chat.id)
        if not link:
            await message.answer(
                "That user is not linked. Ask them to use /my_activity to link.",
                parse_mode=None,
            )
            return
        report = await build_my_activity_report(
            link["player_tag"], link["player_name"], clan_tag
        )
        await message.answer(report, parse_mode=None)
        return

    if not args and message.reply_to_message and message.reply_to_message.from_user:
        target_id = message.reply_to_message.from_user.id
        link = await get_user_link(target_id)
        if not link:
            await message.answer(
                "That user is not linked. Ask them to use /my_activity to link.",
                parse_mode=None,
            )
            return
        report = await build_my_activity_report(
            link["player_tag"], link["player_name"], clan_tag
        )
        await message.answer(report, parse_mode=None)
        return

    if not args:
        await message.answer(
            "Usage: /activity <in-game nickname> or /activity @username (or reply to a user).",
            parse_mode=None,
        )
        return

    candidates = await search_player_candidates(clan_tag, args)
    if not candidates:
        await message.answer(
            "No player found with that nickname in clan data. Please check spelling.",
            parse_mode=None,
        )
        return

    if len(candidates) > 1:
        lines = ["Multiple matches found. Please be more specific:"]
        for index, candidate in enumerate(candidates, 1):
            tag = _normalize_tag(candidate["player_tag"])
            status = "IN CLAN" if candidate.get("in_clan") else "NOT IN CLAN"
            lines.append(f"{index}) {candidate['player_name']} — {tag} — {status}")
        await message.answer("\n".join(lines), parse_mode=None)
        return

    candidate = candidates[0]
    report = await build_my_activity_report(
        _normalize_tag(candidate["player_tag"]),
        candidate["player_name"],
        clan_tag,
    )
    await message.answer(report, parse_mode=None)


@router.message(Command("admin_link_name"))
async def cmd_admin_link_name(message: Message) -> None:
    """Link a user account by nickname (admin-only, reply required)."""
    if message.from_user is None:
        await message.answer("Unable to verify permissions.", parse_mode=None)
        return

    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer(
            "Reply to a user's message with /admin_link_name <nickname>.",
            parse_mode=None,
        )
        return

    try:
        is_admin = await _is_admin_user(message, message.from_user.id)
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer("Unable to verify admin status.", parse_mode=None)
        return

    if not is_admin:
        await message.answer("You do not have permission to use this command.", parse_mode=None)
        return

    args = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            args = parts[1].strip()

    if not args:
        await message.answer("Provide a nickname to link.", parse_mode=None)
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
    if message.from_user is None:
        await message.answer("Unable to verify permissions.", parse_mode=None)
        return

    if message.reply_to_message is None or message.reply_to_message.from_user is None:
        await message.answer(
            "Reply to a user's message with /unlink.",
            parse_mode=None,
        )
        return

    try:
        is_admin = await _is_admin_user(message, message.from_user.id)
    except Exception as e:
        logger.error("Failed to check admin status: %s", e, exc_info=True)
        await message.answer("Unable to verify admin status.", parse_mode=None)
        return

    if not is_admin:
        await message.answer(
            "You do not have permission to use this command.",
            parse_mode=None,
        )
        return

    target_id = message.reply_to_message.from_user.id
    existing = await get_user_link(target_id)
    if not existing:
        await message.answer("No linked account found for this user.", parse_mode=None)
        return

    await delete_user_link(target_id)
    await delete_user_link_request(target_id)
    await message.answer(
        f"Unlinked: {existing['player_name']} ({existing['player_tag']}).",
        parse_mode=None,
    )


@router.message(F.text & (F.chat.type == ChatType.PRIVATE))
async def handle_private_text(message: Message) -> None:
    if message.text is None or message.text.startswith("/"):
        return
    if message.from_user is None:
        return

    state_key = _apply_state_key(message.from_user.id)
    apply_state = await get_app_state(state_key)
    if apply_state:
        status = apply_state.get("status")
        if status == "awaiting_name":
            nickname = message.text.strip()
            if not nickname:
                await message.answer(
                    "Nickname cannot be empty. Send your in-game nickname.",
                    parse_mode=None,
                )
                return
            if len(nickname) > 32:
                await message.answer(
                    "Nickname is too long. Please send a shorter one.",
                    parse_mode=None,
                )
                return

            pending_app = await get_pending_application_for_user(message.from_user.id)
            if pending_app:
                await delete_app_state(state_key)
                await message.answer(
                    "You already have a pending application:\n"
                    f"{_format_application_summary(pending_app)}",
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
                (
                    "New application received:\n"
                    f"ID: {app['id']}\n"
                    f"Player: {nickname}\n"
                    f"Tag: n/a\n"
                    f"User: {user_label} id={message.from_user.id}"
                ),
            )
            await message.answer(
                "Send your player tag (optional) or type 'skip'.",
                parse_mode=None,
            )
            return

        if status == "awaiting_tag":
            app_id = apply_state.get("application_id")
            if not app_id:
                await delete_app_state(state_key)
                await message.answer(
                    "Please restart with /start apply.",
                    parse_mode=None,
                )
                return
            ok, tag = _parse_optional_tag(message.text)
            if not ok:
                await message.answer(
                    "Invalid tag format. Send like #ABC123 or type 'skip'.",
                    parse_mode=None,
                )
                return
            if tag:
                await update_application_tag(app_id, tag)
            await delete_app_state(state_key)
            await message.answer(
                "✅ Application received. Admins will review.",
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
            "Please select your account from the buttons above.", parse_mode=None
        )


@router.callback_query(F.data.startswith("link_select:"))
async def handle_link_select(query: CallbackQuery) -> None:
    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 3:
        await query.answer("Invalid selection.", show_alert=True)
        return

    try:
        target_user_id = int(parts[1])
    except ValueError:
        await query.answer("Invalid selection.", show_alert=True)
        return

    tag = _normalize_tag(parts[2])

    request = await get_user_link_request(target_user_id)
    if not request or request.get("status") != "awaiting_choice":
        await query.answer("This link request has expired.", show_alert=True)
        return

    if query.from_user is None:
        await query.answer("Unable to verify your account.", show_alert=True)
        return

    if query.from_user.id != target_user_id:
        authorized = False
        if query.message is not None:
            try:
                authorized = await _is_admin_user(query.message, query.from_user.id)
            except Exception:
                authorized = False
        if not authorized and query.from_user.id not in ADMIN_USER_IDS:
            await query.answer("You are not allowed to do this.", show_alert=True)
            return

    clan_tag = _require_clan_tag()
    if not clan_tag:
        await query.answer("CLAN_TAG is not configured.", show_alert=True)
        return

    player_name = await get_player_name_for_tag(tag, clan_tag)
    if not player_name:
        player_name = "Unknown"

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
            f"✅ Linked: {player_name} ({tag}). Now use /my_activity.",
            parse_mode=None,
        )
    else:
        await query.bot.send_message(
            target_user_id,
            f"✅ Linked: {player_name} ({tag}). Now use /my_activity.",
            parse_mode=None,
        )

    await query.answer("Linked.")


@router.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def trace_catch_all(message: Message) -> None:
    if message.from_user is None or message.from_user.is_bot:
        return
    if is_bot_command_message(message):
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
