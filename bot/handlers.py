"""Telegram bot command handlers using aiogram v3."""

import logging
from datetime import datetime, timedelta, timezone

from aiogram import Bot, F, Router
from aiogram.enums import ChatMemberStatus, ChatType
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
    BOT_USERNAME,
    CAPTCHA_EXPIRE_MINUTES,
    CAPTCHA_MAX_ATTEMPTS,
    CAPTCHA_REMIND_COOLDOWN_SECONDS,
    CLAN_TAG,
    ENABLE_CAPTCHA,
    INACTIVE_LAST_SEEN_LIMIT,
    LAST_SEEN_RED_DAYS,
    LAST_SEEN_YELLOW_DAYS,
    WELCOME_RULES_MESSAGE_LINK,
)
from cr_api import ClashRoyaleAPIError, get_api_client
from db import (
    delete_verified_user,
    delete_user_link,
    delete_user_link_request,
    expire_active_challenges,
    get_captcha_question,
    get_current_member_tags,
    get_latest_challenge,
    get_or_create_pending_challenge,
    get_pending_challenge,
    get_top_absent_members,
    increment_challenge_attempts,
    is_user_verified,
    mark_challenge_expired,
    mark_challenge_failed,
    mark_challenge_passed,
    mark_pending_challenges_passed,
    get_player_name_for_tag,
    get_challenge_by_id,
    get_user_link,
    get_user_link_request,
    search_player_candidates,
    set_user_verified,
    touch_last_reminded_at,
    update_challenge_message_id,
    upsert_clan_chat,
    upsert_user_link,
    upsert_user_link_request,
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


def _is_debug_admin(user_id: int) -> bool:
    return user_id in ADMIN_TELEGRAM_IDS


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
) -> int | None:
    text = (
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

    general_commands: list[dict[str, object]] = [
        {
            "name": "/help",
            "what": "Show this help message.",
            "where": "Group + DM",
            "who": "Everyone",
            "usage": ["/help"],
            "args": "none",
            "notes": "Shows admin commands only to chat admins.",
        },
        {
            "name": "/start",
            "what": "Show the welcome message and command list.",
            "where": "Group + DM",
            "who": "Everyone",
            "usage": ["/start", "/start link"],
            "args": "Optional: link (starts account linking in DM). Example: /start link",
            "notes": "In groups, linking will ask you to open the bot in private.",
        },
        {
            "name": "/ping",
            "what": "Check bot responsiveness and API status.",
            "where": "Group + DM",
            "who": "Everyone",
            "usage": ["/ping"],
            "args": "none",
            "notes": "Uses the Clash Royale API; may show errors if API is down.",
        },
        {
            "name": "/war",
            "what": "Weekly war report (top active/inactive).",
            "where": "Group + DM",
            "who": "Everyone",
            "usage": ["/war"],
            "args": "none",
            "notes": "Uses the last completed week for the configured clan.",
        },
        {
            "name": "/war8",
            "what": "Rolling report for last 8 completed weeks.",
            "where": "Group + DM",
            "who": "Everyone",
            "usage": ["/war8"],
            "args": "none",
            "notes": "Current members only (latest snapshot).",
        },
        {
            "name": "/war_all",
            "what": "Send weekly, rolling, and kick shortlist reports together.",
            "where": "Group + DM",
            "who": "Everyone",
            "usage": ["/war_all"],
            "args": "none",
            "notes": "Sends three messages in sequence.",
        },
        {
            "name": "/current_war",
            "what": "Current war snapshot from database.",
            "where": "Group + DM",
            "who": "Everyone",
            "usage": ["/current_war"],
            "args": "none",
            "notes": "Data is based on latest DB snapshots (not live UI).",
        },
        {
            "name": "/my_activity",
            "what": "Show your own war activity report.",
            "where": "Group + DM",
            "who": "Everyone",
            "usage": ["/my_activity", "/my_activity <nickname>"],
            "args": "Optional nickname for self-linking in DM. Example: /my_activity Arcaneum",
            "notes": "If not linked, the bot guides you through linking.",
        },
        {
            "name": "/activity",
            "what": "Show a player's activity by nickname, @username, or reply.",
            "where": "Group + DM",
            "who": "Everyone",
            "usage": [
                "/activity <nickname>",
                "/activity @username",
                "Reply + /activity",
            ],
            "args": "Nickname or @username; reply to a user to use their linked account.",
            "notes": "If multiple matches, you will be asked to be more specific.",
        },
        {
            "name": "/promote_candidates",
            "what": "Promotion recommendations (elder/co-leader).",
            "where": "Group + DM",
            "who": "Everyone",
            "usage": ["/promote_candidates"],
            "args": "none",
            "notes": "Based on last 8 weeks and current member snapshot.",
        },
        {
            "name": "/donations",
            "what": "Donation leaderboards for current and recent weeks.",
            "where": "Group + DM",
            "who": "Everyone",
            "usage": ["/donations"],
            "args": "none",
            "notes": "Includes only current members from the latest snapshot.",
        },
        {
            "name": "/list_for_kick",
            "what": "Kick shortlist based on last 8 weeks.",
            "where": "Group + DM",
            "who": "Everyone",
            "usage": ["/list_for_kick"],
            "args": "none",
            "notes": "Applies filters and warnings (revived, donations, last seen).",
        },
        {
            "name": "/inactive",
            "what": "List most absent members by last seen time.",
            "where": "Group + DM",
            "who": "Everyone",
            "usage": ["/inactive"],
            "args": "none",
            "notes": "Current members only, based on latest snapshot.",
        },
    ]

    admin_commands: list[dict[str, object]] = [
        {
            "name": "/bind",
            "what": "Bind this chat for scheduled war reports.",
            "where": "Group only",
            "who": "Chat admins/creators",
            "usage": ["/bind"],
            "args": "none",
            "notes": "Binds this chat to the configured clan tag.",
        },
        {
            "name": "/admin_link_name",
            "what": "Force-link a user to an in-game nickname.",
            "where": "Group + DM",
            "who": "Chat admins/creators or ADMIN_USER_IDS",
            "usage": ["/admin_link_name <nickname> (reply required)"],
            "args": "Nickname (exact or partial). Example: reply + /admin_link_name Arcaneum",
            "notes": "Must be used as a reply to the target user.",
        },
        {
            "name": "/unlink",
            "what": "Remove a user's linked account.",
            "where": "Group + DM",
            "who": "Chat admins/creators or ADMIN_USER_IDS",
            "usage": ["/unlink (reply required)"],
            "args": "none",
            "notes": "Must be used as a reply to the target user.",
        },
    ]

    lines = [
        HEADER_LINE,
        "🤖 Black Poison Bot — Help",
        HEADER_LINE,
        "📌 Quick command list (plain text).",
        "",
        "🧩 GENERAL COMMANDS (everyone)",
        DIVIDER_LINE,
        *_format_help_commands(general_commands),
    ]
    if is_admin:
        lines.extend(
            [
                "",
                "🛡 ADMIN COMMANDS (chat admins only)",
                DIVIDER_LINE,
                *_format_help_commands(admin_commands),
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


@moderation_router.chat_member()
async def handle_member_join(event: ChatMemberUpdated) -> None:
    if not ENABLE_CAPTCHA:
        return
    if event.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    user = event.new_chat_member.user
    if user.is_bot:
        return
    if event.new_chat_member.status in (
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.CREATOR,
    ):
        return
    if event.old_chat_member.status not in (
        ChatMemberStatus.LEFT,
        ChatMemberStatus.KICKED,
    ):
        return
    if event.new_chat_member.status != ChatMemberStatus.MEMBER:
        return

    try:
        if await is_user_verified(event.chat.id, user.id):
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
        return
    if not question:
        question = await get_captcha_question(challenge["question_id"])
    if not question:
        return
    now = datetime.now(timezone.utc)
    last_reminded_at = challenge.get("last_reminded_at")
    if challenge.get("message_id") and isinstance(last_reminded_at, datetime):
        if (now - last_reminded_at).total_seconds() < CAPTCHA_REMIND_COOLDOWN_SECONDS:
            return

    message_id = await _send_captcha_message(
        event.bot,
        event.chat.id,
        challenge_id=challenge["id"],
        question=question,
    )
    if message_id:
        await update_challenge_message_id(challenge["id"], message_id)
        await touch_last_reminded_at(challenge["id"], now)
        logger.info(
            "Captcha sent to user %s in chat %s", user.id, event.chat.id
        )


@moderation_router.message(
    F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP})
)
async def handle_pending_user_message(message: Message) -> None:
    if not ENABLE_CAPTCHA:
        return
    if message.from_user is None or message.from_user.is_bot:
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

    if not challenge.get("message_id"):
        question = await get_captcha_question(challenge["question_id"])
        if question:
            message_id = await _send_captcha_message(
                message.bot,
                message.chat.id,
                challenge_id=challenge["id"],
                question=question,
            )
            if message_id:
                await update_challenge_message_id(challenge["id"], message_id)


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
        return

    attempts = await increment_challenge_attempts(challenge_id)
    if attempts >= CAPTCHA_MAX_ATTEMPTS:
        await mark_challenge_failed(
            challenge_id,
            datetime.now(timezone.utc)
            + timedelta(minutes=max(CAPTCHA_EXPIRE_MINUTES, 1)),
        )
        if query.message:
            await query.message.answer(
                "Too many attempts, try again later.", parse_mode=None
            )
        await query.answer("Too many attempts.", show_alert=False)
        logger.info(
            "Captcha failed for user %s in chat %s",
            challenge["user_id"],
            challenge["chat_id"],
        )
        return

    await query.answer("Wrong answer. Try again.", show_alert=False)

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

    try:
        await delete_verified_user(chat_id, target.id)
    except Exception as e:
        logger.error("Failed to remove verified flag: %s", e, exc_info=True)
        await message.answer(
            "Unable to remove verified flag right now.", parse_mode=None
        )
        return

    await message.answer("Verified flag removed.", parse_mode=None)


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
