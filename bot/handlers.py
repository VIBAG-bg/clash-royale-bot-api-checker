"""Telegram bot command handlers using aiogram v3."""

import logging
from datetime import datetime, timezone

from aiogram import F, Router
from aiogram.enums import ChatMemberStatus, ChatType
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from config import ADMIN_USER_IDS, BOT_USERNAME, CLAN_TAG
from cr_api import ClashRoyaleAPIError, get_api_client
from db import (
    delete_user_link,
    delete_user_link_request,
    get_current_member_tags,
    get_inactive_players,
    get_latest_war_race_state,
    get_player_name_for_tag,
    get_user_link,
    get_user_link_request,
    search_player_candidates,
    upsert_clan_chat,
    upsert_user_link,
    upsert_user_link_request,
)
from reports import (
    build_current_war_report,
    build_kick_shortlist_report,
    build_my_activity_report,
    build_rolling_report,
    build_weekly_report,
)
from riverrace_import import get_last_completed_week, get_last_completed_weeks

logger = logging.getLogger(__name__)

# Create router for handlers
router = Router(name="main_handlers")


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
        "/activity - Show activity by nickname or @username"
    )
    await message.answer(welcome_text, parse_mode=None)


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
        state = await get_latest_war_race_state(clan_tag)
        if state is None:
            await message.answer(
                "No River Race data available yet.\n"
                "The bot needs to fetch data first. Please wait for the next update cycle.",
                parse_mode=None,
            )
            return

        season_id = state["season_id"]
        section_index = state["section_index"]
        is_colosseum = state.get("is_colosseum", False)

        current_members = await get_current_member_tags(clan_tag)
        if not current_members:
            await message.answer(
                "No clan membership snapshot available yet.\n"
                "Please wait for the next update cycle.",
                parse_mode=None,
            )
            return

        inactive_players = await get_inactive_players(
            season_id=season_id,
            section_index=section_index,
            min_decks=4,
            player_tags=current_members,
        )

        week_type = "Colosseum" if is_colosseum else "River Race"

        if not inactive_players:
            await message.answer(
                f"All players are active in the current {week_type}!\n\n"
                f"Season: {season_id}\n"
                f"Week: {section_index + 1}",
                parse_mode=None,
            )
            return

        response_lines = [
            f"Players with low participation in {week_type}:\n",
            f"Season: {season_id} | Week: {section_index + 1}\n",
        ]

        for i, player in enumerate(inactive_players, 1):
            player_name = player.get("player_name", "Unknown")
            decks_used = player.get("decks_used", 0)
            fame = player.get("fame", 0)

            response_lines.append(
                f"{i}. {player_name}\n"
                f"   Decks: {decks_used} | Fame: {fame}"
            )

        response_lines.append(f"\nTotal: {len(inactive_players)} player(s)")

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
