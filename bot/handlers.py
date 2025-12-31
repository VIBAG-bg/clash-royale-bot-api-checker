"""Telegram bot command handlers using aiogram v3."""

import logging
from datetime import datetime, timezone

from aiogram import Router
from aiogram.enums import ChatMemberStatus, ChatType
from aiogram.filters import Command
from aiogram.types import Message

from config import CLAN_TAG
from cr_api import get_api_client, ClashRoyaleAPIError
from db import get_inactive_players, get_latest_war_race_state, upsert_clan_chat
from reports import build_rolling_report, build_weekly_report
from riverrace_import import get_last_completed_week, get_last_completed_weeks

logger = logging.getLogger(__name__)

# Create router for handlers
router = Router(name="main_handlers")


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    """Handle /start command - Welcome message and bot information."""
    welcome_text = (
        "👋 Welcome to the Clash Royale Clan Monitor Bot!\n\n"
        "This bot monitors your clan's River Race participation "
        "and tracks player activity.\n\n"
        "📋 Available commands:\n"
        "/start - Show this welcome message\n"
        "/ping - Check if the bot is responsive\n"
        "/inactive - Show players with low River Race participation\n\n"
        "🔄 The bot automatically fetches clan war stats and stores "
        "participation data for each River Race week."
    )
    await message.answer(welcome_text)


@router.message(Command("ping"))
async def cmd_ping(message: Message) -> None:
    """Handle /ping command - Check bot responsiveness and API status."""
    start_time = datetime.now(timezone.utc)
    
    # Check Clash Royale API connectivity
    api_status = "✅ Connected"
    clan_name = "Unknown"
    
    try:
        api_client = await get_api_client()
        clan_data = await api_client.get_clan(CLAN_TAG)
        # Validate response is a dictionary before accessing properties
        if isinstance(clan_data, dict):
            clan_name = clan_data.get("name", "Unknown")
    except ClashRoyaleAPIError as e:
        api_status = f"❌ Error: {e.message}"
        logger.warning(f"API check failed: {e}")
    except Exception as e:
        api_status = f"❌ Error: {str(e)}"
        logger.error(f"Unexpected error during API check: {e}")
    
    # Calculate response time
    response_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
    
    response_text = (
        "🏓 Pong!\n\n"
        f"⏱ Response time: {response_time:.0f}ms\n"
        f"🎮 Clash Royale API: {api_status}\n"
        f"🏰 Monitoring clan: {clan_name}\n"
        f"📅 Server time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC"
    )
    
    await message.answer(response_text)


@router.message(Command("bind"))
async def cmd_bind(message: Message) -> None:
    """Bind the current group chat for weekly war reports."""
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("This command can only be used in a group chat.")
        return
    if message.from_user is None:
        await message.answer("Unable to verify permissions for this user.")
        return
    try:
        member = await message.bot.get_chat_member(
            chat_id=message.chat.id, user_id=message.from_user.id
        )
    except Exception as e:
        logger.error("Failed to verify chat member: %s", e, exc_info=True)
        await message.answer("Unable to verify permissions right now.")
        return
    if member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR):
        await message.answer("Only chat admins can bind this group.")
        return
    await upsert_clan_chat(CLAN_TAG, message.chat.id, enabled=True)
    await message.answer("Chat bound for weekly war reports.")


@router.message(Command("war"))
async def cmd_war(message: Message) -> None:
    """Show the weekly war report for the last completed week."""
    week = await get_last_completed_week(CLAN_TAG)
    if not week:
        await message.answer("No completed war weeks found yet.")
        return
    season_id, section_index = week
    report = await build_weekly_report(season_id, section_index, CLAN_TAG)
    await message.answer(report)


@router.message(Command("war8"))
async def cmd_war8(message: Message) -> None:
    """Show the rolling war report for the last 8 completed weeks."""
    weeks = await get_last_completed_weeks(8, CLAN_TAG)
    if not weeks:
        await message.answer("No completed war weeks found yet.")
        return
    report = await build_rolling_report(weeks, CLAN_TAG)
    await message.answer(report)


@router.message(Command("inactive"))
async def cmd_inactive(message: Message) -> None:
    """Handle /inactive command - Show players with low River Race participation."""
    try:
        # Get the latest River Race state to know current season/section
        state = await get_latest_war_race_state(CLAN_TAG)
        
        if state is None:
            await message.answer(
                "⚠️ No River Race data available yet.\n"
                "The bot needs to fetch data first. Please wait for the next update cycle."
            )
            return
        
        season_id = state["season_id"]
        section_index = state["section_index"]
        is_colosseum = state.get("is_colosseum", False)
        
        # Get inactive players (those with less than 4 decks used)
        inactive_players = await get_inactive_players(
            season_id=season_id,
            section_index=section_index,
            min_decks=4
        )
        
        week_type = "Colosseum" if is_colosseum else "River Race"
        
        if not inactive_players:
            await message.answer(
                f"✅ All players are active in the current {week_type}!\n\n"
                f"📅 Season: {season_id}\n"
                f"📊 Week: {section_index + 1}"
            )
            return
        
        # Build response message
        response_lines = [
            f"⚠️ Players with low participation in {week_type}:\n",
            f"📅 Season: {season_id} | Week: {section_index + 1}\n",
        ]
        
        for i, player in enumerate(inactive_players, 1):
            player_name = player.get("player_name", "Unknown")
            decks_used = player.get("decks_used", 0)
            fame = player.get("fame", 0)
            
            response_lines.append(
                f"{i}. {player_name}\n"
                f"   🎴 Decks: {decks_used} | ⭐ Fame: {fame}"
            )
        
        response_lines.append(f"\n📊 Total: {len(inactive_players)} player(s)")
        
        await message.answer("\n".join(response_lines))
        
    except Exception as e:
        logger.error(f"Error in /inactive command: {e}", exc_info=True)
        await message.answer(
            "❌ An error occurred while fetching inactive players.\n"
            "Please try again later."
        )
