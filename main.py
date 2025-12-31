"""Main entry point for the Clash Royale Telegram Bot."""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from bot import router
from config import TELEGRAM_BOT_TOKEN, CLAN_TAG, FETCH_INTERVAL_SECONDS
from cr_api import get_api_client, close_api_client, ClashRoyaleAPIError
from db import (
    connect_db,
    close_db,
    delete_app_state,
    get_app_state,
    get_enabled_clan_chats,
    get_session,
    set_app_state,
    save_clan_member_daily,
    save_player_participation,
    save_player_participation_daily,
    save_river_race_state,
)
from reports import build_weekly_report
from riverrace_import import get_last_completed_week, get_latest_riverrace_log_info

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

FETCH_LOCK = asyncio.Lock()
ACTIVE_WEEK_KEY = "active_week"
LAST_REPORTED_WEEK_KEY = "last_reported_week"
BOT: Bot | None = None


async def fetch_river_race_stats() -> None:
    """
    Fetch current River Race stats and store player participation data.
    
    This function:
    1. Gets the current River Race data from the CR API
    2. Extracts season ID, section index, and period type
    3. Determines if it's a Colosseum week
    4. Saves participation data for each clan member
    5. Updates the River Race state in the database
    """
    async with FETCH_LOCK:
        try:
            api_client = await get_api_client()

            # Get current River Race data
            river_race = await api_client.get_current_river_race(CLAN_TAG)

            # Extract race metadata
            season_id = river_race.get("seasonId")
            section_index = river_race.get("sectionIndex")
            period_type = river_race.get("periodType", "unknown") or "unknown"

            missing_season = season_id is None
            missing_section = section_index is None

            if season_id is None:
                season_id = 0
            if section_index is None:
                section_index = 0

            period_type_lower = period_type.lower()

            # Determine if this is a Colosseum week
            # Colosseum weeks occur every 4th week (section_index is 0-based: 3, 7, 11, etc.)
            # The period type may also indicate "colosseum"
            is_colosseum = (
                period_type_lower == "colosseum" or (section_index + 1) % 4 == 0
            )
            
            logger.info(
                f"Fetching River Race stats - Season: {season_id}, "
                f"Section: {section_index}, Period: {period_type}, "
                f"Colosseum: {is_colosseum}"
            )
            
            # Find our clan in the race data
            clan_data = river_race.get("clan", {})
            clan_score = clan_data.get("fame", 0)

            # Get participant data from our clan
            participants = clan_data.get("participants", [])

            snapshot_date = datetime.now(timezone.utc).date()

            async with get_session() as session:
                try:
                    members = await api_client.get_clan_members(CLAN_TAG)
                    for member in members:
                        player_tag = member.get("tag", "")
                        if not player_tag:
                            continue
                        await save_clan_member_daily(
                            snapshot_date=snapshot_date,
                            clan_tag=CLAN_TAG,
                            player_tag=player_tag,
                            player_name=member.get("name", "Unknown"),
                            role=member.get("role"),
                            trophies=member.get("trophies"),
                            session=session,
                        )

                    if period_type_lower == "training":
                        log_info = await get_latest_riverrace_log_info(CLAN_TAG)
                        stored_active_week = await get_app_state(
                            ACTIVE_WEEK_KEY, session=session
                        )
                        if stored_active_week and log_info:
                            await delete_app_state(ACTIVE_WEEK_KEY, session=session)
                            logger.info("Cleared active_week from DB during training")
                        if season_id > 0:
                            await save_river_race_state(
                                clan_tag=CLAN_TAG,
                                season_id=season_id,
                                section_index=section_index,
                                is_colosseum=is_colosseum,
                                period_type=period_type_lower,
                                clan_score=clan_score,
                                session=session,
                            )
                        await session.commit()
                        return

                    resolved_season_id = 0
                    resolved_section_index = 0
                    current_valid = (
                        not missing_season
                        and not missing_section
                        and season_id > 0
                        and 0 <= section_index <= 3
                    )

                    if current_valid:
                        resolved_season_id = season_id
                        resolved_section_index = section_index
                        await set_app_state(
                            ACTIVE_WEEK_KEY,
                            {
                                "season_id": resolved_season_id,
                                "section_index": resolved_section_index,
                                "set_at": datetime.now(timezone.utc).isoformat(),
                            },
                            session=session,
                        )
                        logger.info(
                            "Using week key from currentriverrace: season=%s, section=%s",
                            resolved_season_id,
                            resolved_section_index,
                        )
                    else:
                        stored_active_week = await get_app_state(
                            ACTIVE_WEEK_KEY, session=session
                        )
                        stored_season_id = 0
                        stored_section_index = -1
                        if stored_active_week:
                            try:
                                stored_season_id = int(
                                    stored_active_week.get("season_id", 0)
                                )
                                stored_section_index = int(
                                    stored_active_week.get("section_index", -1)
                                )
                            except (TypeError, ValueError):
                                stored_season_id = 0
                                stored_section_index = -1

                        if stored_season_id > 0 and 0 <= stored_section_index <= 3:
                            resolved_season_id = stored_season_id
                            resolved_section_index = stored_section_index
                            logger.info(
                                "Using stored active_week from DB: season=%s, section=%s",
                                resolved_season_id,
                                resolved_section_index,
                            )
                        else:
                            log_info = await get_latest_riverrace_log_info(CLAN_TAG)
                            if not log_info or log_info["season_id"] <= 0:
                                logger.warning(
                                    "No active week available; skipping participation updates"
                                )
                                await session.commit()
                                return
                            derived_season_id = log_info["season_id"]
                            derived_section_index = log_info["section_index"] + 1
                            if derived_section_index > 3:
                                derived_section_index = 0
                                derived_season_id += 1
                            resolved_season_id = derived_season_id
                            resolved_section_index = derived_section_index
                            await set_app_state(
                                ACTIVE_WEEK_KEY,
                                {
                                    "season_id": resolved_season_id,
                                    "section_index": resolved_section_index,
                                    "set_at": datetime.now(timezone.utc).isoformat(),
                                },
                                session=session,
                            )
                            logger.info(
                                "Derived active_week from log_latest + 1: season=%s, section=%s",
                                resolved_season_id,
                                resolved_section_index,
                            )

                    season_id = resolved_season_id
                    section_index = resolved_section_index
                    is_colosseum = (
                        period_type_lower == "colosseum"
                        or (section_index + 1) % 4 == 0
                    )

                    if season_id <= 0:
                        logger.warning(
                            "Skipping participation updates due to missing season/section"
                        )
                        await session.commit()
                        return

                    if not participants:
                        logger.warning("No participants found in River Race data")
                    else:
                        # Save participation data for each player
                        saved_count = 0
                        for participant in participants:
                            player_tag = participant.get("tag", "")
                            player_name = participant.get("name", "Unknown")
                            fame = participant.get("fame", 0)
                            repair_points = participant.get("repairPoints", 0)
                            boat_attacks = participant.get("boatAttacks", 0)
                            decks_used = participant.get("decksUsed", 0)
                            decks_used_today = participant.get("decksUsedToday", 0)

                            if player_tag:
                                await save_player_participation(
                                    player_tag=player_tag,
                                    player_name=player_name,
                                    season_id=season_id,
                                    section_index=section_index,
                                    is_colosseum=is_colosseum,
                                    fame=fame,
                                    repair_points=repair_points,
                                    boat_attacks=boat_attacks,
                                    decks_used=decks_used,
                                    decks_used_today=decks_used_today,
                                    session=session,
                                )
                                await save_player_participation_daily(
                                    player_tag=player_tag,
                                    player_name=player_name,
                                    season_id=season_id,
                                    section_index=section_index,
                                    is_colosseum=is_colosseum,
                                    snapshot_date=snapshot_date,
                                    fame=fame,
                                    repair_points=repair_points,
                                    boat_attacks=boat_attacks,
                                    decks_used=decks_used,
                                    decks_used_today=decks_used_today,
                                    session=session,
                                )
                                saved_count += 1

                        logger.info(
                            f"Successfully saved participation data for {saved_count} players"
                        )

                    # Save the River Race state
                    await save_river_race_state(
                        clan_tag=CLAN_TAG,
                        season_id=season_id,
                        section_index=section_index,
                        is_colosseum=is_colosseum,
                        period_type=period_type_lower,
                        clan_score=clan_score,
                        session=session,
                    )
                    await session.commit()
                except Exception:
                    await session.rollback()
                    raise
        except ClashRoyaleAPIError as e:
            logger.error(f"Clash Royale API error: {e}")
        except Exception as e:
            logger.error(f"Error fetching River Race stats: {e}", exc_info=True)


async def maybe_post_weekly_report(bot: Bot) -> None:
    week = await get_last_completed_week(CLAN_TAG)
    if not week:
        logger.info("No completed week found for reporting")
        return
    season_id, section_index = week
    last_state = await get_app_state(LAST_REPORTED_WEEK_KEY)
    last_season_id = 0
    last_section_index = -1
    if last_state:
        try:
            last_season_id = int(last_state.get("season_id", 0))
            last_section_index = int(last_state.get("section_index", -1))
        except (TypeError, ValueError):
            last_season_id = 0
            last_section_index = -1
    if last_season_id == season_id and last_section_index == section_index:
        return

    chat_ids = await get_enabled_clan_chats(CLAN_TAG)
    if not chat_ids:
        logger.info("No enabled clan chats for weekly reporting")
        return

    report = await build_weekly_report(season_id, section_index, CLAN_TAG)
    sent_count = 0
    for chat_id in chat_ids:
        try:
            await bot.send_message(chat_id, report)
            sent_count += 1
        except Exception as e:
            logger.error("Failed to send weekly report to %s: %s", chat_id, e)

    await set_app_state(
        LAST_REPORTED_WEEK_KEY,
        {
            "season_id": season_id,
            "section_index": section_index,
            "set_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    logger.info(
        "Posted weekly report for season %s section %s to %s chat(s)",
        season_id,
        section_index,
        sent_count,
    )


async def background_fetch_task() -> None:
    """Background task that periodically fetches River Race stats."""
    logger.info(
        f"Starting background fetch task with interval: {FETCH_INTERVAL_SECONDS}s"
    )
    
    while True:
        try:
            await fetch_river_race_stats()
            if BOT is None:
                logger.warning("Bot instance not available for weekly reports")
            else:
                await maybe_post_weekly_report(BOT)
        except asyncio.CancelledError:
            logger.info("Background fetch task cancelled")
            break
        except Exception as e:
            logger.error(f"Error in background task: {e}", exc_info=True)
        
        # Wait for the next interval
        await asyncio.sleep(FETCH_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(dispatcher: Dispatcher):
    """
    Lifespan context manager for startup and shutdown events.
    
    Handles:
    - Database connection setup/teardown
    - API client cleanup
    - Background task management
    """
    # Startup
    logger.info("Starting bot...")
    
    # Connect to database
    await connect_db()
    logger.info("Connected to PostgreSQL")
    
    # Start background task
    fetch_task = asyncio.create_task(background_fetch_task())
    logger.info("Background fetch task started")
    
    yield
    
    # Shutdown
    logger.info("Shutting down bot...")
    
    # Cancel background task
    fetch_task.cancel()
    try:
        await fetch_task
    except asyncio.CancelledError:
        pass
    
    # Close connections
    await close_api_client()
    await close_db()
    logger.info("Cleanup complete")


async def main() -> None:
    """Main function to run the bot."""
    # Create bot instance
    bot = Bot(
        token=TELEGRAM_BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    global BOT
    BOT = bot
    
    # Create dispatcher
    dp = Dispatcher()
    
    # Register router with handlers
    dp.include_router(router)
    
    # Run bot with lifespan management
    async with lifespan(dp):
        logger.info("Bot is starting polling...")
        await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
