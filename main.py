"""Main entry point for the Clash Royale Telegram Bot."""

import asyncio
import logging
from contextlib import asynccontextmanager

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from bot import router
from config import TELEGRAM_BOT_TOKEN, CLAN_TAG, FETCH_INTERVAL_SECONDS
from cr_api import get_api_client, close_api_client, ClashRoyaleAPIError
from db import (
    connect_db,
    close_db,
    get_session,
    save_player_participation,
    save_river_race_state,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

FETCH_LOCK = asyncio.Lock()


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
            season_id = river_race.get("seasonId", 0)
            section_index = river_race.get("sectionIndex", 0)
            period_type = river_race.get("periodType", "unknown")
            
            # Determine if this is a Colosseum week
            # Colosseum weeks occur every 4th week (section_index is 0-based: 3, 7, 11, etc.)
            # The period type may also indicate "colosseum"
            is_colosseum = (
                period_type.lower() == "colosseum" or
                (section_index + 1) % 4 == 0
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
            
            if not participants:
                logger.warning("No participants found in River Race data")
                return
            
            # Save participation data for each player
            saved_count = 0
            async with get_session() as session:
                try:
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
                            saved_count += 1
                    
                    # Save the River Race state
                    await save_river_race_state(
                        clan_tag=CLAN_TAG,
                        season_id=season_id,
                        section_index=section_index,
                        is_colosseum=is_colosseum,
                        period_type=period_type,
                        clan_score=clan_score,
                        session=session,
                    )
                    await session.commit()
                except Exception:
                    await session.rollback()
                    raise
            
            logger.info(
                f"Successfully saved participation data for {saved_count} players"
            )
        except ClashRoyaleAPIError as e:
            logger.error(f"Clash Royale API error: {e}")
        except Exception as e:
            logger.error(f"Error fetching River Race stats: {e}", exc_info=True)


async def background_fetch_task() -> None:
    """Background task that periodically fetches River Race stats."""
    logger.info(
        f"Starting background fetch task with interval: {FETCH_INTERVAL_SECONDS}s"
    )
    
    while True:
        try:
            await fetch_river_race_stats()
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
