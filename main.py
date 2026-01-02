"""Main entry point for the Clash Royale Telegram Bot."""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from bot import router
from config import (
    CLAN_TAG,
    CR_API_TOKEN,
    FETCH_INTERVAL_SECONDS,
    TELEGRAM_BOT_TOKEN,
    require_env_value,
)
from cr_api import get_api_client, close_api_client, ClashRoyaleAPIError
from db import (
    connect_db,
    close_db,
    get_donation_week_start_date,
    get_colosseum_index_for_season,
    get_app_state,
    get_enabled_clan_chats,
    get_river_race_state_for_week,
    get_session,
    set_colosseum_index_for_season,
    set_app_state,
    save_player_participation,
    save_player_participation_daily,
    save_river_race_state,
    upsert_clan_member_daily,
    upsert_donations_weekly,
)
from reports import (
    build_kick_shortlist_report,
    build_promotion_candidates_report,
    build_rolling_report,
    build_weekly_report,
)
from riverrace_import import get_last_completed_week, get_last_completed_weeks

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

FETCH_LOCK = asyncio.Lock()
ACTIVE_WEEK_KEY = "active_week"
LAST_REPORTED_WEEK_KEY = "last_reported_week"
LAST_PROMOTE_SEASON_KEY = "last_promote_season"
BOT: Bot | None = None


def _ensure_required_config() -> str:
    token = require_env_value("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN)
    require_env_value("CR_API_TOKEN", CR_API_TOKEN)
    require_env_value("CLAN_TAG", CLAN_TAG)
    return token


def _coerce_non_negative_int(value: object) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def _parse_season_id(value: object) -> int | None:
    number = _coerce_non_negative_int(value)
    if number is None or number <= 0:
        return None
    return number


def _parse_section_index(value: object) -> int | None:
    return _coerce_non_negative_int(value)


def _parse_active_week_state(
    state: dict[str, object] | None,
) -> tuple[int | None, int | None]:
    if not isinstance(state, dict):
        return None, None
    season_id = _parse_season_id(state.get("season_id"))
    section_index = _parse_section_index(state.get("section_index"))
    if season_id is None or section_index is None:
        return None, None
    return season_id, section_index


def _build_active_week_state(season_id: int, section_index: int) -> dict[str, object]:
    return {
        "season_id": season_id,
        "section_index": section_index,
        "set_at": datetime.now(timezone.utc).isoformat(),
    }


async def _store_active_week(
    season_id: int, section_index: int, session
) -> None:
    await set_app_state(
        ACTIVE_WEEK_KEY,
        _build_active_week_state(season_id, section_index),
        session=session,
    )


async def _resolve_active_week(
    *,
    current_season_id: int | None,
    current_section_index: int | None,
    session,
) -> tuple[int | None, int | None, str]:
    stored_state = await get_app_state(ACTIVE_WEEK_KEY, session=session)
    stored_season_id, stored_section_index = _parse_active_week_state(stored_state)

    if current_section_index is not None:
        if current_season_id is not None:
            await _store_active_week(current_season_id, current_section_index, session)
            return current_season_id, current_section_index, "currentriverrace"
        if stored_season_id is not None:
            await _store_active_week(stored_season_id, current_section_index, session)
            return stored_season_id, current_section_index, "stored_active_week"
        return None, None, "missing"

    if stored_season_id is not None and stored_section_index is not None:
        return stored_season_id, stored_section_index, "stored_active_week"

    return None, None, "missing"


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
            season_id = _parse_season_id(river_race.get("seasonId"))
            section_index = _parse_section_index(river_race.get("sectionIndex"))
            period_type = river_race.get("periodType", "unknown") or "unknown"

            period_type_lower = period_type.lower()
            
            logger.info(
                "Fetched River Race stats - Season: %s, Section: %s, Period: %s",
                season_id if season_id is not None else "n/a",
                section_index if section_index is not None else "n/a",
                period_type,
            )
            
            # Find our clan in the race data
            clan_data = river_race.get("clan", {})
            clan_score = clan_data.get("fame", 0)

            # Get participant data from our clan
            participants = clan_data.get("participants", [])

            snapshot_date = datetime.now(timezone.utc).date()
            week_start_date = get_donation_week_start_date(datetime.now(timezone.utc))
            members: list[dict[str, object]] = []
            try:
                members = await api_client.get_clan_members(CLAN_TAG)
            except ClashRoyaleAPIError as e:
                logger.warning("Failed to fetch clan members: %s", e)
            except Exception as e:
                logger.warning(
                    "Failed to fetch clan members: %s", e, exc_info=True
                )

            async with get_session() as session:
                try:
                    if members:
                        await upsert_clan_member_daily(
                            snapshot_date=snapshot_date,
                            clan_tag=CLAN_TAG,
                            members=members,
                            session=session,
                        )
                        await upsert_donations_weekly(
                            clan_tag=CLAN_TAG,
                            week_start_date=week_start_date,
                            members=members,
                            session=session,
                        )

                    (
                        resolved_season_id,
                        resolved_section_index,
                        source,
                    ) = await _resolve_active_week(
                        current_season_id=season_id,
                        current_section_index=section_index,
                        session=session,
                    )

                    if (
                        resolved_season_id is not None
                        and resolved_section_index is not None
                    ):
                        logger.info(
                            "Week resolve: source=%s season=%s section=%s period=%s",
                            source,
                            resolved_season_id,
                            resolved_section_index,
                            period_type_lower,
                        )
                    else:
                        logger.warning(
                            "Week resolve failed: source=%s season=%s section=%s period=%s",
                            source,
                            resolved_season_id,
                            resolved_section_index,
                            period_type_lower,
                        )

                    if period_type_lower == "training":
                        if (
                            resolved_season_id is not None
                            and resolved_section_index is not None
                        ):
                            colosseum_index = await get_colosseum_index_for_season(
                                resolved_season_id, session=session
                            )
                            is_colosseum = (
                                resolved_section_index == colosseum_index
                                if colosseum_index is not None
                                else False
                            )
                            await save_river_race_state(
                                clan_tag=CLAN_TAG,
                                season_id=resolved_season_id,
                                section_index=resolved_section_index,
                                is_colosseum=is_colosseum,
                                period_type=period_type_lower,
                                clan_score=clan_score,
                                session=session,
                            )
                        await session.commit()
                        return

                    if resolved_season_id is None or resolved_section_index is None:
                        logger.warning(
                            "Skipping participation updates due to missing season/section"
                        )
                        await session.commit()
                        return

                    season_id = resolved_season_id
                    section_index = resolved_section_index

                    if period_type_lower == "colosseum":
                        updated = await set_colosseum_index_for_season(
                            season_id, section_index, session=session
                        )
                        if updated:
                            logger.info(
                                "Updated colosseum mapping: season=%s section=%s",
                                season_id,
                                section_index,
                            )

                    colosseum_index = await get_colosseum_index_for_season(
                        season_id, session=session
                    )
                    if colosseum_index is not None:
                        is_colosseum = section_index == colosseum_index
                    else:
                        is_colosseum = period_type_lower == "colosseum"

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

    weeks = await get_last_completed_weeks(8, CLAN_TAG)
    if not weeks:
        weeks = [week]
    weekly_report = await build_weekly_report(season_id, section_index, CLAN_TAG)
    rolling_report = await build_rolling_report(weeks, CLAN_TAG)
    kick_report = await build_kick_shortlist_report(weeks, week, CLAN_TAG)
    sent_count = 0
    for chat_id in chat_ids:
        try:
            await bot.send_message(chat_id, weekly_report, parse_mode=None)
            await bot.send_message(chat_id, rolling_report, parse_mode=None)
            await bot.send_message(chat_id, kick_report, parse_mode=None)
            sent_count += 1
        except Exception as e:
            logger.error("Failed to send weekly reports to %s: %s", chat_id, e)

    await set_app_state(
        LAST_REPORTED_WEEK_KEY,
        {
            "season_id": season_id,
            "section_index": section_index,
            "set_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    logger.info(
        "Posted weekly/rolling/kick reports for season %s section %s to %s chat(s)",
        season_id,
        section_index,
        sent_count,
    )


async def maybe_post_promotion_candidates(bot: Bot) -> None:
    week = await get_last_completed_week(CLAN_TAG)
    if not week:
        return
    season_id, section_index = week
    state = await get_river_race_state_for_week(CLAN_TAG, season_id, section_index)
    is_colosseum = bool(state.get("is_colosseum")) if state else False
    if not is_colosseum:
        colosseum_index = await get_colosseum_index_for_season(season_id)
        if colosseum_index is None or colosseum_index != section_index:
            return

    last_state = await get_app_state(LAST_PROMOTE_SEASON_KEY)
    last_season_id = 0
    if last_state:
        try:
            last_season_id = int(last_state.get("season_id", 0))
        except (TypeError, ValueError):
            last_season_id = 0
    if last_season_id == season_id:
        return

    chat_ids = await get_enabled_clan_chats(CLAN_TAG)
    if not chat_ids:
        return

    report = await build_promotion_candidates_report(CLAN_TAG)
    sent_count = 0
    for chat_id in chat_ids:
        try:
            await bot.send_message(chat_id, report, parse_mode=None)
            sent_count += 1
        except Exception as e:
            logger.error("Failed to send promotion report to %s: %s", chat_id, e)

    await set_app_state(
        LAST_PROMOTE_SEASON_KEY,
        {
            "season_id": season_id,
            "set_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    logger.info(
        "Posted promotion recommendations for season %s to %s chat(s)",
        season_id,
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
                await maybe_post_promotion_candidates(BOT)
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
    token = _ensure_required_config()
    # Create bot instance
    bot = Bot(
        token=token,
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
