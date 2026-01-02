"""Main entry point for the Clash Royale Telegram Bot."""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from datetime import timedelta

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from bot import router, moderation_router
from config import (
    CLAN_TAG,
    CR_API_TOKEN,
    FETCH_INTERVAL_SECONDS,
    REMINDER_COLOSSEUM_BANNER_URL,
    REMINDER_COLOSSEUM_BANNER_URL_DAY4,
    REMINDER_ENABLED,
    REMINDER_TIME_UTC,
    REMINDER_WAR_BANNER_URL,
    REMINDER_WAR_BANNER_URL_DAY4,
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
REMINDER_LOCK = asyncio.Lock()
ACTIVE_WEEK_KEY = "active_week"
LAST_REPORTED_WEEK_KEY = "last_reported_week"
LAST_PROMOTE_SEASON_KEY = "last_promote_season"
LAST_WAR_REMINDER_KEY = "last_war_reminder"
WAR_DAY_START_KEY = "war_day_start"
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


def _parse_reminder_time(value: str) -> tuple[int, int] | None:
    if not value:
        return None
    parts = value.split(":")
    if len(parts) != 2:
        return None
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return None
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return hour, minute


async def _store_war_day_start(
    season_id: int,
    section_index: int,
    period_type: str,
    day_number: int,
    session,
) -> None:
    start_date = datetime.now(timezone.utc).date() - timedelta(days=day_number - 1)
    await set_app_state(
        WAR_DAY_START_KEY,
        {
            "season_id": season_id,
            "section_index": section_index,
            "period_type": period_type,
            "start_date": start_date.isoformat(),
        },
        session=session,
    )


async def _resolve_war_day_number(
    *,
    period_index: object,
    season_id: int,
    section_index: int,
    period_type: str,
    session,
) -> int | None:
    parsed_index = _coerce_non_negative_int(period_index)
    if parsed_index is not None:
        day_number = parsed_index + 1
        if 1 <= day_number <= 4:
            await _store_war_day_start(
                season_id, section_index, period_type, day_number, session
            )
            return day_number
        return None

    state = await get_app_state(WAR_DAY_START_KEY, session=session)
    if not isinstance(state, dict):
        return None
    try:
        if int(state.get("season_id", 0)) != season_id:
            return None
        if int(state.get("section_index", -1)) != section_index:
            return None
        if str(state.get("period_type", "")).lower() != period_type:
            return None
        start_date = datetime.fromisoformat(state.get("start_date")).date()
    except Exception:
        return None
    delta_days = (datetime.now(timezone.utc).date() - start_date).days
    day_number = delta_days + 1
    if 1 <= day_number <= 4:
        return day_number
    return None


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


async def maybe_post_daily_war_reminder(bot: Bot) -> None:
    if not REMINDER_ENABLED:
        return
    async with REMINDER_LOCK:
        api_client = await get_api_client()
        river_race = await api_client.get_current_river_race(CLAN_TAG)
        period_type = river_race.get("periodType", "unknown") or "unknown"
        period_type_lower = str(period_type).lower()
        if period_type_lower not in ("warday", "colosseum"):
            logger.info(
                "Reminder skipped: period type %s", period_type_lower
            )
            return

        season_id = _parse_season_id(river_race.get("seasonId"))
        section_index = _parse_section_index(river_race.get("sectionIndex"))

        async with get_session() as session:
            resolved_season_id, resolved_section_index, source = (
                await _resolve_active_week(
                    current_season_id=season_id,
                    current_section_index=section_index,
                    session=session,
                )
            )
            if resolved_season_id is None or resolved_section_index is None:
                logger.warning(
                    "Reminder skipped: unable to resolve week (source=%s)", source
                )
                return
            day_number = await _resolve_war_day_number(
                period_index=river_race.get("periodIndex"),
                season_id=resolved_season_id,
                section_index=resolved_section_index,
                period_type=period_type_lower,
                session=session,
            )
            if day_number is None:
                logger.warning("Reminder skipped: unknown day number")
                return

            last_state = await get_app_state(LAST_WAR_REMINDER_KEY, session=session)
            if isinstance(last_state, dict):
                try:
                    if (
                        int(last_state.get("season_id", 0)) == resolved_season_id
                        and int(last_state.get("section_index", -1))
                        == resolved_section_index
                        and str(last_state.get("period_type", "")).lower()
                        == period_type_lower
                        and int(last_state.get("day_number", 0)) == day_number
                    ):
                        return
                except Exception:
                    pass

            chat_ids = await get_enabled_clan_chats(CLAN_TAG)
            if not chat_ids:
                logger.info("No enabled clan chats for daily reminders")
                return

            if period_type_lower == "colosseum":
                messages = {
                    1: "🏛 COLISEUM WAR HAS STARTED\nDay 1 is live.\n❗ Participation is mandatory.\n⚔️ Play your attacks.",
                    2: "🏛 Coliseum – Day 2\n⚔️ All attacks matter.\n❗ Participation is mandatory.",
                    3: "🏛 Coliseum – Day 3\n🔥 Stay active.\n❗ Participation is mandatory.",
                    4: "🚨 FINAL DAY – COLISEUM\n⚔️ Finish your attacks today.\n📊 Inactive players will be reviewed after war.",
                }
                banner_url = (
                    REMINDER_COLOSSEUM_BANNER_URL_DAY4
                    if day_number == 4
                    else REMINDER_COLOSSEUM_BANNER_URL
                )
            else:
                messages = {
                    1: "🏁 Clan War has begun!\nDay 1 is live.\n⚔️ Use your attacks and bring fame to the clan.",
                    2: "⏳ Clan War – Day 2\nNew war day is open.\n💪 Don’t forget to play your battles.",
                    3: "🔥 Clan War – Day 3\nWe’re close to the finish.\n⚔️ Every attack matters.",
                    4: "🚨 Final Day of Clan War!\n⚔️ Finish your attacks today.\n📊 Results and activity report after war ends.",
                }
                banner_url = (
                    REMINDER_WAR_BANNER_URL_DAY4
                    if day_number == 4
                    else REMINDER_WAR_BANNER_URL
                )

            message = messages.get(day_number)
            if not message:
                logger.warning("Reminder skipped: no template for day %s", day_number)
                return

            sent_count = 0
            for chat_id in chat_ids:
                try:
                    if day_number in (1, 4):
                        try:
                            await bot.send_photo(
                                chat_id,
                                photo=banner_url,
                                caption=message,
                                parse_mode=None,
                            )
                        except Exception:
                            await bot.send_message(
                                chat_id, message, parse_mode=None
                            )
                    else:
                        await bot.send_message(chat_id, message, parse_mode=None)
                    sent_count += 1
                except Exception as e:
                    logger.error(
                        "Failed to send reminder to %s: %s", chat_id, e
                    )

            await set_app_state(
                LAST_WAR_REMINDER_KEY,
                {
                    "clan_tag": CLAN_TAG,
                    "season_id": resolved_season_id,
                    "section_index": resolved_section_index,
                    "period_type": period_type_lower,
                    "day_number": day_number,
                    "sent_at": datetime.now(timezone.utc).isoformat(),
                },
                session=session,
            )

            logger.info(
                "Posted daily reminder day %s (%s) for season %s section %s to %s chat(s)",
                day_number,
                period_type_lower,
                resolved_season_id,
                resolved_section_index,
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


async def daily_reminder_task(bot: Bot) -> None:
    reminder_time = _parse_reminder_time(REMINDER_TIME_UTC)
    if reminder_time is None:
        logger.warning("Invalid REMINDER_TIME_UTC: %s", REMINDER_TIME_UTC)
        return
    hour, minute = reminder_time
    logger.info("Daily reminder scheduler started at %02d:%02d UTC", hour, minute)

    while True:
        try:
            now = datetime.now(timezone.utc)
            target = datetime(
                now.year, now.month, now.day, hour, minute, tzinfo=timezone.utc
            )
            if now < target:
                await asyncio.sleep((target - now).total_seconds())
            await maybe_post_daily_war_reminder(bot)
            next_target = target + timedelta(days=1)
            sleep_for = (next_target - datetime.now(timezone.utc)).total_seconds()
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
        except asyncio.CancelledError:
            logger.info("Daily reminder task cancelled")
            break
        except Exception as e:
            logger.error("Error in daily reminder task: %s", e, exc_info=True)
            await asyncio.sleep(60)


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
    reminder_task = None
    if REMINDER_ENABLED:
        reminder_task = asyncio.create_task(daily_reminder_task(BOT))
    logger.info("Background fetch task started")
    
    yield
    
    # Shutdown
    logger.info("Shutting down bot...")
    
    # Cancel background task
    fetch_task.cancel()
    if reminder_task is not None:
        reminder_task.cancel()
    try:
        await fetch_task
    except asyncio.CancelledError:
        pass
    if reminder_task is not None:
        try:
            await reminder_task
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
    dp.include_router(moderation_router)
    
    # Run bot with lifespan management
    async with lifespan(dp):
        logger.info("Bot is starting polling...")
        await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
