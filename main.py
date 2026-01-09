"""Main entry point for the Clash Royale Telegram Bot."""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone, date, timedelta

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ParseMode

from bot import router, moderation_router
from config import (
    AUTO_INVITE_BATCH_SIZE,
    AUTO_INVITE_CHECK_INTERVAL_MINUTES,
    AUTO_INVITE_ENABLED,
    AUTO_INVITE_INVITE_MINUTES,
    AUTO_INVITE_MAX_ATTEMPTS,
    CLAN_TAG,
    CR_API_TOKEN,
    FETCH_INTERVAL_SECONDS,
    MODLOG_CHAT_ID,
    REMINDER_COLOSSEUM_BANNER_URL,
    REMINDER_COLOSSEUM_BANNER_URL_DAY4,
    REMINDER_ENABLED,
    REMINDER_TIME_UTC,
    REMINDER_WAR_BANNER_URL,
    REMINDER_WAR_BANNER_URL_DAY4,
    TRAINING_DAYS_FALLBACK,
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
    get_first_snapshot_date_for_week,
    get_river_race_state_for_week,
    get_session,
    list_invite_candidates,
    list_invited_applications,
    log_mod_action,
    list_due_scheduled_unmutes,
    mark_application_invited,
    mark_application_joined,
    mark_scheduled_unmute_sent,
    reset_expired_invite,
    try_mark_reminder_posted,
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
from i18n import DEFAULT_LANG, t

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
WAR_DAY_NUMBER_KEY = "war_day_number"
WAR_DAY_NUMBER_DATE_KEY = "war_day_number_date"
WAR_DAY_RESOLVED_BY_KEY = "war_day_resolved_by"
BOT: Bot | None = None


async def _send_modlog(bot: Bot, text: str) -> None:
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
        logger.warning("Failed to send modlog: %s", e)


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


def _parse_cr_timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if not isinstance(value, str):
        return None
    for fmt in ("%Y%m%dT%H%M%S.%fZ", "%Y%m%dT%H%M%SZ"):
        try:
            parsed = datetime.strptime(value, fmt)
        except ValueError:
            continue
        return parsed.replace(tzinfo=timezone.utc)
    return None


def _normalize_clan_tag(tag: str) -> str:
    return tag[1:].upper() if tag.startswith("#") else tag.upper()


def _find_riverrace_log_anchor(
    items: object, clan_tag: str
) -> tuple[datetime | None, str | None]:
    if isinstance(items, dict):
        items = items.get("items", [])
    if not isinstance(items, list):
        return None, None
    target_tag = _normalize_clan_tag(clan_tag)
    for item in items:
        standings = item.get("standings", [])
        if not isinstance(standings, list):
            continue
        clan_entry = None
        for standing in standings:
            clan = standing.get("clan", {})
            tag = clan.get("tag", "")
            if tag and _normalize_clan_tag(tag) == target_tag:
                clan_entry = clan
                break
        if not clan_entry:
            continue
        finish_time = clan_entry.get("finishTime")
        anchor = _parse_cr_timestamp(finish_time)
        if anchor is not None:
            return anchor, "finishTime"
        created_date = item.get("createdDate")
        anchor = _parse_cr_timestamp(created_date)
        if anchor is not None:
            return anchor, "createdDate"
    return None, None


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


async def _store_war_day_number_state(
    season_id: int,
    section_index: int,
    period_type: str,
    day_number: int,
    resolved_by: str,
    war_day_start_time: object,
    session,
) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    await set_app_state(
        WAR_DAY_NUMBER_KEY,
        {
            "day_number": day_number,
            "season_id": season_id,
            "section_index": section_index,
            "period_type": period_type,
            "war_day_start_time": war_day_start_time,
            "set_at": datetime.now(timezone.utc).isoformat(),
        },
        session=session,
    )
    await set_app_state(
        WAR_DAY_NUMBER_DATE_KEY,
        {"date": today},
        session=session,
    )
    await set_app_state(
        WAR_DAY_RESOLVED_BY_KEY,
        {"resolved_by": resolved_by},
        session=session,
    )


async def _resolve_war_day_number(
    *,
    period_index: object,
    season_id: int,
    section_index: int,
    period_type: str,
    first_snapshot_date: date | None,
    log_items: object | None,
) -> tuple[int | None, str | None, dict[str, object]]:
    context: dict[str, object] = {
        "season_id": season_id,
        "section_index": section_index,
        "period_type": period_type,
        "periodIndex": period_index,
        "first_snapshot_date": first_snapshot_date.isoformat()
        if isinstance(first_snapshot_date, date)
        else None,
    }
    today = datetime.now(timezone.utc).date()
    if isinstance(first_snapshot_date, date):
        day_number = (today - first_snapshot_date).days + 1
        if day_number < 1:
            day_number = 1
        if day_number <= 10:
            return day_number, "db", context
        return None, "none", context

    anchor_dt, anchor_source = _find_riverrace_log_anchor(log_items, CLAN_TAG)
    context["finish_anchor"] = (
        anchor_dt.isoformat() if isinstance(anchor_dt, datetime) else None
    )
    context["finish_anchor_source"] = anchor_source
    context["training_days_fallback"] = TRAINING_DAYS_FALLBACK
    if anchor_dt is not None:
        war_start_date = anchor_dt.date() + timedelta(days=TRAINING_DAYS_FALLBACK)
        day_number = (today - war_start_date).days + 1
        if day_number < 1:
            return None, "none", context
        if day_number <= 10:
            return day_number, (anchor_source or "finishTime"), context
        return None, "none", context

    parsed_index = _coerce_non_negative_int(period_index)
    if parsed_index is not None and 0 <= parsed_index <= 10:
        return parsed_index + 1, "periodIndex", context

    return None, "none", context


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
        if (
            stored_season_id is not None
            and stored_section_index is not None
            and current_section_index == 0
            and stored_section_index >= 3
            and (
                current_season_id is None
                or current_season_id <= stored_season_id
            )
        ):
            inferred_season_id = stored_season_id + 1
            await _store_active_week(inferred_season_id, 0, session)
            logger.info(
                "Heuristic rollover active week: stored=(%s,%s) current=(%s,%s) inferred=(%s,%s)",
                stored_season_id,
                stored_section_index,
                current_season_id,
                current_section_index,
                inferred_season_id,
                0,
            )
            return inferred_season_id, 0, "heuristic_rollover"
        if current_season_id is not None:
            await _store_active_week(current_season_id, current_section_index, session)
            return current_season_id, current_section_index, "currentriverrace"
        if stored_season_id is not None and stored_section_index is not None:
            if current_section_index >= stored_section_index:
                await _store_active_week(
                    stored_season_id, current_section_index, session
                )
                return stored_season_id, current_section_index, "stored_active_week"
            return stored_season_id, stored_section_index, "stored_active_week"
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
    weekly_report = await build_weekly_report(
        season_id, section_index, CLAN_TAG, lang=DEFAULT_LANG
    )
    rolling_report = await build_rolling_report(
        weeks, CLAN_TAG, lang=DEFAULT_LANG
    )
    kick_report = await build_kick_shortlist_report(
        weeks, week, CLAN_TAG, lang=DEFAULT_LANG
    )
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


async def maybe_post_daily_war_reminder(
    bot: Bot, *, debug_chat_id: int | None = None
) -> dict[str, object] | None:
    if not REMINDER_ENABLED and debug_chat_id is None:
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
            state = await get_river_race_state_for_week(
                CLAN_TAG, resolved_season_id, resolved_section_index
            )
            is_colosseum = bool(state.get("is_colosseum")) if state else False
            if not state:
                colosseum_index = await get_colosseum_index_for_season(
                    resolved_season_id, session=session
                )
                if colosseum_index is not None:
                    is_colosseum = resolved_section_index == colosseum_index
                else:
                    is_colosseum = period_type_lower == "colosseum"
            effective_period_type = "colosseum" if is_colosseum else "warday"
            first_snapshot_date = await get_first_snapshot_date_for_week(
                resolved_season_id, resolved_section_index, session=session
            )
            override_date = None
            override_season = _coerce_non_negative_int(
                os.getenv("WAR_DAY_OVERRIDE_SEASON")
            )
            override_section = _coerce_non_negative_int(
                os.getenv("WAR_DAY_OVERRIDE_SECTION")
            )
            override_date_raw = os.getenv("WAR_DAY_OVERRIDE_START_DATE")
            if override_date_raw:
                try:
                    override_date = date.fromisoformat(override_date_raw)
                except ValueError:
                    logger.warning(
                        "Invalid WAR_DAY_OVERRIDE_START_DATE: %s",
                        override_date_raw,
                    )
            if (
                override_date is not None
                and override_season is not None
                and override_section is not None
                and override_season == resolved_season_id
                and override_section == resolved_section_index
            ):
                first_snapshot_date = override_date
                logger.info(
                    "Using war day override start_date=%s for season=%s section=%s",
                    override_date.isoformat(),
                    resolved_season_id,
                    resolved_section_index,
                )
            snapshot_value = (
                first_snapshot_date.isoformat()
                if isinstance(first_snapshot_date, date)
                else "n/a"
            )
            override_value = (
                override_date.isoformat() if override_date is not None else "none"
            )
            log_items = None
            if first_snapshot_date is None:
                try:
                    log_items = await api_client.get_river_race_log(CLAN_TAG)
                except ClashRoyaleAPIError as e:
                    logger.info(
                        "Reminder log fallback failed: %s", e
                    )
                except Exception as e:
                    logger.info(
                        "Reminder log fallback failed: %s", e, exc_info=True
                    )

            day_number, resolved_by, context = await _resolve_war_day_number(
                period_index=river_race.get("periodIndex"),
                season_id=resolved_season_id,
                section_index=resolved_section_index,
                period_type=period_type_lower,
                first_snapshot_date=first_snapshot_date,
                log_items=log_items,
            )
            if day_number is None:
                if debug_chat_id is not None:
                    return {
                        "season_id": resolved_season_id,
                        "section_index": resolved_section_index,
                        "day_number": "n/a",
                        "period_type": effective_period_type,
                        "resolved_by": resolved_by or "none",
                        "snapshot": snapshot_value,
                        "override": override_value,
                    }
                logger.info(
                    "Reminder skipped: unknown day number (season=%s section=%s period=%s periodIndex=%r first_snapshot_date=%r finish_anchor=%r finish_source=%r training_days_fallback=%s resolved_by=%r now=%s)",
                    context.get("season_id"),
                    context.get("section_index"),
                    context.get("period_type"),
                    context.get("periodIndex"),
                    context.get("first_snapshot_date"),
                    context.get("finish_anchor"),
                    context.get("finish_anchor_source"),
                    context.get("training_days_fallback"),
                    resolved_by,
                    datetime.now(timezone.utc).isoformat(),
                )
                return
            logger.info(
                "Resolved reminder day: source=%s day=%s season=%s section=%s period=%s",
                resolved_by,
                day_number,
                resolved_season_id,
                resolved_section_index,
                effective_period_type,
            )
            debug_summary = {
                "season_id": resolved_season_id,
                "section_index": resolved_section_index,
                "day_number": day_number,
                "period_type": effective_period_type,
                "resolved_by": resolved_by or "none",
                "snapshot": snapshot_value,
                "override": override_value,
            }

            if debug_chat_id is None:
                last_state = await get_app_state(
                    LAST_WAR_REMINDER_KEY, session=session
                )
                if isinstance(last_state, dict):
                    try:
                        if (
                            int(last_state.get("season_id", 0)) == resolved_season_id
                            and int(last_state.get("section_index", -1))
                            == resolved_section_index
                            and str(last_state.get("period_type", "")).lower()
                            == effective_period_type
                            and int(last_state.get("day_number", 0)) == day_number
                        ):
                            return
                    except Exception:
                        pass

            if debug_chat_id is not None:
                chat_ids = [debug_chat_id]
                logger.info(
                    "Debug reminder: forcing chat_id=%s", debug_chat_id
                )
            else:
                chat_ids = await get_enabled_clan_chats(CLAN_TAG)
                if not chat_ids:
                    logger.info("No enabled clan chats for daily reminders")
                    return

            if effective_period_type == "colosseum":
                messages = {
                    1: t("coliseum_day1", DEFAULT_LANG),
                    2: t("coliseum_day2", DEFAULT_LANG),
                    3: t("coliseum_day3", DEFAULT_LANG),
                    4: t("coliseum_day4", DEFAULT_LANG),
                }
                banner_url = (
                    REMINDER_COLOSSEUM_BANNER_URL_DAY4
                    if day_number == 4
                    else REMINDER_COLOSSEUM_BANNER_URL
                )
            else:
                messages = {
                    1: t("riverside_day1", DEFAULT_LANG),
                    2: t("riverside_day2", DEFAULT_LANG),
                    3: t("riverside_day3", DEFAULT_LANG),
                    4: t("riverside_day4", DEFAULT_LANG),
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

            reminder_date = datetime.now(timezone.utc).date()
            sent_count = 0
            for chat_id in chat_ids:
                if debug_chat_id is None:
                    try:
                        should_send = await try_mark_reminder_posted(
                            chat_id=chat_id,
                            reminder_date=reminder_date,
                            season_id=resolved_season_id,
                            section_index=resolved_section_index,
                            period=effective_period_type,
                            day_number=day_number,
                        )
                    except Exception as e:
                        logger.error(
                            "Failed to mark reminder posted for %s: %s", chat_id, e
                        )
                        should_send = False
                    if not should_send:
                        logger.info(
                            "Reminder already posted, skipping (chat=%s date=%s season=%s section=%s period=%s day=%s source=%s)",
                            chat_id,
                            reminder_date.isoformat(),
                            resolved_season_id,
                            resolved_section_index,
                            effective_period_type,
                            day_number,
                            resolved_by,
                        )
                        continue
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

            if debug_chat_id is None:
                await set_app_state(
                    LAST_WAR_REMINDER_KEY,
                    {
                        "clan_tag": CLAN_TAG,
                        "season_id": resolved_season_id,
                        "section_index": resolved_section_index,
                        "period_type": effective_period_type,
                        "day_number": day_number,
                        "sent_at": datetime.now(timezone.utc).isoformat(),
                    },
                    session=session,
                )

            logger.info(
                "Posted daily reminder day %s (%s) for season %s section %s to %s chat(s)",
                day_number,
                effective_period_type,
                resolved_season_id,
                resolved_section_index,
                sent_count,
            )
            if debug_chat_id is not None:
                return debug_summary
    return None


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

    report = await build_promotion_candidates_report(
        CLAN_TAG, lang=DEFAULT_LANG
    )
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
    late_grace = timedelta(hours=6)
    logger.info("Daily reminder scheduler started at %02d:%02d UTC", hour, minute)

    while True:
        try:
            now = datetime.now(timezone.utc)
            target = datetime(
                now.year, now.month, now.day, hour, minute, tzinfo=timezone.utc
            )
            if now < target:
                await asyncio.sleep((target - now).total_seconds())
                now = datetime.now(timezone.utc)
            if now > target + late_grace:
                logger.info(
                    "Daily reminder skipped (late by %s, target=%s)",
                    now - target,
                    target.isoformat(),
                )
                next_target = target + timedelta(days=1)
                sleep_for = (next_target - now).total_seconds()
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)
                continue
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


async def maybe_auto_invite(bot: Bot) -> None:
    if not AUTO_INVITE_ENABLED:
        return
    if not CLAN_TAG:
        return
    try:
        api_client = await get_api_client()
        members = await api_client.get_clan_members(CLAN_TAG)
    except ClashRoyaleAPIError as e:
        logger.info("Auto-invite skipped: CR API error: %s", e)
        return
    except Exception as e:
        logger.info("Auto-invite skipped: API error: %s", e)
        return

    if not isinstance(members, list):
        return
    member_tags = {
        _normalize_clan_tag(member.get("tag", ""))
        for member in members
        if isinstance(member, dict) and member.get("tag")
    }
    now = datetime.now(timezone.utc)

    invited = await list_invited_applications()
    for app in invited:
        tag = app.get("player_tag")
        if not tag:
            continue
        if _normalize_clan_tag(tag) in member_tags:
            await mark_application_joined(app["id"], now=now)
            await log_mod_action(
                chat_id=0,
                target_user_id=int(app["telegram_user_id"]),
                admin_user_id=0,
                action="joined",
                reason="auto_invite",
            )
            await _send_modlog(
                bot,
                t(
                    "modlog_auto_joined",
                    DEFAULT_LANG,
                    app_id=app["id"],
                    user_id=app["telegram_user_id"],
                ),
            )
            continue
        invite_expires_at = app.get("invite_expires_at")
        if isinstance(invite_expires_at, datetime):
            if invite_expires_at.tzinfo is None:
                invite_expires_at = invite_expires_at.replace(tzinfo=timezone.utc)
            if invite_expires_at < now:
                exhausted = int(app.get("notify_attempts") or 0) >= AUTO_INVITE_MAX_ATTEMPTS
                await reset_expired_invite(
                    app["id"], now=now, exhausted=exhausted
                )

    if len(members) >= 50:
        return

    candidates = await list_invite_candidates(
        max_attempts=AUTO_INVITE_MAX_ATTEMPTS,
        limit=AUTO_INVITE_BATCH_SIZE,
    )
    for app in candidates:
        tag = app.get("player_tag")
        if not tag:
            continue
        if _normalize_clan_tag(tag) in member_tags:
            await mark_application_joined(app["id"], now=now)
            await log_mod_action(
                chat_id=0,
                target_user_id=int(app["telegram_user_id"]),
                admin_user_id=0,
                action="joined",
                reason="auto_invite",
            )
            await _send_modlog(
                bot,
                t(
                    "modlog_auto_joined",
                    DEFAULT_LANG,
                    app_id=app["id"],
                    user_id=app["telegram_user_id"],
                ),
            )
            continue

        invite_expires_at = now + timedelta(minutes=AUTO_INVITE_INVITE_MINUTES)
        text = t(
            "auto_invite_message",
            DEFAULT_LANG,
            clan_tag=f"#{_normalize_clan_tag(CLAN_TAG)}",
            minutes=AUTO_INVITE_INVITE_MINUTES,
        )
        try:
            await bot.send_message(
                int(app["telegram_user_id"]),
                text,
                parse_mode=None,
            )
        except Exception as e:
            logger.info("Auto-invite DM failed for %s: %s", app["id"], e)
            continue

        await mark_application_invited(
            app["id"], now=now, invite_expires_at=invite_expires_at
        )
        await log_mod_action(
            chat_id=0,
            target_user_id=int(app["telegram_user_id"]),
            admin_user_id=0,
            action="auto_invite",
            reason=f"invite_until={invite_expires_at.isoformat()}",
        )
        await _send_modlog(
            bot,
            t(
                "modlog_auto_invite_sent",
                DEFAULT_LANG,
                app_id=app["id"],
                user_id=app["telegram_user_id"],
            ),
        )


async def auto_invite_task(bot: Bot) -> None:
    interval_seconds = AUTO_INVITE_CHECK_INTERVAL_MINUTES * 60
    if interval_seconds <= 0:
        return
    logger.info(
        "Auto-invite task started (interval %sm)",
        AUTO_INVITE_CHECK_INTERVAL_MINUTES,
    )
    while True:
        try:
            await maybe_auto_invite(bot)
            await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            logger.info("Auto-invite task cancelled")
            break
        except Exception as e:
            logger.error("Error in auto-invite task: %s", e, exc_info=True)


async def scheduled_unmute_task(bot: Bot) -> None:
    logger.info("Scheduled unmute notification task started")
    while True:
        try:
            now = datetime.now(timezone.utc)
            due = await list_due_scheduled_unmutes(limit=100)
            for item in due:
                chat_id = int(item["chat_id"])
                user_id = int(item["user_id"])
                try:
                    member = await bot.get_chat_member(chat_id, user_id)
                except Exception as e:
                    logger.warning(
                        "Unmute notify failed: chat=%s user=%s err=%s",
                        chat_id,
                        user_id,
                        type(e).__name__,
                    )
                    continue

                if member.status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
                    logger.warning(
                        "Unmute notify skipped (left): chat=%s user=%s",
                        chat_id,
                        user_id,
                    )
                    continue

                user = member.user
                if user.username:
                    label = f"@{user.username}"
                else:
                    label = f"{user.full_name} ({user.id})"
                try:
                    await bot.send_message(
                        chat_id,
                        t("scheduled_unmute_notice", DEFAULT_LANG, user=label),
                        parse_mode=None,
                    )
                    logger.info(
                        "Unmute notify sent: chat=%s user=%s", chat_id, user_id
                    )
                    await mark_scheduled_unmute_sent(item["id"], sent_at=now)
                except Exception as e:
                    logger.warning(
                        "Unmute notify failed: chat=%s user=%s err=%s",
                        chat_id,
                        user_id,
                        type(e).__name__,
                    )
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            logger.info("Scheduled unmute task cancelled")
            break
        except Exception as e:
            logger.warning("Scheduled unmute task error: %s", e, exc_info=True)
            await asyncio.sleep(30)


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
    invite_task = None
    unmute_task = None
    if REMINDER_ENABLED:
        reminder_task = asyncio.create_task(daily_reminder_task(BOT))
    if AUTO_INVITE_ENABLED:
        invite_task = asyncio.create_task(auto_invite_task(BOT))
    unmute_task = asyncio.create_task(scheduled_unmute_task(BOT))
    logger.info("Scheduled unmute notification task started")
    logger.info("Background fetch task started")
    
    yield
    
    # Shutdown
    logger.info("Shutting down bot...")
    
    # Cancel background task
    fetch_task.cancel()
    if reminder_task is not None:
        reminder_task.cancel()
    if invite_task is not None:
        invite_task.cancel()
    if unmute_task is not None:
        unmute_task.cancel()
    try:
        await fetch_task
    except asyncio.CancelledError:
        pass
    if reminder_task is not None:
        try:
            await reminder_task
        except asyncio.CancelledError:
            pass
    if invite_task is not None:
        try:
            await invite_task
        except asyncio.CancelledError:
            pass
    if unmute_task is not None:
        try:
            await unmute_task
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
        default=DefaultBotProperties(parse_mode=None),
    )
    global BOT
    BOT = bot
    
    # Create dispatcher
    dp = Dispatcher()
    
    # Register router with handlers
    dp.include_router(moderation_router)
    dp.include_router(router)
    
    # Run bot with lifespan management
    async with lifespan(dp):
        logger.info("Bot is starting polling...")
        await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
