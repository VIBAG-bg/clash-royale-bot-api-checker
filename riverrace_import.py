"""Helpers for importing River Race log data."""

import logging
from typing import Any

from config import CLAN_TAG
from cr_api import get_api_client, ClashRoyaleAPIError
from db import (
    get_colosseum_index_map,
    get_last_completed_weeks_from_db,
    get_session,
    save_player_participation,
    save_river_race_state,
)

logger = logging.getLogger(__name__)


def _normalize_tag(tag: str) -> str:
    return tag[1:] if tag.startswith("#") else tag


def _tags_match(a: str, b: str) -> bool:
    return _normalize_tag(a).upper() == _normalize_tag(b).upper()


def _find_clan_entry(standings: list[dict[str, Any]], clan_tag: str) -> dict[str, Any] | None:
    for standing in standings:
        clan = standing.get("clan", {})
        tag = clan.get("tag", "")
        if tag and _tags_match(tag, clan_tag):
            return clan
    return None


def _resolve_is_colosseum(
    item: dict[str, Any], season_id: int, section_index: int, colosseum_map: dict[int, int]
) -> bool:
    if "isColosseum" in item:
        return bool(item.get("isColosseum"))
    mapped_index = colosseum_map.get(season_id)
    if mapped_index is not None:
        return section_index == mapped_index
    return False


async def get_latest_riverrace_log_info(clan_tag: str | None = None) -> dict[str, Any] | None:
    target_tag = clan_tag or CLAN_TAG
    api_client = await get_api_client()
    items = await api_client.get_river_race_log(target_tag)
    if isinstance(items, dict):
        items = items.get("items", [])
    if not items:
        return None
    item = items[0]
    season_id = item.get("seasonId", 0)
    section_index = item.get("sectionIndex", 0)
    period_type = (item.get("periodType") or "completed").lower()
    return {
        "season_id": season_id,
        "section_index": section_index,
        "period_type": period_type,
    }


async def import_riverrace_log(
    weeks: int,
    clan_tag: str | None = None,
    *,
    season_id: int | None = None,
    section_index: int | None = None,
) -> tuple[int, int]:
    target_tag = clan_tag or CLAN_TAG
    api_client = await get_api_client()
    items = await api_client.get_river_race_log(target_tag)
    if isinstance(items, dict):
        items = items.get("items", [])
    if not items:
        return 0, 0

    if season_id is not None and section_index is not None:
        target_season = int(season_id)
        target_section = int(section_index)
        matching_item = None
        for item in items:
            item_season = int(item.get("seasonId", 0) or 0)
            item_section = int(item.get("sectionIndex", 0) or 0)
            if item_season == target_season and item_section == target_section:
                matching_item = item
                break
        if not matching_item:
            logger.warning(
                "No River Race log entry found for season=%s section=%s",
                target_season,
                target_section,
            )
            return 0, 0
        items = [matching_item]
    else:
        items = items[: max(0, weeks)]
    weeks_imported = 0
    players_imported = 0

    async with get_session() as session:
        try:
            colosseum_map = await get_colosseum_index_map(session=session)
            for item in items:
                standings = item.get("standings", [])
                clan = _find_clan_entry(standings, target_tag)
                if not clan:
                    continue

                season_id = item.get("seasonId", 0)
                section_index = item.get("sectionIndex", 0)
                if season_id <= 0:
                    continue

                is_colosseum = _resolve_is_colosseum(
                    item, season_id, section_index, colosseum_map
                )
                period_type = (item.get("periodType") or "completed").lower()
                clan_score = clan.get("fame", 0)

                await save_river_race_state(
                    clan_tag=target_tag,
                    season_id=season_id,
                    section_index=section_index,
                    is_colosseum=is_colosseum,
                    period_type=period_type,
                    clan_score=clan_score,
                    session=session,
                )

                participants = clan.get("participants", [])
                for participant in participants:
                    player_tag = participant.get("tag", "")
                    if not player_tag:
                        continue
                    await save_player_participation(
                        player_tag=player_tag,
                        player_name=participant.get("name", "Unknown"),
                        season_id=season_id,
                        section_index=section_index,
                        is_colosseum=is_colosseum,
                        fame=participant.get("fame", 0),
                        repair_points=participant.get("repairPoints", 0),
                        boat_attacks=participant.get("boatAttacks", 0),
                        decks_used=participant.get("decksUsed", 0),
                        decks_used_today=participant.get("decksUsedToday", 0),
                        session=session,
                    )
                    players_imported += 1

                weeks_imported += 1

            await session.commit()
        except Exception:
            await session.rollback()
            raise

    logger.info(
        "Imported %s week(s) and %s player record(s) from River Race log",
        weeks_imported,
        players_imported,
    )
    return weeks_imported, players_imported


async def get_last_completed_weeks(
    count: int, clan_tag: str | None = None
) -> list[tuple[int, int]]:
    target_tag = clan_tag or CLAN_TAG
    api_client = await get_api_client()
    try:
        items = await api_client.get_river_race_log(target_tag)
    except ClashRoyaleAPIError as e:
        logger.warning("River race log unavailable, falling back to DB: %s", e)
        return await get_last_completed_weeks_from_db(target_tag, limit=count)
    except Exception as e:
        logger.warning(
            "River race log unavailable, falling back to DB: %s",
            e,
            exc_info=True,
        )
        return await get_last_completed_weeks_from_db(target_tag, limit=count)
    if isinstance(items, dict):
        items = items.get("items", [])
    weeks: list[tuple[int, int]] = []
    for item in items[: max(0, count)]:
        season_id = item.get("seasonId", 0)
        section_index = item.get("sectionIndex", 0)
        if season_id <= 0:
            continue
        if section_index is None:
            continue
        weeks.append((int(season_id), int(section_index)))
    return weeks


async def get_last_completed_week(
    clan_tag: str | None = None,
) -> tuple[int, int] | None:
    weeks = await get_last_completed_weeks(1, clan_tag=clan_tag)
    return weeks[0] if weeks else None
