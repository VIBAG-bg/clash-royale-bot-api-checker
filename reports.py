"""Report builders for weekly and rolling war summaries."""

from datetime import date, datetime, timedelta, timezone
from typing import Iterable

from statistics import median

from sqlalchemy import func, select, tuple_

import logging

from cr_api import ClashRoyaleAPIError, get_api_client

from config import (
    DONATION_REVIVE_WTD_THRESHOLD,
    DONATION_WEEKS_WINDOW,
    DONATION_BOX_THRESHOLD,
    KICK_COLOSSEUM_SAVE_DECKS,
    KICK_COLOSSEUM_SAVE_FAME,
    KICK_SHORTLIST_LIMIT,
    LAST_SEEN_FLAG_LIMIT,
    LAST_SEEN_RED_DAYS,
    LAST_SEEN_YELLOW_DAYS,
    RANKING_LOCATION_ID,
    RANKING_NEIGHBORS_ABOVE,
    RANKING_NEIGHBORS_BELOW,
    RANKING_SNAPSHOT_ENABLED,
    RANKING_SNAPSHOT_LIMIT,
    RANKING_SNAPSHOT_MIN_INTERVAL_HOURS,
    PROMOTE_COLEADER_LIMIT,
    PROMOTE_ELDER_LIMIT,
    PROMOTE_MIN_ACTIVE_WEEKS_COLEADER,
    PROMOTE_MIN_ACTIVE_WEEKS_ELDER,
    PROMOTE_MIN_ALLTIME_WEEKS_COLEADER,
    PROMOTE_MIN_AVG_DECKS_COLEADER,
    PROMOTE_MIN_AVG_DECKS_ELDER,
    PROMOTE_MIN_WEEKS_PLAYED_COLEADER,
    PROMOTE_MIN_WEEKS_PLAYED_ELDER,
    NEW_MEMBER_WEEKS_PLAYED,
    PROTECTED_PLAYER_TAGS,
    REVIVED_DECKS_THRESHOLD,
)
from db import (
    ClanMemberDaily,
    PlayerParticipation,
    RiverRaceState,
    get_app_state,
    get_current_member_tags,
    get_current_members_snapshot,
    get_clan_wtd_donation_average,
    get_current_wtd_donations,
    get_current_members_with_wtd_donations,
    get_clan_rank_snapshot_at_or_before,
    get_donation_weekly_sums_for_window,
    get_alltime_weeks_played,
    get_first_snapshot_date_for_week,
    get_last_weeks_from_db,
    get_last_seen_map,
    get_latest_clan_rank_snapshot,
    get_latest_river_race_state,
    get_latest_river_race_place_snapshot,
    get_participation_week_counts,
    get_river_race_state_for_week,
    get_donations_weekly_sums,
    get_war_stats_for_weeks,
    get_rolling_leaderboard,
    get_rolling_summary,
    get_session,
    get_top_donors_window,
    get_top_donors_wtd,
    get_week_decks_map,
    get_week_leaderboard,
    insert_clan_rank_snapshot,
    save_river_race_place_snapshot,
)
from riverrace_import import get_last_completed_weeks
from i18n import DEFAULT_LANG, t

NAME_WIDTH = 20
HEADER_LINE = "══════════════════════════════"
DIVIDER_LINE = "──────────────────────────────"
SEPARATOR_LINE = "---------------------------"

logger = logging.getLogger(__name__)

KICK_COLOSSEUM_D0_OVERRIDES = {
    (125, 3): date(2025, 10, 31),
    (126, 3): date(2025, 11, 28),
}


def _normalize_tag(raw_tag: object) -> str:
    tag = str(raw_tag).strip() if raw_tag else ""
    if not tag:
        return ""
    if not tag.startswith("#"):
        tag = f"#{tag}"
    return tag.upper()


PROTECTED_TAGS_NORMALIZED = {
    _normalize_tag(tag) for tag in PROTECTED_PLAYER_TAGS if _normalize_tag(tag)
}


def _filter_protected(entries: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    if not PROTECTED_TAGS_NORMALIZED:
        return list(entries)
    filtered: list[dict[str, object]] = []
    for row in entries:
        tag = _normalize_tag(row.get("player_tag"))
        if tag and tag in PROTECTED_TAGS_NORMALIZED:
            continue
        filtered.append(row)
    return filtered


def _format_name(raw_name: object, lang: str = DEFAULT_LANG) -> str:
    name = str(raw_name) if raw_name else t("unknown", lang)
    if len(name) > NAME_WIDTH:
        name = f"{name[:NAME_WIDTH - 1]}…"
    return name.ljust(NAME_WIDTH)


def _coerce_int(value: object) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _format_entries(
    entries: Iterable[dict[str, object]],
    donations_wtd: dict[str, dict[str, int | None]] | None = None,
    *,
    lang: str = DEFAULT_LANG,
) -> list[str]:
    rows = list(entries)
    if not rows:
        return [t("report_no_data", lang)]

    decks_width = max(len(str(int(row.get("decks_used", 0)))) for row in rows)
    fame_width = max(len(str(int(row.get("fame", 0)))) for row in rows)
    decks_width = max(decks_width, 2)
    fame_width = max(fame_width, 2)

    lines: list[str] = []
    for index, row in enumerate(rows, 1):
        name = _format_name(row.get("player_name"), lang)
        decks_used = int(row.get("decks_used", 0))
        fame = int(row.get("fame", 0))
        suffix = ""
        if donations_wtd is not None:
            suffix = _format_donation_suffix(
                row.get("player_tag"), donations_wtd, lang=lang
            )
        lines.append(
            t(
                "report_entry_line",
                lang,
                index=index,
                name=name,
                decks=decks_used,
                decks_width=decks_width,
                fame=fame,
                fame_width=fame_width,
                suffix=suffix,
            )
        )
    return lines


def _format_donation_suffix(
    player_tag: object,
    donations_wtd: dict[str, dict[str, int | None]] | None,
    *,
    lang: str = DEFAULT_LANG,
) -> str:
    tag = _normalize_tag(player_tag)
    wtd_value: int | None = None
    if donations_wtd and tag in donations_wtd:
        wtd_value = donations_wtd[tag].get("donations")
    wtd_text = (
        str(wtd_value) if wtd_value is not None else t("na", lang)
    )
    return t("report_donate_suffix", lang, value=wtd_text)


async def _collect_wtd_donations(
    clan_tag: str,
    tags: set[str],
) -> dict[str, dict[str, int | None]]:
    if not tags:
        return {}
    normalized_tags = {_normalize_tag(tag) for tag in tags if tag}
    return await get_current_wtd_donations(clan_tag, player_tags=normalized_tags)


async def _build_top_donors_wtd_block(
    clan_tag: str, *, lang: str = DEFAULT_LANG
) -> list[str]:
    donors = await get_top_donors_wtd(clan_tag, limit=10)
    donors = _filter_protected(donors)[:5]
    lines = [t("donors_wtd_header", lang)]
    if donors:
        for index, row in enumerate(donors, 1):
            name = (
                row.get("player_name")
                or row.get("player_tag")
                or t("unknown", lang)
            )
            donations = row.get("donations")
            donations_text = (
                str(donations) if donations is not None else t("na", lang)
            )
            lines.append(
                t(
                    "donors_wtd_line",
                    lang,
                    index=index,
                    name=name,
                    donations=donations_text,
                )
            )
    else:
        lines.append(t("donors_wtd_none", lang))
    return lines


async def _build_top_donors_window_block(
    clan_tag: str, window_weeks: int, *, lang: str = DEFAULT_LANG
) -> list[str]:
    donors = await get_top_donors_window(
        clan_tag, window_weeks=window_weeks, limit=10
    )
    donors = _filter_protected(donors)[:5]
    lines = [t("donors_window_header", lang, weeks=window_weeks)]
    if donors:
        for index, row in enumerate(donors, 1):
            name = (
                row.get("player_name")
                or row.get("player_tag")
                or t("unknown", lang)
            )
            donations_sum = row.get("donations_sum")
            weeks_present = row.get("weeks_present")
            donations_text = str(donations_sum) if donations_sum is not None else "0"
            weeks_text = (
                f"{weeks_present}/{window_weeks}"
                if weeks_present is not None
                else f"0/{window_weeks}"
            )
            lines.append(
                t(
                    "donors_window_line",
                    lang,
                    index=index,
                    name=name,
                    donations=donations_text,
                    weeks=weeks_text,
                )
            )
    else:
        lines.append(t("donors_window_none", lang))
    return lines


def _coerce_int(value: object) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _format_avg(value: float) -> str:
    return f"{value:.1f}"


def _format_median(values: list[int], lang: str = DEFAULT_LANG) -> str:
    if not values:
        return t("na", lang)
    return f"{median(values):.1f}"


def _parse_last_seen_string(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if not value or not isinstance(value, str):
        return None
    for fmt in ("%Y%m%dT%H%M%S.%fZ", "%Y%m%dT%H%M%SZ"):
        try:
            parsed = datetime.strptime(value, fmt)
        except ValueError:
            continue
        return parsed.replace(tzinfo=timezone.utc)
    return None


def _format_relative(delta: timedelta, lang: str = DEFAULT_LANG) -> str:
    total_seconds = int(delta.total_seconds())
    if total_seconds < 0:
        total_seconds = 0
    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    if days > 0:
        if hours > 0:
            return t("relative_days_hours", lang, days=days, hours=hours)
        return t("relative_days", lang, days=days)
    if hours > 0:
        return t("relative_hours", lang, hours=hours)
    return t("relative_minutes", lang, minutes=minutes)


def _compare_to_avg(value: float, avg: float, lang: str = DEFAULT_LANG) -> str:
    if avg <= 0:
        return t("compare_near", lang)
    ratio = (value - avg) / avg
    if ratio >= 0.05:
        return t("compare_above", lang)
    if ratio <= -0.05:
        return t("compare_below", lang)
    return t("compare_near", lang)


def _compare_simple(value: int, avg: int, lang: str = DEFAULT_LANG) -> str:
    if value > avg:
        return t("compare_above", lang)
    if value < avg:
        return t("compare_below", lang)
    return t("compare_equal", lang)


def _days_absent(last_seen: datetime | None, now: datetime) -> int | None:
    if last_seen is None:
        return None
    if last_seen.tzinfo is None:
        last_seen = last_seen.replace(tzinfo=timezone.utc)
    delta = now - last_seen
    if delta.total_seconds() < 0:
        return 0
    return delta.days


def _absence_flag(days_absent: int | None) -> str:
    if days_absent is None:
        return ""
    if days_absent >= LAST_SEEN_RED_DAYS:
        return "🔴"
    if days_absent >= LAST_SEEN_YELLOW_DAYS:
        return "🟡"
    return ""


def _absence_label(days_absent: int | None, lang: str = DEFAULT_LANG) -> str:
    if days_absent is None:
        return t("absence_no_data", lang)
    if days_absent >= LAST_SEEN_RED_DAYS:
        return t("absence_red", lang)
    if days_absent >= LAST_SEEN_YELLOW_DAYS:
        return t("absence_yellow", lang)
    return t("absence_ok", lang)


def _format_kick_v2_value(value: object, lang: str) -> str:
    if value is None:
        return t("kick_v2_value_na", lang)
    return str(value)


def _format_kick_v2_date(value: date | None, lang: str) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return t("kick_v2_value_na", lang)


def _format_kick_v2_last_seen(
    last_seen: datetime | None, now: datetime, lang: str
) -> str:
    if not isinstance(last_seen, datetime):
        return t("kick_v2_value_na", lang)
    if last_seen.tzinfo is None:
        last_seen = last_seen.replace(tzinfo=timezone.utc)
    return _format_relative(now - last_seen, lang)


async def _resolve_active_week_key(
    clan_tag: str,
) -> tuple[int, int] | None:
    state = await get_app_state("active_week")
    if isinstance(state, dict):
        season_id = _coerce_int(state.get("season_id"))
        section_index = _coerce_int(state.get("section_index"))
        if season_id and section_index is not None and section_index >= 0:
            return season_id, section_index
    latest = await get_latest_river_race_state(clan_tag)
    if latest:
        return int(latest["season_id"]), int(latest["section_index"])
    return None


def _extract_river_race_entries(
    river_race: dict[str, object],
) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    standings = river_race.get("standings")
    if isinstance(standings, list):
        for entry in standings:
            if not isinstance(entry, dict):
                continue
            clan = entry.get("clan")
            clan_data = clan if isinstance(clan, dict) else {}
            tag = clan_data.get("tag") or entry.get("tag")
            tag = _normalize_tag(tag)
            if not tag:
                continue
            name = clan_data.get("name") or entry.get("name") or tag
            fame = _coerce_int(clan_data.get("fame"))
            if fame is None:
                fame = _coerce_int(entry.get("fame")) or 0
            rank = _coerce_int(entry.get("rank"))
            if rank is None:
                rank = _coerce_int(clan_data.get("rank"))
            entries.append(
                {
                    "rank": rank,
                    "tag": tag,
                    "name": name,
                    "fame": int(fame),
                }
            )
    if entries:
        return entries
    clans = river_race.get("clans")
    if isinstance(clans, list):
        for clan in clans:
            if not isinstance(clan, dict):
                continue
            tag = _normalize_tag(clan.get("tag"))
            if not tag:
                continue
            name = clan.get("name") or tag
            fame = _coerce_int(clan.get("fame")) or 0
            rank = _coerce_int(clan.get("rank"))
            entries.append(
                {
                    "rank": rank,
                    "tag": tag,
                    "name": name,
                    "fame": int(fame),
                }
            )
    return entries


def _extract_total_from_top5(top5: list[dict[str, object]], our_rank: int) -> int:
    if top5 and isinstance(top5[0], dict):
        total = _coerce_int(top5[0].get("total"))
        if total is not None and total > 0:
            return total
    return max(len(top5), our_rank)


async def capture_clan_place_snapshot(
    clan_tag: str,
) -> dict[str, object] | None:
    clan_key = _normalize_tag(clan_tag)
    active_week = await _resolve_active_week_key(clan_key)
    if not active_week:
        return None
    season_id, section_index = active_week
    api_client = await get_api_client()
    try:
        river_race = await api_client.get_current_river_race(clan_key)
    except ClashRoyaleAPIError as e:
        logger.warning("Failed to fetch current river race: %s", e)
        return None
    except Exception as e:
        logger.warning("Failed to fetch current river race: %s", e, exc_info=True)
        return None
    if not isinstance(river_race, dict):
        return None
    entries = _extract_river_race_entries(river_race)
    if not entries:
        return None

    use_rank = all(entry.get("rank") is not None for entry in entries)
    if use_rank:
        entries_sorted = sorted(entries, key=lambda row: int(row["rank"]))
        for entry in entries_sorted:
            entry["rank"] = int(entry["rank"])
    else:
        entries_sorted = sorted(entries, key=lambda row: row["fame"], reverse=True)
        for index, entry in enumerate(entries_sorted, 1):
            entry["rank"] = index

    total_clans = len(entries_sorted)
    our_entry = next(
        (entry for entry in entries_sorted if entry.get("tag") == clan_key),
        None,
    )
    if not our_entry:
        return None
    our_rank = int(our_entry["rank"])
    our_fame = int(our_entry["fame"])
    above_entry = None
    if our_rank > 1:
        above_entry = next(
            (entry for entry in entries_sorted if entry.get("rank") == our_rank - 1),
            None,
        )
    above_rank = int(above_entry["rank"]) if above_entry else None
    above_fame = int(above_entry["fame"]) if above_entry else None
    gap_to_above = (
        int(above_fame - our_fame) if above_fame is not None else None
    )

    top_entries = entries_sorted[:5]
    top5_json = [
        {
            "rank": int(entry["rank"]),
            "tag": entry["tag"],
            "name": entry["name"],
            "fame": int(entry["fame"]),
            "total": total_clans,
        }
        for entry in top_entries
    ]

    await save_river_race_place_snapshot(
        clan_tag=clan_key,
        season_id=season_id,
        section_index=section_index,
        our_rank=our_rank,
        our_fame=our_fame,
        above_rank=above_rank,
        above_fame=above_fame,
        gap_to_above=gap_to_above,
        top5_json=top5_json,
    )
    period_type = str(river_race.get("periodType") or "unknown").lower()
    logger.info(
        "Captured clan place snapshot: season=%s section=%s rank=%s fame=%s gap=%s total=%s",
        season_id,
        section_index,
        our_rank,
        our_fame,
        gap_to_above,
        total_clans,
    )
    return {
        "season_id": season_id,
        "section_index": section_index,
        "period_type": period_type,
        "our_rank": our_rank,
        "our_fame": our_fame,
        "above_rank": above_rank,
        "above_fame": above_fame,
        "gap_to_above": gap_to_above,
        "top5_json": top5_json,
        "total_clans": total_clans,
    }


async def build_clan_place_report(
    clan_tag: str,
    *,
    lang: str = DEFAULT_LANG,
) -> str:
    clan_key = _normalize_tag(clan_tag)
    active_week = await _resolve_active_week_key(clan_tag)
    if not active_week:
        return t("clan_place_no_data", lang)
    season_id, section_index = active_week
    snapshot = await get_latest_river_race_place_snapshot(
        clan_key, season_id, section_index
    )
    if snapshot:
        snapshot_ts = snapshot.get("snapshot_ts")
        if isinstance(snapshot_ts, datetime):
            age = datetime.now(timezone.utc) - snapshot_ts
            if age > timedelta(minutes=10):
                snapshot = None
    if not snapshot:
        snapshot = await capture_clan_place_snapshot(clan_key)
    if not snapshot:
        return t("clan_place_no_data", lang)

    top5 = snapshot.get("top5_json") or []
    if not isinstance(top5, list):
        top5 = []
    top5 = [
        row
        for row in top5
        if isinstance(row, dict) and row.get("rank") is not None
    ]
    top5_sorted = sorted(top5, key=lambda row: int(row.get("rank", 0)))

    our_rank = int(snapshot.get("our_rank") or 0)
    our_fame = int(snapshot.get("our_fame") or 0)
    total_clans = snapshot.get("total_clans")
    if total_clans is None:
        total_clans = _extract_total_from_top5(top5_sorted, our_rank)
    period_type = snapshot.get("period_type") or ""
    state = await get_river_race_state_for_week(
        clan_tag, season_id, section_index
    )
    if state:
        is_colosseum = bool(state.get("is_colosseum"))
    else:
        is_colosseum = str(period_type).lower() == "colosseum"
    period_label = (
        t("current_war_colosseum", lang)
        if is_colosseum
        else t("current_war_river_race", lang)
    )

    lines = [
        t("clan_place_title", lang),
        t("clan_place_clan_line", lang, clan=clan_key),
        t(
            "clan_place_week_line",
            lang,
            season=season_id,
            week=section_index + 1,
            period=period_label,
        ),
        t("clan_place_rank_line", lang, rank=our_rank, total=total_clans),
        t("clan_place_fame_line", lang, fame=our_fame),
        "",
        t("clan_place_top_header", lang),
    ]
    for entry in top5_sorted:
        marker = " \u2190" if entry.get("tag") == clan_key else ""
        lines.append(
            t(
                "clan_place_top_line",
                lang,
                rank=entry.get("rank"),
                name=entry.get("name") or entry.get("tag") or t("unknown", lang),
                fame=entry.get("fame") or 0,
                marker=marker,
            )
        )

    gap_to_above = snapshot.get("gap_to_above")
    if our_rank > 1 and gap_to_above is not None:
        lines.extend(
            [
                "",
                t(
                    "clan_place_gap_line",
                    lang,
                    rank=our_rank - 1,
                    gap=int(gap_to_above),
                ),
            ]
        )

    return "\n".join(lines)


def _extract_rank_items(items: object) -> list[dict[str, object]]:
    if isinstance(items, dict):
        items = items.get("items", [])
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _extract_rank_score(item: dict[str, object]) -> int | None:
    return _coerce_int(
        item.get("clanScore")
        or item.get("clanWarTrophies")
        or item.get("score")
    )


def _find_rank_entry(
    items: list[dict[str, object]],
    clan_tag: str,
) -> tuple[int | None, dict[str, object] | None]:
    normalized = _normalize_tag(clan_tag)
    for idx, item in enumerate(items):
        if _normalize_tag(item.get("tag")) == normalized:
            return idx, item
    return None, None


def _build_neighbors_window(
    items: list[dict[str, object]],
    idx: int | None,
    clan_tag: str,
    neighbors_above: int,
    neighbors_below: int,
) -> tuple[list[dict[str, object]], int | None, int | None]:
    if not items:
        return [], None, None

    normalized = _normalize_tag(clan_tag)
    if idx is None:
        window = items[: min(10, len(items))]
        neighbors: list[dict[str, object]] = []
        for item in window:
            score = _extract_rank_score(item)
            neighbors.append(
                {
                    "rank": _coerce_int(item.get("rank")),
                    "tag": _normalize_tag(item.get("tag")),
                    "name": item.get("name"),
                    "score": score,
                    "delta": None,
                    "is_us": False,
                }
            )
        return neighbors, None, None

    start = max(0, idx - neighbors_above)
    end = min(len(items), idx + neighbors_below + 1)
    window = items[start:end]
    our_item = items[idx]
    our_score = _extract_rank_score(our_item)
    neighbors = []
    for item in window:
        score = _extract_rank_score(item)
        delta = (
            score - our_score
            if score is not None and our_score is not None
            else None
        )
        neighbors.append(
            {
                "rank": _coerce_int(item.get("rank")),
                "tag": _normalize_tag(item.get("tag")),
                "name": item.get("name"),
                "score": score,
                "delta": delta,
                "is_us": _normalize_tag(item.get("tag")) == normalized,
            }
        )
    points_to_overtake = None
    if our_score is not None:
        our_pos = idx - start
        if our_pos > 0:
            above_score = _extract_rank_score(window[our_pos - 1])
            if above_score is not None:
                points_to_overtake = max(0, above_score - our_score + 1)
    return neighbors, points_to_overtake, our_score


def _is_snapshot_fresh(snapshot: dict[str, object]) -> bool:
    snapshot_at = snapshot.get("snapshot_at")
    if not isinstance(snapshot_at, datetime):
        return False
    if snapshot_at.tzinfo is None:
        snapshot_at = snapshot_at.replace(tzinfo=timezone.utc)
    return snapshot_at >= datetime.now(timezone.utc) - timedelta(
        hours=RANKING_SNAPSHOT_MIN_INTERVAL_HOURS
    )


def _needs_war_refresh(snapshot: dict[str, object]) -> bool:
    if _coerce_int(snapshot.get("war_rank")) is not None:
        return False
    raw_source = snapshot.get("raw_source")
    limit = None
    if isinstance(raw_source, dict):
        limit = _coerce_int(raw_source.get("war_limit_used"))
        if limit is None:
            limit = _coerce_int(raw_source.get("limit"))
    return limit is not None and limit < 1000


async def collect_clan_rank_snapshot(
    clan_tag: str,
    *,
    force: bool = False,
) -> dict[str, object] | None:
    if not RANKING_SNAPSHOT_ENABLED:
        return None

    clan_key = _normalize_tag(clan_tag)
    location_id = RANKING_LOCATION_ID
    location_name = None
    latest = None
    if location_id is not None:
        latest = await get_latest_clan_rank_snapshot(clan_key, location_id)
        if latest and not force and _is_snapshot_fresh(latest):
            if not _needs_war_refresh(latest):
                return latest

    try:
        api_client = await get_api_client()
        clan_info = await api_client.get_clan(clan_tag)
    except ClashRoyaleAPIError as e:
        logger.warning("Rank snapshot skipped (clan info error): %s", e)
        if latest:
            return latest
        return None
    except Exception as e:
        logger.warning("Rank snapshot skipped (clan info failed): %s", e)
        if latest:
            return latest
        return None

    if not isinstance(clan_info, dict):
        return None

    if location_id is None:
        location = clan_info.get("location")
        if isinstance(location, dict):
            location_id = _coerce_int(location.get("id"))
            location_name = location.get("localizedName") or location.get("name")
        if location_id is None:
            logger.warning("Rank snapshot skipped: missing location id")
            return None
    else:
        location = clan_info.get("location")
        if isinstance(location, dict):
            location_name = location.get("localizedName") or location.get("name")

    if not force:
        latest = await get_latest_clan_rank_snapshot(clan_key, location_id)
        if latest and _is_snapshot_fresh(latest):
            if not _needs_war_refresh(latest):
                return latest

    clan_name = clan_info.get("name")
    clan_score = _coerce_int(clan_info.get("clanScore"))
    clan_war_trophies = _coerce_int(clan_info.get("clanWarTrophies"))
    members = _coerce_int(clan_info.get("members"))
    if clan_score is None or clan_war_trophies is None or members is None:
        logger.warning("Rank snapshot skipped: incomplete clan info")
        return None

    ladder_items: list[dict[str, object]] = []
    war_items: list[dict[str, object]] = []
    war_limit_used = RANKING_SNAPSHOT_LIMIT
    ladder_fetch_failed = False
    war_fetch_failed = False
    try:
        ladder_items = await api_client.get_location_clan_rankings(
            location_id,
            limit=RANKING_SNAPSHOT_LIMIT,
        )
    except ClashRoyaleAPIError as e:
        logger.warning("Ladder rankings fetch failed: %s", e)
        ladder_fetch_failed = True
    except Exception as e:
        logger.warning("Ladder rankings fetch failed: %s", e)
        ladder_fetch_failed = True
    try:
        war_items = await api_client.get_location_clanwar_rankings(
            location_id,
            limit=RANKING_SNAPSHOT_LIMIT,
        )
    except ClashRoyaleAPIError as e:
        logger.warning("War rankings fetch failed: %s", e)
        war_fetch_failed = True
    except Exception as e:
        logger.warning("War rankings fetch failed: %s", e)
        war_fetch_failed = True

    if not force and (ladder_fetch_failed or war_fetch_failed) and latest:
        logger.info("Rank snapshot fallback to latest due to ranking fetch failure")
        return latest

    ladder_items = _extract_rank_items(ladder_items)
    war_items = _extract_rank_items(war_items)

    ladder_idx, ladder_entry = _find_rank_entry(ladder_items, clan_key)
    war_idx, war_entry = _find_rank_entry(war_items, clan_key)
    if war_entry is None and RANKING_SNAPSHOT_LIMIT < 1000:
        try:
            war_items = await api_client.get_location_clanwar_rankings(
                location_id,
                limit=1000,
            )
            war_items = _extract_rank_items(war_items)
            war_idx, war_entry = _find_rank_entry(war_items, clan_key)
            war_limit_used = 1000
        except ClashRoyaleAPIError as e:
            logger.warning("War rankings fallback fetch failed: %s", e)
            war_limit_used = 1000
        except Exception as e:
            logger.warning("War rankings fallback fetch failed: %s", e)
            war_limit_used = 1000

    ladder_neighbors, ladder_points, ladder_list_score = _build_neighbors_window(
        ladder_items,
        ladder_idx,
        clan_key,
        RANKING_NEIGHBORS_ABOVE,
        RANKING_NEIGHBORS_BELOW,
    )
    war_neighbors, war_points, war_list_score = _build_neighbors_window(
        war_items,
        war_idx,
        clan_key,
        RANKING_NEIGHBORS_ABOVE,
        RANKING_NEIGHBORS_BELOW,
    )

    ladder_rank = _coerce_int(ladder_entry.get("rank")) if ladder_entry else None
    ladder_prev = (
        _coerce_int(ladder_entry.get("previousRank")) if ladder_entry else None
    )
    war_rank = _coerce_int(war_entry.get("rank")) if war_entry else None
    war_prev = (
        _coerce_int(war_entry.get("previousRank")) if war_entry else None
    )

    snapshot = {
        "clan_tag": clan_key,
        "location_id": location_id,
        "location_name": location_name,
        "snapshot_at": datetime.now(timezone.utc),
        "ladder_rank": ladder_rank,
        "ladder_previous_rank": ladder_prev,
        "ladder_clan_score": clan_score,
        "war_rank": war_rank,
        "war_previous_rank": war_prev,
        "war_clan_score": war_list_score,
        "clan_war_trophies": clan_war_trophies,
        "members": members,
        "neighbors_ladder_json": ladder_neighbors,
        "neighbors_war_json": war_neighbors,
        "ladder_points_to_overtake_above": ladder_points,
        "war_points_to_overtake_above": war_points,
        "raw_source": {
            "limit": RANKING_SNAPSHOT_LIMIT,
            "neighbors_above": RANKING_NEIGHBORS_ABOVE,
            "neighbors_below": RANKING_NEIGHBORS_BELOW,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "clan_name": clan_name,
            "ladder_found": ladder_rank is not None,
            "war_found": war_rank is not None,
            "war_limit_used": war_limit_used,
        },
    }
    await insert_clan_rank_snapshot(snapshot)
    return await get_latest_clan_rank_snapshot(clan_key, location_id)


def _format_rank_value(value: int | None, na_label: str) -> str:
    if value is None:
        return na_label
    return f"#{value}"


def _format_score_value(value: int | None, na_label: str) -> str:
    if value is None:
        return na_label
    return str(value)


def _format_delta(value: int | None, na_label: str) -> str:
    if value is None:
        return na_label
    if value > 0:
        return f"+{value}"
    return str(value)


def _calc_rank_delta(old: int | None, current: int | None) -> int | None:
    if old is None or current is None:
        return None
    return old - current


def _calc_score_delta(old: int | None, current: int | None) -> int | None:
    if old is None or current is None:
        return None
    return current - old


def _render_neighbors(
    neighbors: list[dict[str, object]] | None,
    *,
    header: str,
    lang: str,
    points_to_overtake: int | None,
    rank_value: int | None,
) -> list[str]:
    if not neighbors:
        return []

    rank_na = t("rank_na", lang)
    lines = [header]
    for entry in neighbors:
        name = entry.get("name") or t("unknown", lang)
        rank = entry.get("rank")
        score = entry.get("score")
        delta = entry.get("delta")
        is_us = bool(entry.get("is_us"))
        rank_text = f"#{rank}" if rank is not None else rank_na
        score_text = _format_score_value(_coerce_int(score), rank_na)
        if delta is None:
            line = f"{rank_text} {name} — {score_text}"
        else:
            delta_text = _format_delta(_coerce_int(delta), rank_na)
            prefix = "➡️ " if is_us else ""
            line = f"{prefix}{rank_text} {name} — {score_text} ({delta_text})"
        lines.append(t("rank_neighbor_line", lang, line=line))
    if points_to_overtake is not None:
        lines.append("")
        lines.append(
            t(
                "rank_neighbors_overtake_line",
                lang,
                points=points_to_overtake,
            )
        )
        lines.append("")
    if rank_value is None:
        lines.append(
            t(
                "rank_neighbors_fallback_note",
                lang,
                limit=RANKING_SNAPSHOT_LIMIT,
                shown=len(neighbors),
            )
        )
    return lines


async def build_rank_report(
    clan_tag: str,
    *,
    lang: str = DEFAULT_LANG,
    force_refresh: bool = False,
) -> str:
    if not RANKING_SNAPSHOT_ENABLED:
        return t("rank_not_configured", lang)

    try:
        snapshot = await collect_clan_rank_snapshot(
            clan_tag, force=force_refresh
        )
    except Exception as e:
        logger.error("Rank report failed: %s", e, exc_info=True)
        return t("rank_error", lang)

    if not snapshot:
        return t("rank_no_snapshot", lang)

    rank_na = t("rank_na", lang)
    clan_key = _normalize_tag(clan_tag)
    location_id = _coerce_int(snapshot.get("location_id"))
    if location_id is None:
        return t("rank_no_snapshot", lang)

    raw_source = snapshot.get("raw_source")
    clan_name = None
    if isinstance(raw_source, dict):
        clan_name = raw_source.get("clan_name")
    if not clan_name:
        clan_name = t("unknown", lang)

    location_name = snapshot.get("location_name") or rank_na

    ladder_rank = _coerce_int(snapshot.get("ladder_rank"))
    war_rank = _coerce_int(snapshot.get("war_rank"))
    ladder_score = _coerce_int(snapshot.get("ladder_clan_score"))
    war_score = _coerce_int(snapshot.get("war_clan_score"))
    war_trophies = _coerce_int(snapshot.get("clan_war_trophies"))

    lines: list[str] = [
        t("rank_title", lang),
        t("rank_clan_line", lang, name=clan_name, tag=clan_key),
        t("rank_location_line", lang, location=location_name),
        "",
        t(
            "rank_ladder_line",
            lang,
            rank=_format_rank_value(ladder_rank, rank_na),
            score=_format_score_value(ladder_score, rank_na),
        ),
        t(
            "rank_war_line",
            lang,
            rank=_format_rank_value(war_rank, rank_na),
            score=_format_score_value(war_score, rank_na),
            trophies=_format_score_value(war_trophies, rank_na),
        ),
        "",
        t("rank_changes_header", lang),
    ]

    now = datetime.now(timezone.utc)
    for days in (7, 30, 365):
        target = now - timedelta(days=days)
        old = await get_clan_rank_snapshot_at_or_before(
            clan_key, location_id, target
        )
        old_ladder_rank = (
            _coerce_int(old.get("ladder_rank")) if old else None
        )
        old_war_rank = _coerce_int(old.get("war_rank")) if old else None
        old_ladder_score = (
            _coerce_int(old.get("ladder_clan_score")) if old else None
        )
        old_war_score = _coerce_int(old.get("war_clan_score")) if old else None
        ladder_rank_delta = _format_delta(
            _calc_rank_delta(old_ladder_rank, ladder_rank), rank_na
        )
        ladder_score_delta = _format_delta(
            _calc_score_delta(old_ladder_score, ladder_score), rank_na
        )
        war_rank_delta = _format_delta(
            _calc_rank_delta(old_war_rank, war_rank), rank_na
        )
        war_score_delta = _format_delta(
            _calc_score_delta(old_war_score, war_score), rank_na
        )
        lines.append(
            t(
                "rank_change_line",
                lang,
                days=days,
                ladder_rank=ladder_rank_delta,
                ladder_score=ladder_score_delta,
                war_rank=war_rank_delta,
                war_score=war_score_delta,
            )
        )

    ladder_neighbors = snapshot.get("neighbors_ladder_json")
    war_neighbors = snapshot.get("neighbors_war_json")
    ladder_lines = _render_neighbors(
        ladder_neighbors if isinstance(ladder_neighbors, list) else None,
        header=t("rank_neighbors_ladder_header", lang),
        lang=lang,
        points_to_overtake=_coerce_int(
            snapshot.get("ladder_points_to_overtake_above")
        ),
        rank_value=ladder_rank,
    )
    if ladder_lines:
        lines.append("")
        lines.extend(ladder_lines)
    war_lines = _render_neighbors(
        war_neighbors if isinstance(war_neighbors, list) else None,
        header=t("rank_neighbors_war_header", lang),
        lang=lang,
        points_to_overtake=_coerce_int(
            snapshot.get("war_points_to_overtake_above")
        ),
        rank_value=war_rank,
    )
    if war_lines:
        lines.append("")
        lines.extend(war_lines)

    return "\n".join(lines)


def _format_timestamp(value: object, lang: str = DEFAULT_LANG) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    if isinstance(value, str):
        return value
    if hasattr(value, "astimezone"):
        try:
            return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception:
            return t("unknown", lang)
    return t("unknown", lang)


async def build_weekly_report(
    season_id: int,
    section_index: int,
    clan_tag: str,
    *,
    lang: str = DEFAULT_LANG,
) -> str:
    inactive, active = await get_week_leaderboard(
        season_id=season_id,
        section_index=section_index,
        clan_tag=clan_tag,
        inactive_limit=15,
        active_limit=25,
    )
    inactive = _filter_protected(inactive)
    member_count = len(await get_current_member_tags(clan_tag))
    lines = [
        HEADER_LINE,
        t(
            "weekly_report_title",
            lang,
            season=season_id,
            week=section_index + 1,
        ),
        HEADER_LINE,
        t("weekly_report_members", lang, count=member_count),
        "",
        t("weekly_report_inactive_header", lang),
        *_format_entries(inactive, lang=lang),
        "",
        DIVIDER_LINE,
        "",
        t("weekly_report_active_header", lang),
        *_format_entries(active, lang=lang),
    ]
    lines.extend(["", DIVIDER_LINE, ""])
    lines.extend(await _build_top_donors_wtd_block(clan_tag, lang=lang))
    return "\n".join(lines)


async def build_rolling_report(
    weeks: list[tuple[int, int]],
    clan_tag: str,
    *,
    lang: str = DEFAULT_LANG,
) -> str:
    inactive, active = await get_rolling_leaderboard(
        weeks=weeks,
        clan_tag=clan_tag,
        inactive_limit=15,
        active_limit=35,
    )
    inactive = _filter_protected(inactive)
    member_count = len(await get_current_member_tags(clan_tag))
    weeks_label = ", ".join(f"{season}/{section + 1}" for season, section in weeks)
    lines = [
        HEADER_LINE,
        t("rolling_report_title", lang, weeks=len(weeks)),
        HEADER_LINE,
        t("rolling_report_members", lang, count=member_count),
        t("rolling_report_weeks", lang, weeks=weeks_label)
        if weeks_label
        else t("rolling_report_weeks_na", lang),
        "",
        t("rolling_report_inactive_header", lang),
        *_format_entries(inactive, lang=lang),
        "",
        DIVIDER_LINE,
        "",
        t("rolling_report_active_header", lang),
        *_format_entries(active, lang=lang),
    ]
    if DONATION_WEEKS_WINDOW > 0:
        lines.extend(["", DIVIDER_LINE, ""])
        lines.extend(
            await _build_top_donors_window_block(
                clan_tag, DONATION_WEEKS_WINDOW, lang=lang
            )
        )
    return "\n".join(lines)


async def build_top_players_report(
    clan_tag: str,
    *,
    lang: str = DEFAULT_LANG,
    limit: int = 10,
    window_weeks: int = 10,
    min_tenure_weeks: int = 6,
) -> str:
    weeks: list[tuple[int, int]] = []
    async with get_session() as session:
        completed_result = await session.execute(
            select(
                RiverRaceState.season_id,
                RiverRaceState.section_index,
            )
            .where(
                RiverRaceState.clan_tag == clan_tag,
                RiverRaceState.period_type == "completed",
            )
            .order_by(
                RiverRaceState.season_id.desc(),
                RiverRaceState.section_index.desc(),
            )
        )
        weeks = [
            (int(row.season_id), int(row.section_index))
            for row in completed_result.all()
        ]
    if not weeks:
        weeks = await get_last_completed_weeks(window_weeks, clan_tag)
    if not weeks:
        return t("top_no_data", lang)

    current_tags = await get_current_member_tags(clan_tag)
    if not current_tags:
        return t("top_no_data", lang)

    war_stats = await get_war_stats_for_weeks(clan_tag, weeks)
    eligible_tags = {
        tag
        for tag in current_tags
        if int(war_stats.get(tag, {}).get("weeks_played", 0))
        >= min_tenure_weeks
    }
    if not eligible_tags:
        return t("top_no_eligible", lang, weeks=min_tenure_weeks)

    summary_rows = await get_rolling_summary(weeks, player_tags=eligible_tags)
    if not summary_rows:
        return t("top_no_data", lang)

    rows: list[dict[str, object]] = []
    for row in summary_rows:
        tag = row.get("player_tag")
        if not tag:
            continue
        name = row.get("player_name") or tag or t("unknown", lang)
        decks_used_total = int(row.get("decks_used", 0))
        fame_total = int(row.get("fame", 0))
        weeks_played = int(war_stats.get(tag, {}).get("weeks_played", 0))
        rows.append(
            {
                "player_tag": tag,
                "player_name": name,
                "decks_used_total": decks_used_total,
                "fame_total": fame_total,
                "weeks_played": weeks_played,
            }
        )

    if not rows:
        return t("top_no_data", lang)

    decks_sorted = sorted(
        rows,
        key=lambda r: (
            -int(r["decks_used_total"]),
            -int(r["fame_total"]),
            str(r["player_name"]),
        ),
    )
    fame_sorted = sorted(
        rows,
        key=lambda r: (
            -int(r["fame_total"]),
            -int(r["decks_used_total"]),
            str(r["player_name"]),
        ),
    )

    limit = max(1, limit)
    window = len(weeks)
    weeks_label = ", ".join(f"{season}/{section + 1}" for season, section in weeks)
    decks_sorted = decks_sorted[:limit]
    fame_sorted = fame_sorted[:limit]

    lines = [
        t("top_title", lang, window=window),
        t("top_filter_line", lang, weeks=min_tenure_weeks),
        t(
            "top_weeks_line",
            lang,
            weeks_label=weeks_label if weeks_label else str(window),
        ),
        "",
        t("top_decks_header", lang, n=len(decks_sorted)),
    ]
    for index, row in enumerate(decks_sorted, 1):
        lines.append(
            t(
                "top_entry_line",
                lang,
                index=index,
                name=row["player_name"],
                decks=row["decks_used_total"],
                fame=row["fame_total"],
                played=row["weeks_played"],
                window=window,
            )
        )
    lines.extend(["", t("top_fame_header", lang, n=len(fame_sorted))])
    for index, row in enumerate(fame_sorted, 1):
        lines.append(
            t(
                "top_entry_line",
                lang,
                index=index,
                name=row["player_name"],
                decks=row["decks_used_total"],
                fame=row["fame_total"],
                played=row["weeks_played"],
                window=window,
            )
        )

    return "\n".join(lines)


async def build_donations_report(
    clan_tag: str,
    clan_name: str | None = None,
    *,
    lang: str = DEFAULT_LANG,
) -> str:
    today_utc = datetime.now(timezone.utc).date().isoformat()
    clan_label = clan_name or t("unknown", lang)

    member_rows = await get_current_members_with_wtd_donations(clan_tag)
    member_rows = _filter_protected(member_rows)
    members_count = len(member_rows)
    donation_values = [
        int(row["donations"])
        for row in member_rows
        if row.get("donations") is not None
    ]

    top_rows = sorted(
        [row for row in member_rows if row.get("donations") is not None],
        key=lambda row: (-int(row.get("donations", 0)), str(row.get("player_name") or "")),
    )[:5]

    lines = [
        HEADER_LINE,
        t("donations_title", lang),
        HEADER_LINE,
        t("donations_clan_line", lang, clan=clan_label, tag=clan_tag),
        t("donations_week_line", lang, date=today_utc),
        "",
        SEPARATOR_LINE,
        t("donations_top_header", lang),
    ]

    if top_rows:
        for index, row in enumerate(top_rows, 1):
            name = (
                row.get("player_name")
                or row.get("player_tag")
                or t("unknown", lang)
            )
            donations = row.get("donations")
            donations_text = (
                str(donations) if donations is not None else t("na", lang)
            )
            lines.append(
                t(
                    "donations_top_line",
                    lang,
                    index=index,
                    name=name,
                    cards=donations_text,
                )
            )
        total_cards = sum(donation_values)
        avg_cards = total_cards / members_count if members_count else 0.0
        lines.extend(
            [
                "",
                t("donations_totals_week_header", lang),
                t("donations_totals_week_total", lang, cards=total_cards),
                t(
                    "donations_totals_week_avg",
                    lang,
                    cards=f"{avg_cards:.1f}",
                    members=members_count,
                ),
                t(
                    "donations_totals_week_median",
                    lang,
                    cards=_format_median(donation_values, lang=lang),
                ),
            ]
        )
    else:
        lines.append(t("donations_none", lang))

    if DONATION_WEEKS_WINDOW > 0:
        weekly_rows, coverage = await get_donation_weekly_sums_for_window(
            clan_tag, DONATION_WEEKS_WINDOW
        )
        weekly_rows = _filter_protected(weekly_rows)
        top_weekly = sorted(
            weekly_rows,
            key=lambda row: (
                -int(row.get("donations_sum", 0)),
                -int(row.get("weeks_present", 0)),
                str(row.get("player_name") or ""),
            ),
        )[:5]

        lines.extend(
            [
                "",
                SEPARATOR_LINE,
                t(
                    "donations_window_header",
                    lang,
                    weeks=DONATION_WEEKS_WINDOW,
                ),
            ]
        )
        if top_weekly:
            for index, row in enumerate(top_weekly, 1):
                name = (
                    row.get("player_name")
                    or row.get("player_tag")
                    or t("unknown", lang)
                )
                donations_sum = int(row.get("donations_sum", 0))
                weeks_present = int(row.get("weeks_present", 0))
                lines.append(
                    t(
                        "donations_window_line",
                        lang,
                        index=index,
                        name=name,
                        cards=donations_sum,
                        weeks=weeks_present,
                        window=DONATION_WEEKS_WINDOW,
                    )
                )

            total_cards_8w = sum(int(row.get("donations_sum", 0)) for row in weekly_rows)
            avg_member_8w = total_cards_8w / members_count if members_count else 0.0
            avg_clan_per_week = total_cards_8w / coverage if coverage else 0.0
            avg_member_per_week = (
                total_cards_8w / (members_count * coverage)
                if members_count and coverage
                else 0.0
            )
            lines.extend(
                [
                    "",
                    t(
                        "donations_totals_window_header",
                        lang,
                        weeks=DONATION_WEEKS_WINDOW,
                    ),
                    t(
                        "donations_totals_window_total",
                        lang,
                        cards=total_cards_8w,
                        coverage=coverage,
                        weeks=DONATION_WEEKS_WINDOW,
                    ),
                    t(
                        "donations_totals_window_avg_member",
                        lang,
                        cards=f"{avg_member_8w:.1f}",
                    ),
                    t(
                        "donations_totals_window_avg_week",
                        lang,
                        cards=f"{avg_clan_per_week:.1f}",
                    ),
                    t(
                        "donations_totals_window_avg_member_week",
                        lang,
                        cards=f"{avg_member_per_week:.1f}",
                    ),
                ]
            )
        else:
            lines.append(t("donations_window_none", lang))

    lines.extend(
        [
            "",
            SEPARATOR_LINE,
            t("donations_notes_header", lang),
            t("donations_notes_members", lang),
            t("donations_notes_week", lang),
        ]
    )
    if DONATION_WEEKS_WINDOW <= 0:
        lines.append(t("donations_notes_window_disabled", lang))

    return "\n".join(lines)


def _normalize_role(role: object) -> str:
    if not role:
        return ""
    return (
        str(role)
        .strip()
        .lower()
        .replace(" ", "")
        .replace("-", "")
        .replace("_", "")
    )


async def build_clan_info_report(
    clan_tag: str, *, lang: str = DEFAULT_LANG
) -> str:
    async def _load_clan_info_fallback() -> tuple[dict[str, object] | None, list[dict[str, object]]]:
        clan_key = _normalize_tag(clan_tag)
        snapshot = None
        if RANKING_LOCATION_ID is not None:
            snapshot = await get_latest_clan_rank_snapshot(
                clan_key, RANKING_LOCATION_ID
            )
        member_rows = await get_current_members_snapshot(clan_tag)
        if not member_rows and not snapshot:
            return None, []

        members: list[dict[str, object]] = []
        for row in member_rows:
            members.append(
                {
                    "tag": row.get("player_tag") or "",
                    "name": row.get("player_name") or t("unknown", lang),
                    "role": row.get("role"),
                    "trophies": row.get("trophies"),
                    "donations": row.get("donations"),
                    "donationsReceived": row.get("donations_received"),
                    "expLevel": row.get("exp_level"),
                    "clanRank": row.get("clan_rank"),
                    "previousClanRank": row.get("previous_clan_rank"),
                    "lastSeen": row.get("last_seen"),
                }
            )

        clan_name = t("unknown", lang)
        members_count = len(members)
        clan_score = None
        war_trophies = None
        location_name = None
        if isinstance(snapshot, dict):
            location_name = snapshot.get("location_name") or location_name
            clan_score = snapshot.get("ladder_clan_score")
            war_trophies = snapshot.get("clan_war_trophies")
            snapshot_members = snapshot.get("members")
            if isinstance(snapshot_members, int):
                members_count = snapshot_members
            raw_source = snapshot.get("raw_source")
            if isinstance(raw_source, dict):
                clan_name = raw_source.get("clan_name") or clan_name

        donations_total = None
        if members:
            donations_total = sum(
                int(member.get("donations") or 0) for member in members
            )

        clan = {
            "tag": clan_tag,
            "name": clan_name,
            "members": members_count,
            "clanScore": clan_score if clan_score is not None else t("na", lang),
            "clanWarTrophies": war_trophies
            if war_trophies is not None
            else t("na", lang),
            "donationsPerWeek": donations_total
            if donations_total is not None
            else t("na", lang),
            "location": {"name": location_name} if location_name else {},
            "type": None,
            "requiredTrophies": None,
        }
        return clan, members

    clan = None
    members: list[dict[str, object]] = []
    try:
        api_client = await get_api_client()
        clan = await api_client.get_clan(clan_tag)
        members = await api_client.get_clan_members(clan_tag)
        if not isinstance(members, list):
            members = []
    except ClashRoyaleAPIError as e:
        if e.status_code in (401, 403):
            return t("clan_info_access_denied", lang)
        logger.warning("Failed to fetch clan info: %s", e)
    except Exception as e:
        logger.warning("Failed to fetch clan info: %s", e, exc_info=True)

    if not clan:
        clan, members = await _load_clan_info_fallback()
        if not clan:
            return t("clan_info_unavailable", lang)

    location = clan.get("location") or {}
    location_name = (
        location.get("localizedName")
        or location.get("name")
        or t("unknown", lang)
    )
    clan_type = str(clan.get("type") or "").lower()
    type_map = {
        "open": t("clan_type_open", lang),
        "inviteonly": t("clan_type_invite_only", lang),
        "closed": t("clan_type_closed", lang),
    }
    type_label = type_map.get(clan_type, t("unknown", lang))

    members_count = clan.get("members")
    if members_count is None:
        members_count = len(members) if isinstance(members, list) else None
    if members_count is None:
        members_line = t("clan_info_members_unknown", lang)
    else:
        members_line = t("clan_info_members", lang, count=members_count)

    required_trophies = clan.get("requiredTrophies")
    required_text = (
        str(required_trophies) if required_trophies is not None else t("na", lang)
    )

    clan_score = clan.get("clanScore")
    war_trophies = clan.get("clanWarTrophies")
    donations_per_week = clan.get("donationsPerWeek")

    now_utc = datetime.now(timezone.utc)
    leader = None
    co_leaders: list[dict[str, object]] = []
    elders: list[dict[str, object]] = []
    for member in members:
        role = _normalize_role(member.get("role"))
        if role == "leader":
            leader = member
        elif role == "coleader":
            co_leaders.append(member)
        elif role == "elder":
            elders.append(member)

    if leader:
        leader_name = leader.get("name", t("unknown", lang))
        leader_trophies = leader.get("trophies")
        leader_trophies_text = (
            str(leader_trophies) if leader_trophies is not None else t("na", lang)
        )
        last_seen = _parse_last_seen_string(leader.get("lastSeen"))
        if last_seen:
            relative = _format_relative(now_utc - last_seen, lang)
        else:
            relative = t("unknown", lang)
        leader_line = (
            t(
                "clan_info_leader_line",
                lang,
                name=leader_name,
                trophies=leader_trophies_text,
                last_seen=relative,
            )
        )
    else:
        leader_line = t("clan_info_leader_unknown", lang)

    def _format_role_list(label: str, items: list[dict[str, object]]) -> str:
        names = [m.get("name", t("unknown", lang)) for m in items]
        total = len(names)
        if total == 0:
            return t("clan_info_role_none", lang, label=label)
        shown = names[:10]
        suffix = "…" if total > 10 else ""
        return t(
            "clan_info_role_list",
            lang,
            label=label,
            total=total,
            names=", ".join(shown),
            suffix=suffix,
        )

    co_leader_line = _format_role_list(
        t("clan_info_co_leaders_label", lang), co_leaders
    )
    elder_line = _format_role_list(
        t("clan_info_elders_label", lang), elders
    )

    donors_sorted = sorted(
        members,
        key=lambda m: (-int(m.get("donations") or 0), str(m.get("name") or "")),
    )[:5]
    trophies_sorted = sorted(
        members,
        key=lambda m: (-int(m.get("trophies") or 0), str(m.get("name") or "")),
    )[:5]

    parsed_seen = []
    for member in members:
        seen = _parse_last_seen_string(member.get("lastSeen"))
        if seen:
            parsed_seen.append((member.get("name", t("unknown", lang)), seen))

    bucket_green = 0
    bucket_yellow = 0
    bucket_red = 0
    for _name, seen in parsed_seen:
        delta = now_utc - seen
        if delta < timedelta(hours=24):
            bucket_green += 1
        elif delta < timedelta(days=3):
            bucket_yellow += 1
        else:
            bucket_red += 1

    longest_missing = sorted(
        parsed_seen, key=lambda item: (now_utc - item[1]), reverse=True
    )[:5]

    lines = [
        HEADER_LINE,
        t(
            "clan_info_title",
            lang,
            name=clan.get("name", t("unknown", lang)),
        ),
        HEADER_LINE,
        t("clan_info_tag", lang, tag=clan.get("tag", clan_tag)),
        t("clan_info_location", lang, location=location_name),
        t("clan_info_type", lang, clan_type=type_label),
        members_line,
        t("clan_info_required_trophies", lang, trophies=required_text),
        "",
        t("clan_info_clan_trophies", lang, trophies=clan_score),
        t("clan_info_war_trophies", lang, trophies=war_trophies),
        t(
            "clan_info_donations_week",
            lang,
            donations=donations_per_week,
        ),
        "",
        DIVIDER_LINE,
        t("clan_info_leadership_header", lang),
        leader_line,
        co_leader_line,
        elder_line,
        "",
        DIVIDER_LINE,
        t("clan_info_top_donors_header", lang),
    ]

    for index, member in enumerate(donors_sorted, 1):
        lines.append(
            t(
                "clan_info_top_donor_line",
                lang,
                index=index,
                name=member.get("name", t("unknown", lang)),
                cards=member.get("donations", 0),
            )
        )

    lines.extend(
        [
            "",
            DIVIDER_LINE,
            t("clan_info_top_trophies_header", lang),
        ]
    )
    for index, member in enumerate(trophies_sorted, 1):
        lines.append(
            t(
                "clan_info_top_trophies_line",
                lang,
                index=index,
                name=member.get("name", t("unknown", lang)),
                trophies=member.get("trophies", 0),
            )
        )

    lines.extend(
        [
            "",
            DIVIDER_LINE,
            t("clan_info_activity_header", lang),
            t(
                "clan_info_activity_counts",
                lang,
                green=bucket_green,
                yellow=bucket_yellow,
                red=bucket_red,
            ),
            "",
            t("clan_info_longest_missing_header", lang),
        ]
    )
    for index, (name, seen) in enumerate(longest_missing, 1):
        relative = _format_relative(now_utc - seen, lang)
        lines.append(
            t(
                "clan_info_longest_missing_line",
                lang,
                index=index,
                name=name,
                last_seen=relative,
            )
        )

    lines.append(HEADER_LINE)
    return "\n".join(lines)


def _min_max_norm(value: float, min_value: float, max_value: float) -> float:
    if max_value <= min_value:
        return 0.0
    return (value - min_value) / (max_value - min_value)


async def build_promotion_candidates_report(
    clan_tag: str, *, lang: str = DEFAULT_LANG
) -> str:
    window_weeks = 8
    weeks = await get_last_completed_weeks(window_weeks, clan_tag)
    season_id = weeks[0][0] if weeks else 0

    member_rows = await get_current_members_snapshot(clan_tag)
    members: dict[str, str] = {}
    roles: dict[str, str] = {}
    for row in member_rows:
        tag = _normalize_tag(row.get("player_tag"))
        if not tag or tag in PROTECTED_TAGS_NORMALIZED:
            continue
        name = row.get("player_name") or t("unknown", lang)
        members[tag] = name
        role_value = row.get("role")
        role_text = str(role_value).strip().lower() if role_value else ""
        roles[tag] = role_text

    war_stats = await get_war_stats_for_weeks(clan_tag, weeks)
    alltime_weeks = await get_alltime_weeks_played(clan_tag)
    donations_rows, donation_coverage = await get_donation_weekly_sums_for_window(
        clan_tag, window_weeks
    )
    donations_map: dict[str, dict[str, int]] = {}
    for row in donations_rows:
        tag = _normalize_tag(row.get("player_tag"))
        if not tag:
            continue
        donations_map[tag] = {
            "donations_sum": int(row.get("donations_sum", 0)),
            "weeks_present": int(row.get("weeks_present", 0)),
        }

    donations_available = any(
        value.get("weeks_present", 0) > 0 for value in donations_map.values()
    )

    stats_rows: list[dict[str, object]] = []
    for tag, name in members.items():
        stats = war_stats.get(tag, {})
        weeks_played = int(stats.get("weeks_played", 0))
        active_weeks = int(stats.get("active_weeks", 0))
        avg_decks = float(stats.get("avg_decks", 0.0))
        avg_fame = float(stats.get("avg_fame", 0.0))
        alltime = int(alltime_weeks.get(tag, 0))
        don_stats = donations_map.get(tag, {})
        don_sum = int(don_stats.get("donations_sum", 0))
        don_weeks = int(don_stats.get("weeks_present", 0))
        don_avg = don_sum / don_weeks if don_weeks > 0 else 0.0
        stats_rows.append(
            {
                "player_tag": tag,
                "player_name": name,
                "role": roles.get(tag, ""),
                "weeks_played": weeks_played,
                "active_weeks": active_weeks,
                "avg_decks": avg_decks,
                "avg_fame": avg_fame,
                "alltime_weeks": alltime,
                "donations_sum": don_sum,
                "donations_weeks": don_weeks,
                "donations_avg": don_avg,
            }
        )

    fame_values = [row["avg_fame"] for row in stats_rows]
    decks_values = [row["avg_decks"] for row in stats_rows]
    don_values = [row["donations_avg"] for row in stats_rows]
    fame_min, fame_max = (min(fame_values), max(fame_values)) if fame_values else (0.0, 0.0)
    decks_min, decks_max = (min(decks_values), max(decks_values)) if decks_values else (0.0, 0.0)
    don_min, don_max = (min(don_values), max(don_values)) if don_values else (0.0, 0.0)

    elder_candidates: list[dict[str, object]] = []
    for row in stats_rows:
        role = str(row.get("role") or "").lower()
        if role in ("elder", "coleader", "leader"):
            continue
        if row["weeks_played"] < PROMOTE_MIN_WEEKS_PLAYED_ELDER:
            continue
        if row["active_weeks"] < PROMOTE_MIN_ACTIVE_WEEKS_ELDER:
            continue
        if row["avg_decks"] < PROMOTE_MIN_AVG_DECKS_ELDER:
            continue
        fame_norm = _min_max_norm(row["avg_fame"], fame_min, fame_max)
        decks_norm = _min_max_norm(row["avg_decks"], decks_min, decks_max)
        if donations_available:
            don_norm = _min_max_norm(row["donations_avg"], don_min, don_max)
        else:
            don_norm = 0.0
        score = 0.55 * fame_norm + 0.30 * decks_norm + 0.15 * don_norm
        row["score"] = score
        elder_candidates.append(row)

    elder_candidates.sort(
        key=lambda row: (
            -float(row.get("score", 0.0)),
            -int(row.get("active_weeks", 0)),
            -int(row.get("donations_sum", 0)),
            str(row.get("player_name") or ""),
        )
    )
    elder_candidates = elder_candidates[: max(PROMOTE_ELDER_LIMIT, 0)]

    top3_fame = {
        row["player_tag"]
        for row in sorted(
            stats_rows,
            key=lambda row: (-float(row.get("avg_fame", 0.0)), str(row.get("player_name") or "")),
        )[:3]
    }
    top3_don = set()
    if donations_available:
        top3_don = {
            row["player_tag"]
            for row in sorted(
                stats_rows,
                key=lambda row: (
                    -int(row.get("donations_sum", 0)),
                    str(row.get("player_name") or ""),
                ),
            )[:3]
        }

    co_candidates: list[dict[str, object]] = []
    if donations_available:
        for row in stats_rows:
            role = str(row.get("role") or "").lower()
            if role != "elder":
                continue
            if row["weeks_played"] < PROMOTE_MIN_WEEKS_PLAYED_COLEADER:
                continue
            if row["active_weeks"] < PROMOTE_MIN_ACTIVE_WEEKS_COLEADER:
                continue
            if row["avg_decks"] < PROMOTE_MIN_AVG_DECKS_COLEADER:
                continue
            if row["alltime_weeks"] < PROMOTE_MIN_ALLTIME_WEEKS_COLEADER:
                continue
            if row["player_tag"] not in top3_fame:
                continue
            if row["player_tag"] not in top3_don:
                continue
            co_candidates.append(row)

    co_candidates.sort(
        key=lambda row: (
            -float(row.get("avg_fame", 0.0)),
            -int(row.get("donations_sum", 0)),
            str(row.get("player_name") or ""),
        )
    )
    co_candidates = co_candidates[: max(PROMOTE_COLEADER_LIMIT, 0)]

    lines = [
        HEADER_LINE,
        t("promotion_title", lang),
        t("promotion_season_line", lang, season=season_id),
        HEADER_LINE,
        t("promotion_based_on", lang),
        t("promotion_note", lang),
        "",
        t("promotion_elder_header", lang, count=len(elder_candidates)),
    ]

    if elder_candidates:
        for index, row in enumerate(elder_candidates, 1):
            lines.append(
                t(
                    "promotion_candidate_line",
                    lang,
                    index=index,
                    name=row["player_name"],
                    tag=row["player_tag"],
                )
            )
            lines.append(
                t(
                    "promotion_war_line",
                    lang,
                    active_weeks=row.get("active_weeks", 0),
                    avg_decks=_format_avg(float(row.get("avg_decks", 0.0))),
                    avg_fame=_format_avg(float(row.get("avg_fame", 0.0))),
                )
            )
            lines.append(
                t(
                    "promotion_donations_line",
                    lang,
                    donations=row.get("donations_sum", 0),
                    avg_donations=_format_avg(float(row.get("donations_avg", 0.0))),
                    donation_weeks=row.get("donations_weeks", 0),
                )
            )
    else:
        lines.append(t("promotion_no_candidates", lang))

    lines.extend(
        ["", DIVIDER_LINE, t("promotion_co_leader_header", lang)]
    )
    if co_candidates:
        for index, row in enumerate(co_candidates, 1):
            lines.append(
                t(
                    "promotion_candidate_line",
                    lang,
                    index=index,
                    name=row["player_name"],
                    tag=row["player_tag"],
                )
            )
            lines.append(
                t(
                    "promotion_war_line",
                    lang,
                    active_weeks=row.get("active_weeks", 0),
                    avg_decks=_format_avg(float(row.get("avg_decks", 0.0))),
                    avg_fame=_format_avg(float(row.get("avg_fame", 0.0))),
                )
            )
            lines.append(
                t(
                    "promotion_donations_line",
                    lang,
                    donations=row.get("donations_sum", 0),
                    avg_donations=_format_avg(float(row.get("donations_avg", 0.0))),
                    donation_weeks=row.get("donations_weeks", 0),
                )
            )
    else:
        lines.append(t("promotion_no_candidate_month", lang))

    lines.extend(
        [
            "",
            DIVIDER_LINE,
            t("promotion_notes_header", lang),
            t("promotion_notes_members", lang),
            t("promotion_notes_new_members", lang),
            t("promotion_notes_protected", lang),
            HEADER_LINE,
        ]
    )
    return "\n".join(lines)


async def build_kick_shortlist_report_legacy(
    weeks: list[tuple[int, int]],
    last_week: tuple[int, int] | None,
    clan_tag: str,
    *,
    lang: str = DEFAULT_LANG,
) -> str:
    lines = [
        HEADER_LINE,
        t("kick_shortlist_title", lang),
        HEADER_LINE,
        t(
            "kick_shortlist_rules",
            lang,
            weeks=NEW_MEMBER_WEEKS_PLAYED,
            revived=REVIVED_DECKS_THRESHOLD,
        ),
    ]
    if not weeks or not last_week:
        lines.append(t("kick_shortlist_none", lang))
        lines.append(t("kick_wtd_note", lang))
        return "\n".join(lines)

    async with get_session() as session:
        inactive, _active = await get_rolling_leaderboard(
            weeks=weeks,
            clan_tag=clan_tag,
            session=session,
        )
        if not inactive:
            lines.append(t("kick_shortlist_none", lang))
            lines.append(t("kick_wtd_note", lang))
            return "\n".join(lines)

        inactive = _filter_protected(inactive)
        inactive_tags = {
            _normalize_tag(row.get("player_tag"))
            for row in inactive
            if row.get("player_tag")
        }
        if not inactive_tags:
            lines.append(t("kick_shortlist_none", lang))
            lines.append(t("kick_wtd_note", lang))
            return "\n".join(lines)

        history_counts = await get_participation_week_counts(
            player_tags=inactive_tags, session=session
        )
        last_week_decks = await get_week_decks_map(
            last_week[0],
            last_week[1],
            player_tags=inactive_tags,
            session=session,
        )

    donations_wtd = await _collect_wtd_donations(clan_tag, inactive_tags)
    last_seen_map = await get_last_seen_map(clan_tag)
    now_utc = datetime.now(timezone.utc)

    candidates: list[dict[str, object]] = []
    warnings: list[dict[str, object]] = []
    donation_warnings: list[dict[str, object]] = []
    new_members: list[dict[str, object]] = []
    for row in inactive:
        tag = row.get("player_tag")
        if not tag:
            continue
        weeks_played = int(history_counts.get(tag, 0))
        last_decks = int(last_week_decks.get(tag, 0))
        wtd_donations = None
        normalized_tag = _normalize_tag(tag)
        if normalized_tag in donations_wtd:
            wtd_donations = donations_wtd[normalized_tag].get("donations")
        last_seen = last_seen_map.get(normalized_tag)
        days_absent = _days_absent(last_seen, now_utc)
        if weeks_played <= NEW_MEMBER_WEEKS_PLAYED:
            if last_decks < REVIVED_DECKS_THRESHOLD:
                new_members.append(
                    {
                        "player_name": row.get("player_name") or t("unknown", lang),
                        "player_tag": normalized_tag,
                        "decks_used": int(row.get("decks_used", 0)),
                        "fame": int(row.get("fame", 0)),
                        "weeks_played": weeks_played,
                    }
                )
            continue
        entry = {
            "player_tag": normalized_tag,
            "player_name": row.get("player_name") or t("unknown", lang),
            "decks_used": int(row.get("decks_used", 0)),
            "fame": int(row.get("fame", 0)),
            "last_week_decks": last_decks,
            "donations_wtd": wtd_donations,
            "days_absent": days_absent,
            "weeks_played": weeks_played,
        }
        if last_decks >= REVIVED_DECKS_THRESHOLD:
            warnings.append(entry)
            continue
        donation_revive = False
        if (
            wtd_donations is not None
            and wtd_donations >= DONATION_REVIVE_WTD_THRESHOLD
        ):
            donation_revive = True
        if donation_revive:
            donation_warnings.append(entry)
        else:
            candidates.append(entry)

    shortlist = candidates[: max(KICK_SHORTLIST_LIMIT, 0)]
    saved_by_colosseum: list[dict[str, object]] = []
    if shortlist:
        colosseum_stats: dict[str, dict[str, int]] = {}
        week_pairs = [
            (int(season_id), int(section_index))
            for season_id, section_index in weeks
        ]
        async with get_session() as session:
            colosseum_result = await session.execute(
                select(
                    RiverRaceState.season_id,
                    RiverRaceState.section_index,
                )
                .where(
                    RiverRaceState.clan_tag == clan_tag,
                    RiverRaceState.is_colosseum.is_(True),
                    tuple_(
                        RiverRaceState.season_id,
                        RiverRaceState.section_index,
                    ).in_(week_pairs),
                )
                .order_by(
                    RiverRaceState.season_id.desc(),
                    RiverRaceState.section_index.desc(),
                )
                .limit(1)
            )
            colosseum_week = colosseum_result.first()
            if colosseum_week:
                colosseum_tags = {
                    row.get("player_tag")
                    for row in shortlist
                    if row.get("player_tag")
                }
                if colosseum_tags:
                    stats_result = await session.execute(
                        select(
                            PlayerParticipation.player_tag,
                            PlayerParticipation.decks_used,
                            PlayerParticipation.fame,
                        ).where(
                            PlayerParticipation.season_id
                            == int(colosseum_week.season_id),
                            PlayerParticipation.section_index
                            == int(colosseum_week.section_index),
                            PlayerParticipation.player_tag.in_(colosseum_tags),
                        )
                    )
                    colosseum_stats = {
                        row.player_tag: {
                            "decks_used": int(row.decks_used),
                            "fame": int(row.fame),
                        }
                        for row in stats_result.all()
                    }

        if colosseum_stats:
            filtered_shortlist: list[dict[str, object]] = []
            for row in shortlist:
                tag = row.get("player_tag")
                stats = colosseum_stats.get(tag)
                if stats and (
                    stats["fame"] >= KICK_COLOSSEUM_SAVE_FAME
                    or stats["decks_used"] >= KICK_COLOSSEUM_SAVE_DECKS
                ):
                    saved_by_colosseum.append(
                        {
                            "player_tag": tag,
                            "player_name": row.get("player_name")
                            or t("unknown", lang),
                            "weeks_played": row.get("weeks_played", 0),
                            "decks_used": stats["decks_used"],
                            "fame": stats["fame"],
                        }
                    )
                else:
                    filtered_shortlist.append(row)
            shortlist = filtered_shortlist
    if shortlist:
        lines.append(t("kick_candidates_header", lang))
        for index, row in enumerate(shortlist, 1):
            days_absent = row.get("days_absent")
            wtd_donations = row.get("donations_wtd")
            flags: list[str] = []
            absence_flag = _absence_flag(days_absent)
            if absence_flag:
                flags.append(absence_flag)
            if (
                wtd_donations is not None
                and wtd_donations >= DONATION_BOX_THRESHOLD
            ):
                flags.append("📦")
            prefix = f"{' '.join(flags)} " if flags else ""
            weeks_played = int(row.get("weeks_played") or 0)
            wp = f" (war:{weeks_played}w)" if weeks_played > 0 else ""
            display_name = f"{prefix}{row.get('player_name')}{wp}"
            name = _format_name(display_name, lang).rstrip()
            donation_suffix = _format_donation_suffix(
                row.get("player_tag"),
                donations_wtd,
                lang=lang,
            )
            lines.append(
                t(
                    "kick_candidate_line",
                    lang,
                    index=index,
                    name=name,
                    decks=row.get("decks_used", 0),
                    fame=row.get("fame", 0),
                    last_week=row.get("last_week_decks", 0),
                    donation_suffix=donation_suffix,
                )
            )
    else:
        lines.append(t("kick_shortlist_none", lang))

    if saved_by_colosseum:
        lines.extend(
            [
                "",
                t("kick_saved_colosseum_header", lang),
            ]
        )
        for index, row in enumerate(saved_by_colosseum, 1):
            player_name = row.get("player_name") or t("unknown", lang)
            weeks_played = int(row.get("weeks_played") or 0)
            wp = f" (war:{weeks_played}w)" if weeks_played > 0 else ""
            display_name = f"{player_name}{wp}"
            name = _format_name(display_name, lang).rstrip()
            lines.append(
                t(
                    "kick_saved_colosseum_line",
                    lang,
                    index=index,
                    name=name,
                    decks=row.get("decks_used", 0),
                    fame=row.get("fame", 0),
                )
            )

    if warnings:
        lines.extend(
            [
                "",
                t("kick_warnings_revived_header", lang),
            ]
        )
        for index, row in enumerate(warnings, 1):
            weeks_played = int(row.get("weeks_played") or 0)
            wp = f" (war:{weeks_played}w)" if weeks_played > 0 else ""
            display_name = f"{row.get('player_name')}{wp}"
            name = _format_name(display_name, lang).rstrip()
            donation_suffix = _format_donation_suffix(
                row.get("player_tag"),
                donations_wtd,
                lang=lang,
            )
            lines.append(
                t(
                    "kick_candidate_line",
                    lang,
                    index=index,
                    name=name,
                    decks=row.get("decks_used", 0),
                    fame=row.get("fame", 0),
                    last_week=row.get("last_week_decks", 0),
                    donation_suffix=donation_suffix,
                )
            )

    if donation_warnings:
        lines.extend(
            [
                "",
                t("kick_warnings_donating_header", lang),
            ]
        )
        for index, row in enumerate(donation_warnings, 1):
            weeks_played = int(row.get("weeks_played") or 0)
            wp = f" (war:{weeks_played}w)" if weeks_played > 0 else ""
            display_name = f"{row.get('player_name')}{wp}"
            name = _format_name(display_name, lang).rstrip()
            donation_suffix = _format_donation_suffix(
                row.get("player_tag"),
                donations_wtd,
                lang=lang,
            )
            lines.append(
                t(
                    "kick_candidate_line",
                    lang,
                    index=index,
                    name=name,
                    decks=row.get("decks_used", 0),
                    fame=row.get("fame", 0),
                    last_week=row.get("last_week_decks", 0),
                    donation_suffix=donation_suffix,
                )
            )

    if LAST_SEEN_FLAG_LIMIT > 0:
        snapshot_rows = await get_current_members_snapshot(clan_tag)
        flagged_rows: list[dict[str, object]] = []
        for row in snapshot_rows:
            days_absent = _days_absent(row.get("last_seen"), now_utc)
            flag = _absence_flag(days_absent)
            if not flag:
                continue
            flagged_rows.append(
                {
                    "player_tag": row.get("player_tag"),
                    "player_name": row.get("player_name") or t("unknown", lang),
                    "days_absent": days_absent,
                    "flag": flag,
                }
            )
        flagged_rows = _filter_protected(flagged_rows)
        red_rows = [
            row for row in flagged_rows if row.get("flag") == "🔴"
        ]
        yellow_rows = [
            row for row in flagged_rows if row.get("flag") == "🟡"
        ]
        red_rows.sort(
            key=lambda row: int(row.get("days_absent") or 0), reverse=True
        )
        yellow_rows.sort(
            key=lambda row: int(row.get("days_absent") or 0), reverse=True
        )
        combined = (red_rows + yellow_rows)[:LAST_SEEN_FLAG_LIMIT]
        if combined:
            lines.extend(["", t("kick_last_seen_header", lang)])
            for index, row in enumerate(combined, 1):
                name = row.get("player_name") or t("unknown", lang)
                days_absent = row.get("days_absent")
                days_text = (
                    t("inactive_days_ago", lang, days=days_absent)
                    if days_absent is not None
                    else t("na", lang)
                )
                lines.append(
                    t(
                        "kick_last_seen_line",
                        lang,
                        index=index,
                        flag=row.get("flag"),
                        name=name,
                        days_text=days_text,
                    )
                )

    if new_members:
        lines.extend(
            [
                "",
                t(
                    "kick_new_members_header",
                    lang,
                    weeks=NEW_MEMBER_WEEKS_PLAYED,
                ),
            ]
        )
        for index, row in enumerate(new_members, 1):
            name = _format_name(row.get("player_name"), lang).rstrip()
            lines.append(
                t(
                    "kick_new_member_line",
                    lang,
                    index=index,
                    name=name,
                    decks=row.get("decks_used", 0),
                    fame=row.get("fame", 0),
                    weeks_played=row.get("weeks_played", 0),
                )
            )

    lines.append(t("kick_wtd_note", lang))
    return "\n".join(lines)


async def build_kick_shortlist_report(
    weeks: list[tuple[int, int]],
    last_week: tuple[int, int] | None,
    clan_tag: str,
    *,
    lang: str = DEFAULT_LANG,
    detailed: bool = False,
) -> str:
    now_utc = datetime.now(timezone.utc)
    colosseum_weeks: list[tuple[int, int]] = []
    d0_map: dict[tuple[int, int], date | None] = {}
    fallback_map: dict[tuple[int, int], date | None] = {}
    eligible_tags_map: dict[tuple[int, int], set[str] | None] = {}
    colosseum_stats: dict[str, dict[tuple[int, int], dict[str, int]]] = {}

    inactive: list[dict[str, object]] = []
    inactive_tags: set[str] = set()
    history_counts: dict[str, int] = {}
    last_week_decks: dict[str, int] = {}

    async with get_session() as session:
        colosseum_result = await session.execute(
            select(RiverRaceState.season_id, RiverRaceState.section_index)
            .where(RiverRaceState.is_colosseum.is_(True))
            .order_by(
                RiverRaceState.season_id.desc(),
                RiverRaceState.section_index.desc(),
            )
            .limit(3)
        )
        colosseum_weeks = [
            (int(row.season_id), int(row.section_index))
            for row in colosseum_result.all()
        ]

        for season_id, section_index in colosseum_weeks:
            d0 = await get_first_snapshot_date_for_week(
                season_id, section_index, session=session
            )
            if d0 is None:
                d0 = KICK_COLOSSEUM_D0_OVERRIDES.get(
                    (season_id, section_index)
                )
            d0_map[(season_id, section_index)] = d0

        for season_id, section_index in colosseum_weeks:
            d0 = d0_map.get((season_id, section_index))
            if d0 is None:
                fallback_map[(season_id, section_index)] = None
                continue
            cutoff = d0 + timedelta(days=2)
            window_start = d0 - timedelta(days=1)
            fallback_result = await session.execute(
                select(func.max(ClanMemberDaily.snapshot_date)).where(
                    ClanMemberDaily.clan_tag == clan_tag,
                    ClanMemberDaily.snapshot_date >= window_start,
                    ClanMemberDaily.snapshot_date <= cutoff,
                )
            )
            fallback_map[(season_id, section_index)] = (
                fallback_result.scalar_one_or_none()
            )

        fallback_dates = {
            value for value in fallback_map.values() if isinstance(value, date)
        }
        roster_tags_by_date: dict[date, set[str]] = {}
        if fallback_dates:
            roster_result = await session.execute(
                select(ClanMemberDaily.snapshot_date, ClanMemberDaily.player_tag)
                .where(
                    ClanMemberDaily.clan_tag == clan_tag,
                    ClanMemberDaily.snapshot_date.in_(fallback_dates),
                )
            )
            for row in roster_result.all():
                roster_tags_by_date.setdefault(row.snapshot_date, set()).add(
                    _normalize_tag(row.player_tag)
                )

        for week in colosseum_weeks:
            fallback_date = fallback_map.get(week)
            if fallback_date is None:
                eligible_tags_map[week] = None
            else:
                eligible_tags_map[week] = roster_tags_by_date.get(
                    fallback_date, set()
                )

        if weeks and last_week:
            inactive, _active = await get_rolling_leaderboard(
                weeks=weeks,
                clan_tag=clan_tag,
                session=session,
            )
            inactive = _filter_protected(inactive)
            inactive_tags = {
                _normalize_tag(row.get("player_tag"))
                for row in inactive
                if row.get("player_tag")
            }
            if inactive_tags:
                history_counts = await get_participation_week_counts(
                    player_tags=inactive_tags, session=session
                )
                last_week_decks = await get_week_decks_map(
                    last_week[0],
                    last_week[1],
                    player_tags=inactive_tags,
                    session=session,
                )
                if colosseum_weeks:
                    stats_result = await session.execute(
                        select(
                            PlayerParticipation.player_tag,
                            PlayerParticipation.season_id,
                            PlayerParticipation.section_index,
                            PlayerParticipation.decks_used,
                            PlayerParticipation.fame,
                        ).where(
                            tuple_(
                                PlayerParticipation.season_id,
                                PlayerParticipation.section_index,
                            ).in_(colosseum_weeks),
                            PlayerParticipation.player_tag.in_(inactive_tags),
                        )
                    )
                    for row in stats_result.all():
                        tag = _normalize_tag(row.player_tag)
                        colosseum_stats.setdefault(tag, {})[
                            (int(row.season_id), int(row.section_index))
                        ] = {
                            "decks_used": int(row.decks_used),
                            "fame": int(row.fame),
                        }

    col_weeks_count = len(colosseum_weeks)
    fallback_start = t("kick_v2_value_na", lang)
    fallback_end = t("kick_v2_value_na", lang)
    if colosseum_weeks:
        latest_d0 = d0_map.get(colosseum_weeks[0])
        if isinstance(latest_d0, date):
            fallback_start = (latest_d0 - timedelta(days=1)).isoformat()
            fallback_end = (latest_d0 + timedelta(days=2)).isoformat()

    season_label = t("kick_v2_label_season", lang)
    week_label = t("kick_v2_label_week", lang)
    colosseum_label = t("kick_v2_label_colosseum", lang)
    na_value = t("kick_v2_value_na", lang)

    donations_wtd = await _collect_wtd_donations(clan_tag, inactive_tags)
    last_seen_map = await get_last_seen_map(clan_tag)

    decks_label = t("kick_v2_label_decks_window", lang)
    fame_label = t("kick_v2_label_fame_window", lang)
    last_week_label = t("kick_v2_label_last_week", lang)
    donations_label = t("kick_v2_label_donations_wtd", lang)
    weeks_label = t("kick_v2_label_weeks_in_clan", lang)
    last_seen_label = t("kick_v2_label_last_seen", lang)
    not_applicable_short = t("kick_v2_value_not_applicable_short", lang)

    def _build_colosseum_lines(
        tag: str,
    ) -> tuple[str, str, list[str], bool, list[tuple[int, int, str]]]:
        history_parts: list[str] = []
        eligible_results: list[str] = []
        has_unknown = False
        eligibility_statuses: list[tuple[int, int, str]] = []
        latest_usable_week: tuple[int, int] | None = None
        for season_id, section_index in colosseum_weeks:
            week_key = (season_id, section_index)
            eligible_tags = eligible_tags_map.get(week_key)
            if eligible_tags is None:
                has_unknown = True
                eligibility_statuses.append(
                    (season_id, section_index, "unknown")
                )
                status_text = t("kick_v2_value_na", lang)
            elif tag in eligible_tags:
                eligibility_statuses.append(
                    (season_id, section_index, "eligible")
                )
                if latest_usable_week is None:
                    latest_usable_week = week_key
                stats = colosseum_stats.get(tag, {}).get(
                    week_key, {"decks_used": 0, "fame": 0}
                )
                is_pass = (
                    stats["decks_used"] >= KICK_COLOSSEUM_SAVE_DECKS
                    and stats["fame"] >= KICK_COLOSSEUM_SAVE_FAME
                )
                status_text = t(
                    "kick_v2_value_pass" if is_pass else "kick_v2_value_fail",
                    lang,
                )
                eligible_results.append("pass" if is_pass else "fail")
            else:
                eligibility_statuses.append(
                    (season_id, section_index, "not_applicable")
                )
                if latest_usable_week is None:
                    latest_usable_week = week_key
                status_text = not_applicable_short
            history_parts.append(
                f"[{season_label}{season_id} {status_text}]"
            )
        history_text = (
            " ".join(history_parts) if history_parts else t("kick_v2_value_na", lang)
        )
        history_line = t(
            "kick_v2_colosseum_history_line", lang, history=history_text
        )

        latest_week = latest_usable_week or (
            colosseum_weeks[0] if colosseum_weeks else None
        )
        if latest_week:
            eligible_tags = eligible_tags_map.get(latest_week)
            stats = colosseum_stats.get(tag, {}).get(
                latest_week, {"decks_used": 0, "fame": 0}
            )
            if eligible_tags is None:
                result_text = t("kick_v2_value_na", lang)
            elif tag in eligible_tags:
                is_pass = (
                    stats["decks_used"] >= KICK_COLOSSEUM_SAVE_DECKS
                    and stats["fame"] >= KICK_COLOSSEUM_SAVE_FAME
                )
                result_text = t(
                    "kick_v2_value_pass" if is_pass else "kick_v2_value_fail",
                    lang,
                )
            else:
                result_text = not_applicable_short
            detail_line = t(
                "kick_v2_colosseum_latest_detail_line",
                lang,
                colosseum=colosseum_label,
                season_label=season_label,
                season=latest_week[0],
                week_label=week_label,
                week=latest_week[1] + 1,
                decks_label=decks_label,
                decks=stats["decks_used"],
                decks_req=KICK_COLOSSEUM_SAVE_DECKS,
                fame_label=fame_label,
                fame=stats["fame"],
                fame_req=KICK_COLOSSEUM_SAVE_FAME,
                result=result_text,
            )
        else:
            detail_line = t(
                "kick_v2_colosseum_latest_detail_line",
                lang,
                colosseum=colosseum_label,
                season_label=season_label,
                season=na_value,
                week_label=week_label,
                week=na_value,
                decks_label=decks_label,
                decks=na_value,
                decks_req=KICK_COLOSSEUM_SAVE_DECKS,
                fame_label=fame_label,
                fame=na_value,
                fame_req=KICK_COLOSSEUM_SAVE_FAME,
                result=t("kick_v2_value_na", lang),
            )

        return (
            history_line,
            detail_line,
            eligible_results,
            has_unknown,
            eligibility_statuses,
        )

    candidate_ordered: list[tuple[str, dict[str, object]]] = []
    control_credit: list[dict[str, object]] = []
    not_applicable: list[dict[str, object]] = []
    revived_activity: list[dict[str, object]] = []
    new_members: list[dict[str, object]] = []

    has_unknown_colosseum = any(
        eligible_tags_map.get(week) is None for week in colosseum_weeks
    )

    for row in inactive:
        tag = row.get("player_tag")
        if not tag:
            continue
        normalized_tag = _normalize_tag(tag)
        weeks_played = int(history_counts.get(normalized_tag, 0))
        last_decks = int(last_week_decks.get(normalized_tag, 0))
        wtd_donations = None
        if normalized_tag in donations_wtd:
            wtd_donations = donations_wtd[normalized_tag].get("donations")
        last_seen = last_seen_map.get(normalized_tag)

        (
            history_line,
            detail_line,
            eligible_results,
            has_unknown,
            eligibility_statuses,
        ) = _build_colosseum_lines(normalized_tag)

        entry = {
            "player_tag": normalized_tag,
            "player_name": row.get("player_name") or t("unknown", lang),
            "decks_used": int(row.get("decks_used", 0)),
            "fame": int(row.get("fame", 0)),
            "last_week_decks": last_decks,
            "donations_wtd": wtd_donations,
            "weeks_played": weeks_played,
            "last_seen": last_seen,
            "history_line": history_line,
            "detail_line": detail_line,
            "eligibility_statuses": eligibility_statuses,
        }

        if weeks_played <= NEW_MEMBER_WEEKS_PLAYED:
            if last_decks < REVIVED_DECKS_THRESHOLD:
                new_members.append(entry)
            continue

        fail_streak = 0
        for result in eligible_results:
            if result == "fail":
                fail_streak += 1
            else:
                break
        classification = None
        if fail_streak >= 2:
            entry["reason"] = t("kick_v2_reason_streak", lang)
            entry["short_status_key"] = "kick_short_status_fail_streak"
            classification = "streak"
        elif fail_streak == 1:
            if (
                len(eligible_results) >= 3
                and eligible_results[1] == "pass"
                and eligible_results[2] == "pass"
            ):
                entry["reason"] = t("kick_v2_reason_credit", lang)
                classification = "control"
            else:
                entry["reason"] = t("kick_v2_reason_last_fail", lang)
                entry["short_status_key"] = "kick_short_status_fail_last"
                classification = "last_fail"
        elif not eligible_results:
            if has_unknown or has_unknown_colosseum or not colosseum_weeks:
                entry["reason"] = t("kick_v2_reason_unknown_colosseum", lang)
            else:
                entry["reason"] = t("kick_v2_reason_not_applicable", lang)
            entry["short_status_key"] = "kick_short_status_not_applicable"
            classification = "not_applicable"

        entry["fail_streak"] = fail_streak
        entry["classification"] = classification

        if last_decks >= REVIVED_DECKS_THRESHOLD:
            entry["short_status_key"] = "kick_short_status_revived"
            revived_activity.append(entry)
            continue

        if classification == "streak":
            candidate_ordered.append(("streak", entry))
        elif classification == "last_fail":
            candidate_ordered.append(("last_fail", entry))
        elif classification == "control":
            control_credit.append(entry)
        elif classification == "not_applicable":
            not_applicable.append(entry)

    limited_candidates = candidate_ordered[: max(KICK_SHORTLIST_LIMIT, 0)]
    candidates_streak: list[dict[str, object]] = []
    candidates_last_fail: list[dict[str, object]] = []
    for category, entry in limited_candidates:
        if category == "streak":
            candidates_streak.append(entry)
        else:
            candidates_last_fail.append(entry)
    candidates_combined = [entry for _, entry in limited_candidates]

    if not detailed:
        lines = [
            HEADER_LINE,
            t("kick_short_title", lang),
            DIVIDER_LINE,
            t("kick_short_rule_colosseum", lang),
            t("kick_short_rule_eligibility", lang),
            t("kick_short_rule_streak", lang),
        ]
        if not weeks or not last_week or not inactive or not inactive_tags:
            lines.append("")
            lines.append(t("kick_shortlist_none", lang))
            return "\n".join(lines)

        short_last_week_label = t("kick_v2_label_last_week", lang)
        short_decks_unit = t("kick_short_decks_unit", lang)
        short_weeks_label = t("kick_v2_label_weeks_in_clan", lang)
        short_last_seen_label = t("kick_v2_label_last_seen", lang)

        def _append_short_section(
            header_key: str, entries: list[dict[str, object]]
        ) -> None:
            if not entries:
                return
            lines.extend(["", t(header_key, lang)])
            for index, row in enumerate(entries, 1):
                display_name = row.get("player_name") or t("unknown", lang)
                name = _format_name(display_name, lang).rstrip()
                lines.append(
                    t(
                        "kick_short_player_line_name",
                        lang,
                        index=index,
                        name=name,
                    )
                )
                status_key = row.get("short_status_key")
                if status_key:
                    lines.append(t(status_key, lang))
                last_seen_text = _format_kick_v2_last_seen(
                    row.get("last_seen"), now_utc, lang
                )
                lines.append(
                    t(
                        "kick_short_stats_line",
                        lang,
                        last_week_label=short_last_week_label,
                        last_week=row.get("last_week_decks", 0),
                        decks_unit=short_decks_unit,
                        weeks_label=short_weeks_label,
                        weeks=row.get("weeks_played", 0),
                        last_seen_label=short_last_seen_label,
                        last_seen=last_seen_text,
                    )
                )

        def _append_new_members_short(
            entries: list[dict[str, object]]
        ) -> None:
            if not entries:
                return
            lines.extend(["", t("kick_short_section_new_members", lang)])
            for row in entries:
                display_name = row.get("player_name") or t("unknown", lang)
                name = _format_name(display_name, lang).rstrip()
                lines.append(
                    t(
                        "kick_short_new_member_bullet",
                        lang,
                        name=name,
                        weeks=row.get("weeks_played", 0),
                    )
                )

        _append_short_section(
            "kick_short_section_candidates", candidates_combined
        )
        _append_short_section(
            "kick_short_section_not_applicable", not_applicable
        )
        _append_short_section(
            "kick_short_section_revived", revived_activity
        )
        _append_new_members_short(new_members)

        if (
            not candidates_combined
            and not not_applicable
            and not revived_activity
            and not new_members
        ):
            lines.append("")
            lines.append(t("kick_shortlist_none", lang))

        return "\n".join(lines)

    lines = [
        HEADER_LINE,
        t("kick_report_title", lang),
        HEADER_LINE,
        t(
            "kick_v2_window_line",
            lang,
            weeks=len(weeks or []),
            col_weeks=col_weeks_count,
        ),
        t("kick_v2_rules_header", lang),
        t("kick_v2_rule_colosseum_priority", lang),
        t("kick_v2_rule_eligibility_cutoff", lang),
        t(
            "kick_v2_rule_eligibility_fallback",
            lang,
            start=fallback_start,
            end=fallback_end,
        ),
        t(
            "kick_v2_rule_pass_and",
            lang,
            decks=KICK_COLOSSEUM_SAVE_DECKS,
            fame=KICK_COLOSSEUM_SAVE_FAME,
        ),
        t("kick_v2_rule_trust_credit", lang),
        t("kick_v2_rule_existing_rules_hint", lang),
        "",
        t("kick_report_colosseum_weeks_header", lang),
    ]

    for week in colosseum_weeks or [(None, None)]:
        season_id, section_index = week
        season_text = season_id if season_id is not None else na_value
        week_text = (
            int(section_index) + 1 if isinstance(section_index, int) else na_value
        )
        lines.append(
            t(
                "kick_v2_colosseum_week_line",
                lang,
                colosseum=colosseum_label,
                season_label=season_label,
                season=season_text,
                week_label=week_label,
                week=week_text,
            )
        )
        d0 = d0_map.get(week) if week in d0_map else None
        cutoff = d0 + timedelta(days=2) if isinstance(d0, date) else None
        fallback = fallback_map.get(week) if week in fallback_map else None
        lines.append(
            t(
                "kick_v2_colosseum_week_dates_line",
                lang,
                d0=_format_kick_v2_date(d0, lang),
                cutoff=_format_kick_v2_date(cutoff, lang),
                fallback=_format_kick_v2_date(fallback, lang),
            )
        )
        status_key = (
            "kick_report_status_unknown"
            if d0 is None or fallback is None
            else "kick_report_status_ok"
        )
        lines.append(
            t(
                "kick_report_colosseum_week_status_line",
                lang,
                status=t(status_key, lang),
            )
        )

    if not weeks or not last_week or not inactive or not inactive_tags:
        lines.append("")
        lines.append(t("kick_shortlist_none", lang))
        return "\n".join(lines)

    def _format_eligibility_line(
        statuses: list[tuple[int, int, str]] | None
    ) -> str:
        parts: list[str] = []
        for season_id, section_index, status in statuses or []:
            if status == "eligible":
                status_text = t("kick_report_eligibility_eligible", lang)
            elif status == "not_applicable":
                status_text = t("kick_report_eligibility_not_applicable", lang)
            else:
                status_text = t("kick_report_eligibility_unknown", lang)
            week_text = (
                int(section_index) + 1
                if isinstance(section_index, int)
                else section_index
            )
            parts.append(
                f"[{season_label}{season_id} {week_label}{week_text} {status_text}]"
            )
        text = " ".join(parts) if parts else t("kick_v2_value_na", lang)
        return t("kick_report_eligibility_line", lang, eligibility=text)

    def _append_section(
        header_key: str, entries: list[dict[str, object]]
    ) -> None:
        if not entries:
            return
        lines.extend(["", t(header_key, lang)])
        for index, row in enumerate(entries, 1):
            reason = row.get("reason") or ""
            reason_suffix = f" — {reason}" if reason else ""
            display_name = row.get("player_name") or t("unknown", lang)
            name = _format_name(display_name, lang).rstrip()
            last_seen_text = _format_kick_v2_last_seen(
                row.get("last_seen"), now_utc, lang
            )
            lines.append(
                t(
                    "kick_v2_player_line_main",
                    lang,
                    index=index,
                    name=name,
                    reason=reason_suffix,
                    decks_label=decks_label,
                    decks=row.get("decks_used", 0),
                    fame_label=fame_label,
                    fame=row.get("fame", 0),
                    last_week_label=last_week_label,
                    last_week=row.get("last_week_decks", 0),
                    donations_label=donations_label,
                    donations=_format_kick_v2_value(
                        row.get("donations_wtd"), lang
                    ),
                    weeks_label=weeks_label,
                    weeks=row.get("weeks_played", 0),
                    last_seen_label=last_seen_label,
                    last_seen=last_seen_text,
                )
            )
            lines.append(row.get("history_line") or "")
            lines.append(row.get("detail_line") or "")
            lines.append(
                _format_eligibility_line(row.get("eligibility_statuses"))
            )
            lines.append(
                t(
                    "kick_report_debug_reason_line",
                    lang,
                    fail_streak=row.get("fail_streak", 0),
                    classification=(
                        t("kick_report_classification_streak", lang)
                        if row.get("classification") == "streak"
                        else t("kick_report_classification_last_fail", lang)
                        if row.get("classification") == "last_fail"
                        else t("kick_report_classification_control", lang)
                        if row.get("classification") == "control"
                        else t("kick_report_classification_not_applicable", lang)
                        if row.get("classification") == "not_applicable"
                        else t("kick_report_classification_none", lang)
                    ),
                    reason=reason or t("kick_report_reason_none", lang),
                )
            )

    def _append_new_members_section(
        entries: list[dict[str, object]]
    ) -> None:
        if not entries:
            return
        lines.extend(["", t("kick_v2_section_new_members", lang)])
        for index, row in enumerate(entries, 1):
            display_name = row.get("player_name") or t("unknown", lang)
            name = _format_name(display_name, lang).rstrip()
            lines.append(
                t(
                    "kick_v2_new_member_line_compact",
                    lang,
                    index=index,
                    name=name,
                    decks_label=decks_label,
                    decks=row.get("decks_used", 0),
                    fame_label=fame_label,
                    fame=row.get("fame", 0),
                    last_week_label=last_week_label,
                    last_week=row.get("last_week_decks", 0),
                    donations_label=donations_label,
                    donations=_format_kick_v2_value(
                        row.get("donations_wtd"), lang
                    ),
                )
            )

    _append_section("kick_v2_section_candidates_streak", candidates_streak)
    _append_section("kick_v2_section_candidates_last_fail", candidates_last_fail)
    _append_section("kick_v2_section_control_credit", control_credit)
    _append_section("kick_v2_section_not_applicable", not_applicable)
    _append_new_members_section(new_members)
    _append_section("kick_v2_section_revived_activity", revived_activity)

    if (
        not candidates_streak
        and not candidates_last_fail
        and not control_credit
        and not not_applicable
        and not new_members
        and not revived_activity
    ):
        lines.append("")
        lines.append(t("kick_shortlist_none", lang))

    return "\n".join(lines)


async def build_kick_debug_report(
    weeks: list[tuple[int, int]],
    last_week: tuple[int, int] | None,
    clan_tag: str,
    *,
    lang: str = DEFAULT_LANG,
) -> str:
    return await build_kick_shortlist_report(
        weeks, last_week, clan_tag, lang=lang, detailed=True
    )


async def build_kick_newbie_report(
    clan_tag: str,
    *,
    lang: str = DEFAULT_LANG,
    limit: int = 10,
) -> str:
    lines = [
        HEADER_LINE,
        t("kick_newbie_title", lang),
        HEADER_LINE,
        t("kick_newbie_rules", lang, limit=limit),
    ]
    async with get_session() as session:
        members = await get_current_members_snapshot(
            clan_tag, session=session
        )
        if not members:
            lines.append(t("kick_newbie_no_snapshot", lang))
            return "\n".join(lines)
        members = _filter_protected(members)
        tags = {
            _normalize_tag(row.get("player_tag"))
            for row in members
            if row.get("player_tag")
        }
        if not tags:
            lines.append(t("kick_newbie_none", lang))
            return "\n".join(lines)
        history_counts = await get_participation_week_counts(
            player_tags=tags, session=session
        )
        totals_result = await session.execute(
            select(
                PlayerParticipation.player_tag,
                func.sum(PlayerParticipation.decks_used).label("decks_sum"),
                func.sum(PlayerParticipation.fame).label("fame_sum"),
            )
            .where(PlayerParticipation.player_tag.in_(tags))
            .group_by(PlayerParticipation.player_tag)
        )
        activity_map = {
            row.player_tag: {
                "decks_sum": int(row.decks_sum or 0),
                "fame_sum": int(row.fame_sum or 0),
            }
            for row in totals_result.all()
        }

    candidates: list[dict[str, object]] = []
    for row in members:
        tag = _normalize_tag(row.get("player_tag"))
        if not tag:
            continue
        full_weeks = int(history_counts.get(tag, 0))
        if full_weeks > 2:
            continue
        totals = activity_map.get(tag, {})
        decks_sum = int(totals.get("decks_sum", 0))
        fame_sum = int(totals.get("fame_sum", 0))
        candidates.append(
            {
                "player_tag": tag,
                "player_name": row.get("player_name") or t("unknown", lang),
                "weeks_in_clan": full_weeks,
                "decks_sum": decks_sum,
                "fame_sum": fame_sum,
            }
        )

    if not candidates:
        lines.append(t("kick_newbie_none", lang))
        return "\n".join(lines)

    candidates.sort(
        key=lambda row: (
            int(row.get("decks_sum", 0)),
            int(row.get("fame_sum", 0)),
            str(row.get("player_name", "")),
        )
    )
    for index, row in enumerate(candidates[: max(limit, 0)], 1):
        lines.append(
            t(
                "kick_newbie_line",
                lang,
                index=index,
                name=row.get("player_name"),
                tag=row.get("player_tag"),
                weeks=row.get("weeks_in_clan"),
                decks=row.get("decks_sum"),
                fame=row.get("fame_sum"),
            )
        )
    return "\n".join(lines)


async def build_tg_list_report(
    clan_tag: str,
    *,
    lang: str = DEFAULT_LANG,
    entries: list[dict[str, str]] | None = None,
) -> str:
    lines = [HEADER_LINE, t("tg_title", lang), HEADER_LINE]
    entries = entries or []
    if not entries:
        lines.append(t("tg_no_users", lang))
        return "\n".join(lines)
    for index, row in enumerate(entries, 1):
        lines.append(
            t(
                "tg_line",
                lang,
                index=index,
                name=row.get("name"),
                username=row.get("username"),
            )
        )
    return "\n".join(lines)


async def build_current_war_report(
    clan_tag: str, *, lang: str = DEFAULT_LANG
) -> str:
    active_week = await _resolve_active_week_key(clan_tag)
    state = None
    if active_week:
        state = await get_river_race_state_for_week(
            clan_tag, active_week[0], active_week[1]
        )
    if not state:
        state = await get_latest_river_race_state(clan_tag)
        if state:
            active_week = (int(state["season_id"]), int(state["section_index"]))

    if active_week:
        season_id, section_index = active_week
    else:
        season_id, section_index = 0, 0

    is_colosseum = bool(state.get("is_colosseum")) if state else False
    period_type = (state.get("period_type") if state else None) or None
    period_type_upper = period_type.upper() if isinstance(period_type, str) else None
    period_index = _coerce_int(state.get("period_index")) if state else None
    last_update = _format_timestamp(
        state.get("updated_at") if state else None, lang
    )

    period_type_label = t("unknown", lang)
    if period_type_upper == "WAR_DAY":
        period_type_label = t("period_type_war_day", lang)
    elif period_type_upper == "COLOSSEUM":
        period_type_label = t("period_type_colosseum", lang)
    elif period_type_upper == "TRAINING":
        period_type_label = t("period_type_training", lang)

    if period_type_upper == "WAR_DAY" and period_index is not None:
        day_display = t(
            "current_war_day_count", lang, day=period_index + 1, total=4
        )
    elif period_type_upper == "COLOSSEUM":
        day_display = t("current_war_day_count", lang, day=1, total=1)
    elif period_type_upper == "TRAINING":
        day_display = t("current_war_day_training", lang)
    else:
        day_display = t("unknown", lang)

    if period_type_upper == "WAR_DAY" and period_index is not None:
        remaining_display = t(
            "current_war_remaining_war_days",
            lang,
            days=max(0, 4 - (period_index + 1)),
        )
    elif period_type_upper == "COLOSSEUM":
        remaining_display = t("current_war_remaining_none", lang)
    elif period_type_upper == "TRAINING":
        remaining_display = t("current_war_remaining_training", lang)
    else:
        remaining_display = t("unknown", lang)

    phase_line = t(
        "current_war_phase_line",
        lang,
        phase=period_type_label,
        day=day_display,
    )
    colosseum_label = (
        t("current_war_colosseum", lang)
        if is_colosseum
        else t("current_war_river_race", lang)
    )

    if period_type_upper == "TRAINING":
        last_completed_line = None
        async with get_session() as session:
            last_completed_result = await session.execute(
                select(
                    RiverRaceState.season_id,
                    RiverRaceState.section_index,
                    RiverRaceState.is_colosseum,
                )
                .where(
                    RiverRaceState.clan_tag == clan_tag,
                    RiverRaceState.period_type == "completed",
                )
                .order_by(
                    RiverRaceState.season_id.desc(),
                    RiverRaceState.section_index.desc(),
                )
                .limit(1)
            )
            last_completed_row = last_completed_result.first()
            if last_completed_row:
                last_completed_war_type = (
                    t("current_war_colosseum", lang)
                    if bool(last_completed_row.is_colosseum)
                    else t("current_war_river_race", lang)
                )
                last_completed_line = t(
                    "current_war_training_last_completed",
                    lang,
                    season=int(last_completed_row.season_id),
                    week=int(last_completed_row.section_index) + 1,
                    war_type=last_completed_war_type,
                )
        lines = [
            t("current_war_title", lang),
            t("current_war_clan_line", lang, clan=clan_tag),
            t("current_war_data_notice", lang),
            t("current_war_last_update", lang, last_update=last_update),
            "",
            SEPARATOR_LINE,
            phase_line,
            t("current_war_remaining_line", lang, remaining=remaining_display),
            "",
            t("current_war_training_msg_1", lang),
            t("current_war_training_msg_2", lang),
        ]
        if last_completed_line:
            lines.append(last_completed_line)
        lines.append(t("current_war_training_msg_4", lang))
        return "\n".join(lines)

    member_tags = await get_current_member_tags(clan_tag)
    total_decks = 0
    total_fame = 0
    member_count = 0
    top_rows: list[dict[str, object]] = []
    bottom_rows: list[dict[str, object]] = []
    if member_tags and season_id > 0:
        async with get_session() as session:
            totals_result = await session.execute(
                select(
                    func.sum(PlayerParticipation.decks_used).label("decks_sum"),
                    func.sum(PlayerParticipation.fame).label("fame_sum"),
                    func.count().label("member_count"),
                ).where(
                    PlayerParticipation.season_id == season_id,
                    PlayerParticipation.section_index == section_index,
                    PlayerParticipation.player_tag.in_(member_tags),
                )
            )
            totals = totals_result.first()
            if totals:
                total_decks = int(totals.decks_sum or 0)
                total_fame = int(totals.fame_sum or 0)
                member_count = int(totals.member_count or 0)

            base_query = select(
                PlayerParticipation.player_tag,
                PlayerParticipation.player_name,
                PlayerParticipation.decks_used,
                PlayerParticipation.fame,
            ).where(
                PlayerParticipation.season_id == season_id,
                PlayerParticipation.section_index == section_index,
                PlayerParticipation.player_tag.in_(member_tags),
            )
            top_result = await session.execute(
                base_query.order_by(
                    PlayerParticipation.decks_used.desc(),
                    PlayerParticipation.fame.desc(),
                ).limit(10)
            )
            top_rows = [
                {
                    "player_tag": row.player_tag,
                    "player_name": row.player_name,
                    "decks_used": int(row.decks_used),
                    "fame": int(row.fame),
                }
                for row in top_result.all()
            ]

            bottom_result = await session.execute(
                base_query.order_by(
                    PlayerParticipation.decks_used.asc(),
                    PlayerParticipation.fame.asc(),
                ).limit(60)
            )
            bottom_rows = [
                {
                    "player_tag": row.player_tag,
                    "player_name": row.player_name,
                    "decks_used": int(row.decks_used),
                    "fame": int(row.fame),
                }
                for row in bottom_result.all()
            ]

    filtered_bottom = [
        row
        for row in bottom_rows
        if _normalize_tag(row.get("player_tag")) not in PROTECTED_TAGS_NORMALIZED
    ][:10]

    lines = [
        t("current_war_title", lang),
        t("current_war_clan_line", lang, clan=clan_tag),
        t("current_war_data_notice", lang),
        t("current_war_last_update", lang, last_update=last_update),
        "",
        SEPARATOR_LINE,
        t(
            "current_war_week_line",
            lang,
            season=season_id,
            week=section_index + 1,
            war_type=colosseum_label,
        ),
        phase_line,
        t("current_war_remaining_line", lang, remaining=remaining_display),
        "",
        t("current_war_structure_header", lang),
        t("current_war_structure_line", lang),
        t("current_war_structure_note", lang),
        "",
        SEPARATOR_LINE,
        t("current_war_totals_header", lang),
        t("current_war_total_decks", lang, decks=total_decks),
        t("current_war_total_fame", lang, fame=total_fame),
        t("current_war_members_count", lang, members=member_count),
        "",
        SEPARATOR_LINE,
        t("current_war_top_header", lang),
    ]
    if top_rows:
        for index, row in enumerate(top_rows, 1):
            name = row.get("player_name") or row.get("player_tag") or t(
                "unknown", lang
            )
            lines.append(
                t(
                    "current_war_entry_line",
                    lang,
                    index=index,
                    name=name,
                    decks=row.get("decks_used", 0),
                    fame=row.get("fame", 0),
                )
            )
    else:
        lines.append(t("report_no_data", lang))

    lines.extend(
        [
            "",
            SEPARATOR_LINE,
            t("current_war_bottom_header", lang),
        ]
    )
    if filtered_bottom:
        for index, row in enumerate(filtered_bottom, 1):
            name = row.get("player_name") or row.get("player_tag") or t(
                "unknown", lang
            )
            lines.append(
                t(
                    "current_war_entry_line",
                    lang,
                    index=index,
                    name=name,
                    decks=row.get("decks_used", 0),
                    fame=row.get("fame", 0),
                )
            )
    else:
        lines.append(t("report_no_data", lang))

    return "\n".join(lines)


async def build_my_activity_report(
    player_tag: str,
    player_name: str,
    clan_tag: str,
    lang: str = DEFAULT_LANG,
) -> str:
    active_week = await _resolve_active_week_key(clan_tag)
    state = None
    if active_week:
        state = await get_river_race_state_for_week(
            clan_tag, active_week[0], active_week[1]
        )
    if not state:
        state = await get_latest_river_race_state(clan_tag)
        if state:
            active_week = (int(state["season_id"]), int(state["section_index"]))

    if active_week:
        season_id, section_index = active_week
    else:
        season_id, section_index = 0, 0

    is_colosseum = bool(state.get("is_colosseum")) if state else False
    colosseum_label = (
        t("current_war_colosseum", lang)
        if is_colosseum
        else t("current_war_river_race", lang)
    )
    period_type = (state.get("period_type") if state else None) or None
    period_type_lower = (
        period_type.lower() if isinstance(period_type, str) else None
    )
    training = period_type_lower == "training"

    member_tags = await get_current_member_tags(clan_tag)
    member_count = len(member_tags)
    member_snapshot = await get_current_members_snapshot(clan_tag)
    role_value = None
    target_tag = _normalize_tag(player_tag)
    for row in member_snapshot:
        if _normalize_tag(row.get("player_tag")) == target_tag:
            role_value = row.get("role")
            break
    role_text = _normalize_role(role_value)
    if role_text == "leader":
        role_label = t("my_activity_role_leader", lang)
    elif role_text == "coleader":
        role_label = t("my_activity_role_coleader", lang)
    elif role_text == "elder":
        role_label = t("my_activity_role_elder", lang)
    elif role_text == "member":
        role_label = t("my_activity_role_member", lang)
    else:
        role_label = t("my_activity_role_unknown", lang)
    role_line = t("my_activity_role_line", lang, role=role_label)

    current_decks = 0
    current_fame = 0
    rank_decks: str | int = t("na", lang)
    rank_fame: str | int = t("na", lang)
    last_completed_week_line: str | None = None
    last_completed_stats_line: str | None = None
    last_completed_rank_line: str | None = None

    async with get_session() as session:
        week_rows: list[dict[str, object]] = []
        if member_tags and season_id > 0:
            week_result = await session.execute(
                select(
                    PlayerParticipation.player_tag,
                    PlayerParticipation.player_name,
                    PlayerParticipation.decks_used,
                    PlayerParticipation.fame,
                ).where(
                    PlayerParticipation.season_id == season_id,
                    PlayerParticipation.section_index == section_index,
                    PlayerParticipation.player_tag.in_(member_tags),
                )
            )
            week_rows = [
                {
                    "player_tag": row.player_tag,
                    "player_name": row.player_name,
                    "decks_used": int(row.decks_used),
                    "fame": int(row.fame),
                }
                for row in week_result.all()
            ]
            for row in week_rows:
                if row["player_tag"] == player_tag:
                    current_decks = int(row["decks_used"])
                    current_fame = int(row["fame"])
                    break

            if week_rows:
                decks_sorted = sorted(
                    week_rows,
                    key=lambda row: (-int(row["decks_used"]), -int(row["fame"])),
                )
                fame_sorted = sorted(
                    week_rows,
                    key=lambda row: (-int(row["fame"]), -int(row["decks_used"])),
                )
                for index, row in enumerate(decks_sorted, 1):
                    if row["player_tag"] == player_tag:
                        rank_decks = index
                        break
                for index, row in enumerate(fame_sorted, 1):
                    if row["player_tag"] == player_tag:
                        rank_fame = index
                        break

        if (current_decks == 0 and current_fame == 0) and season_id > 0:
            user_week = await session.execute(
                select(
                    PlayerParticipation.decks_used,
                    PlayerParticipation.fame,
                ).where(
                    PlayerParticipation.season_id == season_id,
                    PlayerParticipation.section_index == section_index,
                    PlayerParticipation.player_tag == player_tag,
                )
            )
            row = user_week.first()
            if row:
                current_decks = int(row.decks_used or 0)
                current_fame = int(row.fame or 0)
                rank_decks = t("na", lang)
                rank_fame = t("na", lang)

        if training:
            last_completed_rank_decks: str | int = t("na", lang)
            last_completed_rank_fame: str | int = t("na", lang)
            last_completed_total: str | int = t("na", lang)
            last_completed_result = await session.execute(
                select(
                    RiverRaceState.season_id,
                    RiverRaceState.section_index,
                    RiverRaceState.is_colosseum,
                )
                .where(
                    RiverRaceState.clan_tag == clan_tag,
                    RiverRaceState.period_type == "completed",
                )
                .order_by(
                    RiverRaceState.season_id.desc(),
                    RiverRaceState.section_index.desc(),
                )
                .limit(1)
            )
            last_completed_row = last_completed_result.first()
            if last_completed_row:
                last_completed_season = int(last_completed_row.season_id)
                last_completed_section = int(last_completed_row.section_index)
                last_completed_war_type = (
                    t("current_war_colosseum", lang)
                    if bool(last_completed_row.is_colosseum)
                    else t("current_war_river_race", lang)
                )
                last_completed_week_line = t(
                    "my_activity_last_completed_week",
                    lang,
                    season=last_completed_season,
                    week=last_completed_section + 1,
                    war_type=last_completed_war_type,
                )
                last_completed_user = await session.execute(
                    select(
                        PlayerParticipation.decks_used,
                        PlayerParticipation.fame,
                    ).where(
                        PlayerParticipation.player_tag == player_tag,
                        PlayerParticipation.season_id == last_completed_season,
                        PlayerParticipation.section_index
                        == last_completed_section,
                    )
                )
                last_completed_user_row = last_completed_user.first()
                if last_completed_user_row:
                    last_completed_stats_line = t(
                        "my_activity_last_completed_stats",
                        lang,
                        decks=int(last_completed_user_row.decks_used or 0),
                        fame=int(last_completed_user_row.fame or 0),
                    )
                else:
                    last_completed_stats_line = t(
                        "my_activity_last_completed_stats_none", lang
                    )

                if member_tags:
                    last_week_result = await session.execute(
                        select(
                            PlayerParticipation.player_tag,
                            PlayerParticipation.decks_used,
                            PlayerParticipation.fame,
                        ).where(
                            PlayerParticipation.season_id
                            == last_completed_season,
                            PlayerParticipation.section_index
                            == last_completed_section,
                            PlayerParticipation.player_tag.in_(member_tags),
                        )
                    )
                    last_week_rows = [
                        {
                            "player_tag": row.player_tag,
                            "decks_used": int(row.decks_used),
                            "fame": int(row.fame),
                        }
                        for row in last_week_result.all()
                    ]
                    if last_week_rows:
                        decks_sorted = sorted(
                            last_week_rows,
                            key=lambda row: (
                                -int(row["decks_used"]),
                                -int(row["fame"]),
                            ),
                        )
                        fame_sorted = sorted(
                            last_week_rows,
                            key=lambda row: (
                                -int(row["fame"]),
                                -int(row["decks_used"]),
                            ),
                        )
                        for index, row in enumerate(decks_sorted, 1):
                            if row["player_tag"] == player_tag:
                                last_completed_rank_decks = index
                                break
                        for index, row in enumerate(fame_sorted, 1):
                            if row["player_tag"] == player_tag:
                                last_completed_rank_fame = index
                                break
                        last_completed_total = member_count

                last_completed_rank_line = t(
                    "my_activity_last_completed_rank",
                    lang,
                    rank_decks=last_completed_rank_decks,
                    rank_fame=last_completed_rank_fame,
                    total=last_completed_total,
                )
            if last_completed_stats_line is None:
                last_completed_stats_line = t(
                    "my_activity_last_completed_stats_none", lang
                )
            if last_completed_rank_line is None:
                last_completed_rank_line = t(
                    "my_activity_last_completed_rank",
                    lang,
                    rank_decks=t("na", lang),
                    rank_fame=t("na", lang),
                    total=t("na", lang),
                )

        weeks = await get_last_weeks_from_db(clan_tag, limit=8)
        user_rows: list[tuple[int, int]] = []
        if weeks:
            user_result = await session.execute(
                select(
                    PlayerParticipation.decks_used,
                    PlayerParticipation.fame,
                ).where(
                    PlayerParticipation.player_tag == player_tag,
                    tuple_(
                        PlayerParticipation.season_id,
                        PlayerParticipation.section_index,
                    ).in_(weeks),
                )
            )
            user_rows = [
                (int(row.decks_used), int(row.fame)) for row in user_result.all()
            ]

        weeks_available = len(user_rows)
        active_weeks = sum(1 for decks, _ in user_rows if decks >= 8)
        low_weeks = sum(1 for decks, _ in user_rows if 1 <= decks <= 7)
        zero_weeks = sum(1 for decks, _ in user_rows if decks == 0)
        total_user_decks = sum(decks for decks, _ in user_rows)
        total_user_fame = sum(fame for _, fame in user_rows)
        avg_user_decks = (
            total_user_decks / weeks_available if weeks_available else 0.0
        )
        avg_user_fame = total_user_fame / weeks_available if weeks_available else 0.0

        clan_avg_decks = 0.0
        clan_avg_fame = 0.0
        if weeks and member_tags:
            totals = await session.execute(
                select(
                    func.sum(PlayerParticipation.decks_used).label("decks_sum"),
                    func.sum(PlayerParticipation.fame).label("fame_sum"),
                ).where(
                    tuple_(
                        PlayerParticipation.season_id,
                        PlayerParticipation.section_index,
                    ).in_(weeks),
                    PlayerParticipation.player_tag.in_(member_tags),
                )
            )
            totals_row = totals.first()
            if totals_row:
                total_decks = float(totals_row.decks_sum or 0)
                total_fame = float(totals_row.fame_sum or 0)
                denominator = max(1, len(weeks) * len(member_tags))
                clan_avg_decks = total_decks / denominator
                clan_avg_fame = total_fame / denominator

    decks_comp = _compare_to_avg(avg_user_decks, clan_avg_decks, lang)
    fame_comp = _compare_to_avg(avg_user_fame, clan_avg_fame, lang)

    donations_wtd_map = await get_current_wtd_donations(
        clan_tag, player_tags={_normalize_tag(player_tag)}
    )
    donations_8w_map = await get_donations_weekly_sums(
        clan_tag,
        player_tags={_normalize_tag(player_tag)},
        window_weeks=DONATION_WEEKS_WINDOW,
    )
    last_seen_map = await get_last_seen_map(clan_tag)
    last_seen = last_seen_map.get(_normalize_tag(player_tag))
    now_utc = datetime.now(timezone.utc)
    days_absent = _days_absent(last_seen, now_utc)
    if isinstance(last_seen, datetime):
        last_seen_ts = last_seen.astimezone(timezone.utc).strftime(
            "%Y-%m-%d %H:%M UTC"
        )
        last_seen_line = t(
            "my_activity_last_seen",
            lang,
            last_seen=last_seen_ts,
            days=days_absent,
            absence=_absence_label(days_absent, lang),
        )
    else:
        last_seen_line = t("my_activity_last_seen_na", lang)
    tag_key = _normalize_tag(player_tag)
    wtd_entry = donations_wtd_map.get(tag_key, {})
    wtd_donations = wtd_entry.get("donations")
    wtd_received = wtd_entry.get("donations_received")
    clan_avg_wtd = await get_clan_wtd_donation_average(clan_tag)
    donation_sum = 0
    donation_weeks = 0
    if DONATION_WEEKS_WINDOW > 0 and tag_key in donations_8w_map:
        donation_sum = int(donations_8w_map[tag_key].get("sum", 0))
        donation_weeks = int(donations_8w_map[tag_key].get("weeks_present", 0))

    if training:
        if avg_user_decks >= clan_avg_decks or avg_user_fame >= clan_avg_fame:
            status_label = t("my_activity_training_status_active", lang)
            reason_line = t("my_activity_training_reason_above_avg", lang)
        else:
            status_label = t("my_activity_training_status_inactive", lang)
            reason_line = t("my_activity_training_reason_below_avg", lang)
    elif current_decks == 0:
        status_label = t("my_activity_status_danger", lang)
        reason_line = t("my_activity_reason_zero_decks", lang)
    elif avg_user_decks < clan_avg_decks and avg_user_fame < clan_avg_fame:
        status_label = t("my_activity_status_risk", lang)
        reason_line = t("my_activity_reason_below_avg", lang)
    else:
        status_label = t("my_activity_status_safe", lang)
        reason_line = t("my_activity_reason_solid", lang)

    avg_user_decks_str = _format_avg(avg_user_decks)
    avg_user_fame_str = _format_avg(avg_user_fame)
    clan_avg_decks_str = _format_avg(clan_avg_decks)
    clan_avg_fame_str = _format_avg(clan_avg_fame)
    wtd_donations_text = (
        str(wtd_donations) if wtd_donations is not None else t("na", lang)
    )
    wtd_received_text = (
        str(wtd_received) if wtd_received is not None else t("na", lang)
    )
    if wtd_donations is None or clan_avg_wtd is None:
        donation_compare_line = t("my_activity_donations_compare_na", lang)
    else:
        donation_compare_line = t(
            "my_activity_donations_compare",
            lang,
            you=wtd_donations,
            clan=clan_avg_wtd,
            compare=_compare_simple(int(wtd_donations), int(clan_avg_wtd), lang),
        )
    donation_lines = [
        t("my_activity_donations_header", lang),
        t(
            "my_activity_donations_wtd",
            lang,
            donations=wtd_donations_text,
            received=wtd_received_text,
        ),
    ]
    if DONATION_WEEKS_WINDOW > 0:
        donation_lines.append(
            t(
                "my_activity_donations_window",
                lang,
                window=DONATION_WEEKS_WINDOW,
                total=donation_sum,
                weeks=donation_weeks,
            )
        )
    if DONATION_WEEKS_WINDOW > 0:
        if donation_weeks > 0:
            avg_donations = donation_sum / donation_weeks
            donation_lines.append(
                t(
                    "my_activity_donations_avg",
                    lang,
                    avg=f"{avg_donations:.1f}",
                )
            )

    if training:
        training_lines = [
            t(
                "my_activity_training_week_line",
                lang,
                war_type=colosseum_label,
            ),
            t("my_activity_training_notice", lang),
        ]
        if last_completed_week_line:
            training_lines.append(last_completed_week_line)
        if last_completed_stats_line:
            training_lines.append(last_completed_stats_line)
        if last_completed_rank_line:
            training_lines.append(last_completed_rank_line)
        training_lines.append(last_seen_line)
        lines = [
            t("my_activity_title", lang),
            t("my_activity_player_line", lang, player=player_name),
            t("my_activity_tag_line", lang, tag=player_tag),
            t("my_activity_clan_line", lang, clan=clan_tag),
            role_line,
            "",
            SEPARATOR_LINE,
            *training_lines,
            "",
            SEPARATOR_LINE,
            t("my_activity_summary_header", lang),
            t(
                "my_activity_summary_active",
                lang,
                active=active_weeks,
                total=weeks_available,
            ),
            t("my_activity_summary_low", lang, count=low_weeks),
            t("my_activity_summary_zero", lang, count=zero_weeks),
            "",
            t("my_activity_avg_decks_line", lang, avg=avg_user_decks_str),
            t("my_activity_avg_fame_line", lang, avg=avg_user_fame_str),
            "",
            SEPARATOR_LINE,
            t("my_activity_compare_header", lang),
            t(
                "my_activity_compare_decks_line",
                lang,
                you=avg_user_decks_str,
                clan=clan_avg_decks_str,
                compare=decks_comp,
            ),
            t(
                "my_activity_compare_fame_line",
                lang,
                you=avg_user_fame_str,
                clan=clan_avg_fame_str,
                compare=fame_comp,
            ),
            donation_compare_line,
            "",
            SEPARATOR_LINE,
            *donation_lines,
            "",
            SEPARATOR_LINE,
            t("my_activity_status_line", lang, status=status_label),
            reason_line,
        ]
    else:
        lines = [
            t("my_activity_title", lang),
            t("my_activity_player_line", lang, player=player_name),
            t("my_activity_tag_line", lang, tag=player_tag),
            t("my_activity_clan_line", lang, clan=clan_tag),
            role_line,
            "",
            SEPARATOR_LINE,
            t(
                "my_activity_current_week_line",
                lang,
                season=season_id,
                week=section_index + 1,
                war_type=colosseum_label,
            ),
            t("my_activity_decks_used_line", lang, decks=current_decks),
            t("my_activity_fame_line", lang, fame=current_fame),
            last_seen_line,
            "",
            t(
                "my_activity_rank_decks_line",
                lang,
                rank=rank_decks,
                members=member_count,
            ),
            t(
                "my_activity_rank_fame_line",
                lang,
                rank=rank_fame,
                members=member_count,
            ),
            "",
            SEPARATOR_LINE,
            t("my_activity_summary_header", lang),
            t(
                "my_activity_summary_active",
                lang,
                active=active_weeks,
                total=weeks_available,
            ),
            t("my_activity_summary_low", lang, count=low_weeks),
            t("my_activity_summary_zero", lang, count=zero_weeks),
            "",
            t("my_activity_avg_decks_line", lang, avg=avg_user_decks_str),
            t("my_activity_avg_fame_line", lang, avg=avg_user_fame_str),
            "",
            SEPARATOR_LINE,
            t("my_activity_compare_header", lang),
            t(
                "my_activity_compare_decks_line",
                lang,
                you=avg_user_decks_str,
                clan=clan_avg_decks_str,
                compare=decks_comp,
            ),
            t(
                "my_activity_compare_fame_line",
                lang,
                you=avg_user_fame_str,
                clan=clan_avg_fame_str,
                compare=fame_comp,
            ),
            donation_compare_line,
            "",
            SEPARATOR_LINE,
            *donation_lines,
            "",
            SEPARATOR_LINE,
            t("my_activity_status_line", lang, status=status_label),
            reason_line,
        ]

    return "\n".join(lines)
