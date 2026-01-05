"""Report builders for weekly and rolling war summaries."""

from datetime import datetime, timedelta, timezone
from typing import Iterable

from statistics import median

from sqlalchemy import func, select, tuple_

import logging

from cr_api import ClashRoyaleAPIError, get_api_client

from config import (
    DONATION_REVIVE_WTD_THRESHOLD,
    DONATION_WEEKS_WINDOW,
    DONATION_BOX_THRESHOLD,
    KICK_SHORTLIST_LIMIT,
    LAST_SEEN_FLAG_LIMIT,
    LAST_SEEN_RED_DAYS,
    LAST_SEEN_YELLOW_DAYS,
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
    PlayerParticipation,
    get_app_state,
    get_current_member_tags,
    get_current_members_snapshot,
    get_clan_wtd_donation_average,
    get_current_wtd_donations,
    get_current_members_with_wtd_donations,
    get_donation_weekly_sums_for_window,
    get_alltime_weeks_played,
    get_last_weeks_from_db,
    get_last_seen_map,
    get_latest_membership_date,
    get_latest_river_race_state,
    get_member_first_seen_dates,
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
)
from riverrace_import import get_last_completed_weeks
from i18n import DEFAULT_LANG, t

NAME_WIDTH = 20
HEADER_LINE = "══════════════════════════════"
DIVIDER_LINE = "──────────────────────────────"
SEPARATOR_LINE = "---------------------------"

logger = logging.getLogger(__name__)


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
    weeks = await get_last_weeks_from_db(clan_tag, limit=window_weeks)
    if not weeks:
        weeks = await get_last_completed_weeks(window_weeks, clan_tag)
    if not weeks:
        return t("top_no_data", lang)

    current_tags = await get_current_member_tags(clan_tag)
    if not current_tags:
        return t("top_no_data", lang)

    first_seen_map = await get_member_first_seen_dates(
        clan_tag, player_tags=current_tags
    )
    latest_date = await get_latest_membership_date(clan_tag)
    if latest_date is None:
        latest_date = datetime.now(timezone.utc).date()
    min_days = min_tenure_weeks * 7
    eligible_tags = {
        tag
        for tag in current_tags
        if tag in first_seen_map
        and (latest_date - first_seen_map[tag]).days >= min_days
    }
    if not eligible_tags:
        return t("top_no_eligible", lang, weeks=min_tenure_weeks)

    summary_rows = await get_rolling_summary(weeks, player_tags=eligible_tags)
    if not summary_rows:
        return t("top_no_data", lang)

    war_stats = await get_war_stats_for_weeks(clan_tag, weeks)
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
    try:
        api_client = await get_api_client()
        clan = await api_client.get_clan(clan_tag)
        members = await api_client.get_clan_members(clan_tag)
    except ClashRoyaleAPIError as e:
        if e.status_code in (401, 403):
            return t("clan_info_access_denied", lang)
        return t("clan_info_unavailable", lang)
    except Exception as e:
        logger.warning("Failed to fetch clan info: %s", e)
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


async def build_kick_shortlist_report(
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
            display_name = f"{prefix}{row.get('player_name')}"
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

    if warnings:
        lines.extend(
            [
                "",
                t("kick_warnings_revived_header", lang),
            ]
        )
        for index, row in enumerate(warnings, 1):
            name = _format_name(row.get("player_name"), lang).rstrip()
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
            name = _format_name(row.get("player_name"), lang).rstrip()
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
            week=section_index,
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

    member_tags = await get_current_member_tags(clan_tag)
    member_count = len(member_tags)

    current_decks = 0
    current_fame = 0
    rank_decks: str | int = t("na", lang)
    rank_fame: str | int = t("na", lang)

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

    if current_decks == 0:
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

    lines = [
        t("my_activity_title", lang),
        t("my_activity_player_line", lang, player=player_name),
        t("my_activity_tag_line", lang, tag=player_tag),
        t("my_activity_clan_line", lang, clan=clan_tag),
        "",
        SEPARATOR_LINE,
        t(
            "my_activity_current_week_line",
            lang,
            season=season_id,
            week=section_index,
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
