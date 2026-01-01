"""Report builders for weekly and rolling war summaries."""

from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy import func, select, tuple_

from config import (
    DONATION_REVIVE_8W_THRESHOLD,
    DONATION_REVIVE_WTD_THRESHOLD,
    DONATION_WEEKS_WINDOW,
    KICK_SHORTLIST_LIMIT,
    NEW_MEMBER_WEEKS_PLAYED,
    PROTECTED_PLAYER_TAGS,
    REVIVED_DECKS_THRESHOLD,
)
from db import (
    PlayerParticipation,
    get_app_state,
    get_current_member_tags,
    get_current_wtd_donations,
    get_donations_weekly_sums,
    get_last_weeks_from_db,
    get_latest_river_race_state,
    get_participation_week_counts,
    get_river_race_state_for_week,
    get_rolling_leaderboard,
    get_session,
    get_week_decks_map,
    get_week_leaderboard,
)

NAME_WIDTH = 20
HEADER_LINE = "══════════════════════════════"
DIVIDER_LINE = "──────────────────────────────"
SEPARATOR_LINE = "---------------------------"


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


def _format_name(raw_name: object) -> str:
    name = str(raw_name) if raw_name else "Unknown"
    if len(name) > NAME_WIDTH:
        name = f"{name[:NAME_WIDTH - 1]}…"
    return name.ljust(NAME_WIDTH)


def _format_entries(
    entries: Iterable[dict[str, object]],
    donations_wtd: dict[str, dict[str, int | None]] | None = None,
    donations_8w: dict[str, dict[str, int]] | None = None,
    donation_window: int | None = None,
) -> list[str]:
    rows = list(entries)
    if not rows:
        return ["No data available."]

    decks_width = max(len(str(int(row.get("decks_used", 0)))) for row in rows)
    fame_width = max(len(str(int(row.get("fame", 0)))) for row in rows)
    decks_width = max(decks_width, 2)
    fame_width = max(fame_width, 2)

    lines: list[str] = []
    for index, row in enumerate(rows, 1):
        name = _format_name(row.get("player_name"))
        decks_used = int(row.get("decks_used", 0))
        fame = int(row.get("fame", 0))
        suffix = _format_donation_suffix(
            row.get("player_tag"),
            donations_wtd,
            donations_8w,
            donation_window,
        )
        lines.append(
            f"{index:>2}) {name} — decks: {decks_used:>{decks_width}} | fame: {fame:>{fame_width}}{suffix}"
        )
    return lines


def _format_donation_suffix(
    player_tag: object,
    donations_wtd: dict[str, dict[str, int | None]] | None,
    donations_8w: dict[str, dict[str, int]] | None,
    donation_window: int | None,
) -> str:
    tag = _normalize_tag(player_tag)
    wtd_value: int | None = None
    if donations_wtd and tag in donations_wtd:
        wtd_value = donations_wtd[tag].get("donations")
    wtd_text = f"donWTD: {wtd_value}" if wtd_value is not None else "donWTD: n/a"
    parts = [wtd_text]
    if donation_window and donation_window > 0:
        weekly_sum = 0
        weeks_present = 0
        if donations_8w and tag in donations_8w:
            weekly_sum = int(donations_8w[tag].get("sum", 0))
            weeks_present = int(donations_8w[tag].get("weeks_present", 0))
        parts.append(
            f"don{donation_window}w: {weekly_sum} ({weeks_present}/{donation_window})"
        )
    return f" | {' | '.join(parts)}"


async def _collect_donation_maps(
    clan_tag: str,
    tags: set[str],
) -> tuple[dict[str, dict[str, int | None]], dict[str, dict[str, int]]]:
    if not tags:
        return {}, {}
    normalized_tags = {_normalize_tag(tag) for tag in tags if tag}
    donations_wtd = await get_current_wtd_donations(
        clan_tag, player_tags=normalized_tags
    )
    donations_8w: dict[str, dict[str, int]] = {}
    if DONATION_WEEKS_WINDOW > 0:
        donations_8w = await get_donations_weekly_sums(
            clan_tag, player_tags=normalized_tags, window_weeks=DONATION_WEEKS_WINDOW
        )
    return donations_wtd, donations_8w


def _coerce_int(value: object) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _format_avg(value: float) -> str:
    return f"{value:.1f}"


def _compare_to_avg(value: float, avg: float) -> str:
    if avg <= 0:
        return "≈ Near"
    ratio = (value - avg) / avg
    if ratio >= 0.05:
        return "✅ Above"
    if ratio <= -0.05:
        return "⬇️ Below"
    return "≈ Near"


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


def _format_timestamp(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    if isinstance(value, str):
        return value
    if hasattr(value, "astimezone"):
        try:
            return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception:
            return "Unknown"
    return "Unknown"


async def build_weekly_report(
    season_id: int, section_index: int, clan_tag: str
) -> str:
    inactive, active = await get_week_leaderboard(
        season_id=season_id,
        section_index=section_index,
        clan_tag=clan_tag,
    )
    inactive = _filter_protected(inactive)
    tags = {
        _normalize_tag(row.get("player_tag"))
        for row in [*inactive, *active]
        if row.get("player_tag")
    }
    donations_wtd, donations_8w = await _collect_donation_maps(clan_tag, tags)
    member_count = len(await get_current_member_tags(clan_tag))
    lines = [
        HEADER_LINE,
        f"🏁 WAR REPORT — Season {season_id} / Week {section_index + 1}",
        HEADER_LINE,
        f"Members considered: {member_count} (current clan members)",
        "",
        "🧊 TOP 10 INACTIVE (lowest decks, then fame)",
        *_format_entries(
            inactive,
            donations_wtd=donations_wtd,
            donations_8w=donations_8w,
            donation_window=DONATION_WEEKS_WINDOW,
        ),
        "",
        DIVIDER_LINE,
        "",
        "🔥 TOP 10 ACTIVE (highest decks, then fame)",
        *_format_entries(
            active,
            donations_wtd=donations_wtd,
            donations_8w=donations_8w,
            donation_window=DONATION_WEEKS_WINDOW,
        ),
    ]
    return "\n".join(lines)


async def build_rolling_report(
    weeks: list[tuple[int, int]], clan_tag: str
) -> str:
    inactive, active = await get_rolling_leaderboard(
        weeks=weeks,
        clan_tag=clan_tag,
    )
    inactive = _filter_protected(inactive)
    tags = {
        _normalize_tag(row.get("player_tag"))
        for row in [*inactive, *active]
        if row.get("player_tag")
    }
    donations_wtd, donations_8w = await _collect_donation_maps(clan_tag, tags)
    member_count = len(await get_current_member_tags(clan_tag))
    weeks_label = ", ".join(f"{season}/{section + 1}" for season, section in weeks)
    lines = [
        HEADER_LINE,
        f"📊 ROLLING REPORT — Last {len(weeks)} weeks",
        HEADER_LINE,
        f"Members considered: {member_count} (current clan members)",
        f"Weeks: {weeks_label}" if weeks_label else "Weeks: n/a",
        "",
        "🧊 TOP 10 INACTIVE (sum of decks, then fame)",
        *_format_entries(
            inactive,
            donations_wtd=donations_wtd,
            donations_8w=donations_8w,
            donation_window=DONATION_WEEKS_WINDOW,
        ),
        "",
        DIVIDER_LINE,
        "",
        "🔥 TOP 10 ACTIVE (sum of decks, then fame)",
        *_format_entries(
            active,
            donations_wtd=donations_wtd,
            donations_8w=donations_8w,
            donation_window=DONATION_WEEKS_WINDOW,
        ),
    ]
    return "\n".join(lines)


async def build_kick_shortlist_report(
    weeks: list[tuple[int, int]],
    last_week: tuple[int, int] | None,
    clan_tag: str,
) -> str:
    lines = [
        HEADER_LINE,
        "🚪 KICK SHORTLIST — based on last 8 weeks",
        HEADER_LINE,
        "Rules: top-10 inactive (8w) • exclude protected • exclude weeks<=%s • revived>=%s in warnings"
        % (NEW_MEMBER_WEEKS_PLAYED, REVIVED_DECKS_THRESHOLD),
    ]
    if not weeks or not last_week:
        lines.append("No clear kick candidates.")
        return "\n".join(lines)

    async with get_session() as session:
        inactive, _active = await get_rolling_leaderboard(
            weeks=weeks,
            clan_tag=clan_tag,
            session=session,
        )
        if not inactive:
            lines.append("No clear kick candidates.")
            return "\n".join(lines)

        inactive = _filter_protected(inactive)
        inactive_tags = {
            _normalize_tag(row.get("player_tag"))
            for row in inactive
            if row.get("player_tag")
        }
        if not inactive_tags:
            lines.append("No clear kick candidates.")
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

    donations_wtd = await get_current_wtd_donations(clan_tag, player_tags=inactive_tags)
    donations_8w = await get_donations_weekly_sums(
        clan_tag, player_tags=inactive_tags, window_weeks=DONATION_WEEKS_WINDOW
    )

    candidates: list[dict[str, object]] = []
    warnings: list[dict[str, object]] = []
    donation_warnings: list[dict[str, object]] = []
    new_members: list[dict[str, object]] = []
    for row in inactive:
        tag = row.get("player_tag")
        if not tag:
            continue
        normalized_tag = _normalize_tag(tag)
        weeks_played = int(history_counts.get(tag, 0))
        last_decks = int(last_week_decks.get(tag, 0))
        wtd_donations = None
        if normalized_tag in donations_wtd:
            wtd_donations = donations_wtd[normalized_tag].get("donations")
        donation_sum = 0
        donation_weeks = 0
        if DONATION_WEEKS_WINDOW > 0 and normalized_tag in donations_8w:
            donation_sum = int(donations_8w[normalized_tag].get("sum", 0))
            donation_weeks = int(donations_8w[normalized_tag].get("weeks_present", 0))
        if weeks_played <= NEW_MEMBER_WEEKS_PLAYED:
            if last_decks < REVIVED_DECKS_THRESHOLD:
                new_members.append(
                    {
                        "player_name": row.get("player_name") or "Unknown",
                        "player_tag": normalized_tag,
                        "decks_used": int(row.get("decks_used", 0)),
                        "fame": int(row.get("fame", 0)),
                        "weeks_played": weeks_played,
                        "donations_wtd": wtd_donations,
                        "donations_sum": donation_sum,
                        "donations_weeks": donation_weeks,
                    }
                )
            continue
        entry = {
            "player_tag": normalized_tag,
            "player_name": row.get("player_name") or "Unknown",
            "decks_used": int(row.get("decks_used", 0)),
            "fame": int(row.get("fame", 0)),
            "last_week_decks": last_decks,
            "donations_wtd": wtd_donations,
            "donations_sum": donation_sum,
            "donations_weeks": donation_weeks,
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
        if (
            DONATION_WEEKS_WINDOW > 0
            and DONATION_REVIVE_8W_THRESHOLD > 0
            and donation_sum >= DONATION_REVIVE_8W_THRESHOLD
        ):
            donation_revive = True
        if donation_revive:
            donation_warnings.append(entry)
        else:
            candidates.append(entry)

    shortlist = candidates[: max(KICK_SHORTLIST_LIMIT, 0)]
    if shortlist:
        lines.append("Kick candidates:")
        for index, row in enumerate(shortlist, 1):
            name = _format_name(row.get("player_name"))
            donation_suffix = _format_donation_suffix(
                row.get("player_tag"),
                donations_wtd,
                donations_8w,
                DONATION_WEEKS_WINDOW,
            )
            lines.append(
                f"{index}) {name} — 8w decks: {row.get('decks_used', 0)} | "
                f"8w fame: {row.get('fame', 0)} | last week: {row.get('last_week_decks', 0)}"
                f"{donation_suffix}"
            )
    else:
        lines.append("No clear kick candidates.")

    if warnings:
        lines.extend(
            [
                "",
                "Warnings: inactive overall, but revived last week — keep for now",
            ]
        )
        for index, row in enumerate(warnings, 1):
            name = _format_name(row.get("player_name"))
            donation_suffix = _format_donation_suffix(
                row.get("player_tag"),
                donations_wtd,
                donations_8w,
                DONATION_WEEKS_WINDOW,
            )
            lines.append(
                f"{index}) {name} — 8w decks: {row.get('decks_used', 0)} | "
                f"8w fame: {row.get('fame', 0)} | last week: {row.get('last_week_decks', 0)}"
                f"{donation_suffix}"
            )

    if donation_warnings:
        lines.extend(
            [
                "",
                "Warnings: inactive overall, but donating — consider keeping",
            ]
        )
        for index, row in enumerate(donation_warnings, 1):
            name = _format_name(row.get("player_name"))
            donation_suffix = _format_donation_suffix(
                row.get("player_tag"),
                donations_wtd,
                donations_8w,
                DONATION_WEEKS_WINDOW,
            )
            lines.append(
                f"{index}) {name} — 8w decks: {row.get('decks_used', 0)} | "
                f"8w fame: {row.get('fame', 0)} | last week: {row.get('last_week_decks', 0)}"
                f"{donation_suffix}"
            )

    if new_members:
        lines.extend(
            [
                "",
                "Attention: new members (under %s CW weeks in clan)"
                % NEW_MEMBER_WEEKS_PLAYED,
            ]
        )
        for index, row in enumerate(new_members, 1):
            name = _format_name(row.get("player_name"))
            donation_suffix = _format_donation_suffix(
                row.get("player_tag"),
                donations_wtd,
                donations_8w,
                DONATION_WEEKS_WINDOW,
            )
            lines.append(
                f"{index}) {name} — 8w decks: {row.get('decks_used', 0)} | "
                f"8w fame: {row.get('fame', 0)} | weeks played: {row.get('weeks_played', 0)}"
                f"{donation_suffix}"
            )

    return "\n".join(lines)


async def build_current_war_report(clan_tag: str) -> str:
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
    last_update = _format_timestamp(state.get("updated_at") if state else None)

    if period_type_upper == "WAR_DAY" and period_index is not None:
        day_display = f"{period_index + 1} / 4"
    elif period_type_upper == "COLOSSEUM":
        day_display = "1 / 1"
    elif period_type_upper == "TRAINING":
        day_display = "Training"
    else:
        day_display = "?"

    if period_type_upper == "WAR_DAY" and period_index is not None:
        remaining_display = f"{max(0, 4 - (period_index + 1))} war day(s) + Colosseum"
    elif period_type_upper == "COLOSSEUM":
        remaining_display = "0"
    elif period_type_upper == "TRAINING":
        remaining_display = "War starts soon"
    else:
        remaining_display = "?"

    phase_line = (
        f"🗓 Phase: {period_type_upper} (Day {day_display})"
        if period_type_upper
        else "🗓 Phase: Unknown"
    )
    colosseum_label = "COLOSSEUM" if is_colosseum else "RIVER RACE"

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
                ).limit(5)
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
                ).limit(30)
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

    tags = {
        _normalize_tag(row.get("player_tag"))
        for row in [*top_rows, *bottom_rows]
        if row.get("player_tag")
    }
    donations_wtd, donations_8w = await _collect_donation_maps(clan_tag, tags)

    filtered_bottom = [
        row
        for row in bottom_rows
        if _normalize_tag(row.get("player_tag")) not in PROTECTED_TAGS_NORMALIZED
    ][:5]

    lines = [
        "⚔️ Current War Snapshot (LIVE)",
        f"🏠 Clan: {clan_tag}",
        "⚠️ Data may change while the war is ongoing.",
        f"🕒 Last DB update: {last_update}",
        "",
        SEPARATOR_LINE,
        f"📅 Week: S{season_id} • W{section_index} • {colosseum_label}",
        phase_line,
        f"⏳ Remaining: {remaining_display}",
        "",
        "🧭 Week structure:",
        "• Training → War Days → Colosseum",
        "• Note: for simplicity we treat Colosseum as Week 4 of the cycle (but actual COLOSSEUM is detected by DB state).",
        "",
        SEPARATOR_LINE,
        "📊 Clan totals (this week so far)",
        f"🃏 Total decks used: {total_decks}",
        f"🏆 Total fame: {total_fame}",
        f"👥 Members counted: {member_count}",
        "",
        SEPARATOR_LINE,
        "🥇 Top 5 active (decks • fame)",
    ]
    if top_rows:
        for index, row in enumerate(top_rows, 1):
            name = row.get("player_name") or row.get("player_tag") or "Unknown"
            donation_suffix = _format_donation_suffix(
                row.get("player_tag"),
                donations_wtd,
                donations_8w,
                DONATION_WEEKS_WINDOW,
            )
            lines.append(
                f"{index}) {name} — {row.get('decks_used', 0)} • {row.get('fame', 0)}"
                f"{donation_suffix}"
            )
    else:
        lines.append("No data available.")

    lines.extend(
        [
            "",
            SEPARATOR_LINE,
            "🚫 Bottom 5 (risk) (decks • fame)",
        ]
    )
    if filtered_bottom:
        for index, row in enumerate(filtered_bottom, 1):
            name = row.get("player_name") or row.get("player_tag") or "Unknown"
            donation_suffix = _format_donation_suffix(
                row.get("player_tag"),
                donations_wtd,
                donations_8w,
                DONATION_WEEKS_WINDOW,
            )
            lines.append(
                f"{index}) {name} — {row.get('decks_used', 0)} • {row.get('fame', 0)}"
                f"{donation_suffix}"
            )
    else:
        lines.append("No data available.")

    return "\n".join(lines)


async def build_my_activity_report(
    player_tag: str, player_name: str, clan_tag: str
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
    colosseum_label = "COLOSSEUM" if is_colosseum else "RIVER RACE"

    member_tags = await get_current_member_tags(clan_tag)
    member_count = len(member_tags)

    current_decks = 0
    current_fame = 0
    rank_decks: str | int = "N/A"
    rank_fame: str | int = "N/A"

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
                rank_decks = "N/A"
                rank_fame = "N/A"

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

    decks_comp = _compare_to_avg(avg_user_decks, clan_avg_decks)
    fame_comp = _compare_to_avg(avg_user_fame, clan_avg_fame)

    donations_wtd_map = await get_current_wtd_donations(
        clan_tag, player_tags={_normalize_tag(player_tag)}
    )
    donations_8w_map = await get_donations_weekly_sums(
        clan_tag,
        player_tags={_normalize_tag(player_tag)},
        window_weeks=DONATION_WEEKS_WINDOW,
    )
    tag_key = _normalize_tag(player_tag)
    wtd_entry = donations_wtd_map.get(tag_key, {})
    wtd_donations = wtd_entry.get("donations")
    wtd_received = wtd_entry.get("donations_received")
    donation_sum = 0
    donation_weeks = 0
    if DONATION_WEEKS_WINDOW > 0 and tag_key in donations_8w_map:
        donation_sum = int(donations_8w_map[tag_key].get("sum", 0))
        donation_weeks = int(donations_8w_map[tag_key].get("weeks_present", 0))

    if current_decks == 0:
        status_label = "🔴 DANGER"
        reason_line = "Reason: 0 decks this week."
    elif avg_user_decks < clan_avg_decks and avg_user_fame < clan_avg_fame:
        status_label = "🟡 AT RISK"
        reason_line = "Reason: below clan average over last 8 weeks."
    else:
        status_label = "✅ SAFE"
        reason_line = "Reason: solid activity."

    avg_user_decks_str = _format_avg(avg_user_decks)
    avg_user_fame_str = _format_avg(avg_user_fame)
    clan_avg_decks_str = _format_avg(clan_avg_decks)
    clan_avg_fame_str = _format_avg(clan_avg_fame)
    wtd_donations_text = str(wtd_donations) if wtd_donations is not None else "n/a"
    wtd_received_text = str(wtd_received) if wtd_received is not None else "n/a"
    donation_lines = [
        "🤝 Donations",
        f"• this donation week (WTD): {wtd_donations_text} | received: {wtd_received_text}",
    ]
    if DONATION_WEEKS_WINDOW > 0:
        donation_lines.append(
            f"• last {DONATION_WEEKS_WINDOW} donation weeks: {donation_sum} ({donation_weeks}/{DONATION_WEEKS_WINDOW})"
        )
        if donation_weeks > 0:
            avg_donations = donation_sum / donation_weeks
            donation_lines.append(
                f"• average per active week: {avg_donations:.1f}"
            )

    lines = [
        "👤 My War Activity",
        f"🧾 Player: {player_name}",
        f"🏷 Tag: {player_tag}",
        f"🏠 Clan: {clan_tag}",
        "",
        SEPARATOR_LINE,
        f"📅 Current week: S{season_id} • W{section_index} • {colosseum_label}",
        f"🃏 Decks used: {current_decks} / 16",
        f"🏆 Fame: {current_fame}",
        "",
        f"📈 Rank (decks): {rank_decks} / {member_count}",
        f"📈 Rank (fame):  {rank_fame}  / {member_count}",
        "",
        SEPARATOR_LINE,
        "🗓 Last 8 weeks summary (current members view)",
        f"✅ Active weeks (>=8 decks): {active_weeks} / {weeks_available}",
        f"🟡 Low weeks (1–7 decks):    {low_weeks}",
        f"🔴 Zero weeks (0 decks):      {zero_weeks}",
        "",
        f"🃏 Avg decks / week: {avg_user_decks_str}",
        f"🏆 Avg fame / week:  {avg_user_fame_str}",
        "",
        SEPARATOR_LINE,
        "🏁 Compared to clan average (last 8 weeks)",
        f"🃏 You: {avg_user_decks_str} | Clan avg: {clan_avg_decks_str} → {decks_comp}",
        f"🏆 You: {avg_user_fame_str}  | Clan avg: {clan_avg_fame_str}  → {fame_comp}",
        "",
        SEPARATOR_LINE,
        *donation_lines,
        "",
        SEPARATOR_LINE,
        f"🚦 Status: {status_label}",
        reason_line,
    ]

    return "\n".join(lines)
