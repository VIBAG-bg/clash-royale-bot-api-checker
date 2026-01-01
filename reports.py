"""Report builders for weekly and rolling war summaries."""

from html import escape
from typing import Iterable

from config import (
    KICK_SHORTLIST_LIMIT,
    NEW_MEMBER_WEEKS_PLAYED,
    PROTECTED_PLAYER_TAGS,
    REVIVED_DECKS_THRESHOLD,
)
from db import (
    get_current_member_tags,
    get_participation_week_counts,
    get_rolling_leaderboard,
    get_session,
    get_week_decks_map,
    get_week_leaderboard,
)

NAME_WIDTH = 20
HEADER_LINE = "══════════════════════════════"
DIVIDER_LINE = "──────────────────────────────"


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
    name = name.ljust(NAME_WIDTH)
    return escape(name)


def _format_entries(entries: Iterable[dict[str, object]]) -> list[str]:
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
        lines.append(
            f"{index:>2}) {name} — decks: {decks_used:>{decks_width}} | fame: {fame:>{fame_width}}"
        )
    return lines


async def build_weekly_report(
    season_id: int, section_index: int, clan_tag: str
) -> str:
    inactive, active = await get_week_leaderboard(
        season_id=season_id,
        section_index=section_index,
        clan_tag=clan_tag,
    )
    inactive = _filter_protected(inactive)
    member_count = len(await get_current_member_tags(clan_tag))
    lines = [
        HEADER_LINE,
        f"🏁 WAR REPORT — Season {season_id} / Week {section_index + 1}",
        HEADER_LINE,
        f"Members considered: {member_count} (current clan members)",
        "",
        "🧊 TOP 10 INACTIVE (lowest decks, then fame)",
        *_format_entries(inactive),
        "",
        DIVIDER_LINE,
        "",
        "🔥 TOP 10 ACTIVE (highest decks, then fame)",
        *_format_entries(active),
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
        *_format_entries(inactive),
        "",
        DIVIDER_LINE,
        "",
        "🔥 TOP 10 ACTIVE (sum of decks, then fame)",
        *_format_entries(active),
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
        inactive_tags = {row.get("player_tag") for row in inactive if row.get("player_tag")}
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

    candidates: list[dict[str, object]] = []
    warnings: list[dict[str, object]] = []
    new_members: list[dict[str, object]] = []
    for row in inactive:
        tag = row.get("player_tag")
        if not tag:
            continue
        weeks_played = int(history_counts.get(tag, 0))
        last_decks = int(last_week_decks.get(tag, 0))
        if weeks_played <= NEW_MEMBER_WEEKS_PLAYED:
            if last_decks < REVIVED_DECKS_THRESHOLD:
                new_members.append(
                    {
                        "player_name": row.get("player_name") or "Unknown",
                        "decks_used": int(row.get("decks_used", 0)),
                        "fame": int(row.get("fame", 0)),
                        "weeks_played": weeks_played,
                    }
                )
            continue
        entry = {
            "player_tag": tag,
            "player_name": row.get("player_name") or "Unknown",
            "decks_used": int(row.get("decks_used", 0)),
            "fame": int(row.get("fame", 0)),
            "last_week_decks": last_decks,
        }
        if last_decks >= REVIVED_DECKS_THRESHOLD:
            warnings.append(entry)
        else:
            candidates.append(entry)

    shortlist = candidates[: max(KICK_SHORTLIST_LIMIT, 0)]
    if shortlist:
        lines.append("Kick candidates:")
        for index, row in enumerate(shortlist, 1):
            name = _format_name(row.get("player_name"))
            lines.append(
                f"{index}) {name} — 8w decks: {row.get('decks_used', 0)} | "
                f"8w fame: {row.get('fame', 0)} | last week: {row.get('last_week_decks', 0)}"
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
            lines.append(
                f"{index}) {name} — 8w decks: {row.get('decks_used', 0)} | "
                f"8w fame: {row.get('fame', 0)} | last week: {row.get('last_week_decks', 0)}"
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
            lines.append(
                f"{index}) {name} — 8w decks: {row.get('decks_used', 0)} | "
                f"8w fame: {row.get('fame', 0)} | weeks played: {row.get('weeks_played', 0)}"
            )

    return "\n".join(lines)
