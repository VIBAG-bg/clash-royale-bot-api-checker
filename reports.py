"""Report builders for weekly and rolling war summaries."""

from html import escape
from typing import Iterable

from config import PROTECTED_PLAYER_TAGS
from db import get_current_member_tags, get_rolling_leaderboard, get_week_leaderboard

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
