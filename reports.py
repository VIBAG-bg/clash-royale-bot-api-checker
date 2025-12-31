"""Report builders for weekly and rolling war summaries."""

from html import escape
from typing import Iterable

from db import get_rolling_leaderboard, get_week_leaderboard


def _format_entries(entries: Iterable[dict[str, object]]) -> list[str]:
    lines: list[str] = []
    for index, row in enumerate(entries, 1):
        player_name = row.get("player_name") or "Unknown"
        name = escape(str(player_name))
        decks_used = int(row.get("decks_used", 0))
        fame = int(row.get("fame", 0))
        lines.append(f"{index}) {name} - decks: {decks_used}, fame: {fame}")
    if not lines:
        lines.append("No data available.")
    return lines


async def build_weekly_report(
    season_id: int, section_index: int, clan_tag: str
) -> str:
    inactive, active = await get_week_leaderboard(
        season_id=season_id,
        section_index=section_index,
        clan_tag=clan_tag,
    )
    lines = [
        f"War Report - Season {season_id} Week {section_index + 1}",
        "(weekly totals; current members only)",
        "",
        "Top 10 Inactive (lowest decks, then fame)",
        *_format_entries(inactive),
        "",
        "Top 10 Active (highest decks, then fame)",
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
    lines = [
        f"Rolling Report - Last {len(weeks)} completed weeks",
        "(aggregated totals; current members only)",
        "",
        "Top 10 Inactive (lowest decks, then fame)",
        *_format_entries(inactive),
        "",
        "Top 10 Active (highest decks, then fame)",
        *_format_entries(active),
    ]
    return "\n".join(lines)
