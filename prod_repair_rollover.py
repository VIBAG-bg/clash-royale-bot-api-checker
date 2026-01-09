"""One-shot production repair for season rollover/active_week corruption.

Safe to run multiple times; logs each step and uses idempotent upserts.
"""

import argparse
import asyncio
import logging
from datetime import datetime, timezone

from config import CLAN_TAG, require_env_value
from cr_api import close_api_client, get_api_client
from db import (
    close_db,
    connect_db,
    get_app_state,
    get_latest_river_race_state,
    get_latest_war_race_state,
    set_app_state,
)
from main import ACTIVE_WEEK_KEY, _parse_active_week_state, fetch_river_race_stats
from riverrace_import import import_riverrace_log

logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="One-shot production repair for season rollover state."
    )
    parser.add_argument("--restore-season", type=int, default=None)
    parser.add_argument("--restore-section", type=int, default=None)
    parser.add_argument(
        "--no-auto-restore", action="store_true", default=False
    )
    return parser.parse_args()


def _format_week(state: dict[str, object] | None) -> str:
    if not state:
        return "none"
    season_id = state.get("season_id")
    section_index = state.get("section_index")
    period_type = state.get("period_type")
    return f"season={season_id} section={section_index} period={period_type}"


async def _restore_week(
    clan_tag: str,
    season_id: int,
    section_index: int,
    *,
    reason: str,
) -> None:
    logger.info(
        "Restoring River Race log week: season=%s section=%s reason=%s",
        season_id,
        section_index,
        reason,
    )
    try:
        weeks_imported, players_imported = await import_riverrace_log(
            weeks=1,
            clan_tag=clan_tag,
            season_id=season_id,
            section_index=section_index,
        )
        logger.info(
            "Restore result: weeks_imported=%s players_imported=%s",
            weeks_imported,
            players_imported,
        )
    except Exception as e:
        logger.warning(
            "Restore failed (season=%s section=%s): %s",
            season_id,
            section_index,
            e,
        )


async def _run(args: argparse.Namespace) -> None:
    clan_tag = require_env_value("CLAN_TAG", CLAN_TAG)
    await connect_db()
    try:
        active_state = await get_app_state(ACTIVE_WEEK_KEY)
        active_season_id, active_section_index = _parse_active_week_state(
            active_state
        )
        logger.info(
            "App state active_week: raw=%r parsed=(%s,%s)",
            active_state,
            active_season_id,
            active_section_index,
        )

        latest_war_state = await get_latest_war_race_state(clan_tag)
        logger.info(
            "Latest war state (non-training): %s",
            _format_week(latest_war_state),
        )
        latest_river_state = await get_latest_river_race_state(clan_tag)
        logger.info(
            "Latest river race state (any): %s",
            _format_week(latest_river_state),
        )

        if latest_war_state:
            season_id = int(latest_war_state.get("season_id") or 0)
            section_index = int(latest_war_state.get("section_index") or 0)
            payload = {
                "season_id": season_id,
                "section_index": section_index,
                "set_at": datetime.now(timezone.utc).isoformat(),
                "source": "prod_repair_bootstrap",
            }
            await set_app_state(ACTIVE_WEEK_KEY, payload)
            logger.info(
                "Bootstrapped active_week to season=%s section=%s source=prod_repair_bootstrap",
                season_id,
                section_index,
            )
            bootstrap_state = await get_app_state(ACTIVE_WEEK_KEY)
            logger.info(
                "Active_week after bootstrap: %r",
                bootstrap_state,
            )
        else:
            logger.info(
                "Bootstrap skipped: no latest non-training week in DB"
            )

        if (args.restore_season is None) ^ (args.restore_section is None):
            logger.error(
                "Both --restore-season and --restore-section are required together."
            )
            return

        if args.restore_season is not None and args.restore_section is not None:
            await _restore_week(
                clan_tag,
                int(args.restore_season),
                int(args.restore_section),
                reason="manual",
            )
        elif args.no_auto_restore:
            logger.info("Auto-restore skipped (--no-auto-restore)")
        else:
            if (
                latest_war_state
                and active_season_id is not None
                and active_section_index is not None
            ):
                latest_war_season = int(latest_war_state.get("season_id") or 0)
                latest_war_section = int(
                    latest_war_state.get("section_index") or 0
                )
                if (
                    active_season_id == latest_war_season
                    and active_section_index == 0
                    and latest_war_section >= 3
                ):
                    await _restore_week(
                        clan_tag,
                        active_season_id,
                        0,
                        reason="auto_restore",
                    )
                else:
                    logger.info(
                        "Auto-restore not triggered: active=(%s,%s) latest_war=(%s,%s)",
                        active_season_id,
                        active_section_index,
                        latest_war_season,
                        latest_war_section,
                    )
            else:
                logger.info(
                    "Auto-restore not attempted: missing active_week or latest_war"
                )

        try:
            api_client = await get_api_client()
            current = await api_client.get_current_river_race(clan_tag)
            season_raw = current.get("seasonId") if isinstance(current, dict) else None
            section_raw = (
                current.get("sectionIndex") if isinstance(current, dict) else None
            )
            period_raw = (
                current.get("periodType") if isinstance(current, dict) else None
            )
            logger.info(
                "CR API current river race: seasonId=%r sectionIndex=%r periodType=%r",
                season_raw,
                section_raw,
                period_raw,
            )
        except Exception as e:
            logger.warning("Failed to fetch current river race: %s", e)

        logger.info("Running fetch_river_race_stats() once...")
        await fetch_river_race_stats()
        logger.info("fetch_river_race_stats() finished.")

        final_active_state = await get_app_state(ACTIVE_WEEK_KEY)
        final_season_id, final_section_index = _parse_active_week_state(
            final_active_state
        )
        logger.info(
            "Final active_week: season=%s section=%s",
            final_season_id,
            final_section_index,
        )
        final_war_state = await get_latest_war_race_state(clan_tag)
        logger.info(
            "Final latest war state (non-training): %s",
            _format_week(final_war_state),
        )
        final_river_state = await get_latest_river_race_state(clan_tag)
        logger.info(
            "Final latest river race state (any): %s",
            _format_week(final_river_state),
        )
    finally:
        try:
            await close_api_client()
        finally:
            await close_db()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = _parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
