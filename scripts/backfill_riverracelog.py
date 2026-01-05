"""Backfill River Race log entries into weekly totals."""

import argparse
import asyncio
import logging

from config import BACKFILL_WEEKS
from cr_api import close_api_client
from db import close_db, connect_db
from riverrace_import import import_riverrace_log

logger = logging.getLogger(__name__)


async def run_backfill(
    weeks: int, *, season_id: int | None = None, section_index: int | None = None
) -> None:
    await connect_db()
    try:
        weeks_imported, players_imported = await import_riverrace_log(
            weeks, season_id=season_id, section_index=section_index
        )
        logger.info(
            "Backfill complete: %s week(s), %s player record(s)",
            weeks_imported,
            players_imported,
        )
    finally:
        await close_api_client()
        await close_db()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(description="Backfill River Race log data.")
    parser.add_argument(
        "--weeks",
        type=int,
        default=BACKFILL_WEEKS,
        help="Number of weeks to import (default: BACKFILL_WEEKS).",
    )
    parser.add_argument(
        "--season",
        type=int,
        help="Season ID to import (requires --week or --section).",
    )
    parser.add_argument(
        "--week",
        type=int,
        help="1-based week number to import (requires --season).",
    )
    parser.add_argument(
        "--section",
        type=int,
        help="0-based sectionIndex to import (requires --season).",
    )
    args = parser.parse_args()
    season_id = args.season
    section_index = None
    if args.week is not None:
        if args.week < 1:
            logger.error("--week must be >= 1")
            return
        section_index = args.week - 1
    elif args.section is not None:
        section_index = args.section

    if season_id is not None and section_index is None:
        logger.error("--season requires --week or --section")
        return
    if season_id is None and section_index is not None:
        logger.error("--week/--section requires --season")
        return

    asyncio.run(
        run_backfill(
            args.weeks, season_id=season_id, section_index=section_index
        )
    )


if __name__ == "__main__":
    main()
