"""Backfill River Race log entries into weekly totals."""

import argparse
import asyncio
import logging

from config import BACKFILL_WEEKS
from cr_api import close_api_client
from db import close_db, connect_db
from riverrace_import import import_riverrace_log

logger = logging.getLogger(__name__)


async def run_backfill(weeks: int) -> None:
    await connect_db()
    try:
        weeks_imported, players_imported = await import_riverrace_log(weeks)
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
    args = parser.parse_args()
    asyncio.run(run_backfill(args.weeks))


if __name__ == "__main__":
    main()
