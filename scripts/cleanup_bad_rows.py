"""Cleanup bad River Race rows with season_id = 0 or bogus 128-0."""

import asyncio
import logging

from sqlalchemy import and_, delete, or_

from db import (
    PlayerParticipation,
    PlayerParticipationDaily,
    RiverRaceState,
    close_db,
    connect_db,
    get_session,
)

logger = logging.getLogger(__name__)


async def cleanup_bad_rows() -> None:
    await connect_db()
    async with get_session() as session:
        try:
            pp_result = await session.execute(
                delete(PlayerParticipation).where(
                    or_(
                        PlayerParticipation.season_id == 0,
                        and_(
                            PlayerParticipation.season_id == 128,
                            PlayerParticipation.section_index == 0,
                        ),
                    )
                )
            )
            rr_result = await session.execute(
                delete(RiverRaceState).where(
                    or_(
                        RiverRaceState.season_id == 0,
                        and_(
                            RiverRaceState.season_id == 128,
                            RiverRaceState.section_index == 0,
                        ),
                    )
                )
            )
            ppd_result = await session.execute(
                delete(PlayerParticipationDaily).where(
                    or_(
                        PlayerParticipationDaily.season_id == 0,
                        and_(
                            PlayerParticipationDaily.season_id == 128,
                            PlayerParticipationDaily.section_index == 0,
                        ),
                    )
                )
            )
            await session.commit()
        except Exception:
            await session.rollback()
            raise
    await close_db()

    logger.info(
        "Deleted rows (season_id=0 or season_id=128 section_index=0) - "
        "player_participation: %s, river_race_state: %s, player_participation_daily: %s",
        pp_result.rowcount,
        rr_result.rowcount,
        ppd_result.rowcount,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    asyncio.run(cleanup_bad_rows())


if __name__ == "__main__":
    main()
