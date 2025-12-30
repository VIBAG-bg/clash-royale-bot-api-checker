"""Database module for PostgreSQL operations using SQLAlchemy async."""

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator

from sqlalchemy import Boolean, DateTime, Index, Integer, String, UniqueConstraint, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from config import DATABASE_URL


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class RiverRaceState(Base):
    __tablename__ = "river_race_state"
    __table_args__ = (
        UniqueConstraint(
            "clan_tag",
            "season_id",
            "section_index",
            name="uq_river_race_state_clan_season_section",
        ),
        Index(
            "ix_river_race_state_clan_season_section",
            "clan_tag",
            "season_id",
            "section_index",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    clan_tag: Mapped[str] = mapped_column(String(32), nullable=False)
    season_id: Mapped[int] = mapped_column(Integer, nullable=False)
    section_index: Mapped[int] = mapped_column(Integer, nullable=False)
    is_colosseum: Mapped[bool] = mapped_column(Boolean, nullable=False)
    period_type: Mapped[str] = mapped_column(String(32), nullable=False)
    clan_score: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, onupdate=_utc_now, nullable=False
    )


class PlayerParticipation(Base):
    __tablename__ = "player_participation"
    __table_args__ = (
        UniqueConstraint(
            "player_tag",
            "season_id",
            "section_index",
            name="uq_player_participation_player_season_section",
        ),
        Index(
            "ix_player_participation_season_section_decks",
            "season_id",
            "section_index",
            "decks_used",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    player_tag: Mapped[str] = mapped_column(String(32), nullable=False)
    player_name: Mapped[str] = mapped_column(String(128), nullable=False)
    season_id: Mapped[int] = mapped_column(Integer, nullable=False)
    section_index: Mapped[int] = mapped_column(Integer, nullable=False)
    is_colosseum: Mapped[bool] = mapped_column(Boolean, nullable=False)
    fame: Mapped[int] = mapped_column(Integer, nullable=False)
    repair_points: Mapped[int] = mapped_column(Integer, nullable=False)
    boat_attacks: Mapped[int] = mapped_column(Integer, nullable=False)
    decks_used: Mapped[int] = mapped_column(Integer, nullable=False)
    decks_used_today: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, onupdate=_utc_now, nullable=False
    )


_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def _require_database_url() -> str:
    if not DATABASE_URL:
        raise ValueError(
            "DATABASE_URL is not set. Configure it in the environment before starting the bot."
        )
    return DATABASE_URL


def _build_async_database_url(raw_url: str) -> str:
    if raw_url.startswith("postgres://"):
        return raw_url.replace("postgres://", "postgresql+asyncpg://", 1)
    if raw_url.startswith("postgresql+psycopg://"):
        return raw_url.replace("postgresql+psycopg://", "postgresql+asyncpg://", 1)
    if raw_url.startswith("postgresql://"):
        return raw_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return raw_url


async def connect_db() -> AsyncEngine:
    """Create the async engine and session factory."""
    global _engine, _session_factory
    if _engine is None:
        raw_url = _require_database_url()
        async_url = _build_async_database_url(raw_url)
        _engine = create_async_engine(async_url, pool_pre_ping=True)
        _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    return _engine


async def close_db() -> None:
    """Dispose the async engine."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None


@asynccontextmanager
async def _get_session() -> AsyncIterator[AsyncSession]:
    if _session_factory is None:
        await connect_db()
    assert _session_factory is not None
    async with _session_factory() as session:
        yield session


def _player_participation_to_dict(player: PlayerParticipation) -> dict[str, Any]:
    return {
        "id": player.id,
        "player_tag": player.player_tag,
        "player_name": player.player_name,
        "season_id": player.season_id,
        "section_index": player.section_index,
        "is_colosseum": player.is_colosseum,
        "fame": player.fame,
        "repair_points": player.repair_points,
        "boat_attacks": player.boat_attacks,
        "decks_used": player.decks_used,
        "decks_used_today": player.decks_used_today,
        "created_at": player.created_at,
        "updated_at": player.updated_at,
    }


def _river_race_state_to_dict(state: RiverRaceState) -> dict[str, Any]:
    return {
        "id": state.id,
        "clan_tag": state.clan_tag,
        "season_id": state.season_id,
        "section_index": state.section_index,
        "is_colosseum": state.is_colosseum,
        "period_type": state.period_type,
        "clan_score": state.clan_score,
        "created_at": state.created_at,
        "updated_at": state.updated_at,
    }


async def save_player_participation(
    player_tag: str,
    player_name: str,
    season_id: int,
    section_index: int,
    is_colosseum: bool,
    fame: int,
    repair_points: int,
    boat_attacks: int,
    decks_used: int,
    decks_used_today: int,
) -> None:
    """Save or update player participation data for a River Race week."""
    now = _utc_now()
    async with _get_session() as session:
        result = await session.execute(
            select(PlayerParticipation).where(
                PlayerParticipation.player_tag == player_tag,
                PlayerParticipation.season_id == season_id,
                PlayerParticipation.section_index == section_index,
            )
        )
        existing = result.scalar_one_or_none()
        if existing:
            existing.player_name = player_name
            existing.is_colosseum = is_colosseum
            existing.fame = fame
            existing.repair_points = repair_points
            existing.boat_attacks = boat_attacks
            existing.decks_used = decks_used
            existing.decks_used_today = decks_used_today
            existing.updated_at = now
        else:
            session.add(
                PlayerParticipation(
                    player_tag=player_tag,
                    player_name=player_name,
                    season_id=season_id,
                    section_index=section_index,
                    is_colosseum=is_colosseum,
                    fame=fame,
                    repair_points=repair_points,
                    boat_attacks=boat_attacks,
                    decks_used=decks_used,
                    decks_used_today=decks_used_today,
                    created_at=now,
                    updated_at=now,
                )
            )
        await session.commit()


async def get_inactive_players(
    season_id: int, section_index: int, min_decks: int = 4
) -> list[dict[str, Any]]:
    """Get players who haven't used enough decks in the current River Race week."""
    async with _get_session() as session:
        result = await session.execute(
            select(PlayerParticipation)
            .where(
                PlayerParticipation.season_id == season_id,
                PlayerParticipation.section_index == section_index,
                PlayerParticipation.decks_used < min_decks,
            )
            .order_by(PlayerParticipation.decks_used.asc())
        )
        players = result.scalars().all()
        return [_player_participation_to_dict(player) for player in players]


async def get_all_participation_for_week(
    season_id: int, section_index: int
) -> list[dict[str, Any]]:
    """Get all player participation records for a specific week."""
    async with _get_session() as session:
        result = await session.execute(
            select(PlayerParticipation)
            .where(
                PlayerParticipation.season_id == season_id,
                PlayerParticipation.section_index == section_index,
            )
            .order_by(PlayerParticipation.fame.desc())
        )
        players = result.scalars().all()
        return [_player_participation_to_dict(player) for player in players]


async def get_player_history(player_tag: str, limit: int = 10) -> list[dict[str, Any]]:
    """Get participation history for a specific player."""
    async with _get_session() as session:
        result = await session.execute(
            select(PlayerParticipation)
            .where(PlayerParticipation.player_tag == player_tag)
            .order_by(
                PlayerParticipation.season_id.desc(),
                PlayerParticipation.section_index.desc(),
            )
            .limit(limit)
        )
        players = result.scalars().all()
        return [_player_participation_to_dict(player) for player in players]


async def save_river_race_state(
    clan_tag: str,
    season_id: int,
    section_index: int,
    is_colosseum: bool,
    period_type: str,
    clan_score: int,
) -> None:
    """Save the current River Race state for tracking."""
    now = _utc_now()
    async with _get_session() as session:
        result = await session.execute(
            select(RiverRaceState).where(
                RiverRaceState.clan_tag == clan_tag,
                RiverRaceState.season_id == season_id,
                RiverRaceState.section_index == section_index,
            )
        )
        existing = result.scalar_one_or_none()
        if existing:
            existing.is_colosseum = is_colosseum
            existing.period_type = period_type
            existing.clan_score = clan_score
            existing.updated_at = now
        else:
            session.add(
                RiverRaceState(
                    clan_tag=clan_tag,
                    season_id=season_id,
                    section_index=section_index,
                    is_colosseum=is_colosseum,
                    period_type=period_type,
                    clan_score=clan_score,
                    created_at=now,
                    updated_at=now,
                )
            )
        await session.commit()


async def get_latest_river_race_state(clan_tag: str) -> dict[str, Any] | None:
    """Get the latest River Race state for a clan."""
    async with _get_session() as session:
        result = await session.execute(
            select(RiverRaceState)
            .where(RiverRaceState.clan_tag == clan_tag)
            .order_by(
                RiverRaceState.season_id.desc(),
                RiverRaceState.section_index.desc(),
            )
            .limit(1)
        )
        state = result.scalar_one_or_none()
        return _river_race_state_to_dict(state) if state else None
