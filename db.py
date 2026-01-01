"""Database module for PostgreSQL operations using SQLAlchemy async."""

from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
import os
from typing import Any, AsyncIterator

from sqlalchemy import (
    Boolean,
    BigInteger,
    Date,
    DateTime,
    Index,
    Integer,
    String,
    UniqueConstraint,
    delete,
    func,
    select,
    tuple_,
)
from sqlalchemy.dialects.postgresql import JSONB, insert as pg_insert
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


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


class PlayerParticipationDaily(Base):
    __tablename__ = "player_participation_daily"
    __table_args__ = (
        UniqueConstraint(
            "player_tag",
            "season_id",
            "section_index",
            "is_colosseum",
            "snapshot_date",
            name="uq_player_participation_daily_player_season_section_date",
        ),
        Index(
            "ix_player_participation_daily_season_section_date",
            "season_id",
            "section_index",
            "snapshot_date",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    player_tag: Mapped[str] = mapped_column(String(32), nullable=False)
    player_name: Mapped[str] = mapped_column(String(128), nullable=False)
    season_id: Mapped[int] = mapped_column(Integer, nullable=False)
    section_index: Mapped[int] = mapped_column(Integer, nullable=False)
    is_colosseum: Mapped[bool] = mapped_column(Boolean, nullable=False)
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False)
    fame: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    repair_points: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    boat_attacks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    decks_used: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    decks_used_today: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, onupdate=_utc_now, nullable=False
    )


class ClanMemberDaily(Base):
    __tablename__ = "clan_member_daily"
    __table_args__ = (
        UniqueConstraint(
            "snapshot_date",
            "clan_tag",
            "player_tag",
            name="uq_clan_member_daily_date_clan_player",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False)
    clan_tag: Mapped[str] = mapped_column(String(32), nullable=False)
    player_tag: Mapped[str] = mapped_column(String(32), nullable=False)
    player_name: Mapped[str] = mapped_column(String(128), nullable=False)
    role: Mapped[str | None] = mapped_column(String(32))
    trophies: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, onupdate=_utc_now, nullable=False
    )


class ClanChat(Base):
    __tablename__ = "clan_chats"
    __table_args__ = (
        UniqueConstraint(
            "clan_tag",
            "chat_id",
            name="uq_clan_chats_clan_chat",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    clan_tag: Mapped[str] = mapped_column(String(32), nullable=False)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )


class AppState(Base):
    __tablename__ = "app_state"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, onupdate=_utc_now, nullable=False
    )


_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def _require_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError(
            "DATABASE_URL is not set. Configure it in the environment before starting the bot."
        )
    return database_url


def _build_async_database_url(raw_url: str) -> str:
    if raw_url.startswith("postgres://"):
        return raw_url.replace("postgres://", "postgresql+asyncpg://", 1)
    if raw_url.startswith("postgresql+psycopg://"):
        return raw_url.replace("postgresql+psycopg://", "postgresql+asyncpg://", 1)
    if raw_url.startswith("postgresql://"):
        return raw_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return raw_url


async def connect_db() -> None:
    """Create the async engine and session factory."""
    global _engine, _session_factory
    if _engine is None:
        raw_url = _require_database_url()
        async_url = _build_async_database_url(raw_url)
        _engine = create_async_engine(async_url, pool_pre_ping=True)
        _session_factory = async_sessionmaker(_engine, expire_on_commit=False)


async def close_db() -> None:
    """Dispose the async engine."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None


@asynccontextmanager
async def get_session() -> AsyncIterator[AsyncSession]:
    async with _get_session() as session:
        yield session


@asynccontextmanager
async def _get_session() -> AsyncIterator[AsyncSession]:
    if _session_factory is None:
        await connect_db()
    assert _session_factory is not None
    async with _session_factory() as session:
        yield session


async def _upsert_player_participation(
    session: AsyncSession,
    now: datetime,
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
    stmt = pg_insert(PlayerParticipation.__table__).values(
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
    stmt = stmt.on_conflict_do_update(
        index_elements=["player_tag", "season_id", "section_index"],
        set_={
            "player_name": stmt.excluded.player_name,
            "is_colosseum": stmt.excluded.is_colosseum,
            "fame": stmt.excluded.fame,
            "repair_points": stmt.excluded.repair_points,
            "boat_attacks": stmt.excluded.boat_attacks,
            "decks_used": stmt.excluded.decks_used,
            "decks_used_today": stmt.excluded.decks_used_today,
            "updated_at": now,
        },
    )
    await session.execute(stmt)


async def _upsert_river_race_state(
    session: AsyncSession,
    now: datetime,
    clan_tag: str,
    season_id: int,
    section_index: int,
    is_colosseum: bool,
    period_type: str,
    clan_score: int,
) -> None:
    stmt = pg_insert(RiverRaceState.__table__).values(
        clan_tag=clan_tag,
        season_id=season_id,
        section_index=section_index,
        is_colosseum=is_colosseum,
        period_type=period_type,
        clan_score=clan_score,
        created_at=now,
        updated_at=now,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["clan_tag", "season_id", "section_index"],
        set_={
            "is_colosseum": stmt.excluded.is_colosseum,
            "period_type": stmt.excluded.period_type,
            "clan_score": stmt.excluded.clan_score,
            "updated_at": now,
        },
    )
    await session.execute(stmt)


async def _upsert_player_participation_daily(
    session: AsyncSession,
    now: datetime,
    snapshot_date: date,
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
    stmt = pg_insert(PlayerParticipationDaily.__table__).values(
        player_tag=player_tag,
        player_name=player_name,
        season_id=season_id,
        section_index=section_index,
        is_colosseum=is_colosseum,
        snapshot_date=snapshot_date,
        fame=fame,
        repair_points=repair_points,
        boat_attacks=boat_attacks,
        decks_used=decks_used,
        decks_used_today=decks_used_today,
        created_at=now,
        updated_at=now,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[
            "player_tag",
            "season_id",
            "section_index",
            "is_colosseum",
            "snapshot_date",
        ],
        set_={
            "player_name": stmt.excluded.player_name,
            "fame": stmt.excluded.fame,
            "repair_points": stmt.excluded.repair_points,
            "boat_attacks": stmt.excluded.boat_attacks,
            "decks_used": stmt.excluded.decks_used,
            "decks_used_today": stmt.excluded.decks_used_today,
            "updated_at": now,
        },
    )
    await session.execute(stmt)


async def _upsert_clan_member_daily(
    session: AsyncSession,
    now: datetime,
    snapshot_date: date,
    clan_tag: str,
    player_tag: str,
    player_name: str,
    role: str | None,
    trophies: int | None,
) -> None:
    stmt = pg_insert(ClanMemberDaily.__table__).values(
        snapshot_date=snapshot_date,
        clan_tag=clan_tag,
        player_tag=player_tag,
        player_name=player_name,
        role=role,
        trophies=trophies,
        created_at=now,
        updated_at=now,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["snapshot_date", "clan_tag", "player_tag"],
        set_={
            "player_name": stmt.excluded.player_name,
            "role": stmt.excluded.role,
            "trophies": stmt.excluded.trophies,
            "updated_at": now,
        },
    )
    await session.execute(stmt)


async def _upsert_clan_chat(
    session: AsyncSession, now: datetime, clan_tag: str, chat_id: int, enabled: bool
) -> None:
    stmt = pg_insert(ClanChat.__table__).values(
        clan_tag=clan_tag,
        chat_id=chat_id,
        enabled=enabled,
        created_at=now,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["clan_tag", "chat_id"],
        set_={"enabled": stmt.excluded.enabled},
    )
    await session.execute(stmt)


async def _upsert_app_state(
    session: AsyncSession, now: datetime, key: str, value: dict[str, Any]
) -> None:
    stmt = pg_insert(AppState.__table__).values(
        key=key,
        value=value,
        updated_at=now,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["key"],
        set_={
            "value": stmt.excluded.value,
            "updated_at": now,
        },
    )
    await session.execute(stmt)


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
    session: AsyncSession | None = None,
) -> None:
    """Save or update player participation data for a River Race week."""
    now = _utc_now()
    if session is None:
        async with _get_session() as session:
            try:
                await _upsert_player_participation(
                    session,
                    now,
                    player_tag,
                    player_name,
                    season_id,
                    section_index,
                    is_colosseum,
                    fame,
                    repair_points,
                    boat_attacks,
                    decks_used,
                    decks_used_today,
                )
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await _upsert_player_participation(
            session,
            now,
            player_tag,
            player_name,
            season_id,
            section_index,
            is_colosseum,
            fame,
            repair_points,
            boat_attacks,
            decks_used,
            decks_used_today,
        )


async def get_inactive_players(
    season_id: int,
    section_index: int,
    min_decks: int = 4,
    player_tags: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Get players who haven't used enough decks in the current River Race week."""
    async with _get_session() as session:
        if player_tags is not None and not player_tags:
            return []
        query = select(PlayerParticipation).where(
            PlayerParticipation.season_id == season_id,
            PlayerParticipation.section_index == section_index,
            PlayerParticipation.decks_used < min_decks,
        )
        if player_tags:
            query = query.where(PlayerParticipation.player_tag.in_(player_tags))
        result = await session.execute(
            query.order_by(PlayerParticipation.decks_used.asc())
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
    session: AsyncSession | None = None,
) -> None:
    """Save the current River Race state for tracking."""
    now = _utc_now()
    if session is None:
        async with _get_session() as session:
            try:
                await _upsert_river_race_state(
                    session,
                    now,
                    clan_tag,
                    season_id,
                    section_index,
                    is_colosseum,
                    period_type,
                    clan_score,
                )
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await _upsert_river_race_state(
            session,
            now,
            clan_tag,
            season_id,
            section_index,
            is_colosseum,
            period_type,
            clan_score,
        )


async def save_player_participation_daily(
    player_tag: str,
    player_name: str,
    season_id: int,
    section_index: int,
    is_colosseum: bool,
    snapshot_date: date,
    fame: int,
    repair_points: int,
    boat_attacks: int,
    decks_used: int,
    decks_used_today: int,
    session: AsyncSession | None = None,
) -> None:
    """Save or update daily participation snapshot for a player."""
    now = _utc_now()
    if session is None:
        async with _get_session() as session:
            try:
                await _upsert_player_participation_daily(
                    session,
                    now,
                    snapshot_date,
                    player_tag,
                    player_name,
                    season_id,
                    section_index,
                    is_colosseum,
                    fame,
                    repair_points,
                    boat_attacks,
                    decks_used,
                    decks_used_today,
                )
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await _upsert_player_participation_daily(
            session,
            now,
            snapshot_date,
            player_tag,
            player_name,
            season_id,
            section_index,
            is_colosseum,
            fame,
            repair_points,
            boat_attacks,
            decks_used,
            decks_used_today,
        )


async def save_clan_member_daily(
    snapshot_date: date,
    clan_tag: str,
    player_tag: str,
    player_name: str,
    role: str | None,
    trophies: int | None,
    session: AsyncSession | None = None,
) -> None:
    """Save or update daily snapshot of a clan member."""
    now = _utc_now()
    if session is None:
        async with _get_session() as session:
            try:
                await _upsert_clan_member_daily(
                    session,
                    now,
                    snapshot_date,
                    clan_tag,
                    player_tag,
                    player_name,
                    role,
                    trophies,
                )
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await _upsert_clan_member_daily(
            session,
            now,
            snapshot_date,
            clan_tag,
            player_tag,
            player_name,
            role,
            trophies,
        )


async def upsert_clan_chat(
    clan_tag: str, chat_id: int, enabled: bool = True, session: AsyncSession | None = None
) -> None:
    now = _utc_now()
    if session is None:
        async with _get_session() as session:
            try:
                await _upsert_clan_chat(session, now, clan_tag, chat_id, enabled)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await _upsert_clan_chat(session, now, clan_tag, chat_id, enabled)


async def get_enabled_clan_chats(
    clan_tag: str, session: AsyncSession | None = None
) -> list[int]:
    if session is None:
        async with _get_session() as session:
            result = await session.execute(
                select(ClanChat.chat_id).where(
                    ClanChat.clan_tag == clan_tag, ClanChat.enabled.is_(True)
                )
            )
            return [row[0] for row in result.all()]
    result = await session.execute(
        select(ClanChat.chat_id).where(
            ClanChat.clan_tag == clan_tag, ClanChat.enabled.is_(True)
        )
    )
    return [row[0] for row in result.all()]


async def get_latest_membership_date(
    clan_tag: str, session: AsyncSession | None = None
) -> date | None:
    if session is None:
        async with _get_session() as session:
            result = await session.execute(
                select(func.max(ClanMemberDaily.snapshot_date)).where(
                    ClanMemberDaily.clan_tag == clan_tag
                )
            )
            return result.scalar_one()
    result = await session.execute(
        select(func.max(ClanMemberDaily.snapshot_date)).where(
            ClanMemberDaily.clan_tag == clan_tag
        )
    )
    return result.scalar_one()


async def get_current_member_tags(
    clan_tag: str, session: AsyncSession | None = None
) -> set[str]:
    if session is None:
        async with _get_session() as session:
            latest_date = await get_latest_membership_date(clan_tag, session=session)
            if latest_date is None:
                return set()
            result = await session.execute(
                select(ClanMemberDaily.player_tag).where(
                    ClanMemberDaily.clan_tag == clan_tag,
                    ClanMemberDaily.snapshot_date == latest_date,
                )
            )
            return {row[0] for row in result.all()}
    latest_date = await get_latest_membership_date(clan_tag, session=session)
    if latest_date is None:
        return set()
    result = await session.execute(
        select(ClanMemberDaily.player_tag).where(
            ClanMemberDaily.clan_tag == clan_tag,
            ClanMemberDaily.snapshot_date == latest_date,
        )
    )
    return {row[0] for row in result.all()}


async def get_week_leaderboard(
    season_id: int,
    section_index: int,
    clan_tag: str,
    session: AsyncSession | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if session is None:
        async with _get_session() as session:
            return await get_week_leaderboard(
                season_id, section_index, clan_tag, session=session
            )
    current_members = await get_current_member_tags(clan_tag, session=session)
    if not current_members:
        return [], []
    base = (
        select(
            PlayerParticipation.player_name,
            PlayerParticipation.player_tag,
            PlayerParticipation.decks_used,
            PlayerParticipation.fame,
        )
        .where(
            PlayerParticipation.season_id == season_id,
            PlayerParticipation.section_index == section_index,
            PlayerParticipation.player_tag.in_(current_members),
        )
    )
    inactive_result = await session.execute(
        base.order_by(
            PlayerParticipation.decks_used.asc(),
            PlayerParticipation.fame.asc(),
        ).limit(10)
    )
    active_result = await session.execute(
        base.order_by(
            PlayerParticipation.decks_used.desc(),
            PlayerParticipation.fame.desc(),
        ).limit(10)
    )
    inactive = [
        {
            "player_name": row.player_name,
            "player_tag": row.player_tag,
            "decks_used": int(row.decks_used),
            "fame": int(row.fame),
        }
        for row in inactive_result.all()
    ]
    active = [
        {
            "player_name": row.player_name,
            "player_tag": row.player_tag,
            "decks_used": int(row.decks_used),
            "fame": int(row.fame),
        }
        for row in active_result.all()
    ]
    return inactive, active


async def get_rolling_leaderboard(
    weeks: list[tuple[int, int]],
    clan_tag: str,
    session: AsyncSession | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if session is None:
        async with _get_session() as session:
            return await get_rolling_leaderboard(weeks, clan_tag, session=session)
    if not weeks:
        return [], []
    current_members = await get_current_member_tags(clan_tag, session=session)
    if not current_members:
        return [], []
    decks_sum = func.sum(PlayerParticipation.decks_used).label("decks_used_sum")
    fame_sum = func.sum(PlayerParticipation.fame).label("fame_sum")
    name_max = func.max(PlayerParticipation.player_name).label("player_name")
    base = (
        select(
            PlayerParticipation.player_tag,
            name_max,
            decks_sum,
            fame_sum,
        )
        .where(
            tuple_(PlayerParticipation.season_id, PlayerParticipation.section_index).in_(
                weeks
            ),
            PlayerParticipation.player_tag.in_(current_members),
        )
        .group_by(PlayerParticipation.player_tag)
    )
    inactive_result = await session.execute(
        base.order_by(decks_sum.asc(), fame_sum.asc()).limit(10)
    )
    active_result = await session.execute(
        base.order_by(decks_sum.desc(), fame_sum.desc()).limit(10)
    )
    inactive = [
        {
            "player_name": row.player_name,
            "player_tag": row.player_tag,
            "decks_used": int(row.decks_used_sum or 0),
            "fame": int(row.fame_sum or 0),
        }
        for row in inactive_result.all()
    ]
    active = [
        {
            "player_name": row.player_name,
            "player_tag": row.player_tag,
            "decks_used": int(row.decks_used_sum or 0),
            "fame": int(row.fame_sum or 0),
        }
        for row in active_result.all()
    ]
    return inactive, active


async def get_app_state(
    key: str, session: AsyncSession | None = None
) -> dict[str, Any] | None:
    if session is None:
        async with _get_session() as session:
            result = await session.execute(
                select(AppState).where(AppState.key == key)
            )
            state = result.scalar_one_or_none()
            return state.value if state else None
    result = await session.execute(select(AppState).where(AppState.key == key))
    state = result.scalar_one_or_none()
    return state.value if state else None


async def set_app_state(
    key: str, value: dict[str, Any], session: AsyncSession | None = None
) -> None:
    now = _utc_now()
    if session is None:
        async with _get_session() as session:
            try:
                await _upsert_app_state(session, now, key, value)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await _upsert_app_state(session, now, key, value)


async def delete_app_state(key: str, session: AsyncSession | None = None) -> None:
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(delete(AppState).where(AppState.key == key))
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(delete(AppState).where(AppState.key == key))


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


async def get_latest_war_race_state(clan_tag: str) -> dict[str, Any] | None:
    """Get the latest non-training River Race state for a clan."""
    async with _get_session() as session:
        result = await session.execute(
            select(RiverRaceState)
            .where(
                RiverRaceState.clan_tag == clan_tag,
                RiverRaceState.period_type != "training",
            )
            .order_by(
                RiverRaceState.season_id.desc(),
                RiverRaceState.section_index.desc(),
            )
            .limit(1)
        )
        state = result.scalar_one_or_none()
        return _river_race_state_to_dict(state) if state else None
