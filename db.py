"""Database module for PostgreSQL operations using SQLAlchemy async."""

from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
import logging
import os
from typing import Any, AsyncIterator

from sqlalchemy import (
    Boolean,
    BigInteger,
    Date,
    DateTime,
    Index,
    Integer,
    ForeignKey,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    case,
    delete,
    func,
    select,
    tuple_,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB, insert as pg_insert
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_last_seen(value: object) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if not value:
        return None
    if isinstance(value, str):
        for fmt in ("%Y%m%dT%H%M%S.%fZ", "%Y%m%dT%H%M%SZ"):
            try:
                parsed = datetime.strptime(value, fmt)
            except ValueError:
                continue
            return parsed.replace(tzinfo=timezone.utc)
    return None


def get_donation_week_start_date(dt_utc: datetime) -> date:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    current_date = dt_utc.astimezone(timezone.utc).date()
    weekday = current_date.weekday()  # Mon=0..Sun=6
    days_since_sunday = (weekday + 1) % 7
    return current_date - timedelta(days=days_since_sunday)


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
    donations: Mapped[int | None] = mapped_column(Integer)
    donations_received: Mapped[int | None] = mapped_column(Integer)
    clan_rank: Mapped[int | None] = mapped_column(Integer)
    previous_clan_rank: Mapped[int | None] = mapped_column(Integer)
    exp_level: Mapped[int | None] = mapped_column(Integer)
    last_seen: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, onupdate=_utc_now, nullable=False
    )


class ClanMemberDonationsWeekly(Base):
    __tablename__ = "clan_member_donations_weekly"
    __table_args__ = (
        UniqueConstraint(
            "clan_tag",
            "week_start_date",
            "player_tag",
            name="uq_clan_member_donations_weekly_clan_week_player",
        ),
        Index(
            "ix_clan_member_donations_weekly_clan_week",
            "clan_tag",
            "week_start_date",
        ),
        Index(
            "ix_clan_member_donations_weekly_player_tag",
            "player_tag",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    clan_tag: Mapped[str] = mapped_column(String(32), nullable=False)
    week_start_date: Mapped[date] = mapped_column(Date, nullable=False)
    player_tag: Mapped[str] = mapped_column(String(32), nullable=False)
    player_name: Mapped[str | None] = mapped_column(String(128))
    donations_week_total: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )
    donations_received_week_total: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )
    snapshots_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
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


class DailyReminderPost(Base):
    __tablename__ = "daily_reminder_posts"
    __table_args__ = (
        UniqueConstraint(
            "chat_id",
            "reminder_date",
            "season_id",
            "section_index",
            "period",
            "day_number",
            name="uq_daily_reminder_posts_unique",
        ),
        Index(
            "ix_daily_reminder_posts_date_chat",
            "reminder_date",
            "chat_id",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    reminder_date: Mapped[date] = mapped_column(Date, nullable=False)
    season_id: Mapped[int] = mapped_column(Integer, nullable=False)
    section_index: Mapped[int] = mapped_column(Integer, nullable=False)
    period: Mapped[str] = mapped_column(String(32), nullable=False)
    day_number: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )


class ClanApplication(Base):
    __tablename__ = "clan_applications"
    __table_args__ = (
        Index(
            "ix_clan_applications_status_created",
            "status",
            "created_at",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    telegram_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    telegram_username: Mapped[str | None] = mapped_column(Text)
    telegram_display_name: Mapped[str | None] = mapped_column(Text)
    player_name: Mapped[str] = mapped_column(Text, nullable=False)
    player_tag: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(16), default="pending", nullable=False)
    last_notified_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    notify_attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    invite_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, onupdate=_utc_now, nullable=False
    )


class ChatSettings(Base):
    __tablename__ = "chat_settings"

    chat_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    raid_mode: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    flood_window_seconds: Mapped[int] = mapped_column(
        Integer, nullable=False, default=10
    )
    flood_max_messages: Mapped[int] = mapped_column(
        Integer, nullable=False, default=6
    )
    flood_mute_minutes: Mapped[int] = mapped_column(
        Integer, nullable=False, default=10
    )
    new_user_link_block_hours: Mapped[int] = mapped_column(
        Integer, nullable=False, default=72
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, onupdate=_utc_now, nullable=False
    )


class ModAction(Base):
    __tablename__ = "mod_actions"
    __table_args__ = (
        Index("ix_mod_actions_chat_created", "chat_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    target_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    admin_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    reason: Mapped[str | None] = mapped_column(Text)
    message_id: Mapped[int | None] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )


class UserWarning(Base):
    __tablename__ = "user_warnings"
    __table_args__ = (
        UniqueConstraint("chat_id", "user_id", name="uq_user_warnings_chat_user"),
    )

    chat_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_warned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class UserPenalty(Base):
    __tablename__ = "user_penalties"
    __table_args__ = (
        UniqueConstraint(
            "chat_id",
            "user_id",
            "penalty",
            name="uq_user_penalties_chat_user_penalty",
        ),
    )

    chat_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    penalty: Mapped[str] = mapped_column(String(16), primary_key=True)
    until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )


class RateCounter(Base):
    __tablename__ = "rate_counters"
    __table_args__ = (
        UniqueConstraint(
            "chat_id",
            "user_id",
            name="uq_rate_counters_chat_user",
        ),
    )

    chat_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class ScheduledUnmute(Base):
    __tablename__ = "scheduled_unmutes"
    __table_args__ = (
        UniqueConstraint(
            "chat_id",
            "user_id",
            name="uq_scheduled_unmutes_chat_user",
        ),
        Index("ix_scheduled_unmutes_unmute_at", "unmute_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    unmute_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    reason: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AppState(Base):
    __tablename__ = "app_state"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, onupdate=_utc_now, nullable=False
    )


class UserLink(Base):
    __tablename__ = "user_links"

    telegram_user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    player_tag: Mapped[str] = mapped_column(String(32), nullable=False)
    player_name: Mapped[str] = mapped_column(String(128), nullable=False)
    linked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )
    source: Mapped[str] = mapped_column(String(16), nullable=False)


class UserLinkRequest(Base):
    __tablename__ = "user_link_requests"

    telegram_user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    origin_chat_id: Mapped[int | None] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )


class CaptchaQuestion(Base):
    __tablename__ = "captcha_questions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    question_text: Mapped[str] = mapped_column(Text, nullable=False)
    option_a: Mapped[str] = mapped_column(Text, nullable=False)
    option_b: Mapped[str] = mapped_column(Text, nullable=False)
    option_c: Mapped[str] = mapped_column(Text, nullable=False)
    option_d: Mapped[str] = mapped_column(Text, nullable=False)
    correct_option: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )


class CaptchaChallenge(Base):
    __tablename__ = "captcha_challenges"
    __table_args__ = (
        Index("ix_captcha_challenges_chat_user", "chat_id", "user_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    question_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("captcha_questions.id"), nullable=False
    )
    message_id: Mapped[int | None] = mapped_column(BigInteger)
    attempts: Mapped[int] = mapped_column(SmallInteger, default=0, nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="pending", nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, onupdate=_utc_now, nullable=False
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_reminded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class VerifiedUser(Base):
    __tablename__ = "verified_users"
    __table_args__ = (
        UniqueConstraint("chat_id", "user_id", name="uq_verified_users_chat_user"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    verified_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )


APP_STATE_COLOSSEUM_KEY = "colosseum_index_by_season"


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
        pool_kwargs = {"pool_pre_ping": True}
        def _pool_int(name, raw):
            if raw is None or raw.strip() == "":
                return None
            try:
                return int(raw)
            except ValueError:
                logger.warning("Invalid %s value %r; ignoring.", name, raw)
                return None

        pool_size = _pool_int("DB_POOL_SIZE", os.getenv("DB_POOL_SIZE"))
        max_overflow = _pool_int("DB_MAX_OVERFLOW", os.getenv("DB_MAX_OVERFLOW"))
        pool_timeout = _pool_int("DB_POOL_TIMEOUT", os.getenv("DB_POOL_TIMEOUT"))
        pool_recycle = _pool_int("DB_POOL_RECYCLE", os.getenv("DB_POOL_RECYCLE"))
        if pool_size is not None:
            pool_kwargs["pool_size"] = pool_size
        if max_overflow is not None:
            pool_kwargs["max_overflow"] = max_overflow
        if pool_timeout is not None:
            pool_kwargs["pool_timeout"] = pool_timeout
        if pool_recycle is not None:
            pool_kwargs["pool_recycle"] = pool_recycle
        _engine = create_async_engine(async_url, **pool_kwargs)
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
    donations: int | None,
    donations_received: int | None,
    clan_rank: int | None,
    previous_clan_rank: int | None,
    exp_level: int | None,
    last_seen: datetime | None,
) -> None:
    stmt = pg_insert(ClanMemberDaily.__table__).values(
        snapshot_date=snapshot_date,
        clan_tag=clan_tag,
        player_tag=player_tag,
        player_name=player_name,
        role=role,
        trophies=trophies,
        donations=donations,
        donations_received=donations_received,
        clan_rank=clan_rank,
        previous_clan_rank=previous_clan_rank,
        exp_level=exp_level,
        last_seen=last_seen,
        created_at=now,
        updated_at=now,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["snapshot_date", "clan_tag", "player_tag"],
        set_={
            "player_name": stmt.excluded.player_name,
            "role": stmt.excluded.role,
            "trophies": stmt.excluded.trophies,
            "donations": stmt.excluded.donations,
            "donations_received": stmt.excluded.donations_received,
            "clan_rank": stmt.excluded.clan_rank,
            "previous_clan_rank": stmt.excluded.previous_clan_rank,
            "exp_level": stmt.excluded.exp_level,
            "last_seen": stmt.excluded.last_seen,
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
    donations: int | None = None,
    donations_received: int | None = None,
    clan_rank: int | None = None,
    previous_clan_rank: int | None = None,
    exp_level: int | None = None,
    last_seen: datetime | None = None,
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
                    donations,
                    donations_received,
                    clan_rank,
                    previous_clan_rank,
                    exp_level,
                    _parse_last_seen(last_seen),
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
            donations,
            donations_received,
            clan_rank,
            previous_clan_rank,
            exp_level,
            _parse_last_seen(last_seen),
        )


async def upsert_clan_member_daily(
    snapshot_date: date,
    clan_tag: str,
    members: list[dict[str, Any]],
    session: AsyncSession | None = None,
) -> None:
    now = _utc_now()
    if session is None:
        async with _get_session() as session:
            try:
                await upsert_clan_member_daily(
                    snapshot_date, clan_tag, members, session=session
                )
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            return
    for member in members:
        player_tag = member.get("tag", "")
        if not player_tag:
            continue
        donations = member.get("donations")
        donations_received = member.get("donationsReceived")
        clan_rank = member.get("clanRank")
        previous_clan_rank = member.get("previousClanRank")
        exp_level = member.get("expLevel")
        last_seen = _parse_last_seen(member.get("lastSeen"))
        await _upsert_clan_member_daily(
            session,
            now,
            snapshot_date,
            clan_tag,
            player_tag,
            member.get("name", "Unknown"),
            member.get("role"),
            member.get("trophies"),
            int(donations) if donations is not None else None,
            int(donations_received) if donations_received is not None else None,
            int(clan_rank) if clan_rank is not None else None,
            int(previous_clan_rank) if previous_clan_rank is not None else None,
            int(exp_level) if exp_level is not None else None,
            last_seen,
        )


async def upsert_donations_weekly(
    clan_tag: str,
    week_start_date: date,
    members: list[dict[str, Any]],
    session: AsyncSession | None = None,
) -> None:
    now = _utc_now()
    if session is None:
        async with _get_session() as session:
            try:
                await upsert_donations_weekly(
                    clan_tag, week_start_date, members, session=session
                )
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            return

    for member in members:
        player_tag = member.get("tag", "")
        if not player_tag:
            continue
        donations = member.get("donations")
        donations_received = member.get("donationsReceived")
        stmt = pg_insert(ClanMemberDonationsWeekly.__table__).values(
            clan_tag=clan_tag,
            week_start_date=week_start_date,
            player_tag=player_tag,
            player_name=member.get("name"),
            donations_week_total=int(donations) if donations is not None else 0,
            donations_received_week_total=int(donations_received)
            if donations_received is not None
            else 0,
            snapshots_count=1,
            updated_at=now,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["clan_tag", "week_start_date", "player_tag"],
            set_={
                "player_name": stmt.excluded.player_name,
                "donations_week_total": func.greatest(
                    ClanMemberDonationsWeekly.donations_week_total,
                    stmt.excluded.donations_week_total,
                ),
                "donations_received_week_total": func.greatest(
                    ClanMemberDonationsWeekly.donations_received_week_total,
                    stmt.excluded.donations_received_week_total,
                ),
                "snapshots_count": ClanMemberDonationsWeekly.snapshots_count + 1,
                "updated_at": now,
            },
        )
        await session.execute(stmt)


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


async def try_mark_reminder_posted(
    *,
    chat_id: int,
    reminder_date: date,
    season_id: int,
    section_index: int,
    period: str,
    day_number: int,
    session: AsyncSession | None = None,
) -> bool:
    now = _utc_now()
    stmt = pg_insert(DailyReminderPost.__table__).values(
        chat_id=chat_id,
        reminder_date=reminder_date,
        season_id=season_id,
        section_index=section_index,
        period=period,
        day_number=day_number,
        created_at=now,
    )
    stmt = stmt.on_conflict_do_nothing(
        constraint="uq_daily_reminder_posts_unique"
    ).returning(DailyReminderPost.id)
    if session is None:
        async with _get_session() as session:
            try:
                result = await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        result = await session.execute(stmt)
    inserted_id = result.scalar_one_or_none()
    return inserted_id is not None


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


async def get_first_snapshot_date_for_week(
    season_id: int, section_index: int, session: AsyncSession | None = None
) -> date | None:
    if session is None:
        async with _get_session() as session:
            return await get_first_snapshot_date_for_week(
                season_id, section_index, session=session
            )
    result = await session.execute(
        select(func.min(PlayerParticipationDaily.snapshot_date)).where(
            PlayerParticipationDaily.season_id == season_id,
            PlayerParticipationDaily.section_index == section_index,
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


async def get_current_member_snapshot(
    clan_tag: str, session: AsyncSession | None = None
) -> dict[str, str]:
    if session is None:
        async with _get_session() as session:
            return await get_current_member_snapshot(clan_tag, session=session)
    latest_date = await get_latest_membership_date(clan_tag, session=session)
    if latest_date is None:
        return {}
    result = await session.execute(
        select(ClanMemberDaily.player_tag, ClanMemberDaily.player_name).where(
            ClanMemberDaily.clan_tag == clan_tag,
            ClanMemberDaily.snapshot_date == latest_date,
        )
    )
    return {row.player_tag: row.player_name for row in result.all()}


async def get_latest_member_snapshot_date(
    clan_tag: str, session: AsyncSession | None = None
) -> date | None:
    if session is None:
        async with _get_session() as session:
            return await get_latest_member_snapshot_date(clan_tag, session=session)
    return await get_latest_membership_date(clan_tag, session=session)


async def get_current_members_snapshot(
    clan_tag: str, session: AsyncSession | None = None
) -> list[dict[str, object]]:
    if session is None:
        async with _get_session() as session:
            return await get_current_members_snapshot(clan_tag, session=session)
    latest_date = await get_latest_membership_date(clan_tag, session=session)
    if latest_date is None:
        return []
    result = await session.execute(
        select(
            ClanMemberDaily.player_tag,
            ClanMemberDaily.player_name,
            ClanMemberDaily.role,
            ClanMemberDaily.last_seen,
            ClanMemberDaily.donations,
        ).where(
            ClanMemberDaily.clan_tag == clan_tag,
            ClanMemberDaily.snapshot_date == latest_date,
        )
    )
    return [
        {
            "player_tag": row.player_tag,
            "player_name": row.player_name,
            "role": row.role,
            "last_seen": row.last_seen,
            "donations": int(row.donations) if row.donations is not None else None,
        }
        for row in result.all()
    ]


async def get_last_seen_map(
    clan_tag: str, session: AsyncSession | None = None
) -> dict[str, datetime | None]:
    if session is None:
        async with _get_session() as session:
            return await get_last_seen_map(clan_tag, session=session)
    snapshot = await get_current_members_snapshot(clan_tag, session=session)
    return {row["player_tag"]: row.get("last_seen") for row in snapshot}


async def get_top_absent_members(
    clan_tag: str, limit: int, session: AsyncSession | None = None
) -> list[dict[str, object]]:
    if limit <= 0:
        return []
    if session is None:
        async with _get_session() as session:
            return await get_top_absent_members(clan_tag, limit, session=session)
    rows = await get_current_members_snapshot(clan_tag, session=session)
    if not rows:
        return []
    now = _utc_now()
    for row in rows:
        last_seen = row.get("last_seen")
        if isinstance(last_seen, datetime):
            if last_seen.tzinfo is None:
                last_seen = last_seen.replace(tzinfo=timezone.utc)
            delta = now - last_seen
            days_absent = max(0, delta.days)
        else:
            days_absent = None
        row["days_absent"] = days_absent
    rows.sort(
        key=lambda row: (
            row.get("days_absent") is None,
            -(row.get("days_absent") or 0),
        )
    )
    return rows[:limit]


async def get_current_wtd_donations(
    clan_tag: str,
    player_tags: set[str] | None = None,
    session: AsyncSession | None = None,
) -> dict[str, dict[str, int | None]]:
    if session is None:
        async with _get_session() as session:
            return await get_current_wtd_donations(
                clan_tag, player_tags=player_tags, session=session
            )
    if player_tags is not None and not player_tags:
        return {}
    latest_date = await get_latest_membership_date(clan_tag, session=session)
    if latest_date is None:
        return {}
    query = select(
        ClanMemberDaily.player_tag,
        ClanMemberDaily.donations,
        ClanMemberDaily.donations_received,
    ).where(
        ClanMemberDaily.clan_tag == clan_tag,
        ClanMemberDaily.snapshot_date == latest_date,
    )
    if player_tags:
        query = query.where(ClanMemberDaily.player_tag.in_(player_tags))
    result = await session.execute(query)
    donations_map: dict[str, dict[str, int | None]] = {}
    for row in result.all():
        donations_map[row.player_tag] = {
            "donations": int(row.donations) if row.donations is not None else None,
            "donations_received": int(row.donations_received)
            if row.donations_received is not None
            else None,
        }
    return donations_map


async def get_clan_wtd_donation_average(
    clan_tag: str,
    snapshot_date: date | None = None,
    session: AsyncSession | None = None,
) -> int | None:
    if session is None:
        async with _get_session() as session:
            return await get_clan_wtd_donation_average(
                clan_tag, snapshot_date=snapshot_date, session=session
            )
    if snapshot_date is None:
        snapshot_date = await get_latest_membership_date(clan_tag, session=session)
    if snapshot_date is None:
        return None
    result = await session.execute(
        select(func.avg(ClanMemberDaily.donations)).where(
            ClanMemberDaily.clan_tag == clan_tag,
            ClanMemberDaily.snapshot_date == snapshot_date,
            ClanMemberDaily.donations.is_not(None),
        )
    )
    avg_value = result.scalar_one_or_none()
    if avg_value is None:
        return None
    return int(round(avg_value))


async def get_current_members_with_wtd_donations(
    clan_tag: str,
    session: AsyncSession | None = None,
) -> list[dict[str, object]]:
    if session is None:
        async with _get_session() as session:
            return await get_current_members_with_wtd_donations(
                clan_tag, session=session
            )
    latest_date = await get_latest_membership_date(clan_tag, session=session)
    if latest_date is None:
        return []
    result = await session.execute(
        select(
            ClanMemberDaily.player_tag,
            ClanMemberDaily.player_name,
            ClanMemberDaily.donations,
        ).where(
            ClanMemberDaily.clan_tag == clan_tag,
            ClanMemberDaily.snapshot_date == latest_date,
        )
    )
    return [
        {
            "player_tag": row.player_tag,
            "player_name": row.player_name,
            "donations": int(row.donations) if row.donations is not None else None,
        }
        for row in result.all()
    ]


async def get_donation_weekly_sums_for_window(
    clan_tag: str,
    window_weeks: int,
    session: AsyncSession | None = None,
) -> tuple[list[dict[str, object]], int]:
    if window_weeks <= 0:
        return [], 0
    if session is None:
        async with _get_session() as session:
            return await get_donation_weekly_sums_for_window(
                clan_tag, window_weeks=window_weeks, session=session
            )
    latest_date = await get_latest_membership_date(clan_tag, session=session)
    if latest_date is None:
        return [], 0
    current_members = (
        select(ClanMemberDaily.player_tag, ClanMemberDaily.player_name)
        .where(
            ClanMemberDaily.clan_tag == clan_tag,
            ClanMemberDaily.snapshot_date == latest_date,
        )
        .subquery()
    )
    week_dates_result = await session.execute(
        select(ClanMemberDonationsWeekly.week_start_date)
        .where(ClanMemberDonationsWeekly.clan_tag == clan_tag)
        .distinct()
        .order_by(ClanMemberDonationsWeekly.week_start_date.desc())
        .limit(window_weeks)
    )
    week_dates = [row.week_start_date for row in week_dates_result.all()]
    if not week_dates:
        return [], 0
    coverage = len(week_dates)
    name_expr = func.coalesce(
        func.max(ClanMemberDonationsWeekly.player_name),
        func.max(current_members.c.player_name),
    )
    sum_expr = func.sum(ClanMemberDonationsWeekly.donations_week_total)
    weeks_expr = func.count(func.distinct(ClanMemberDonationsWeekly.week_start_date))
    result = await session.execute(
        select(
            ClanMemberDonationsWeekly.player_tag,
            name_expr.label("player_name"),
            sum_expr.label("donations_sum"),
            weeks_expr.label("weeks_present"),
        )
        .join(
            current_members,
            current_members.c.player_tag == ClanMemberDonationsWeekly.player_tag,
        )
        .where(
            ClanMemberDonationsWeekly.clan_tag == clan_tag,
            ClanMemberDonationsWeekly.week_start_date.in_(week_dates),
        )
        .group_by(ClanMemberDonationsWeekly.player_tag)
    )
    return (
        [
            {
                "player_tag": row.player_tag,
                "player_name": row.player_name,
                "donations_sum": int(row.donations_sum or 0),
                "weeks_present": int(row.weeks_present or 0),
            }
            for row in result.all()
        ],
        coverage,
    )


async def get_war_stats_for_weeks(
    clan_tag: str,
    weeks: list[tuple[int, int]],
    session: AsyncSession | None = None,
) -> dict[str, dict[str, float | int]]:
    if not weeks:
        return {}
    if session is None:
        async with _get_session() as session:
            return await get_war_stats_for_weeks(clan_tag, weeks, session=session)
    current_members = await get_current_member_tags(clan_tag, session=session)
    if not current_members:
        return {}
    result = await session.execute(
        select(
            PlayerParticipation.player_tag,
            func.count().label("weeks_played"),
            func.sum(
                case((PlayerParticipation.decks_used >= 8, 1), else_=0)
            ).label("active_weeks"),
            func.avg(PlayerParticipation.decks_used).label("avg_decks"),
            func.avg(PlayerParticipation.fame).label("avg_fame"),
        )
        .where(
            tuple_(
                PlayerParticipation.season_id, PlayerParticipation.section_index
            ).in_(weeks),
            PlayerParticipation.player_tag.in_(current_members),
        )
        .group_by(PlayerParticipation.player_tag)
    )
    stats: dict[str, dict[str, float | int]] = {}
    for row in result.all():
        stats[row.player_tag] = {
            "weeks_played": int(row.weeks_played or 0),
            "active_weeks": int(row.active_weeks or 0),
            "avg_decks": float(row.avg_decks or 0),
            "avg_fame": float(row.avg_fame or 0),
        }
    return stats


async def get_alltime_weeks_played(
    clan_tag: str, session: AsyncSession | None = None
) -> dict[str, int]:
    if session is None:
        async with _get_session() as session:
            return await get_alltime_weeks_played(clan_tag, session=session)
    current_members = await get_current_member_tags(clan_tag, session=session)
    if not current_members:
        return {}
    distinct_weeks = (
        select(
            PlayerParticipation.player_tag,
            PlayerParticipation.season_id,
            PlayerParticipation.section_index,
        )
        .where(PlayerParticipation.player_tag.in_(current_members))
        .group_by(
            PlayerParticipation.player_tag,
            PlayerParticipation.season_id,
            PlayerParticipation.section_index,
        )
        .subquery()
    )
    result = await session.execute(
        select(
            distinct_weeks.c.player_tag,
            func.count().label("weeks_played"),
        ).group_by(distinct_weeks.c.player_tag)
    )
    return {
        row.player_tag: int(row.weeks_played or 0) for row in result.all()
    }


async def get_top_donors_wtd(
    clan_tag: str,
    limit: int = 5,
    session: AsyncSession | None = None,
) -> list[dict[str, object]]:
    if limit <= 0:
        return []
    if session is None:
        async with _get_session() as session:
            return await get_top_donors_wtd(clan_tag, limit=limit, session=session)
    latest_date = await get_latest_membership_date(clan_tag, session=session)
    if latest_date is None:
        return []
    result = await session.execute(
        select(
            ClanMemberDaily.player_tag,
            ClanMemberDaily.player_name,
            ClanMemberDaily.donations,
        )
        .where(
            ClanMemberDaily.clan_tag == clan_tag,
            ClanMemberDaily.snapshot_date == latest_date,
            ClanMemberDaily.donations.is_not(None),
        )
        .order_by(
            ClanMemberDaily.donations.desc(),
            ClanMemberDaily.player_name.asc(),
        )
        .limit(limit)
    )
    return [
        {
            "player_tag": row.player_tag,
            "player_name": row.player_name,
            "donations": int(row.donations) if row.donations is not None else None,
        }
        for row in result.all()
    ]


async def get_top_donors_window(
    clan_tag: str,
    window_weeks: int,
    limit: int = 5,
    session: AsyncSession | None = None,
) -> list[dict[str, object]]:
    if window_weeks <= 0 or limit <= 0:
        return []
    if session is None:
        async with _get_session() as session:
            return await get_top_donors_window(
                clan_tag, window_weeks=window_weeks, limit=limit, session=session
            )
    latest_date = await get_latest_membership_date(clan_tag, session=session)
    if latest_date is None:
        return []
    current_members = (
        select(ClanMemberDaily.player_tag, ClanMemberDaily.player_name)
        .where(
            ClanMemberDaily.clan_tag == clan_tag,
            ClanMemberDaily.snapshot_date == latest_date,
        )
        .subquery()
    )
    week_dates_result = await session.execute(
        select(ClanMemberDonationsWeekly.week_start_date)
        .where(ClanMemberDonationsWeekly.clan_tag == clan_tag)
        .distinct()
        .order_by(ClanMemberDonationsWeekly.week_start_date.desc())
        .limit(window_weeks)
    )
    week_dates = [row.week_start_date for row in week_dates_result.all()]
    if not week_dates:
        return []
    name_expr = func.coalesce(
        func.max(ClanMemberDonationsWeekly.player_name),
        func.max(current_members.c.player_name),
    )
    sum_expr = func.sum(ClanMemberDonationsWeekly.donations_week_total)
    weeks_expr = func.count(func.distinct(ClanMemberDonationsWeekly.week_start_date))
    result = await session.execute(
        select(
            ClanMemberDonationsWeekly.player_tag,
            name_expr.label("player_name"),
            sum_expr.label("donations_sum"),
            weeks_expr.label("weeks_present"),
        )
        .join(
            current_members,
            current_members.c.player_tag == ClanMemberDonationsWeekly.player_tag,
        )
        .where(
            ClanMemberDonationsWeekly.clan_tag == clan_tag,
            ClanMemberDonationsWeekly.week_start_date.in_(week_dates),
        )
        .group_by(ClanMemberDonationsWeekly.player_tag)
        .order_by(sum_expr.desc(), weeks_expr.desc(), name_expr.asc())
        .limit(limit)
    )
    return [
        {
            "player_tag": row.player_tag,
            "player_name": row.player_name,
            "donations_sum": int(row.donations_sum or 0),
            "weeks_present": int(row.weeks_present or 0),
        }
        for row in result.all()
    ]


async def get_donations_weekly_sums(
    clan_tag: str,
    player_tags: set[str] | None,
    window_weeks: int,
    session: AsyncSession | None = None,
) -> dict[str, dict[str, int]]:
    if window_weeks <= 0:
        return {}
    if session is None:
        async with _get_session() as session:
            return await get_donations_weekly_sums(
                clan_tag,
                player_tags=player_tags,
                window_weeks=window_weeks,
                session=session,
            )
    if player_tags is not None and not player_tags:
        return {}
    week_dates_result = await session.execute(
        select(ClanMemberDonationsWeekly.week_start_date)
        .where(ClanMemberDonationsWeekly.clan_tag == clan_tag)
        .distinct()
        .order_by(ClanMemberDonationsWeekly.week_start_date.desc())
        .limit(window_weeks)
    )
    week_dates = [row.week_start_date for row in week_dates_result.all()]
    if not week_dates:
        return {}

    query = (
        select(
            ClanMemberDonationsWeekly.player_tag,
            func.sum(ClanMemberDonationsWeekly.donations_week_total).label("sum_total"),
            func.count().label("weeks_present"),
        )
        .where(
            ClanMemberDonationsWeekly.clan_tag == clan_tag,
            ClanMemberDonationsWeekly.week_start_date.in_(week_dates),
        )
        .group_by(ClanMemberDonationsWeekly.player_tag)
    )
    if player_tags:
        query = query.where(ClanMemberDonationsWeekly.player_tag.in_(player_tags))
    result = await session.execute(query)
    return {
        row.player_tag: {
            "sum": int(row.sum_total or 0),
            "weeks_present": int(row.weeks_present or 0),
        }
        for row in result.all()
    }


async def get_player_name_for_tag(
    player_tag: str,
    clan_tag: str,
    session: AsyncSession | None = None,
) -> str | None:
    if session is None:
        async with _get_session() as session:
            return await get_player_name_for_tag(
                player_tag, clan_tag, session=session
            )
    latest_date = await get_latest_membership_date(clan_tag, session=session)
    if latest_date is not None:
        result = await session.execute(
            select(ClanMemberDaily.player_name).where(
                ClanMemberDaily.clan_tag == clan_tag,
                ClanMemberDaily.snapshot_date == latest_date,
                ClanMemberDaily.player_tag == player_tag,
            )
        )
        name = result.scalar_one_or_none()
        if name:
            return name
    result = await session.execute(
        select(func.max(PlayerParticipation.player_name)).where(
            PlayerParticipation.player_tag == player_tag
        )
    )
    return result.scalar_one_or_none()


async def get_member_first_seen_dates(
    clan_tag: str,
    player_tags: set[str] | None = None,
    session: AsyncSession | None = None,
) -> dict[str, date]:
    if session is None:
        async with _get_session() as session:
            return await get_member_first_seen_dates(
                clan_tag, player_tags=player_tags, session=session
            )
    if player_tags is not None and not player_tags:
        return {}
    query = (
        select(
            ClanMemberDaily.player_tag,
            func.min(ClanMemberDaily.snapshot_date).label("first_seen"),
        )
        .where(ClanMemberDaily.clan_tag == clan_tag)
        .group_by(ClanMemberDaily.player_tag)
    )
    if player_tags:
        query = query.where(ClanMemberDaily.player_tag.in_(player_tags))
    result = await session.execute(query)
    return {row.player_tag: row.first_seen for row in result.all()}


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


async def get_week_decks_map(
    season_id: int,
    section_index: int,
    player_tags: set[str] | None = None,
    session: AsyncSession | None = None,
) -> dict[str, int]:
    if session is None:
        async with _get_session() as session:
            return await get_week_decks_map(
                season_id, section_index, player_tags=player_tags, session=session
            )
    if player_tags is not None and not player_tags:
        return {}
    query = select(
        PlayerParticipation.player_tag,
        PlayerParticipation.decks_used,
    ).where(
        PlayerParticipation.season_id == season_id,
        PlayerParticipation.section_index == section_index,
    )
    if player_tags:
        query = query.where(PlayerParticipation.player_tag.in_(player_tags))
    result = await session.execute(query)
    return {row.player_tag: int(row.decks_used) for row in result.all()}


async def get_participation_week_counts(
    player_tags: set[str] | None = None,
    session: AsyncSession | None = None,
) -> dict[str, int]:
    if session is None:
        async with _get_session() as session:
            return await get_participation_week_counts(
                player_tags=player_tags, session=session
            )
    if player_tags is not None and not player_tags:
        return {}
    query = (
        select(
            PlayerParticipation.player_tag,
            func.count().label("week_count"),
        )
        .group_by(PlayerParticipation.player_tag)
    )
    if player_tags:
        query = query.where(PlayerParticipation.player_tag.in_(player_tags))
    result = await session.execute(query)
    return {row.player_tag: int(row.week_count) for row in result.all()}


async def get_rolling_summary(
    weeks: list[tuple[int, int]],
    player_tags: set[str] | None = None,
    session: AsyncSession | None = None,
) -> list[dict[str, Any]]:
    if session is None:
        async with _get_session() as session:
            return await get_rolling_summary(weeks, player_tags=player_tags, session=session)
    if not weeks:
        return []
    if player_tags is not None and not player_tags:
        return []
    decks_sum = func.sum(PlayerParticipation.decks_used).label("decks_used_sum")
    fame_sum = func.sum(PlayerParticipation.fame).label("fame_sum")
    name_max = func.max(PlayerParticipation.player_name).label("player_name")
    query = (
        select(
            PlayerParticipation.player_tag,
            name_max,
            decks_sum,
            fame_sum,
        )
        .where(
            tuple_(PlayerParticipation.season_id, PlayerParticipation.section_index).in_(
                weeks
            )
        )
        .group_by(PlayerParticipation.player_tag)
    )
    if player_tags:
        query = query.where(PlayerParticipation.player_tag.in_(player_tags))
    result = await session.execute(query)
    return [
        {
            "player_tag": row.player_tag,
            "player_name": row.player_name,
            "decks_used": int(row.decks_used_sum or 0),
            "fame": int(row.fame_sum or 0),
        }
        for row in result.all()
    ]


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


def _application_to_dict(app: ClanApplication) -> dict[str, Any]:
    return {
        "id": app.id,
        "telegram_user_id": app.telegram_user_id,
        "telegram_username": app.telegram_username,
        "telegram_display_name": app.telegram_display_name,
        "player_name": app.player_name,
        "player_tag": app.player_tag,
        "status": app.status,
        "last_notified_at": app.last_notified_at,
        "notify_attempts": app.notify_attempts,
        "invite_expires_at": app.invite_expires_at,
        "created_at": app.created_at,
        "updated_at": app.updated_at,
    }


async def get_pending_application_for_user(
    telegram_user_id: int, session: AsyncSession | None = None
) -> dict[str, Any] | None:
    if session is None:
        async with _get_session() as session:
            return await get_pending_application_for_user(
                telegram_user_id, session=session
            )
    result = await session.execute(
        select(ClanApplication)
        .where(
            ClanApplication.telegram_user_id == telegram_user_id,
            ClanApplication.status == "pending",
        )
        .order_by(ClanApplication.created_at.desc())
        .limit(1)
    )
    app = result.scalar_one_or_none()
    if not app:
        return None
    return _application_to_dict(app)


async def get_last_rejected_time_for_user(
    telegram_user_id: int, session: AsyncSession | None = None
) -> datetime | None:
    if session is None:
        async with _get_session() as session:
            return await get_last_rejected_time_for_user(
                telegram_user_id, session=session
            )
    result = await session.execute(
        select(func.max(ClanApplication.updated_at)).where(
            ClanApplication.telegram_user_id == telegram_user_id,
            ClanApplication.status == "rejected",
        )
    )
    return result.scalar_one()


async def count_pending_applications(
    session: AsyncSession | None = None,
) -> int:
    if session is None:
        async with _get_session() as session:
            return await count_pending_applications(session=session)
    result = await session.execute(
        select(func.count()).select_from(ClanApplication).where(
            ClanApplication.status == "pending"
        )
    )
    return int(result.scalar_one() or 0)


async def create_application(
    *,
    telegram_user_id: int,
    telegram_username: str | None,
    telegram_display_name: str | None,
    player_name: str,
    player_tag: str | None,
    session: AsyncSession | None = None,
) -> dict[str, Any]:
    now = _utc_now()
    stmt = pg_insert(ClanApplication.__table__).values(
        telegram_user_id=telegram_user_id,
        telegram_username=telegram_username,
        telegram_display_name=telegram_display_name,
        player_name=player_name,
        player_tag=player_tag,
        status="pending",
        created_at=now,
        updated_at=now,
    ).returning(ClanApplication.id)
    if session is None:
        async with _get_session() as session:
            try:
                result = await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        result = await session.execute(stmt)
    app_id = result.scalar_one()
    return {
        "id": app_id,
        "telegram_user_id": telegram_user_id,
        "telegram_username": telegram_username,
        "telegram_display_name": telegram_display_name,
        "player_name": player_name,
        "player_tag": player_tag,
        "status": "pending",
        "created_at": now,
        "updated_at": now,
    }


async def update_application_tag(
    app_id: int,
    player_tag: str | None,
    session: AsyncSession | None = None,
) -> bool:
    now = _utc_now()
    stmt = (
        update(ClanApplication)
        .where(ClanApplication.id == app_id)
        .values(player_tag=player_tag, updated_at=now)
    )
    if session is None:
        async with _get_session() as session:
            try:
                result = await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        result = await session.execute(stmt)
    return bool(result.rowcount)


async def list_pending_applications(
    limit: int = 10, session: AsyncSession | None = None
) -> list[dict[str, Any]]:
    if session is None:
        async with _get_session() as session:
            return await list_pending_applications(limit=limit, session=session)
    result = await session.execute(
        select(ClanApplication)
        .where(ClanApplication.status == "pending")
        .order_by(ClanApplication.created_at.desc())
        .limit(limit)
    )
    return [_application_to_dict(app) for app in result.scalars().all()]


async def get_application_by_id(
    app_id: int, session: AsyncSession | None = None
) -> dict[str, Any] | None:
    if session is None:
        async with _get_session() as session:
            return await get_application_by_id(app_id, session=session)
    result = await session.execute(
        select(ClanApplication).where(ClanApplication.id == app_id)
    )
    app = result.scalar_one_or_none()
    if not app:
        return None
    return _application_to_dict(app)


async def set_application_status(
    app_id: int, status: str, session: AsyncSession | None = None
) -> bool:
    now = _utc_now()
    stmt = (
        update(ClanApplication)
        .where(ClanApplication.id == app_id)
        .values(status=status, updated_at=now)
    )
    if session is None:
        async with _get_session() as session:
            try:
                result = await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        result = await session.execute(stmt)
    return bool(result.rowcount)


def _chat_settings_to_dict(settings: ChatSettings) -> dict[str, Any]:
    return {
        "chat_id": settings.chat_id,
        "raid_mode": settings.raid_mode,
        "flood_window_seconds": settings.flood_window_seconds,
        "flood_max_messages": settings.flood_max_messages,
        "flood_mute_minutes": settings.flood_mute_minutes,
        "new_user_link_block_hours": settings.new_user_link_block_hours,
        "updated_at": settings.updated_at,
    }


async def get_chat_settings(
    chat_id: int,
    *,
    defaults: dict[str, int | bool],
    session: AsyncSession | None = None,
) -> dict[str, Any]:
    if session is None:
        async with _get_session() as session:
            return await get_chat_settings(
                chat_id, defaults=defaults, session=session
            )
    result = await session.execute(
        select(ChatSettings).where(ChatSettings.chat_id == chat_id)
    )
    settings = result.scalar_one_or_none()
    if settings:
        return _chat_settings_to_dict(settings)
    now = _utc_now()
    stmt = pg_insert(ChatSettings.__table__).values(
        chat_id=chat_id,
        raid_mode=bool(defaults.get("raid_mode", False)),
        flood_window_seconds=int(defaults.get("flood_window_seconds", 10)),
        flood_max_messages=int(defaults.get("flood_max_messages", 6)),
        flood_mute_minutes=int(defaults.get("flood_mute_minutes", 10)),
        new_user_link_block_hours=int(
            defaults.get("new_user_link_block_hours", 72)
        ),
        updated_at=now,
    )
    stmt = stmt.on_conflict_do_nothing(index_elements=["chat_id"])
    await session.execute(stmt)
    await session.commit()
    settings = await session.get(ChatSettings, chat_id)
    if settings:
        return _chat_settings_to_dict(settings)
    return {
        "chat_id": chat_id,
        "raid_mode": bool(defaults.get("raid_mode", False)),
        "flood_window_seconds": int(defaults.get("flood_window_seconds", 10)),
        "flood_max_messages": int(defaults.get("flood_max_messages", 6)),
        "flood_mute_minutes": int(defaults.get("flood_mute_minutes", 10)),
        "new_user_link_block_hours": int(
            defaults.get("new_user_link_block_hours", 72)
        ),
        "updated_at": now,
    }


async def set_chat_raid_mode(
    chat_id: int, raid_mode: bool, session: AsyncSession | None = None
) -> None:
    now = _utc_now()
    stmt = pg_insert(ChatSettings.__table__).values(
        chat_id=chat_id,
        raid_mode=raid_mode,
        updated_at=now,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["chat_id"],
        set_={"raid_mode": raid_mode, "updated_at": now},
    )
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(stmt)


async def record_rate_counter(
    chat_id: int,
    user_id: int,
    *,
    window_seconds: int,
    now: datetime,
    session: AsyncSession | None = None,
) -> int:
    if session is None:
        async with _get_session() as session:
            return await record_rate_counter(
                chat_id,
                user_id,
                window_seconds=window_seconds,
                now=now,
                session=session,
            )
    result = await session.execute(
        select(RateCounter).where(
            RateCounter.chat_id == chat_id, RateCounter.user_id == user_id
        )
    )
    counter = result.scalar_one_or_none()
    if counter is None:
        stmt = pg_insert(RateCounter.__table__).values(
            chat_id=chat_id,
            user_id=user_id,
            window_start=now,
            count=1,
        )
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["chat_id", "user_id"]
        )
        await session.execute(stmt)
        await session.commit()
        return 1
    elapsed = (now - counter.window_start).total_seconds()
    if elapsed > window_seconds:
        counter.window_start = now
        counter.count = 1
    else:
        counter.count += 1
    await session.commit()
    return int(counter.count)


async def increment_user_warning(
    chat_id: int,
    user_id: int,
    *,
    now: datetime,
    session: AsyncSession | None = None,
) -> int:
    stmt = pg_insert(UserWarning.__table__).values(
        chat_id=chat_id,
        user_id=user_id,
        count=1,
        last_warned_at=now,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["chat_id", "user_id"],
        set_={
            "count": UserWarning.__table__.c.count + 1,
            "last_warned_at": now,
        },
    ).returning(UserWarning.count)
    if session is None:
        async with _get_session() as session:
            try:
                result = await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        result = await session.execute(stmt)
    return int(result.scalar_one())


async def get_warning_count(
    chat_id: int, user_id: int, session: AsyncSession | None = None
) -> int:
    if session is None:
        async with _get_session() as session:
            return await get_warning_count(chat_id, user_id, session=session)
    result = await session.execute(
        select(UserWarning.count).where(
            UserWarning.chat_id == chat_id, UserWarning.user_id == user_id
        )
    )
    value = result.scalar_one_or_none()
    return int(value or 0)


async def get_warning_info(
    chat_id: int, user_id: int, session: AsyncSession | None = None
) -> dict[str, Any] | None:
    if session is None:
        async with _get_session() as session:
            return await get_warning_info(chat_id, user_id, session=session)
    result = await session.execute(
        select(UserWarning).where(
            UserWarning.chat_id == chat_id, UserWarning.user_id == user_id
        )
    )
    warning = result.scalar_one_or_none()
    if not warning:
        return None
    return {
        "count": warning.count,
        "last_warned_at": warning.last_warned_at,
    }


async def reset_user_warnings(
    chat_id: int, user_id: int, session: AsyncSession | None = None
) -> None:
    now = _utc_now()
    stmt = (
        update(UserWarning)
        .where(UserWarning.chat_id == chat_id, UserWarning.user_id == user_id)
        .values(count=0, last_warned_at=now)
    )
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(stmt)


async def set_user_penalty(
    chat_id: int,
    user_id: int,
    penalty: str,
    until: datetime | None,
    session: AsyncSession | None = None,
) -> None:
    now = _utc_now()
    stmt = pg_insert(UserPenalty.__table__).values(
        chat_id=chat_id,
        user_id=user_id,
        penalty=penalty,
        until=until,
        created_at=now,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["chat_id", "user_id", "penalty"],
        set_={"until": until, "created_at": now},
    )
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(stmt)


async def clear_user_penalty(
    chat_id: int, user_id: int, penalty: str, session: AsyncSession | None = None
) -> None:
    stmt = delete(UserPenalty).where(
        UserPenalty.chat_id == chat_id,
        UserPenalty.user_id == user_id,
        UserPenalty.penalty == penalty,
    )
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(stmt)


async def log_mod_action(
    *,
    chat_id: int,
    target_user_id: int,
    admin_user_id: int,
    action: str,
    reason: str | None = None,
    message_id: int | None = None,
    session: AsyncSession | None = None,
) -> None:
    now = _utc_now()
    stmt = pg_insert(ModAction.__table__).values(
        chat_id=chat_id,
        target_user_id=target_user_id,
        admin_user_id=admin_user_id,
        action=action,
        reason=reason,
        message_id=message_id,
        created_at=now,
    )
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(stmt)


async def schedule_unmute_notification(
    *,
    chat_id: int,
    user_id: int,
    unmute_at: datetime,
    reason: str | None = None,
    session: AsyncSession | None = None,
) -> None:
    now = _utc_now()
    stmt = pg_insert(ScheduledUnmute.__table__).values(
        chat_id=chat_id,
        user_id=user_id,
        unmute_at=unmute_at,
        reason=reason,
        created_at=now,
        sent_at=None,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["chat_id", "user_id"],
        set_={
            "unmute_at": unmute_at,
            "reason": reason,
            "created_at": now,
            "sent_at": None,
        },
    )
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(stmt)


async def list_due_scheduled_unmutes(
    *,
    limit: int = 100,
    session: AsyncSession | None = None,
) -> list[dict[str, Any]]:
    if session is None:
        async with _get_session() as session:
            return await list_due_scheduled_unmutes(limit=limit, session=session)
    now = _utc_now()
    result = await session.execute(
        select(ScheduledUnmute)
        .where(
            ScheduledUnmute.sent_at.is_(None),
            ScheduledUnmute.unmute_at <= now,
        )
        .order_by(ScheduledUnmute.unmute_at.asc())
        .limit(limit)
    )
    rows = []
    for row in result.scalars().all():
        rows.append(
            {
                "id": row.id,
                "chat_id": row.chat_id,
                "user_id": row.user_id,
                "unmute_at": row.unmute_at,
                "reason": row.reason,
            }
        )
    return rows


async def mark_scheduled_unmute_sent(
    unmute_id: int,
    *,
    sent_at: datetime | None = None,
    session: AsyncSession | None = None,
) -> None:
    when = sent_at or _utc_now()
    stmt = (
        update(ScheduledUnmute)
        .where(ScheduledUnmute.id == unmute_id)
        .values(sent_at=when)
    )
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(stmt)


async def list_mod_actions(
    chat_id: int, limit: int = 20, session: AsyncSession | None = None
) -> list[dict[str, Any]]:
    if session is None:
        async with _get_session() as session:
            return await list_mod_actions(chat_id, limit=limit, session=session)
    result = await session.execute(
        select(ModAction)
        .where(ModAction.chat_id == chat_id)
        .order_by(ModAction.created_at.desc())
        .limit(limit)
    )
    actions = []
    for action in result.scalars().all():
        actions.append(
            {
                "id": action.id,
                "chat_id": action.chat_id,
                "target_user_id": action.target_user_id,
                "admin_user_id": action.admin_user_id,
                "action": action.action,
                "reason": action.reason,
                "message_id": action.message_id,
                "created_at": action.created_at,
            }
        )
    return actions


async def list_mod_actions_for_user(
    chat_id: int,
    user_id: int,
    actions: list[str],
    limit: int = 5,
    session: AsyncSession | None = None,
) -> list[dict[str, Any]]:
    if session is None:
        async with _get_session() as session:
            return await list_mod_actions_for_user(
                chat_id, user_id, actions, limit=limit, session=session
            )
    result = await session.execute(
        select(ModAction)
        .where(
            ModAction.chat_id == chat_id,
            ModAction.target_user_id == user_id,
            ModAction.action.in_(actions),
        )
        .order_by(ModAction.created_at.desc())
        .limit(limit)
    )
    rows = []
    for action in result.scalars().all():
        rows.append(
            {
                "id": action.id,
                "action": action.action,
                "reason": action.reason,
                "created_at": action.created_at,
                "admin_user_id": action.admin_user_id,
            }
        )
    return rows


async def get_first_seen_time(
    chat_id: int, user_id: int, session: AsyncSession | None = None
) -> datetime | None:
    if session is None:
        async with _get_session() as session:
            return await get_first_seen_time(chat_id, user_id, session=session)
    result = await session.execute(
        select(func.min(CaptchaChallenge.created_at)).where(
            CaptchaChallenge.chat_id == chat_id,
            CaptchaChallenge.user_id == user_id,
        )
    )
    return result.scalar_one()


async def list_invite_candidates(
    *,
    max_attempts: int,
    limit: int,
    session: AsyncSession | None = None,
) -> list[dict[str, Any]]:
    if session is None:
        async with _get_session() as session:
            return await list_invite_candidates(
                max_attempts=max_attempts, limit=limit, session=session
            )
    result = await session.execute(
        select(ClanApplication)
        .where(
            ClanApplication.status == "pending",
            ClanApplication.player_tag.is_not(None),
            ClanApplication.notify_attempts < max_attempts,
        )
        .order_by(ClanApplication.created_at.asc())
        .limit(limit)
    )
    return [_application_to_dict(app) for app in result.scalars().all()]


async def list_invited_applications(
    session: AsyncSession | None = None,
) -> list[dict[str, Any]]:
    if session is None:
        async with _get_session() as session:
            return await list_invited_applications(session=session)
    result = await session.execute(
        select(ClanApplication)
        .where(ClanApplication.status == "invited")
        .order_by(ClanApplication.updated_at.desc())
    )
    return [_application_to_dict(app) for app in result.scalars().all()]


async def mark_application_invited(
    app_id: int,
    *,
    now: datetime,
    invite_expires_at: datetime,
    session: AsyncSession | None = None,
) -> None:
    stmt = (
        update(ClanApplication)
        .where(ClanApplication.id == app_id)
        .values(
            status="invited",
            last_notified_at=now,
            notify_attempts=ClanApplication.notify_attempts + 1,
            invite_expires_at=invite_expires_at,
            updated_at=now,
        )
    )
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(stmt)


async def mark_application_joined(
    app_id: int, *, now: datetime, session: AsyncSession | None = None
) -> None:
    stmt = (
        update(ClanApplication)
        .where(ClanApplication.id == app_id)
        .values(status="joined", updated_at=now)
    )
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(stmt)


async def reset_expired_invite(
    app_id: int,
    *,
    now: datetime,
    exhausted: bool,
    session: AsyncSession | None = None,
) -> None:
    status = "expired" if exhausted else "pending"
    stmt = (
        update(ClanApplication)
        .where(ClanApplication.id == app_id)
        .values(status=status, invite_expires_at=None, updated_at=now)
    )
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(stmt)

async def get_user_link(
    telegram_user_id: int, session: AsyncSession | None = None
) -> dict[str, Any] | None:
    if session is None:
        async with _get_session() as session:
            return await get_user_link(telegram_user_id, session=session)
    result = await session.execute(
        select(UserLink).where(UserLink.telegram_user_id == telegram_user_id)
    )
    link = result.scalar_one_or_none()
    if not link:
        return None
    return {
        "telegram_user_id": link.telegram_user_id,
        "player_tag": link.player_tag,
        "player_name": link.player_name,
        "linked_at": link.linked_at,
        "source": link.source,
    }


async def upsert_user_link(
    telegram_user_id: int,
    player_tag: str,
    player_name: str,
    source: str,
    session: AsyncSession | None = None,
) -> None:
    now = _utc_now()
    stmt = pg_insert(UserLink.__table__).values(
        telegram_user_id=telegram_user_id,
        player_tag=player_tag,
        player_name=player_name,
        linked_at=now,
        source=source,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["telegram_user_id"],
        set_={
            "player_tag": stmt.excluded.player_tag,
            "player_name": stmt.excluded.player_name,
            "linked_at": now,
            "source": stmt.excluded.source,
        },
    )
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(stmt)


async def get_user_link_request(
    telegram_user_id: int, session: AsyncSession | None = None
) -> dict[str, Any] | None:
    if session is None:
        async with _get_session() as session:
            return await get_user_link_request(telegram_user_id, session=session)
    result = await session.execute(
        select(UserLinkRequest).where(
            UserLinkRequest.telegram_user_id == telegram_user_id
        )
    )
    request = result.scalar_one_or_none()
    if not request:
        return None
    return {
        "telegram_user_id": request.telegram_user_id,
        "status": request.status,
        "origin_chat_id": request.origin_chat_id,
        "created_at": request.created_at,
    }


async def upsert_user_link_request(
    telegram_user_id: int,
    status: str,
    origin_chat_id: int | None,
    session: AsyncSession | None = None,
) -> None:
    now = _utc_now()
    stmt = pg_insert(UserLinkRequest.__table__).values(
        telegram_user_id=telegram_user_id,
        status=status,
        origin_chat_id=origin_chat_id,
        created_at=now,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["telegram_user_id"],
        set_={
            "status": stmt.excluded.status,
            "origin_chat_id": stmt.excluded.origin_chat_id,
            "created_at": now,
        },
    )
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(stmt)


async def delete_user_link_request(
    telegram_user_id: int, session: AsyncSession | None = None
) -> None:
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(
                    delete(UserLinkRequest).where(
                        UserLinkRequest.telegram_user_id == telegram_user_id
                    )
                )
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(
            delete(UserLinkRequest).where(
                UserLinkRequest.telegram_user_id == telegram_user_id
            )
        )


async def delete_user_link(
    telegram_user_id: int, session: AsyncSession | None = None
) -> None:
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(
                    delete(UserLink).where(
                        UserLink.telegram_user_id == telegram_user_id
                    )
                )
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(
            delete(UserLink).where(UserLink.telegram_user_id == telegram_user_id)
        )


def _question_to_dict(question: CaptchaQuestion) -> dict[str, Any]:
    return {
        "id": question.id,
        "question_text": question.question_text,
        "option_a": question.option_a,
        "option_b": question.option_b,
        "option_c": question.option_c,
        "option_d": question.option_d,
        "correct_option": question.correct_option,
        "is_active": question.is_active,
        "created_at": question.created_at,
    }


def _challenge_to_dict(challenge: CaptchaChallenge) -> dict[str, Any]:
    return {
        "id": challenge.id,
        "chat_id": challenge.chat_id,
        "user_id": challenge.user_id,
        "question_id": challenge.question_id,
        "message_id": challenge.message_id,
        "attempts": challenge.attempts,
        "status": challenge.status,
        "created_at": challenge.created_at,
        "updated_at": challenge.updated_at,
        "expires_at": challenge.expires_at,
        "last_reminded_at": challenge.last_reminded_at,
    }


async def is_user_verified(
    chat_id: int, user_id: int, session: AsyncSession | None = None
) -> bool:
    if session is None:
        async with _get_session() as session:
            return await is_user_verified(chat_id, user_id, session=session)
    result = await session.execute(
        select(VerifiedUser).where(
            VerifiedUser.chat_id == chat_id, VerifiedUser.user_id == user_id
        )
    )
    return result.scalar_one_or_none() is not None


async def set_user_verified(
    chat_id: int, user_id: int, session: AsyncSession | None = None
) -> None:
    now = _utc_now()
    stmt = pg_insert(VerifiedUser.__table__).values(
        chat_id=chat_id, user_id=user_id, verified_at=now
    )
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["chat_id", "user_id"]
    )
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(stmt)


async def get_captcha_question(
    question_id: int, session: AsyncSession | None = None
) -> dict[str, Any] | None:
    if session is None:
        async with _get_session() as session:
            return await get_captcha_question(question_id, session=session)
    result = await session.execute(
        select(CaptchaQuestion).where(CaptchaQuestion.id == question_id)
    )
    question = result.scalar_one_or_none()
    if not question:
        return None
    return _question_to_dict(question)


async def get_random_captcha_question(
    session: AsyncSession | None = None,
) -> dict[str, Any] | None:
    if session is None:
        async with _get_session() as session:
            return await get_random_captcha_question(session=session)
    result = await session.execute(
        select(CaptchaQuestion)
        .where(CaptchaQuestion.is_active.is_(True))
        .order_by(func.random())
        .limit(1)
    )
    question = result.scalar_one_or_none()
    if not question:
        return None
    return _question_to_dict(question)


async def get_pending_challenge(
    chat_id: int, user_id: int, session: AsyncSession | None = None
) -> dict[str, Any] | None:
    if session is None:
        async with _get_session() as session:
            return await get_pending_challenge(chat_id, user_id, session=session)
    now = _utc_now()
    result = await session.execute(
        select(CaptchaChallenge)
        .where(
            CaptchaChallenge.chat_id == chat_id,
            CaptchaChallenge.user_id == user_id,
            CaptchaChallenge.status == "pending",
        )
        .order_by(CaptchaChallenge.created_at.desc())
        .limit(1)
    )
    challenge = result.scalar_one_or_none()
    if not challenge:
        return None
    if challenge.expires_at and challenge.expires_at < now:
        await mark_challenge_expired(challenge.id, session=session)
        return None
    return _challenge_to_dict(challenge)


async def get_or_create_pending_challenge(
    chat_id: int,
    user_id: int,
    expire_minutes: int,
    session: AsyncSession | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if session is None:
        async with _get_session() as session:
            try:
                result = await get_or_create_pending_challenge(
                    chat_id, user_id, expire_minutes, session=session
                )
                await session.commit()
                return result
            except Exception:
                await session.rollback()
                raise
    now = _utc_now()
    pending = await get_pending_challenge(chat_id, user_id, session=session)
    if pending:
        question = await get_captcha_question(pending["question_id"], session=session)
        return pending, question

    result = await session.execute(
        select(CaptchaChallenge)
        .where(
            CaptchaChallenge.chat_id == chat_id,
            CaptchaChallenge.user_id == user_id,
            CaptchaChallenge.status == "failed",
        )
        .order_by(CaptchaChallenge.updated_at.desc())
        .limit(1)
    )
    failed = result.scalar_one_or_none()
    if failed and failed.expires_at and failed.expires_at > now:
        return _challenge_to_dict(failed), None

    question = await get_random_captcha_question(session=session)
    if not question:
        return None, None
    expires_at = now + timedelta(minutes=max(expire_minutes, 1))
    stmt = pg_insert(CaptchaChallenge.__table__).values(
        chat_id=chat_id,
        user_id=user_id,
        question_id=question["id"],
        attempts=0,
        status="pending",
        created_at=now,
        updated_at=now,
        expires_at=expires_at,
    )
    result = await session.execute(stmt.returning(CaptchaChallenge.id))
    challenge_id = result.scalar_one()
    challenge = await session.get(CaptchaChallenge, challenge_id)
    if not challenge:
        return None, None
    return _challenge_to_dict(challenge), question


async def create_fresh_captcha_challenge(
    chat_id: int,
    user_id: int,
    expire_minutes: int,
    session: AsyncSession | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if session is None:
        async with _get_session() as session:
            try:
                result = await create_fresh_captcha_challenge(
                    chat_id, user_id, expire_minutes, session=session
                )
                await session.commit()
                return result
            except Exception:
                await session.rollback()
                raise
    now = _utc_now()
    question = await get_random_captcha_question(session=session)
    if not question:
        return None, None
    expires_at = now + timedelta(minutes=max(expire_minutes, 1))
    stmt = pg_insert(CaptchaChallenge.__table__).values(
        chat_id=chat_id,
        user_id=user_id,
        question_id=question["id"],
        attempts=0,
        status="pending",
        created_at=now,
        updated_at=now,
        expires_at=expires_at,
    )
    result = await session.execute(stmt.returning(CaptchaChallenge.id))
    challenge_id = result.scalar_one()
    challenge = await session.get(CaptchaChallenge, challenge_id)
    if not challenge:
        return None, None
    return _challenge_to_dict(challenge), question


async def get_challenge_by_id(
    challenge_id: int, session: AsyncSession | None = None
) -> dict[str, Any] | None:
    if session is None:
        async with _get_session() as session:
            return await get_challenge_by_id(challenge_id, session=session)
    result = await session.execute(
        select(CaptchaChallenge).where(CaptchaChallenge.id == challenge_id)
    )
    challenge = result.scalar_one_or_none()
    if not challenge:
        return None
    return _challenge_to_dict(challenge)


async def get_latest_challenge(
    chat_id: int, user_id: int, session: AsyncSession | None = None
) -> dict[str, Any] | None:
    if session is None:
        async with _get_session() as session:
            return await get_latest_challenge(chat_id, user_id, session=session)
    result = await session.execute(
        select(CaptchaChallenge)
        .where(
            CaptchaChallenge.chat_id == chat_id,
            CaptchaChallenge.user_id == user_id,
        )
        .order_by(CaptchaChallenge.created_at.desc())
        .limit(1)
    )
    challenge = result.scalar_one_or_none()
    if not challenge:
        return None
    return _challenge_to_dict(challenge)


async def update_challenge_message_id(
    challenge_id: int, message_id: int, session: AsyncSession | None = None
) -> None:
    now = _utc_now()
    stmt = (
        update(CaptchaChallenge)
        .where(CaptchaChallenge.id == challenge_id)
        .values(message_id=message_id, updated_at=now)
    )
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(stmt)


async def expire_active_challenges(
    chat_id: int, user_id: int, session: AsyncSession | None = None
) -> int:
    now = _utc_now()
    stmt = (
        update(CaptchaChallenge)
        .where(
            CaptchaChallenge.chat_id == chat_id,
            CaptchaChallenge.user_id == user_id,
            CaptchaChallenge.status.in_(["pending", "failed"]),
        )
        .values(status="expired", updated_at=now, expires_at=now)
    )
    if session is None:
        async with _get_session() as session:
            try:
                result = await session.execute(stmt)
                await session.commit()
                return int(result.rowcount or 0)
            except Exception:
                await session.rollback()
                raise
    result = await session.execute(stmt)
    return int(result.rowcount or 0)


async def mark_pending_challenges_passed(
    chat_id: int, user_id: int, session: AsyncSession | None = None
) -> int:
    now = _utc_now()
    stmt = (
        update(CaptchaChallenge)
        .where(
            CaptchaChallenge.chat_id == chat_id,
            CaptchaChallenge.user_id == user_id,
            CaptchaChallenge.status == "pending",
        )
        .values(status="passed", updated_at=now, expires_at=None)
    )
    if session is None:
        async with _get_session() as session:
            try:
                result = await session.execute(stmt)
                await session.commit()
                return int(result.rowcount or 0)
            except Exception:
                await session.rollback()
                raise
    result = await session.execute(stmt)
    return int(result.rowcount or 0)


async def increment_challenge_attempts(
    challenge_id: int, session: AsyncSession | None = None
) -> int:
    now = _utc_now()
    stmt = (
        update(CaptchaChallenge)
        .where(CaptchaChallenge.id == challenge_id)
        .values(attempts=CaptchaChallenge.attempts + 1, updated_at=now)
        .returning(CaptchaChallenge.attempts)
    )
    if session is None:
        async with _get_session() as session:
            try:
                result = await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        result = await session.execute(stmt)
    return int(result.scalar_one())


async def mark_challenge_passed(
    challenge_id: int, session: AsyncSession | None = None
) -> None:
    now = _utc_now()
    stmt = (
        update(CaptchaChallenge)
        .where(CaptchaChallenge.id == challenge_id)
        .values(status="passed", updated_at=now, expires_at=None)
    )
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(stmt)


async def delete_verified_user(
    chat_id: int, user_id: int, session: AsyncSession | None = None
) -> int:
    stmt = delete(VerifiedUser).where(
        VerifiedUser.chat_id == chat_id, VerifiedUser.user_id == user_id
    )
    if session is None:
        async with _get_session() as session:
            try:
                result = await session.execute(stmt)
                await session.commit()
                return int(result.rowcount or 0)
            except Exception:
                await session.rollback()
                raise
    result = await session.execute(stmt)
    return int(result.rowcount or 0)


async def mark_challenge_failed(
    challenge_id: int,
    expires_at: datetime,
    session: AsyncSession | None = None,
) -> None:
    now = _utc_now()
    stmt = (
        update(CaptchaChallenge)
        .where(CaptchaChallenge.id == challenge_id)
        .values(status="failed", updated_at=now, expires_at=expires_at)
    )
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(stmt)


async def mark_challenge_expired(
    challenge_id: int, session: AsyncSession | None = None
) -> None:
    now = _utc_now()
    stmt = (
        update(CaptchaChallenge)
        .where(CaptchaChallenge.id == challenge_id)
        .values(status="expired", updated_at=now)
    )
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(stmt)


async def touch_last_reminded_at(
    challenge_id: int,
    now: datetime,
    session: AsyncSession | None = None,
) -> None:
    stmt = (
        update(CaptchaChallenge)
        .where(CaptchaChallenge.id == challenge_id)
        .values(last_reminded_at=now, updated_at=now)
    )
    if session is None:
        async with _get_session() as session:
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        await session.execute(stmt)


async def search_player_candidates(
    clan_tag: str,
    nickname: str,
    session: AsyncSession | None = None,
) -> list[dict[str, Any]]:
    if session is None:
        async with _get_session() as session:
            return await search_player_candidates(clan_tag, nickname, session=session)
    nickname = nickname.strip()
    if not nickname:
        return []
    current_members = await get_current_member_tags(clan_tag, session=session)
    latest_date = await get_latest_membership_date(clan_tag, session=session)

    def _format_rows(rows: list[Any], in_clan: bool) -> list[dict[str, Any]]:
        return [
            {
                "player_tag": row.player_tag,
                "player_name": row.player_name,
                "in_clan": in_clan,
            }
            for row in rows
        ]

    if latest_date is not None:
        exact_query = select(
            ClanMemberDaily.player_tag, ClanMemberDaily.player_name
        ).where(
            ClanMemberDaily.clan_tag == clan_tag,
            ClanMemberDaily.snapshot_date == latest_date,
            func.lower(ClanMemberDaily.player_name) == nickname.lower(),
        )
        exact_rows = (await session.execute(exact_query)).all()
        if exact_rows:
            return _format_rows(exact_rows, True)

        contains_query = select(
            ClanMemberDaily.player_tag, ClanMemberDaily.player_name
        ).where(
            ClanMemberDaily.clan_tag == clan_tag,
            ClanMemberDaily.snapshot_date == latest_date,
            ClanMemberDaily.player_name.ilike(f"%{nickname}%"),
        )
        contains_rows = (await session.execute(contains_query)).all()
        if contains_rows:
            return _format_rows(contains_rows, True)

    exact_hist_query = (
        select(
            PlayerParticipation.player_tag,
            func.max(PlayerParticipation.player_name).label("player_name"),
        )
        .where(func.lower(PlayerParticipation.player_name) == nickname.lower())
        .group_by(PlayerParticipation.player_tag)
    )
    exact_hist = (await session.execute(exact_hist_query)).all()
    if exact_hist:
        return [
            {
                "player_tag": row.player_tag,
                "player_name": row.player_name,
                "in_clan": row.player_tag in current_members,
            }
            for row in exact_hist
        ]

    contains_hist_query = (
        select(
            PlayerParticipation.player_tag,
            func.max(PlayerParticipation.player_name).label("player_name"),
        )
        .where(PlayerParticipation.player_name.ilike(f"%{nickname}%"))
        .group_by(PlayerParticipation.player_tag)
    )
    contains_hist = (await session.execute(contains_hist_query)).all()
    return [
        {
            "player_tag": row.player_tag,
            "player_name": row.player_name,
            "in_clan": row.player_tag in current_members,
        }
        for row in contains_hist
    ]


def _coerce_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_colosseum_map(value: Any) -> dict[int, int]:
    mapping: dict[int, int] = {}
    if not isinstance(value, dict):
        return mapping
    for key, raw_val in value.items():
        season_id = _coerce_int(key)
        section_index = _coerce_int(raw_val)
        if season_id is None or section_index is None:
            continue
        if season_id < 0 or section_index < 0:
            continue
        mapping[season_id] = section_index
    return mapping


async def get_colosseum_index_map(
    session: AsyncSession | None = None,
) -> dict[int, int]:
    state = await get_app_state(APP_STATE_COLOSSEUM_KEY, session=session)
    return _parse_colosseum_map(state)


async def get_colosseum_index_for_season(
    season_id: int, session: AsyncSession | None = None
) -> int | None:
    mapping = await get_colosseum_index_map(session=session)
    return mapping.get(season_id)


async def _apply_colosseum_corrections(
    session: AsyncSession, now: datetime, season_id: int, section_index: int
) -> None:
    logger.info(
        "Running colosseum correction for season=%s section=%s",
        season_id,
        section_index,
    )
    is_colosseum_expr = case(
        (PlayerParticipation.section_index == section_index, True),
        else_=False,
    )
    await session.execute(
        update(PlayerParticipation)
        .where(PlayerParticipation.season_id == season_id)
        .values(is_colosseum=is_colosseum_expr, updated_at=now)
    )
    is_colosseum_daily_expr = case(
        (PlayerParticipationDaily.section_index == section_index, True),
        else_=False,
    )
    await session.execute(
        update(PlayerParticipationDaily)
        .where(PlayerParticipationDaily.season_id == season_id)
        .values(is_colosseum=is_colosseum_daily_expr, updated_at=now)
    )
    is_colosseum_state_expr = case(
        (RiverRaceState.section_index == section_index, True),
        else_=False,
    )
    await session.execute(
        update(RiverRaceState)
        .where(RiverRaceState.season_id == season_id)
        .values(is_colosseum=is_colosseum_state_expr, updated_at=now)
    )


async def _set_colosseum_index_for_season(
    session: AsyncSession, now: datetime, season_id: int, section_index: int
) -> bool:
    current_state = await get_app_state(APP_STATE_COLOSSEUM_KEY, session=session)
    mapping = _parse_colosseum_map(current_state)
    if mapping.get(season_id) == section_index:
        return False
    mapping[season_id] = section_index
    logger.info(
        "Set colosseum mapping for season=%s section=%s",
        season_id,
        section_index,
    )
    await _upsert_app_state(
        session,
        now,
        APP_STATE_COLOSSEUM_KEY,
        {str(key): value for key, value in mapping.items()},
    )
    await _apply_colosseum_corrections(session, now, season_id, section_index)
    return True


async def set_colosseum_index_for_season(
    season_id: int, section_index: int, session: AsyncSession | None = None
) -> bool:
    now = _utc_now()
    if session is None:
        async with _get_session() as session:
            try:
                updated = await _set_colosseum_index_for_season(
                    session, now, season_id, section_index
                )
                await session.commit()
                return updated
            except Exception:
                await session.rollback()
                raise
    return await _set_colosseum_index_for_season(session, now, season_id, section_index)


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


async def get_river_race_state_for_week(
    clan_tag: str, season_id: int, section_index: int
) -> dict[str, Any] | None:
    async with _get_session() as session:
        result = await session.execute(
            select(RiverRaceState).where(
                RiverRaceState.clan_tag == clan_tag,
                RiverRaceState.season_id == season_id,
                RiverRaceState.section_index == section_index,
            )
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


async def get_last_weeks_from_db(
    clan_tag: str, limit: int = 8
) -> list[tuple[int, int]]:
    async with _get_session() as session:
        result = await session.execute(
            select(RiverRaceState.season_id, RiverRaceState.section_index)
            .where(
                RiverRaceState.clan_tag == clan_tag,
                RiverRaceState.period_type != "training",
            )
            .order_by(
                RiverRaceState.season_id.desc(),
                RiverRaceState.section_index.desc(),
            )
            .limit(limit)
        )
        return [(int(row.season_id), int(row.section_index)) for row in result.all()]
