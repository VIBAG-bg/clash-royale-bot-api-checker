"""Minimal realistic seed builders for DB tests."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Iterable

from sqlalchemy.ext.asyncio import AsyncSession

from db import (
    CaptchaChallenge,
    CaptchaQuestion,
    ClanApplication,
    ClanMemberDaily,
    ClanMemberDonationsWeekly,
    PlayerParticipation,
    PlayerParticipationDaily,
    RiverRaceState,
    UserPenalty,
    UserWarning,
    VerifiedUser,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


async def seed_war_week(
    session: AsyncSession,
    *,
    clan_tag: str,
    season_id: int,
    section_index: int,
    period_type: str = "completed",
    is_colosseum: bool = False,
    clan_score: int = 0,
    participants: Iterable[dict[str, object]] = (),
    snapshot_date: date | None = None,
) -> None:
    now = _utc_now()
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
    snap = snapshot_date or now.date()
    for item in participants:
        tag = str(item.get("player_tag", ""))
        name = str(item.get("player_name", "Unknown"))
        fame = int(item.get("fame", 0) or 0)
        decks = int(item.get("decks_used", 0) or 0)
        session.add(
            PlayerParticipation(
                player_tag=tag,
                player_name=name,
                season_id=season_id,
                section_index=section_index,
                is_colosseum=is_colosseum,
                fame=fame,
                repair_points=int(item.get("repair_points", 0) or 0),
                boat_attacks=int(item.get("boat_attacks", 0) or 0),
                decks_used=decks,
                decks_used_today=int(item.get("decks_used_today", decks) or 0),
                created_at=now,
                updated_at=now,
            )
        )
        session.add(
            PlayerParticipationDaily(
                player_tag=tag,
                player_name=name,
                season_id=season_id,
                section_index=section_index,
                is_colosseum=is_colosseum,
                snapshot_date=snap,
                fame=fame,
                repair_points=int(item.get("repair_points", 0) or 0),
                boat_attacks=int(item.get("boat_attacks", 0) or 0),
                decks_used=decks,
                decks_used_today=int(item.get("decks_used_today", decks) or 0),
                created_at=now,
                updated_at=now,
            )
        )
    await session.flush()


async def seed_members(
    session: AsyncSession,
    *,
    clan_tag: str,
    snapshot_date: date,
    members: Iterable[dict[str, object]],
) -> None:
    now = _utc_now()
    for member in members:
        session.add(
            ClanMemberDaily(
                snapshot_date=snapshot_date,
                clan_tag=clan_tag,
                player_tag=str(member.get("player_tag", "")),
                player_name=str(member.get("player_name", "Unknown")),
                role=member.get("role"),
                trophies=member.get("trophies"),
                donations=member.get("donations"),
                donations_received=member.get("donations_received"),
                clan_rank=member.get("clan_rank"),
                previous_clan_rank=member.get("previous_clan_rank"),
                exp_level=member.get("exp_level"),
                last_seen=member.get("last_seen"),
                created_at=now,
                updated_at=now,
            )
        )
    await session.flush()


async def seed_donations(
    session: AsyncSession,
    *,
    clan_tag: str,
    week_start_date: date,
    rows: Iterable[dict[str, object]],
) -> None:
    now = _utc_now()
    for row in rows:
        session.add(
            ClanMemberDonationsWeekly(
                clan_tag=clan_tag,
                week_start_date=week_start_date,
                player_tag=str(row.get("player_tag", "")),
                player_name=row.get("player_name"),
                donations_week_total=int(row.get("donations_week_total", 0) or 0),
                donations_received_week_total=int(
                    row.get("donations_received_week_total", 0) or 0
                ),
                snapshots_count=int(row.get("snapshots_count", 1) or 1),
                updated_at=now,
            )
        )
    await session.flush()


async def seed_warnings_penalties(
    session: AsyncSession,
    *,
    chat_id: int,
    user_id: int,
    warning_count: int = 0,
    penalty: str | None = None,
    until: datetime | None = None,
) -> None:
    now = _utc_now()
    if warning_count >= 0:
        session.add(
            UserWarning(
                chat_id=chat_id,
                user_id=user_id,
                count=warning_count,
                last_warned_at=now,
            )
        )
    if penalty:
        session.add(
            UserPenalty(
                chat_id=chat_id,
                user_id=user_id,
                penalty=penalty,
                until=until,
                created_at=now,
            )
        )
    await session.flush()


async def seed_applications(
    session: AsyncSession,
    rows: Iterable[dict[str, object]],
) -> None:
    now = _utc_now()
    for row in rows:
        session.add(
            ClanApplication(
                telegram_user_id=int(row.get("telegram_user_id", 0)),
                telegram_username=row.get("telegram_username"),
                telegram_display_name=row.get("telegram_display_name"),
                player_name=str(row.get("player_name", "Unknown")),
                player_tag=row.get("player_tag"),
                status=str(row.get("status", "pending")),
                last_notified_at=row.get("last_notified_at"),
                notify_attempts=int(row.get("notify_attempts", 0) or 0),
                invite_expires_at=row.get("invite_expires_at"),
                created_at=row.get("created_at") or now,
                updated_at=row.get("updated_at") or now,
            )
        )
    await session.flush()


async def seed_captcha(
    session: AsyncSession,
    *,
    chat_id: int,
    user_id: int,
    question_text: str = "Q?",
    correct_option: int = 1,
    challenge_status: str = "pending",
    verified: bool = False,
) -> int:
    now = _utc_now()
    question = CaptchaQuestion(
        question_text=question_text,
        option_a="A",
        option_b="B",
        option_c="C",
        option_d="D",
        correct_option=correct_option,
        is_active=True,
        created_at=now,
    )
    session.add(question)
    await session.flush()

    challenge = CaptchaChallenge(
        chat_id=chat_id,
        user_id=user_id,
        question_id=question.id,
        message_id=None,
        attempts=0,
        status=challenge_status,
        created_at=now,
        updated_at=now,
        expires_at=None,
        last_reminded_at=None,
    )
    session.add(challenge)
    if verified:
        session.add(
            VerifiedUser(
                chat_id=chat_id,
                user_id=user_id,
                language="en",
                verified_at=now,
            )
        )
    await session.flush()
    return question.id

