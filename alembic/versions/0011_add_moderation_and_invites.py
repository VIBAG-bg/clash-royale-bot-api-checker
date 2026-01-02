"""Add moderation tables and auto-invite fields.

Revision ID: 0011_add_moderation_and_invites
Revises: 0010_add_clan_applications
Create Date: 2026-01-03 01:00:00
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0011_add_moderation_and_invites"
down_revision = "0010_add_clan_applications"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "clan_applications",
        sa.Column("last_notified_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "clan_applications",
        sa.Column("notify_attempts", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "clan_applications",
        sa.Column("invite_expires_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "chat_settings",
        sa.Column("chat_id", sa.BigInteger(), primary_key=True),
        sa.Column("raid_mode", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("flood_window_seconds", sa.Integer(), nullable=False, server_default="10"),
        sa.Column("flood_max_messages", sa.Integer(), nullable=False, server_default="6"),
        sa.Column("flood_mute_minutes", sa.Integer(), nullable=False, server_default="10"),
        sa.Column("new_user_link_block_hours", sa.Integer(), nullable=False, server_default="72"),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    op.create_table(
        "mod_actions",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("target_user_id", sa.BigInteger(), nullable=False),
        sa.Column("admin_user_id", sa.BigInteger(), nullable=False),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("message_id", sa.BigInteger(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index(
        "ix_mod_actions_chat_created",
        "mod_actions",
        ["chat_id", "created_at"],
    )

    op.create_table(
        "user_warnings",
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_warned_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("chat_id", "user_id", name="uq_user_warnings_chat_user"),
    )

    op.create_table(
        "user_penalties",
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("penalty", sa.String(length=16), nullable=False),
        sa.Column("until", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "chat_id",
            "user_id",
            "penalty",
            name="uq_user_penalties_chat_user_penalty",
        ),
    )

    op.create_table(
        "rate_counters",
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False, server_default="0"),
        sa.UniqueConstraint(
            "chat_id",
            "user_id",
            name="uq_rate_counters_chat_user",
        ),
    )


def downgrade() -> None:
    op.drop_table("rate_counters")
    op.drop_table("user_penalties")
    op.drop_table("user_warnings")
    op.drop_index("ix_mod_actions_chat_created", table_name="mod_actions")
    op.drop_table("mod_actions")
    op.drop_table("chat_settings")
    op.drop_column("clan_applications", "invite_expires_at")
    op.drop_column("clan_applications", "notify_attempts")
    op.drop_column("clan_applications", "last_notified_at")
