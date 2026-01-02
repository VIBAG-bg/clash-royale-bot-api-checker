"""Add clan applications table.

Revision ID: 0010_add_clan_applications
Revises: 0009_add_daily_reminder_posts
Create Date: 2026-01-03 00:15:00
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0010_add_clan_applications"
down_revision = "0009_add_daily_reminder_posts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "clan_applications",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("telegram_user_id", sa.BigInteger(), nullable=False),
        sa.Column("telegram_username", sa.Text(), nullable=True),
        sa.Column("telegram_display_name", sa.Text(), nullable=True),
        sa.Column("player_name", sa.Text(), nullable=False),
        sa.Column("player_tag", sa.Text(), nullable=True),
        sa.Column(
            "status",
            sa.String(length=16),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index(
        "ix_clan_applications_status_created",
        "clan_applications",
        ["status", "created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_clan_applications_status_created",
        table_name="clan_applications",
    )
    op.drop_table("clan_applications")
