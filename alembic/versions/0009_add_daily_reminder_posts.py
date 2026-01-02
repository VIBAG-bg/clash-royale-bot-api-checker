"""Add daily reminder posts table.

Revision ID: 0009_add_daily_reminder_posts
Revises: 0008_add_captcha_tables
Create Date: 2026-01-03 00:00:00
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0009_add_daily_reminder_posts"
down_revision = "0008_add_captcha_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "daily_reminder_posts",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("reminder_date", sa.Date(), nullable=False),
        sa.Column("season_id", sa.Integer(), nullable=False),
        sa.Column("section_index", sa.Integer(), nullable=False),
        sa.Column("period", sa.String(length=32), nullable=False),
        sa.Column("day_number", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "chat_id",
            "reminder_date",
            "season_id",
            "section_index",
            "period",
            "day_number",
            name="uq_daily_reminder_posts_unique",
        ),
    )
    op.create_index(
        "ix_daily_reminder_posts_date_chat",
        "daily_reminder_posts",
        ["reminder_date", "chat_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_daily_reminder_posts_date_chat",
        table_name="daily_reminder_posts",
    )
    op.drop_table("daily_reminder_posts")
