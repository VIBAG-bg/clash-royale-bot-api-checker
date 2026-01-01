"""Add user link tables.

Revision ID: 0005_add_user_links
Revises: 0004_add_clan_chats
Create Date: 2026-01-01 00:00:00
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0005_add_user_links"
down_revision = "0004_add_clan_chats"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "user_links",
        sa.Column("telegram_user_id", sa.BigInteger(), primary_key=True),
        sa.Column("player_tag", sa.String(length=32), nullable=False),
        sa.Column("player_name", sa.String(length=128), nullable=False),
        sa.Column(
            "linked_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("source", sa.String(length=16), nullable=False),
    )
    op.create_table(
        "user_link_requests",
        sa.Column("telegram_user_id", sa.BigInteger(), primary_key=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("origin_chat_id", sa.BigInteger(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )


def downgrade() -> None:
    op.drop_table("user_link_requests")
    op.drop_table("user_links")
