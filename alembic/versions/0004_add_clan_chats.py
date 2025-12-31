"""Add clan_chats table.

Revision ID: 0004_add_clan_chats
Revises: 0003_add_app_state
Create Date: 2025-01-04 00:00:00
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0004_add_clan_chats"
down_revision = "0003_add_app_state"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "clan_chats",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("clan_tag", sa.String(length=32), nullable=False),
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("clan_tag", "chat_id", name="uq_clan_chats_clan_chat"),
    )


def downgrade() -> None:
    op.drop_table("clan_chats")
