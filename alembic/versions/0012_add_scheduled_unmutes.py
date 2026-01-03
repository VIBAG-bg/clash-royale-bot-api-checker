"""Add scheduled unmute notifications table.

Revision ID: 0012_add_scheduled_unmutes
Revises: 0011_add_moderation_and_invites
Create Date: 2026-01-03 02:10:00
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0012_add_scheduled_unmutes"
down_revision = "0011_add_moderation_and_invites"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "scheduled_unmutes",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("unmute_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("chat_id", "user_id", name="uq_scheduled_unmutes_chat_user"),
    )
    op.create_index(
        "ix_scheduled_unmutes_unmute_at",
        "scheduled_unmutes",
        ["unmute_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_scheduled_unmutes_unmute_at", table_name="scheduled_unmutes")
    op.drop_table("scheduled_unmutes")
