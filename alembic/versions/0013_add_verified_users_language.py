"""Add language to verified_users.

Revision ID: 0013_add_verified_users_language
Revises: 0012_add_scheduled_unmutes
Create Date: 2026-01-03 22:30:00
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0013_add_verified_users_language"
down_revision = "0012_add_scheduled_unmutes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "verified_users",
        sa.Column(
            "language",
            sa.String(length=2),
            nullable=False,
            server_default="ru",
        ),
    )


def downgrade() -> None:
    op.drop_column("verified_users", "language")
