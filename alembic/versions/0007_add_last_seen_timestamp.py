"""Alter last_seen column to timestamp with timezone.

Revision ID: 0007_add_last_seen_timestamp
Revises: 0006_add_member_donations
Create Date: 2026-01-02 00:00:00
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0007_add_last_seen_timestamp"
down_revision = "0006_add_member_donations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "clan_member_daily",
        "last_seen",
        existing_type=sa.String(length=64),
        type_=sa.DateTime(timezone=True),
        nullable=True,
        postgresql_using="""
        CASE
            WHEN last_seen IS NULL OR last_seen = '' THEN NULL
            WHEN last_seen ~ '^\\d{8}T\\d{6}\\.\\d+Z$' THEN
                (to_timestamp(last_seen, 'YYYYMMDD"T"HH24MISS.MS"Z"')::timestamp AT TIME ZONE 'UTC')
            WHEN last_seen ~ '^\\d{8}T\\d{6}Z$' THEN
                (to_timestamp(last_seen, 'YYYYMMDD"T"HH24MISS"Z"')::timestamp AT TIME ZONE 'UTC')
            WHEN last_seen ~ '^\\d{4}-\\d{2}-\\d{2} ' THEN
                last_seen::timestamptz
            ELSE NULL
        END
        """,
    )


def downgrade() -> None:
    op.alter_column(
        "clan_member_daily",
        "last_seen",
        existing_type=sa.DateTime(timezone=True),
        type_=sa.String(length=64),
        nullable=True,
    )
