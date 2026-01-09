"""Add river_race_place_snapshots table.

Revision ID: 0014_add_river_race_place_snapshots
Revises: 0013_add_verified_users_language
Create Date: 2026-01-09 00:00:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "0014_add_river_race_place_snapshots"
down_revision = "0013_add_verified_users_language"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "river_race_place_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("clan_tag", sa.String(length=32), nullable=False),
        sa.Column("season_id", sa.Integer(), nullable=False),
        sa.Column("section_index", sa.Integer(), nullable=False),
        sa.Column(
            "snapshot_ts",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("our_rank", sa.Integer(), nullable=False),
        sa.Column("our_fame", sa.Integer(), nullable=False),
        sa.Column("above_rank", sa.Integer(), nullable=True),
        sa.Column("above_fame", sa.Integer(), nullable=True),
        sa.Column("gap_to_above", sa.Integer(), nullable=True),
        sa.Column("top5_json", postgresql.JSONB(), nullable=True),
    )
    op.create_index(
        "ix_river_race_place_snapshots_clan_season_section_ts",
        "river_race_place_snapshots",
        [
            "clan_tag",
            "season_id",
            "section_index",
            sa.text("snapshot_ts DESC"),
        ],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_river_race_place_snapshots_clan_season_section_ts",
        table_name="river_race_place_snapshots",
    )
    op.drop_table("river_race_place_snapshots")
