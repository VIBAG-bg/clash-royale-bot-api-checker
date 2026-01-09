"""Add clan_rank_snapshots table.

Revision ID: 0015_clan_rank_snap
Revises: 0014_rr_place_snapshots
Create Date: 2026-01-10 00:00:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "0015_clan_rank_snap"
down_revision = "0014_rr_place_snapshots"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "clan_rank_snapshots",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("clan_tag", sa.Text(), nullable=False),
        sa.Column("location_id", sa.Integer(), nullable=False),
        sa.Column("location_name", sa.Text(), nullable=True),
        sa.Column(
            "snapshot_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("ladder_rank", sa.Integer(), nullable=True),
        sa.Column("ladder_previous_rank", sa.Integer(), nullable=True),
        sa.Column("ladder_clan_score", sa.Integer(), nullable=False),
        sa.Column("war_rank", sa.Integer(), nullable=True),
        sa.Column("war_previous_rank", sa.Integer(), nullable=True),
        sa.Column("war_clan_score", sa.Integer(), nullable=True),
        sa.Column("clan_war_trophies", sa.Integer(), nullable=False),
        sa.Column("members", sa.Integer(), nullable=False),
        sa.Column("neighbors_ladder_json", postgresql.JSONB(), nullable=True),
        sa.Column("neighbors_war_json", postgresql.JSONB(), nullable=True),
        sa.Column("ladder_points_to_overtake_above", sa.Integer(), nullable=True),
        sa.Column("war_points_to_overtake_above", sa.Integer(), nullable=True),
        sa.Column("raw_source", postgresql.JSONB(), nullable=True),
    )
    op.create_index(
        "ix_clan_rank_snapshots_clan_location_ts",
        "clan_rank_snapshots",
        ["clan_tag", "location_id", sa.text("snapshot_at DESC")],
    )
    op.create_index(
        "ix_clan_rank_snapshots_location_ts",
        "clan_rank_snapshots",
        ["location_id", sa.text("snapshot_at DESC")],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_clan_rank_snapshots_location_ts",
        table_name="clan_rank_snapshots",
    )
    op.drop_index(
        "ix_clan_rank_snapshots_clan_location_ts",
        table_name="clan_rank_snapshots",
    )
    op.drop_table("clan_rank_snapshots")
