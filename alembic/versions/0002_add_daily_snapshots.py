"""Add daily participation and clan member snapshots.

Revision ID: 0002_add_daily_snapshots
Revises: 0001_create_tables
Create Date: 2025-01-02 00:00:00
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0002_add_daily_snapshots"
down_revision = "0001_create_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "player_participation_daily",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("player_tag", sa.String(length=32), nullable=False),
        sa.Column("player_name", sa.String(length=128), nullable=False),
        sa.Column("season_id", sa.Integer(), nullable=False),
        sa.Column("section_index", sa.Integer(), nullable=False),
        sa.Column("is_colosseum", sa.Boolean(), nullable=False),
        sa.Column("snapshot_date", sa.Date(), nullable=False),
        sa.Column(
            "fame",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "repair_points",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "boat_attacks",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "decks_used",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "decks_used_today",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
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
        sa.UniqueConstraint(
            "player_tag",
            "season_id",
            "section_index",
            "is_colosseum",
            "snapshot_date",
            name="uq_player_participation_daily_player_season_section_date",
        ),
    )
    op.create_index(
        "ix_player_participation_daily_season_section_date",
        "player_participation_daily",
        ["season_id", "section_index", "snapshot_date"],
    )

    op.create_table(
        "clan_member_daily",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("snapshot_date", sa.Date(), nullable=False),
        sa.Column("clan_tag", sa.String(length=32), nullable=False),
        sa.Column("player_tag", sa.String(length=32), nullable=False),
        sa.Column("player_name", sa.String(length=128), nullable=False),
        sa.Column("role", sa.String(length=32)),
        sa.Column("trophies", sa.Integer()),
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
        sa.UniqueConstraint(
            "snapshot_date",
            "clan_tag",
            "player_tag",
            name="uq_clan_member_daily_date_clan_player",
        ),
    )


def downgrade() -> None:
    op.drop_table("clan_member_daily")
    op.drop_index(
        "ix_player_participation_daily_season_section_date",
        table_name="player_participation_daily",
    )
    op.drop_table("player_participation_daily")
