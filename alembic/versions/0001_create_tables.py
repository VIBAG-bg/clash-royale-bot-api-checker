"""Create river race tables.

Revision ID: 0001_create_tables
Revises: 
Create Date: 2025-01-01 00:00:00
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0001_create_tables"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "river_race_state",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("clan_tag", sa.String(length=32), nullable=False),
        sa.Column("season_id", sa.Integer(), nullable=False),
        sa.Column("section_index", sa.Integer(), nullable=False),
        sa.Column("is_colosseum", sa.Boolean(), nullable=False),
        sa.Column("period_type", sa.String(length=32), nullable=False),
        sa.Column("clan_score", sa.Integer(), nullable=False),
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
            "clan_tag",
            "season_id",
            "section_index",
            name="uq_river_race_state_clan_season_section",
        ),
    )
    op.create_index(
        "ix_river_race_state_clan_season_section",
        "river_race_state",
        ["clan_tag", "season_id", "section_index"],
    )

    op.create_table(
        "player_participation",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("player_tag", sa.String(length=32), nullable=False),
        sa.Column("player_name", sa.String(length=128), nullable=False),
        sa.Column("season_id", sa.Integer(), nullable=False),
        sa.Column("section_index", sa.Integer(), nullable=False),
        sa.Column("is_colosseum", sa.Boolean(), nullable=False),
        sa.Column("fame", sa.Integer(), nullable=False),
        sa.Column("repair_points", sa.Integer(), nullable=False),
        sa.Column("boat_attacks", sa.Integer(), nullable=False),
        sa.Column("decks_used", sa.Integer(), nullable=False),
        sa.Column("decks_used_today", sa.Integer(), nullable=False),
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
            name="uq_player_participation_player_season_section",
        ),
    )
    op.create_index(
        "ix_player_participation_season_section_decks",
        "player_participation",
        ["season_id", "section_index", "decks_used"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_player_participation_season_section_decks",
        table_name="player_participation",
    )
    op.drop_table("player_participation")
    op.drop_index(
        "ix_river_race_state_clan_season_section",
        table_name="river_race_state",
    )
    op.drop_table("river_race_state")
