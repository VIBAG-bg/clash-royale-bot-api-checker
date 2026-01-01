"""Add clan member donations tracking.

Revision ID: 0006_add_member_donations
Revises: 0005_add_user_links
Create Date: 2026-01-02 00:00:00
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0006_add_member_donations"
down_revision = "0005_add_user_links"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "clan_member_daily",
        sa.Column("donations", sa.Integer(), nullable=True),
    )
    op.add_column(
        "clan_member_daily",
        sa.Column("donations_received", sa.Integer(), nullable=True),
    )
    op.add_column(
        "clan_member_daily",
        sa.Column("clan_rank", sa.Integer(), nullable=True),
    )
    op.add_column(
        "clan_member_daily",
        sa.Column("previous_clan_rank", sa.Integer(), nullable=True),
    )
    op.add_column(
        "clan_member_daily",
        sa.Column("exp_level", sa.Integer(), nullable=True),
    )
    op.add_column(
        "clan_member_daily",
        sa.Column("last_seen", sa.String(length=64), nullable=True),
    )

    op.create_table(
        "clan_member_donations_weekly",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("clan_tag", sa.String(length=32), nullable=False),
        sa.Column("week_start_date", sa.Date(), nullable=False),
        sa.Column("player_tag", sa.String(length=32), nullable=False),
        sa.Column("player_name", sa.String(length=128), nullable=True),
        sa.Column(
            "donations_week_total",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "donations_received_week_total",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "snapshots_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "clan_tag",
            "week_start_date",
            "player_tag",
            name="uq_clan_member_donations_weekly_clan_week_player",
        ),
    )
    op.create_index(
        "ix_clan_member_donations_weekly_clan_week",
        "clan_member_donations_weekly",
        ["clan_tag", "week_start_date"],
    )
    op.create_index(
        "ix_clan_member_donations_weekly_player_tag",
        "clan_member_donations_weekly",
        ["player_tag"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_clan_member_donations_weekly_player_tag",
        table_name="clan_member_donations_weekly",
    )
    op.drop_index(
        "ix_clan_member_donations_weekly_clan_week",
        table_name="clan_member_donations_weekly",
    )
    op.drop_table("clan_member_donations_weekly")

    op.drop_column("clan_member_daily", "last_seen")
    op.drop_column("clan_member_daily", "exp_level")
    op.drop_column("clan_member_daily", "previous_clan_rank")
    op.drop_column("clan_member_daily", "clan_rank")
    op.drop_column("clan_member_daily", "donations_received")
    op.drop_column("clan_member_daily", "donations")
