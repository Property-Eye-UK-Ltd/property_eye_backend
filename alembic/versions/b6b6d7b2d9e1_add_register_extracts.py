"""Add register_extracts cache table

Revision ID: b6b6d7b2d9e1
Revises: f3a9c1b2d4e8
Create Date: 2026-05-06
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b6b6d7b2d9e1"
down_revision: Union[str, Sequence[str], None] = "f3a9c1b2d4e8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "register_extracts",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("fraud_match_id", sa.String(), nullable=False),
        sa.Column("title_number", sa.String(), nullable=True),
        sa.Column("raw_xml", sa.Text(), nullable=True),
        sa.Column("parsed_json", sa.JSON(), nullable=True),
        sa.Column("fetched_at", sa.DateTime(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["fraud_match_id"], ["fraud_matches.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("fraud_match_id"),
    )
    op.create_index(
        op.f("ix_register_extracts_fraud_match_id"),
        "register_extracts",
        ["fraud_match_id"],
        unique=True,
    )
    op.create_index(
        op.f("ix_register_extracts_title_number"),
        "register_extracts",
        ["title_number"],
        unique=False,
    )
    op.create_index(
        op.f("ix_register_extracts_status"),
        "register_extracts",
        ["status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_register_extracts_status"), table_name="register_extracts")
    op.drop_index(op.f("ix_register_extracts_title_number"), table_name="register_extracts")
    op.drop_index(op.f("ix_register_extracts_fraud_match_id"), table_name="register_extracts")
    op.drop_table("register_extracts")
