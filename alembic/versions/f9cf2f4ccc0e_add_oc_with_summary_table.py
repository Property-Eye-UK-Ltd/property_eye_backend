"""add oc_with_summary table

Revision ID: f9cf2f4ccc0e
Revises: c1f4b7e9a2d3
Create Date: 2026-05-09 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f9cf2f4ccc0e"
down_revision = "c1f4b7e9a2d3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "oc_with_summary",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("fraud_report_id", sa.String(), nullable=False),
        sa.Column("title_number", sa.String(), nullable=True),
        sa.Column("response_code", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("poll_id", sa.String(), nullable=True),
        sa.Column("expected_at", sa.DateTime(), nullable=True),
        sa.Column("raw_xml", sa.Text(), nullable=True),
        sa.Column("parsed_json", sa.JSON(), nullable=True),
        sa.Column("pdf_filename", sa.String(), nullable=True),
        sa.Column("pdf_base64", sa.Text(), nullable=True),
        sa.Column("fetched_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["fraud_report_id"], ["fraud_matches.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("fraud_report_id"),
    )
    op.create_index(
        op.f("ix_oc_with_summary_fraud_report_id"),
        "oc_with_summary",
        ["fraud_report_id"],
        unique=True,
    )
    op.create_index(
        op.f("ix_oc_with_summary_title_number"),
        "oc_with_summary",
        ["title_number"],
        unique=False,
    )
    op.create_index(
        op.f("ix_oc_with_summary_response_code"),
        "oc_with_summary",
        ["response_code"],
        unique=False,
    )
    op.create_index(
        op.f("ix_oc_with_summary_status"),
        "oc_with_summary",
        ["status"],
        unique=False,
    )
    op.create_index(
        op.f("ix_oc_with_summary_poll_id"),
        "oc_with_summary",
        ["poll_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_oc_with_summary_poll_id"), table_name="oc_with_summary")
    op.drop_index(op.f("ix_oc_with_summary_status"), table_name="oc_with_summary")
    op.drop_index(op.f("ix_oc_with_summary_response_code"), table_name="oc_with_summary")
    op.drop_index(op.f("ix_oc_with_summary_title_number"), table_name="oc_with_summary")
    op.drop_index(op.f("ix_oc_with_summary_fraud_report_id"), table_name="oc_with_summary")
    op.drop_table("oc_with_summary")
