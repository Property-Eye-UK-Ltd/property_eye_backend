"""Add vendor_name to property_listings

Revision ID: f3a9c1b2d4e8
Revises: e9b2f87d326f
Create Date: 2026-05-02

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f3a9c1b2d4e8"
down_revision: Union[str, Sequence[str], None] = "e9b2f87d326f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add nullable vendor_name for seller-side name from extraction."""
    op.add_column(
        "property_listings",
        sa.Column("vendor_name", sa.String(), nullable=True),
    )


def downgrade() -> None:
    """Remove vendor_name column."""
    op.drop_column("property_listings", "vendor_name")
