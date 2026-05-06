"""merge alembic heads

Revision ID: c1f4b7e9a2d3
Revises: b32da22b2669, b6b6d7b2d9e1
Create Date: 2026-05-06
"""

from typing import Sequence, Union


# revision identifiers, used by Alembic.
revision: str = "c1f4b7e9a2d3"
down_revision: Union[str, Sequence[str], None] = ("b32da22b2669", "b6b6d7b2d9e1")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Merge the two schema heads into a single lineage."""
    pass


def downgrade() -> None:
    """Re-open the two independent heads if this merge is rolled back."""
    pass
