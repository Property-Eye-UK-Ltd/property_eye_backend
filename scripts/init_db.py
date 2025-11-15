"""
Database initialization script.

This script creates all database tables defined in the ORM models.
Run this script before starting the application for the first time.

Usage:
    python scripts/init_db.py
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.base import Base, engine
from src.models import Agency, FraudMatch, PropertyListing  # noqa: F401


async def init_db():
    """Create all database tables."""
    print("Creating database tables...")

    async with engine.begin() as conn:
        # Drop all tables (use with caution in production)
        # await conn.run_sync(Base.metadata.drop_all)

        # Create all tables
        await conn.run_sync(Base.metadata.create_all)

    print("Database tables created successfully!")


async def main():
    """Main entry point."""
    try:
        await init_db()
    except Exception as e:
        print(f"Error initializing database: {e}")
        sys.exit(1)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
