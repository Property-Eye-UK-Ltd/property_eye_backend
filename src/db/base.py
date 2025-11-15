"""
SQLAlchemy base configuration for the Fraud Detection POC.

This module sets up the declarative base and async engine configuration
for database operations.
"""

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import declarative_base

from src.core.config import settings

# Create async engine
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    future=True,
)

# Declarative base for ORM models
Base = declarative_base()
