"""Shared pytest fixtures for the Property Eye backend tests."""

import asyncio
from typing import AsyncGenerator

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.db.base import Base
from src.db.session import get_db
from src.main import app

# Test database URL using a local SQLite file
TEST_DATABASE_URL = "sqlite+aiosqlite:///./test_property_eye.db"

# Create a dedicated async engine for tests
test_engine = create_async_engine(TEST_DATABASE_URL, future=True, echo=False)

# Create async session factory bound to the test engine
TestSessionLocal = async_sessionmaker(
    test_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


@pytest.fixture(scope="session")
def event_loop() -> AsyncGenerator[asyncio.AbstractEventLoop, None]:
    """Provide a shared event loop for async tests."""
    loop = asyncio.new_event_loop()
    try:
        yield loop
    finally:
        loop.close()


@pytest.fixture(scope="session", autouse=True)
async def prepare_test_database() -> AsyncGenerator[None, None]:
    """Create all tables in the test database before running tests."""
    # Import models to ensure they are registered on Base.metadata
    import src.models.agency  # noqa: F401
    import src.models.fraud_match  # noqa: F401
    import src.models.property_listing  # noqa: F401
    import src.models.register_extract  # noqa: F401

    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    yield

    await test_engine.dispose()


@pytest.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Provide an async SQLAlchemy session bound to the test database."""
    async with TestSessionLocal() as session:
        yield session


@pytest.fixture
async def api_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an AsyncClient wired to the FastAPI app with test DB."""

    # Define override for FastAPI's get_db dependency
    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        async with TestSessionLocal() as session:
            yield session

    # Apply dependency override on the FastAPI app
    app.dependency_overrides[get_db] = override_get_db

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    app.dependency_overrides.clear()

