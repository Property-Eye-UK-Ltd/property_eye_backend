"""
Core configuration module for the Fraud Detection POC application.

This module uses Pydantic BaseSettings to manage environment-based
configuration for database connections, API keys, and application settings.
"""

import os
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    Uses Pydantic BaseSettings for automatic environment variable parsing
    and validation.
    """

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    # Application Settings
    APP_NAME: str = "Property Eye Fraud Detection POC"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = False

    # Database Configuration
    # PostgreSQL for production, SQLite for POC (PPD scanning and output only)
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "sqlite+aiosqlite:///./fraud_detection.db"
    )

    # PPD Storage Configuration
    PPD_VOLUME_PATH: str = os.getenv("PPD_VOLUME_PATH", "./data/ppd")
    PPD_COMPRESSION: str = os.getenv("PPD_COMPRESSION", "snappy")
    CSV_VOLUME_PATH: str = os.getenv("CSV_VOLUME_PATH", "./data/csv")
    SYNC_PPD: bool = os.getenv("SYNC_PPD", "false").lower() in ("true", "1", "yes")

    # Land Registry API Configuration
    LAND_REGISTRY_API_KEY: Optional[str] = os.getenv("LAND_REGISTRY_API_KEY")
    LAND_REGISTRY_API_URL: str = os.getenv(
        "LAND_REGISTRY_API_URL", "https://api.landregistry.gov.uk"
    )

    # Redis Configuration (for future caching)
    REDIS_URL: Optional[str] = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # CORS Configuration
    CORS_ORIGINS: list = ["*"]  # Configure appropriately for production

    # API Configuration
    API_V1_PREFIX: str = "/api/v1"

    # File Upload Configuration
    MAX_UPLOAD_SIZE_MB: int = 100
    ALLOWED_UPLOAD_EXTENSIONS: list = [".csv", ".xlsx", ".xls", ".pdf"]

    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


# Global settings instance
settings = Settings()
