"""
Core configuration module for the Fraud Detection POC application.

This module uses Pydantic BaseSettings to manage environment-based
configuration for database connections, API keys, and application settings.
"""

from typing import Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    Uses Pydantic BaseSettings for automatic environment variable parsing
    and validation.
    """

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=True, extra="ignore"
    )

    # Application Settings
    APP_NAME: str = "Property Eye Fraud Detection POC"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = False
    APP_ENV: str = "dev"

    # Database Configuration
    # PostgreSQL for production, SQLite for POC (PPD scanning and output only)
    DATABASE_URL: str = "sqlite+aiosqlite:///./fraud_detection.db"

    @field_validator("DATABASE_URL")
    @classmethod
    def assemble_db_connection(cls, v: str) -> str:
        """
        Ensure the database URL uses the asyncpg driver for PostgreSQL.
        Railway provides 'postgresql://', but SQLAlchemy async engine needs 'postgresql+asyncpg://'.
        """
        if v and v.startswith("postgresql://"):
            return v.replace("postgresql://", "postgresql+asyncpg://", 1)
        return v

    @field_validator("APP_ENV")
    @classmethod
    def normalize_app_env(cls, v: str) -> str:
        return v.strip().lower()

    # PPD Storage Configuration
    # For Railway: use /data (mounted volume), for local: use ./data/ppd
    PPD_VOLUME_PATH: str = "/data/ppd"
    PPD_COMPRESSION: str = "snappy"
    CSV_VOLUME_PATH: str = "/data/csv"
    SYNC_PPD: bool = False

    # Fraud Detection Tuning
    FRAUD_LOOKBACK_MONTHS: int = 3  # months before withdrawal to search
    FRAUD_LOOKAHEAD_MONTHS: int = (
        60  # months after withdrawal to search (default 5 yrs)
    )
    FRAUD_RISK_CRITICAL_DAYS: int = 180
    FRAUD_RISK_HIGH_DAYS: int = 365
    FRAUD_RISK_MEDIUM_DAYS: int = 1095
    FRAUD_MIN_CONFIDENCE: float = 70.0
    FRAUD_HIGH_CONFIDENCE: float = 85.0
    FRAUD_MIN_ADDRESS_SIMILARITY: float = 80.0

    LAND_REGISTRY_API_KEY: Optional[str] = None
    LAND_REGISTRY_API_URL: str = "https://api.landregistry.gov.uk"

    # HMLR Business Gateway (Online Owner Verification)
    HMLR_BG_BASE_URL: str = "https://bgtest.landregistry.gov.uk"
    HMLR_OOV_PATH: str = (
        "/b2b/EOOV_SoapEngine/OnlineOwnershipVerificationV1_0WebService"
    )
    HMLR_RES_PATH: str = ""
    HMLR_BG_USERNAME: str
    HMLR_BG_PASSWORD: str
    HMLR_TLS_CERT_PATH: str
    HMLR_TLS_KEY_PATH: str
    # Optional custom CA bundle for BG test / production (PEM file)
    HMLR_CA_BUNDLE_PATH: Optional[str] = None
    HMLR_TLS_CERT_PEM: Optional[str] = None
    HMLR_TLS_KEY_PEM: Optional[str] = None
    HMLR_CA_BUNDLE_PEM: Optional[str] = None
    HMLR_TIMEOUT_SECONDS: int = 20

    # Redis Configuration (for future caching)
    REDIS_URL: str = "redis://localhost:6379/0"

    # CORS Configuration
    CORS_ORIGINS: list = [
        "http://localhost:5173",
        "http://localhost:3000",
        "https://propertyeye-pilot.vercel.app",
    ]

    # API Configuration
    API_V1_PREFIX: str = "/api/v1"

    # File Upload Configuration
    MAX_UPLOAD_SIZE_MB: int = 100
    ALLOWED_UPLOAD_EXTENSIONS: list = [".csv", ".xlsx", ".xls", ".pdf"]

    # Logging Configuration
    LOG_LEVEL: str = "INFO"

    # Security
    SECRET_KEY: str
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 24 * 60 * 60

    # Alto (Zoopla) Configuration
    ALTO_ENV: str = "sandbox"  # "sandbox" or "production"

    # Sandbox defaults
    ALTO_SANDBOX_AUTH_URL: str = "https://api.alto.zoopladev.co.uk/token"
    ALTO_SANDBOX_API_BASE: str = "https://api.alto.zoopladev.co.uk"

    # Production defaults
    ALTO_PRODUCTION_AUTH_URL: str = "https://api.alto.zoopla.co.uk/token"
    ALTO_PRODUCTION_API_BASE: str = "https://api.alto.zoopla.co.uk"

    ALTO_CLIENT_ID: str
    ALTO_CLIENT_SECRET: str
    ALTO_INTEGRATION_ID: str

    # AI Configuration (Column Mapping Agent)
    GOOGLE_API_KEY: Optional[str] = None
    GROQ_API_KEY: Optional[str] = None
    EXTRACTOR_LLM_PROVIDER: str = "gemini"
    EXTRACTOR_LLM_MODEL: Optional[str] = None

    # # Optional: per-agency identifiers if needed globally
    # ALTO_DEFAULT_AGENCY_ID: Optional[str] = None
    # ALTO_DEFAULT_BRANCH_ID: Optional[str] = None

    @property
    def alto_auth_url(self) -> str:
        return (
            self.ALTO_SANDBOX_AUTH_URL
            if self.ALTO_ENV == "sandbox"
            else self.ALTO_PRODUCTION_AUTH_URL
        )

    @property
    def alto_api_base_url(self) -> str:
        return (
            self.ALTO_SANDBOX_API_BASE
            if self.ALTO_ENV == "sandbox"
            else self.ALTO_PRODUCTION_API_BASE
        )


# Global settings instance
settings = Settings()
