"""
Configuration constants for the Fraud Detection POC system.

This module defines all configurable parameters for fraud detection,
PPD data management, and Land Registry API integration.
"""

from dataclasses import dataclass, field
from typing import List


@dataclass
class FraudDetectionConfig:
    """Configuration dataclass for fraud detection system parameters."""

    # PPD Storage (loaded from settings at runtime)
    PPD_VOLUME_PATH: str = field(default="./data/ppd")
    PPD_COMPRESSION: str = field(default="snappy")
    CSV_VOLUME_PATH: str = field(default="./data/csv")
    SYNC_PPD: bool = field(default=False)

    # Scan window — configurable via FRAUD_LOOKBACK_MONTHS / FRAUD_LOOKAHEAD_MONTHS env vars
    LOOKBACK_MONTHS: int = field(default=3)    # months BEFORE withdrawal to search PPD
    LOOKAHEAD_MONTHS: int = field(default=60)  # months AFTER withdrawal to search PPD

    # Legacy alias kept for any references in ppd_service (maps to LOOKAHEAD_MONTHS)
    @property
    def SCAN_WINDOW_MONTHS(self) -> int:  # noqa: N802
        return self.LOOKAHEAD_MONTHS

    # Risk level day thresholds
    RISK_CRITICAL_DAYS: int = field(default=180)   # <= N days => CRITICAL
    RISK_HIGH_DAYS: int = field(default=365)        # <= N days => HIGH
    RISK_MEDIUM_DAYS: int = field(default=1095)     # <= N days => MEDIUM (else LOW)

    # Confidence Scoring
    MIN_CONFIDENCE_THRESHOLD: float = field(default=70.0)   # Store matches above this
    HIGH_CONFIDENCE_THRESHOLD: float = field(default=85.0)  # Flag for LR verification

    # Address Matching
    MIN_ADDRESS_SIMILARITY: float = field(default=80.0)
    POSTCODE_MATCH_BONUS: float = field(default=10.0)

    # Confidence Score Weights
    ADDRESS_SIMILARITY_WEIGHT: float = 0.70
    DATE_PROXIMITY_WEIGHT: float = 0.20
    POSTCODE_MATCH_WEIGHT: float = 0.10

    # Required Fields for legacy document parser (mapped DataFrame columns)
    # Buyer client_name is optional — many exports only carry vendor/seller names.
    REQUIRED_FIELDS: List[str] = field(
        default_factory=lambda: [
            "address",
            "status",
            "withdrawn_date",
            "postcode",
        ]
    )

    # Allowed Upload File Extensions
    ALLOWED_UPLOAD_EXTENSIONS: List[str] = field(
        default_factory=lambda: [".csv", ".xlsx", ".xls", ".pdf"]
    )

    # Land Registry API Configuration
    LAND_REGISTRY_API_URL: str = field(default="https://api.landregistry.gov.uk")
    LAND_REGISTRY_API_KEY: str = field(default="")
    LAND_REGISTRY_TIMEOUT: int = 30  # seconds
    LAND_REGISTRY_MAX_RETRIES: int = 3

    # HMLR Business Gateway (Online Owner Verification)
    HMLR_BG_BASE_URL: str = field(default="https://bgtest.landregistry.gov.uk")
    HMLR_RES_PATH: str = field(default="")
    APP_ENV: str = field(default="dev")

    HMLR_BG_USERNAME: str = field(default="")
    HMLR_BG_PASSWORD: str = field(default="")
    HMLR_TLS_CERT_PATH: str = field(default="")
    HMLR_TLS_KEY_PATH: str = field(default="")
    HMLR_CA_BUNDLE_PATH: str = field(default="")
    HMLR_TLS_CERT_PEM: str = field(default="")
    HMLR_TLS_KEY_PEM: str = field(default="")
    HMLR_CA_BUNDLE_PEM: str = field(default="")
    HMLR_TIMEOUT_SECONDS: int = 20

    # Parquet File Sizing
    TARGET_PARQUET_SIZE_MB: int = 500  # Target 500MB per file (between 100MB-1GB)

    # Owner Name Matching
    OWNER_NAME_SIMILARITY_THRESHOLD: float = (
        85.0  # Fuzzy match threshold for owner verification
    )


# Initialize config with values from settings
def get_config():
    """
    Get fraud detection config populated with environment values.
    Import settings here to avoid circular imports.
    """
    from src.core.config import settings

    return FraudDetectionConfig(
        PPD_VOLUME_PATH=settings.PPD_VOLUME_PATH,
        PPD_COMPRESSION=settings.PPD_COMPRESSION,
        CSV_VOLUME_PATH=settings.CSV_VOLUME_PATH,
        SYNC_PPD=settings.SYNC_PPD,
        # Scan window
        LOOKBACK_MONTHS=settings.FRAUD_LOOKBACK_MONTHS,
        LOOKAHEAD_MONTHS=settings.FRAUD_LOOKAHEAD_MONTHS,
        # Risk thresholds
        RISK_CRITICAL_DAYS=settings.FRAUD_RISK_CRITICAL_DAYS,
        RISK_HIGH_DAYS=settings.FRAUD_RISK_HIGH_DAYS,
        RISK_MEDIUM_DAYS=settings.FRAUD_RISK_MEDIUM_DAYS,
        # Confidence
        MIN_CONFIDENCE_THRESHOLD=settings.FRAUD_MIN_CONFIDENCE,
        HIGH_CONFIDENCE_THRESHOLD=settings.FRAUD_HIGH_CONFIDENCE,
        MIN_ADDRESS_SIMILARITY=settings.FRAUD_MIN_ADDRESS_SIMILARITY,
        # Land Registry
        LAND_REGISTRY_API_URL=settings.LAND_REGISTRY_API_URL,
        LAND_REGISTRY_API_KEY=settings.LAND_REGISTRY_API_KEY or "",
        HMLR_BG_BASE_URL=settings.HMLR_BG_BASE_URL,
        HMLR_RES_PATH=settings.HMLR_RES_PATH,
        APP_ENV=settings.APP_ENV,
        HMLR_BG_USERNAME=settings.HMLR_BG_USERNAME,
        HMLR_BG_PASSWORD=settings.HMLR_BG_PASSWORD,
        HMLR_TLS_CERT_PATH=settings.HMLR_TLS_CERT_PATH,
        HMLR_TLS_KEY_PATH=settings.HMLR_TLS_KEY_PATH,
        HMLR_CA_BUNDLE_PATH=settings.HMLR_CA_BUNDLE_PATH or "",
        HMLR_TLS_CERT_PEM=settings.HMLR_TLS_CERT_PEM or "",
        HMLR_TLS_KEY_PEM=settings.HMLR_TLS_KEY_PEM or "",
        HMLR_CA_BUNDLE_PEM=settings.HMLR_CA_BUNDLE_PEM or "",
        HMLR_TIMEOUT_SECONDS=settings.HMLR_TIMEOUT_SECONDS,
    )


# Global configuration instance
config = get_config()
