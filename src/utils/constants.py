"""
Configuration constants for the Fraud Detection POC system.

This module defines all configurable parameters for fraud detection,
PPD data management, and Land Registry API integration.
"""

import os
from dataclasses import dataclass
from typing import List


@dataclass
class FraudDetectionConfig:
    """Configuration dataclass for fraud detection system parameters."""

    # PPD Storage (from environment variables)
    PPD_VOLUME_PATH: str = os.getenv("PPD_VOLUME_PATH", "/data/ppd")
    PPD_COMPRESSION: str = os.getenv("PPD_COMPRESSION", "snappy")  # or "zstd"
    CSV_VOLUME_PATH: str = os.getenv("CSV_VOLUME_PATH", "./data/csv")
    SYNC_PPD: bool = os.getenv("SYNC_PPD", "false").lower() in ("true", "1", "yes")

    # PPD Filtering
    SCAN_WINDOW_MONTHS: int = 12  # Check PPD records up to 12 months after withdrawal

    # Confidence Scoring
    MIN_CONFIDENCE_THRESHOLD: float = 70.0  # Store matches above this score
    HIGH_CONFIDENCE_THRESHOLD: float = 85.0  # Recommend for Land Registry verification

    # Address Matching
    MIN_ADDRESS_SIMILARITY: float = 80.0  # Minimum fuzzy match score
    POSTCODE_MATCH_BONUS: float = 10.0  # Bonus points for exact postcode match

    # Confidence Score Weights
    ADDRESS_SIMILARITY_WEIGHT: float = 0.70  # 70% weight for address matching
    DATE_PROXIMITY_WEIGHT: float = 0.20  # 20% weight for date proximity
    POSTCODE_MATCH_WEIGHT: float = 0.10  # 10% weight for postcode match

    # Required Fields for Agency Document Upload
    REQUIRED_FIELDS: List[str] = None

    def __post_init__(self):
        """Initialize fields that need to be mutable."""
        if self.REQUIRED_FIELDS is None:
            self.REQUIRED_FIELDS = [
                "address",
                "client_name",
                "status",
                "withdrawn_date",
                "postcode",
            ]

    # Land Registry API Configuration
    LAND_REGISTRY_API_URL: str = os.getenv(
        "LAND_REGISTRY_API_URL", "https://api.landregistry.gov.uk"
    )
    LAND_REGISTRY_API_KEY: str = os.getenv("LAND_REGISTRY_API_KEY", "")
    LAND_REGISTRY_TIMEOUT: int = 30  # seconds
    LAND_REGISTRY_MAX_RETRIES: int = 3

    # Parquet File Sizing
    TARGET_PARQUET_SIZE_MB: int = 500  # Target 500MB per file (between 100MB-1GB)

    # Owner Name Matching
    OWNER_NAME_SIMILARITY_THRESHOLD: float = (
        85.0  # Fuzzy match threshold for owner verification
    )


# Global configuration instance
config = FraudDetectionConfig()
