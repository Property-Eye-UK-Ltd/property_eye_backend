"""
FraudMatch ORM model.

Represents a potential or confirmed fraud match between an agency property
and a PPD transaction record.
"""

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import relationship

from src.db.base import Base


class FraudMatch(Base):
    """
    FraudMatch model representing a fraud detection match.

    This model stores both suspicious matches (stage 1) and verified matches (stage 2).
    PPD data is denormalized from Parquet query results for efficient access.

    Attributes:
        id: Unique identifier (UUID)
        property_listing_id: Foreign key to PropertyListing
        ppd_transaction_id: PPD transaction UUID
        ppd_price: Sale price from PPD
        ppd_transfer_date: Transfer date from PPD
        ppd_postcode: Postcode from PPD
        ppd_full_address: Full address from PPD
        confidence_score: Overall confidence score (0-100)
        address_similarity: Address similarity score (0-100)
        verification_status: Status (suspicious, confirmed_fraud, not_fraud, error)
        land_registry_response: JSON response from Land Registry API
        verified_owner_name: Owner name from Land Registry
        is_confirmed_fraud: Boolean flag for confirmed fraud
        detected_at: Timestamp of detection
        verified_at: Timestamp of verification
        property_listing: Relationship to PropertyListing model
    """

    __tablename__ = "fraud_matches"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    property_listing_id = Column(
        String, ForeignKey("property_listings.id"), nullable=False
    )

    # PPD data (denormalized from Parquet query results)
    ppd_transaction_id = Column(String)
    ppd_price = Column(Integer)
    ppd_transfer_date = Column(DateTime)
    ppd_postcode = Column(String)
    ppd_full_address = Column(String)

    # Match details
    confidence_score = Column(Float)  # 0-100
    address_similarity = Column(Float)  # 0-100

    # Verification status: suspicious, confirmed_fraud, not_fraud, error
    verification_status = Column(
        String, index=True, default="suspicious", nullable=False
    )
    land_registry_response = Column(Text, nullable=True)  # JSON
    verified_owner_name = Column(String, nullable=True)
    is_confirmed_fraud = Column(Boolean, default=False, nullable=False)

    # Timestamps
    detected_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    verified_at = Column(DateTime, nullable=True)

    # Relationships
    property_listing = relationship("PropertyListing", back_populates="fraud_matches")

    def __repr__(self) -> str:
        return (
            f"<FraudMatch(id={self.id}, "
            f"confidence_score={self.confidence_score}, "
            f"verification_status={self.verification_status})>"
        )
