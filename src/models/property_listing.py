"""
PropertyListing ORM model.

Represents a property listing from an agency document.
"""

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, String
from sqlalchemy.orm import relationship

from src.db.base import Base


class PropertyListing(Base):
    """
    PropertyListing model representing an agency property listing.

    Attributes:
        id: Unique identifier (UUID)
        agency_id: Foreign key to Agency
        address: Original property address
        normalized_address: Cleaned address for matching
        postcode: UK postcode
        client_name: Name of the client associated with the property
        status: Property status (withdrawn, active, sold, etc.)
        withdrawn_date: Date when property was withdrawn
        created_at: Timestamp of record creation
        agency: Relationship to Agency model
        fraud_matches: Relationship to FraudMatch model
    """

    __tablename__ = "property_listings"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    agency_id = Column(String, ForeignKey("agencies.id"), nullable=False)

    # Core fields (from field mapping)
    address = Column(String, nullable=False)
    normalized_address = Column(String, index=True)
    postcode = Column(String, index=True)
    client_name = Column(String)
    status = Column(String, index=True)  # withdrawn, active, sold, etc.
    withdrawn_date = Column(DateTime, nullable=True)

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relationships
    agency = relationship("Agency", back_populates="property_listings")
    fraud_matches = relationship(
        "FraudMatch", back_populates="property_listing", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<PropertyListing(id={self.id}, address={self.address}, status={self.status})>"
