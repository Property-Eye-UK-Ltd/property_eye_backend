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
        address: Property address (cleaned form from extraction pipeline)
        normalized_address: Canonical address form used for duplicate matching
        postcode: UK postcode
        region: Town or city
        county: County
        property_number: House/flat/plot number
        title_number: Land Registry title number if present
        client_name: Buyer / purchaser / applicant name when explicitly captured (nullable)
        vendor_name: Seller / vendor / landlord name when captured (nullable)
        status: Property status (withdrawn, active, sold, etc.)
        withdrawn_date: Date when property was withdrawn
        price: Listing price amount
        commission: Commission amount or percentage
        contract_duration: Agency contract duration if provided
        created_at: Timestamp of record creation
        agency: Relationship to Agency model
        fraud_matches: Relationship to FraudMatch model
    """

    __tablename__ = "property_listings"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    agency_id = Column(String, ForeignKey("agencies.id"), nullable=False)

    # Core fields (from ingestion + extraction mapping)
    address = Column(String, nullable=False)
    normalized_address = Column(String, index=True)
    postcode = Column(String, index=True)
    region = Column(String, nullable=True)
    county = Column(String, nullable=True)
    property_number = Column(String, nullable=True)
    title_number = Column(String, nullable=True, index=True)
    client_name = Column(String, nullable=True)
    vendor_name = Column(String, nullable=True)
    status = Column(String, index=True)  # withdrawn, active, sold, etc.
    withdrawn_date = Column(DateTime, nullable=True)
    price = Column(String, nullable=True)
    commission = Column(String, nullable=True)
    contract_duration = Column(String, nullable=True)

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relationships
    agency = relationship("Agency", back_populates="property_listings")
    fraud_matches = relationship(
        "FraudMatch", back_populates="property_listing", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<PropertyListing(id={self.id}, address={self.address}, status={self.status})>"
