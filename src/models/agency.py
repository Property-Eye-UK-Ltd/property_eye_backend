"""
Agency ORM model.

Represents a real estate agency in the system.
"""

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, String
from sqlalchemy.orm import relationship

from src.db.base import Base


class Agency(Base):
    """
    Agency model representing a real estate agency.

    Attributes:
        id: Unique identifier (UUID)
        name: Agency name
        username: Agency username
        hashed_password: Agency hashed password
        access_token: Agency access token
        created_at: Timestamp of agency creation
        property_listings: Relationship to PropertyListing model
    """

    __tablename__ = "agencies"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=False)
    username = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    access_token = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    alto_agency_ref = Column(String, nullable=True, index=True)

    # Relationships
    property_listings = relationship(
        "PropertyListing", back_populates="agency", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Agency(id={self.id}, name={self.name}, alto_agency_ref={self.alto_agency_ref})>"
