"""Register extract cache model for admin fraud-case review."""

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, JSON, String, Text
from sqlalchemy.orm import relationship

from src.db.base import Base


class RegisterExtract(Base):
    """Cached Register Extract Service response for a fraud match."""

    __tablename__ = "register_extracts"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    fraud_match_id = Column(
        String,
        ForeignKey("fraud_matches.id"),
        nullable=False,
        unique=True,
        index=True,
    )
    title_number = Column(String, nullable=True, index=True)
    raw_xml = Column(Text, nullable=True)
    parsed_json = Column(JSON, nullable=True)
    fetched_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    status = Column(String, default="pending", nullable=False, index=True)
    error_message = Column(Text, nullable=True)

    fraud_match = relationship("FraudMatch", back_populates="register_extract")
