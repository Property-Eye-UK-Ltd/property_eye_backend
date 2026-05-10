"""OC with Summary cache model."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, JSON, String, Text
from sqlalchemy.orm import relationship

from src.db.base import Base


class OCWithSummary(Base):
    """Cached Official Copy with Summary response for a fraud report."""

    __tablename__ = "oc_with_summary"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    fraud_report_id = Column(
        String,
        ForeignKey("fraud_matches.id"),
        nullable=False,
        unique=True,
        index=True,
    )
    title_number = Column(String, nullable=True, index=True)
    response_code = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False, index=True)
    poll_id = Column(String, nullable=True, index=True)
    expected_at = Column(DateTime, nullable=True)
    raw_xml = Column(Text, nullable=True)
    parsed_json = Column(JSON, nullable=True)
    pdf_filename = Column(String, nullable=True)
    pdf_base64 = Column(Text, nullable=True)
    fetched_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    fraud_match = relationship("FraudMatch", back_populates="oc_with_summary")

