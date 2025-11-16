"""
PPD Upload Job ORM model for tracking background processing.
"""

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, Float, Integer, String

from src.db.base import Base


class PPDUploadJob(Base):
    """
    Model for tracking PPD upload and processing jobs.

    Attributes:
        id: Unique job identifier (UUID)
        filename: Original uploaded filename
        csv_path: Path to stored CSV file
        year: PPD data year
        month: PPD data month
        file_size_mb: File size in megabytes
        status: Job status (uploaded, processing, completed, failed)
        records_processed: Number of records processed
        error_message: Error message if failed
        uploaded_at: Upload timestamp
        processed_at: Processing completion timestamp
    """

    __tablename__ = "ppd_upload_jobs"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    filename = Column(String, nullable=False)
    csv_path = Column(String, nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    file_size_mb = Column(Float, nullable=False)
    status = Column(
        String, nullable=False, default="uploaded"
    )  # uploaded, processing, completed, failed
    records_processed = Column(Integer, nullable=True)
    error_message = Column(String, nullable=True)
    uploaded_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    processed_at = Column(DateTime, nullable=True)

    def __repr__(self) -> str:
        return f"<PPDUploadJob(id={self.id}, filename={self.filename}, status={self.status})>"
