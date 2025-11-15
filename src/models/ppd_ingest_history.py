"""
PPD Ingest History ORM model.

Tracks which PPD CSV files have been ingested to avoid duplicate processing.
"""

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String

from src.db.base import Base


class PPDIngestHistory(Base):
    """
    PPD Ingest History model for tracking processed CSV files.

    Attributes:
        id: Unique identifier (UUID)
        csv_filename: Name of the CSV file that was ingested
        csv_path: Full path to the CSV file
        parquet_path: Path to the generated Parquet file
        year: Year partition
        month: Month partition
        records_processed: Number of records successfully processed
        ingested_at: Timestamp of ingestion
    """

    __tablename__ = "ppd_ingest_history"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    csv_filename = Column(String, nullable=False, unique=True, index=True)
    csv_path = Column(String, nullable=False)
    parquet_path = Column(String, nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    records_processed = Column(Integer, nullable=False, default=0)
    ingested_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self) -> str:
        return f"<PPDIngestHistory(csv_filename={self.csv_filename}, year={self.year}, month={self.month})>"
