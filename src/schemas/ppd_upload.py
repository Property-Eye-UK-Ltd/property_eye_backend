"""
PPD upload Pydantic schemas for API requests and responses.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class PPDUploadResponse(BaseModel):
    """Schema for PPD upload response."""

    upload_id: str = Field(..., description="Unique upload identifier")
    filename: str = Field(..., description="Uploaded filename")
    year: int = Field(..., description="PPD data year")
    month: int = Field(..., description="PPD data month")
    file_size_mb: float = Field(..., description="File size in megabytes")
    status: str = Field(..., description="Upload status (uploaded, processing)")
    message: str = Field(..., description="Status message")
    uploaded_at: datetime = Field(..., description="Upload timestamp")


class PPDUploadStatusResponse(BaseModel):
    """Schema for PPD upload status check."""

    upload_id: str = Field(..., description="Upload identifier")
    filename: str = Field(..., description="Uploaded filename")
    year: int = Field(..., description="PPD data year")
    month: int = Field(..., description="PPD data month")
    status: str = Field(
        ...,
        description="Processing status (uploaded, processing, completed, failed)",
    )
    records_processed: Optional[int] = Field(
        None, description="Number of records processed"
    )
    error_message: Optional[str] = Field(None, description="Error message if failed")
    uploaded_at: datetime = Field(..., description="Upload timestamp")
    processed_at: Optional[datetime] = Field(
        None, description="Processing completion timestamp"
    )
