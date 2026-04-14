"""Document ingestion request and response schemas."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class ListingsIngestRequest(BaseModel):
    """Request schema for direct listings ingestion (JSON payload)."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "rows": [
                    [
                        "11 Hamlet Hill, Roydon, Harlow, Essex, CM19 5LA",
                        "£725,000",
                        "17 January 2020",
                        "John Smith",
                    ]
                ],
                "record": {
                    "address": "8 Woodstock, Brookfield Lane West, Cheshunt, Waltham Cross, Hertfordshire, EN8 0QH",
                    "postcode": "EN8 0QH",
                    "client_name": "Nina Baum",
                    "withdrawn_date": "09/07/2022",
                },
            }
        }
    )

    rows: Optional[List[Any]] = Field(
        default=None,
        description="Optional nested list or list-of-dicts listing content",
    )
    record: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional single manual listing record",
    )


class DocumentUploadResponse(BaseModel):
    """
    Response schema for document upload.

    Attributes:
        upload_id: Unique identifier for this upload
        status: Upload status (success, partial_success, failed)
        records_processed: Number of records successfully processed
        records_skipped: Number of duplicate records skipped
        message: Human-readable status message
    """

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "upload_id": "660e8400-e29b-41d4-a716-446655440001",
                "status": "success",
                "records_processed": 150,
                "records_skipped": 5,
                "message": "Document processed successfully",
            }
        }
    )

    upload_id: str = Field(
        ...,
        description="Unique identifier for this upload operation",
    )
    status: str = Field(
        ...,
        description="Upload status: success, partial_success, or failed",
    )
    records_processed: int = Field(
        ...,
        description="Number of records successfully processed and stored",
    )
    records_skipped: int = Field(
        ..., description="Number of duplicate records skipped"
    )
    message: str = Field(
        ...,
        description="Human-readable status message",
    )
