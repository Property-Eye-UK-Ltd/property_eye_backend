"""
Document upload request and response schemas.
"""

from typing import Dict

from pydantic import BaseModel, Field


class DocumentUploadRequest(BaseModel):
    """
    Request schema for document upload.

    Attributes:
        agency_id: Unique identifier for the agency
        field_mapping: Dictionary mapping agency columns to system fields
    """

    agency_id: str = Field(
        ...,
        description="Unique identifier for the agency uploading the document",
        example="550e8400-e29b-41d4-a716-446655440000",
    )
    field_mapping: Dict[str, str] = Field(
        ...,
        description="Mapping of agency document columns to system field names",
        example={
            "Property Address": "address",
            "Client Full Name": "client_name",
            "Status": "status",
            "Date Withdrawn": "withdrawn_date",
            "Postcode": "postcode",
        },
    )

    class Config:
        json_schema_extra = {
            "example": {
                "agency_id": "550e8400-e29b-41d4-a716-446655440000",
                "field_mapping": {
                    "Property Address": "address",
                    "Client Full Name": "client_name",
                    "Status": "status",
                    "Date Withdrawn": "withdrawn_date",
                    "Postcode": "postcode",
                },
            }
        }


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

    upload_id: str = Field(
        ...,
        description="Unique identifier for this upload operation",
        example="660e8400-e29b-41d4-a716-446655440001",
    )
    status: str = Field(
        ...,
        description="Upload status: success, partial_success, or failed",
        example="success",
    )
    records_processed: int = Field(
        ...,
        description="Number of records successfully processed and stored",
        example=150,
    )
    records_skipped: int = Field(
        ..., description="Number of duplicate records skipped", example=5
    )
    message: str = Field(
        ...,
        description="Human-readable status message",
        example="Document processed successfully",
    )

    class Config:
        json_schema_extra = {
            "example": {
                "upload_id": "660e8400-e29b-41d4-a716-446655440001",
                "status": "success",
                "records_processed": 150,
                "records_skipped": 5,
                "message": "Document processed successfully",
            }
        }
