"""
Verification schemas for Land Registry API integration.

Defines schemas for Stage 2 verification requests and responses.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class VerificationRequest(BaseModel):
    """
    Request schema for verifying suspicious matches.

    Accepts a list of match IDs to verify via Land Registry API.
    """

    match_ids: list[str] = Field(
        ...,
        description="List of fraud match IDs to verify",
        min_length=1,
        example=["770e8400-e29b-41d4-a716-446655440002"],
    )

    class Config:
        json_schema_extra = {
            "example": {
                "match_ids": [
                    "770e8400-e29b-41d4-a716-446655440002",
                    "770e8400-e29b-41d4-a716-446655440003",
                ]
            }
        }


class VerificationResult(BaseModel):
    """
    Result of a single match verification.

    Contains the verification outcome for one suspicious match.
    """

    match_id: str = Field(..., description="Fraud match ID")
    property_address: str = Field(..., description="Property address")
    client_name: str = Field(..., description="Agency client name")

    verification_status: str = Field(
        ..., description="Verification status: confirmed_fraud, not_fraud, or error"
    )
    verified_owner_name: Optional[str] = Field(
        None, description="Owner name from Land Registry"
    )
    is_confirmed_fraud: bool = Field(..., description="Whether fraud is confirmed")
    verified_at: datetime = Field(..., description="Timestamp of verification")
    error_message: Optional[str] = Field(
        None, description="Error message if verification failed"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "match_id": "770e8400-e29b-41d4-a716-446655440002",
                "property_address": "123 High Street, London",
                "client_name": "John Smith",
                "verification_status": "confirmed_fraud",
                "verified_owner_name": "John Smith",
                "is_confirmed_fraud": True,
                "verified_at": "2025-03-01T14:30:00",
                "error_message": None,
            }
        }


class VerificationSummary(BaseModel):
    """
    Summary of Stage 2 verification results.

    Provides counts by verification status and detailed results.
    """

    total_verified: int = Field(..., description="Total number of matches verified")
    confirmed_fraud_count: int = Field(
        ..., description="Number of confirmed fraud cases"
    )
    not_fraud_count: int = Field(
        ..., description="Number of matches ruled out as fraud"
    )
    error_count: int = Field(..., description="Number of verification errors")
    results: list[VerificationResult] = Field(
        ..., description="Detailed verification results for each match"
    )
    message: str = Field(..., description="Summary message for verification results")

    class Config:
        json_schema_extra = {
            "example": {
                "total_verified": 12,
                "confirmed_fraud_count": 8,
                "not_fraud_count": 3,
                "error_count": 1,
                "results": [],
                "message": "Stage 2 complete: 8 confirmed fraud cases, 3 ruled out, 1 error",
            }
        }
