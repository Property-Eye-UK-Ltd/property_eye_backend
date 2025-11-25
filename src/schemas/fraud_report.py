"""
Fraud report schemas for API responses.

Defines schemas for fraud detection results, including suspicious matches
and confidence score distributions.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class FraudMatchSchema(BaseModel):
    """
    Schema for a single fraud match record.

    Represents a potential or confirmed fraud case with all relevant details.
    """

    id: str = Field(..., description="Unique match identifier")
    property_listing_id: str = Field(..., description="Property listing ID")

    # Property details
    property_address: str = Field(..., description="Agency property address")
    client_name: str = Field(..., description="Agency client name")
    withdrawn_date: Optional[datetime] = Field(
        None, description="Date property was withdrawn"
    )

    # PPD details
    ppd_transaction_id: str = Field(..., description="PPD transaction ID")
    ppd_price: int = Field(..., description="Sale price from PPD")
    ppd_transfer_date: datetime = Field(..., description="Transfer date from PPD")
    ppd_postcode: str = Field(..., description="Postcode from PPD")
    ppd_full_address: str = Field(..., description="Full address from PPD")

    # Match scoring
    confidence_score: float = Field(..., description="Overall confidence score (0-100)")
    address_similarity: float = Field(
        ..., description="Address similarity score (0-100)"
    )
    risk_level: Optional[str] = Field(
        None, description="Risk level: CRITICAL, HIGH, MEDIUM, LOW"
    )

    # Verification details
    verification_status: str = Field(
        ...,
        description="Verification status: suspicious, confirmed_fraud, not_fraud, error",
    )
    verified_owner_name: Optional[str] = Field(
        None, description="Owner name from Land Registry"
    )
    is_confirmed_fraud: bool = Field(..., description="Whether fraud is confirmed")

    # Timestamps
    detected_at: datetime = Field(..., description="When match was detected")
    verified_at: Optional[datetime] = Field(None, description="When match was verified")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "770e8400-e29b-41d4-a716-446655440002",
                "property_listing_id": "880e8400-e29b-41d4-a716-446655440003",
                "property_address": "123 High Street, London",
                "client_name": "John Smith",
                "withdrawn_date": "2025-01-15T00:00:00",
                "ppd_transaction_id": "{ABC123-DEF456}",
                "ppd_price": 450000,
                "ppd_transfer_date": "2025-02-20T00:00:00",
                "ppd_postcode": "SW1A 1AA",
                "ppd_full_address": "123 HIGH STREET, LONDON, SW1A 1AA",
                "confidence_score": 87.5,
                "address_similarity": 92.0,
                "verification_status": "suspicious",
                "verified_owner_name": None,
                "is_confirmed_fraud": False,
                "detected_at": "2025-03-01T10:30:00",
                "verified_at": None,
            }
        }


class ConfidenceDistribution(BaseModel):
    """
    Distribution of matches by confidence score ranges.

    Helps agencies prioritize which matches to verify first.
    """

    high_confidence: int = Field(
        ..., description="Matches with confidence >= 85% (recommended for verification)"
    )
    medium_confidence: int = Field(..., description="Matches with confidence 70-84%")
    low_confidence: int = Field(..., description="Matches with confidence < 70%")

    class Config:
        json_schema_extra = {
            "example": {
                "high_confidence": 12,
                "medium_confidence": 28,
                "low_confidence": 5,
            }
        }


class SuspiciousMatchSummary(BaseModel):
    """
    Summary of suspicious matches detected in Stage 1.

    This is returned after the initial fraud detection scan,
    before any Land Registry verification calls are made.
    """

    total_matches: int = Field(
        ..., description="Total number of suspicious matches detected"
    )
    confidence_distribution: ConfidenceDistribution = Field(
        ..., description="Distribution of matches by confidence score"
    )
    matches: list[FraudMatchSchema] = Field(
        ..., description="List of all suspicious matches"
    )
    message: str = Field(..., description="Summary message for the detection results")

    class Config:
        json_schema_extra = {
            "example": {
                "total_matches": 45,
                "confidence_distribution": {
                    "high_confidence": 12,
                    "medium_confidence": 28,
                    "low_confidence": 5,
                },
                "matches": [],
                "message": "Stage 1 complete: 45 suspicious matches detected. Review high-confidence matches for Land Registry verification.",
            }
        }
