"""Schemas for admin register extract retrieval and rendering."""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class RegisterExtractPropertySchema(BaseModel):
    address: Optional[str] = None
    tenure: Optional[str] = None
    description: Optional[str] = None


class RegisterExtractProprietorSchema(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    address: Optional[str] = None
    mismatch: bool = False


class RegisterExtractEntrySchema(BaseModel):
    entry_number: Optional[str] = None
    entry_text: Optional[str] = None
    registration_date: Optional[str] = None
    raw: dict[str, Any] = Field(default_factory=dict)


class RegisterExtractResponseSchema(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "report_id": "770e8400-e29b-41d4-a716-446655440002",
                "title_number": "GR506405",
                "fetched_at": "2026-05-06T10:00:00Z",
                "status": "complete",
                "property": {
                    "address": "10 BOLEYN COURT, BROXBOURNE, EN10 7HJ",
                    "tenure": "Freehold",
                    "description": "Freehold land shown edged red on the plan.",
                },
                "proprietors": [
                    {
                        "name": "JOHN SMITH",
                        "type": "Individual",
                        "address": "1 TEST STREET, HERTS, EN10 7HJ",
                        "mismatch": True,
                    }
                ],
                "charges": [],
                "restrictions": [],
                "leases": [],
                "notices": [],
                "quick_reference_flags": [
                    "Owner name differs from agency seller name"
                ],
                "official_copy_available": True,
            }
        }
    )

    report_id: str
    title_number: Optional[str] = None
    fetched_at: datetime
    status: str
    property: RegisterExtractPropertySchema
    proprietors: list[RegisterExtractProprietorSchema] = Field(default_factory=list)
    charges: list[RegisterExtractEntrySchema] = Field(default_factory=list)
    restrictions: list[RegisterExtractEntrySchema] = Field(default_factory=list)
    leases: list[RegisterExtractEntrySchema] = Field(default_factory=list)
    notices: list[RegisterExtractEntrySchema] = Field(default_factory=list)
    quick_reference_flags: list[str] = Field(default_factory=list)
    official_copy_available: bool = False
    error_message: Optional[str] = None


class AdminFraudCaseListItemSchema(BaseModel):
    id: str
    property_listing_id: str
    agency_id: str
    agency_name: str
    property_address: str
    title_number: Optional[str] = None
    client_name: Optional[str] = None
    vendor_name: Optional[str] = None
    price: Optional[str] = None
    postcode: Optional[str] = None
    withdrawn_date: Optional[datetime] = None
    confidence_score: float
    risk_level: Optional[str] = None
    verification_status: str
    verified_owner_name: Optional[str] = None
    is_confirmed_fraud: bool
    detected_at: datetime
    verified_at: Optional[datetime] = None
    register_extract_status: Optional[str] = None
    register_extract_fetched_at: Optional[datetime] = None


class AdminFraudCaseDetailSchema(AdminFraudCaseListItemSchema):
    ppd_transaction_id: Optional[str] = None
    ppd_price: Optional[int] = None
    ppd_transfer_date: Optional[datetime] = None
    ppd_postcode: Optional[str] = None
    ppd_full_address: Optional[str] = None
    address_similarity: float
    land_registry_response: Optional[str] = None
    register_extract: Optional["RegisterExtractResponseSchema"] = None


class AdminFraudCaseListResponseSchema(BaseModel):
    items: list[AdminFraudCaseListItemSchema]
    total: int
    page: int
    page_size: int


class AdminFraudAgencyFilterSchema(BaseModel):
    id: str
    name: str
