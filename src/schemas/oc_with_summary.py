"""Schemas for HM Land Registry Official Copy with Summary."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class OCAddressSchema(BaseModel):
    """Structured address fields returned by OC with Summary."""

    address_lines: list[str] = Field(default_factory=list)


class OCTitleDetailsSchema(BaseModel):
    """Title metadata returned in the summary."""

    title_number: Optional[str] = None
    title_absolute: Optional[str] = None
    property_address: Optional[OCAddressSchema] = None
    district_name: Optional[str] = None
    administrative_area: Optional[str] = None
    land_registry_office_name: Optional[str] = None
    latest_edition_date: Optional[date] = None


class OCIndividualNameSchema(BaseModel):
    """Registered proprietor individual name."""

    forename: Optional[str] = None
    surname: Optional[str] = None


class OCOrganisationNameSchema(BaseModel):
    """Registered proprietor organisation name."""

    name: Optional[str] = None


class OCProprietorNameSchema(BaseModel):
    """Registered proprietor name, either individual or organisation."""

    individual_name: Optional[OCIndividualNameSchema] = None
    organisation_name: Optional[OCOrganisationNameSchema] = None


class OCPartySchema(BaseModel):
    """Party details from the OC summary."""

    proprietor_name: Optional[OCProprietorNameSchema] = None
    company_registration_number: Optional[str] = None
    proprietor_address: Optional[OCAddressSchema] = None
    proprietorship_date: Optional[date] = None
    trading_name: Optional[str] = None
    party_number: Optional[str] = None
    party_description: Optional[str] = None


class OCEntryDetailsSchema(BaseModel):
    """Generic register entry block."""

    entry_number: Optional[str] = None
    entry_text: Optional[str] = None
    registration_date: Optional[date] = None
    sub_register_code: Optional[str] = None
    schedule_code: Optional[str] = None
    infills: dict[str, Any] = Field(default_factory=dict)


class OCProprietorshipDetailSchema(BaseModel):
    """Proprietorship register entry."""

    current_proprietorship_date: Optional[date] = None
    registered_proprietor_party: list[OCPartySchema] = Field(default_factory=list)
    cautioner_party: list[OCPartySchema] = Field(default_factory=list)


class OCChargeSchema(BaseModel):
    """Charge register entry."""

    charge_id: Optional[str] = None
    charge_date: Optional[date] = None
    registered_charge: dict[str, Any] = Field(default_factory=dict)
    charge_proprietor: dict[str, Any] = Field(default_factory=dict)
    sub_charges: list[dict[str, Any]] = Field(default_factory=list)


class OCRestrictionSchema(BaseModel):
    """Restriction register entry."""

    restriction_type_code: Optional[str] = None
    entry_details: Optional[OCEntryDetailsSchema] = None
    raw: dict[str, Any] = Field(default_factory=dict)


class OCLicenseSchema(BaseModel):
    """Lease entry."""

    lease_term: Optional[str] = None
    lease_date: Optional[date] = None
    rent: Optional[str] = None
    lease_party: list[OCPartySchema] = Field(default_factory=list)
    entry_details: Optional[OCEntryDetailsSchema] = None
    raw: dict[str, Any] = Field(default_factory=dict)


class OCPricePaidSchema(BaseModel):
    """Price paid entry."""

    amount: Optional[str] = None
    date: Optional[date] = None


class OCAttachmentSchema(BaseModel):
    """Attachment metadata for the PDF official copy."""

    filename: Optional[str] = None
    mime_type: Optional[str] = None


class OCPollDetailsSchema(BaseModel):
    """Queued-response metadata from Business Gateway."""

    poll_id: Optional[str] = None
    expected_at: Optional[datetime] = None
    message_text: Optional[str] = None


class OCWithSummaryResponseSchema(BaseModel):
    """Parsed OC with Summary response payload."""

    model_config = ConfigDict(json_schema_extra={"example": {}})

    report_id: str
    title_number: Optional[str] = None
    fetched_at: datetime
    status: str
    response_code: str
    title_details: Optional[OCTitleDetailsSchema] = None
    proprietorship_details: list[OCProprietorshipDetailSchema] = Field(default_factory=list)
    charges: list[OCChargeSchema] = Field(default_factory=list)
    restrictions: list[OCRestrictionSchema] = Field(default_factory=list)
    leases: list[OCLicenseSchema] = Field(default_factory=list)
    price_paid: Optional[OCPricePaidSchema] = None
    attachments: list[OCAttachmentSchema] = Field(default_factory=list)
    poll_details: Optional[OCPollDetailsSchema] = None
    official_copy_available: bool = False
    error_message: Optional[str] = None

