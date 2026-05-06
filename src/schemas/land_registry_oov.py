from typing import List, Optional

from pydantic import BaseModel, Field


class OovPersonName(BaseModel):
    """Person name payload for OOV requests."""

    title: Optional[str] = None
    forename: Optional[str] = None
    middle_names: Optional[str] = None
    surname: Optional[str] = None


class OovAddress(BaseModel):
    """Address payload for OOV requests and responses."""

    sub_building_name: Optional[str] = None
    building_name: Optional[str] = None
    building_number: Optional[str] = None
    street: Optional[str] = None
    town: Optional[str] = None
    postcode: Optional[str] = None


class OovRequest(BaseModel):
    """High-level request model for Online Owner Verification."""

    external_reference: str
    customer_reference: Optional[str] = None
    person_name: Optional[OovPersonName] = None
    company_name: Optional[str] = None
    address: Optional[OovAddress] = None
    title_number: Optional[str] = None
    historical_match: bool = True
    partial_match: bool = True
    highlight_additional_owners: bool = True


class OovOwner(BaseModel):
    """Owner match information returned from OOV."""

    name_match_type: str
    forename: Optional[str] = None
    surname: Optional[str] = None
    company_name: Optional[str] = None
    is_current_owner: bool
    is_historical_owner: bool


class OovMatchedTitle(BaseModel):
    """Matched title and associated owner matches."""

    title_number: str
    address: OovAddress
    owners: List[OovOwner] = Field(default_factory=list)


class OovResponse(BaseModel):
    """Parsed Online Owner Verification response."""

    external_reference: str
    status_code: str
    status_message: Optional[str] = None
    matches: List[OovMatchedTitle] = Field(default_factory=list)
    raw_status_code: int
    raw_body: Optional[str] = None
