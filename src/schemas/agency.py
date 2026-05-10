"""
Agency Pydantic schemas for API requests and responses.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class AgencyBase(BaseModel):
    """Base schema for agency data."""

    name: str = Field(..., min_length=1, max_length=255, description="Agency name")
    username: str = Field(..., min_length=3, max_length=50, description="Username for login")


class AgencyCreate(AgencyBase):
    """Schema for creating a new agency."""

    password: str = Field(..., min_length=6, description="Password")


class AgencyLogin(BaseModel):
    """Schema for agency login."""

    username: str
    password: str


class AgencyChangePassword(BaseModel):
    """Schema for changing agency password."""

    username: str
    new_password: str
    confirm_passcode: str


class Token(BaseModel):
    """Schema for JWT token."""

    access_token: str
    token_type: str
    agency_name: str
    agency_id: str


class AgencyUpdate(BaseModel):
    """Schema for updating an existing agency."""

    name: Optional[str] = Field(
        None, min_length=1, max_length=255, description="Agency name"
    )


class AgencyResponse(AgencyBase):
    """Schema for agency response."""

    id: str = Field(..., description="Unique agency identifier")
    created_at: datetime = Field(..., description="Agency creation timestamp")

    model_config = ConfigDict(from_attributes=True)


class AgencyListResponse(BaseModel):
    """Schema for paginated agency list response."""

    agencies: list[AgencyResponse] = Field(..., description="List of agencies")
    total: int = Field(..., description="Total number of agencies")
    page: int = Field(..., description="Current page number")
    page_size: int = Field(..., description="Number of items per page")


class AgencyStatsResponse(BaseModel):
    """Schema for agency statistics."""

    total_listings: int = Field(..., description="Total number of property listings")
    suspicious_matches: int = Field(..., description="Number of suspicious matches found")
    confirmed_fraud: int = Field(..., description="Number of confirmed fraud cases")
    potential_savings: float = Field(..., description="Estimated potential savings (GBP)")
