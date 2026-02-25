from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field
from src.schemas.agency import AgencyResponse


class AgencyAltoUpdate(BaseModel):
    """Schema for updating Alto settings for an agency."""

    alto_agency_ref: Optional[str] = Field(
        None, description="Alto Agency Reference (Production only)"
    )
    enable_production: bool = Field(
        False, description="Enable Alto in production for this agency"
    )


class AgencyAltoDetail(AgencyResponse):
    """Agency details with Alto integration status."""

    alto_agency_ref: Optional[str] = Field(None, description="Alto Agency Reference")
    alto_env: str = Field(
        ..., description="Current System Alto Environment (sandbox/production)"
    )
    alto_status: str = Field(
        ..., description="Derived status e.g. 'Sandbox only', 'Production connected'"
    )


class AgencyAltoLog(BaseModel):
    """Audit log for Alto changes."""

    # Assuming we might want to return logs later, though requirements just said "Log changes" (could be server logs).
    # But "Implement auditing: Log changes..." likely means logging to a table or file.
    # The requirement "Log all changes ... for audit" usually implies persistence.
    # However, "Database / models" only asked for `alto_agency_ref`.
    # So I will use standard Logger for now, or maybe the existing audit system if any.
    pass


class AgencyAltoListResponse(BaseModel):
    items: List[AgencyAltoDetail]
    total: int
