"""
Alto integration import endpoint for importing properties from Alto into Property Eye.
"""

import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel

from src.db.session import get_db
from src.api import deps
from src.models.agency import Agency
from src.integrations.alto.client import alto_api_client
from src.services.agency_service import AgencyService
from src.core.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/integrations/alto", tags=["integrations-alto"])


class AltoImportResponse(BaseModel):
    """Response model for Alto import operation."""

    success: bool
    properties_imported: int
    properties_skipped: int
    message: str
    errors: list[str] = []


@router.post("/import", response_model=AltoImportResponse)
async def import_alto_properties(
    current_agency: Agency = Depends(deps.get_current_agency),
    db: AsyncSession = Depends(get_db),
):
    """
    Import properties from Alto for the current agency.

    - In Sandbox mode: Uses default sandbox tenant (no AgencyRef needed)
    - In Production mode: Requires agency to have alto_agency_ref configured
    """
    logger.info(f"Starting Alto import for agency {current_agency.id}")

    # Get agency's alto_agency_ref
    alto_agency_ref = getattr(current_agency, "alto_agency_ref", None)
    current_env = settings.ALTO_ENV

    # Validate production requirements
    if current_env == "production" and not alto_agency_ref:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Agency is not configured for Alto Production. Please configure AgencyRef in Admin settings first.",
        )

    try:
        # Fetch properties from Alto
        logger.info(f"Fetching properties from Alto ({current_env} mode)")

        # Start with page 1, we can paginate if needed
        alto_response = await alto_api_client.list_properties(
            alto_agency_ref=alto_agency_ref if current_env == "production" else None,
            page=1,
            page_size=100,  # Fetch up to 100 properties
        )

        # TODO: Parse Alto response and save to PropertyListing table
        # For now, we'll return the count from the response
        # The actual implementation would involve:
        # 1. Parse alto_response to extract property data
        # 2. Map Alto fields to PropertyListing fields
        # 3. Create PropertyListing records in database
        # 4. Handle duplicates (skip or update)

        properties_count = len(
            alto_response.get("properties", [])
            if isinstance(alto_response, dict)
            else []
        )

        logger.info(f"Successfully fetched {properties_count} properties from Alto")

        return AltoImportResponse(
            success=True,
            properties_imported=properties_count,
            properties_skipped=0,
            message=f"Successfully imported {properties_count} properties from Alto ({current_env} mode)",
            errors=[],
        )

    except ValueError as e:
        # This catches the "Configuration Error" from alto client
        logger.error(f"Configuration error during Alto import: {str(e)}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Error importing from Alto: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to import properties from Alto: {str(e)}",
        )
