"""
Alto integration import endpoint for importing properties from Alto into Property Eye.
"""

import logging
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel

from src.db.session import get_db
from src.api import deps
from src.models.agency import Agency
from src.models.property_listing import PropertyListing
from src.integrations.alto.client import alto_api_client
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

        # Parse Alto response - handle different response formats
        properties_data = []
        if isinstance(alto_response, dict):
            # Check for common API response patterns
            if "properties" in alto_response:
                properties_data = alto_response["properties"]
            elif (
                "_embedded" in alto_response
                and "properties" in alto_response["_embedded"]
            ):
                properties_data = alto_response["_embedded"]["properties"]
            elif "data" in alto_response:
                properties_data = alto_response["data"]
            else:
                # Might be a direct list or other structure
                properties_data = alto_response.get("items", [])
        elif isinstance(alto_response, list):
            properties_data = alto_response

        logger.info(f"Found {len(properties_data)} properties in Alto response")

        # Import properties into database
        properties_imported = 0
        properties_skipped = 0
        errors = []

        for prop_data in properties_data:
            try:
                # Extract address components
                address_obj = prop_data.get("address", {})
                if isinstance(address_obj, dict):
                    address_parts = [
                        address_obj.get("address_line_1", ""),
                        address_obj.get("address_line_2", ""),
                        address_obj.get("town", ""),
                    ]
                    full_address = ", ".join([p for p in address_parts if p])
                    postcode = address_obj.get("postcode", "")
                else:
                    # Fallback if address is a string
                    full_address = (
                        str(address_obj) if address_obj else "Unknown Address"
                    )
                    postcode = ""

                # Skip if no meaningful address
                if not full_address or full_address == "Unknown Address":
                    properties_skipped += 1
                    continue

                # Map Alto status to our status
                alto_status = prop_data.get("status", "").lower()
                status_mapping = {
                    "available": "active",
                    "sold": "sold",
                    "withdrawn": "withdrawn",
                    "under_offer": "active",
                    "let": "sold",
                }
                property_status = status_mapping.get(alto_status, "active")

                # Check if property already exists (by address and postcode)
                existing_stmt = select(PropertyListing).where(
                    PropertyListing.agency_id == current_agency.id,
                    PropertyListing.address == full_address,
                    PropertyListing.postcode == postcode,
                )
                existing_result = await db.execute(existing_stmt)
                existing_property = existing_result.scalar_one_or_none()

                if existing_property:
                    # Skip duplicates
                    properties_skipped += 1
                    logger.debug(f"Skipping duplicate property: {full_address}")
                    continue

                # Create new property listing
                new_listing = PropertyListing(
                    agency_id=current_agency.id,
                    address=full_address,
                    normalized_address=full_address.lower().strip(),
                    postcode=postcode,
                    client_name="Alto Import",  # Default since Alto doesn't provide client name
                    status=property_status,
                    withdrawn_date=None,  # Could parse from updated_at if status is withdrawn
                )

                db.add(new_listing)
                properties_imported += 1

            except Exception as e:
                error_msg = f"Error importing property: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
                continue

        # Commit all new listings
        await db.commit()

        logger.info(
            f"Successfully imported {properties_imported} properties, skipped {properties_skipped}"
        )

        return AltoImportResponse(
            success=True,
            properties_imported=properties_imported,
            properties_skipped=properties_skipped,
            message=f"Successfully imported {properties_imported} properties from Alto ({current_env} mode). {properties_skipped} duplicates skipped.",
            errors=errors,
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
