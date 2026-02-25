import logging
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import get_db
from src.services.agency_service import AgencyService
from src.schemas.admin_alto import (
    AgencyAltoUpdate,
    AgencyAltoListResponse,
    AgencyAltoDetail,
)
from src.core.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/alto-agencies", tags=["admin-alto"])


@router.get(
    "",
    response_model=AgencyAltoListResponse,
    summary="List agencies with Alto status",
    description="List all agencies including their Alto integration status and reference.",
)
async def list_alto_agencies(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1),
    db: AsyncSession = Depends(get_db),
):
    skip = (page - 1) * page_size
    agencies, total = await AgencyService.get_agencies(db, skip=skip, limit=page_size)

    current_env = settings.ALTO_ENV

    items = []
    for agency in agencies:
        alto_agency_ref = getattr(agency, "alto_agency_ref", None)

        # Calculate status
        if current_env == "sandbox":
            alto_status = "Sandbox (Implicit)"
        else:
            if alto_agency_ref:
                alto_status = "Production Connected"
            else:
                alto_status = "Not Enabled"

        # Construct detail response manually with all required fields
        detail = AgencyAltoDetail(
            id=agency.id,
            name=agency.name,
            username=agency.username,
            created_at=agency.created_at,
            alto_agency_ref=alto_agency_ref,
            alto_env=current_env,
            alto_status=alto_status,
        )

        items.append(detail)

    return AgencyAltoListResponse(
        items=items,
        total=total,
    )


@router.patch(
    "/{agency_id}/settings",
    response_model=AgencyAltoDetail,
    summary="Update Alto settings for an agency",
)
async def update_alto_settings(
    agency_id: str,
    update_data: AgencyAltoUpdate,
    db: AsyncSession = Depends(get_db),
):
    """
    Update agency's Alto settings (AgencyRef and Production Toggle).
    """
    # Validation
    new_ref = None
    if update_data.enable_production:
        if not update_data.alto_agency_ref:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="alto_agency_ref is required when enabling Production.",
            )
        new_ref = update_data.alto_agency_ref
    else:
        # If disabled, we clear the reference to ensure it's not used.
        new_ref = None

    # Get current agency for logging
    old_agency = await AgencyService.get_agency(db, agency_id)
    if not old_agency:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Agency with ID {agency_id} not found",
        )

    old_ref = getattr(old_agency, "alto_agency_ref", None)

    # Perform update
    updated_agency = await AgencyService.update_agency_alto(db, agency_id, new_ref)

    if updated_agency:
        logger.info(
            f"AUDIT: Agency {agency_id} Alto settings updated. "
            f"Ref: '{old_ref}' -> '{new_ref}'. Production Enabled: {update_data.enable_production}"
        )

    # Prepare response
    current_env = settings.ALTO_ENV

    if current_env == "sandbox":
        alto_status = "Sandbox (Implicit)"
    else:
        alto_status = "Production Connected" if new_ref else "Not Enabled"

    detail = AgencyAltoDetail(
        id=updated_agency.id,
        name=updated_agency.name,
        username=updated_agency.username,
        created_at=updated_agency.created_at,
        alto_agency_ref=new_ref,
        alto_env=current_env,
        alto_status=alto_status,
    )

    return detail
