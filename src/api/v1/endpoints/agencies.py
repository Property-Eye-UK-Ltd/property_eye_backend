"""
Agency CRUD API endpoints.

Handles agency creation, retrieval, update, and deletion operations.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.api import deps
from src.db.session import get_db
from src.models.agency import Agency
from src.schemas.agency import (
    AgencyCreate,
    AgencyListResponse,
    AgencyResponse,
    AgencyStatsResponse,
    AgencyUpdate,
)
from src.services.agency_service import AgencyService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agencies", tags=["agencies"])


@router.post(
    "",
    response_model=AgencyResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new agency",
    description="Create a new real estate agency in the system.",
)
async def create_agency(
    agency_data: AgencyCreate,
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new agency.

    Args:
        agency_data: Agency creation data
        db: Database session

    Returns:
        Created agency details
    """
    logger.info(f"Creating new agency: {agency_data.name}")

    agency = await AgencyService.create_agency(db, agency_data)

    logger.info(f"Agency created successfully: {agency.id}")
    return agency


@router.get(
    "",
    response_model=AgencyListResponse,
    status_code=status.HTTP_200_OK,
    summary="List all agencies",
    description="Retrieve a paginated list of all agencies in the system.",
)
async def list_agencies(
    page: int = Query(1, ge=1, description="Page number (starts at 1)"),
    page_size: int = Query(50, ge=1, le=100, description="Number of items per page"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get paginated list of agencies.

    Args:
        page: Page number (1-indexed)
        page_size: Number of items per page
        db: Database session

    Returns:
        Paginated list of agencies
    """
    skip = (page - 1) * page_size
    agencies, total = await AgencyService.get_agencies(db, skip=skip, limit=page_size)

    logger.info(f"Retrieved {len(agencies)} agencies (page {page})")

    return AgencyListResponse(
        agencies=agencies,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get(
    "/stats",
    response_model=AgencyStatsResponse,
    status_code=status.HTTP_200_OK,
    summary="Get agency statistics",
    description="Get dashboard statistics for the current agency.",
)
async def get_agency_stats(
    current_agency: Agency = Depends(deps.get_current_agency),
    db: AsyncSession = Depends(get_db),
):
    """
    Get agency statistics for the authenticated user.
    """
    stats = await AgencyService.get_agency_stats(db, current_agency.id)
    return stats

@router.get(
    "/{agency_id}",
    response_model=AgencyResponse,
    status_code=status.HTTP_200_OK,
    summary="Get agency by ID",
    description="Retrieve detailed information about a specific agency.",
)
async def get_agency(
    agency_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Get agency by ID.

    Args:
        agency_id: Agency identifier
        db: Database session

    Returns:
        Agency details

    Raises:
        HTTPException: If agency not found
    """
    agency = await AgencyService.get_agency(db, agency_id)

    if not agency:
        logger.warning(f"Agency not found: {agency_id}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Agency with ID {agency_id} not found",
        )

    return agency


@router.patch(
    "/{agency_id}",
    response_model=AgencyResponse,
    status_code=status.HTTP_200_OK,
    summary="Update agency",
    description="Update an existing agency's information.",
)
async def update_agency(
    agency_id: str,
    agency_data: AgencyUpdate,
    db: AsyncSession = Depends(get_db),
):
    """
    Update an existing agency.

    Args:
        agency_id: Agency identifier
        agency_data: Agency update data
        db: Database session

    Returns:
        Updated agency details

    Raises:
        HTTPException: If agency not found
    """
    logger.info(f"Updating agency: {agency_id}")

    agency = await AgencyService.update_agency(db, agency_id, agency_data)

    if not agency:
        logger.warning(f"Agency not found for update: {agency_id}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Agency with ID {agency_id} not found",
        )

    logger.info(f"Agency updated successfully: {agency_id}")
    return agency


@router.delete(
    "/{agency_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete agency",
    description="Delete an agency and all associated property listings.",
)
async def delete_agency(
    agency_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Delete an agency.

    Args:
        agency_id: Agency identifier
        db: Database session

    Raises:
        HTTPException: If agency not found
    """
    logger.info(f"Deleting agency: {agency_id}")

    deleted = await AgencyService.delete_agency(db, agency_id)

    if not deleted:
        logger.warning(f"Agency not found for deletion: {agency_id}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Agency with ID {agency_id} not found",
        )

    logger.info(f"Agency deleted successfully: {agency_id}")

