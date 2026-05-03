"""
Verification API endpoints (Stage 2).

Handles Land Registry verification of suspicious matches.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.api import deps
from src.db.session import get_db
from src.models.agency import Agency
from src.models.fraud_match import FraudMatch
from src.models.property_listing import PropertyListing
from src.schemas.verification import (
    VerificationRequest,
    VerificationResult,
    VerificationSummary,
)
from src.services.land_registry_client import LandRegistryClient
from src.services.verification_service import VerificationService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/verification", tags=["verification"])


@router.post(
    "/verify",
    response_model=VerificationSummary,
    status_code=status.HTTP_200_OK,
    summary="Verify suspicious matches (Stage 2)",
    description="""
    Execute Stage 2 verification: verify suspicious matches via Land Registry API.
    
    This endpoint processes selected suspicious matches (typically high-confidence ones)
    and calls the Land Registry API to verify current property ownership.
    
    The owner name from Land Registry is compared with the agency's client name using
    fuzzy matching (85% threshold). Matches are updated with verification status:
    - confirmed_fraud: Owner matches client (fraud confirmed)
    - not_fraud: Owner doesn't match client (ruled out)
    - error: API call failed or other error
    """,
)
async def verify_matches(
    request: VerificationRequest, db: AsyncSession = Depends(get_db)
):
    """
    Verify suspicious matches via Land Registry API.

    Args:
        request: VerificationRequest with match IDs
        db: Database session

    Returns:
        VerificationSummary with results
    """
    logger.info(f"Starting verification for {len(request.match_ids)} matches")

    try:
        # Initialize services
        land_registry_client = LandRegistryClient()
        verification_service = VerificationService(land_registry_client)

        # Execute verification
        summary = await verification_service.verify_suspicious_matches(
            request.match_ids, db
        )

        # Close Land Registry client
        await land_registry_client.close()

        return summary

    except Exception as e:
        logger.error(f"Error during verification: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Verification failed: {str(e)}",
        )


@router.get(
    "/status/{match_id}",
    response_model=VerificationResult,
    status_code=status.HTTP_200_OK,
    summary="Get verification status for a match",
    description="""
    Check the verification status of a specific fraud match.
    
    Returns the current verification status and details including:
    - Verification status (suspicious, confirmed_fraud, not_fraud, error)
    - Verified owner name (if verification completed)
    - Verification timestamp
    - Error message (if verification failed)
    """,
)
async def get_verification_status(
    match_id: str = Path(..., description="Fraud match ID"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get verification status for a specific match.

    Args:
        match_id: Fraud match ID
        db: Database session

    Returns:
        VerificationResult with status details
    """
    logger.info(f"Retrieving verification status for match {match_id}")

    try:
        # Retrieve match from database with property_listing eager-loaded for async
        stmt = (
            select(FraudMatch)
            .where(FraudMatch.id == match_id)
            .options(selectinload(FraudMatch.property_listing))
        )
        result = await db.execute(stmt)
        fraud_match = result.scalar_one_or_none()

        if not fraud_match:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Match {match_id} not found",
            )

        property_listing = fraud_match.property_listing

        return VerificationResult(
            match_id=match_id,
            property_address=property_listing.address,
            client_name=property_listing.client_name,
            vendor_name=property_listing.vendor_name,
            verification_status=fraud_match.verification_status,
            verified_owner_name=fraud_match.verified_owner_name,
            is_confirmed_fraud=fraud_match.is_confirmed_fraud,
            verified_at=fraud_match.verified_at or fraud_match.detected_at,
            error_message=None
            if fraud_match.verification_status != "error"
            else "Verification error",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving verification status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve verification status: {str(e)}",
        )


@router.post(
    "/verify-listing/{listing_id}",
    response_model=VerificationSummary,
    status_code=status.HTTP_200_OK,
    summary="Manually verify a property listing via HMLR (direct)",
    description="""
    Directly calls HMLR Online Owner Verification for a single property listing.

    This bypasses PPD screening and fraud matches so the HMLR integration can be
    tested directly from the listings page.

    Returns a VerificationSummary containing a single direct verification result.
    """,
)
async def verify_listing(
    listing_id: str = Path(..., description="Property listing ID"),
    current_agency: Agency = Depends(deps.get_current_agency),
    db: AsyncSession = Depends(get_db),
):
    """
    Verify a single property listing directly via HMLR.
    """
    agency_id = current_agency.id
    logger.info(
        f"Manual HMLR verification requested for listing {listing_id} by agency {agency_id}"
    )

    try:
        # 1. Fetch the listing and verify it belongs to this agency
        listing_stmt = (
            select(PropertyListing)
            .where(PropertyListing.id == listing_id)
            .where(PropertyListing.agency_id == agency_id)
        )
        listing_result = await db.execute(listing_stmt)
        listing = listing_result.scalar_one_or_none()

        if not listing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Property listing {listing_id} not found or access denied",
            )

        # Direct HMLR verification for the listing only (no PPD scan).
        land_registry_client = LandRegistryClient()
        verification_service = VerificationService(land_registry_client)
        result = await verification_service.verify_listing_direct(listing)
        await land_registry_client.close()

        confirmed_fraud_count = 1 if result.verification_status == "confirmed_fraud" else 0
        not_fraud_count = 1 if result.verification_status == "not_fraud" else 0
        error_count = 1 if result.verification_status == "error" else 0

        return VerificationSummary(
            total_verified=1,
            confirmed_fraud_count=confirmed_fraud_count,
            not_fraud_count=not_fraud_count,
            error_count=error_count,
            results=[result],
            message="Direct HMLR verification complete.",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error during manual listing verification: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Verification failed: {str(e)}",
        )
