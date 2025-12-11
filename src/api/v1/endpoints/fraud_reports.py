"""
Fraud detection API endpoints (Stage 1).

Handles fraud detection scans to identify suspicious matches.
"""

import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.api import deps
from src.db.session import get_db
from src.models.agency import Agency
from src.schemas.fraud_report import FraudMatchSchema, SuspiciousMatchSummary
from src.services.address_normalizer import AddressNormalizer
from src.services.fraud_detector import FraudDetector
from src.services.ppd_service import PPDService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/fraud", tags=["fraud-detection"])


@router.post(
    "/scan",
    response_model=SuspiciousMatchSummary,
    status_code=status.HTTP_200_OK,
    summary="Scan for suspicious fraud matches (Stage 1)",
    description="""
    Execute Stage 1 fraud detection: scan withdrawn properties against PPD data.
    
    This endpoint identifies suspicious matches based on address similarity and
    confidence scoring WITHOUT making Land Registry API calls.
    
    All matches above the minimum confidence threshold are stored with status="suspicious"
    and returned in the response for review.
    
    High-confidence matches (>= 85%) should be selected for Stage 2 verification.
    """,
)
async def scan_for_fraud(
    current_agency: Agency = Depends(deps.get_current_agency),
    db: AsyncSession = Depends(get_db),
):
    """
    Execute Stage 1 fraud detection scan.

    Args:
        current_agency: Authenticated agency
        db: Database session

    Returns:
        SuspiciousMatchSummary with all detected matches
    """
    agency_id = current_agency.id
    logger.info(f"Starting fraud scan for agency {agency_id}")

    try:
        # Initialize services
        ppd_service = PPDService()
        address_normalizer = AddressNormalizer()
        fraud_detector = FraudDetector(ppd_service, address_normalizer)

        # Execute fraud detection
        summary = await fraud_detector.detect_suspicious_matches(agency_id, db)

        return summary

    except Exception as e:
        logger.error(f"Error during fraud scan: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Fraud scan failed: {str(e)}",
        )


@router.get(
    "/reports",
    response_model=list[FraudMatchSchema],
    status_code=status.HTTP_200_OK,
    summary="Get fraud reports for agency",
    description="""
    Retrieve stored fraud matches for an agency.
    
    Supports filtering by confidence score and verification status.
    Results are paginated for large datasets.
    """,
)
async def get_fraud_reports(
    min_confidence: float = Query(None, description="Minimum confidence score filter"),
    verification_status: str = Query(
        None,
        description="Filter by status: suspicious, confirmed_fraud, not_fraud, error",
    ),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return"),
    current_agency: Agency = Depends(deps.get_current_agency),
    db: AsyncSession = Depends(get_db),
):
    """
    Get fraud reports for an agency with optional filtering.

    Args:
        min_confidence: Minimum confidence score filter
        verification_status: Verification status filter
        skip: Pagination offset
        limit: Pagination limit
        current_agency: Authenticated agency
        db: Database session

    Returns:
        List of FraudMatchSchema objects
    """
    from sqlalchemy import select
    from sqlalchemy.orm import joinedload
    from src.models.fraud_match import FraudMatch
    from src.models.property_listing import PropertyListing

    agency_id = current_agency.id
    logger.info(f"Retrieving fraud reports for agency {agency_id}")

    try:
        # Build query
        stmt = (
            select(FraudMatch)
            .options(joinedload(FraudMatch.property_listing))
            .join(PropertyListing)
            .where(PropertyListing.agency_id == agency_id)
        )

        # Apply filters
        if min_confidence is not None:
            stmt = stmt.where(FraudMatch.confidence_score >= min_confidence)

        if verification_status:
            stmt = stmt.where(FraudMatch.verification_status == verification_status)

        # Apply pagination
        stmt = stmt.offset(skip).limit(limit)

        # Execute query
        result = await db.execute(stmt)
        matches = result.scalars().all()

        # Convert to schemas
        match_schemas = [
            FraudMatchSchema(
                id=m.id,
                property_listing_id=m.property_listing_id,
                property_address=m.property_listing.address,
                client_name=m.property_listing.client_name,
                withdrawn_date=m.property_listing.withdrawn_date,
                ppd_transaction_id=m.ppd_transaction_id,
                ppd_price=m.ppd_price,
                ppd_transfer_date=m.ppd_transfer_date,
                ppd_postcode=m.ppd_postcode,
                ppd_full_address=m.ppd_full_address,
                confidence_score=m.confidence_score,
                address_similarity=m.address_similarity,
                verification_status=m.verification_status,
                verified_owner_name=m.verified_owner_name,
                is_confirmed_fraud=m.is_confirmed_fraud,
                detected_at=m.detected_at,
                verified_at=m.verified_at,
            )
            for m in matches
        ]

        return match_schemas

    except Exception as e:
        logger.error(f"Error retrieving fraud reports: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve fraud reports: {str(e)}",
        )


@router.patch(
    "/reports/{report_id}",
    response_model=FraudMatchSchema,
    status_code=status.HTTP_200_OK,
    summary="Update a fraud report",
    description="Update specific fields of a fraud report record.",
)
async def update_fraud_report(
    report_id: str,
    update_data: dict,
    current_agency: Agency = Depends(deps.get_current_agency),
    db: AsyncSession = Depends(get_db),
):
    """
    Update a fraud report record.

    Args:
        report_id: Fraud report ID to update
        update_data: Dictionary of fields to update
        current_agency: Authenticated agency
        db: Database session

    Returns:
        Updated FraudMatchSchema object
    """
    from sqlalchemy import select
    from sqlalchemy.orm import joinedload
    from src.models.fraud_match import FraudMatch
    from src.models.property_listing import PropertyListing

    agency_id = current_agency.id
    logger.info(f"Updating fraud report {report_id} for agency {agency_id}")

    try:
        # Get the fraud match and verify ownership
        stmt = (
            select(FraudMatch)
            .options(joinedload(FraudMatch.property_listing))
            .join(PropertyListing)
            .where(FraudMatch.id == report_id)
            .where(PropertyListing.agency_id == agency_id)
        )

        result = await db.execute(stmt)
        fraud_match = result.scalar_one_or_none()

        if not fraud_match:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Fraud report {report_id} not found or access denied",
            )

        # Update allowed fields
        allowed_fields = {
            "verification_status",
            "verified_owner_name",
            "is_confirmed_fraud",
        }

        for field, value in update_data.items():
            if field in allowed_fields and hasattr(fraud_match, field):
                setattr(fraud_match, field, value)

        if "verification_status" in update_data or "is_confirmed_fraud" in update_data:
            fraud_match.verified_at = datetime.utcnow()

        await db.commit()
        await db.refresh(fraud_match)

        # Return updated schema
        return FraudMatchSchema(
            id=fraud_match.id,
            property_listing_id=fraud_match.property_listing_id,
            property_address=fraud_match.property_listing.address,
            client_name=fraud_match.property_listing.client_name,
            withdrawn_date=fraud_match.property_listing.withdrawn_date,
            ppd_transaction_id=fraud_match.ppd_transaction_id,
            ppd_price=fraud_match.ppd_price,
            ppd_transfer_date=fraud_match.ppd_transfer_date,
            ppd_postcode=fraud_match.ppd_postcode,
            ppd_full_address=fraud_match.ppd_full_address,
            confidence_score=fraud_match.confidence_score,
            address_similarity=fraud_match.address_similarity,
            verification_status=fraud_match.verification_status,
            verified_owner_name=fraud_match.verified_owner_name,
            is_confirmed_fraud=fraud_match.is_confirmed_fraud,
            detected_at=fraud_match.detected_at,
            verified_at=fraud_match.verified_at,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating fraud report: {str(e)}")
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update fraud report: {str(e)}",
        )


@router.delete(
    "/reports/{report_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a fraud report",
    description="Delete a fraud report record.",
)
async def delete_fraud_report(
    report_id: str,
    current_agency: Agency = Depends(deps.get_current_agency),
    db: AsyncSession = Depends(get_db),
):
    """
    Delete a fraud report record.

    Args:
        report_id: Fraud report ID to delete
        current_agency: Authenticated agency
        db: Database session

    Returns:
        None (204 No Content)
    """
    from sqlalchemy import select, delete
    from src.models.fraud_match import FraudMatch
    from src.models.property_listing import PropertyListing

    agency_id = current_agency.id
    logger.info(f"Deleting fraud report {report_id} for agency {agency_id}")

    try:
        # Verify ownership before deletion
        stmt = (
            select(FraudMatch)
            .join(PropertyListing)
            .where(FraudMatch.id == report_id)
            .where(PropertyListing.agency_id == agency_id)
        )

        result = await db.execute(stmt)
        fraud_match = result.scalar_one_or_none()

        if not fraud_match:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Fraud report {report_id} not found or access denied",
            )

        # Delete the record
        delete_stmt = delete(FraudMatch).where(FraudMatch.id == report_id)
        await db.execute(delete_stmt)
        await db.commit()

        logger.info(f"Successfully deleted fraud report {report_id}")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting fraud report: {str(e)}")
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete fraud report: {str(e)}",
        )
