"""
Fraud detection API endpoints (Stage 1).

Handles fraud detection scans to identify suspicious matches.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import get_db
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
    agency_id: str = Query(..., description="Agency identifier to scan"),
    db: AsyncSession = Depends(get_db),
):
    """
    Execute Stage 1 fraud detection scan.

    Args:
        agency_id: Agency identifier
        db: Database session

    Returns:
        SuspiciousMatchSummary with all detected matches
    """
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
    "/reports/{agency_id}",
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
    agency_id: str,
    min_confidence: float = Query(None, description="Minimum confidence score filter"),
    verification_status: str = Query(
        None,
        description="Filter by status: suspicious, confirmed_fraud, not_fraud, error",
    ),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get fraud reports for an agency with optional filtering.

    Args:
        agency_id: Agency identifier
        min_confidence: Minimum confidence score filter
        verification_status: Verification status filter
        skip: Pagination offset
        limit: Pagination limit
        db: Database session

    Returns:
        List of FraudMatchSchema objects
    """
    from sqlalchemy import select
    from src.models.fraud_match import FraudMatch
    from src.models.property_listing import PropertyListing

    logger.info(f"Retrieving fraud reports for agency {agency_id}")

    try:
        # Build query
        stmt = (
            select(FraudMatch)
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
