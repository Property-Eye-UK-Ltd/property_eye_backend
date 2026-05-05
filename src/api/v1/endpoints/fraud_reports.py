"""
Fraud detection API endpoints (Stage 1).

Handles fraud detection scans to identify suspicious matches.
"""

import logging
import re
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.api import deps
from src.db.session import get_db
from src.models.agency import Agency
from src.schemas.fraud_report import (
    FraudMatchSchema,
    FraudReportGroupSchema,
    SuspiciousMatchSummary,
)
from src.services.address_normalizer import AddressNormalizer
from src.services.fraud_detector import FraudDetector
from src.services.ppd_service import PPDService
from lib.extractor.field_extractors import extract_property_number

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/fraud", tags=["fraud-detection"])

_RISK_PRIORITY = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}
_STATUS_FIELDS = ("suspicious", "confirmed_fraud", "not_fraud", "error")


def _format_postcode(postcode: str | None) -> str | None:
    """Normalize a UK postcode into standard spaced uppercase form."""
    if not postcode:
        return None
    compact = "".join(str(postcode).upper().split())
    if len(compact) < 5 or len(compact) > 7:
        return compact or None
    return f"{compact[:-3]} {compact[-3:]}"


def _derive_property_number(address: str | None, property_number: str | None) -> str | None:
    """Extract a stable house or plot number from the visible property address first."""
    extracted = extract_property_number(address or "")
    if extracted:
        return extracted.upper()
    fallback = str(property_number).strip().upper() if property_number else ""
    return fallback or None


def _build_street_signature(
    address: str | None,
    postcode: str | None,
    property_number: str | None,
    address_normalizer: AddressNormalizer,
) -> str:
    """Reduce an address to a normalized street signature for same-property grouping."""
    normalized = address_normalizer.normalize(address or "", postcode or "")
    formatted_postcode = _format_postcode(postcode)
    if formatted_postcode:
        normalized = normalized.replace(formatted_postcode, " ").strip()

    if property_number:
        number_pattern = re.escape(property_number)
        normalized = re.sub(
            rf"^(?:FLAT|APT|APARTMENT|UNIT|PLOT|NO)\s+{number_pattern}\b",
            "",
            normalized,
        ).strip()
        normalized = re.sub(rf"^{number_pattern}\b", "", normalized).strip()

    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _to_fraud_match_schema(match) -> FraudMatchSchema:
    """Convert ORM match + joined property listing into API schema."""
    return FraudMatchSchema(
        id=match.id,
        property_listing_id=match.property_listing_id,
        property_address=match.property_listing.address,
        client_name=match.property_listing.client_name,
        vendor_name=match.property_listing.vendor_name,
        withdrawn_date=match.property_listing.withdrawn_date,
        ppd_transaction_id=match.ppd_transaction_id,
        ppd_price=match.ppd_price,
        ppd_transfer_date=match.ppd_transfer_date,
        ppd_postcode=match.ppd_postcode,
        ppd_full_address=match.ppd_full_address,
        confidence_score=match.confidence_score,
        address_similarity=match.address_similarity,
        risk_level=match.risk_level,
        verification_status=match.verification_status,
        verified_owner_name=match.verified_owner_name,
        is_confirmed_fraud=match.is_confirmed_fraud,
        detected_at=match.detected_at,
        verified_at=match.verified_at,
    )


def _group_fraud_matches(matches) -> list[FraudReportGroupSchema]:
    """Group flat fraud matches into likely same-property resale clusters."""
    address_normalizer = AddressNormalizer()
    grouped: dict[str, dict] = {}

    for match in matches:
        listing = match.property_listing
        postcode = _format_postcode(listing.postcode or match.ppd_postcode)
        property_number = _derive_property_number(
            listing.address, listing.property_number
        )
        street_signature = _build_street_signature(
            listing.address,
            postcode,
            property_number,
            address_normalizer,
        )

        if postcode and property_number and street_signature:
            group_key = f"{postcode}|{property_number}|{street_signature}"
        else:
            group_key = listing.normalized_address or f"listing:{listing.id}"

        bucket = grouped.setdefault(
            group_key,
            {
                "group_key": group_key,
                "property_address": listing.address,
                "postcode": postcode,
                "property_number": property_number,
                "items": [],
            },
        )
        bucket["items"].append(_to_fraud_match_schema(match))

    groups: list[FraudReportGroupSchema] = []
    for bucket in grouped.values():
        items = sorted(
            bucket["items"],
            key=lambda item: (
                item.ppd_transfer_date or item.detected_at,
                item.detected_at,
            ),
            reverse=True,
        )

        status_counts = {status: 0 for status in _STATUS_FIELDS}
        for item in items:
            if item.verification_status in status_counts:
                status_counts[item.verification_status] += 1

        top_risk = max(
            (item.risk_level for item in items if item.risk_level),
            key=lambda level: _RISK_PRIORITY.get(level, 0),
            default=None,
        )

        latest_transfer_date = max(
            (item.ppd_transfer_date for item in items if item.ppd_transfer_date),
            default=None,
        )

        groups.append(
            FraudReportGroupSchema(
                group_key=bucket["group_key"],
                property_address=bucket["property_address"],
                postcode=bucket["postcode"],
                property_number=bucket["property_number"],
                total_matches=len(items),
                highest_confidence_score=max(
                    item.confidence_score for item in items
                ),
                risk_level=top_risk,
                latest_transfer_date=latest_transfer_date,
                suspicious_count=status_counts["suspicious"],
                confirmed_fraud_count=status_counts["confirmed_fraud"],
                cleared_count=status_counts["not_fraud"],
                error_count=status_counts["error"],
                items=items,
            )
        )

    return sorted(
        groups,
        key=lambda group: (
            group.latest_transfer_date or datetime.min,
            group.highest_confidence_score,
            group.property_address,
        ),
        reverse=True,
    )


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

        return [_to_fraud_match_schema(m) for m in matches]

    except Exception as e:
        logger.error(f"Error retrieving fraud reports: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve fraud reports: {str(e)}",
        )


@router.get(
    "/reports/grouped",
    response_model=list[FraudReportGroupSchema],
    status_code=status.HTTP_200_OK,
    summary="Get grouped fraud reports for agency",
    description="""
    Retrieve stored fraud matches grouped into likely same-property resale clusters.

    Grouping uses postcode plus extracted house or plot number, with a normalized
    street signature fallback so repeated resales are easier to review together.
    """,
)
async def get_grouped_fraud_reports(
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
    """Get grouped fraud reports for an agency with optional filtering."""
    from sqlalchemy import select
    from sqlalchemy.orm import joinedload
    from src.models.fraud_match import FraudMatch
    from src.models.property_listing import PropertyListing

    agency_id = current_agency.id
    logger.info(f"Retrieving grouped fraud reports for agency {agency_id}")

    try:
        stmt = (
            select(FraudMatch)
            .options(joinedload(FraudMatch.property_listing))
            .join(PropertyListing)
            .where(PropertyListing.agency_id == agency_id)
        )

        if min_confidence is not None:
            stmt = stmt.where(FraudMatch.confidence_score >= min_confidence)

        if verification_status:
            stmt = stmt.where(FraudMatch.verification_status == verification_status)

        stmt = stmt.offset(skip).limit(limit)

        result = await db.execute(stmt)
        matches = result.scalars().all()
        return _group_fraud_matches(matches)

    except Exception as e:
        logger.error(f"Error retrieving grouped fraud reports: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve grouped fraud reports: {str(e)}",
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
        return _to_fraud_match_schema(fraud_match)

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
