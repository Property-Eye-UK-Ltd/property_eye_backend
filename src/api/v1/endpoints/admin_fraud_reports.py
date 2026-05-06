"""Admin fraud-case endpoints for cross-agency review and RES retrieval."""

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import func, inspect, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from src.db.session import get_db
from src.models.agency import Agency
from src.models.fraud_match import FraudMatch
from src.models.property_listing import PropertyListing
from src.schemas.register_extract import (
    AdminFraudAgencyFilterSchema,
    AdminFraudCaseDetailSchema,
    AdminFraudCaseListItemSchema,
    AdminFraudCaseListResponseSchema,
    RegisterExtractResponseSchema,
)
from src.services.register_extract_service import RegisterExtractService

router = APIRouter(prefix="/admin/fraud-reports", tags=["admin-fraud-reports"])


async def _has_register_extract_table(db: AsyncSession) -> bool:
    """Check whether the register_extracts table exists in the current database."""
    return await db.run_sync(
        lambda session: inspect(session.get_bind()).has_table("register_extracts")
    )


def _to_admin_case(match: FraudMatch) -> AdminFraudCaseListItemSchema:
    listing = match.property_listing
    agency = listing.agency
    register_extract = match.register_extract
    return AdminFraudCaseListItemSchema(
        id=match.id,
        property_listing_id=listing.id,
        agency_id=agency.id,
        agency_name=agency.name,
        property_address=listing.address,
        title_number=listing.title_number,
        client_name=listing.client_name,
        vendor_name=listing.vendor_name,
        price=listing.price,
        postcode=listing.postcode,
        withdrawn_date=listing.withdrawn_date,
        confidence_score=match.confidence_score,
        risk_level=match.risk_level,
        verification_status=match.verification_status,
        verified_owner_name=match.verified_owner_name,
        is_confirmed_fraud=match.is_confirmed_fraud,
        detected_at=match.detected_at,
        verified_at=match.verified_at,
        register_extract_status=register_extract.status if register_extract else None,
        register_extract_fetched_at=register_extract.fetched_at if register_extract else None,
    )


@router.get("", response_model=AdminFraudCaseListResponseSchema)
async def list_admin_fraud_reports(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    search: str | None = Query(None),
    status: str | None = Query(None),
    agency_id: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    has_register_extract = await _has_register_extract_table(db)

    stmt = (
        select(FraudMatch)
        .join(PropertyListing)
        .join(Agency)
        .options(
            joinedload(FraudMatch.property_listing).joinedload(PropertyListing.agency),
            *( [joinedload(FraudMatch.register_extract)] if has_register_extract else [] ),
        )
        .order_by(FraudMatch.detected_at.desc())
    )
    count_stmt = (
        select(func.count(FraudMatch.id))
        .select_from(FraudMatch)
        .join(PropertyListing)
        .join(Agency)
    )

    filters = []
    if search:
        term = f"%{search.strip()}%"
        filters.append(
            or_(
                PropertyListing.address.ilike(term),
                Agency.name.ilike(term),
                PropertyListing.vendor_name.ilike(term),
            )
        )
    if status and status != "all":
        filters.append(FraudMatch.verification_status == status)
    if agency_id and agency_id != "all":
        filters.append(PropertyListing.agency_id == agency_id)

    for filter_expr in filters:
        stmt = stmt.where(filter_expr)
        count_stmt = count_stmt.where(filter_expr)

    total_result = await db.execute(count_stmt)
    total = int(total_result.scalar() or 0)

    result = await db.execute(stmt.offset((page - 1) * page_size).limit(page_size))
    items = result.scalars().unique().all()
    return AdminFraudCaseListResponseSchema(
        items=[_to_admin_case(item) for item in items],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/agencies", response_model=list[AdminFraudAgencyFilterSchema])
async def list_admin_fraud_agencies(
    db: AsyncSession = Depends(get_db),
):
    stmt = (
        select(Agency.id, Agency.name)
        .join(PropertyListing, PropertyListing.agency_id == Agency.id)
        .join(FraudMatch, FraudMatch.property_listing_id == PropertyListing.id)
        .group_by(Agency.id, Agency.name)
        .order_by(Agency.name.asc())
    )
    result = await db.execute(stmt)
    return [
        AdminFraudAgencyFilterSchema(id=agency_id, name=name)
        for agency_id, name in result.all()
    ]


@router.get("/{report_id}", response_model=AdminFraudCaseDetailSchema)
async def get_admin_fraud_report(
    report_id: str,
    db: AsyncSession = Depends(get_db),
):
    has_register_extract = await _has_register_extract_table(db)

    stmt = (
        select(FraudMatch)
        .options(
            joinedload(FraudMatch.property_listing).joinedload(PropertyListing.agency),
            *( [joinedload(FraudMatch.register_extract)] if has_register_extract else [] ),
        )
        .where(FraudMatch.id == report_id)
    )
    result = await db.execute(stmt)
    match = result.scalar_one_or_none()
    if not match:
        raise HTTPException(status_code=404, detail=f"Fraud report {report_id} not found")

    base = _to_admin_case(match).model_dump()
    return AdminFraudCaseDetailSchema(
        **base,
        ppd_transaction_id=match.ppd_transaction_id,
        ppd_price=match.ppd_price,
        ppd_transfer_date=match.ppd_transfer_date,
        ppd_postcode=match.ppd_postcode,
        ppd_full_address=match.ppd_full_address,
        address_similarity=match.address_similarity,
        land_registry_response=match.land_registry_response,
    )


@router.get(
    "/{report_id}/register-extract",
    response_model=RegisterExtractResponseSchema,
)
async def get_register_extract(
    report_id: str,
    mock: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    service = RegisterExtractService()
    try:
        return await service.get_or_fetch(report_id=report_id, db=db, mock=mock)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{report_id}/register-extract/pdf")
async def download_register_extract_pdf(
    report_id: str,
    mock: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    service = RegisterExtractService()
    try:
        pdf_bytes, filename = await service.get_pdf_bytes(
            report_id=report_id,
            db=db,
            mock=mock,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return StreamingResponse(
        iter([pdf_bytes]),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
