"""OC with Summary endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from src.api import deps
from src.db.session import get_db
from src.models.fraud_match import FraudMatch
from src.models.property_listing import PropertyListing
from src.schemas.oc_with_summary import OCWithSummaryResponseSchema
from src.services.oc_with_summary_service import OCWithSummaryService

router = APIRouter(prefix="/fraud-reports", tags=["oc-with-summary"])


async def _get_authorized_fraud_match(
    report_id: str,
    current_agency,
    db: AsyncSession,
) -> FraudMatch:
    stmt = (
        select(FraudMatch)
        .options(
            joinedload(FraudMatch.property_listing).joinedload(PropertyListing.agency),
            joinedload(FraudMatch.oc_with_summary),
        )
        .join(PropertyListing)
        .where(FraudMatch.id == report_id)
        .where(PropertyListing.agency_id == current_agency.id)
    )
    result = await db.execute(stmt)
    fraud_match = result.scalar_one_or_none()
    if not fraud_match:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Fraud report {report_id} not found or access denied",
        )
    return fraud_match


@router.post(
    "/{report_id}/oc-with-summary",
    response_model=OCWithSummaryResponseSchema,
    status_code=status.HTTP_200_OK,
    summary="Request Official Copy with Summary",
)
async def request_oc_with_summary(
    report_id: str,
    current_agency=Depends(deps.get_current_agency),
    db: AsyncSession = Depends(get_db),
):
    fraud_match = await _get_authorized_fraud_match(report_id, current_agency, db)
    service = OCWithSummaryService()
    try:
        payload = await service.get_or_fetch(report_id=report_id, db=db)
        if payload.status == "pending":
            return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=payload.model_dump(mode="json"))
        return payload
    except ValueError as exc:
        message = str(exc)
        if message.startswith("Title number is required"):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=message) from exc
        if message.startswith("Rejection") or ": " in message:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message) from exc
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=message) from exc


@router.get(
    "/{report_id}/oc-with-summary",
    response_model=OCWithSummaryResponseSchema,
    status_code=status.HTTP_200_OK,
    summary="Get cached Official Copy with Summary",
)
async def get_cached_oc_with_summary(
    report_id: str,
    current_agency=Depends(deps.get_current_agency),
    db: AsyncSession = Depends(get_db),
):
    fraud_match = await _get_authorized_fraud_match(report_id, current_agency, db)
    cached = fraud_match.oc_with_summary
    if cached and cached.status == "complete" and cached.parsed_json:
        return OCWithSummaryResponseSchema.model_validate(cached.parsed_json)
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="No completed Official Copy with Summary is cached for this report.",
    )


@router.get(
    "/poll/{poll_id}",
    response_model=OCWithSummaryResponseSchema,
    status_code=status.HTTP_200_OK,
    summary="Poll Official Copy with Summary status",
)
async def poll_oc_with_summary(
    poll_id: str,
    current_agency=Depends(deps.get_current_agency),
    db: AsyncSession = Depends(get_db),
):
    service = OCWithSummaryService()
    record = await service._get_record_by_poll_id(poll_id, db)
    if not record.fraud_match or record.fraud_match.property_listing.agency_id != current_agency.id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Poll ID {poll_id} not found or access denied",
        )
    try:
        payload = await service.poll(poll_id=poll_id, db=db)
        if payload.status == "pending":
            return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=payload.model_dump(mode="json"))
        return payload
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
