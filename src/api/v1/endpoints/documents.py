"""
Document upload API endpoints.

Handles agency document uploads with field mapping and validation.
"""

import json
import logging
import os
import pandas as pd
import uuid
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List

from fastapi import APIRouter, Body, Depends, File, HTTPException, UploadFile, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api import deps
from src.db.session import get_db
from src.models.agency import Agency
from src.models.property_listing import PropertyListing
from src.schemas.document_upload import DocumentUploadResponse, ListingsIngestRequest
from src.services.address_normalizer import AddressNormalizer
from src.utils.constants import config

from lib.extractor import extract_structured
from lib.extractor.ai_agent import run_agent
from lib.extractor.column_mapper import interpret as map_interpret

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/documents", tags=["documents"])


def parse_date(date_value) -> datetime.date:
    """
    Parse date from various formats to datetime.date object.

    Args:
        date_value: Date value (string, datetime, or date object)

    Returns:
        datetime.date object or None
    """
    if pd.isna(date_value) or date_value is None:
        return None

    if isinstance(date_value, datetime):
        return date_value.date()

    if isinstance(date_value, pd.Timestamp):
        return date_value.date()

    if isinstance(date_value, str):
        # Try common date formats
        for fmt in [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y/%m/%d",
            "%d %B %Y",
            "%d %b %Y",
        ]:
            try:
                return datetime.strptime(date_value, fmt).date()
            except ValueError:
                continue
        # If none work, raise error
        raise ValueError(f"Unable to parse date: {date_value}")

    return date_value


def _coerce_str(value: Any) -> str | None:
    """Convert a value to cleaned string or None for empty values."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def _normalise_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Map incoming dict payloads to canonical extractor field names."""
    aliases = {
        "clientName": "client_name",
        "vendorName": "vendor_name",
        "propertyNumber": "property_number",
        "titleNumber": "title_number",
        "withdrawnDate": "withdrawn_date",
        "contractDuration": "contract_duration",
    }
    out: Dict[str, Any] = {}
    for k, v in record.items():
        key = aliases.get(k, k)
        out[key] = v
    return out


def _extract_rows_from_nested_lists(rows: List[Any]) -> List[Dict[str, Any]]:
    """Convert nested row payloads (list[list|dict]) into canonical record dicts."""
    if not rows:
        return []

    # If caller sends list-of-dicts, trust keys and normalize aliases.
    if isinstance(rows[0], dict):
        return [_normalise_record(dict(r)) for r in rows if isinstance(r, dict)]

    # If caller sends nested lists, infer mapping from first row then map all rows.
    if isinstance(rows[0], list):
        raw_rows = [r for r in rows if isinstance(r, list)]
        if not raw_rows:
            return []
        mapping = run_agent(sample_row=raw_rows[0])
        return [map_interpret(mapping, r) for r in raw_rows]

    raise ValueError("rows must be list[dict] or list[list]")


def _listing_response_payload(listing: PropertyListing) -> Dict[str, Any]:
    """Build a consistent listing response object for frontend rendering."""
    return {
        "id": listing.id,
        "address": listing.address,
        "postcode": listing.postcode,
        "region": listing.region,
        "county": listing.county,
        "property_number": listing.property_number,
        "title_number": listing.title_number,
        "client_name": listing.client_name,
        "vendor_name": listing.vendor_name,
        "status": listing.status,
        "withdrawn_date": listing.withdrawn_date,
        "price": listing.price,
        "commission": listing.commission,
        "contract_duration": listing.contract_duration,
        "created_at": listing.created_at,
    }


@router.post(
    "/upload",
    response_model=DocumentUploadResponse,
    status_code=status.HTTP_200_OK,
    summary="Upload agency document (CSV/PDF)",
    description="""
    Upload an agency property listing document (CSV or PDF).
    Field mapping is automatic via the extractor pipeline.
    Duplicate records (same normalized address) are skipped.
    """,
)
async def upload_document(
    file: UploadFile = File(..., description="Document file (CSV, Excel, or PDF)"),
    current_agency: Agency = Depends(deps.get_current_agency),
    db: AsyncSession = Depends(get_db),
):
    """Upload and process a CSV/PDF with automatic extraction and mapping."""
    agency_id = current_agency.id
    logger.info(f"Received document upload for agency {agency_id}")

    # Save uploaded file temporarily
    file_extension = Path(file.filename).suffix

    if file_extension.lower() not in [".csv", ".pdf"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported file type: {file_extension}. Allowed: {', '.join(config.ALLOWED_UPLOAD_EXTENSIONS)}",
        )

    try:
        with NamedTemporaryFile(delete=False, suffix=file_extension) as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name

        # Parse with the AI extractor pipeline.
        rows = extract_structured(temp_file_path)
        logger.info(f"Parsed {len(rows)} records from extractor pipeline")

        # Process and store records
        address_normalizer = AddressNormalizer()
        records_processed = 0
        records_skipped = 0

        for row in rows:
            row = _normalise_record(row)
            address = _coerce_str(row.get("address"))
            postcode = _coerce_str(row.get("postcode"))
            if not address:
                records_skipped += 1
                continue

            # Normalize address
            normalized_address = address_normalizer.normalize(
                address, postcode or ""
            )

            # Check for duplicates
            stmt = select(PropertyListing).where(
                PropertyListing.agency_id == agency_id,
                PropertyListing.normalized_address == normalized_address,
            )
            result = await db.execute(stmt)
            existing = result.scalar_one_or_none()

            if existing:
                records_skipped += 1
                continue

            # Parse withdrawn_date to proper date object
            withdrawn_date_raw = row.get("withdrawn_date")
            withdrawn_date = parse_date(withdrawn_date_raw)

            # HEURISTIC: If withdrawn_date is present, the status is "withdrawn"
            status_val = _coerce_str(row.get("status"))
            if not status_val and withdrawn_date:
                status_val = "withdrawn"

            # Create new property listing
            property_listing = PropertyListing(
                agency_id=agency_id,
                address=address,
                normalized_address=normalized_address,
                postcode=postcode,
                region=_coerce_str(row.get("region")),
                county=_coerce_str(row.get("county")),
                property_number=_coerce_str(row.get("property_number")),
                title_number=_coerce_str(row.get("title_number")),
                client_name=_coerce_str(row.get("client_name")),
                vendor_name=_coerce_str(row.get("vendor_name")),
                status=(status_val or "").lower(),
                withdrawn_date=withdrawn_date,
                price=_coerce_str(row.get("price")),
                commission=_coerce_str(row.get("commission")),
                contract_duration=_coerce_str(row.get("contract_duration")),
            )

            db.add(property_listing)
            records_processed += 1

        await db.commit()

        # Clean up temp file
        Path(temp_file_path).unlink()

        upload_id = str(uuid.uuid4())

        logger.info(
            f"Upload complete: {records_processed} processed, {records_skipped} skipped"
        )

        return DocumentUploadResponse(
            upload_id=upload_id,
            status="success",
            records_processed=records_processed,
            records_skipped=records_skipped,
            message="Document processed successfully",
        )

    except ValueError as e:
        await db.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Error processing document: {str(e)}")
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process document: {str(e)}",
        )


@router.post(
    "/ingest",
    response_model=DocumentUploadResponse,
    status_code=status.HTTP_200_OK,
    summary="Ingest listings payload (nested-list or manual dict)",
)
async def ingest_listings_payload(
    payload: ListingsIngestRequest = Body(...),
    current_agency: Agency = Depends(deps.get_current_agency),
    db: AsyncSession = Depends(get_db),
):
    """Ingest listings from JSON payloads (rows list and/or single manual record)."""
    agency_id = current_agency.id
    logger.info("Received JSON listing ingest for agency %s", agency_id)

    if not payload.rows and not payload.record:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide at least one of: rows or record",
        )

    try:
        extracted_rows: List[Dict[str, Any]] = []
        if payload.rows:
            extracted_rows.extend(_extract_rows_from_nested_lists(payload.rows))
        if payload.record:
            extracted_rows.append(_normalise_record(payload.record))

        address_normalizer = AddressNormalizer()
        records_processed = 0
        records_skipped = 0

        for row in extracted_rows:
            address = _coerce_str(row.get("address"))
            postcode = _coerce_str(row.get("postcode"))
            if not address:
                records_skipped += 1
                continue

            normalized_address = address_normalizer.normalize(address, postcode or "")
            stmt = select(PropertyListing).where(
                PropertyListing.agency_id == agency_id,
                PropertyListing.normalized_address == normalized_address,
            )
            result = await db.execute(stmt)
            existing = result.scalar_one_or_none()
            if existing:
                records_skipped += 1
                continue

            # Parse withdrawn_date
            withdrawn_date = parse_date(row.get("withdrawn_date"))

            # HEURISTIC: If withdrawn_date is present, the status is "withdrawn"
            status_val = _coerce_str(row.get("status"))
            if not status_val and withdrawn_date:
                status_val = "withdrawn"

            listing = PropertyListing(
                agency_id=agency_id,
                address=address,
                normalized_address=normalized_address,
                postcode=postcode,
                region=_coerce_str(row.get("region")),
                county=_coerce_str(row.get("county")),
                property_number=_coerce_str(row.get("property_number")),
                title_number=_coerce_str(row.get("title_number")),
                client_name=_coerce_str(row.get("client_name")),
                vendor_name=_coerce_str(row.get("vendor_name")),
                status=(status_val or "").lower(),
                withdrawn_date=withdrawn_date,
                price=_coerce_str(row.get("price")),
                commission=_coerce_str(row.get("commission")),
                contract_duration=_coerce_str(row.get("contract_duration")),
            )
            db.add(listing)
            records_processed += 1

        await db.commit()
        return DocumentUploadResponse(
            upload_id=str(uuid.uuid4()),
            status="success",
            records_processed=records_processed,
            records_skipped=records_skipped,
            message="Listings ingested successfully",
        )
    except ValueError as e:
        await db.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        await db.rollback()
        logger.error("Error ingesting listings payload: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to ingest listings: {str(e)}",
        )


@router.get(
    "/listings",
    response_model=list[dict],
    status_code=status.HTTP_200_OK,
    summary="Get uploaded listings",
    description="Get all property listings uploaded by the current agency.",
)
async def get_uploaded_listings(
    current_agency: Agency = Depends(deps.get_current_agency),
    db: AsyncSession = Depends(get_db),
):
    """
    Get all uploaded listings for the current agency.
    """
    stmt = (
        select(PropertyListing)
        .where(PropertyListing.agency_id == current_agency.id)
        .order_by(PropertyListing.created_at.desc())
    )

    result = await db.execute(stmt)
    listings = result.scalars().all()

    return [_listing_response_payload(l) for l in listings]


@router.patch(
    "/listings/{listing_id}",
    response_model=dict,
    status_code=status.HTTP_200_OK,
    summary="Update a property listing",
    description="Update specific fields of a property listing record.",
)
async def update_listing(
    listing_id: str,
    update_data: dict,
    current_agency: Agency = Depends(deps.get_current_agency),
    db: AsyncSession = Depends(get_db),
):
    """
    Update a property listing record.

    Args:
        listing_id: Property listing ID to update
        update_data: Dictionary of fields to update
        current_agency: Authenticated agency
        db: Database session

    Returns:
        Updated property listing object
    """
    agency_id = current_agency.id
    logger.info(f"Updating listing {listing_id} for agency {agency_id}")

    try:
        # Get the listing and verify ownership
        stmt = select(PropertyListing).where(
            PropertyListing.id == listing_id, PropertyListing.agency_id == agency_id
        )

        result = await db.execute(stmt)
        listing = result.scalar_one_or_none()

        if not listing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Listing {listing_id} not found or access denied",
            )

        # Update allowed fields
        allowed_fields = {
            "address",
            "postcode",
            "region",
            "county",
            "property_number",
            "title_number",
            "client_name",
            "vendor_name",
            "status",
            "withdrawn_date",
            "price",
            "commission",
            "contract_duration",
        }

        address_normalizer = AddressNormalizer()

        for field, value in update_data.items():
            if field in allowed_fields and hasattr(listing, field):
                if field == "withdrawn_date" and value:
                    # Parse date if it's a string
                    setattr(listing, field, parse_date(value))
                else:
                    setattr(listing, field, value)

        # Re-normalize address if address or postcode changed
        if "address" in update_data or "postcode" in update_data:
            listing.normalized_address = address_normalizer.normalize(
                listing.address, listing.postcode
            )

        await db.commit()
        await db.refresh(listing)

        return _listing_response_payload(listing)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating listing: {str(e)}")
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update listing: {str(e)}",
        )


@router.delete(
    "/listings/{listing_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a property listing",
    description="Delete a property listing record and all associated fraud matches.",
)
async def delete_listing(
    listing_id: str,
    current_agency: Agency = Depends(deps.get_current_agency),
    db: AsyncSession = Depends(get_db),
):
    """
    Delete a property listing record.

    Args:
        listing_id: Property listing ID to delete
        current_agency: Authenticated agency
        db: Database session

    Returns:
        None (204 No Content)
    """
    from sqlalchemy import delete as sql_delete

    agency_id = current_agency.id
    logger.info(f"Deleting listing {listing_id} for agency {agency_id}")

    try:
        # Verify ownership before deletion
        stmt = select(PropertyListing).where(
            PropertyListing.id == listing_id, PropertyListing.agency_id == agency_id
        )

        result = await db.execute(stmt)
        listing = result.scalar_one_or_none()

        if not listing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Listing {listing_id} not found or access denied",
            )

        # Delete the record (cascade will handle fraud_matches)
        delete_stmt = sql_delete(PropertyListing).where(
            PropertyListing.id == listing_id
        )
        await db.execute(delete_stmt)
        await db.commit()

        logger.info(f"Successfully deleted listing {listing_id}")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting listing: {str(e)}")
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete listing: {str(e)}",
        )
