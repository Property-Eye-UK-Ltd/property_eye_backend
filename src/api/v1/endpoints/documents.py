"""
Document upload API endpoints.

Handles agency document uploads with field mapping and validation.
"""

import logging
import pandas as pd
import uuid
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import get_db
from src.models.agency import Agency
from src.models.property_listing import PropertyListing
from src.schemas.document_upload import DocumentUploadResponse
from src.services.address_normalizer import AddressNormalizer
from src.services.document_parser import DocumentParser
from src.utils.constants import config

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/documents", tags=["documents"])


@router.post(
    "/upload",
    response_model=DocumentUploadResponse,
    status_code=status.HTTP_200_OK,
    summary="Upload agency document",
    description="""
    Upload an agency property listing document (CSV, Excel, or PDF).
    
    The document must include all required fields mapped via the field_mapping parameter.
    Duplicate records (same address and listing date) will be skipped.
    
    Required fields: address, client_name, status, withdrawn_date, postcode
    """,
)
async def upload_document(
    agency_id: str = Form(..., description="Agency identifier"),
    field_mapping: str = Form(
        ..., description="JSON string mapping agency columns to system fields"
    ),
    file: UploadFile = File(..., description="Document file (CSV, Excel, or PDF)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Upload and process agency document.
    """
    logger.info(f"Received document upload for agency {agency_id}")

    # Parse field mapping from JSON string
    import json

    try:
        field_mapping_dict: Dict[str, str] = json.loads(field_mapping)
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid field_mapping JSON format",
        )

    # Validate field mapping contains all required fields
    missing_fields = [
        field
        for field in config.REQUIRED_FIELDS
        if field not in field_mapping_dict.values()
    ]

    if missing_fields:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Field mapping missing required fields: {', '.join(missing_fields)}",
        )

    # Get or create agency
    stmt = select(Agency).where(Agency.id == agency_id)
    result = await db.execute(stmt)
    agency = result.scalar_one_or_none()

    if not agency:
        # Create placeholder agency for POC
        agency = Agency(id=agency_id, name=f"Agency {agency_id}")
        db.add(agency)
        await db.commit()
        logger.info(f"Created new agency: {agency_id}")

    # Save uploaded file temporarily
    file_extension = Path(file.filename).suffix

    if file_extension.lower() not in config.ALLOWED_UPLOAD_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported file type: {file_extension}. Allowed: {', '.join(config.ALLOWED_UPLOAD_EXTENSIONS)}",
        )

    try:
        with NamedTemporaryFile(delete=False, suffix=file_extension) as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name

        # Parse document
        parser = DocumentParser()
        df: pd.DataFrame     = await parser.parse(
            file_path=temp_file_path,
            file_type=file_extension,
            field_mapping=field_mapping_dict,
        )

        logger.info(f"Parsed {len(df)} records from document")

        # Process and store records
        address_normalizer = AddressNormalizer()
        records_processed = 0
        records_skipped = 0

        for _, row in df.iterrows():
            # Normalize address
            normalized_address = address_normalizer.normalize(
                str(row.get("address", "")), str(row.get("postcode", ""))
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

            # Create new property listing
            property_listing = PropertyListing(
                agency_id=agency_id,
                address=str(row.get("address", "")),
                normalized_address=normalized_address,
                postcode=str(row.get("postcode", "")),
                client_name=str(row.get("client_name", "")),
                status=str(row.get("status", "")).lower(),
                withdrawn_date=row.get("withdrawn_date"),
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Error processing document: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process document: {str(e)}",
        )
