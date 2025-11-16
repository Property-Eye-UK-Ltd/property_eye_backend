"""
PPD Upload API endpoints.

Handles streaming upload of large PPD CSV files with background processing.
"""

import asyncio
import logging
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    UploadFile,
    status,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import get_db
from src.models.ppd_upload_job import PPDUploadJob
from src.schemas.ppd_upload import PPDUploadResponse, PPDUploadStatusResponse
from src.services.ppd_upload_service import PPDUploadService
from src.utils.constants import config

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ppd", tags=["ppd"])

CHUNK_SIZE = 10 * 1024 * 1024


@router.post(
    "/upload",
    response_model=PPDUploadResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Upload PPD CSV file",
    description="""
    Upload a large PPD (Price Paid Data) CSV file for processing.
    
    The file is streamed to disk and processed in the background.
    Use the returned upload_id to check processing status.
    
    The system will automatically convert the CSV to Parquet format
    and make it available for fraud detection queries.
    """,
)
async def upload_ppd_csv(
    year: int = Form(..., ge=1995, le=2030, description="PPD data year"),
    month: int = Form(..., ge=1, le=12, description="PPD data month"),
    file: UploadFile = File(..., description="PPD CSV file"),
    db: AsyncSession = Depends(get_db),
):
    """
    Upload PPD CSV file with streaming support for large files.
    """
    if not file.filename.endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only CSV files are supported",
        )

    upload_id = str(uuid.uuid4())
    csv_volume = Path(config.CSV_VOLUME_PATH)
    csv_volume.mkdir(parents=True, exist_ok=True)

    safe_filename = f"pp-{year}-{month:02d}-{upload_id}.csv"
    csv_path = csv_volume / safe_filename

    logger.info(f"Starting PPD upload: {safe_filename} (year={year}, month={month})")

    try:
        total_bytes = 0

        with open(csv_path, "wb") as f:
            while chunk := await file.read(CHUNK_SIZE):
                f.write(chunk)
                total_bytes += len(chunk)

        file_size_mb = total_bytes / (1024 * 1024)

        logger.info(f"File uploaded successfully: {file_size_mb:.2f} MB")

        upload_job = PPDUploadJob(
            id=upload_id,
            filename=safe_filename,
            csv_path=str(csv_path),
            year=year,
            month=month,
            file_size_mb=round(file_size_mb, 2),
            status="uploaded",
        )

        db.add(upload_job)
        await db.commit()

        asyncio.create_task(PPDUploadService().process_upload(upload_id))

        logger.info(f"Background processing started for upload: {upload_id}")

        return PPDUploadResponse(
            upload_id=upload_id,
            filename=safe_filename,
            year=year,
            month=month,
            file_size_mb=round(file_size_mb, 2),
            status="uploaded",
            message="File uploaded successfully. Processing in background.",
            uploaded_at=datetime.utcnow(),
        )

    except Exception as e:
        if csv_path.exists():
            csv_path.unlink()

        logger.error(f"Error uploading PPD file: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload file: {str(e)}",
        )


@router.get(
    "/upload/{upload_id}",
    response_model=PPDUploadStatusResponse,
    status_code=status.HTTP_200_OK,
    summary="Get PPD upload status",
    description="Check the processing status of an uploaded PPD file.",
)
async def get_upload_status(
    upload_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Get upload job status.
    """
    stmt = select(PPDUploadJob).where(PPDUploadJob.id == upload_id)
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Upload job {upload_id} not found",
        )

    return PPDUploadStatusResponse(
        upload_id=job.id,
        filename=job.filename,
        year=job.year,
        month=job.month,
        status=job.status,
        records_processed=job.records_processed,
        error_message=job.error_message,
        uploaded_at=job.uploaded_at,
        processed_at=job.processed_at,
    )


@router.get(
    "/uploads",
    response_model=list[PPDUploadStatusResponse],
    status_code=status.HTTP_200_OK,
    summary="List all PPD uploads",
    description="Get a list of all PPD upload jobs with their status.",
)
async def list_uploads(
    db: AsyncSession = Depends(get_db),
):
    """
    List all upload jobs.
    """
    stmt = select(PPDUploadJob).order_by(PPDUploadJob.uploaded_at.desc()).limit(100)
    result = await db.execute(stmt)
    jobs = result.scalars().all()

    return [
        PPDUploadStatusResponse(
            upload_id=job.id,
            filename=job.filename,
            year=job.year,
            month=job.month,
            status=job.status,
            records_processed=job.records_processed,
            error_message=job.error_message,
            uploaded_at=job.uploaded_at,
            processed_at=job.processed_at,
        )
        for job in jobs
    ]
