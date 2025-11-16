"""
PPD Upload Service for handling background processing of uploaded CSV files.
"""

import logging
from datetime import datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.base import engine
from src.models.ppd_ingest_history import PPDIngestHistory
from src.models.ppd_upload_job import PPDUploadJob
from src.services.ppd_service import PPDService

logger = logging.getLogger(__name__)


class PPDUploadService:
    """Service for processing uploaded PPD CSV files in background."""

    def __init__(self):
        self.ppd_service = PPDService()

    async def process_upload(self, upload_id: str) -> None:
        """
        Process uploaded PPD CSV file in background.

        Args:
            upload_id: Upload job identifier
        """
        async with AsyncSession(engine) as session:
            try:
                # Get upload job
                stmt = select(PPDUploadJob).where(PPDUploadJob.id == upload_id)
                result = await session.execute(stmt)
                job = result.scalar_one_or_none()

                if not job:
                    logger.error(f"Upload job not found: {upload_id}")
                    return

                # Update status to processing
                job.status = "processing"
                await session.commit()

                logger.info(
                    f"Processing PPD upload: {job.filename} (year={job.year}, month={job.month})"
                )

                # Ingest CSV to Parquet
                ingest_summary = await self.ppd_service.ingest_ppd_csv(
                    csv_path=job.csv_path, year=job.year, month=job.month
                )

                if ingest_summary.successful > 0:
                    # Record in history
                    parquet_path = self.ppd_service._get_parquet_path(
                        job.year, job.month
                    )

                    history_record = PPDIngestHistory(
                        csv_filename=job.filename,
                        csv_path=job.csv_path,
                        parquet_path=str(parquet_path),
                        year=job.year,
                        month=job.month,
                        records_processed=ingest_summary.successful,
                    )

                    session.add(history_record)

                    # Update job status
                    job.status = "completed"
                    job.records_processed = ingest_summary.successful
                    job.processed_at = datetime.utcnow()

                    await session.commit()

                    logger.info(
                        f"Successfully processed {ingest_summary.successful} records from {job.filename}"
                    )
                else:
                    # Mark as failed
                    job.status = "failed"
                    job.error_message = "; ".join(ingest_summary.errors)
                    job.processed_at = datetime.utcnow()
                    await session.commit()

                    logger.error(
                        f"Failed to process {job.filename}: {job.error_message}"
                    )

            except Exception as e:
                logger.error(f"Error processing upload {upload_id}: {str(e)}")

                # Update job status to failed
                try:
                    stmt = select(PPDUploadJob).where(PPDUploadJob.id == upload_id)
                    result = await session.execute(stmt)
                    job = result.scalar_one_or_none()

                    if job:
                        job.status = "failed"
                        job.error_message = str(e)
                        job.processed_at = datetime.utcnow()
                        await session.commit()
                except Exception as update_error:
                    logger.error(f"Failed to update job status: {str(update_error)}")
