"""
PPD Upload Service for handling background processing of uploaded CSV files.
"""

import logging
from datetime import datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import AsyncSessionLocal
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
        async with AsyncSessionLocal() as session:
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

                # Extract values before they might be expired (though AsyncSessionLocal has expire_on_commit=False)
                csv_path = job.csv_path
                year = job.year
                month = job.month
                filename = job.filename

                logger.info(
                    f"Processing PPD upload: {filename} (year={year}, month={month})"
                )

                # Ingest CSV to Parquet
                ingest_summary = await self.ppd_service.ingest_ppd_csv(
                    csv_path=csv_path, year=year, month=month
                )

                if ingest_summary.successful > 0:
                    # Record in history
                    # Updated to use year-only partitioning
                    parquet_path = self.ppd_service._get_parquet_path(
                        year
                    )

                    history_record = PPDIngestHistory(
                        csv_filename=filename,
                        csv_path=csv_path,
                        parquet_path=str(parquet_path),
                        year=year,
                        month=month,
                        records_processed=ingest_summary.successful,
                    )

                    session.add(history_record)

                    # Update job status
                    job.status = "completed"
                    job.records_processed = ingest_summary.successful
                    job.processed_at = datetime.utcnow()

                    await session.commit()

                    logger.info(
                        f"Successfully processed {ingest_summary.successful} records from {filename}"
                    )
                else:
                    # Mark as failed
                    job.status = "failed"
                    job.error_message = "; ".join(ingest_summary.errors)
                    job.processed_at = datetime.utcnow()
                    await session.commit()

                    logger.error(
                        f"Failed to process {filename}: {job.error_message}"
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

    async def delete_upload(self, upload_id: str) -> bool:
        """
        Delete a PPD upload job and its associated files.

        Args:
            upload_id: Upload job identifier

        Returns:
            True if deleted successfully, False otherwise
        """
        async with AsyncSessionLocal() as session:
            try:
                # Get upload job
                stmt = select(PPDUploadJob).where(PPDUploadJob.id == upload_id)
                result = await session.execute(stmt)
                job = result.scalar_one_or_none()

                if not job:
                    logger.warning(f"Upload job not found for deletion: {upload_id}")
                    return False

                # 1. Delete CSV file
                try:
                    csv_path = Path(job.csv_path)
                    if csv_path.exists():
                        csv_path.unlink()
                        logger.info(f"Deleted CSV file: {csv_path}")
                except Exception as e:
                    logger.error(f"Failed to delete CSV file {job.csv_path}: {e}")

                # 2. Delete Parquet file (if exists)
                # Note: This deletes the specific year partition file if it matches
                try:
                    parquet_path = self.ppd_service._get_parquet_path(job.year)
                    if parquet_path.exists():
                        parquet_path.unlink()
                        logger.info(f"Deleted Parquet file: {parquet_path}")
                        
                        # Try to remove the directory if empty
                        if parquet_path.parent.exists() and not any(parquet_path.parent.iterdir()):
                            parquet_path.parent.rmdir()
                except Exception as e:
                    logger.error(f"Failed to delete Parquet file for year {job.year}: {e}")

                # 3. Delete DB record
                await session.delete(job)
                await session.commit()
                
                logger.info(f"Deleted upload job: {upload_id}")
                return True

            except Exception as e:
                logger.error(f"Error deleting upload {upload_id}: {str(e)}")
                return False
