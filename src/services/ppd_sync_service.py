"""
PPD Sync Service for automatic ingestion on startup.

Scans CSV volume for new PPD files and ingests them automatically.
"""

import logging
import re
from pathlib import Path
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.base import engine
from src.models.ppd_ingest_history import PPDIngestHistory
from src.services.ppd_service import PPDService
from src.utils.constants import config

logger = logging.getLogger(__name__)


class PPDSyncService:
    """
    Service for automatic PPD CSV ingestion on startup.

    Scans CSV volume directory and ingests any files not in history.
    """

    def __init__(self):
        self.csv_volume_path = Path(config.CSV_VOLUME_PATH)
        self.ppd_service = PPDService()

    async def sync_ppd_data(self) -> dict:
        """
        Scan CSV volume and ingest new PPD files.

        Returns:
            Summary dict with ingestion results
        """
        summary = {
            "total_files_found": 0,
            "already_ingested": 0,
            "newly_ingested": 0,
            "failed": 0,
            "errors": [],
        }

        try:
            # Ensure CSV volume exists
            if not self.csv_volume_path.exists():
                logger.warning(
                    f"CSV volume path does not exist: {self.csv_volume_path}"
                )
                logger.info(f"Creating CSV volume directory: {self.csv_volume_path}")
                self.csv_volume_path.mkdir(parents=True, exist_ok=True)
                return summary

            # Find all CSV files
            csv_files = list(self.csv_volume_path.glob("*.csv"))
            summary["total_files_found"] = len(csv_files)

            if not csv_files:
                logger.info(f"No CSV files found in {self.csv_volume_path}")
                return summary

            logger.info(f"Found {len(csv_files)} CSV files in {self.csv_volume_path}")

            # Get ingestion history
            async with AsyncSession(engine) as session:
                # Get already ingested filenames
                result = await session.execute(select(PPDIngestHistory.csv_filename))
                ingested_filenames = {row[0] for row in result.fetchall()}

                # Process each CSV file
                for csv_file in csv_files:
                    filename = csv_file.name

                    # Skip if already ingested
                    if filename in ingested_filenames:
                        logger.info(f"Skipping already ingested file: {filename}")
                        summary["already_ingested"] += 1
                        continue

                    # Extract year and month from filename
                    year, month = self._extract_year_month(filename)

                    if year is None or month is None:
                        error_msg = (
                            f"Could not extract year/month from filename: {filename}"
                        )
                        logger.warning(error_msg)
                        summary["errors"].append(error_msg)
                        summary["failed"] += 1
                        continue

                    # Ingest the file
                    logger.info(f"Ingesting {filename} (year={year}, month={month})...")

                    try:
                        ingest_summary = await self.ppd_service.ingest_ppd_csv(
                            csv_path=str(csv_file), year=year, month=month
                        )

                        if ingest_summary.successful > 0:
                            # Record in history
                            parquet_path = self.ppd_service._get_parquet_path(
                                year, month
                            )

                            history_record = PPDIngestHistory(
                                csv_filename=filename,
                                csv_path=str(csv_file),
                                parquet_path=str(parquet_path),
                                year=year,
                                month=month,
                                records_processed=ingest_summary.successful,
                            )

                            session.add(history_record)
                            await session.commit()

                            logger.info(
                                f"Successfully ingested {ingest_summary.successful} records from {filename}"
                            )
                            summary["newly_ingested"] += 1
                        else:
                            error_msg = f"Failed to ingest {filename}: {', '.join(ingest_summary.errors)}"
                            logger.error(error_msg)
                            summary["errors"].append(error_msg)
                            summary["failed"] += 1

                    except Exception as e:
                        error_msg = f"Error ingesting {filename}: {str(e)}"
                        logger.error(error_msg)
                        summary["errors"].append(error_msg)
                        summary["failed"] += 1

        except Exception as e:
            error_msg = f"PPD sync failed: {str(e)}"
            logger.error(error_msg)
            summary["errors"].append(error_msg)

        return summary

    def _extract_year_month(self, filename: str) -> tuple[Optional[int], Optional[int]]:
        """
        Extract year and month from PPD filename.

        Expected formats:
        - pp-2025.csv (full year)
        - pp-2025-01.csv (specific month)
        - pp-monthly-update-2025-01.csv

        Args:
            filename: CSV filename

        Returns:
            Tuple of (year, month) or (None, None) if not found
        """
        # Try pattern: pp-YYYY-MM.csv or pp-YYYY.csv
        pattern1 = r"pp-(\d{4})(?:-(\d{1,2}))?\.csv"
        match = re.search(pattern1, filename, re.IGNORECASE)

        if match:
            year = int(match.group(1))
            month = int(match.group(2)) if match.group(2) else 1
            return year, month

        # Try pattern: pp-monthly-update-YYYY-MM.csv
        pattern2 = r"pp-monthly-update-(\d{4})-(\d{1,2})\.csv"
        match = re.search(pattern2, filename, re.IGNORECASE)

        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            return year, month

        # Try generic pattern: any YYYY-MM or YYYY
        pattern3 = r"(\d{4})-(\d{1,2})"
        match = re.search(pattern3, filename)

        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            return year, month

        pattern4 = r"(\d{4})"
        match = re.search(pattern4, filename)

        if match:
            year = int(match.group(1))
            return year, 1

        return None, None
