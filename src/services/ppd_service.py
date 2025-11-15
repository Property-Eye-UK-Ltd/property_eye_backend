"""
PPD (Price Paid Data) service with Parquet storage and DuckDB queries.

Handles ingestion of UK Land Registry PPD data and efficient querying
using DuckDB for fraud detection.
"""

import logging
from datetime import timedelta
from pathlib import Path
from typing import List, Optional

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.models.property_listing import PropertyListing
from src.services.address_normalizer import AddressNormalizer
from src.utils.constants import config

logger = logging.getLogger(__name__)


class IngestionSummary:
    """Summary of PPD data ingestion."""

    def __init__(self, successful: int = 0, failed: int = 0, errors: List[str] = None):
        self.successful = successful
        self.failed = failed
        self.errors = errors or []


class PPDService:
    """
    Service for managing PPD data in Parquet format with DuckDB queries.

    Handles ingestion of PPD CSV files and efficient querying for fraud detection.
    """

    # PPD CSV column names (based on data/pp-2025.csv structure)
    PPD_COLUMNS = [
        "transaction_id",
        "price",
        "transfer_date",
        "postcode",
        "property_type",
        "old_new",
        "duration",
        "paon",
        "saon",
        "street",
        "locality",
        "town",
        "district",
        "county",
        "ppd_category",
        "record_status",
    ]

    def __init__(
        self, volume_path: Optional[str] = None, compression: Optional[str] = None
    ):
        """
        Initialize PPD service.

        Args:
            volume_path: Path to PPD storage volume (defaults to config)
            compression: Compression algorithm (snappy or zstd, defaults to config)
        """
        self.volume_path = Path(volume_path or config.PPD_VOLUME_PATH)
        self.compression = compression or config.PPD_COMPRESSION
        self.address_normalizer = AddressNormalizer()

        # Create volume path if it doesn't exist
        self.volume_path.mkdir(parents=True, exist_ok=True)

        # Initialize DuckDB connection (in-memory for queries)
        self.duckdb_conn = duckdb.connect(":memory:")

    async def ingest_ppd_csv(
        self, csv_path: str, year: int, month: int
    ) -> IngestionSummary:
        """
        Ingest PPD CSV and convert to Parquet format.

        Steps:
        1. Read CSV with pandas
        2. Normalize addresses
        3. Validate data
        4. Write to partitioned Parquet file
        5. Return summary

        Args:
            csv_path: Path to PPD CSV file
            year: Year for partitioning
            month: Month for partitioning

        Returns:
            IngestionSummary with success/failure counts
        """
        summary = IngestionSummary()

        try:
            logger.info(f"Starting PPD ingestion from {csv_path}")

            # Read CSV
            df = pd.read_csv(
                csv_path,
                names=self.PPD_COLUMNS,
                header=None,
                parse_dates=["transfer_date"],
            )

            total_records = len(df)
            logger.info(f"Read {total_records} records from CSV")

            # Add derived columns
            df["full_address"] = df.apply(self._build_full_address, axis=1)
            df["normalized_address"] = df["full_address"].apply(
                lambda addr: self.address_normalizer.normalize(addr)
            )
            df["year"] = year
            df["month"] = month

            # Validate data
            valid_df = df.dropna(
                subset=["transaction_id", "transfer_date", "full_address"]
            )
            invalid_count = total_records - len(valid_df)

            if invalid_count > 0:
                logger.warning(f"Dropped {invalid_count} invalid records")
                summary.failed = invalid_count
                summary.errors.append(
                    f"{invalid_count} records missing required fields"
                )

            # Write to Parquet
            parquet_path = self._get_parquet_path(year, month)
            parquet_path.parent.mkdir(parents=True, exist_ok=True)

            # Convert to PyArrow Table for better control
            table = pa.Table.from_pandas(valid_df)

            # Write Parquet file
            pq.write_table(table, parquet_path, compression=self.compression)

            summary.successful = len(valid_df)
            logger.info(
                f"Successfully ingested {summary.successful} records to {parquet_path}"
            )

        except Exception as e:
            error_msg = f"Failed to ingest PPD data: {str(e)}"
            logger.error(error_msg)
            summary.errors.append(error_msg)
            summary.failed = summary.failed or 0

        return summary

    def query_ppd_for_properties(
        self,
        properties: List[PropertyListing],
        scan_window_months: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Use DuckDB to query Parquet files for matching PPD records.

        Filters by:
        - Date range (withdrawal_date to +scan_window_months)
        - Postcode prefix (if available)

        Args:
            properties: List of PropertyListing objects to match
            scan_window_months: Months to scan after withdrawal (defaults to config)

        Returns:
            DataFrame with matching PPD records
        """
        if not properties:
            return pd.DataFrame()

        scan_window = scan_window_months or config.SCAN_WINDOW_MONTHS

        # Build date range filter
        min_date = None
        max_date = None
        postcodes = set()

        for prop in properties:
            if prop.withdrawn_date:
                if min_date is None or prop.withdrawn_date < min_date:
                    min_date = prop.withdrawn_date

                end_date = prop.withdrawn_date + timedelta(days=scan_window * 30)
                if max_date is None or end_date > max_date:
                    max_date = end_date

            if prop.postcode:
                # Extract postcode prefix (first 2-4 characters)
                prefix = (
                    prop.postcode.split()[0]
                    if " " in prop.postcode
                    else prop.postcode[:4]
                )
                postcodes.add(prefix)

        # Build DuckDB query
        parquet_pattern = str(self.volume_path / "year=*/month=*/*.parquet")

        query = f"""
            SELECT *
            FROM read_parquet('{parquet_pattern}')
            WHERE 1=1
        """

        # Add date filter if we have withdrawal dates
        if min_date and max_date:
            query += f"""
                AND transfer_date BETWEEN '{min_date.strftime("%Y-%m-%d")}'
                AND '{max_date.strftime("%Y-%m-%d")}'
            """

        # Add postcode filter if we have postcodes
        if postcodes:
            postcode_conditions = " OR ".join(
                [f"postcode LIKE '{prefix}%'" for prefix in postcodes]
            )
            query += f" AND ({postcode_conditions})"

        try:
            logger.info(f"Executing DuckDB query: {query}")
            result_df = self.duckdb_conn.execute(query).fetchdf()
            logger.info(f"Query returned {len(result_df)} PPD records")
            return result_df
        except Exception as e:
            logger.error(f"DuckDB query failed: {str(e)}")
            return pd.DataFrame()

    def _get_parquet_path(self, year: int, month: int) -> Path:
        """
        Generate partitioned Parquet file path.

        Args:
            year: Year for partitioning
            month: Month for partitioning

        Returns:
            Path to Parquet file
        """
        partition_dir = self.volume_path / f"year={year}" / f"month={month:02d}"
        filename = f"ppd_{year}{month:02d}.parquet"
        return partition_dir / filename

    def _build_full_address(self, row: pd.Series) -> str:
        """
        Build full address from PPD components.

        Args:
            row: DataFrame row with address components

        Returns:
            Full address string
        """
        components = []

        # Add SAON (Secondary Addressable Object Name) if present
        if pd.notna(row.get("saon")) and row["saon"]:
            components.append(str(row["saon"]))

        # Add PAON (Primary Addressable Object Name) if present
        if pd.notna(row.get("paon")) and row["paon"]:
            components.append(str(row["paon"]))

        # Add street
        if pd.notna(row.get("street")) and row["street"]:
            components.append(str(row["street"]))

        # Add locality
        if pd.notna(row.get("locality")) and row["locality"]:
            components.append(str(row["locality"]))

        # Add town
        if pd.notna(row.get("town")) and row["town"]:
            components.append(str(row["town"]))

        # Add postcode
        if pd.notna(row.get("postcode")) and row["postcode"]:
            components.append(str(row["postcode"]))

        return ", ".join(components)
