"""
PPD (Price Paid Data) service with Parquet storage and DuckDB queries.

Handles ingestion of UK Land Registry PPD data and efficient querying
using DuckDB for fraud detection.
"""

import logging
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Set

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.models.property_listing import PropertyListing
from src.services.address_normalizer import AddressNormalizer
from src.utils.constants import config

logger = logging.getLogger(__name__)

# UK postcode pattern for fallback extraction from free-text address
_UK_POSTCODE_RE = re.compile(
    r"\b([A-Z]{1,2}\d[A-Z0-9]?\s*\d[A-Z]{2})\b", re.IGNORECASE
)


def _sql_literal(value: str) -> str:
    """Escape a value for safe inclusion in a DuckDB SQL string literal."""
    return (value or "").replace("'", "''")


def _withdrawn_as_date(withdrawn) -> Optional[date]:
    """Coerce listing withdrawn_date (date or datetime) to date for PPD windowing."""
    if withdrawn is None:
        return None
    if isinstance(withdrawn, datetime):
        return withdrawn.date()
    if isinstance(withdrawn, date):
        return withdrawn
    return None


def _effective_postcode(prop: PropertyListing) -> Optional[str]:
    """Prefer stored postcode; otherwise try to parse from address."""
    if prop.postcode and str(prop.postcode).strip():
        return str(prop.postcode).strip()
    if prop.address:
        m = _UK_POSTCODE_RE.search(prop.address)
        if m:
            return re.sub(r"\s+", " ", m.group(1).upper()).strip()
    return None


def _postcode_prefix(postcode: str) -> str:
    """First outward code segment for DuckDB LIKE filters."""
    pc = postcode.strip().upper()
    return pc.split()[0] if " " in pc else pc[:4]


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
        self, csv_path: str, year: int, month: int = 0
    ) -> IngestionSummary:
        """
        Ingest PPD CSV and convert to Parquet format.
        
        Offloads the heavy synchronous data processing to a background thread
        so it doesn't block the asyncio event loop.
        """
        import asyncio
        return await asyncio.to_thread(
            self._ingest_ppd_csv_blocking, csv_path, year, month
        )

    def _ingest_ppd_csv_blocking(
        self, csv_path: str, year: int, month: int = 0
    ) -> IngestionSummary:
        """
        Synchronous blocking logic for PPD ingestion.
        
        Steps:
        1. Read CSV with pandas
        2. Normalize addresses
        3. Validate data
        4. Sort data by transfer_date and postcode
        5. Write to partitioned Parquet file
        6. Return summary
        """
        summary = IngestionSummary()

        try:
            logger.info(f"[Ingestion] Starting PPD ingestion from {csv_path}")

            # Read CSV
            df = pd.read_csv(
                csv_path,
                names=self.PPD_COLUMNS,
                header=None,
                parse_dates=["transfer_date"],
            )

            total_records = len(df)
            logger.info(f"[Ingestion] Read {total_records} records from CSV")

            # Add derived columns
            df["full_address"] = df.apply(self._build_full_address, axis=1)
            df["normalized_address"] = df["full_address"].apply(
                lambda addr: self.address_normalizer.normalize(addr)
            )
            df["year"] = year

            # Validate data
            valid_df = df.dropna(
                subset=["transaction_id", "transfer_date", "full_address"]
            )
            invalid_count = total_records - len(valid_df)

            if invalid_count > 0:
                logger.warning(f"[Ingestion] Dropped {invalid_count} invalid records")
                summary.failed = invalid_count
                summary.errors.append(
                    f"{invalid_count} records missing required fields"
                )

            # Sort data for better query performance (indexing optimization)
            valid_df = valid_df.sort_values(by=["transfer_date", "postcode"])

            # Write to Parquet
            parquet_path = self._get_parquet_path(year)
            parquet_path.parent.mkdir(parents=True, exist_ok=True)

            # Convert to PyArrow Table for better control
            table = pa.Table.from_pandas(valid_df)

            # Write Parquet file
            pq.write_table(table, parquet_path, compression=self.compression)

            summary.successful = len(valid_df)
            logger.info(
                f"[Ingestion] Successfully ingested {summary.successful} records to {parquet_path}"
            )

        except Exception as e:
            error_msg = f"[Ingestion] Failed to ingest PPD data: {str(e)}"
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
        - Postcode prefix (from listing.postcode or parsed from address)
        - Optional town / county overlap (from listing.region / listing.county)

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
        min_date: Optional[date] = None
        max_date: Optional[date] = None
        postcodes: Set[str] = set()
        towns: Set[str] = set()
        counties: Set[str] = set()
        props_without_date = 0

        for prop in properties:
            wd = _withdrawn_as_date(prop.withdrawn_date)
            if wd:
                # Look back N months before withdrawal
                start_date = wd - timedelta(days=config.LOOKBACK_MONTHS * 30)
                if min_date is None or start_date < min_date:
                    min_date = start_date

                # Look forward N months after withdrawal
                end_date = wd + timedelta(days=config.LOOKAHEAD_MONTHS * 30)
                if max_date is None or end_date > max_date:
                    max_date = end_date
            else:
                props_without_date += 1

            pc = _effective_postcode(prop)
            if pc:
                postcodes.add(_postcode_prefix(pc))

            if getattr(prop, "region", None) and str(prop.region).strip():
                towns.add(str(prop.region).strip())
            if getattr(prop, "county", None) and str(prop.county).strip():
                counties.add(str(prop.county).strip())

        # Build DuckDB query
        # Updated pattern for year-only partitioning
        parquet_pattern = str(self.volume_path / "year=*/ppd_*.parquet")

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
        else:
            logger.warning(
                "[DuckDB Scan] No withdrawal dates on listings — omitting date filter "
                "(scan may be broad). Listings missing date: %s",
                props_without_date,
            )

        geo_clauses: List[str] = []

        # Postcode filter (primary geographic narrowing)
        if postcodes:
            postcode_conditions = " OR ".join(
                [f"postcode LIKE '{_sql_literal(p)}%'" for p in sorted(postcodes)]
            )
            geo_clauses.append(f"({postcode_conditions})")

        # Town / county hints from richer listing fields (helps when postcode is wrong/missing)
        if towns:
            town_parts = []
            for t in towns:
                lit = _sql_literal(t)
                town_parts.append(
                    f"(lower(town) LIKE lower('%{lit}%') "
                    f"OR lower(locality) LIKE lower('%{lit}%'))"
                )
            geo_clauses.append("(" + " OR ".join(town_parts) + ")")

        if counties:
            county_parts = []
            for c in counties:
                lit = _sql_literal(c)
                county_parts.append(
                    f"(lower(county) LIKE lower('%{lit}%') "
                    f"OR lower(district) LIKE lower('%{lit}%'))"
                )
            geo_clauses.append("(" + " OR ".join(county_parts) + ")")

        if geo_clauses:
            query += " AND (" + " OR ".join(geo_clauses) + ")"

        try:
            logger.info(
                "[DuckDB Scan] Initializing scan for %s listings using Parquet/DuckDB",
                len(properties)
            )
            logger.info(
                "[DuckDB Scan] Filters: dates=[%s to %s], postcode_prefixes=%s, towns=%s, counties=%s",
                min_date.isoformat() if min_date else "N/A",
                max_date.isoformat() if max_date else "N/A",
                sorted(postcodes) if postcodes else "None",
                list(towns) if towns else "None",
                list(counties) if counties else "None",
            )
            
            result_df = self.duckdb_conn.execute(query).fetchdf()
            
            logger.info(
                "[DuckDB Scan] Complete. Found %s candidate PPD records in storage volume.",
                len(result_df)
            )
            return result_df
        except Exception as e:
            logger.error(f"[DuckDB Scan] Query failed: {str(e)}")
            return pd.DataFrame()

    def _get_parquet_path(self, year: int) -> Path:
        """
        Generate partitioned Parquet file path.

        Args:
            year: Year for partitioning

        Returns:
            Path to Parquet file
        """
        partition_dir = self.volume_path / f"year={year}"
        filename = f"ppd_{year}.parquet"
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
