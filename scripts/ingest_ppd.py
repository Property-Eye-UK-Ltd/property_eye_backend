"""
PPD data ingestion script.

Converts PPD CSV files to Parquet format with partitioning by year and month.

Usage:
    python scripts/ingest_ppd.py --csv data/pp-2025.csv --year 2025 --month 1
    python scripts/ingest_ppd.py --csv data/pp-2024.csv --year 2024 --month 12
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.services.ppd_service import PPDService
from src.utils.constants import config


async def main():
    """Main entry point for PPD ingestion."""
    parser = argparse.ArgumentParser(
        description="Ingest PPD CSV and convert to Parquet format"
    )
    parser.add_argument("--csv", required=True, help="Path to PPD CSV file")
    parser.add_argument(
        "--year", type=int, required=True, help="Year for partitioning (e.g., 2025)"
    )
    parser.add_argument(
        "--month", type=int, required=True, help="Month for partitioning (1-12)"
    )
    parser.add_argument(
        "--volume-path",
        default=None,
        help=f"PPD volume path (default: {config.PPD_VOLUME_PATH})",
    )
    parser.add_argument(
        "--compression",
        choices=["snappy", "zstd"],
        default=None,
        help=f"Compression algorithm (default: {config.PPD_COMPRESSION})",
    )

    args = parser.parse_args()

    # Validate inputs
    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"Error: CSV file not found: {csv_path}")
        sys.exit(1)

    if args.month < 1 or args.month > 12:
        print(f"Error: Month must be between 1 and 12, got {args.month}")
        sys.exit(1)

    # Verify PPD volume path
    volume_path = Path(args.volume_path or config.PPD_VOLUME_PATH)
    if not volume_path.exists():
        print(f"Creating PPD volume directory: {volume_path}")
        volume_path.mkdir(parents=True, exist_ok=True)

    if not volume_path.is_dir():
        print(f"Error: PPD volume path is not a directory: {volume_path}")
        sys.exit(1)

    print("=" * 60)
    print("PPD Data Ingestion")
    print("=" * 60)
    print(f"CSV File: {csv_path}")
    print(f"Year: {args.year}")
    print(f"Month: {args.month}")
    print(f"Volume Path: {volume_path}")
    print(f"Compression: {args.compression or config.PPD_COMPRESSION}")
    print("=" * 60)
    print()

    # Initialize PPD service
    ppd_service = PPDService(volume_path=str(volume_path), compression=args.compression)

    # Perform ingestion
    print("Starting ingestion...")
    print()

    try:
        summary = await ppd_service.ingest_ppd_csv(
            csv_path=str(csv_path), year=args.year, month=args.month
        )

        print()
        print("=" * 60)
        print("Ingestion Summary")
        print("=" * 60)
        print(f"Successful records: {summary.successful}")
        print(f"Failed records: {summary.failed}")

        if summary.errors:
            print()
            print("Errors:")
            for error in summary.errors:
                print(f"  - {error}")

        print("=" * 60)

        if summary.successful > 0:
            print()
            print("✓ Ingestion completed successfully!")

            # Show Parquet file location
            parquet_path = ppd_service._get_parquet_path(args.year, args.month)
            print(f"Parquet file: {parquet_path}")

            if parquet_path.exists():
                file_size_mb = parquet_path.stat().st_size / (1024 * 1024)
                print(f"File size: {file_size_mb:.2f} MB")
        else:
            print()
            print("✗ Ingestion failed - no records processed")
            sys.exit(1)

    except Exception as e:
        print()
        print("=" * 60)
        print("Error during ingestion:")
        print(f"  {str(e)}")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
