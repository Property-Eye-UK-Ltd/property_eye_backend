# Price Paid Data (PPD) Management

Property Eye maintains a local, optimized cache of the UK Land Registry Price Paid Data to facilitate rapid and cost-effective fraud screening.

---

## Storage Architecture

The system uses a modern data stack for handling millions of PPD records efficiently:

1.  **Parquet Files**: Data is stored in Apache Parquet format, which is columnar and highly compressed.
2.  **Partitioning**: Parquet files are partitioned by year (e.g., `year=2024/ppd_2024.parquet`) to minimize the amount of data read during queries.
3.  **DuckDB**: An in-process SQL OLAP database management system used to query the Parquet files directly without needing a separate database server.

---

## Ingestion Process

PPD data is typically downloaded as large CSV files from the HM Land Registry website. The `PPDService` handles the conversion:

1.  **CSV Parsing**: The system reads the CSV file using `pandas`.
2.  **Address Normalization**: For each record, the system builds a full address string and normalizes it (lowercase, stripped whitespace, etc.) using `AddressNormalizer`.
3.  **Validation**: Records missing critical fields (Transaction ID, Transfer Date, Address) are dropped.
4.  **Sorting**: Data is sorted by `transfer_date` and `postcode` to optimize future queries.
5.  **Parquet Writing**: The validated and sorted data is written to partitioned Parquet files using `pyarrow`.

---

## Querying Logic

When performing fraud detection (Stage 1), the system queries the Parquet files via DuckDB:

- **Temporal Pruning**: The query only scans files for the years relevant to the property's withdrawal date and the configured lookback/lookahead window.
- **Geographic Pruning**: DuckDB uses `LIKE` filters on the `postcode` column and town/county hints to further narrow down the result set.
- **In-Memory Results**: Matching PPD records are returned as a `pandas` DataFrame for immediate processing by the `FraudDetector`.

---

## Technical Details

- **Storage Path**: Configurable via `PPD_VOLUME_PATH`.
- **Compression**: Uses `snappy` or `zstd` compression for Parquet files.
- **Address Components**: PPD addresses are built from PAON (Primary Addressable Object Name), SAON (Secondary), Street, Locality, Town, and Postcode.

---

## Automation & Sync

The system includes a `PPDSyncService` that can be configured to automatically ingest new PPD files found in the `CSV_VOLUME_PATH` on application startup. This is controlled by the `SYNC_PPD` configuration setting. When enabled, it identifies files that haven't been ingested yet (by checking the `ppd_ingest_history` table) and processes them sequentially.
