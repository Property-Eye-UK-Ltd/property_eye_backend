# Property Eye - Fraud Detection POC

A Proof of Concept system for detecting property fraud by comparing real estate agency listings against UK Land Registry Price Paid Data (PPD).

## Overview

Property Eye helps UK real estate agencies recover lost commissions by detecting cases where sellers and buyers bypass the agency to complete sales privately after being introduced through the agency.

### Two-Stage Detection Pipeline

1. **Stage 1: Suspicious Match Detection**

   - Bulk comparison of withdrawn properties against PPD data
   - Address matching with confidence scoring
   - No Land Registry API calls (cost-free)
   - Returns all suspicious matches for review

2. **Stage 2: Land Registry Verification**
   - Targeted verification of high-confidence matches
   - Land Registry API confirms owner identity
   - Compares owner with agency client records
   - Confirms or rules out fraud

## Technology Stack

- **Python**: 3.11+
- **API Framework**: FastAPI (async, auto-documentation)
- **Database**: SQLAlchemy 2.0 with async support (SQLite for POC, PostgreSQL-ready)
- **Analytics Engine**: DuckDB for querying Parquet files
- **Data Storage**: Parquet format with Snappy/Zstd compression
- **Document Parsing**: pandas (CSV/Excel), pdfplumber (PDF - TODO)
- **String Matching**: rapidfuzz for fuzzy address matching

## Installation

### Prerequisites

- Python 3.11 or higher
- pip or uv package manager

### Install Dependencies

```bash
# Using pip
pip install -r requirements.txt

# Or using uv (recommended)
uv pip install -r requirements.txt
```

## Environment Setup

Create a `.env` file in the project root (use `.env.example` as template):

```bash
# Application Configuration
APP_NAME="Property Eye Fraud Detection POC"
DEBUG=False
LOG_LEVEL=INFO

# Database Configuration
DATABASE_URL=sqlite+aiosqlite:///./fraud_detection.db

# PPD Storage Configuration
PPD_VOLUME_PATH=./data/ppd
PPD_COMPRESSION=snappy

# Land Registry API Configuration
LAND_REGISTRY_API_KEY=your_api_key_here
LAND_REGISTRY_API_URL=https://api.landregistry.gov.uk

# Redis Configuration (for future caching)
REDIS_URL=redis://localhost:6379/0
```

### Key Environment Variables

- **PPD_VOLUME_PATH**: Directory for storing Parquet files (default: `./data/ppd`)
- **PPD_COMPRESSION**: Compression algorithm - `snappy` (faster) or `zstd` (better compression)
- **LAND_REGISTRY_API_KEY**: API key for UK Land Registry ownership verification
- **DATABASE_URL**: Database connection string

## Database Setup

Initialize the database tables:

```bash
python scripts/init_db.py
```

## PPD Data Ingestion

### Download PPD Data

Download UK Land Registry Price Paid Data from:
https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads

### Ingest PPD CSV to Parquet

```bash
python scripts/ingest_ppd.py --csv data/pp-2025.csv --year 2025 --month 1
```

**Options:**

- `--csv`: Path to PPD CSV file (required)
- `--year`: Year for partitioning (required)
- `--month`: Month for partitioning 1-12 (required)
- `--volume-path`: Custom PPD volume path (optional)
- `--compression`: Compression algorithm: snappy or zstd (optional)

### Parquet Storage Structure

```
data/ppd/
├── year=2025/
│   ├── month=01/
│   │   └── ppd_202501.parquet
│   ├── month=02/
│   │   └── ppd_202502.parquet
│   └── month=11/
│       └── ppd_202511.parquet
└── year=2024/
    └── month=12/
        └── ppd_202412.parquet
```

## Running the Application

### Development Server

```bash
uvicorn src.main:app --reload
```

The API will be available at:

- **API**: http://localhost:8000
- **Interactive Docs**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Production Server

```bash
uvicorn src.main:app --host 0.0.0.0 --port 8000 --workers 4
```

## API Usage

### Two-Stage Fraud Detection Workflow

#### 1. Upload Agency Document

```bash
curl -X POST "http://localhost:8000/api/v1/documents/upload" \
  -F "agency_id=test-agency-001" \
  -F 'field_mapping={"Property Address":"address","Client Full Name":"client_name","Status":"status","Date Withdrawn":"withdrawn_date","Postcode":"postcode"}' \
  -F "file=@test_data/sample_agency_listings.csv"
```

**Required Fields:**

- `address`: Property address
- `client_name`: Client name
- `status`: Property status (must include "withdrawn" for fraud detection)
- `withdrawn_date`: Date property was withdrawn
- `postcode`: UK postcode

#### 2. Stage 1: Scan for Suspicious Matches

```bash
curl -X POST "http://localhost:8000/api/v1/fraud/scan?agency_id=test-agency-001"
```

**Response includes:**

- Total matches found
- Confidence score distribution (high/medium/low)
- Detailed match information
- No Land Registry API calls made

#### 3. Review Suspicious Matches

```bash
curl -X GET "http://localhost:8000/api/v1/fraud/reports/test-agency-001?min_confidence=85"
```

**Query Parameters:**

- `min_confidence`: Filter by minimum confidence score
- `verification_status`: Filter by status (suspicious, confirmed_fraud, not_fraud, error)
- `skip`: Pagination offset
- `limit`: Maximum records to return

#### 4. Stage 2: Verify High-Confidence Matches

```bash
curl -X POST "http://localhost:8000/api/v1/verification/verify" \
  -H "Content-Type: application/json" \
  -d '{"match_ids": ["match-id-1", "match-id-2"]}'
```

**This step:**

- Calls Land Registry API for each match
- Compares owner name with client name (85% fuzzy match threshold)
- Updates match status to: `confirmed_fraud`, `not_fraud`, or `error`

#### 5. Check Verification Status

```bash
curl -X GET "http://localhost:8000/api/v1/verification/status/{match_id}"
```

## Field Mapping

Agency documents must map their columns to system-required fields:

```json
{
  "Your Column Name": "system_field_name"
}
```

**Required System Fields:**

- `address`: Property address
- `client_name`: Client full name
- `status`: Property status
- `withdrawn_date`: Withdrawal date
- `postcode`: UK postcode

**Example:**

```json
{
  "Property Address": "address",
  "Client Full Name": "client_name",
  "Status": "status",
  "Date Withdrawn": "withdrawn_date",
  "Postcode": "postcode"
}
```

## Configuration Constants

Edit `src/utils/constants.py` to adjust fraud detection parameters:

```python
SCAN_WINDOW_MONTHS = 12  # Check PPD up to 12 months after withdrawal
MIN_CONFIDENCE_THRESHOLD = 70.0  # Store matches above 70%
HIGH_CONFIDENCE_THRESHOLD = 85.0  # Recommend for verification
MIN_ADDRESS_SIMILARITY = 80.0  # Minimum fuzzy match score

# Confidence Score Weights
ADDRESS_SIMILARITY_WEIGHT = 0.70  # 70% weight
DATE_PROXIMITY_WEIGHT = 0.20  # 20% weight
POSTCODE_MATCH_WEIGHT = 0.10  # 10% weight
```

## DuckDB Query Examples

The system uses DuckDB to query Parquet files efficiently:

```sql
-- Query all PPD records for a date range
SELECT * FROM read_parquet('data/ppd/year=*/month=*/*.parquet')
WHERE transfer_date BETWEEN '2025-01-01' AND '2025-12-31'
AND postcode LIKE 'SW1%';

-- Count records by year
SELECT year, COUNT(*) as count
FROM read_parquet('data/ppd/year=*/month=*/*.parquet')
GROUP BY year;
```

## Testing

### Sample Data

Sample agency listings are provided in `test_data/sample_agency_listings.csv`.

See `test_data/README.md` for testing workflow.

### Run Tests

```bash
# Unit tests
pytest tests/unit/

# Integration tests
pytest tests/integration/

# All tests
pytest
```

## Troubleshooting

### PPD Volume Not Accessible

**Error:** "PPD volume path does not exist"

**Solution:**

```bash
mkdir -p ./data/ppd
# Or set PPD_VOLUME_PATH in .env to an existing directory
```

### Parquet File Access Issues

**Error:** "Failed to read Parquet file"

**Solution:**

- Verify PPD data has been ingested: `ls -la data/ppd/year=*/month=*/`
- Check file permissions
- Ensure DuckDB and pyarrow are installed

### No Suspicious Matches Found

**Possible causes:**

- No withdrawn properties in agency data
- PPD data doesn't overlap with agency property dates/locations
- Addresses don't match (check normalization)

**Debug:**

```bash
# Check withdrawn properties
curl "http://localhost:8000/api/v1/fraud/reports/test-agency-001?verification_status=suspicious"

# Verify PPD data exists
ls -la data/ppd/year=*/month=*/
```

### Land Registry API Errors

**Error:** "Land Registry API integration pending"

**Note:** Land Registry API integration is a placeholder in the POC. The endpoint structure is ready but requires actual API documentation to complete implementation.

## Project Structure

```
src/
├── main.py                    # FastAPI app entry point
├── api/v1/endpoints/          # API endpoints
│   ├── documents.py           # Document upload
│   ├── fraud_reports.py       # Fraud detection (Stage 1)
│   └── verification.py        # Verification (Stage 2)
├── core/
│   └── config.py              # Configuration management
├── models/                    # SQLAlchemy ORM models
│   ├── agency.py
│   ├── property_listing.py
│   └── fraud_match.py
├── schemas/                   # Pydantic schemas
├── services/                  # Business logic
│   ├── address_normalizer.py
│   ├── document_parser.py
│   ├── ppd_service.py
│   ├── fraud_detector.py
│   ├── verification_service.py
│   └── land_registry_client.py
├── db/                        # Database session management
└── utils/                     # Utilities and constants
```

## Future Enhancements

- [ ] Complete PDF parsing implementation
- [ ] Redis caching for Land Registry API responses
- [ ] Background job processing with BullMQ/RQ
- [ ] Multi-year PPD support with automatic loading
- [ ] Enhanced authentication and authorization
- [ ] Frontend dashboard for agencies

## License

Proprietary - Property Eye

## Support

For issues or questions, contact the development team.
