# Test Data

This directory contains sample data for testing the Fraud Detection POC.

## Sample Agency Listings

**File:** `sample_agency_listings.csv`

This CSV file contains sample property listings from a fictional UK real estate agency.

### Field Mapping

When uploading this file via the API, use the following field mapping:

```json
{
  "Property Address": "address",
  "Client Full Name": "client_name",
  "Status": "status",
  "Date Withdrawn": "withdrawn_date",
  "Postcode": "postcode"
}
```

### Data Description

- **Property Address**: Full property address
- **Client Full Name**: Name of the client associated with the property
- **Status**: Property status (withdrawn, active, sold)
- **Date Withdrawn**: Date when property was withdrawn (if applicable)
- **Postcode**: UK postcode

### Testing Workflow

1. **Initialize Database**

   ```bash
   python scripts/init_db.py
   ```

2. **Ingest PPD Data** (if you have PPD CSV file)

   ```bash
   python scripts/ingest_ppd.py --csv data/pp-2025.csv --year 2025 --month 1
   ```

3. **Upload Sample Document**

   ```bash
   curl -X POST "http://localhost:8000/api/v1/documents/upload" \
     -F "agency_id=test-agency-001" \
     -F 'field_mapping={"Property Address":"address","Client Full Name":"client_name","Status":"status","Date Withdrawn":"withdrawn_date","Postcode":"postcode"}' \
     -F "file=@test_data/sample_agency_listings.csv"
   ```

4. **Run Fraud Detection Scan (Stage 1)**

   ```bash
   curl -X POST "http://localhost:8000/api/v1/fraud/scan?agency_id=test-agency-001"
   ```

5. **Verify High-Confidence Matches (Stage 2)**
   ```bash
   curl -X POST "http://localhost:8000/api/v1/verification/verify" \
     -H "Content-Type: application/json" \
     -d '{"match_ids": ["<match-id-1>", "<match-id-2>"]}'
   ```

### Notes

- The sample data includes properties with "withdrawn" status that can be used for fraud detection testing
- Addresses are realistic UK locations but data is fictional
- For meaningful fraud detection results, you'll need actual PPD data that overlaps with these addresses and dates
