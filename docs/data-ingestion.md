# Data Ingestion

Property Eye ingests property listing data from real estate agencies to monitor for suspicious withdrawals.

---

## Supported Formats

The system supports several file formats for agency data:

- **CSV**: Standard comma-separated values.
- **Excel**: `.xlsx` and `.xls` files (using the `openpyxl` engine).
- **PDF**: (Planned) Table extraction from PDF reports.

---

## Ingestion Pipeline

### 1. Document Upload
Agencies upload their listing reports via the `documents` API endpoint. The system stores the file temporarily and records the metadata.

### 2. Field Mapping
Since every agency uses different column headers (e.g., "Address 1" vs "Property"), the system uses a **Field Mapping** dictionary. This maps agency-specific columns to standard system fields:
- `address`: Full property address.
- `postcode`: Property postcode.
- `client_name` / `vendor_name`: Name of the property owner.
- `status`: Current listing status (e.g., "Available", "Withdrawn").
- `withdrawn_date`: Date the property was removed from the market.
- `price`: Original listing price.

### 3. Parsing & Validation
The `DocumentParser` service:
- Loads the file into a `pandas` DataFrame.
- Renames columns based on the field mapping.
- Validates that all **Required Fields** are present and correctly formatted.

### 4. Database Storage
Validated listings are stored in the `PropertyListing` table, linked to the uploading agency. These listings form the baseline for the Fraud Detection system.

---

## Technical Components

- **`DocumentParser`**: Handles multi-format reading and column mapping.
- **`PropertyListing` Model**: The database representation of an agency listing.
- **`config.REQUIRED_FIELDS`**: Defines the minimum data needed for fraud detection.
