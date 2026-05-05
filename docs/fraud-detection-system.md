# Fraud Detection System

Property Eye identifies potential "backdoor" property sales where a property is withdrawn from an agency but sold shortly after to a buyer introduced by the agency, bypassing the agreed commission.

The system operates in two distinct stages to balance speed, cost, and accuracy.

---

## Stage 1: Suspicious Match Detection (PPD Screening)

The first stage compares agency withdrawn properties against the UK Land Registry **Price Paid Data (PPD)**. This stage is designed to be fast and cost-effective as it uses locally cached data.

### 1. Data Retrieval
The system identifies properties marked as "Withdrawn" or having a "Withdrawn Date" in the agency's records. It then queries the local PPD storage (managed by `PPDService`) for any transactions matching the property's geographic and temporal window.

### 2. Matching Logic
The matching process uses several filters and algorithms:
- **Geographic Filter**: Narrowed down by outward postcode (e.g., EN10) or Town.
- **Strict Postcode Match**: If a full postcode is available in the listing, an exact match in PPD is required.
- **Date Window**: Scans for transactions occurring between 3 months before and 5 years after the withdrawal date.
- **House Number Filter**: A "hard filter" that ensures the door/house number matches between the listing and the PPD record.
- **Address Similarity**: Uses fuzzy matching (via `rapidfuzz`) to compare normalized address strings.

### 3. Confidence & Risk Scoring
Each potential match is assigned:
- **Confidence Score**: Calculated based on address similarity, date proximity, and postcode match.
- **Risk Level**: Categorized as `LOW`, `MEDIUM`, `HIGH`, or `CRITICAL` based on the number of days between the withdrawal and the actual sale.

Matches meeting the minimum confidence threshold are stored as `FraudMatch` records with a status of `suspicious`.

---

## Stage 2: Land Registry Verification (Final Confirmation)

The second stage performs final verification using the **HM Land Registry Online Owner Verification (OOV)** service. This is a "surgical" check performed only on suspicious matches identified in Stage 1.

### 1. Ownership Verification
The system calls the Land Registry Business Gateway API with the property details and the name of the agency's client (the vendor).

### 2. HMLR Outcome
The Land Registry returns whether the provided name matches the current or historical registered owners of the property.
- **Match Found**: The client is or was a registered owner.
- **No Valid Match**: The client was never a registered owner of that property.
- **Property Not Found**: The Land Registry could not locate the property with the provided details.

### 3. Final Confirmation
If HMLR returns a match, the system performs a final fuzzy comparison between the name returned by HMLR and the agency's client name.
- If they match, the status is updated to `confirmed_fraud`.
- If they do not match, or if HMLR found no match, the status is updated to `not_fraud`.

---

## Technical Components

- **`FraudDetector`**: Orchestrates Stage 1 logic.
- **`VerificationService`**: Orchestrates Stage 2 logic.
- **`PPDService`**: Handles local PPD data querying.
- **`LandRegistryClient`**: Manages SOAP communication with HMLR.
