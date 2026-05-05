# Land Registry Integration (OOV)

Property Eye integrates with the HM Land Registry (HMLR) Business Gateway to perform definitive ownership verification using the **Online Owner Verification (OOV)** service.

---

## Technical Overview

The integration uses the HMLR Business Gateway's SOAP-based API.

### 1. Security & Authentication
- **Mutual TLS (mTLS)**: Communication requires client certificates issued by HMLR. The system must provide its certificate and private key for every request.
- **WS-Security**: In addition to TLS, requests include a `UsernameToken` in the SOAP header containing HMLR-provided credentials.
- **CA Verification**: The system verifies the HMLR server's certificate against a specific HMLR CA bundle (system CAs are often insufficient).

### 2. SOAP Communication
- **Protocol**: SOAP 1.1.
- **Endpoint**: Configurable for Test and Production environments.
- **Requests**: Built manually using XML templates to ensure strict compliance with the HMLR XSD schemas.
- **Responses**: Parsed using `xmltodict` with namespace processing.

---

## Online Owner Verification (OOV) Logic

The OOV service allows checking if a specific person is or was the owner of a property.

### Input Parameters
- **Subject Property**: Identified by either **Title Number** (preferred) or **Property Address** (Building Number/Name, Street, Town, Postcode).
- **Person Name**: The system provides the Forename and Surname of the agency's client.
- **Indicators**:
    - `HistoricalMatch`: Set to `true` to check if the person was a previous owner.
    - `PartialMatching`: Set to `true` to allow for minor variations in name spelling.

### Verification Outcomes
The Land Registry returns a `TypeCode` indicating the result:
- **`30` (Success)**: The service completed. It may return `MATCH_FOUND` or `NO_MATCHES`.
- **`20` (Rejection)**: The request was rejected, often because the property could not be found or the input format was invalid.
- **`10` (Acknowledgement)**: The request was received but not yet processed (rare in this synchronous flow).

---

## Implementation Details

- **`LandRegistryClient`**: The core class handling mTLS, XML construction, and parsing.
- **Postcode Normalization**: Postcodes are normalized to uppercase with a single space (e.g., `EN10 6PX`) as required by HMLR business rules.
- **Address Splitting**: Free-form addresses from agency listings are heuristically split into Building Name/Number and Street Name for the SOAP payload.
- **Error Handling**: The client handles DNS resolution errors, timeouts, SOAP faults, and API rejections with detailed logging.
