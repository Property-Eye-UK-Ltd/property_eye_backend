# Property Eye Documentation

This directory contains technical documentation for the Property Eye backend system.

## Core Systems

- **[Fraud Detection System](./fraud-detection-system.md)**: Overview of the two-stage verification process (Suspicious Match Detection and Land Registry Verification).
- **[Price Paid Data (PPD) Management](./ppd-data-management.md)**: Details on how UK Land Registry PPD data is ingested, stored in Parquet format, and queried using DuckDB.
- **[Land Registry Integration](./land-registry-integration.md)**: Technical details on the Online Owner Verification (OOV) integration with HM Land Registry Business Gateway.
- **[Data Ingestion](./data-ingestion.md)**: Documentation on how agency property listings are parsed from CSV/Excel and stored.
- **[Frontend Overview](./frontend-overview.md)**: Overview of the React-based frontend application.

## External Integrations

- **[Alto Integration Guide](./alto-integration.md)**: Guide for integrating with Zoopla's Alto platform.
- **[Alto Integration Flow](./alto-integration-flow.md)**: Detailed technical flow for Alto production and sandbox environments.
