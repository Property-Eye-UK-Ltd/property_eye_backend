# 🏡 Property Eye: Commission Fraud Detection Backend

Property Eye is a high-performance backend system built with **FastAPI** designed to automate the detection and flagging of commission fraud within UK real estate agencies. It acts as a continuous digital watchdog, enabling agencies to recover lost revenue from "off-market" or "behind closed doors" transactions.

## 🎯 Core Problem & Solution

### The Problem

Real estate agencies frequently lose rightful commissions when clients they introduced to a property subsequently bypass the agency and complete the sale privately. This often happens after a listing is officially withdrawn.

### The Solution

Property Eye systematically ingests agency listing data and cross-references it with publicly available UK Land Registry Price Paid Data (PPD) to identify suspicious sales in real-time. It then initiates a high-confidence verification phase to confirm critical fraud instances.

## ✨ Key Features

- **Asynchronous Data Ingestion:** Handles large uploads (CSV/PDF) of agency listings and automatically processes them via a background worker queue (simulated using BullMQ/RQ concepts).

- **Two-Phase Fraud Detection Pipeline:**

  1. **Screening:** Bulk comparison of withdrawn agency listings against the UK Land Registry PPD.

  2. **Critical Verification:** Real-time, targeted API calls to confirm new ownership matches agency client records.

- **Performance & Documentation:** Built on FastAPI, ensuring blazing fast performance, native asynchronous support, and best-in-class automated API documentation (`/docs`).

- **Data Normalization:** Utilizes data processing libraries (Pandas) to clean and standardize property addresses and client names for robust fuzzy matching.

## 💻 Technology Stack

| Category                   | Technology              | Purpose                                                                         |
| -------------------------- | ----------------------- | ------------------------------------------------------------------------------- |
| **Backend Framework**      | FastAPI                 | High-performance API layer.                                                     |
| **Data Validation/Config** | Pydantic                | Schema definition, configuration management (`core/config.py`).                 |
| **Database ORM**           | SQLAlchemy 2.0+ (Async) | Persistent data storage for clients, listings, and fraud reports.               |
| **Task Queue**             | BullMQ/RQ (Concept)     | Handles heavy lifting like file parsing and bulk data ingestion asynchronously. |
| **Caching**                | Redis                   | Caching results of expensive, paid third-party verification API calls.          |
| **Data Processing**        | Pandas, OCR tools       | Ingestion and normalization of raw agency/public data.                          |

## 🚀 Getting Started

### Prerequisites

1. Python 3.11+

2. `pip` (Python package installer)

3. Docker (Recommended for running Redis/Database locally)

### 1. Setup Environment
