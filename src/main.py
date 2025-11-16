"""
Property Eye Fraud Detection POC - Main Application Entry Point
"""

import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.v1.endpoints import agencies, documents, fraud_reports, verification
from src.core.config import settings
from src.db.base import engine
from src.utils.constants import config

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title=settings.APP_NAME,
    description="""
    POC system for detecting property fraud by comparing agency listings against UK Land Registry data.
    
    ## Two-Stage Fraud Detection Workflow
    
    ### Stage 1: Suspicious Match Detection
    - Upload agency documents via `/api/v1/documents/upload`
    - Scan for suspicious matches via `/api/v1/fraud/scan`
    - Review matches with confidence scores and distribution
    - No Land Registry API calls made in this stage
    
    ### Stage 2: Land Registry Verification
    - Select high-confidence matches for verification
    - Verify via `/api/v1/verification/verify`
    - Land Registry API confirms or rules out fraud
    - Results categorized as: confirmed_fraud, not_fraud, or error
    
    ## Key Features
    - Multi-format document upload (CSV, Excel, PDF)
    - Configurable field mapping for agency data
    - Efficient PPD querying with DuckDB and Parquet storage
    - Fuzzy address matching with confidence scoring
    - Two-stage verification to minimize API costs
    """,
    version=settings.APP_VERSION,
    redoc_url="/docs",
    # docs_url="/docs",
)

# Configure CORS for future frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(agencies.router, prefix=settings.API_V1_PREFIX)
app.include_router(documents.router, prefix=settings.API_V1_PREFIX)
app.include_router(fraud_reports.router, prefix=settings.API_V1_PREFIX)
app.include_router(verification.router, prefix=settings.API_V1_PREFIX)


@app.get("/", tags=["Health"])
async def root():
    """Root endpoint - API health check"""
    return {
        "status": "ok",
        "message": settings.APP_NAME,
        "version": settings.APP_VERSION,
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """
    Health check endpoint.

    Verifies:
    - API is running
    - PPD volume is accessible
    """
    ppd_volume = Path(config.PPD_VOLUME_PATH)
    ppd_accessible = ppd_volume.exists() and ppd_volume.is_dir()

    return {
        "status": "healthy" if ppd_accessible else "degraded",
        "service": "fraud-detection-api",
        "version": settings.APP_VERSION,
        "ppd_volume_accessible": ppd_accessible,
        "ppd_volume_path": str(ppd_volume),
    }


@app.on_event("startup")
async def startup_event():
    """Initialize resources on application startup"""
    logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")

    # Verify PPD volume path exists
    ppd_volume = Path(config.PPD_VOLUME_PATH)
    if not ppd_volume.exists():
        logger.warning(f"PPD volume path does not exist: {ppd_volume}")
        logger.info(f"Creating PPD volume directory: {ppd_volume}")
        ppd_volume.mkdir(parents=True, exist_ok=True)
    else:
        logger.info(f"PPD volume path verified: {ppd_volume}")

    # Verify CSV volume path exists
    csv_volume = Path(config.CSV_VOLUME_PATH)
    if not csv_volume.exists():
        logger.info(f"Creating CSV volume directory: {csv_volume}")
        csv_volume.mkdir(parents=True, exist_ok=True)
    else:
        logger.info(f"CSV volume path verified: {csv_volume}")

    # Auto-sync PPD data if enabled
    if config.SYNC_PPD:
        logger.info("SYNC_PPD enabled - starting automatic PPD ingestion")
        from src.services.ppd_sync_service import PPDSyncService

        sync_service = PPDSyncService()
        sync_summary = await sync_service.sync_ppd_data()

        logger.info(
            f"PPD sync complete: {sync_summary['newly_ingested']} new files ingested, "
            f"{sync_summary['already_ingested']} already processed, "
            f"{sync_summary['failed']} failed"
        )

        if sync_summary["errors"]:
            for error in sync_summary["errors"]:
                logger.error(f"  - {error}")
    else:
        logger.info("SYNC_PPD disabled - skipping automatic PPD ingestion")

    logger.info("Application startup complete")


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on application shutdown"""
    logger.info("Shutting down application")

    # Close database connection
    await engine.dispose()

    logger.info("Application shutdown complete")
