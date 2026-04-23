"""
Arq worker for background tasks.

Handles scheduled polling of Land Registry results when the service is out of hours.
"""

import asyncio
import json
import logging
from datetime import datetime

from arq import cron
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from src.core.config import settings
from src.models.fraud_match import FraudMatch
from src.services.land_registry_client import LandRegistryClient
from src.services.verification_service import VerificationService
from src.services.address_normalizer import AddressNormalizer

logger = logging.getLogger(__name__)

async def poll_hmlr_task(ctx, match_id: str):
    """
    Background task to poll HMLR for a queued verification result.
    """
    logger.info(f"Polling HMLR for match_id: {match_id}")
    
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with async_session() as db:
        lr_client = LandRegistryClient()
        
        try:
            from sqlalchemy import select
            from sqlalchemy.orm import selectinload
            
            stmt = select(FraudMatch).where(FraudMatch.id == match_id).options(selectinload(FraudMatch.property_listing))
            result = await db.execute(stmt)
            fraud_match = result.scalar_one_or_none()
            
            if not fraud_match:
                logger.error(f"FraudMatch {match_id} not found for polling")
                return
            
            # The message_id used for OOV is what we need to poll.
            raw_resp = json.loads(fraud_match.land_registry_response) if fraud_match.land_registry_response else {}
            message_id = raw_resp.get("external_reference")
            
            if not message_id:
                logger.error(f"MessageId not found in stored response for match {match_id}")
                return
                
            poll_response = await lr_client.poll_verification_result(message_id)
            
            if poll_response.status_code == "bg.acknowledgement":
                logger.info(f"HMLR still reporting queued for match {match_id}. Expected at {poll_response.expected_response_datetime}")
                # We could potentially raise an error here to trigger an arq retry,
                # or just let it finish and rely on the next scheduled poll if we had one.
                # HMLR says "poll at specified time", so we should ideally retry.
                raise Exception(f"HMLR request still queued for match {match_id}")
            
            # Process the outcome using the refactored method
            outcome = lr_client.process_oov_outcome(poll_response, fraud_match.property_listing.client_name)
            
            # Update the FraudMatch record
            fraud_match.land_registry_response = json.dumps(outcome.raw_response) if outcome.raw_response else None
            fraud_match.verified_at = datetime.utcnow()
            
            if outcome.verification_status == "error":
                fraud_match.verification_status = "error"
                fraud_match.is_confirmed_fraud = False
            elif outcome.verification_status == "not_fraud":
                fraud_match.verification_status = "not_fraud"
                fraud_match.is_confirmed_fraud = False
            elif outcome.verification_status == "ok":
                # Do fuzzy comparison logic like in VerificationService
                service = VerificationService(lr_client)
                is_match = service._compare_owner_names(
                    outcome.owner_name, fraud_match.property_listing.client_name
                )
                fraud_match.verified_owner_name = outcome.owner_name
                if is_match:
                    fraud_match.verification_status = "confirmed_fraud"
                    fraud_match.is_confirmed_fraud = True
                else:
                    fraud_match.verification_status = "not_fraud"
                    fraud_match.is_confirmed_fraud = False
            
            await db.commit()
            logger.info(f"Successfully updated match {match_id} status to {fraud_match.verification_status}")
            
        except Exception as e:
            logger.error(f"Error in poll_hmlr_task for match {match_id}: {e}")
            # Raise to trigger arq retry if appropriate
            raise
        finally:
            await lr_client.close()
            await engine.dispose()


async def startup(ctx):
    logger.info("Worker starting up...")

async def shutdown(ctx):
    logger.info("Worker shutting down...")

from arq.connections import RedisSettings

class WorkerSettings:
    functions = [poll_hmlr_task]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = RedisSettings.from_dsn(settings.REDIS_URL)

