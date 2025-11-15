"""
Verification service for Stage 2: Land Registry Verification.

Processes suspicious matches through Land Registry API to confirm fraud cases.
"""

import json
import logging
from datetime import datetime
from typing import List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.fraud_match import FraudMatch
from src.schemas.verification import VerificationResult, VerificationSummary
from src.services.address_normalizer import AddressNormalizer
from src.services.land_registry_client import LandRegistryClient
from src.utils.constants import config

logger = logging.getLogger(__name__)


class VerificationService:
    """
    Service for verifying suspicious matches via Land Registry API (Stage 2).

    Processes suspicious matches to confirm or rule out fraud by comparing
    Land Registry owner data with agency client records.
    """

    def __init__(self, land_registry_client: LandRegistryClient):
        """
        Initialize verification service.

        Args:
            land_registry_client: Client for Land Registry API calls
        """
        self.land_registry_client = land_registry_client
        self.address_normalizer = AddressNormalizer()

    async def verify_suspicious_matches(
        self, match_ids: List[str], db: AsyncSession
    ) -> VerificationSummary:
        """
        Stage 2: Verify suspicious matches via Land Registry API.

        Steps:
        1. Retrieve suspicious matches by IDs
        2. For each match, call Land Registry API
        3. Compare returned owner name with agency client name
        4. Update match status (confirmed_fraud, not_fraud, error)
        5. Return verification summary

        Args:
            match_ids: List of fraud match IDs to verify
            db: Database session

        Returns:
            VerificationSummary with counts by status
        """
        logger.info(f"Starting verification for {len(match_ids)} matches")

        results = []
        confirmed_fraud_count = 0
        not_fraud_count = 0
        error_count = 0

        for match_id in match_ids:
            result = await self.verify_single_match(match_id, db)
            results.append(result)

            if result.verification_status == "confirmed_fraud":
                confirmed_fraud_count += 1
            elif result.verification_status == "not_fraud":
                not_fraud_count += 1
            else:
                error_count += 1

        message = (
            f"Stage 2 complete: {confirmed_fraud_count} confirmed fraud cases, "
            f"{not_fraud_count} ruled out, {error_count} error(s)"
        )

        return VerificationSummary(
            total_verified=len(match_ids),
            confirmed_fraud_count=confirmed_fraud_count,
            not_fraud_count=not_fraud_count,
            error_count=error_count,
            results=results,
            message=message,
        )

    async def verify_single_match(
        self, match_id: str, db: AsyncSession
    ) -> VerificationResult:
        """
        Verify a single suspicious match.

        Args:
            match_id: Fraud match ID
            db: Database session

        Returns:
            VerificationResult for this match
        """
        # Retrieve match from database
        stmt = select(FraudMatch).where(FraudMatch.id == match_id)
        result = await db.execute(stmt)
        fraud_match = result.scalar_one_or_none()

        if not fraud_match:
            logger.error(f"Match {match_id} not found")
            return VerificationResult(
                match_id=match_id,
                property_address="Unknown",
                client_name="Unknown",
                verification_status="error",
                verified_owner_name=None,
                is_confirmed_fraud=False,
                verified_at=datetime.utcnow(),
                error_message="Match not found in database",
            )

        # Get property listing details
        property_listing = fraud_match.property_listing

        try:
            # Call Land Registry API
            api_result = await self.land_registry_client.verify_ownership(
                property_address=fraud_match.ppd_full_address,
                postcode=fraud_match.ppd_postcode,
                expected_owner_name=property_listing.client_name,
            )

            # Store API response
            fraud_match.land_registry_response = (
                json.dumps(api_result.raw_response) if api_result.raw_response else None
            )
            fraud_match.verified_at = datetime.utcnow()

            # Check for API errors
            if api_result.verification_status == "error":
                fraud_match.verification_status = "error"
                fraud_match.is_confirmed_fraud = False
                await db.commit()

                return VerificationResult(
                    match_id=match_id,
                    property_address=property_listing.address,
                    client_name=property_listing.client_name,
                    verification_status="error",
                    verified_owner_name=None,
                    is_confirmed_fraud=False,
                    verified_at=fraud_match.verified_at,
                    error_message=api_result.error_message,
                )

            # Compare owner names
            fraud_match.verified_owner_name = api_result.owner_name

            is_match = self._compare_owner_names(
                api_result.owner_name, property_listing.client_name
            )

            if is_match:
                fraud_match.verification_status = "confirmed_fraud"
                fraud_match.is_confirmed_fraud = True
                logger.info(f"Match {match_id} confirmed as fraud")
            else:
                fraud_match.verification_status = "not_fraud"
                fraud_match.is_confirmed_fraud = False
                logger.info(f"Match {match_id} ruled out as fraud")

            await db.commit()

            return VerificationResult(
                match_id=match_id,
                property_address=property_listing.address,
                client_name=property_listing.client_name,
                verification_status=fraud_match.verification_status,
                verified_owner_name=fraud_match.verified_owner_name,
                is_confirmed_fraud=fraud_match.is_confirmed_fraud,
                verified_at=fraud_match.verified_at,
                error_message=None,
            )

        except Exception as e:
            logger.error(f"Error verifying match {match_id}: {str(e)}")

            fraud_match.verification_status = "error"
            fraud_match.is_confirmed_fraud = False
            fraud_match.verified_at = datetime.utcnow()
            await db.commit()

            return VerificationResult(
                match_id=match_id,
                property_address=property_listing.address,
                client_name=property_listing.client_name,
                verification_status="error",
                verified_owner_name=None,
                is_confirmed_fraud=False,
                verified_at=fraud_match.verified_at,
                error_message=str(e),
            )

    def _compare_owner_names(self, api_owner_name: str, client_name: str) -> bool:
        """
        Fuzzy compare owner names using rapidfuzz.

        Args:
            api_owner_name: Owner name from Land Registry API
            client_name: Client name from agency records

        Returns:
            True if similarity > 85%, False otherwise
        """
        if not api_owner_name or not client_name:
            return False

        # Normalize both names
        norm_api = api_owner_name.upper().strip()
        norm_client = client_name.upper().strip()

        # Calculate similarity using address normalizer's method
        # (it uses rapidfuzz internally)
        similarity = self.address_normalizer.calculate_similarity(norm_api, norm_client)

        threshold = config.OWNER_NAME_SIMILARITY_THRESHOLD
        is_match = similarity >= threshold

        logger.info(
            f"Owner name comparison: '{norm_api}' vs '{norm_client}' "
            f"= {similarity:.2f}% (threshold: {threshold}%) -> {is_match}"
        )

        return is_match
