"""
Verification service for Stage 2: Land Registry Verification.

Processes suspicious matches through Land Registry API to confirm fraud cases.
"""

import json
import logging
import re
from datetime import datetime
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.models.fraud_match import FraudMatch
from src.models.property_listing import PropertyListing
from src.schemas.verification import VerificationResult, VerificationSummary
from src.services.address_normalizer import AddressNormalizer
from src.services.land_registry_client import LandRegistryClient
from src.utils.constants import config

logger = logging.getLogger(__name__)

_UK_POSTCODE_RE = re.compile(
    r"\b([A-Z]{1,2}\d[A-Z0-9]?\s*\d[A-Z]{2})\b", re.IGNORECASE
)


def _effective_postcode_for_lr(listing: PropertyListing, ppd_postcode: str) -> str:
    """Prefer agency postcode; else parse from address; else PPD match postcode."""
    if listing.postcode and str(listing.postcode).strip():
        return str(listing.postcode).strip()
    if listing.address:
        m = _UK_POSTCODE_RE.search(listing.address)
        if m:
            return re.sub(r"\s+", " ", m.group(1).upper()).strip()
    return (ppd_postcode or "").strip()




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
        # Retrieve match from database with property_listing eager-loaded for async
        stmt = (
            select(FraudMatch)
            .where(FraudMatch.id == match_id)
            .options(selectinload(FraudMatch.property_listing))
        )
        result = await db.execute(stmt)
        fraud_match = result.scalar_one_or_none()

        if not fraud_match:
            logger.error(f"Match {match_id} not found")
            return VerificationResult(
                match_id=match_id,
                property_address="Unknown",
                client_name="Unknown",
                vendor_name=None,
                verification_status="error",
                verified_owner_name=None,
                is_confirmed_fraud=False,
                verified_at=datetime.utcnow(),
                error_message="Match not found in database",
            )

        # Get property listing details
        property_listing = fraud_match.property_listing

        try:
            title_no = (
                (property_listing.title_number or "").strip()
                if getattr(property_listing, "title_number", None)
                else ""
            )
            verify_pc = _effective_postcode_for_lr(
                property_listing, fraud_match.ppd_postcode or ""
            )
            town = (property_listing.region or "").strip() or None

            pc_src = (
                "listing"
                if (property_listing.postcode and str(property_listing.postcode).strip())
                else (
                    "parsed_address"
                    if property_listing.address
                    and _UK_POSTCODE_RE.search(property_listing.address)
                    else "ppd_fallback"
                )
            )
            logger.info(
                "Verifying match %s: mode=%s listing_addr_len=%s postcode_source=%s town_set=%s",
                match_id,
                "title_number" if title_no else "address",
                len(property_listing.address or ""),
                pc_src,
                bool(town),
            )

            # Call Land Registry API (prefer listing + title; fall back to PPD address if empty)
            api_result = await self.land_registry_client.verify_ownership(
                property_address=property_listing.address or fraud_match.ppd_full_address,
                postcode=verify_pc or fraud_match.ppd_postcode,
                expected_owner_name=property_listing.client_name,
                message_id=fraud_match.ppd_transaction_id,
                title_number=title_no or None,
                town=town,
                building_name_or_number=property_listing.property_number,
            )

            # Store API response
            fraud_match.land_registry_response = (
                json.dumps(api_result.raw_response) if api_result.raw_response else None
            )
            fraud_match.verified_at = datetime.utcnow()

            # API/infrastructure failure — could not complete verification.
            if api_result.verification_status == "error":
                raw_preview = None
                if api_result.raw_response:
                    raw_preview = json.dumps(api_result.raw_response, default=str)
                    if len(raw_preview) > 800:
                        raw_preview = f"{raw_preview[:800]}…"
                logger.warning(
                    "HMLR verification error for match %s: %s raw=%s",
                    match_id,
                    api_result.error_message,
                    raw_preview or "<none>",
                )
                fraud_match.verification_status = "error"
                fraud_match.is_confirmed_fraud = False
                await db.commit()

                return VerificationResult(
                    match_id=match_id,
                    property_address=property_listing.address,
                    client_name=property_listing.client_name,
                    vendor_name=property_listing.vendor_name,
                    verification_status="error",
                    verified_owner_name=None,
                    is_confirmed_fraud=False,
                    verified_at=fraud_match.verified_at,
                    error_message=api_result.error_message,
                )

            # HMLR verified the property but the owner name did not match —
            # the client is confirmed NOT to be the registered owner.
            if api_result.verification_status == "not_fraud":
                fraud_match.verification_status = "not_fraud"
                fraud_match.is_confirmed_fraud = False
                await db.commit()
                logger.info(f"Match {match_id} ruled out as fraud (name mismatch from HMLR)")

                return VerificationResult(
                    match_id=match_id,
                    property_address=property_listing.address,
                    client_name=property_listing.client_name,
                    vendor_name=property_listing.vendor_name,
                    verification_status="not_fraud",
                    verified_owner_name=None,
                    is_confirmed_fraud=False,
                    verified_at=fraud_match.verified_at,
                    error_message=None,
                )

            # HMLR returned a name match — do a fuzzy compare as a second gate.
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
                vendor_name=property_listing.vendor_name,
                verification_status=fraud_match.verification_status,
                verified_owner_name=fraud_match.verified_owner_name,
                is_confirmed_fraud=fraud_match.is_confirmed_fraud,
                verified_at=fraud_match.verified_at,
                error_message=None,
            )

        except Exception as e:
            logger.exception("Error verifying match %s: %s", match_id, str(e))

            fraud_match.verification_status = "error"
            fraud_match.is_confirmed_fraud = False
            fraud_match.verified_at = datetime.utcnow()
            await db.commit()

            return VerificationResult(
                match_id=match_id,
                property_address=property_listing.address,
                client_name=property_listing.client_name,
                vendor_name=property_listing.vendor_name,
                verification_status="error",
                verified_owner_name=None,
                is_confirmed_fraud=False,
                verified_at=fraud_match.verified_at,
                error_message=str(e),
            )

    async def verify_listing_direct(
        self, listing: PropertyListing
    ) -> VerificationResult:
        """
        Directly verify a listing via HMLR without PPD screening.

        Args:
            listing: Property listing to verify

        Returns:
            VerificationResult for this listing
        """
        title_no = (
            (listing.title_number or "").strip()
            if getattr(listing, "title_number", None)
            else ""
        )
        verify_pc = _effective_postcode_for_lr(listing, "")
        town = (listing.region or "").strip() or None

        pc_src = (
            "listing"
            if (listing.postcode and str(listing.postcode).strip())
            else (
                "parsed_address"
                if listing.address and _UK_POSTCODE_RE.search(listing.address)
                else "empty"
            )
        )
        logger.info(
            "Direct HMLR verify listing %s: mode=%s listing_addr_len=%s postcode_source=%s town_set=%s",
            listing.id,
            "title_number" if title_no else "address",
            len(listing.address or ""),
            pc_src,
            bool(town),
        )

        api_result = await self.land_registry_client.verify_ownership(
            property_address=listing.address,
            postcode=verify_pc,
            expected_owner_name=listing.client_name or "",
            message_id=listing.id,
            title_number=title_no or None,
            town=town,
            building_name_or_number=listing.property_number,
        )

        verified_at = datetime.utcnow()
        client_name = listing.client_name or "Unknown"

        if api_result.verification_status == "error":
            return VerificationResult(
                match_id=listing.id,
                property_address=listing.address,
                client_name=client_name,
                vendor_name=listing.vendor_name,
                verification_status="error",
                verified_owner_name=None,
                is_confirmed_fraud=False,
                verified_at=verified_at,
                error_message=api_result.error_message,
            )

        if api_result.verification_status == "not_fraud":
            return VerificationResult(
                match_id=listing.id,
                property_address=listing.address,
                client_name=client_name,
                vendor_name=listing.vendor_name,
                verification_status="not_fraud",
                verified_owner_name=None,
                is_confirmed_fraud=False,
                verified_at=verified_at,
                error_message=None,
            )

        is_match = self._compare_owner_names(api_result.owner_name, listing.client_name or "")
        status = "confirmed_fraud" if is_match else "not_fraud"
        return VerificationResult(
            match_id=listing.id,
            property_address=listing.address,
            client_name=client_name,
            vendor_name=listing.vendor_name,
            verification_status=status,
            verified_owner_name=api_result.owner_name,
            is_confirmed_fraud=is_match,
            verified_at=verified_at,
            error_message=None,
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
