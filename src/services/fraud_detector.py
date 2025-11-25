"""
Fraud detector service for Stage 1: Suspicious Match Detection.

Compares withdrawn properties against PPD data to identify potential fraud cases.
"""

import logging
from datetime import datetime
from typing import List

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.fraud_match import FraudMatch
from src.models.property_listing import PropertyListing
from src.schemas.fraud_report import (
    ConfidenceDistribution,
    FraudMatchSchema,
    SuspiciousMatchSummary,
)
from src.services.address_normalizer import AddressNormalizer
from src.services.ppd_service import PPDService
from src.utils.constants import config

logger = logging.getLogger(__name__)


class FraudDetector:
    """
    Service for detecting suspicious fraud matches (Stage 1).

    Compares agency withdrawn properties against PPD data using
    address matching and confidence scoring.
    """

    def __init__(self, ppd_service: PPDService, address_normalizer: AddressNormalizer):
        """
        Initialize fraud detector.

        Args:
            ppd_service: PPD service for querying Parquet data
            address_normalizer: Address normalization service
        """
        self.ppd_service = ppd_service
        self.address_normalizer = address_normalizer

    async def detect_suspicious_matches(
        self, agency_id: str, db: AsyncSession
    ) -> SuspiciousMatchSummary:
        """
        Stage 1: Detect suspicious matches without Land Registry calls.

        Steps:
        1. Get all withdrawn properties for agency
        2. Query PPD Parquet files via DuckDB with date/postcode filters
        3. Compare addresses and calculate confidence scores
        4. Store all matches with status="suspicious"
        5. Return summary with match counts and confidence distribution

        Args:
            agency_id: Agency identifier
            db: Database session

        Returns:
            SuspiciousMatchSummary with all detected matches
        """
        logger.info(f"Starting fraud detection for agency {agency_id}")

        # Get all withdrawn properties for agency
        stmt = select(PropertyListing).where(
            PropertyListing.agency_id == agency_id,
            PropertyListing.status == "withdrawn",
        )
        result = await db.execute(stmt)
        properties = result.scalars().all()

        if not properties:
            logger.info(f"No withdrawn properties found for agency {agency_id}")
            return SuspiciousMatchSummary(
                total_matches=0,
                confidence_distribution=ConfidenceDistribution(
                    high_confidence=0, medium_confidence=0, low_confidence=0
                ),
                matches=[],
                message="No withdrawn properties found for fraud detection",
            )

        logger.info(f"Found {len(properties)} withdrawn properties")

        # Query PPD data via DuckDB
        ppd_df = self.ppd_service.query_ppd_for_properties(properties)

        if ppd_df.empty:
            logger.info("No PPD records found in date/postcode range")
            return SuspiciousMatchSummary(
                total_matches=0,
                confidence_distribution=ConfidenceDistribution(
                    high_confidence=0, medium_confidence=0, low_confidence=0
                ),
                matches=[],
                message="No PPD records found matching the date and postcode criteria",
            )

        logger.info(f"Found {len(ppd_df)} PPD records to compare")

        # Match properties against PPD data
        all_matches = []
        for prop in properties:
            matches = await self._match_property_to_ppd(prop, ppd_df, db)
            all_matches.extend(matches)

        logger.info(f"Detected {len(all_matches)} suspicious matches")

        # Calculate confidence distribution
        high_confidence = sum(
            1
            for m in all_matches
            if m.confidence_score >= config.HIGH_CONFIDENCE_THRESHOLD
        )
        medium_confidence = sum(
            1
            for m in all_matches
            if config.MIN_CONFIDENCE_THRESHOLD
            <= m.confidence_score
            < config.HIGH_CONFIDENCE_THRESHOLD
        )
        low_confidence = sum(
            1
            for m in all_matches
            if m.confidence_score < config.MIN_CONFIDENCE_THRESHOLD
        )

        # Convert to schemas
        match_schemas = [
            FraudMatchSchema(
                id=m.id,
                property_listing_id=m.property_listing_id,
                property_address=m.property_listing.address,
                client_name=m.property_listing.client_name,
                withdrawn_date=m.property_listing.withdrawn_date,
                ppd_transaction_id=m.ppd_transaction_id,
                ppd_price=m.ppd_price,
                ppd_transfer_date=m.ppd_transfer_date,
                ppd_postcode=m.ppd_postcode,
                ppd_full_address=m.ppd_full_address,
                confidence_score=m.confidence_score,
                address_similarity=m.address_similarity,
                risk_level=m.risk_level,
                verification_status=m.verification_status,
                verified_owner_name=m.verified_owner_name,
                is_confirmed_fraud=m.is_confirmed_fraud,
                detected_at=m.detected_at,
                verified_at=m.verified_at,
            )
            for m in all_matches
        ]

        return SuspiciousMatchSummary(
            total_matches=len(all_matches),
            confidence_distribution=ConfidenceDistribution(
                high_confidence=high_confidence,
                medium_confidence=medium_confidence,
                low_confidence=low_confidence,
            ),
            matches=match_schemas,
            message=f"Stage 1 complete: {len(all_matches)} suspicious matches detected. "
            f"Review {high_confidence} high-confidence matches for Land Registry verification.",
        )

    async def _match_property_to_ppd(
        self, property: PropertyListing, ppd_dataframe: pd.DataFrame, db: AsyncSession
    ) -> List[FraudMatch]:
        """
        Compare property against PPD DataFrame.

        Args:
            property: PropertyListing to match
            ppd_dataframe: DataFrame with PPD records
            db: Database session

        Returns:
            List of FraudMatch objects above MIN_CONFIDENCE_THRESHOLD
        """
        matches = []

        # Normalize property address
        prop_normalized = (
            property.normalized_address
            or self.address_normalizer.normalize(property.address, property.postcode)
        )

        # Compare against each PPD record
        for _, ppd_row in ppd_dataframe.iterrows():
            # Calculate address similarity
            ppd_normalized = ppd_row.get("normalized_address", "")
            address_similarity = self.address_normalizer.calculate_similarity(
                prop_normalized, ppd_normalized
            )

            # Skip if below minimum threshold
            if address_similarity < config.MIN_ADDRESS_SIMILARITY:
                continue

            # Calculate confidence score
            confidence_score = self._calculate_confidence_score(
                property, ppd_row, address_similarity
            )

            # Calculate risk level
            risk_level = "LOW"
            if property.withdrawn_date and pd.notna(ppd_row.get("transfer_date")):
                ppd_date = pd.to_datetime(ppd_row["transfer_date"])
                days_diff = abs((ppd_date - property.withdrawn_date).days)
                risk_level = self._calculate_risk_level(days_diff, confidence_score)

            # Store if above minimum confidence threshold
            if confidence_score >= config.MIN_CONFIDENCE_THRESHOLD:
                fraud_match = FraudMatch(
                    property_listing_id=property.id,
                    ppd_transaction_id=str(ppd_row.get("transaction_id", "")),
                    ppd_price=int(ppd_row.get("price", 0)),
                    ppd_transfer_date=pd.to_datetime(ppd_row.get("transfer_date")),
                    ppd_postcode=str(ppd_row.get("postcode", "")),
                    ppd_full_address=str(ppd_row.get("full_address", "")),
                    confidence_score=confidence_score,
                    address_similarity=address_similarity,
                    risk_level=risk_level,
                    verification_status="suspicious",
                    is_confirmed_fraud=False,
                    detected_at=datetime.utcnow(),
                )

                db.add(fraud_match)
                matches.append(fraud_match)

        # Commit matches for this property
        if matches:
            await db.commit()
            # Refresh to get IDs and relationships
            for match in matches:
                await db.refresh(match)
                # Manually set the relationship to avoid lazy load later
                match.property_listing = property

        return matches

    def _calculate_risk_level(self, days_diff: int, confidence_score: float) -> str:
        """
        Calculate risk level based on date difference and confidence.

        Levels:
        - CRITICAL: 1-6 months (approx 180 days)
        - HIGH: 6 months - 1 year (approx 365 days)
        - MEDIUM: 1-3 years (approx 1095 days)
        - LOW: 3-6 years (approx 2190 days)
        """
        # Adjust based on confidence if needed, for now strictly date based as requested
        # but ensuring high confidence for Critical/High

        if days_diff <= 180:
            return "CRITICAL"
        elif days_diff <= 365:
            return "HIGH"
        elif days_diff <= 1095:
            return "MEDIUM"
        else:
            return "LOW"

    def _calculate_confidence_score(
        self, property: PropertyListing, ppd_row: pd.Series, address_similarity: float
    ) -> float:
        """
        Calculate confidence score based on multiple factors.

        Weights:
        - Address similarity: 70%
        - Date proximity: 20%
        - Postcode exact match: 10%

        Args:
            property: PropertyListing object
            ppd_row: PPD DataFrame row
            address_similarity: Pre-calculated address similarity (0-100)

        Returns:
            Confidence score (0-100)
        """
        # Address similarity component (70% weight)
        address_component = address_similarity * config.ADDRESS_SIMILARITY_WEIGHT

        # Date proximity component (20% weight)
        date_component = 0.0
        if property.withdrawn_date and pd.notna(ppd_row.get("transfer_date")):
            ppd_date = pd.to_datetime(ppd_row["transfer_date"])
            days_diff = abs((ppd_date - property.withdrawn_date).days)

            # Score decreases as days increase (max 365 days in scan window)
            max_days = config.SCAN_WINDOW_MONTHS * 30
            date_score = max(0, 100 - (days_diff / max_days * 100))
            date_component = date_score * config.DATE_PROXIMITY_WEIGHT

        # Postcode exact match component (10% weight)
        postcode_component = 0.0
        if property.postcode and ppd_row.get("postcode"):
            prop_postcode = property.postcode.replace(" ", "").upper()
            ppd_postcode = str(ppd_row["postcode"]).replace(" ", "").upper()

            if prop_postcode == ppd_postcode:
                postcode_component = 100 * config.POSTCODE_MATCH_WEIGHT

        # Calculate total confidence score
        confidence_score = address_component + date_component + postcode_component

        return round(confidence_score, 2)
