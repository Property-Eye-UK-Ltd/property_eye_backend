"""
Fraud detector service for Stage 1: Suspicious Match Detection.

Compares withdrawn properties against PPD data to identify potential fraud cases.
"""

import logging
import re
from datetime import date, datetime
from typing import List, Optional

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

_COUNTY_BONUS_CAP = 5.0
_PRICE_BONUS_CAP = 5.0


def _as_date(val) -> Optional[date]:
    """Normalize SQLAlchemy date/datetime to date for PPD comparisons."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    return None


def _parse_listing_price_gbp(text: Optional[str]) -> Optional[int]:
    """Parse '£515,000' style listing price to integer GBP (None if not parseable)."""
    if not text:
        return None
    m = re.search(r"£?\s*([\d,]+)", str(text))
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _norm_token(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", str(s).strip().lower())


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

        logger.info(
            "Found %s withdrawn properties (rich fields: title=%s price=%s region=%s)",
            len(properties),
            sum(1 for p in properties if getattr(p, "title_number", None)),
            sum(1 for p in properties if getattr(p, "price", None)),
            sum(1 for p in properties if getattr(p, "region", None)),
        )

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

        # Normalize property address (reuse stored normalized_address when present)
        base_addr = property.address or ""
        if getattr(property, "property_number", None) and str(
            property.property_number
        ).strip():
            pn = str(property.property_number).strip().upper()
            if pn and pn not in base_addr.upper():
                base_addr = f"{property.property_number} {base_addr}".strip()

        prop_normalized = property.normalized_address or self.address_normalizer.normalize(
            base_addr, property.postcode
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
            wd = _as_date(property.withdrawn_date)
            if wd and pd.notna(ppd_row.get("transfer_date")):
                ppd_date = pd.to_datetime(ppd_row["transfer_date"]).date()
                days_diff = abs((ppd_date - wd).days)
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

        Base weights (see FraudDetectionConfig): address, date proximity, postcode.
        Optional small bonuses: county alignment vs PPD, listing price vs PPD price (capped).

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
        wd = _as_date(property.withdrawn_date)
        if wd and pd.notna(ppd_row.get("transfer_date")):
            ppd_date = pd.to_datetime(ppd_row["transfer_date"]).date()
            days_diff = abs((ppd_date - wd).days)

            # Score decreases as days increase (max 365 days in scan window)
            max_days = config.SCAN_WINDOW_MONTHS * 30
            date_score = max(0, 100 - (days_diff / max_days * 100))
            date_component = date_score * config.DATE_PROXIMITY_WEIGHT

        # Postcode exact match component (10% weight)
        postcode_component = 0.0
        prop_pc = (property.postcode or "").replace(" ", "").upper()
        if not prop_pc and property.address:
            m = re.search(
                r"\b([A-Z]{1,2}\d[A-Z0-9]?\s*\d[A-Z]{2})\b",
                property.address,
                re.IGNORECASE,
            )
            if m:
                prop_pc = re.sub(r"\s+", "", m.group(1).upper())
        if prop_pc and ppd_row.get("postcode"):
            ppd_postcode = str(ppd_row["postcode"]).replace(" ", "").upper()

            if prop_pc == ppd_postcode:
                postcode_component = 100 * config.POSTCODE_MATCH_WEIGHT

        # County alignment bonus (listing.county vs PPD county column)
        county_bonus = 0.0
        listing_county = getattr(property, "county", None)
        ppd_county = ppd_row.get("county")
        if listing_county and pd.notna(ppd_county) and str(ppd_county).strip():
            lc = _norm_token(listing_county)
            pc = _norm_token(str(ppd_county))
            if lc and pc and (lc in pc or pc in lc or lc == pc):
                county_bonus = _COUNTY_BONUS_CAP

        # Listing price vs PPD transaction price proximity (optional)
        price_bonus = 0.0
        listing_price = _parse_listing_price_gbp(getattr(property, "price", None))
        ppd_price = ppd_row.get("price")
        if (
            listing_price
            and pd.notna(ppd_price)
            and int(ppd_price) > 0
        ):
            ratio = min(listing_price, int(ppd_price)) / max(
                listing_price, int(ppd_price)
            )
            if ratio >= 0.85:
                price_bonus = _PRICE_BONUS_CAP
            elif ratio >= 0.70:
                price_bonus = _PRICE_BONUS_CAP * 0.6

        # Calculate total confidence score
        confidence_score = (
            address_component
            + date_component
            + postcode_component
            + county_bonus
            + price_bonus
        )
        confidence_score = min(100.0, confidence_score)

        return round(confidence_score, 2)
