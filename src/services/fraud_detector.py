"""
Fraud detector service for Stage 1: Suspicious Match Detection.

Compiles withdrawn properties against PPD data to identify potential fraud cases.
"""

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Optional

import pandas as pd
from sqlalchemy import select, func, or_
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
_TITLE_NUMBER_MATCH_SCORE = 100.0
_UNIT_MARKERS = ("FLAT", "APT", "APARTMENT", "UNIT")
_UNIT_RE = re.compile(
    r"^\s*(?:FLAT|APT|APARTMENT|UNIT)\s+([A-Z0-9-]+)\b(?:\s*,?\s*(\d+[A-Z]?))?",
    re.IGNORECASE,
)
_PLOT_RE = re.compile(r"^\s*PLOT\s+([A-Z0-9-]+)\b", re.IGNORECASE)
_NUMBER_RE = re.compile(r"^\s*(?:NO\.?\s*)?(\d+[A-Za-z]?)\b", re.IGNORECASE)


@dataclass(frozen=True)
class AddressIdentity:
    kind: str
    primary_id: str = ""
    building_id: str = ""


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


def _house_num(text: str) -> str:
    """Extract the leading door/house number from an address or PAON/SAON field.

    Strips commas and periods first (CSV artefacts like "53,") then matches
    only the leading numeric token.  Returns '' when nothing found so that
    callers can safely skip the check rather than false-rejecting.

    Examples:
        "53 Lammasmead, Wormley"  -> "53"
        "33B, LAMMASMEAD"         -> "33B"
        "53,"                     -> "53"
        "Plot 42, Silver Birch"   -> ""   (starts with a word, not a number)
        "Oliva, Middle Street"    -> ""   (named property)
        ""                        -> ""
    """
    cleaned = re.sub(r"[,.]+", "", text.strip())
    m = re.match(r"^(\d+[A-Za-z]?)\b", cleaned)
    return m.group(1).upper() if m else ""


def _clean_token(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).upper()


def _parse_unit_identity(text: str) -> Optional[AddressIdentity]:
    match = _UNIT_RE.match(text)
    if not match:
        return None
    unit_id = _clean_token(match.group(1))
    building_id = _clean_token(match.group(2))
    return AddressIdentity(kind="flat", primary_id=unit_id, building_id=building_id)


def _parse_plot_identity(text: str) -> Optional[AddressIdentity]:
    match = _PLOT_RE.match(text)
    if not match:
        return None
    return AddressIdentity(kind="plot", primary_id=_clean_token(match.group(1)))


def _parse_street_number_identity(text: str) -> Optional[AddressIdentity]:
    match = _NUMBER_RE.match(text)
    if not match:
        return None
    return AddressIdentity(kind="street_number", primary_id=_clean_token(match.group(1)))


def _listing_identity(address: Optional[str]) -> AddressIdentity:
    text = _clean_token(address)
    if not text:
        return AddressIdentity(kind="unknown")

    for parser in (_parse_unit_identity, _parse_plot_identity, _parse_street_number_identity):
        identity = parser(text)
        if identity:
            return identity

    return AddressIdentity(kind="named_building")


def _ppd_identity(ppd_row: pd.Series) -> AddressIdentity:
    saon = _clean_token(ppd_row.get("saon"))
    paon = _clean_token(ppd_row.get("paon"))
    full_address = _clean_token(ppd_row.get("full_address"))

    unit_identity = _parse_unit_identity(saon) or _parse_unit_identity(full_address)
    if unit_identity:
        building_id = unit_identity.building_id or _clean_token(_house_num(paon))
        return AddressIdentity(
            kind="flat",
            primary_id=unit_identity.primary_id,
            building_id=building_id,
        )

    plot_identity = _parse_plot_identity(paon) or _parse_plot_identity(full_address)
    if plot_identity:
        return plot_identity

    street_identity = _parse_street_number_identity(paon) or _parse_street_number_identity(
        full_address
    )
    if street_identity:
        return street_identity

    return AddressIdentity(kind="named_building" if paon or full_address else "unknown")


def _identities_are_compatible(
    listing_identity: AddressIdentity, ppd_identity: AddressIdentity
) -> bool:
    if listing_identity.kind == "plot":
        return (
            ppd_identity.kind == "plot"
            and listing_identity.primary_id
            and listing_identity.primary_id == ppd_identity.primary_id
        )

    if listing_identity.kind == "flat":
        if ppd_identity.kind != "flat":
            return False
        if (
            listing_identity.primary_id
            and ppd_identity.primary_id
            and listing_identity.primary_id != ppd_identity.primary_id
        ):
            return False
        if (
            listing_identity.building_id
            and ppd_identity.building_id
            and listing_identity.building_id != ppd_identity.building_id
        ):
            return False
        return bool(listing_identity.primary_id or ppd_identity.primary_id)

    if listing_identity.kind == "street_number":
        return (
            ppd_identity.kind == "street_number"
            and listing_identity.primary_id
            and listing_identity.primary_id == ppd_identity.primary_id
        )

    if listing_identity.kind == "named_building" and ppd_identity.kind in {
        "flat",
        "plot",
    }:
        return False

    return True


def _identity_match_bonus(
    listing_identity: AddressIdentity, ppd_identity: AddressIdentity
) -> float:
    if listing_identity.kind != ppd_identity.kind:
        return 0.0
    if (
        listing_identity.primary_id
        and listing_identity.primary_id == ppd_identity.primary_id
    ):
        return 15.0
    return 0.0


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
        """
        logger.info(f"[Fraud Detection] Starting scan for agency: {agency_id}")

        # Get total count and unique statuses for debugging
        total_stmt = select(func.count(PropertyListing.id)).where(PropertyListing.agency_id == agency_id)
        status_stmt = select(PropertyListing.status).where(PropertyListing.agency_id == agency_id).distinct()
        
        total_count = (await db.execute(total_stmt)).scalar() or 0
        status_results = (await db.execute(status_stmt)).scalars().all()
        unique_statuses = [s for s in status_results if s is not None]

        # Get withdrawn properties
        stmt = select(PropertyListing).where(
            PropertyListing.agency_id == agency_id,
            or_(
                func.lower(PropertyListing.status).in_(["withdrawn", "withdrawn date", "withdrawn_date"]),
                PropertyListing.withdrawn_date.isnot(None)
            ),
        )
        result = await db.execute(stmt)
        properties = result.scalars().all()

        if not properties:
            logger.warning(f"[Fraud Detection] No withdrawn properties found for agency {agency_id}.")
            return SuspiciousMatchSummary(
                total_matches=0,
                confidence_distribution=ConfidenceDistribution(
                    high_confidence=0, medium_confidence=0, low_confidence=0
                ),
                matches=[],
                message="No withdrawn properties found for fraud detection",
            )

        logger.info("[Fraud Detection] Analyzing %s withdrawn properties", len(properties))

        # Query PPD data via DuckDB
        ppd_df = self.ppd_service.query_ppd_for_properties(properties)

        if ppd_df.empty:
            logger.info("[Fraud Detection] No PPD records found in candidate window")
            return SuspiciousMatchSummary(
                total_matches=0,
                confidence_distribution=ConfidenceDistribution(
                    high_confidence=0, medium_confidence=0, low_confidence=0
                ),
                matches=[],
                message="No PPD records found matching criteria",
            )

        if 'transfer_date' in ppd_df.columns:
            ppd_df['transfer_date_dt'] = pd.to_datetime(ppd_df['transfer_date'])

        # Match properties against PPD data
        all_matches = []
        for prop in properties:
            # MULTI-MATCH: We return all valid matches for transparency, 
            # but apply strict geographic boundaries to prevent cross-town noise.
            matches = await self._match_property_to_ppd_multi(prop, ppd_df, db)
            all_matches.extend(matches)

        logger.info("[Fraud Detection] Scan complete. Detected %s suspicious matches.", len(all_matches))

        # Distribution and Schemas
        high_confidence = sum(1 for m in all_matches if m.confidence_score >= config.HIGH_CONFIDENCE_THRESHOLD)
        medium_confidence = sum(1 for m in all_matches if config.MIN_CONFIDENCE_THRESHOLD <= m.confidence_score < config.HIGH_CONFIDENCE_THRESHOLD)
        low_confidence = sum(1 for m in all_matches if m.confidence_score < config.MIN_CONFIDENCE_THRESHOLD)

        match_schemas = [
            FraudMatchSchema(
                id=m.id,
                property_listing_id=m.property_listing_id,
                property_address=m.property_listing.address,
                client_name=m.property_listing.client_name,
                vendor_name=m.property_listing.vendor_name,
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
            message=f"Stage 1 complete: {len(all_matches)} matches detected. Geographic noise filtered.",
        )

    async def _match_property_to_ppd_multi(
        self, property: PropertyListing, ppd_dataframe: pd.DataFrame, db: AsyncSession
    ) -> List[FraudMatch]:
        """
        Match property against PPD and return ALL valid matches for transparency.
        
        Strictness:
        - Must share outward postcode (e.g. EN10) or Town.
        - Cross-town matches (different outward postcode) are REJECTED unless address is >98% match.
        """
        prop_pc = (property.postcode or "").strip().upper()
        # Determine if we have a full postcode (e.g. "EN10 6PX") or just outward (e.g. "EN10")
        has_full_postcode = " " in prop_pc
        outward_pc = prop_pc.split()[0] if has_full_postcode else prop_pc[:4]
        prop_town = (property.region or "").strip().lower()
        
        candidates = ppd_dataframe
        
        # 1. Geographic Filter — narrow to outward postcode or town first
        if outward_pc:
            candidates = candidates[candidates['postcode'].str.startswith(outward_pc, na=False)]
        elif prop_town:
            candidates = ppd_dataframe[
                (ppd_dataframe['town'].str.lower() == prop_town) | 
                (ppd_dataframe['locality'].str.lower() == prop_town)
            ]

        if candidates.empty:
            return []

        # 1b. STRICT POSTCODE MATCH — if the listing has a full postcode, require exact match.
        #     Postcodes do not change when a property is sold. A different full postcode means
        #     a physically different house, even if the street name looks similar.
        if has_full_postcode:
            exact_match = candidates[candidates['postcode'].str.upper() == prop_pc]
            if not exact_match.empty:
                # Prefer exact-postcode candidates. Only fall back if we have nothing.
                candidates = exact_match
            else:
                # Listing postcode not found in PPD — could be a data entry error.
                # Log and skip — don't let cross-postcode noise through.
                logger.debug(
                    "[Fuzzy Matching] No exact postcode match for %s (%s) in PPD. Skipping.",
                    property.address[:40], prop_pc
                )
                return []

        # 2. Date Filter (3 months before to 5 years after)
        wd = _as_date(property.withdrawn_date)
        if wd:
            start_date = pd.Timestamp(wd - timedelta(days=config.LOOKBACK_MONTHS * 30))
            end_date = pd.Timestamp(wd + timedelta(days=config.LOOKAHEAD_MONTHS * 30))
            if 'transfer_date_dt' in candidates.columns:
                candidates = candidates[
                    (candidates['transfer_date_dt'] >= start_date) & 
                    (candidates['transfer_date_dt'] <= end_date)
                ]

        if candidates.empty:
            return []

        # 3. Fuzzy Matching — candidates are now guaranteed to be the same postcode
        matches = []
        listing_identity = _listing_identity(property.address or "")

        base_addr = property.address or ""
        if (
            listing_identity.kind == "street_number"
            and listing_identity.primary_id
            and listing_identity.primary_id not in base_addr.upper()
        ):
            base_addr = f"{property.property_number} {base_addr}".strip()

        prop_normalized = property.normalized_address or self.address_normalizer.normalize(
            base_addr, property.postcode
        )

        for _, ppd_row in candidates.iterrows():
            ppd_identity = _ppd_identity(ppd_row)
            if not _identities_are_compatible(listing_identity, ppd_identity):
                logger.debug(
                    "[Fuzzy Matching] Rejected identity mismatch listing=%s ppd=%s address=%s ppd_address=%s",
                    listing_identity,
                    ppd_identity,
                    property.address[:60],
                    str(ppd_row.get("full_address", ""))[:60],
                )
                continue

            ppd_norm = str(ppd_row.get("normalized_address", ""))
            address_similarity = self.address_normalizer.calculate_similarity(prop_normalized, ppd_norm)

            identity_boost = _identity_match_bonus(listing_identity, ppd_identity)
            if identity_boost:
                address_similarity = min(100.0, address_similarity + identity_boost)

            ppd_pc = str(ppd_row.get("postcode", "")).strip().upper()

            if address_similarity < config.MIN_ADDRESS_SIMILARITY and not identity_boost:
                continue

            confidence_score = self._calculate_confidence_score(property, ppd_row, address_similarity)

            if confidence_score >= config.MIN_CONFIDENCE_THRESHOLD:
                risk_level = "LOW"
                ppd_transfer_date_val = None
                days_diff = None
                if wd and pd.notna(ppd_row.get("transfer_date")):
                    ppd_date = pd.to_datetime(ppd_row["transfer_date"]).date()
                    ppd_transfer_date_val = ppd_date
                    days_diff = (ppd_date - wd).days  # signed: positive = sold AFTER withdrawal
                    risk_level = self._calculate_risk_level(abs(days_diff), confidence_score)

                # --- MATCH FOUND: Log side-by-side comparison for review ---
                logger.info(
                    "[Match Found] ══════════════════════════════════════════\n"
                    "  AGENCY LISTING:\n"
                    "    Address   : %s\n"
                    "    Postcode  : %s\n"
                    "    Withdrawn : %s\n"
                    "    Price     : %s\n"
                    "  PPD RECORD (Land Registry):\n"
                    "    Address   : %s\n"
                    "    Postcode  : %s\n"
                    "    Sold On   : %s\n"
                    "    Price     : £%s\n"
                    "    Txn ID    : %s\n"
                    "  SCORES:\n"
                    "    Address Similarity : %.1f%%\n"
                    "    Confidence Score   : %.1f%%\n"
                    "    Risk Level         : %s\n"
                    "    Days from Withdrawal to Sale: %s (+ = after, - = before)\n"
                    "══════════════════════════════════════════",
                    property.address, property.postcode, wd, property.price,
                    ppd_row.get("full_address", "N/A"), ppd_pc,
                    ppd_transfer_date_val, ppd_row.get("price", 0),
                    ppd_row.get("transaction_id", "N/A"),
                    address_similarity, confidence_score, risk_level, days_diff,
                )

                fraud_match = FraudMatch(
                    property_listing_id=property.id,
                    ppd_transaction_id=str(ppd_row.get("transaction_id", "")),
                    ppd_price=int(ppd_row.get("price", 0)),
                    ppd_transfer_date=pd.to_datetime(ppd_row.get("transfer_date")),
                    ppd_postcode=ppd_pc,
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

        if matches:
            await db.commit()
            for m in matches:
                await db.refresh(m)
                m.property_listing = property
                
        return matches

    def _calculate_risk_level(self, days_diff: int, confidence_score: float) -> str:
        if days_diff <= config.RISK_CRITICAL_DAYS: return "CRITICAL"
        elif days_diff <= config.RISK_HIGH_DAYS: return "HIGH"
        elif days_diff <= config.RISK_MEDIUM_DAYS: return "MEDIUM"
        else: return "LOW"

    def _calculate_confidence_score(self, property: PropertyListing, ppd_row: pd.Series, address_similarity: float) -> float:
        address_component = address_similarity * config.ADDRESS_SIMILARITY_WEIGHT
        date_component = 0.0
        wd = _as_date(property.withdrawn_date)
        if wd and pd.notna(ppd_row.get("transfer_date")):
            ppd_date = pd.to_datetime(ppd_row["transfer_date"]).date()
            days_diff = abs((ppd_date - wd).days)
            max_days = config.SCAN_WINDOW_MONTHS * 30
            date_score = max(0, 100 - (days_diff / max_days * 100))
            date_component = date_score * config.DATE_PROXIMITY_WEIGHT

        postcode_component = 0.0
        prop_pc = (property.postcode or "").replace(" ", "").upper()
        if prop_pc and ppd_row.get("postcode"):
            ppd_postcode = str(ppd_row["postcode"]).replace(" ", "").upper()
            if prop_pc == ppd_postcode:
                postcode_component = 100 * config.POSTCODE_MATCH_WEIGHT

        return round(min(100.0, address_component + date_component + postcode_component), 2)
