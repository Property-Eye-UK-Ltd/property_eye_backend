# field_extractors.py — Regex-based value extractors for each canonical field type.
# Called by column_mapper.py for ">idx" DSL instructions where the value is embedded inside a cell.

from __future__ import annotations

import re
from typing import Optional

from .cleaner import normalize_price_text, scrub_cell_text

# ---------------------------------------------------------------------------
# Compiled patterns
# ---------------------------------------------------------------------------

_UK_POSTCODE = re.compile(
    r"\b([A-Z]{1,2}\d[A-Z0-9]?\s*\d[A-Z]{2})\b", re.IGNORECASE
)

_DATE_LONG_MONTH = re.compile(
    r"\b(\d{1,2}\s+"
    r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{4})\b",
    re.IGNORECASE,
)
_DATE_ISO = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
_DATE_SLASH = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b")
_DATE_DOT = re.compile(r"\b(\d{1,2}\.\d{1,2}\.\d{2,4})\b")

_PRICE = re.compile(r"[\u00a3£]([\d,]+(?:\.\d{1,2})?)")
_COMMISSION_PCT = re.compile(r"(\d+(?:\.\d+)?)\s*%")
_COMMISSION_AMT = re.compile(r"[\u00a3£]([\d,]+(?:\.\d{1,2})?)")

_PROPERTY_NUMBER = re.compile(
    r"(?:(?:Flat|Apt|Unit|Plot|No\.?)\s*)?(\d+[A-Za-z]?)\b",
    re.IGNORECASE,
)

# UK county names (top-level list for heuristic extraction from address segments)
_KNOWN_COUNTIES = {
    "hertfordshire", "essex", "kent", "surrey", "suffolk", "norfolk",
    "cambridgeshire", "oxfordshire", "berkshire", "hampshire", "wiltshire",
    "dorset", "somerset", "devon", "cornwall", "gloucestershire", "warwickshire",
    "northamptonshire", "bedfordshire", "buckinghamshire", "leicestershire",
    "nottinghamshire", "derbyshire", "staffordshire", "shropshire", "cheshire",
    "lancashire", "yorkshire", "durham", "northumberland", "cumbria",
}


def extract_postcode(text: str) -> Optional[str]:
    """Extract and normalize the first UK postcode found in text."""
    m = _UK_POSTCODE.search(text)
    if not m:
        return None
    return m.group(1).upper().replace("  ", " ").strip()


def extract_date(text: str) -> Optional[str]:
    """Return the first recognisable date string in the cell."""
    for pattern in (_DATE_LONG_MONTH, _DATE_ISO, _DATE_SLASH, _DATE_DOT):
        m = pattern.search(text)
        if m:
            return scrub_cell_text(m.group(1))
    return None


def extract_price(text: str) -> Optional[str]:
    """Extract a monetary amount as a clean '£X,XXX' string."""
    cleaned = normalize_price_text(text)
    m = _PRICE.search(cleaned)
    if m:
        return f"£{m.group(1)}"
    return None


def extract_commission(text: str) -> Optional[str]:
    """Extract commission as a percentage or flat amount."""
    m = _COMMISSION_PCT.search(text)
    if m:
        return f"{m.group(1)}%"
    cleaned = normalize_price_text(text)
    m = _COMMISSION_AMT.search(cleaned)
    if m:
        return f"£{m.group(1)}"
    return None


def extract_county(text: str) -> Optional[str]:
    """Find a known UK county name from comma-separated address segments."""
    segments = [s.strip().lower() for s in text.split(",")]
    for seg in reversed(segments):
        if seg in _KNOWN_COUNTIES:
            return seg.title()
    for seg in reversed(segments):
        for county in _KNOWN_COUNTIES:
            if county in seg:
                return county.title()
    return None


def extract_region(text: str) -> Optional[str]:
    """Return the town/city segment (second-to-last non-postcode comma-segment)."""
    segments = [s.strip() for s in text.split(",") if s.strip()]
    # Drop any trailing postcode segment
    cleaned = [s for s in segments if not _UK_POSTCODE.fullmatch(s)]
    if len(cleaned) >= 2:
        return cleaned[-2]
    if cleaned:
        return cleaned[-1]
    return None


def extract_property_number(text: str) -> Optional[str]:
    """Extract a house/flat/plot number from address text."""
    m = _PROPERTY_NUMBER.search(text)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Dispatcher — used by column_mapper for ">idx" DSL
# ---------------------------------------------------------------------------

_DISPATCHER = {
    "postcode": extract_postcode,
    "withdrawn_date": extract_date,
    "date": extract_date,
    "price": extract_price,
    "listing_price": extract_price,
    "commission": extract_commission,
    "county": extract_county,
    "region": extract_region,
    "property_number": extract_property_number,
}


def extract_field(field_name: str, text: str) -> Optional[str]:
    """Dispatch to the appropriate extractor for the given canonical field name."""
    fn = _DISPATCHER.get(field_name.lower())
    if fn is None:
        return scrub_cell_text(text) or None
    return fn(text)
