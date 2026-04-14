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
    r"\b(\d{1,2})\s+"
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(\d{4})\b",
    re.IGNORECASE,
)
_DATE_ISO = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")
_DATE_SLASH = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b")
_DATE_DOT = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{2,4})\b")

# Matches price amounts with optional pence — allows no-space before £
_PRICE = re.compile(r"[\u00a3£]\s*([\d,]+(?:\.\d{1,2})?)")
_COMMISSION_PCT = re.compile(r"(\d+(?:\.\d+)?)\s*%")
_COMMISSION_AMT = re.compile(r"[\u00a3£]\s*([\d,]+(?:\.\d{1,2})?)")

_PROPERTY_NUMBER = re.compile(
    r"(?:(?:Flat|Apt|Unit|Plot|No\.?)\s*)?(\d+[A-Za-z]?)\b",
    re.IGNORECASE,
)

# HMLR title number: 2–3 uppercase district letters + 1–6 digits (e.g. HD567890, TGL12345)
# Must be word-bounded to avoid matching partial strings inside larger codes.
_TITLE_NUMBER = re.compile(r"\b([A-Z]{2,3}\d{1,6})\b")

# Month name → number mapping for long-form date conversion
_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

# UK county names (top-level list for heuristic extraction from address segments)
_KNOWN_COUNTIES = {
    "hertfordshire", "essex", "kent", "surrey", "suffolk", "norfolk",
    "cambridgeshire", "oxfordshire", "berkshire", "hampshire", "wiltshire",
    "dorset", "somerset", "devon", "cornwall", "gloucestershire", "warwickshire",
    "northamptonshire", "bedfordshire", "buckinghamshire", "leicestershire",
    "nottinghamshire", "derbyshire", "staffordshire", "shropshire", "cheshire",
    "lancashire", "yorkshire", "durham", "northumberland", "cumbria",
}


def _normalise_year(year_str: str) -> int:
    """Convert 2-digit year to 4-digit (assumes 2000s)."""
    y = int(year_str)
    return y + 2000 if y < 100 else y


def _to_dmy(day: int, month: int, year: int) -> str:
    """Format as DD/MM/YYYY."""
    return f"{day:02d}/{month:02d}/{year:04d}"


def extract_postcode(text: str) -> Optional[str]:
    """Extract and normalize the first UK postcode found in text."""
    m = _UK_POSTCODE.search(text)
    if not m:
        return None
    return re.sub(r"\s+", " ", m.group(1).upper()).strip()


def extract_date(text: str) -> Optional[str]:
    """Return the first recognisable date, normalized to DD/MM/YYYY."""
    # "17 January 2020" style
    m = _DATE_LONG_MONTH.search(text)
    if m:
        day = int(m.group(1))
        month = _MONTH_MAP[m.group(2).lower()]
        year = int(m.group(3))
        return _to_dmy(day, month, year)

    # "2020-01-17" ISO style
    m = _DATE_ISO.search(text)
    if m:
        year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return _to_dmy(day, month, year)

    # "17/01/2020" or "17/01/20" slash style
    m = _DATE_SLASH.search(text)
    if m:
        day, month = int(m.group(1)), int(m.group(2))
        year = _normalise_year(m.group(3))
        return _to_dmy(day, month, year)

    # "17.01.2020" dot style
    m = _DATE_DOT.search(text)
    if m:
        day, month = int(m.group(1)), int(m.group(2))
        year = _normalise_year(m.group(3))
        return _to_dmy(day, month, year)

    return None


def extract_price(text: str) -> Optional[str]:
    """Extract only the monetary amount as '£X,XXX', stripping any surrounding narrative."""
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


def extract_title_number(text: str) -> Optional[str]:
    """
    Extract an HMLR title number (2–3 district letters + 1–6 digits, e.g. HD567890).
    Returns the value uppercased and stripped; None if no valid pattern found.
    """
    cleaned = scrub_cell_text(text).upper()
    m = _TITLE_NUMBER.search(cleaned)
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
    "title_number": extract_title_number,
}


def extract_field(field_name: str, text: str) -> Optional[str]:
    """Dispatch to the appropriate extractor for the given canonical field name."""
    fn = _DISPATCHER.get(field_name.lower())
    if fn is None:
        return scrub_cell_text(text) or None
    return fn(text)
