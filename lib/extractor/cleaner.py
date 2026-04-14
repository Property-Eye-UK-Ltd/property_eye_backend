# cleaner.py — Unicode/whitespace sanitizers shared across the extractor package.

from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict

# Invisible Unicode characters frequently embedded in PDF text exports
_JUNK_CHARS_RE = re.compile(r"[\u200b\u200c\u200d\ufeff\u00ad\u2060]")
# Curly/smart quotes and stray backtick/accent that corrupt numeric fields
_SMART_QUOTES_RE = re.compile(r'[\u201c\u201d\u2018\u2019"\'`´]')

_UK_POSTCODE_RE = re.compile(
    r"\b([A-Z]{1,2}\d[A-Z0-9]?\s*\d[A-Z]{2})\b", re.IGNORECASE
)

# Trailing postcode with optional preceding comma/space — used to strip from address
_TRAILING_POSTCODE_RE = re.compile(
    r",?\s*[A-Z]{1,2}\d[A-Z0-9]?\s*\d[A-Z]{2}\s*$", re.IGNORECASE
)

# Known UK counties for stripping from address (title-cased)
_COUNTY_STRIP_RE = re.compile(
    r",?\s*(?:Hertfordshire|Essex|Kent|Surrey|Suffolk|Norfolk|Cambridgeshire|"
    r"Oxfordshire|Berkshire|Hampshire|Wiltshire|Dorset|Somerset|Devon|Cornwall|"
    r"Gloucestershire|Warwickshire|Northamptonshire|Bedfordshire|Buckinghamshire|"
    r"Leicestershire|Nottinghamshire|Derbyshire|Staffordshire|Shropshire|Cheshire|"
    r"Lancashire|Yorkshire|Durham|Northumberland|Cumbria)\b",
    re.IGNORECASE,
)


def scrub_cell_text(s: str) -> str:
    """Normalize unicode, strip invisible chars, collapse whitespace/newlines to single space."""
    if not s:
        return s
    t = unicodedata.normalize("NFKC", s)
    t = _JUNK_CHARS_RE.sub("", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def normalize_price_text(s: str) -> str:
    """Normalize a raw price string: fix £ sign, remove junk chars, collapse spaces."""
    t = unicodedata.normalize("NFKC", s)
    t = t.replace("\u00a3", "£").replace("GBP", "")
    t = _JUNK_CHARS_RE.sub("", t)
    t = _SMART_QUOTES_RE.sub("", t)
    t = re.sub(r"\s+", " ", t).strip()
    # Ensure no space between £ and digits (handles "£ 699,950" or "£699,950")
    t = re.sub(r"\s*£\s*", "£", t)
    return t.strip()


def sanitize_field(key: str, value: str) -> str:
    """Dispatch per-field sanitization; price gets normalization, dates get DD/MM/YYYY."""
    key_l = key.lower()
    if key_l in ("price", "listing_price", "commission"):
        return normalize_price_text(value)
    if key_l in ("withdrawn_date", "date"):
        # Lazy import avoids circular dependency (field_extractors imports cleaner)
        from .field_extractors import extract_date
        normalized = extract_date(value)
        return normalized if normalized else scrub_cell_text(value)
    return scrub_cell_text(value)


def strip_extracted_from_address(address: str, row: Dict[str, Any]) -> str:
    """
    Remove extracted sub-fields (postcode, county) from the tail of an address string.
    Region is intentionally kept — it reads naturally as part of the address.
    """
    cleaned = address

    # Strip postcode — always trailing, safe to remove
    cleaned = _TRAILING_POSTCODE_RE.sub("", cleaned).strip().rstrip(",").strip()

    # Strip county if it was successfully extracted
    if row.get("county"):
        cleaned = _COUNTY_STRIP_RE.sub("", cleaned).strip().rstrip(",").strip()

    return cleaned or address  # never return empty string


def finalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Scrub every string field; back-fill postcode from address when missing;
    strip extracted sub-fields from the address tail.
    """
    out: Dict[str, Any] = {}
    for k, v in row.items():
        out[k] = sanitize_field(k, v) if isinstance(v, str) else v

    # Back-fill postcode from address if not already extracted
    addr = out.get("address")
    if isinstance(addr, str) and addr.strip() and not out.get("postcode"):
        m = _UK_POSTCODE_RE.search(addr)
        if m:
            out["postcode"] = re.sub(r"\s+", " ", m.group(1).upper()).strip()

    # Strip postcode and county from the address string now that they are separate fields
    if isinstance(addr, str) and addr.strip():
        out["address"] = strip_extracted_from_address(addr, out)

    return out
