# cleaner.py — Unicode/whitespace sanitizers shared across the extractor package.

from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict

# Invisible Unicode characters frequently embedded in PDF text exports
_JUNK_CHARS_RE = re.compile(r"[\u200b\u200c\u200d\ufeff\u00ad\u2060]")
# Curly/smart quotes and stray backtick/accent that corrupt numeric fields
_SMART_QUOTES_RE = re.compile(r'[\u201c\u201d\u2018\u2019"\'`´]')

# Lazily imported to avoid circular dependency; populated on first call.
_UK_POSTCODE_RE: re.Pattern[str] | None = None


def _postcode_re() -> re.Pattern[str]:
    global _UK_POSTCODE_RE
    if _UK_POSTCODE_RE is None:
        _UK_POSTCODE_RE = re.compile(
            r"\b([A-Z]{1,2}\d[A-Z0-9]?\s*\d[A-Z]{2})\b", re.IGNORECASE
        )
    return _UK_POSTCODE_RE


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
    t = re.sub(r"\s*£\s*", "£", t)
    return t.strip()


def sanitize_field(key: str, value: str) -> str:
    """Dispatch per-field sanitization; price gets extra normalization."""
    if key.lower() in ("price", "listing_price", "commission"):
        return normalize_price_text(value)
    return scrub_cell_text(value)


def finalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Scrub every string field; back-fill postcode from address when missing."""
    out: Dict[str, Any] = {}
    for k, v in row.items():
        out[k] = sanitize_field(k, v) if isinstance(v, str) else v

    addr = out.get("address")
    if isinstance(addr, str) and addr.strip():
        m = _postcode_re().search(addr)
        if m:
            out["postcode"] = m.group(1).upper().replace("  ", " ").strip()

    if isinstance(out.get("withdrawn_date"), str):
        out["date"] = out["withdrawn_date"]
    return out
