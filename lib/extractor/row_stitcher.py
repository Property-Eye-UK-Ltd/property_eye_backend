# row_stitcher.py — Detect and merge continuation rows before AI mapping.
#
# Some PDF exports render one logical record across two raw text lines.
# This module identifies when a row is a continuation of the previous one
# and appends its cells to the primary row, producing one wider logical row.
# The resulting combined row indices are used consistently by column_mapper.interpret.

from __future__ import annotations

import re
from statistics import mode
from typing import List

# Patterns that indicate a row carries its own address or price — not a continuation
_HAS_STANDALONE_DATA = re.compile(
    r"(?:[A-Z][a-z]+\s+){1,4}(?:Road|Street|Lane|Drive|Close|Avenue|Way|Place|"
    r"Crescent|Hill|Mews|Row|Green|Gardens?|Park|Court|Grove)\b"
    r"|[\u00a3£]\s*[\d,]+",
    re.IGNORECASE,
)

_HAS_POSTCODE = re.compile(r"\b[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}\b", re.IGNORECASE)


def _populated_count(row: List[str]) -> int:
    """Count non-empty cells in a row."""
    return sum(1 for c in row if c and c.strip())


def _modal_width(rows: List[List[str]]) -> int:
    """Return the most common populated-cell count across all rows."""
    if not rows:
        return 0
    counts = [_populated_count(r) for r in rows]
    try:
        return mode(counts)
    except Exception:
        return max(counts)


def is_continuation_row(
    prev_row: List[str],
    next_row: List[str],
    mode_width: int,
) -> bool:
    """
    True when next_row looks like the overflow tail of prev_row.

    Criteria (all must hold):
    1. next_row has at most half the populated cells of mode_width.
    2. next_row does not contain a standalone address or price (own property data).
    3. next_row first non-empty cell does not look like a new postcode-anchored address.
    """
    next_pop = _populated_count(next_row)
    if mode_width > 0 and next_pop > mode_width // 2:
        return False

    joined = " ".join(c for c in next_row if c and c.strip())
    if _HAS_STANDALONE_DATA.search(joined):
        return False

    # If it starts with a postcode it's likely a new record fragment, not a tail
    first_cell = next((c for c in next_row if c and c.strip()), "")
    if _HAS_POSTCODE.match(first_cell.strip()):
        return False

    return True


def stitch(raw_rows: List[List[str]]) -> List[List[str]]:
    """
    Merge continuation rows into their predecessor and return logical rows.

    The cells from a continuation row are appended (not interleaved) to the
    primary row's cell list. The AI column-mapper then sees a wider row and the
    DSL index for a stitched field will be len(primary) + offset_in_continuation.
    """
    if not raw_rows:
        return []

    mw = _modal_width(raw_rows)
    result: List[List[str]] = []
    i = 0

    while i < len(raw_rows):
        current = list(raw_rows[i])

        # Keep absorbing continuation rows greedily
        while i + 1 < len(raw_rows) and is_continuation_row(current, raw_rows[i + 1], mw):
            i += 1
            current = current + raw_rows[i]

        result.append(current)
        i += 1

    return result
