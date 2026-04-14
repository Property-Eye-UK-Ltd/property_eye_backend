# row_selector.py — Pick the most representative sample row(s) to send to the AI agent.
#
# Instead of blindly using raw_rows[0], score the first three rows and pick the richest.
# A "context row" (the row immediately after the best one) is also returned together
# with a hint about whether it looks like a continuation of the primary row.

from __future__ import annotations

import re
from typing import List, Optional, Tuple

# Patterns that signal a cell carries a meaningful canonical value
_HAS_ADDRESS = re.compile(
    r"(?:[A-Z][a-z]+\s+){1,4}(?:Road|Street|Lane|Drive|Close|Avenue|Way|Place|"
    r"Crescent|Hill|Mews|Row|Green|Gardens?|Park|Court|Grove)\b",
    re.IGNORECASE,
)
_HAS_DATE = re.compile(
    r"\b\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+\d{4}\b"
    r"|\b\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}\b",
    re.IGNORECASE,
)
_HAS_PRICE = re.compile(r"[\u00a3£]\s*[\d,]+")
_HAS_POSTCODE = re.compile(r"\b[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}\b", re.IGNORECASE)
_HAS_STATUS = re.compile(
    r"\b(Withdrawn|Sold|Active|Pending|Under Offer|STC|SSTC)\b", re.IGNORECASE
)

# Weight per signal found
_SCORE_PER_NONEMPTY_CELL = 1
_SCORE_ADDRESS = 5
_SCORE_DATE = 4
_SCORE_PRICE = 4
_SCORE_POSTCODE = 3
_SCORE_STATUS = 2


def score_row(row: List[str]) -> int:
    """
    Score a row by data completeness.
    More non-empty cells and canonical field signals raise the score.
    """
    total = 0
    for cell in row:
        if not cell or not cell.strip():
            continue
        total += _SCORE_PER_NONEMPTY_CELL
        if _HAS_ADDRESS.search(cell):
            total += _SCORE_ADDRESS
        if _HAS_DATE.search(cell):
            total += _SCORE_DATE
        if _HAS_PRICE.search(cell):
            total += _SCORE_PRICE
        if _HAS_POSTCODE.search(cell):
            total += _SCORE_POSTCODE
        if _HAS_STATUS.search(cell):
            total += _SCORE_STATUS
    return total


def _is_continuation_hint(primary: List[str], candidate: List[str]) -> bool:
    """
    Heuristic: returns True if candidate looks like a tail/overflow of primary.
    Conditions:
    - Candidate has fewer than half the populated cells of primary, AND
    - Candidate has no address-like or price-like content of its own.
    """
    primary_populated = sum(1 for c in primary if c and c.strip())
    candidate_populated = sum(1 for c in candidate if c and c.strip())
    if primary_populated == 0:
        return False
    if candidate_populated > primary_populated / 2:
        return False
    joined = " ".join(c for c in candidate if c)
    has_own_data = bool(
        _HAS_ADDRESS.search(joined) or _HAS_PRICE.search(joined)
    )
    return not has_own_data


def select_sample_rows(
    raw_rows: List[List[str]],
) -> Tuple[List[str], Optional[List[str]], bool]:
    """
    Choose the best sample row from the first three candidates.

    Args:
        raw_rows: All raw row lists extracted from the PDF.

    Returns:
        (best_row, context_row, is_continuation) where:
          - best_row is the highest-scoring row among rows 0–2.
          - context_row is the row immediately after best_row (or None).
          - is_continuation is True when context_row looks like an overflow of best_row.
    """
    if not raw_rows:
        return [], None, False

    # Score up to first 3 rows; pick the best
    candidates = raw_rows[: min(3, len(raw_rows))]
    scores = [score_row(r) for r in candidates]
    best_idx = scores.index(max(scores))
    best_row = raw_rows[best_idx]

    context_idx = best_idx + 1
    if context_idx >= len(raw_rows):
        return best_row, None, False

    context_row = raw_rows[context_idx]
    is_cont = _is_continuation_hint(best_row, context_row)
    return best_row, context_row, is_cont
