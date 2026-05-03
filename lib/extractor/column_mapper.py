# column_mapper.py — Interprets the AI-produced DSL column map against raw row lists.
#
# DSL grammar (value of each canonical-field key):
#   "3"     → direct:    use row[3] verbatim
#   "5+6"   → concat:    join row[5] and row[6] with a space
#   ">0"    → extract:   run field-type regex extractor on row[0]
#   "3?"    → uncertain: same as "3" but value is tagged _uncertain=True for human review
#   null    → absent:    field not present in the data

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from .cleaner import finalize_row, scrub_cell_text
from .field_extractors import extract_field

# All canonical output fields — every row will include each key, value or None.
CANONICAL_FIELDS: List[str] = [
    "address",
    "postcode",
    "region",
    "county",
    "property_number",
    "withdrawn_date",
    "price",
    "commission",
    "client_name",
    "vendor_name",
    "contract_duration",
    "title_number",
]

# Pattern that validates a DSL value token (optional trailing '?' for uncertainty)
_DIRECT_RE = re.compile(r"^(\d+)\??$")
_CONCAT_RE = re.compile(r"^(\d+)(\+\d+)+$")
_EXTRACT_RE = re.compile(r"^>(\d+)$")
_UNCERTAIN_RE = re.compile(r"\?$")


class DslParseError(ValueError):
    """Raised when a DSL instruction cannot be parsed."""


def _safe_index(row: List[str], idx: int) -> str:
    """Return scrubbed cell text; empty string when index is out of bounds."""
    if idx < 0 or idx >= len(row):
        return ""
    return scrub_cell_text(row[idx])


def _strip_uncertain(instr: str) -> Tuple[str, bool]:
    """Strip the trailing '?' flag and return (clean_instr, is_uncertain)."""
    if _UNCERTAIN_RE.search(instr):
        return instr[:-1], True
    return instr, False


def _interpret_instruction(
    field: str,
    instruction: str,
    row: List[str],
) -> Tuple[Optional[str], bool]:
    """
    Evaluate one DSL instruction against a single raw row.

    Returns (value_or_None, is_uncertain).
    """
    instr, uncertain = _strip_uncertain(instruction.strip())

    # ">idx" — regex-extract field type from the referenced column
    m = _EXTRACT_RE.match(instr)
    if m:
        source_text = _safe_index(row, int(m.group(1)))
        if not source_text:
            return None, uncertain
        return extract_field(field, source_text), uncertain

    # "idx+idx+..." — concatenate multiple columns (uncertain flag not supported here)
    if _CONCAT_RE.match(instr):
        parts = [_safe_index(row, int(i)) for i in instr.split("+")]
        joined = " ".join(p for p in parts if p).strip()
        return (joined or None), uncertain

    # "idx" or "idx?" — direct column reference
    m2 = _DIRECT_RE.match(instr)
    if m2:
        val = _safe_index(row, int(m2.group(1)))
        return (val or None), uncertain

    raise DslParseError(
        f"Cannot parse DSL instruction '{instruction}' for field '{field}'"
    )


def interpret(
    mapping: Dict[str, Optional[str]],
    row: List[str],
) -> Dict[str, Any]:
    """
    Apply the AI-produced column map to a single raw row.

    Args:
        mapping: DSL dict returned by the agent (canonical_field → instruction | null).
        row:     One row as a list of raw string cells.

    Returns:
        Dict with canonical field names and extracted/cleaned values.
        Fields with an uncertain flag include a sibling key  '<field>_uncertain': True.
    """
    result: Dict[str, Any] = {}

    for field, instruction in mapping.items():
        if instruction is None:
            continue
        try:
            value, uncertain = _interpret_instruction(field, str(instruction), row)
        except (DslParseError, ValueError):
            value, uncertain = None, False
        if value is not None:
            result[field] = value
            if uncertain:
                result[f"{field}_uncertain"] = True

    finalized = finalize_row(result)

    # Ensure every canonical field is present — set to None when absent or unmapped
    for field in CANONICAL_FIELDS:
        if field not in finalized:
            finalized[field] = None

    return finalized


def validate_mapping(
    mapping: Dict[str, Optional[str]],
    sample_row: List[str],
) -> List[str]:
    """
    Sanity-check a mapping dict against a sample row.

    Returns a list of warning strings (empty list means all is well).
    """
    warnings: List[str] = []
    n_cols = len(sample_row)

    for field, instruction in mapping.items():
        if instruction is None:
            continue
        # Strip uncertainty flag before parsing
        instr, _ = _strip_uncertain(str(instruction).strip())
        indices: List[int] = []

        m = _EXTRACT_RE.match(instr)
        if m:
            indices = [int(m.group(1))]
        elif _CONCAT_RE.match(instr):
            indices = [int(i) for i in instr.split("+")]
        else:
            m2 = _DIRECT_RE.match(instr)
            if m2:
                indices = [int(m2.group(1))]
            else:
                warnings.append(f"Unrecognised DSL syntax for '{field}': '{instr}'")
                continue

        for idx in indices:
            if idx >= n_cols:
                warnings.append(
                    f"Field '{field}' references column {idx} but row only has {n_cols} columns"
                )

    return warnings
