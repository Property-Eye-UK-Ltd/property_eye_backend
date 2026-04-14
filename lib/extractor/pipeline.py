# pipeline.py — Public entry point for AI-powered structured extraction from PDFs and CSVs.
#
# Usage (standalone test):
#   python -m lib.extractor.pipeline "data/temp/Expert Agent Print Preview.pdf"
#   python -m lib.extractor.pipeline "data/agency_export.csv"
#
# Usage (in code):
#   from lib.extractor import extract_structured
#   rows = extract_structured("path/to/file.pdf")   # or .csv

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from .column_mapper import interpret as mapper_interpret
from .column_mapper import validate_mapping


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _ensure_project_root() -> None:
    """Add the project root (property_eye_backend) to sys.path if needed."""
    root = Path(__file__).resolve().parents[2]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def _extract_structured_csv(
    path: str,
    model=None,
    skip_ai: bool = False,
) -> List[Dict[str, Any]]:
    """
    CSV path: read with pandas, send real header names to the agent (header-mode),
    then apply the DSL mapping to every data row.
    """
    import pandas as pd

    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    header_row: List[str] = df.columns.tolist()
    raw_rows: List[List[str]] = df.values.tolist()  # type: ignore[assignment]

    if not raw_rows:
        return []

    if skip_ai:
        # Return raw dicts keyed by original CSV headers (useful for offline smoke tests)
        return [dict(zip(header_row, row)) for row in raw_rows]

    from .ai_agent import run_agent

    # CSV always has real headers — use header-mode; still pass first data row as sample
    mapping = run_agent(
        sample_row=raw_rows[0],
        header_row=header_row,
        model=model,
    )

    if not mapping:
        return [dict(zip(header_row, row)) for row in raw_rows]

    warnings = validate_mapping(mapping, raw_rows[0])
    for w in warnings:
        print(f"[extractor/csv] WARNING: {w}", file=sys.stderr)

    structured = [mapper_interpret(mapping, row) for row in raw_rows]
    return [r for r in structured if any(v for v in r.values() if v)]


def _extract_structured_pdf(
    path: str,
    model=None,
    skip_ai: bool = False,
) -> List[Dict[str, Any]]:
    """
    PDF path: extract raw rows, stitch continuations, pick best sample row,
    run the AI agent, then apply the DSL mapping to all stitched rows.
    """
    _ensure_project_root()

    from lib.pdf_csv_extractor import ImagePdfError
    from lib.pdf_csv_extractor import extract_from_pdf

    try:
        outcome = extract_from_pdf(path)
    except ImagePdfError as exc:
        raise ValueError(str(exc)) from exc

    if outcome.source == "none" or not outcome.raw_rows:
        # Nothing was extractable — return whatever the regex extractor managed
        return outcome.rows

    if skip_ai:
        return outcome.rows

    from .ai_agent import run_agent
    from .row_selector import select_sample_rows
    from .row_stitcher import stitch

    # 1. Stitch continuation rows so the AI sees complete logical records
    stitched = stitch(outcome.raw_rows)

    # 2. Pick the richest sample row (up to row index 2) plus a context row
    best_row, context_row, is_continuation = select_sample_rows(stitched)

    # 3. If the PDF exposed a real header row, use header-mode (more reliable)
    mapping = run_agent(
        sample_row=best_row,
        context_row=context_row,
        is_continuation=is_continuation,
        header_row=outcome.header_row,
        model=model,
    )

    if not mapping:
        return outcome.rows

    warnings = validate_mapping(mapping, best_row)
    for w in warnings:
        print(f"[extractor/pdf] WARNING: {w}", file=sys.stderr)

    structured = [mapper_interpret(mapping, row) for row in stitched]
    return [r for r in structured if any(v for v in r.values() if v)]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_structured(
    path: str,
    model=None,
    skip_ai: bool = False,
) -> List[Dict[str, Any]]:
    """
    Extract structured property records from a PDF or CSV file using the AI column mapper.

    Args:
        path:     Path to the source file (.pdf or .csv).
        model:    Optional LangChain chat model (defaults to gemini-1.5-flash).
        skip_ai:  When True, return the raw/regex-classified rows without calling the AI.

    Returns:
        List of dicts with canonical field names.

    Raises:
        ValueError: If the PDF is a scanned image with no extractable text.
        FileNotFoundError: If the file does not exist.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {path}")

    suffix = p.suffix.lower()
    if suffix == ".csv":
        return _extract_structured_csv(path, model=model, skip_ai=skip_ai)
    return _extract_structured_pdf(path, model=model, skip_ai=skip_ai)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description=(
            "Run the AI column-mapper pipeline on a PDF or CSV and print structured rows.\n"
            "File type is detected automatically from the extension."
        )
    )
    ap.add_argument("file", help="Path to the PDF or CSV file")
    ap.add_argument(
        "--json", dest="json_out", help="Write output JSON to this file path"
    )
    ap.add_argument(
        "--skip-ai",
        action="store_true",
        help="Skip the AI agent; return regex/header-classified rows only (no API calls)",
    )
    args = ap.parse_args()

    try:
        rows = extract_structured(args.file, skip_ai=args.skip_ai)
    except (ValueError, FileNotFoundError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    output = json.dumps(rows, indent=2, ensure_ascii=False)
    print(output)

    if args.json_out:
        Path(args.json_out).write_text(output, encoding="utf-8")
        print(f"Saved to {args.json_out}", file=sys.stderr)
