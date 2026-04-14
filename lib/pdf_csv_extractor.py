# pdf_csv_extractor.py
# Hybrid PDF extraction: pdfplumber for grid tables; PyMuPDF + regex for headerless quoted "CSV" text.
# Usage: python pdf_csv_extractor.py <pdf_file> [--json out.json]

from __future__ import annotations

import csv
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import fitz  # PyMuPDF
import pdfplumber

# Cleaner helpers now live in extractor/cleaner.py; import back for backward compat.
from lib.extractor.cleaner import (
    finalize_row as _finalize_row,
    sanitize_field as _sanitize_field,
    scrub_cell_text as _scrub_cell_text,
)


class ImagePdfError(ValueError):
    """Raised when the PDF appears to be a scanned image with no extractable text."""

# --- Table detection (pdfplumber) ---
MIN_TABLE_ROWS = 1
MIN_TABLE_COLS = 2

# --- Delimiter fallback (no real headers; synthetic column names) ---
MIN_CSV_LINES = 2

# --- Regex rows: max quoted-row chunks (safety cap) ---
MAX_REGEX_ROWS = 500

# Quoted runs like "a", "b", "c" (chunk may be one logical row from the PDF)
QUOTED_CSV_CHUNK = re.compile(
    r'"([^"]*)"(?:\s*,\s*"([^"]*)")*',
    re.MULTILINE,
)

UK_POSTCODE_IN_TEXT = re.compile(
    r"\b([A-Z]{1,2}\d[A-Z0-9]?\s*\d[A-Z]{2})\b",
    re.IGNORECASE,
)


# Order matters: first matching pattern wins per cell (same idea as legacy extract_csv_from_pdf).
FIELD_PATTERNS: Tuple[Tuple[str, re.Pattern[str]], ...] = (
    (
        "address",
        re.compile(
            r"(?:[A-Z][a-z]+(?:\s+(?:Hill|Road|Street|Lane|Drive|Close|Avenue|Way|Place|"
            r"Crescent|Mews|Essex|Hertfordshire|CM\d+|\d+[A-Z]{2}))+,?\s*)+",
            re.IGNORECASE,
        ),
    ),
    ("postcode", UK_POSTCODE_IN_TEXT),
    (
        "property_type",
        re.compile(
            r"\b(House|Flat|Bungalow|Detached|Semi|Semi-Detached|Terraced|Apartment)\b",
            re.IGNORECASE,
        ),
    ),
    ("price", re.compile(r"[\u00a3£]\s*[\d,]+")),
    (
        "withdrawn_date",
        re.compile(
            r"\d{1,2}\s+"
            r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+"
            r"\d{4}",
            re.IGNORECASE,
        ),
    ),
    (
        "withdrawn_date",
        re.compile(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b|\b\d{4}-\d{2}-\d{2}\b"),
    ),
    (
        "status",
        re.compile(
            r"\b(Withdrawn|Sold|Active|Pending|Under Offer|STC|SSTC)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "client_name",
        re.compile(
            r"\b([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b",
        ),
    ),
)


@dataclass
class PdfExtractOutcome:
    """Detected extraction path plus row dicts (canonical + legacy keys)."""

    source: str  # pdfplumber_table | pymupdf_regex | pymupdf_csv | none
    headers: List[str]
    rows: List[Dict[str, Any]]
    detail: str
    # Raw column lists (pre-classification) fed to the AI column-mapper agent.
    # Each inner list is one row; cells are cleaned strings in their original column order.
    raw_rows: List[List[str]] = None  # type: ignore[assignment]
    # Detected column header row (when the PDF contains an explicit header line).
    header_row: Optional[List[str]] = None

    def __post_init__(self) -> None:
        if self.raw_rows is None:
            self.raw_rows = []

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["rows"] = self.rows
        return d


def _matrix_to_clean_rows(matrix: Sequence[Sequence[Any]]) -> List[List[str]]:
    """Drop fully empty rows; normalize cells to strings and scrub whitespace."""
    out: List[List[str]] = []
    for row in matrix:
        cells = [_scrub_cell_text(str(c).strip()) if c is not None else "" for c in row]
        if any(cells):
            out.append(cells)
    return out


def _row_looks_like_header(cells: List[str]) -> bool:
    """True when first row is probably column titles, not property data."""
    joined = " ".join(cells).lower()
    header_hints = (
        "address",
        "postcode",
        "post code",
        "status",
        "withdrawn",
        "vendor",
        "client",
        "property",
        "price",
        "type",
        "date",
        "agent",
    )
    if any(h in joined for h in header_hints):
        return True
    if all(len(c) <= 28 for c in cells if c) and cells:
        return True
    return False


_FOOTER_PATTERNS = (
    re.compile(r"^\s*page\s+\d+\s+of\s+\d+\s*$", re.IGNORECASE),
    re.compile(r"^\s*\d+\s*$"),                              # bare page number
    re.compile(r"^\s*printed\s+on\b", re.IGNORECASE),        # "Printed on DD/MM/YYYY"
    re.compile(r"^\s*\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}\s+\d{2}:\d{2}", re.IGNORECASE),  # timestamp
    re.compile(r"^\s*(confidential|copyright|©|all rights reserved)", re.IGNORECASE),
)

_ADDRESS_FRAGMENT = re.compile(
    r"\b(?:Road|Street|Lane|Drive|Close|Avenue|Way|Place|Crescent)\b",
    re.IGNORECASE,
)


def _is_footer_row(cells: List[str]) -> bool:
    """True when the concatenated cell content looks like a page footer."""
    joined = " ".join(c for c in cells if c).strip()
    if not joined:
        return False
    # Never strip rows that contain address-like content
    if _ADDRESS_FRAGMENT.search(joined):
        return False
    for pat in _FOOTER_PATTERNS:
        if pat.match(joined):
            return True
    # Very short rows with no recognisable data (likely print artefacts)
    if len(joined) < 40 and not re.search(r"[£\d]", joined):
        return True
    return False


def _extract_quoted_strings_from_chunk(chunk: str) -> List[str]:
    """All double-quoted fields inside a matched CSV-like chunk."""
    return [
        _scrub_cell_text(p)
        for p in re.findall(r'"([^"]*)"', chunk)
        if p.strip()
    ]


def _classify_parts_to_row(parts: List[str]) -> Dict[str, Any]:
    """Map free-text cells to logical fields using regex heuristics (no headers)."""
    row: Dict[str, Any] = {}

    for part in parts:
        part = _scrub_cell_text(part)
        if not part:
            continue
        matched = False
        for field_name, pattern in FIELD_PATTERNS:
            if not pattern.search(part):
                continue
            if field_name == "postcode":
                m = UK_POSTCODE_IN_TEXT.search(part)
                row[field_name] = (
                    m.group(1).upper().replace("  ", " ").strip() if m else part
                )
            else:
                row[field_name] = part
            matched = True
            break

        if matched:
            continue
        if "£" in part or "\u00a3" in part:
            row.setdefault("price", part)
        elif re.match(r"\d{1,2}\s+\w+", part):
            row.setdefault("withdrawn_date", part)
        elif part in ("Withdrawn", "Sold", "Active", "Pending"):
            row.setdefault("status", part)
        elif len(part.split(",")) > 2:
            row.setdefault("address", part)

    if "client_name" in row:
        row.setdefault("seller", row["client_name"])

    return _finalize_row(row)


def _regex_rows_from_text(
    full_text: str,
) -> tuple[List[Dict[str, Any]], List[List[str]]]:
    """
    Scan for quoted comma-separated chunks; return (classified_rows, raw_part_rows).
    raw_part_rows preserves the original cell order for the AI column-mapper agent.
    Footer rows are filtered out.
    """
    rows: List[Dict[str, Any]] = []
    raw_rows: List[List[str]] = []
    seen_spans: set = set()

    for m in QUOTED_CSV_CHUNK.finditer(full_text):
        start = m.start()
        if start in seen_spans:
            continue
        chunk = m.group(0)
        parts = _extract_quoted_strings_from_chunk(chunk)
        if len(parts) < 2:
            continue
        if _is_footer_row(parts):
            continue
        row = _classify_parts_to_row(parts)
        if len(row) >= 3:
            rows.append(row)
            raw_rows.append(parts)
            seen_spans.add(start)
        if len(rows) >= MAX_REGEX_ROWS:
            break

    return rows, raw_rows


def _pymupdf_full_text(pdf_path: str) -> str:
    """Concatenate all page text (reading order)."""
    doc = fitz.open(pdf_path)
    try:
        return "\n".join((page.get_text(sort=True) or "") for page in doc)
    finally:
        doc.close()


def _rows_to_dicts(
    headers: List[str], data_rows: List[List[str]]
) -> List[Dict[str, Any]]:
    """Build dict rows from explicit headers."""
    seen: Dict[str, int] = {}
    colnames: List[str] = []
    for h in headers:
        base = (h or "").strip() or "column"
        n = seen.get(base, 0)
        seen[base] = n + 1
        colnames.append(base if n == 0 else f"{base} ({n + 1})")

    records: List[Dict[str, Any]] = []
    for row in data_rows:
        rec: Dict[str, Any] = {}
        for i, col in enumerate(colnames):
            raw = row[i].strip() if i < len(row) else ""
            rec[col] = _sanitize_field(col, raw) if raw else ""
        records.append(_finalize_row(rec))
    return records


def _extract_tables_pdfplumber(pdf_path: str) -> List[Tuple[int, List[List[str]]]]:
    """Return (page_num, row_matrix) for each usable table."""
    chunks: List[Tuple[int, List[List[str]]]] = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            pnum = i + 1
            tables: List[Any] = []
            try:
                tables = (
                    page.extract_tables(
                        table_settings={
                            "vertical_strategy": "lines",
                            "horizontal_strategy": "lines",
                            "intersection_tolerance": 5,
                        }
                    )
                    or []
                )
            except Exception:
                tables = []
            if not tables:
                try:
                    tables = page.extract_tables() or []
                except Exception:
                    tables = []
            for matrix in tables:
                clean = _matrix_to_clean_rows(matrix or [])
                if (
                    len(clean) >= MIN_TABLE_ROWS + 1
                    and max((len(r) for r in clean), default=0) >= MIN_TABLE_COLS
                ):
                    chunks.append((pnum, clean))
    return chunks


def _best_plumber_table(pdf_path: str) -> Optional[Tuple[int, List[List[str]]]]:
    chunks = _extract_tables_pdfplumber(pdf_path)
    if not chunks:
        return None
    best = max(chunks, key=lambda x: len(x[1]) * max(len(r) for r in x[1]))
    return best


def _plumber_table_to_outcome(pnum: int, rows: List[List[str]]) -> PdfExtractOutcome:
    """Use header row when it looks like titles; otherwise regex-classify every row's cells."""
    max_w = max(len(r) for r in rows)
    norm = [r + [""] * (max_w - len(r)) for r in rows]

    if _row_looks_like_header(norm[0]):
        detected_header = [c if c else f"column_{i + 1}" for i, c in enumerate(norm[0])]
        # Filter footer rows from data rows
        data_rows = [r for r in norm[1:] if not _is_footer_row(r)]
        dict_rows = _rows_to_dicts(detected_header, data_rows)
        return PdfExtractOutcome(
            "pdfplumber_table",
            detected_header,
            dict_rows,
            f"Table on page {pnum}, header row detected, {len(dict_rows)} data rows",
            raw_rows=data_rows,
            header_row=detected_header,
        )

    dict_rows: List[Dict[str, Any]] = []
    raw_rows: List[List[str]] = []
    for line_cells in norm:
        parts = [c for c in line_cells if c]
        if len(parts) < 2:
            continue
        if _is_footer_row(parts):
            continue
        row = _classify_parts_to_row(parts)
        if len(row) >= 3:
            dict_rows.append(row)
            raw_rows.append(parts)
    field_keys = sorted({k for r in dict_rows for k in r.keys()})
    return PdfExtractOutcome(
        "pdfplumber_table",
        field_keys,
        dict_rows,
        f"Table on page {pnum}, headerless — regex-classified cells, {len(dict_rows)} rows",
        raw_rows=raw_rows,
    )


def _infer_delimiter_for_text_lines(lines: List[str]) -> Optional[str]:
    """Pick comma, tab, or pipe when column counts are stable."""
    candidates = [",", "\t", "|"]
    best_delim: Optional[str] = None
    best_ratio = 0.0
    sample = [ln for ln in lines if ln.strip()][:200]
    if len(sample) < MIN_CSV_LINES:
        return None
    for delim in candidates:
        lengths: List[int] = []
        for ln in sample:
            try:
                row = next(csv.reader([ln], delimiter=delim, quotechar='"'))
            except csv.Error:
                continue
            if len(row) < MIN_TABLE_COLS:
                continue
            lengths.append(len(row))
        if len(lengths) < MIN_CSV_LINES:
            continue
        mode_len = max(set(lengths), key=lengths.count)
        stable = sum(1 for L in lengths if L == mode_len)
        ratio = stable / len(lengths)
        if ratio >= 0.75 and mode_len >= MIN_TABLE_COLS and ratio > best_ratio:
            best_ratio = ratio
            best_delim = delim
    return best_delim


def _parse_text_as_csv(full_text: str, delimiter: str) -> List[List[str]]:
    lines = [ln.strip() for ln in full_text.splitlines() if ln.strip()]
    rows: List[List[str]] = []
    for ln in lines:
        try:
            row = next(csv.reader([ln], delimiter=delimiter, quotechar='"'))
        except csv.Error:
            continue
        if len(row) >= MIN_TABLE_COLS:
            rows.append([c.strip() for c in row])
    return rows


def _extract_delimited_no_header(
    full_text: str,
) -> Optional[Tuple[List[str], List[List[str]]]]:
    """Stable delimiter lines; treat every line as data (synthetic col_0, col_1, ...)."""
    raw_lines = [ln.strip() for ln in full_text.splitlines() if ln.strip()]
    if len(raw_lines) < MIN_CSV_LINES:
        return None
    delim = _infer_delimiter_for_text_lines(raw_lines)
    if not delim:
        return None
    matrix = _parse_text_as_csv(full_text, delim)
    if len(matrix) < MIN_CSV_LINES:
        return None
    width = max(len(r) for r in matrix)
    padded = [r + [""] * (width - len(r)) for r in matrix]
    headers = [f"col_{i}" for i in range(width)]
    return headers, padded


def _count_pdf_pages(pdf_path: str) -> int:
    """Return the number of pages in the PDF without full parsing."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            return len(pdf.pages)
    except Exception:
        return 0


def extract_from_pdf(pdf_path: str) -> PdfExtractOutcome:
    """
    1) pdfplumber table (with header row or headerless cell regex).
    2) PyMuPDF text + quoted-field regex rows (no headers).
    3) PyMuPDF delimiter lines, no header row (synthetic columns).

    Raises ImagePdfError when the file has pages but no extractable text.
    """
    path = str(Path(pdf_path).resolve())
    if not Path(path).exists():
        return PdfExtractOutcome("none", [], [], f"File not found: {path}")

    table_hit = _best_plumber_table(path)
    if table_hit is not None:
        pnum, rows = table_hit
        return _plumber_table_to_outcome(pnum, rows)

    full_text = _pymupdf_full_text(path)
    regex_rows, regex_raw = _regex_rows_from_text(full_text)
    if regex_rows:
        keys = sorted({k for r in regex_rows for k in r.keys()})
        return PdfExtractOutcome(
            "pymupdf_regex",
            keys,
            regex_rows,
            f"Headerless quoted fields + regex, {len(regex_rows)} rows",
            raw_rows=regex_raw,
        )

    delim_hit = _extract_delimited_no_header(full_text)
    if delim_hit is not None:
        headers, data_rows = delim_hit
        dict_rows = _rows_to_dicts(headers, data_rows)
        return PdfExtractOutcome(
            "pymupdf_csv",
            headers,
            dict_rows,
            f"Delimiter lines, synthetic headers, {len(dict_rows)} rows",
            raw_rows=data_rows,
        )

    # Nothing extracted — check whether the file is a scanned image PDF
    n_pages = _count_pdf_pages(path)
    char_count = len(full_text.strip())
    if n_pages > 0 and char_count < 50 * n_pages:
        raise ImagePdfError(
            f"PDF has {n_pages} page(s) but only {char_count} characters of text could be "
            "extracted. This appears to be a scanned image PDF. Please provide a "
            "text-based PDF or convert the scan to text first."
        )

    return PdfExtractOutcome(
        "none",
        [],
        [],
        "No table, no regex-matched quoted rows, and no stable delimiter text",
    )


def extract_csv_from_pdf(pdf_path: str) -> List[Dict[str, Any]]:
    """Return parsed rows as dicts."""
    return extract_from_pdf(pdf_path).rows


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Extract table or headerless CSV-like data from PDF."
    )
    ap.add_argument("pdf_file")
    ap.add_argument(
        "--json", dest="json_out", help="Write full outcome JSON to this path"
    )
    args = ap.parse_args()

    if not Path(args.pdf_file).exists():
        print(f"File not found: {args.pdf_file}", file=sys.stderr)
        sys.exit(1)

    outcome = extract_from_pdf(args.pdf_file)
    print(json.dumps(outcome.to_dict(), indent=2))
    if args.json_out:
        Path(args.json_out).write_text(
            json.dumps(outcome.to_dict(), indent=2), encoding="utf-8"
        )
        print(f"Saved to {args.json_out}", file=sys.stderr)
