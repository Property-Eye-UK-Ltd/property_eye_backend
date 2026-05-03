# ai_agent.py — LangGraph reflection agent for AI-powered column mapping.
#
# Two-node graph:   extractor → reviewer → (approved | retry → extractor)
#
# Only a single sample row + optional context row is ever sent to the model.
# All private listing data stays local.
#
# The reviewer node is DETERMINISTIC Python — not an LLM — so it cannot hallucinate.
# It checks hard structural rules (bounds, bare-index misuse, commission signals).
# If the extractor's first pass is valid it is approved immediately at zero extra cost.
#
# Two prompt modes:
#   header_mode — the file has real column header names (CSV or PDF with header row).
#                 The AI just name-matches; much cleaner and more reliable.
#   data_mode   — no header row; the AI infers from cell content.
#                 An optional context_row (with continuation hint) provides extra signal.

from __future__ import annotations

import json
import logging
import re
from typing import Annotated, Any, Dict, List, Optional

from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    SystemMessage,
)  # SystemMessage used by extractor prompts
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict

from src.core.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DSL reference — injected into every extractor prompt
# ---------------------------------------------------------------------------

_DSL_REFERENCE = """
COLUMN MAP DSL
==============
Return a single JSON object where each key is a canonical field name and the
value is one of:

  "N"      — Use the cell at index N (0-based) verbatim.
  "N+M"    — Concatenate cells at indices N and M with a space (e.g. first + last name,
             or town + county that are split across columns).
  ">N"     — The canonical value is *embedded inside* cell N; the system will
             regex-extract it automatically.  Use for:
               - postcode hidden at the end of a full address string
               - date embedded in a description field
               - price buried inside a narrative cell (e.g. "Offers in excess of £670,000")
  "N?"     — Same as "N" but you are UNCERTAIN about this mapping; the system will
             flag the extracted value as low-confidence for human review.
             Use "N?" when a cell plausibly contains the field but you are not sure.
  null     — Field is not present in this data source.

Canonical fields (use exactly these keys):
  address           — Full property address (street number, name, town, postcode)
  postcode          — UK postcode (e.g. EN10 7DA).  Use ">N" when embedded in address.
  region            — Town or city.  Use ">N" when embedded in address, or "N+M" if split.
  county            — County (e.g. Hertfordshire, Essex).  Use ">N" when embedded in address.
  withdrawn_date    — Date the property was withdrawn from the market.
  price             — Listing price.  Use ">N" when the cell contains narrative.
  commission        — Agent commission.  Patterns to detect:
                        "1.5%", "1.75% + VAT", "£3,000 flat fee",
                        "Sole Agency 1.25%", "Multi Agency 2%".
                      Map the cell containing a percentage near a currency symbol or
                      near the word "agency" / "commission".  Return null if absent.
  client_name       — Introduced BUYER / purchaser / applicant / prospect (the purchasing-side
                      contact you may later compare to Land Registry).  Map ONLY when the column
                      header explicitly signals the purchasing party (e.g. Applicant, Buyer,
                      Purchaser, Prospect, "Purchasing Client").  Return null if unsure or if the
                      column is vendor/seller/landlord or an ambiguous "Name" / "Client" alone
                      (UK CRMs often label the vendor as "Client").
  vendor_name       — Seller / vendor / landlord (selling party).  Map Vendor Name, Seller,
                      Landlord, Lessor, or ambiguous Name/Contact when there is no explicit
                      purchaser column.  Use "N+M" if first and last name are split across cells.
  contract_duration — Length of the agency contract (e.g. "12 weeks", "3 months").
  property_number   — House or flat number only (e.g. "11", "Flat 3").
  title_number      — HM Land Registry title number (2–3 uppercase district letters followed by
                     1–6 digits, e.g. "HD567890", "TGL12345", "AMS1234").  Most agencies do NOT
                     record this.  Return null unless a column clearly contains this pattern.
                     Use ">N" if the title number appears embedded inside a reference or notes
                     field alongside other text.

Rules (STRICT — the reviewer will reject violations):
1. Use ">N" — NOT a bare "N" — whenever the canonical value is only a PART of the cell.
   Examples where ">N" is REQUIRED:
     • postcode is the last token of a full address string  →  ">0", not "0"
     • price is inside "Offers in excess of £670,000"       →  ">2", not "2"
     • region / county are embedded inside the address      →  ">0", not separate indices
2. Use "N+M" only when the cells are separate columns that should be joined.
3. Use "N?" when you are guessing; never silently map a dubious cell without "?".
4. NEVER map vendor-only columns (Vendor, Seller, Landlord, etc.) to client_name — use vendor_name.
   NEVER map generic "Name" or "Client" alone to client_name unless the header clearly indicates
   the purchaser (Buyer, Applicant, Prospect, Purchaser, etc.).
5. Output ONLY valid JSON — no markdown fences, no explanation outside the JSON.
"""

# ---------------------------------------------------------------------------
# Concrete one-shot examples — appended to each extractor system prompt
# ---------------------------------------------------------------------------

_EXAMPLE_DATA_MODE = """
─── WORKED EXAMPLE (data mode) ───────────────────────────────────────────────
Input row:
  [0] '11 Hamlet Hill, Roydon, Harlow, Essex, CM19 5LA'
  [1] 'House'
  [2] 'Offers in excess of £725,000'
  [3] '17 January 2020'
  [4] 'Withdrawn'
  [5] 'Sole Agency 1.5%'
  [6] 'John Smith'
  [7] 'HD567890'

Correct output:
{
  "address": "0",
  "postcode": ">0",
  "region": ">0",
  "county": ">0",
  "property_number": ">0",
  "price": ">2",
  "withdrawn_date": "3",
  "commission": "5",
  "vendor_name": "6",
  "client_name": null,
  "title_number": "7",
  "contract_duration": null
}

Note: postcode/region/county all use ">0" because they are substrings of cell 0.
      price uses ">2" because the amount is buried inside narrative text.
      Cell [6] is a lone personal name with no column context — treat as vendor_name (seller side),
      not client_name (buyer); leave client_name null unless a separate cell clearly indicates the purchaser.
      title_number uses "7" because cell 7 is a dedicated HMLR title number (HD567890).
      Use ">N" for title_number only if the HMLR code is embedded inside a larger notes field.
──────────────────────────────────────────────────────────────────────────────
"""

_EXAMPLE_HEADER_MODE = """
─── WORKED EXAMPLE (header mode) ─────────────────────────────────────────────
Column headers:
  [0] 'Property Address'
  [1] 'Property Type'
  [2] 'Asking Price'
  [3] 'Date Withdrawn'
  [4] 'Status'
  [5] 'Vendor Name'
  [6] 'Applicant Name'
  [7] 'Commission'
  [8] 'Title Number'

Correct output:
{
  "address": "0",
  "postcode": ">0",
  "region": ">0",
  "county": ">0",
  "property_number": ">0",
  "price": "2",
  "withdrawn_date": "3",
  "vendor_name": "5",
  "client_name": "6",
  "commission": "7",
  "title_number": "8",
  "contract_duration": null
}

Note: 'Property Address' header implies the column holds a full address string, so
      postcode/region/county all use ">0".  Dedicated columns like 'Asking Price'
      use a bare index because the cell will contain just the value.
      'Vendor Name' maps to vendor_name (seller), never to client_name.  'Applicant Name' maps to
      client_name (buyer).  title_number maps to "8" because the header is explicitly named 'Title Number'.
──────────────────────────────────────────────────────────────────────────────
"""

# ---------------------------------------------------------------------------
# System messages
# ---------------------------------------------------------------------------

_EXTRACTOR_SYSTEM_HEADER = SystemMessage(
    content=(
        "You are a precise data schema analyst specialised in UK estate agent file formats. "
        "The user will provide a list of column header names taken directly from a CSV or PDF. "
        "Your job is to map each header (by its index) to the canonical field it represents, "
        "using the DSL format below.\n\n"
        "Because these are actual column headers, prefer direct index mapping ('N'). "
        "Only use '>N' if the header name implies its value contains another canonical field "
        "(e.g. a header called 'Full Address' which would embed the postcode).\n\n"
        + _DSL_REFERENCE
        + _EXAMPLE_HEADER_MODE
    )
)

_EXTRACTOR_SYSTEM_DATA = SystemMessage(
    content=(
        "You are a precise data schema analyst specialised in messy UK estate agent exports. "
        "The user will provide a sample data row (and optionally a context row) extracted from "
        "a PDF. Infer which cell holds which canonical field from the cell content itself.\n\n"
        + _DSL_REFERENCE
        + _EXAMPLE_DATA_MODE
    )
)

MAX_ITERATIONS = 3

# ---------------------------------------------------------------------------
# Deterministic reviewer — replaces the LLM reviewer entirely
# ---------------------------------------------------------------------------

# Narrative words that mean a price cell needs ">N" not a bare index
_PRICE_NARRATIVE_RE = re.compile(
    r"\b(offers?\s+in\s+excess\s+of|guide\s+price|oieo|poa|approximately|asking\s+price)\b",
    re.IGNORECASE,
)
# Any cell that contains a "%" or explicit commission vocabulary
_COMMISSION_SIGNAL_RE = re.compile(
    r"%|commission|agency\s+fee|sole\s+agency|multi.?agency",
    re.IGNORECASE,
)
# A cell that looks like a full comma-separated address (multi-segment)
_FULL_ADDRESS_RE = re.compile(r"[A-Za-z][^,]+,[^,]+[A-Za-z]")

# DSL instruction patterns (must match column_mapper.py)
_RE_EXTRACT = re.compile(r"^>(\d+)$")
_RE_CONCAT = re.compile(r"^(\d+)(\+\d+)+$")
_RE_DIRECT = re.compile(r"^(\d+)\??$")

# Column headers that explicitly indicate the purchasing party (client_name is allowed).
_BUYER_IN_HEADER_RE = re.compile(
    r"\b(buyers?|purchasers?|applicants?|prospects?|purchasing|introduced\s+party)\b",
    re.IGNORECASE,
)
# Headers that refer to the selling party — must map to vendor_name, not client_name.
_SELLER_IN_HEADER_RE = re.compile(
    r"\b(vendors?|sellers?|landlords?|lessors?)\b",
    re.IGNORECASE,
)
# Ambiguous CRM labels (often the vendor) — do not map to client_name without a buyer qualifier.
_AMBIGUOUS_NAME_HEADER_RE = re.compile(
    r"^(client|client\s+name|name|contact|contact\s+name)(\s+\d+)?$",
    re.IGNORECASE,
)


def _header_implies_seller_not_buyer(header: Optional[str]) -> bool:
    """True when this column header must not be the sole source for client_name (buyer-only)."""
    if not header or not str(header).strip():
        return False
    h = str(header).strip().lower()
    if _BUYER_IN_HEADER_RE.search(h):
        return False
    if _SELLER_IN_HEADER_RE.search(h):
        return True
    if _AMBIGUOUS_NAME_HEADER_RE.match(h.strip()):
        return True
    return False


def _client_name_mapped_indices(instr: str) -> List[int]:
    """Column indices referenced by a client_name DSL value (direct or concat; skip '>N')."""
    s = str(instr).strip().rstrip("?")
    if _RE_EXTRACT.match(s):
        return []
    if _RE_CONCAT.match(s):
        return [int(i) for i in s.split("+")]
    m2 = _RE_DIRECT.match(s)
    if m2:
        return [int(m2.group(1))]
    return []


def _parse_instr(instr: str):
    """Return (indices, is_extract) from a DSL value string; ([], False) if unrecognised."""
    s = instr.strip().rstrip("?")
    m = _RE_EXTRACT.match(s)
    if m:
        return [int(m.group(1))], True
    if _RE_CONCAT.match(s):
        return [int(i) for i in s.split("+")], False
    m2 = _RE_DIRECT.match(s)
    if m2:
        return [int(m2.group(1))], False
    return [], False


def _deterministic_review(
    mapping: Dict[str, Any],
    sample_row: List[str],
    header_row: Optional[List[str]] = None,
) -> tuple[bool, Optional[str]]:
    """
    Validate a DSL mapping against the sample row using hard coded rules.
    Returns (approved, critique_or_None).
    """
    issues: List[str] = []
    n_cols = len(sample_row)

    for field, instr in mapping.items():
        if instr is None:
            continue

        indices, is_extract = _parse_instr(str(instr))

        # Rule 1 — Index bounds: all referenced indices must exist
        for idx in indices:
            if idx >= n_cols:
                issues.append(
                    f"'{field}' references index {idx} but row has only "
                    f"{n_cols} column(s) (0–{n_cols - 1})"
                )

        # Rules below only apply to bare-index (non->N) single-column mappings
        if is_extract or len(indices) != 1:
            continue
        idx = indices[0]
        if idx >= n_cols:
            continue
        cell = sample_row[idx]

        # Rule 2 — Price in narrative cell: bare index when ">N" is needed
        if field == "price" and _PRICE_NARRATIVE_RE.search(cell):
            issues.append(
                f"'price' uses bare index '{instr}' but cell contains narrative "
                f"(e.g. '{cell[:60]}') — use '>N' to extract just the amount"
            )

        # Rule 3 — Postcode/region/county: bare index when cell is a full address
        if field in ("postcode", "region", "county") and _FULL_ADDRESS_RE.search(cell):
            issues.append(
                f"'{field}' uses bare index '{instr}' but cell looks like a full "
                f"address ('{cell[:60]}') — use '>N' to extract the value"
            )

    # Rule 4 — Commission null despite visible commission signal in any cell
    if mapping.get("commission") is None:
        for cell in sample_row:
            if _COMMISSION_SIGNAL_RE.search(cell):
                issues.append(
                    f"'commission' is null but a commission signal was detected "
                    f"in the row ('{cell[:60]}')"
                )
                break

    # Rule 5 — client_name must not reference vendor-only or ambiguous name headers
    client_instr = mapping.get("client_name")
    if client_instr is not None and header_row:
        for idx in _client_name_mapped_indices(str(client_instr)):
            if 0 <= idx < len(header_row):
                hdr = header_row[idx]
                if _header_implies_seller_not_buyer(hdr):
                    issues.append(
                        f"'client_name' maps column {idx} ({hdr!r}) but the header suggests seller "
                        f"or ambiguous CRM labelling — use vendor_name for that column and set "
                        f"client_name to null unless a column explicitly indicates the purchaser "
                        f"(buyer, applicant, prospect, purchaser, etc.)"
                    )

    if issues:
        critique = "; ".join(issues)
        return False, critique
    return True, None


# ---------------------------------------------------------------------------
# LangGraph state
# ---------------------------------------------------------------------------


class AgentState(TypedDict):
    """Mutable state threaded through the LangGraph nodes."""

    messages: Annotated[List[Any], add_messages]
    last_mapping: Optional[Dict[str, Any]]
    iteration: int
    approved: bool
    # Primary sample row (best scored or first data row)
    sample_row: List[str]
    # Optional context: the row immediately after sample_row
    context_row: Optional[List[str]]
    # True when context_row looks like an overflow of sample_row
    is_continuation: bool
    # Real column headers (CSV or PDF header row) — triggers header_mode prompting
    header_row: Optional[List[str]]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# Build a Gemini chat model from environment config.
def _build_gemini_model(model_name: str):
    """Construct a Gemini model instance and validate required key."""
    try:
        from langchain_google_genai import ChatGoogleGenerativeAI
    except ImportError as exc:
        raise ImportError(
            "langchain-google-genai is required for Gemini. "
            "Run: pip install langchain-google-genai"
        ) from exc

    api_key = settings.GOOGLE_API_KEY
    if not api_key:
        logger.error("GOOGLE_API_KEY is missing from settings")
        raise EnvironmentError(
            "GOOGLE_API_KEY is not set. Add it to .env or the environment."
        )
    logger.info("Building Gemini model (%s)", model_name)
    return ChatGoogleGenerativeAI(
        model=model_name,
        google_api_key=api_key,
        temperature=0,
    )


# Build a Groq chat model from environment config.
def _build_groq_model(model_name: str):
    """Construct a Groq model instance and validate required key."""
    try:
        from langchain_groq import ChatGroq
    except ImportError as exc:
        raise ImportError(
            "langchain-groq is required for Groq. Run: pip install langchain-groq"
        ) from exc

    api_key = settings.GROQ_API_KEY
    if not api_key:
        logger.error("GROQ_API_KEY is missing from settings")
        raise EnvironmentError(
            "GROQ_API_KEY is not set. Add it to .env or the environment."
        )
    logger.info("Building Groq model (%s)", model_name)
    return ChatGroq(
        model=model_name,
        api_key=api_key,
        temperature=0,
    )


# Resolve provider and model from env, then build the chat model.
def _build_default_model():
    """Construct the default provider model based on environment variables."""
    provider = (settings.EXTRACTOR_LLM_PROVIDER or "gemini").strip().lower()
    model_name = (settings.EXTRACTOR_LLM_MODEL or "").strip()

    if provider == "gemini":
        resolved_model = model_name or "gemini-2.5-flash-lite"
        return _build_gemini_model(resolved_model)

    if provider == "groq":
        resolved_model = model_name or "llama-3.1-8b-instant"
        return _build_groq_model(resolved_model)

    supported = "gemini, groq"
    logger.error("Unsupported EXTRACTOR_LLM_PROVIDER value: %s", provider)
    raise EnvironmentError(
        f"Unsupported EXTRACTOR_LLM_PROVIDER='{provider}'. Supported values: {supported}."
    )


def _parse_json_from_response(text: str) -> Dict[str, Any]:
    """Extract a JSON object from model output, stripping markdown fences if present."""
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return json.loads(text)


def _format_indexed_list(items: List[str], label: str = "cell") -> str:
    """Format a list as a numbered indexed block for the prompt."""
    lines = [f"  [{i}] {item!r}" for i, item in enumerate(items)]
    return f"{label} list ({len(items)} items):\n" + "\n".join(lines)


# ---------------------------------------------------------------------------
# Graph nodes
# ---------------------------------------------------------------------------


def _extractor_node(state: AgentState, model) -> Dict[str, Any]:
    """Produce a DSL column mapping for the sample (or header) row."""
    header_row = state.get("header_row")
    sample_row = state["sample_row"]
    context_row = state.get("context_row")
    is_continuation = state.get("is_continuation", False)
    history = state.get("messages", [])
    logger.info(
        "Extractor node iteration=%s header_mode=%s sample_cells=%s context_present=%s continuation=%s",
        state.get("iteration", 0),
        bool(header_row),
        len(sample_row),
        context_row is not None,
        is_continuation,
    )

    if header_row:
        # --- Header mode: map header names to canonical fields ---
        system_msg = _EXTRACTOR_SYSTEM_HEADER
        user_content = (
            "These are the actual column headers from the file:\n"
            + _format_indexed_list(header_row, "header")
            + "\n\nFor reference, here is the first data row:\n"
            + _format_indexed_list(sample_row, "data row")
        )
    else:
        # --- Data mode: infer from cell content ---
        system_msg = _EXTRACTOR_SYSTEM_DATA
        user_content = "Primary sample row:\n" + _format_indexed_list(
            sample_row, "primary row"
        )
        if context_row:
            cont_label = (
                "CONTINUATION of the primary row (overflow cells)"
                if is_continuation
                else "NEW record (separate property)"
            )
            user_content += (
                f"\n\nContext row immediately following the primary row "
                f"[{cont_label}]:\n"
                + _format_indexed_list(context_row, "context row")
                + "\n\nUse context row cells only if the primary row is missing a field you need."
            )

    # Attach previous critique on retry iterations
    if state["iteration"] > 0:
        last_critique = next(
            (
                m.content
                for m in reversed(history)
                if isinstance(m, AIMessage) and "critique" in m.content
            ),
            None,
        )
        if last_critique:
            user_content += (
                f"\n\nYour previous attempt was rejected:\n{last_critique}"
                "\n\nPlease fix the mapping accordingly."
            )

    response = model.invoke([system_msg, HumanMessage(content=user_content)])
    # Log the extractor node's full model response so reviewer interaction is observable.
    logger.info(
        "Extractor output (iteration=%s): %s",
        state.get("iteration", 0),
        str(response.content),
    )

    try:
        mapping = _parse_json_from_response(response.content)
        logger.info("Extractor parsed mapping successfully")
        # logger.info(
        #     "Extractor parsed mapping JSON: %s", json.dumps(mapping, ensure_ascii=False)
        # )
    except (json.JSONDecodeError, ValueError):
        logger.exception(
            "Extractor returned invalid JSON; reusing previous mapping if present"
        )
        mapping = state.get("last_mapping") or {}

    return {
        "messages": [response],
        "last_mapping": mapping,
        "iteration": state["iteration"] + 1,
    }


def _reviewer_node(state: AgentState, _model) -> Dict[str, Any]:
    """Validate the mapping deterministically — no LLM call, no hallucinations."""
    mapping = state.get("last_mapping") or {}
    sample_row = state["sample_row"]
    iteration = state.get("iteration", 0)

    logger.info(
        "Reviewer (deterministic) iteration=%s mapping_keys=%s",
        iteration,
        len(mapping),
    )

    approved, critique = _deterministic_review(
        mapping, sample_row, state.get("header_row")
    )

    if approved:
        logger.info("Reviewer approved mapping")
    else:
        logger.info("Reviewer rejected mapping — critique: %s", critique)

    # Package the critique as an AIMessage so the extractor can pick it up on retry
    critique_msg = AIMessage(
        content=f"critique: {critique}" if critique else "approved"
    )
    return {"messages": [critique_msg], "approved": approved}


def _should_continue(state: AgentState) -> str:
    """Route back to extractor for another attempt, or finish."""
    if state.get("approved") or state["iteration"] >= MAX_ITERATIONS:
        logger.info(
            "Reflection loop ended approved=%s iteration=%s max=%s",
            state.get("approved", False),
            state["iteration"],
            MAX_ITERATIONS,
        )
        return END
    logger.info(
        "Reflection loop continuing to extractor iteration=%s", state["iteration"]
    )
    return "extractor"


# ---------------------------------------------------------------------------
# Graph factory
# ---------------------------------------------------------------------------


def build_agent(model=None):
    """
    Build and compile the reflection graph.

    Args:
        model: Any LangChain chat model. Defaults to env-selected provider/model.
    """
    if model is None:
        logger.info("No model override provided; constructing default model")
        model = _build_default_model()

    graph = StateGraph(AgentState)
    graph.add_node("extractor", lambda s: _extractor_node(s, model))
    graph.add_node("reviewer", lambda s: _reviewer_node(s, model))
    graph.set_entry_point("extractor")
    graph.add_edge("extractor", "reviewer")
    graph.add_conditional_edges("reviewer", _should_continue)
    return graph.compile()


def run_agent(
    sample_row: List[str],
    context_row: Optional[List[str]] = None,
    is_continuation: bool = False,
    header_row: Optional[List[str]] = None,
    model=None,
) -> Dict[str, Any]:
    """
    Run the reflection agent on a sample row.

    Args:
        sample_row:     Best data row (or first data row for CSV).
        context_row:    Row immediately following sample_row (optional extra context).
        is_continuation: True when context_row appears to be an overflow of sample_row.
        header_row:     Real column header names when available (activates header-mode).
        model:          Optional LangChain chat model override.

    Returns:
        DSL column-map dict (canonical_field → instruction | null).
    """
    logger.info(
        "run_agent called sample_cells=%s context_present=%s continuation=%s header_mode=%s",
        len(sample_row),
        context_row is not None,
        is_continuation,
        bool(header_row),
    )
    app = build_agent(model)

    initial_state: AgentState = {
        "messages": [],
        "last_mapping": None,
        "iteration": 0,
        "approved": False,
        "sample_row": sample_row,
        "context_row": context_row,
        "is_continuation": is_continuation,
        "header_row": header_row,
    }

    try:
        final_state = app.invoke(initial_state)
    except Exception:
        logger.exception("LangGraph invocation failed during run_agent")
        raise
    logger.info(
        "run_agent completed approved=%s iterations=%s mapping_keys=%s",
        final_state.get("approved"),
        final_state.get("iteration"),
        len((final_state.get("last_mapping") or {}).keys()),
    )
    return final_state.get("last_mapping") or {}
