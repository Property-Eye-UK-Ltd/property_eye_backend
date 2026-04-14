# ai_agent.py — LangGraph reflection agent for AI-powered column mapping.
#
# Two-node graph:   extractor → reviewer → (approved | retry → extractor)
#
# Only a single sample row + optional context row is ever sent to the model.
# All private listing data stays local.
#
# Two prompt modes:
#   header_mode — the file has real column header names (CSV or PDF with header row).
#                 The AI just name-matches; much cleaner and more reliable.
#   data_mode   — no header row; the AI infers from cell content.
#                 An optional context_row (with continuation hint) provides extra signal.

from __future__ import annotations

import json
import logging
import os
import re
from typing import Annotated, Any, Dict, List, Optional

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict

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
  client_name       — Property owner / vendor full name.  Use "N+M" if split across cells.
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
4. Output ONLY valid JSON — no markdown fences, no explanation outside the JSON.
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
  "client_name": "6",
  "title_number": "7",
  "contract_duration": null
}

Note: postcode/region/county all use ">0" because they are substrings of cell 0.
      price uses ">2" because the amount is buried inside narrative text.
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
  [6] 'Commission'
  [7] 'Title Number'

Correct output:
{
  "address": "0",
  "postcode": ">0",
  "region": ">0",
  "county": ">0",
  "property_number": ">0",
  "price": "2",
  "withdrawn_date": "3",
  "client_name": "5",
  "commission": "6",
  "title_number": "7",
  "contract_duration": null
}

Note: 'Property Address' header implies the column holds a full address string, so
      postcode/region/county all use ">0".  Dedicated columns like 'Asking Price'
      use a bare index because the cell will contain just the value.
      title_number maps to "7" because the header is explicitly named 'Title Number'.
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

_REVIEWER_SYSTEM = SystemMessage(
    content=(
        "You are a meticulous schema auditor. You receive a sample row and a proposed column map.\n\n"
        "EVALUATION PROTOCOL — follow these steps in order for EVERY field:\n\n"
        "Step 1 — READ THE ACTUAL MAPPING VALUE first.\n"
        "  Look up what the proposed column map says for each field (e.g. '0', '>0', '5+6', null).\n"
        "  Do NOT apply any rule until you have read that value.\n\n"
        "Step 2 — Apply the rules below ONLY based on what you actually read:\n\n"
        "  Rule A — Bounds check:\n"
        "    If the value is a bare index or concat (e.g. '2', '5+6'), verify the index exists in the row.\n\n"
        "  Rule B — '>N' check (ONLY applies when the value is a BARE INDEX like '0', '2', not '>0', '>2'):\n"
        "    If the value is '>N' — the field is ALREADY correctly using extraction. DO NOT flag it.\n"
        "    Only flag if the value is a bare 'N' (no '>') AND the cell content contains more than\n"
        "    just the canonical value. Specifically:\n"
        "      • Flag postcode='N' if cell N is a full address string (contains commas + street name).\n"
        "        DO NOT flag postcode='>N' — it is already correct.\n"
        "      • Flag price='N' if cell N contains narrative words: 'Offers', 'in excess of',\n"
        "        'Guide price', 'OIEO', 'POA', 'approximately'. A cell with just '£699,950' is fine as 'N'.\n"
        "        DO NOT flag price='>N' — it is already correct.\n"
        "      • Flag region='N' or county='N' if cell N is a combined full address string.\n"
        "        DO NOT flag region='>N' or county='>N' — already correct.\n\n"
        "  Rule C — Wrong cell check:\n"
        "    Flag if a field is clearly mapped to the wrong type (e.g. a date cell mapped to price).\n\n"
        "  Rule D — Commission check:\n"
        "    Flag commission=null ONLY if you can see a cell that literally contains a % symbol\n"
        "    or the words 'commission', 'agency fee', or 'sole agency' / 'multi agency' with a rate.\n"
        "    A plain price like '£699,950' is NOT evidence of commission. Do NOT flag commission=null\n"
        "    just because price is present.\n\n"
        "  Rule E — Title number vs property number:\n"
        "    title_number refers to an HMLR title number (e.g. 'HD567890', 'TGL12345' — letters then digits).\n"
        "    property_number is the house number ('11', 'Flat 3'). These are different fields.\n"
        "    Do NOT flag title_number=null because a property number exists.\n\n"
        "  Rule F — Uncertain flag:\n"
        "    If a field uses 'N?', verify the cell content; suggest removing '?' if the mapping is clearly correct.\n\n"
        "Respond with ONLY valid JSON in one of two forms:\n"
        '  { "approved": true }\n'
        '  { "approved": false, "critique": "<cite the field name, its actual mapped value, and why it is wrong>" }'
    )
)

MAX_ITERATIONS = 3


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

    api_key = os.environ.get("GOOGLE_API_KEY", "")
    if not api_key:
        logger.error("GOOGLE_API_KEY is missing from environment")
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

    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        logger.error("GROQ_API_KEY is missing from environment")
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
    provider = os.environ.get("EXTRACTOR_LLM_PROVIDER", "gemini").strip().lower()
    model_name = os.environ.get("EXTRACTOR_LLM_MODEL", "").strip()

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


def _reviewer_node(state: AgentState, model) -> Dict[str, Any]:
    """Validate the latest mapping; approve or return a critique."""
    sample_row = state["sample_row"]
    header_row = state.get("header_row")
    mapping_text = json.dumps(state.get("last_mapping", {}), indent=2)

    if header_row:
        row_text = (
            "Column headers:\n"
            + _format_indexed_list(header_row, "header")
            + "\n\nFirst data row (for content verification):\n"
            + _format_indexed_list(sample_row, "data row")
        )
    else:
        row_text = _format_indexed_list(sample_row, "primary row")

    user_content = f"{row_text}\n\nProposed column map:\n{mapping_text}"
    logger.info(
        "Reviewer node started iteration=%s mapping_keys=%s",
        state.get("iteration", 0),
        len((state.get("last_mapping") or {}).keys()),
    )
    response = model.invoke([_REVIEWER_SYSTEM, HumanMessage(content=user_content)])
    # Log the reviewer node's full model response for reflection transparency.
    logger.info(
        "Reviewer output (iteration=%s): %s",
        state.get("iteration", 0),
        str(response.content),
    )

    try:
        verdict = _parse_json_from_response(response.content)
        approved = bool(verdict.get("approved", False))
        logger.info("Reviewer verdict parsed approved=%s", approved)
        # logger.info(
        #     "Reviewer parsed verdict JSON: %s", json.dumps(verdict, ensure_ascii=False)
        # )
    except (json.JSONDecodeError, ValueError):
        logger.exception("Reviewer returned invalid JSON; defaulting to approved=True")
        approved = True  # malformed verdict → accept current mapping

    return {"messages": [response], "approved": approved}


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
