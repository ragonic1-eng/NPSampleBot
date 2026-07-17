"""Free, local, CPU-only enrichment via Ollama — replaces the Haiku calls.

Why this exists: the taste/category columns used to be filled by Claude Haiku
over the API. This module does the same job on the user's own PC for $0, with
no Anthropic dependency at all.

Design notes that actually matter for output quality:

  • **House style, not invention.** The "Taste describe" column is a short
    blurb in a house format — "savoury aged cheese — creamy umami profile",
    NOT a keyword dump. 16,350 existing rows share only ~4,474 distinct
    blurbs, so the field is a semi-controlled vocabulary. We few-shot the
    model with REAL examples pulled from that data (_fewshot.json when
    present, otherwise the baked-in defaults below), which transfers the
    existing style far better than any prompt description could.

  • **Blurbs are short → fast.** A house-style blurb is ~10 tokens vs ~170
    for the old keyword format. That alone makes the local run ~10x faster.

  • **Category needs business context.** 78% of the catalogue is "Snack"
    (powder dusted on chips/pellets). Without that context a general model
    reads "BBQ SEASONING" and says "Sauces & Mixes" — measured 8% accuracy.
    The prompt states the base rate and few-shots real products per class,
    and the JSON schema pins output to the 6 valid strings so category
    drift is structurally impossible.

  • **Hybrid-reasoning trap.** Qwen3.5 / Gemma 4 think by default, which
    turns a 10-token answer into 600 tokens and a 2-hour job into 20. We
    always send think=False.

Where this runs: LOCALLY (the batch scripts on the PC). Railway cannot reach
this Ollama, and that's fine — sync_engine leaves new codes blank, then a
local batch run fills them into the sheet, and the sheet is the cache every
later lookup reads from (see enrich.resolve_taste / resolve_category).

Public API:
    available() -> bool
    describe_taste(name) -> str        # "" on failure
    classify_category(name) -> str     # "" or one of enrich.CATEGORIES
"""
from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request

import config

log = logging.getLogger(__name__)

CATEGORIES = [
    "Snack",
    "Noodle & Instant Soup",
    "Sauces & Mixes",
    "Marinades",
    "Oil",
    "Beverage",
]

# Tuning for a CPU-only box (measured on an i7-12700K, 32 GB):
#   num_thread 12 — hybrid P/E cores peak around 12-16, and DROP at 20.
#   num_ctx 1024  — these prompts are tiny; the 4k default just wastes KV RAM.
#   temperature 0 — this is classification/canonicalisation, not creativity.
_OPTIONS = {
    "temperature": 0,
    "num_thread": int(os.getenv("OLLAMA_NUM_THREAD", "12")),
    "num_ctx": 1024,
}
_TIMEOUT = float(os.getenv("OLLAMA_TIMEOUT", "120"))

# Fallbacks used when _fewshot.json isn't present. Real rows from the sheet.
_DEFAULT_TASTE_SHOTS = [
    ("NAMKEEN SEASONING", "savoury seasoning — flavour-balanced base"),
    ("BBQ SEASONING", "smoky barbecue — sweet-savoury wood-smoke profile"),
    ("CHEESE SEASONING", "savoury aged cheese — creamy umami profile"),
    ("TANDOORI CHICKEN SEASONING", "chicken savoury — rich umami chicken"),
    ("TOMATO SOUP BASE", "tomato — sweet-tangy"),
    ("SWEET CHILLI & RED PEPPER SEASONING", "chili savoury heat"),
    ("KOREAN BBQ CHICKEN", "Korean-style sweet-spicy"),
    ("VEGETABLE PASTA SOUP CONC.", "vegetable savoury — mixed vegetable umami"),
]
_DEFAULT_CAT_SHOTS = [
    ("BLACK PEPPER OIL SEASONING", "Snack"),
    ("sambal balado", "Snack"),
    ("RENDANG DRY NOODLE SEASONING", "Noodle & Instant Soup"),
    ("TOM YUM NOODLE SOUP SEASONING", "Noodle & Instant Soup"),
    ("BLACK GARLIC SEASONING", "Sauces & Mixes"),
    ("SPICY GOCHUJANG SEASONING", "Sauces & Mixes"),
    ("GRILLED KEBAB MARINADE", "Marinades"),
    ("WIENER SPICE MARINADE (VIENNA-STYLE)", "Marinades"),
    ("ROASTED GARLIC OIL", "Oil"),
    ("SHRIMP OIL", "Oil"),
    ("MOCHA FLAVOUR", "Beverage"),
    ("BANANA MILK SEASONING", "Beverage"),
]

_FEWSHOT_PATH = os.path.join(os.path.dirname(__file__), "_fewshot.json")


def _load_shots() -> tuple[list, list]:
    """Prefer exemplars mined from the live sheet; fall back to the baked-in set."""
    try:
        with open(_FEWSHOT_PATH, encoding="utf-8") as f:
            d = json.load(f)
        taste = [tuple(x) for x in d.get("taste", [])] or _DEFAULT_TASTE_SHOTS
        cat = [tuple(x) for x in d.get("cat", [])] or _DEFAULT_CAT_SHOTS
        return taste, cat
    except Exception:  # noqa: BLE001 — file optional
        return _DEFAULT_TASTE_SHOTS, _DEFAULT_CAT_SHOTS


_TASTE_SHOTS, _CAT_SHOTS = _load_shots()

_TASTE_SCHEMA = {
    "type": "object",
    "properties": {"taste": {"type": "string"}},
    "required": ["taste"],
}
_CAT_SCHEMA = {
    "type": "object",
    "properties": {"category": {"type": "string", "enum": CATEGORIES}},
    "required": ["category"],
}


def _taste_prompt(name: str) -> str:
    shots = "\n".join(f"{n}  ->  {t}" for n, t in _TASTE_SHOTS)
    return (
        "You write the 'Taste describe' field for NP Foods, a seasoning "
        "manufacturer. It is a SHORT house-style blurb naming the dominant "
        "flavour, optionally followed by ' — ' and a brief profile.\n\n"
        "Copy this style exactly:\n"
        f"{shots}\n\n"
        "Rules:\n"
        "  - Describe ONLY what the product name states. Never invent "
        "ingredients, dishes or cuisines that aren't implied by the name.\n"
        "  - Lowercase, except real proper nouns (Korean, Japanese, Italian).\n"
        "  - Max ~10 words. No marketing words. No product codes.\n"
        "  - Reuse an example's wording verbatim when the product matches.\n\n"
        f"Product name: {name}\n"
        'Return JSON: {"taste": "<blurb>"}'
    )


def _category_prompt(name: str) -> str:
    shots = "\n".join(f"{n}  ->  {c}" for n, c in _CAT_SHOTS)
    return (
        "Classify an NP Foods product into exactly ONE category.\n\n"
        "CRITICAL CONTEXT: NP Foods mainly sells POWDERED SEASONING dusted "
        "onto snacks. About 78% of the catalogue is 'Snack'. A plain "
        "flavour name like 'BBQ SEASONING', 'CHEESE SEASONING' or 'BUTTER "
        "SEASONING' is a snack-dusting powder => Snack. Only choose another "
        "category when the name explicitly says so:\n"
        "  - Noodle & Instant Soup: says noodle / ramen / instant soup\n"
        "  - Sauces & Mixes: says sauce / paste / dip / rice mix / concentrate\n"
        "  - Marinades: says marinade / kebab / grill rub for raw meat\n"
        "  - Oil: says oil\n"
        "  - Beverage: a drink — milk / coffee / mocha / latte / juice\n"
        "  - Snack: everything else, including bare flavour names\n\n"
        "Examples:\n"
        f"{shots}\n\n"
        f"Product name: {name}\n"
        'Return JSON: {"category": "<one of the six>"}'
    )


# Availability is probed ONCE and remembered. This matters on Railway, where
# no Ollama exists: without the latch, every new product code would attempt a
# connection (and any slow failure would stall the nightly sync). One cheap
# 3s probe decides for the whole process; unset OLLAMA_PROBE_ONCE=0 to re-probe.
_available: bool | None = None


def _is_available() -> bool:
    global _available
    if _available is None or os.getenv("OLLAMA_PROBE_ONCE", "1") != "1":
        _available = available()
        if not _available:
            log.info(
                "local_llm: no Ollama at %s — taste/category left blank "
                "(expected on Railway; a local batch run fills them later)",
                config.OLLAMA_URL,
            )
    return _available


def _generate(prompt: str, schema: dict) -> dict | None:
    """One Ollama call with structured output. Returns parsed dict or None."""
    if not _is_available():
        return None
    body = {
        "model": config.OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "format": schema,
        # Qwen3.5 / Gemma 4 are hybrid-reasoning: without this a 10-token
        # answer becomes 600 and the batch takes 10x longer.
        "think": False,
        "options": _OPTIONS,
    }
    req = urllib.request.Request(
        f"{config.OLLAMA_URL}/api/generate",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as r:
            resp = json.loads(r.read())
        return json.loads(resp.get("response") or "{}")
    except Exception as e:  # noqa: BLE001 — offline / timeout / bad JSON
        log.warning("local_llm: %s failed: %s", config.OLLAMA_MODEL, e)
        return None


def available() -> bool:
    """True when an Ollama server is reachable (i.e. we're on the local PC)."""
    try:
        with urllib.request.urlopen(f"{config.OLLAMA_URL}/api/version", timeout=3):
            return True
    except Exception:  # noqa: BLE001
        return False


def describe_taste(name: str) -> str:
    """House-style taste blurb for a product name. "" if unavailable."""
    if not (name or "").strip():
        return ""
    out = _generate(_taste_prompt(name.strip()), _TASTE_SCHEMA)
    if not out:
        return ""
    return " ".join(str(out.get("taste", "")).split())[:120]


def _clean_category(raw) -> str:
    cat = str(raw or "").strip()
    if cat in CATEGORIES:
        return cat
    for valid in CATEGORIES:  # tolerate case drift
        if cat.lower() == valid.lower():
            return valid
    return ""


def classify_category(name: str) -> str:
    """One of CATEGORIES, or "" if unavailable / invalid."""
    if not (name or "").strip():
        return ""
    out = _generate(_category_prompt(name.strip()), _CAT_SCHEMA)
    if not out:
        return ""
    return _clean_category(out.get("category"))


_BOTH_SCHEMA = {
    "type": "object",
    "properties": {
        "taste": {"type": "string"},
        "category": {"type": "string", "enum": CATEGORIES},
    },
    "required": ["taste", "category"],
}


def describe_and_classify(name: str) -> tuple[str, str]:
    """Both fields in ONE call — half the CPU time of calling them separately.

    Prompt-processing dominates here (the few-shot blocks are far longer than
    the answers), so merging the two prompts costs little and saves a whole
    round of weight-streaming. Returns ("", "") when Ollama is unreachable;
    callers treat that as "leave the cell blank".
    """
    name = (name or "").strip()
    if not name:
        return "", ""
    taste_shots = "\n".join(f"{n}  ->  {t}" for n, t in _TASTE_SHOTS)
    cat_shots = "\n".join(f"{n}  ->  {c}" for n, c in _CAT_SHOTS)
    prompt = (
        "You are cataloguing a product for NP Foods, a seasoning "
        "manufacturer. Produce TWO fields for the product name below.\n\n"
        "FIELD 1 — taste: a SHORT house-style blurb naming the dominant "
        "flavour, optionally ' — ' then a brief profile. Copy this style:\n"
        f"{taste_shots}\n"
        "  Describe ONLY what the name states — never invent ingredients, "
        "dishes or cuisines. Lowercase except proper nouns. Max ~10 words. "
        "Reuse an example's wording verbatim when the product matches.\n\n"
        "FIELD 2 — category: exactly one of "
        f"{', '.join(CATEGORIES)}.\n"
        "  CRITICAL: NP Foods mainly sells POWDERED SEASONING dusted onto "
        "snacks — ~78% of the catalogue is 'Snack'. A bare flavour name "
        "('BBQ SEASONING', 'CHEESE SEASONING') is a snack-dusting powder "
        "=> Snack. Only pick another category when the name says so: "
        "noodle/ramen/instant soup => Noodle & Instant Soup; "
        "sauce/paste/dip/rice mix/concentrate => Sauces & Mixes; "
        "marinade/kebab/grill rub for raw meat => Marinades; "
        "oil => Oil; a drink (milk/coffee/mocha/latte) => Beverage.\n"
        f"{cat_shots}\n\n"
        f"Product name: {name}\n"
        'Return JSON: {"taste": "<blurb>", "category": "<one of the six>"}'
    )
    out = _generate(prompt, _BOTH_SCHEMA)
    if not out:
        return "", ""
    taste = " ".join(str(out.get("taste", "")).split())[:120]
    return taste, _clean_category(out.get("category"))
