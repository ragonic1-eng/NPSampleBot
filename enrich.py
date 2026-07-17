"""Runtime enrichment helpers for /updatesamplelist.

When MMS returns a sample-submission row, three of the columns Full Sample
Listing wants are not in MMS itself:

  - Country     (often blank on legacy MMS rows)
  - Taste describe   (~20 keywords; built by Haiku, cached on disk)
  - Category    (one of 6 fixed values; derived from category tabs + Haiku)

This module loads the same on-disk caches that the offline backfill scripts
(_country_cache.json, _taste_keywords_cache.json, _category_cache.json)
populated, and falls back to Haiku 4.5 only when an unseen product code
shows up. Cheapest-first: cache hit → free; cache miss → 1 small Haiku call.

The cache files are local-disk; Railway's filesystem is ephemeral, so on a
fresh deploy the bot rebuilds the cache lazily as new MMS data is pulled.
That's fine — the offline scripts already populated the bulk of the
catalogue, and running /updatesamplelist a few times catches up.
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Iterable

import config

log = logging.getLogger(__name__)

_BASE = os.path.dirname(__file__)
COUNTRY_CACHE_PATH = os.path.join(_BASE, "_country_cache.json")
TASTE_CACHE_PATH = os.path.join(_BASE, "_taste_keywords_cache.json")
CATEGORY_CACHE_PATH = os.path.join(_BASE, "_category_cache.json")


# ---------- cache load/save ----------

def _load_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        return json.load(open(path, encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        log.warning("enrich: failed to load %s: %s", path, e)
        return {}


def _save_json(path: str, data: dict) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
    except Exception as e:  # noqa: BLE001
        log.warning("enrich: failed to save %s: %s", path, e)


# ---------- Country (cheapest first) ----------

CATEGORIES = [
    "Snack",
    "Noodle & Instant Soup",
    "Sauces & Mixes",
    "Marinades",
    "Oil",
    "Beverage",
]

COMPANY_SUFFIX_TO_COUNTRY = {
    "pte ltd": "Singapore", "pte. ltd": "Singapore", "pte. ltd.": "Singapore",
    "sdn bhd": "Malaysia", "sdn. bhd.": "Malaysia",
    "pvt ltd": "India", "pvt. ltd.": "India",
    "(p) ltd": "India", "(pvt) ltd": "India", "(pvt.) ltd": "India",
    "fzco": "United Arab Emirates", "fz-llc": "United Arab Emirates",
    "fz llc": "United Arab Emirates",
}
COUNTRY_TOKENS = [
    "Bangladesh", "Vietnam", "Indonesia", "Thailand", "Malaysia", "Singapore",
    "Philippines", "Cambodia", "Myanmar", "Nepal", "Pakistan", "Sri Lanka",
    "India", "China", "Taiwan", "Korea", "Japan",
    "Saudi Arabia", "Bahrain", "Kuwait", "Oman", "Qatar", "Jordan", "Lebanon",
    "Egypt", "Syria", "Iraq", "Iran", "Yemen",
    "Australia", "New Zealand", "United Arab Emirates", "United States",
    "United Kingdom",
]

# 2-letter ISO codes → canonical full name. Used to normalize values like
# "SG" or "SG (Singapore)" coming out of MMS.
ISO_TO_COUNTRY = {
    "SG": "Singapore", "MY": "Malaysia", "TH": "Thailand", "ID": "Indonesia",
    "VN": "Vietnam", "PH": "Philippines", "IN": "India", "BD": "Bangladesh",
    "LK": "Sri Lanka", "PK": "Pakistan", "NP": "Nepal", "KH": "Cambodia",
    "MM": "Myanmar", "CN": "China", "TW": "Taiwan", "KR": "Korea", "JP": "Japan",
    "SA": "Saudi Arabia", "BH": "Bahrain", "KW": "Kuwait", "OM": "Oman",
    "QA": "Qatar", "JO": "Jordan", "LB": "Lebanon", "EG": "Egypt", "SY": "Syria",
    "IQ": "Iraq", "IR": "Iran", "YE": "Yemen", "AU": "Australia", "NZ": "New Zealand",
    "AE": "United Arab Emirates", "US": "United States", "USA": "United States",
    "GB": "United Kingdom", "UK": "United Kingdom",
}

# Free-text aliases that aren't ISO codes but show up in MMS / customer notes.
COUNTRY_ALIASES = {
    "uae": "United Arab Emirates", "u.a.e": "United Arab Emirates",
    "u.a.e.": "United Arab Emirates", "emirates": "United Arab Emirates",
    "ksa": "Saudi Arabia", "kingdom of saudi arabia": "Saudi Arabia",
    "south korea": "Korea", "republic of korea": "Korea",
    "korea, south": "Korea", "korea (south)": "Korea",
    "viet nam": "Vietnam",
    "burma": "Myanmar", "myanmar (burma)": "Myanmar",
    "p.r. china": "China", "prc": "China", "p.r.c.": "China",
    "philippine": "Philippines", "philipines": "Philippines",
}


def normalize_country(s: str) -> str:
    """Canonicalize a country string into the project's preferred format.

    Examples:
      "SG"                -> "Singapore"
      "SG (Singapore)"    -> "Singapore"
      "Singapore (SG)"    -> "Singapore"
      "UAE"               -> "United Arab Emirates"
      "south korea"       -> "Korea"
      "Singapore"         -> "Singapore"   (passthrough)
      ""                  -> ""

    Unknown values are returned trimmed but otherwise untouched so we don't
    accidentally blank out something legitimate the maintainer typed by hand.
    """
    if not s:
        return ""
    raw = str(s).strip()
    if not raw:
        return ""

    # Pull "outside (inside)" apart so both halves get a chance to match.
    import re
    m = re.match(r"^\s*([^()]+?)\s*\(([^)]+)\)\s*$", raw)
    candidates = [m.group(1).strip(), m.group(2).strip()] if m else [raw]

    for cand in candidates:
        if not cand:
            continue
        upper = cand.upper().replace(".", "").strip()
        if upper in ISO_TO_COUNTRY:
            return ISO_TO_COUNTRY[upper]
        lower = cand.lower().strip()
        if lower in COUNTRY_ALIASES:
            return COUNTRY_ALIASES[lower]
        for tok in COUNTRY_TOKENS:
            if tok.lower() == lower:
                return tok

    # Fallback: hand back the cleaned outside half so manual entries survive.
    return candidates[0].strip()


def _norm_company(name: str) -> str:
    return " ".join((name or "").lower().split())


def _country_from_suffix(name: str) -> str:
    n = _norm_company(name)
    for suf, country in COMPANY_SUFFIX_TO_COUNTRY.items():
        if n.endswith(suf) or f" {suf})" in n:
            return country
    return ""


def _country_from_tokens(name: str) -> str:
    if not name:
        return ""
    low = name.lower()
    for tok in COUNTRY_TOKENS:
        if tok.lower() in low:
            return tok
    return ""


def resolve_country(
    *,
    raw_country: str,
    customer_name: str,
    customer_map: dict[str, str],
    country_cache: dict[str, str],
    haiku_client=None,
) -> str:
    """Resolve a sample-row's Country, cheapest path first.

    Stages:
      1. raw_country (whatever MMS gave us)
      2. customer_map lookup (other rows of the same customer)
      3. country word inside customer name
      4. unambiguous company suffix
      5. country_cache hit
      6. Haiku call (only if client provided AND name nonempty)
    """
    if (raw_country or "").strip():
        return normalize_country(raw_country)
    if not customer_name:
        return ""
    inferred = customer_map.get(_norm_company(customer_name))
    if inferred:
        return normalize_country(inferred)
    tok = _country_from_tokens(customer_name)
    if tok:
        return tok  # already canonical
    suf = _country_from_suffix(customer_name)
    if suf:
        return suf  # already canonical
    if customer_name in country_cache:
        return normalize_country(country_cache[customer_name])
    if haiku_client is None:
        return ""
    # Last resort: ask Haiku, then normalize whatever it returns.
    guess = normalize_country(_ask_haiku_country(haiku_client, customer_name))
    country_cache[customer_name] = guess
    _save_json(COUNTRY_CACHE_PATH, country_cache)
    return guess


def _ask_haiku_country(client, customer_name: str) -> str:
    prompt = (
        "You're cataloguing customer companies for a Singapore-based food "
        "ingredients supplier (NP Foods) whose customers are primarily in "
        "South & Southeast Asia, the Middle East, and Oceania. Given the "
        "customer name below, return STRICT JSON only with the country you "
        "believe they're based in. If you can't tell, return an empty "
        "string.\n\nCustomer name: " + customer_name +
        '\n\nReturn JSON: {"country": "<country or empty>"}'
    )
    try:
        msg = client.messages.create(
            model="claude-haiku-4-5", max_tokens=80,
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text").strip()
        s, e = text.find("{"), text.rfind("}")
        if s == -1 or e == -1:
            return ""
        return str(json.loads(text[s : e + 1]).get("country", "")).strip()
    except Exception as ex:  # noqa: BLE001
        log.warning("enrich.country Haiku failed for %r: %s", customer_name, ex)
        return ""


# ---------- Taste describe (house-style blurb) ----------
#
# The prompt itself now lives in local_llm.py, which few-shots the model with
# REAL rows from the sheet — that transfers the house style ("savoury aged
# cheese — creamy umami profile") far better than a written spec could.


def resolve_taste(
    *,
    code: str,
    name: str,
    taste_cache: dict[str, str],
    haiku_client=None,
    fsl_map: dict[str, str] | None = None,
) -> str:
    """Resolve the taste blurb — free paths only (V1.17.1: no Anthropic).

    Order:
      1. fsl_map     — already in Full Sample Listing (free, persistent)
      2. taste_cache — on-disk JSON (free; lost on Railway redeploy)
      3. local_llm   — Ollama on the local PC (free); unreachable from
                       Railway, which is fine: the cell stays blank and a
                       later local batch run fills it into the sheet, after
                       which step 1 serves it forever.

    ``haiku_client`` is accepted and ignored — kept so existing callers
    (sync_engine, the backfill scripts) keep working unchanged.
    """
    if not code:
        return ""
    upper = code.strip().upper()
    if fsl_map and fsl_map.get(upper):
        return fsl_map[upper]
    if upper in taste_cache and taste_cache[upper]:
        return taste_cache[upper]
    blurb = _ask_local_taste(name)
    if blurb:
        taste_cache[upper] = blurb
        _save_json(TASTE_CACHE_PATH, taste_cache)
    return blurb


def _ask_local_taste(name: str) -> str:
    """Free local generation. Returns "" when Ollama isn't reachable."""
    if not name:
        return ""
    try:
        import local_llm
        return local_llm.describe_taste(name)
    except Exception as ex:  # noqa: BLE001
        log.warning("enrich.taste local_llm failed for %r: %s", name, ex)
        return ""


# ---------- Category (one of 6 strings) ----------
#
# Prompt lives in local_llm.py. Note the hard-won detail encoded there: the
# model MUST be told ~78% of this catalogue is "Snack" (powder dusted on
# chips). Without that context a general model reads "BBQ SEASONING" and
# answers "Sauces & Mixes" — measured 8% accuracy vs 92% with it.


def resolve_category(
    *,
    code: str,
    name: str,
    tab_map: dict[str, str],
    category_cache: dict[str, str],
    haiku_client=None,
    fsl_map: dict[str, str] | None = None,
) -> str:
    """Resolve category cheapest-first.

    Order (V1.17.1: no Anthropic — free paths only):
      1. tab_map        — the 6 authoritative category tabs (free)
      2. fsl_map        — past FSL row for this code (free, persistent)
      3. category_cache — on-disk JSON (free; lost on Railway redeploy)
      4. local_llm      — Ollama on the local PC (free); unreachable from
                          Railway, so the cell stays blank there until a
                          local batch run fills it into the sheet.

    ``haiku_client`` is accepted and ignored — kept for caller compatibility.
    """
    if not code:
        return ""
    upper = code.strip().upper()
    if upper in tab_map:
        return tab_map[upper]
    if fsl_map and fsl_map.get(upper) in CATEGORIES:
        return fsl_map[upper]
    if upper in category_cache and category_cache[upper] in CATEGORIES:
        return category_cache[upper]
    cat = _ask_local_category(name)
    if cat:
        category_cache[upper] = cat
        _save_json(CATEGORY_CACHE_PATH, category_cache)
    return cat


def _ask_local_category(name: str) -> str:
    """Free local classification. Returns "" when Ollama isn't reachable."""
    if not name:
        return ""
    try:
        import local_llm
        return local_llm.classify_category(name)
    except Exception as ex:  # noqa: BLE001
        log.warning("enrich.category local_llm failed for %r: %s", name, ex)
        return ""


# ---------- Convenience: load all caches at once ----------

def load_all_caches() -> tuple[dict, dict, dict]:
    return (
        _load_json(COUNTRY_CACHE_PATH),
        _load_json(TASTE_CACHE_PATH),
        _load_json(CATEGORY_CACHE_PATH),
    )


def haiku_client():
    """Lazy Anthropic client; returns None if no key configured."""
    if not config.ANTHROPIC_API_KEY:
        return None
    try:
        from anthropic import Anthropic
        return Anthropic(api_key=config.ANTHROPIC_API_KEY)
    except Exception as e:  # noqa: BLE001
        log.warning("enrich: Anthropic client init failed: %s", e)
        return None
