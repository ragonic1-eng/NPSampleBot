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
    "Australia", "New Zealand",
]


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
        return raw_country.strip()
    if not customer_name:
        return ""
    inferred = customer_map.get(_norm_company(customer_name))
    if inferred:
        return inferred
    tok = _country_from_tokens(customer_name)
    if tok:
        return tok
    suf = _country_from_suffix(customer_name)
    if suf:
        return suf
    if customer_name in country_cache:
        return country_cache[customer_name]
    if haiku_client is None:
        return ""
    # Last resort: ask Haiku.
    guess = _ask_haiku_country(haiku_client, customer_name)
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


# ---------- Taste describe (~20 keywords) ----------

_TASTE_PROMPT = """\
You're tagging a seasoning product for an internal search index. Given the
product code and name, output ~20 comma-separated KEYWORDS a sales rep would
realistically type when looking for this flavour profile.

Product code: {code}
Product name: {name}

Rules:
  - Lowercase only.
  - Single words or short multi-word phrases (≤3 words each).
  - Mix of: flavour notes, heat level, cuisine/region, dish association,
    ingredient cues.
  - Don't repeat the literal product name. Don't include the product code.
  - No marketing fluff.
  - ~20 keywords; 18–22 fine.
  - Output STRICT JSON only:
    {{"keywords": ["kw1", "kw2", ...]}}
"""


def resolve_taste(
    *,
    code: str,
    name: str,
    taste_cache: dict[str, str],
    haiku_client=None,
) -> str:
    if not code:
        return ""
    if code in taste_cache and taste_cache[code]:
        return taste_cache[code]
    if haiku_client is None:
        return ""
    kws = _ask_haiku_taste(haiku_client, code, name)
    taste_cache[code] = kws
    _save_json(TASTE_CACHE_PATH, taste_cache)
    return kws


def _ask_haiku_taste(client, code: str, name: str) -> str:
    try:
        msg = client.messages.create(
            model="claude-haiku-4-5", max_tokens=300,
            messages=[{"role": "user", "content": _TASTE_PROMPT.format(code=code, name=name)}],
        )
        text = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text").strip()
        s, e = text.find("{"), text.rfind("}")
        if s == -1 or e == -1:
            return ""
        kws = json.loads(text[s : e + 1]).get("keywords", [])
        cleaned: list[str] = []
        for k in kws:
            if not isinstance(k, str):
                continue
            kk = k.strip().lower()
            if kk and kk not in cleaned:
                cleaned.append(kk)
        return ", ".join(cleaned)
    except Exception as ex:  # noqa: BLE001
        log.warning("enrich.taste Haiku failed for %s: %s", code, ex)
        return ""


# ---------- Category (one of 6 strings) ----------

_CATEGORY_PROMPT = """\
You're classifying a seasoning product into ONE of exactly six categories
used by NP Foods:

  - Snack
  - Noodle & Instant Soup
  - Sauces & Mixes
  - Marinades
  - Oil
  - Beverage

Rules:
  - Pick exactly ONE category from the list above. Output it verbatim.
  - "Snack" = powdered seasonings dusted on chips/popcorn/biscuits/pellets.
  - "Noodle & Instant Soup" = noodle/ramen/instant-soup flavourings.
  - "Sauces & Mixes" = wet sauces, dipping sauces, dry mixes.
  - "Marinades" = grilling/BBQ/chicken/meat marinades.
  - "Oil" = chilli oil, infused oils, cooking oils.
  - "Beverage" = drink mixes/powders.

Product code: {code}
Product name: {name}

Output STRICT JSON only: {{"category": "<one of the six>"}}
"""


def resolve_category(
    *,
    code: str,
    name: str,
    tab_map: dict[str, str],
    category_cache: dict[str, str],
    haiku_client=None,
) -> str:
    if not code:
        return ""
    upper = code.strip().upper()
    if upper in tab_map:
        return tab_map[upper]
    if upper in category_cache and category_cache[upper] in CATEGORIES:
        return category_cache[upper]
    if haiku_client is None:
        return ""
    cat = _ask_haiku_category(haiku_client, code, name)
    category_cache[upper] = cat
    _save_json(CATEGORY_CACHE_PATH, category_cache)
    return cat


def _ask_haiku_category(client, code: str, name: str) -> str:
    try:
        msg = client.messages.create(
            model="claude-haiku-4-5", max_tokens=80,
            messages=[{"role": "user", "content": _CATEGORY_PROMPT.format(code=code, name=name)}],
        )
        text = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text").strip()
        s, e = text.find("{"), text.rfind("}")
        if s == -1 or e == -1:
            return ""
        cat = str(json.loads(text[s : e + 1]).get("category", "")).strip()
        if cat in CATEGORIES:
            return cat
        for valid in CATEGORIES:
            if cat.lower() == valid.lower():
                return valid
        return ""
    except Exception as ex:  # noqa: BLE001
        log.warning("enrich.category Haiku failed for %s: %s", code, ex)
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
