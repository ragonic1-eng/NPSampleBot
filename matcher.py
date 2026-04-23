"""Fuzzy suggestion helpers."""
from __future__ import annotations

from typing import Any

from rapidfuzz import fuzz, process, utils

import re

_PRICE_NUM = re.compile(r"[-+]?\d*\.?\d+")

# "below 4.5", "under $3", "less than 5 usd", "cheaper than 2.50", "<=4.5", "<4".
_PRICE_MAX_RE = re.compile(
    r"(?:below|under|less\s+than|cheaper\s+than|max(?:imum)?|<=?)\s*\$?\s*(\d+(?:\.\d+)?)",
    re.IGNORECASE,
)
# Same pattern plus optional trailing "usd" / "dollars" — used to strip the
# filter phrase out of the query before fuzzy-matching.
_PRICE_STRIP_RE = re.compile(
    r"(?:below|under|less\s+than|cheaper\s+than|max(?:imum)?|<=?)\s*\$?\s*\d+(?:\.\d+)?\s*(?:usd|dollars?|sgd)?",
    re.IGNORECASE,
)
_LONELY_CURRENCY_RE = re.compile(r"\b(?:usd|sgd|dollars?)\b", re.IGNORECASE)


def _parse_price(raw: Any) -> float:
    """Convert a price cell like '$4.96' or '5.20' to a float. Unknown → +inf so it sorts last."""
    if raw is None:
        return float("inf")
    m = _PRICE_NUM.search(str(raw))
    if not m:
        return float("inf")
    try:
        return float(m.group())
    except ValueError:
        return float("inf")


def parse_seasoning_query(query: str) -> tuple[str, float | None]:
    """Pull a max-price constraint out of a natural-language query.

    "cheese seasoning below 4.5 usd" → ("cheese seasoning", 4.5)
    "bbq under $3"                   → ("bbq", 3.0)
    "cheese for bangladesh"          → ("cheese for bangladesh", None)
    """
    q = query.strip()
    m = _PRICE_MAX_RE.search(q)
    max_price: float | None = None
    if m:
        try:
            max_price = float(m.group(1))
        except ValueError:
            max_price = None
    cleaned = _PRICE_STRIP_RE.sub(" ", q)
    cleaned = _LONELY_CURRENCY_RE.sub(" ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned, max_price


def top_seasonings(
    query: str,
    seasonings: list[dict[str, Any]],
    limit: int = 5,
    pool: int = 30,
) -> list[dict[str, Any]]:
    """Fuzzy-match the query, then return the `limit` cheapest from the top `pool`.

    Understands a max-price filter in the query itself ("below 4.5 usd",
    "under $3", "<=2.50"). Market / style / flavor hints (e.g. "for bangladesh",
    "chinese style") are handled implicitly: fuzzy WRatio scores names that
    contain those keywords higher.
    """
    if not query.strip() or not seasonings:
        return []

    cleaned_query, max_price = parse_seasoning_query(query)

    # Apply the price cap first so we never suggest things out of budget.
    candidates = seasonings
    if max_price is not None:
        candidates = [
            s for s in candidates
            if _parse_price(s.get("price")) <= max_price
        ]
        if not candidates:
            return []

    # If the user only typed a price ("below 4"), return the cheapest matches
    # outright — nothing to fuzzy-match against.
    if not cleaned_query:
        ranked = sorted(
            candidates,
            key=lambda s: _parse_price(s.get("price")),
        )
        out = []
        for s in ranked[:limit]:
            out.append({**s, "score": 0, "_price_num": _parse_price(s.get("price"))})
        return out

    choices = {i: s["name"] for i, s in enumerate(candidates)}
    results = process.extract(
        cleaned_query,
        choices,
        scorer=fuzz.WRatio,
        processor=utils.default_process,
        limit=pool,
    )
    pooled: list[dict[str, Any]] = []
    for _name, score, idx in results:
        if score < 60:
            continue
        s = candidates[idx]
        pooled.append(
            {**s, "score": score, "_price_num": _parse_price(s.get("price"))}
        )
    pooled.sort(key=lambda s: (s["_price_num"], -s["score"]))
    return pooled[:limit]


def top_companies(query: str, customers: list[dict[str, str]], limit: int = 3) -> list[dict[str, str]]:
    if not query.strip() or not customers:
        return []
    choices = {i: c.get("Company Name", "") for i, c in enumerate(customers)}
    results = process.extract(
        query, choices, scorer=fuzz.WRatio, processor=utils.default_process, limit=limit
    )
    out = []
    for _name, score, idx in results:
        if score < 60:
            continue
        out.append({**customers[idx], "score": score})
    return out


def top_customer_master(
    query: str, master: list[dict[str, str]], limit: int = 5
) -> list[dict[str, str]]:
    """Fuzzy-match against the customer master (keys: 'name', 'code')."""
    if not query.strip() or not master:
        return []
    choices = {i: c["name"] for i, c in enumerate(master)}
    results = process.extract(
        query, choices, scorer=fuzz.WRatio, processor=utils.default_process, limit=limit
    )
    out = []
    for _name, score, idx in results:
        if score < 55:
            continue
        out.append({**master[idx], "score": score})
    return out
