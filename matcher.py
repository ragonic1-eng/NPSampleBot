"""Fuzzy suggestion helpers."""
from __future__ import annotations

from typing import Any

from rapidfuzz import fuzz, process, utils

import re

# Generic words that appear in nearly every seasoning name AND most user
# queries — they massively inflate fuzzy scores without adding signal. We
# strip them from BOTH the query and the catalog choice strings before
# scoring so the remaining tokens (cuisine, flavour, application) actually
# drive ranking. Without this, "spicy korean noodle seasoning" is
# essentially scored as just "seasoning" against the catalog and pulls
# unrelated cheese / onion / pickled items to the top.
_GENERIC_TERMS = {
    "seasoning", "seasonings",
    "powder", "powders",
    "flavor", "flavors", "flavour", "flavours",
    "flavored", "flavoured",
    "blend", "blends",
    "mix", "mixes",
}
_GENERIC_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(w) for w in _GENERIC_TERMS) + r")\b",
    re.IGNORECASE,
)


def _strip_generic(s: str) -> str:
    """Remove generic seasoning-domain words so they don't dominate scoring."""
    return re.sub(r"\s+", " ", _GENERIC_RE.sub(" ", s)).strip()


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
    cleaned = _strip_generic(cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned, max_price


def top_seasonings(
    query: str,
    seasonings: list[dict[str, Any]],
    limit: int = 5,
    pool: int = 30,
    past_submissions: list[dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    """Fuzzy-match the query, then return the `limit` cheapest from the top `pool`.

    Understands a max-price filter in the query itself ("below 4.5 usd",
    "under $3", "<=2.50"). Market / style / flavor hints (e.g. "for bangladesh",
    "chinese style") are handled implicitly: fuzzy WRatio scores names that
    contain those keywords higher.

    If ``past_submissions`` is supplied, items whose code shows up against a
    similar past query get a +score boost — surfaces "korea spicy noodle"
    style requests by remembering what was picked for past similar queries.
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

    # Score against seasoning name + category — fold category into the choice
    # so "korean noodle" rewards items in the Noodle category tab. Also strip
    # generic terms from the choice string so "seasoning" / "powder" /
    # "flavour" don't dominate the score.
    choices = {
        i: _strip_generic(f"{s['name']} {s.get('category', '')}").strip()
        for i, s in enumerate(candidates)
    }

    # Combined scorer: max(WRatio, token_set_ratio).
    #  - WRatio handles typos and continuous-substring matches well
    #  - token_set_ratio is order-insensitive ("spicy korean noodle" should
    #    score the same as "korean spicy noodle")
    # WRatio alone drops the noodle item from 90 → 86 just because the user
    # reordered tokens, which is a real-world bug we hit.
    def _combined(q: str, c: str, **kwargs) -> float:
        a = fuzz.WRatio(q, c, **kwargs)
        b = fuzz.token_set_ratio(q, c, **kwargs)
        return max(a, b)

    results = process.extract(
        cleaned_query,
        choices,
        scorer=_combined,
        processor=utils.default_process,
        limit=pool,
    )
    pooled: dict[int, dict[str, Any]] = {}
    for _name, score, idx in results:
        if score < 50:  # combined scorer; AI rerank does the final ordering
            continue
        s = candidates[idx]
        pooled[idx] = {
            **s, "score": float(score), "_price_num": _parse_price(s.get("price")),
            "_past_hits": 0,
        }

    # Past-submissions boost: fuzzy-match the user's query against historical
    # request text. For each strong hit, find that submission's matched_code
    # in the catalog and bump its score / add it to the pool.
    if past_submissions:
        past_choices = {i: p.get("query_text", "") for i, p in enumerate(past_submissions)}
        past_results = process.extract(
            cleaned_query,
            past_choices,
            scorer=fuzz.WRatio,
            processor=utils.default_process,
            limit=20,
        )
        # code -> [list of past-match scores] so we can boost proportionally.
        past_code_scores: dict[str, list[float]] = {}
        for _txt, pscore, pidx in past_results:
            if pscore < 65:
                continue
            code = past_submissions[pidx].get("matched_code", "").strip().upper()
            if not code:
                continue
            past_code_scores.setdefault(code, []).append(float(pscore))

        if past_code_scores:
            # Index catalog by code for quick lookup.
            by_code = {
                str(s.get("code", "")).strip().upper(): (i, s)
                for i, s in enumerate(candidates)
                if s.get("code")
            }
            for code, pscores in past_code_scores.items():
                if code not in by_code:
                    continue
                idx, s = by_code[code]
                avg_pscore = sum(pscores) / len(pscores)
                # Boost: scale 0..15 based on the avg past-score (65→0, 100→15)
                boost = max(0.0, (avg_pscore - 65.0) * (15.0 / 35.0))
                if idx in pooled:
                    pooled[idx]["score"] = pooled[idx]["score"] + boost
                    pooled[idx]["_past_hits"] = len(pscores)
                else:
                    # Surface from the past even if the catalog fuzzy missed it.
                    pooled[idx] = {
                        **s,
                        "score": 60.0 + boost,
                        "_price_num": _parse_price(s.get("price")),
                        "_past_hits": len(pscores),
                    }

    # Dedupe by code: the same product code lives in both its proper category
    # tab and the historical "Sample Master List 2024-Present" tab — without
    # this dedupe the top-5 is half-duplicates. Keep the highest-scoring
    # entry per code.
    by_code: dict[str, dict[str, Any]] = {}
    no_code_entries: list[dict[str, Any]] = []
    for s in pooled.values():
        code = str(s.get("code", "")).strip().upper()
        if not code:
            no_code_entries.append(s)
            continue
        existing = by_code.get(code)
        if existing is None or s["score"] > existing["score"]:
            by_code[code] = s

    ranked = list(by_code.values()) + no_code_entries
    # Best-first by score, then cheapest within ties.
    ranked.sort(key=lambda s: (-s["score"], s["_price_num"]))
    return ranked[:limit]


def find_by_code(code: str, seasonings: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Look up a seasoning by its exact product code (case-insensitive).

    Tries suffix-trim fallback for coded variants — e.g. if the user pastes
    ``S-6AUH2-12-Y1`` but the master only stores ``S-6AUH2-12`` or
    ``S-6AUH2``, we still resolve it. Same rule the V0.4.0 bulk parser uses.

    Returns ``None`` when nothing matches — callers should then fall back
    to fuzzy name matching.
    """
    q = (code or "").strip().upper()
    if not q:
        return None
    # Exact match first.
    for s in seasonings:
        c = str(s.get("code", "")).strip().upper()
        if c and c == q:
            return s
    # Suffix-trim fallback: S-6AUH2-12-Y1 → S-6AUH2-12 → S-6AUH2.
    trimmed = q
    while "-" in trimmed:
        trimmed = trimmed.rsplit("-", 1)[0]
        if not trimmed:
            break
        for s in seasonings:
            c = str(s.get("code", "")).strip().upper()
            if c and c == trimmed:
                return s
    return None


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
