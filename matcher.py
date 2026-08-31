"""Fuzzy suggestion helpers."""
from __future__ import annotations

from datetime import date as _date
from typing import Any

from rapidfuzz import fuzz, process, utils
from rapidfuzz.distance import Levenshtein

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
_LONELY_CURRENCY_RE = re.compile(
    r"\b(?:usd|sgd|dollars?|thb|baht|idr|rupiah|rp|myr|rm|jpy|yen)\b",
    re.IGNORECASE)

# Currency the rep TYPED next to a price ("below 100 thb"). The cap is
# compared against USD-normalised prices, so convert it — before this, the
# 100 was read as $100 (no filtering at all) and the header then claimed
# "under $100 USD" while 'thb' polluted the fuzzy keywords.
_QUERY_CCY_RE = re.compile(
    r"\b(usd|dollars?|sgd|thb|baht|idr|rupiah|rp|myr|rm|jpy|yen)\b",
    re.IGNORECASE)
_QUERY_CCY_RATE = {
    "usd": 1.0, "dollar": 1.0, "dollars": 1.0,
    "sgd": 0.74,
    "thb": 0.029, "baht": 0.029,
    "idr": 0.000063, "rupiah": 0.000063, "rp": 0.000063,
    "myr": 0.21, "rm": 0.21,
    "jpy": 0.0064, "yen": 0.0064,
}


# Catalog prices are MIXED CURRENCY — as of Jul 2026 the live catalogue is
# ~50% THB, 20% IDR, 15% USD, 15% SGD. A "below 4 usd" filter compared against
# the bare number in "THB 147.9" or "IDR 49,892" excludes everything and the
# rep gets zero results, which is exactly what happened. Normalise to USD
# before any comparison or sort.
#
# Rates are approximate but stable enough for filtering: the rep is expressing
# a rough budget, not requesting a quotation. Exact pricing comes from /pp,
# which uses MMS3's live rate table. Kept in sync with the table in
# bot._run_seasoning_search.
_USD_RATE = {
    "USD": 1.0,
    "SGD": 0.74,
    "THB": 0.029,
    "IDR": 0.000063,
    "MYR": 0.21,
    "JPY": 0.0064,
}
# Currency prefix + number, tolerating thousands separators.
_CURRENCY_PRICE_RE = re.compile(
    r"^\s*([A-Z]{3,4}|RM|S\$|\$)\s*([\d,]+(?:\.\d+)?)\s*$", re.IGNORECASE
)
_CURRENCY_ALIAS = {"S$": "SGD", "$": "USD", "RM": "MYR"}


def _parse_price(raw: Any) -> float:
    """Price cell → USD-equivalent float. Unknown → +inf so it sorts last.

    Handles 'USD 4.96', 'THB 162.9', 'IDR 49,892', 'S$6.60', '5.20'.
    A bare number is treated as USD (legacy Singapore convention).

    NB thousands separators matter: the old bare-regex parser read
    'IDR 49,892' as **49**, which made expensive Indonesian items look like
    pocket change and let them slip past a low budget filter.
    """
    if raw is None:
        return float("inf")
    s = str(raw).strip()
    if not s:
        return float("inf")

    # Bare number → USD by convention.
    try:
        v = float(s.replace(",", ""))
        return v if v > 0 else float("inf")
    except ValueError:
        pass

    m = _CURRENCY_PRICE_RE.match(s)
    if m:
        cur = m.group(1).upper()
        cur = _CURRENCY_ALIAS.get(cur, cur)
        rate = _USD_RATE.get(cur)
        if rate is not None:
            try:
                v = float(m.group(2).replace(",", ""))
            except ValueError:
                return float("inf")
            return v * rate if v > 0 else float("inf")
        # Known shape, unknown currency: don't invent a conversion.
        return float("inf")

    # Last resort: first number in the string, commas stripped.
    m2 = _PRICE_NUM.search(s.replace(",", ""))
    if not m2:
        return float("inf")
    try:
        v = float(m2.group())
        return v if v > 0 else float("inf")
    except ValueError:
        return float("inf")


# --- factory / region code-prefix filter -----------------------------------
#
# Product codes are namespaced by factory: S- Singapore, J- Indonesia,
# B- Thailand (T- legacy). Reps think in those terms ("show me the J codes"),
# so a search should be filterable by prefix.
#
# The trigger deliberately REQUIRES the word "code(s)". A bare country word is
# ambiguous in this catalogue and must not hard-filter: "singapore laksa" and
# "thai tom yum" are FLAVOURS sold from every factory, so treating "singapore"
# as a filter would hide the very products the rep asked for. "singapore
# codes", by contrast, can only mean the factory.
_PREFIX_WORDS = {
    "s": "S", "sg": "S", "singapore": "S",
    "j": "J", "id": "J", "indo": "J", "indonesia": "J", "jakarta": "J",
    "b": "B", "th": "B", "thailand": "B", "bangkok": "B",
    "t": "T",
}
_CODE_PREFIX_RE = re.compile(
    r"\b(" + "|".join(sorted(_PREFIX_WORDS, key=len, reverse=True)) + r")[\s\-]*codes?\b",
    re.IGNORECASE,
)


def parse_code_prefix(query: str) -> tuple[str, str | None]:
    """Pull a factory code-prefix filter out of a query.

    "sesame j code"      → ("sesame", "J")
    "S codes cheese"     → ("cheese", "S")
    "b-codes below 4usd" → ("below 4usd", "B")
    "singapore laksa"    → ("singapore laksa", None)   # flavour, not a filter
    """
    q = (query or "").strip()
    m = _CODE_PREFIX_RE.search(q)
    if not m:
        return q, None
    prefix = _PREFIX_WORDS.get(m.group(1).lower())
    cleaned = re.sub(r"\s+", " ", _CODE_PREFIX_RE.sub(" ", q)).strip()
    return cleaned, prefix


# Destination countries seen in the FSL 'Country' column. The trigger REQUIRES
# a preposition ("to vietnam", "sent to china") — a bare country word stays a
# flavour ("singapore laksa", "thai tom yum"), same principle as the factory
# prefix requiring the word "code".
_DEST_COUNTRIES = {
    "vietnam": "Vietnam", "singapore": "Singapore", "indonesia": "Indonesia",
    "thailand": "Thailand", "malaysia": "Malaysia", "philippines": "Philippines",
    "india": "India", "japan": "Japan", "china": "China", "korea": "Korea",
    "south korea": "Korea", "bangladesh": "Bangladesh", "myanmar": "Myanmar",
    "cambodia": "Cambodia", "taiwan": "Taiwan", "hong kong": "Hong Kong",
    "usa": "USA", "america": "USA", "australia": "Australia", "uae": "UAE",
    "dubai": "UAE", "laos": "Laos", "sri lanka": "Sri Lanka",
    "pakistan": "Pakistan", "nepal": "Nepal", "brunei": "Brunei",
    "new zealand": "New Zealand",
}
_DEST_RE = re.compile(
    r"\b(?:sent\s+to|to|for)\s+("
    + "|".join(sorted(_DEST_COUNTRIES, key=len, reverse=True))
    + r")\b",
    re.IGNORECASE,
)


def parse_country_filter(query: str) -> tuple[str, str | None]:
    """Pull a destination-country filter out of a query.

    "cheese to vietnam"   → ("cheese", "Vietnam")
    "rich sent to china"  → ("rich", "China")
    "to indonesia"        → ("", "Indonesia")
    "singapore laksa"     → ("singapore laksa", None)   # flavour, not a filter
    """
    q = (query or "").strip()
    m = _DEST_RE.search(q)
    if not m:
        return q, None
    country = _DEST_COUNTRIES.get(m.group(1).lower())
    cleaned = re.sub(r"\s+", " ", _DEST_RE.sub(" ", q)).strip()
    return cleaned, country


def code_has_prefix(code: Any, prefix: str) -> bool:
    """True when `code` belongs to the `prefix` factory (S-/J-/B-/T-)."""
    c = str(code or "").strip().upper()
    return c.startswith(f"{prefix.upper()}-")


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
    if max_price is not None:
        ccy = _QUERY_CCY_RE.search(q)
        if ccy:
            max_price *= _QUERY_CCY_RATE.get(ccy.group(1).lower(), 1.0)
    cleaned = _PRICE_STRIP_RE.sub(" ", q)
    cleaned = _LONELY_CURRENCY_RE.sub(" ", cleaned)
    cleaned = _strip_generic(cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned, max_price


def _recency_ord(v: Any) -> int:
    """Sortable ordinal for a `last_sent` value; 0 when unknown.

    Tolerates date, datetime and ISO string, because the catalog can be built
    from either the FSL fallback (parsed dates) or the legacy category tabs
    (no date column at all) — an undated product must sort last, not crash.
    """
    if v is None:
        return 0
    if isinstance(v, _date):
        return v.toordinal()
    try:
        return _date.fromisoformat(str(v)[:10]).toordinal()
    except (ValueError, TypeError):
        return 0


def top_seasonings(
    query: str,
    seasonings: list[dict[str, Any]],
    limit: int = 5,
    pool: int = 30,
    past_submissions: list[dict[str, str]] | None = None,
    strict_price: bool = True,
    prefix: str | None = None,
) -> list[dict[str, Any]]:
    """Fuzzy-match the query, then return the `limit` cheapest from the top `pool`.

    Understands a max-price filter in the query itself ("below 4.5 usd",
    "under $3", "<=2.50") and a factory code-prefix filter ("j code",
    "S codes"), either passed via ``prefix`` or written in the query.
    Market / style / flavor hints (e.g. "for bangladesh", "chinese style")
    are handled implicitly: fuzzy WRatio scores names that contain those
    keywords higher.

    If ``past_submissions`` is supplied, items whose code shows up against a
    similar past query get a +score boost — surfaces "korea spicy noodle"
    style requests by remembering what was picked for past similar queries.
    """
    if not query.strip() or not seasonings:
        return []

    # Factory filter: explicit argument wins, else read it out of the query.
    query, q_prefix = parse_code_prefix(query)
    prefix = prefix or q_prefix

    cleaned_query, max_price = parse_seasoning_query(query)

    # Apply the price cap first so we never suggest things out of budget —
    # unless `strict_price=False`, in which case the cap is dropped (used by
    # the caller as a fallback when the strict pool comes back empty, so
    # the user gets the closest above-budget items rather than nothing).
    candidates = seasonings
    if prefix:
        candidates = [s for s in candidates if code_has_prefix(s.get("code"), prefix)]
        if not candidates:
            return []
    if max_price is not None and strict_price:
        candidates = [
            s for s in candidates
            if _parse_price(s.get("price")) <= max_price
        ]
        if not candidates:
            return []

    # If the user only typed a price ("below 4"), return the cheapest matches
    # outright — nothing to fuzzy-match against.
    if not cleaned_query:
        if max_price is None and not prefix:
            # Not a price/prefix query — real words that ALL stripped as
            # generic ('seasoning powder'). Returning the cheapest items in
            # the whole catalogue here presented unrelated SKUs as
            # "matches"; an honest no-result lets the router try its other
            # probes and say so.
            return []
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
    # Newest-first BEFORE scoring. This is load-bearing, not cosmetic: the
    # catalog holds 35k products and thousands of them tie at the same fuzzy
    # score (every "* CHEESE SEASONING" scores 95 against "cheese"). rapidfuzz
    # keeps only `pool` results and breaks score ties by iteration order, so
    # an unsorted list handed back whatever 30 items it happened to reach
    # first — which is why "cheese b code" surfaced products last sampled in
    # 2010. Sorting here makes the pool itself the newest N, and the final
    # sort then orders within it.
    candidates = sorted(
        candidates, key=lambda s: -_recency_ord(s.get("last_sent"))
    )

    choices = {
        i: _strip_generic(f"{s['name']} {s.get('category', '')}").strip()
        for i, s in enumerate(candidates)
    }

    # Build a catalog vocabulary so we can tell "real" query tokens (words
    # that appear somewhere in the catalog) from "filter" tokens (markets /
    # countries / hints like "bangladesh", "for", "vietnam"). Filter tokens
    # don't count toward coverage, so "cheese for bangladesh" doesn't get
    # penalised when no item literally contains "bangladesh".
    #
    # CRITICAL: build vocab from the FULL seasoning list, not from the
    # already-price-filtered `candidates`. Otherwise a query like "masala
    # noodle below 4usd" loses "masala" from vocab when no masala item fits
    # the budget — and the scorer stops penalising non-masala results.
    _catalog_vocab: set[str] = set()
    for s in seasonings:
        name_cat = _strip_generic(f"{s.get('name', '')} {s.get('category', '')}")
        for tok in re.findall(r"[a-z0-9]+", name_cat.lower()):
            if len(tok) >= 4:
                _catalog_vocab.add(tok)

    # Combined scorer: AVERAGE of WRatio and token_set_ratio + token-coverage
    # penalty for queries where significant catalog tokens are missing.
    #
    # Why average instead of max? WRatio plateaus at ~86 for short multi-word
    # queries, so MAX(W, ts) made every plausible candidate look equally
    # good. token_set_ratio differentiates them — averaging lets it
    # contribute. Combined with token-coverage, MASALA NOODLE no longer
    # matches CHICKEN NOODLE just because the noodle-tab fold picks it up.
    def _combined(q: str, c: str, **kwargs) -> float:
        a = fuzz.WRatio(q, c, **kwargs)
        b = fuzz.token_set_ratio(q, c, **kwargs)
        avg = (a + b) / 2

        # Full-coverage promotion (V1.17.32). token_set_ratio ~100 means every
        # word the rep typed appears in the product name — the product IS what
        # they asked for; it just carries an extra descriptor. WRatio still
        # docks it for those extra words (100 -> 90), dragging the average into
        # a lower relevance band, and since the final sort is band-then-recency
        # the item loses to ANY plain-named product no matter how old.
        #
        # Real case: "hokkaido milk" ranked a dozen 2024-2025 "HOKKAIDO MILK
        # SEASONING" rows above "HOKKAIDO MILK SEASONING (IN DOUGH)" — which
        # was the sample actually shipped last week. Treat full-coverage hits
        # as the same relevance tier and let recency decide between them.
        if b >= 98:
            avg = max(avg, b)

        # Coverage penalty for multi-token queries.
        # Only count tokens that BOTH look meaningful (≥4 chars) AND appear
        # somewhere in the catalog vocab — that excludes filler words and
        # market hints that wouldn't be in any product name.
        q_real = [
            w for w in re.findall(r"[a-z0-9]+", q.lower())
            if len(w) >= 4 and w in _catalog_vocab
        ]
        if len(q_real) >= 2:
            c_lower = c.lower()
            missing = sum(1 for w in q_real if w not in c_lower)
            if missing > 0:
                # Each missing real token costs 30% off, floored at 40%.
                # 1 missing → 0.7×, 2 missing → 0.49×, ≥3 → 0.4×
                avg *= max(0.4, 0.7 ** missing)
        return avg

    results = process.extract(
        cleaned_query,
        choices,
        scorer=_combined,
        processor=utils.default_process,
        limit=pool,
    )
    pooled: dict[int, dict[str, Any]] = {}
    for _name, score, idx in results:
        # Threshold 60 (not 50): with the token-coverage penalty,
        # items missing a key query token (e.g. CHICKEN NOODLE for "masala
        # noodle") land around 52. Bumping to 60 drops them so the soft-
        # fallback path triggers and the user gets a clear "no masala items
        # under $4" message + above-budget masala suggestions.
        if score < 60:
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

    # V1.17.35 — literal-name priority. The generic-word strip exists so
    # "seasoning"/"powder" don't inflate fuzzy scores, but it also DELETED
    # the distinguishing word when a rep typed a product's literal name:
    # "chicken powder" became "chicken", every chicken product token-set
    # matched at 100, and the actual CHICKEN POWDER items (older dates)
    # never even made the fuzzy pool — the rep got the newest chicken-
    # anything instead. Guarantee: a query that IS a product's raw name
    # (punctuation/space-insensitive) always surfaces and always wins.
    # Containment ("chicken powder" ⊂ "SPECIAL CHICKEN POWDER MIX") ranks
    # just below exact. Both sit ABOVE the 100-score fuzzy band; recency
    # still orders within each tier. Injection scans `candidates` (already
    # prefix/price-filtered, newest-first), so caps and factory filters
    # are respected and the newest matches enter the pool first.
    _raw_kw = _LONELY_CURRENCY_RE.sub(" ", _PRICE_STRIP_RE.sub(" ", query))
    _kw_sq = re.sub(r"[^a-z0-9]", "", _raw_kw.lower())
    if len(_kw_sq) >= 6:
        _exact_n = _contain_n = 0
        for _i, _s in enumerate(candidates):
            if _exact_n >= pool and _contain_n >= pool:
                break
            _name_sq = re.sub(r"[^a-z0-9]", "", str(_s.get("name") or "").lower())
            if not _name_sq or _kw_sq not in _name_sq:
                continue
            _is_exact = _name_sq == _kw_sq
            if _is_exact and _exact_n >= pool:
                continue
            if not _is_exact and _contain_n >= pool:
                continue
            _e = pooled.get(_i)
            if _e is None:
                _e = {
                    **_s,
                    "score": 0.0,
                    "_price_num": _parse_price(_s.get("price")),
                    "_past_hits": 0,
                }
                pooled[_i] = _e
            _e["score"] = max(_e["score"], 110.0 if _is_exact else 105.0)
            if _is_exact:
                _exact_n += 1
            else:
                _contain_n += 1

    # Dedupe by code: a product code can appear in more than one tab when
    # the workbook is mid-cleanup, or when a code lives in a category tab AND
    # in the (now-retired) "Sample Master List 2024-Present" tab from older
    # data. Without this pass the top-5 can be half-duplicates. Keep the
    # highest-scoring entry per code.
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
    # Best-first by relevance, then MOST RECENTLY SAMPLED, then cheapest.
    #
    # Recency is a tiebreaker, not the primary key: a rep asking for "spicy"
    # must still get spicy things first. But fuzzy scores cluster hard — a
    # dozen SPICY * SEASONING entries routinely land within a point or two of
    # each other — and among equally-relevant products the useful one is the
    # one actually being sampled today, not a 2019 code that may be long dead.
    # So scores are bucketed into 5-point bands and recency orders within the
    # band. Undated rows sort last (SENTINEL), never ahead of a dated one.
    ranked.sort(
        key=lambda s: (
            # floor, NOT round: _combined averages two ints so half of all
            # scores end in .5, and banker's rounding made band edges
            # asymmetric (92.5 banded WITH 87.5 but apart from 93.0 —
            # letting a 5-points-worse match win on recency).
            -int(s["score"] // 5),
            -_recency_ord(s.get("last_sent")),
            s["_price_num"],
        )
    )
    return ranked[:limit]


def find_by_code(code: str, seasonings: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Backwards-compatible single-result code lookup. Returns the first match
    from `find_codes_matching` or None.
    """
    matches = find_codes_matching(code, seasonings)
    return matches[0] if matches else None


def find_codes_matching(
    code: str, seasonings: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Look up catalog entries whose code matches the user's input.

    Resolution order (each later step skips codes already collected):
      1. Exact case-insensitive match
      2. Prefix expansion: catalog codes starting with `<input>-` so a user
         typing ``S-668U1`` finds ``S-668U1-02``, ``S-668U1-03``, etc.
      3. Suffix-trim of the user's input: ``S-6AUH2-12-Y1`` → ``S-6AUH2-12``
         → ``S-6AUH2`` (handy when sales paste an over-specific code from a
         downstream system).

    Returns a list (possibly empty) deduped by code, in the order above so
    callers can show the most-confident match first.
    """
    q = (code or "").strip().upper()
    if not q:
        return []

    matches: list[dict[str, Any]] = []
    seen_codes: set[str] = set()

    def _add(s: dict[str, Any]) -> None:
        c = str(s.get("code", "")).strip().upper()
        if c and c not in seen_codes:
            seen_codes.add(c)
            matches.append(s)

    # 1) Exact
    for s in seasonings:
        c = str(s.get("code", "")).strip().upper()
        if c == q:
            _add(s)

    # 2) Prefix: user typed a base, catalog has variants like base-XX.
    prefix = q + "-"
    for s in seasonings:
        c = str(s.get("code", "")).strip().upper()
        if c.startswith(prefix):
            _add(s)

    # 3) Suffix-trim of user's input (over-specific code → progressively shorter).
    trimmed = q
    while "-" in trimmed:
        trimmed = trimmed.rsplit("-", 1)[0]
        if not trimmed:
            break
        for s in seasonings:
            c = str(s.get("code", "")).strip().upper()
            if c == trimmed:
                _add(s)

    return matches


def close_code_matches(
    code: str,
    catalog_codes: set[str] | list[str],
    limit: int = 3,
    max_distance: int = 2,
) -> list[tuple[str, int]]:
    """Rank catalog codes by edit distance to a mistyped/misread code.

    Used for "did you mean" suggestions when a typed or OCR'd code isn't
    found anywhere. Guardrails:
      • candidate must share the same prefix letter (S-/J-/B-/T-) — the
        prefix routes to a factory, so cross-prefix guesses mislead
      • length may differ by at most 1 (misreads add/drop one char at most)
      • Levenshtein distance ≤ ``max_distance``

    Returns [(catalog_code, distance), ...] closest-first, then
    alphabetical for deterministic ordering on ties.
    """
    q = (code or "").strip().upper()
    if len(q) < 3 or "-" not in q:
        return []
    prefix = q.split("-", 1)[0]
    out: list[tuple[str, int]] = []
    for c in catalog_codes:
        cu = str(c).strip().upper()
        if not cu or cu == q:
            continue
        if cu.split("-", 1)[0] != prefix:
            continue
        if abs(len(cu) - len(q)) > 1:
            continue
        d = Levenshtein.distance(q, cu, score_cutoff=max_distance)
        if d <= max_distance:
            out.append((cu, d))
    out.sort(key=lambda t: (t[1], t[0]))
    return out[:limit]


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
