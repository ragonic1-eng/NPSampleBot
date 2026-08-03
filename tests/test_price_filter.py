"""Regression tests for budget search — "sesame below 4usd" / "sesame <4usd".

Pins two bugs that made price-filtered search return NOTHING once the
catalogue became mixed-currency (Jul 2026: ~50% THB, 20% IDR, 15% USD,
15% SGD):

  1. prices were compared as bare numbers, so "THB 147.9" was read as 147.9
     and excluded from every "under $X USD" filter;
  2. thousands separators truncated the number — "IDR 49,892" parsed as 49,
     making expensive items look like pocket change.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "")
os.environ.setdefault("GOOGLE_SHEETS_ID", "")

import matcher  # noqa: E402

CATALOG = [
    {"name": "BLACK SESAME SEASONING", "code": "J-1", "price": "IDR 45,021", "category": "Snack"},
    {"name": "SESAME SEASONING",       "code": "B-1", "price": "THB 114.6",  "category": "Snack"},
    {"name": "SESAME BUTTER SEASONING","code": "B-2", "price": "THB 106.9",  "category": "Snack"},
    {"name": "SESAME OIL SEASONING",   "code": "S-1", "price": "SGD 9.90",   "category": "Oil"},
    {"name": "PREMIUM SESAME BLEND",   "code": "S-2", "price": "USD 12.50",  "category": "Snack"},
    {"name": "CHEESE SEASONING",       "code": "S-3", "price": "USD 3.10",   "category": "Snack"},
]


# ---------- query parsing: every syntax the rep might type ----------

def test_all_budget_syntaxes_parse():
    for q in (
        "sesame below 4usd", "sesame <4usd", "sesame < 4 usd",
        "sesame under $4", "sesame below 4", "sesame less than 4 usd",
        "sesame cheaper than 4", "sesame <=4usd", "sesame max 4",
    ):
        cleaned, cap = matcher.parse_seasoning_query(q)
        assert cap == 4.0, f"{q!r} -> cap {cap}"
        assert "sesame" in cleaned, f"{q!r} lost the keyword: {cleaned!r}"


def test_query_without_price_has_no_cap():
    cleaned, cap = matcher.parse_seasoning_query("sesame seasoning")
    assert cap is None
    assert "sesame" in cleaned


# ---------- currency-aware price parsing ----------

def test_prices_normalise_to_usd():
    assert matcher._parse_price("USD 4.96") == 4.96
    assert abs(matcher._parse_price("THB 147.9") - 4.29) < 0.05
    assert abs(matcher._parse_price("SGD 6.60") - 4.88) < 0.05
    assert abs(matcher._parse_price("S$4.00") - 2.96) < 0.05
    assert matcher._parse_price("5.20") == 5.20          # bare == USD


def test_thousands_separator_not_truncated():
    """'IDR 49,892' must not parse as 49 — that's ~$3.14, not 49 cents."""
    v = matcher._parse_price("IDR 49,892")
    assert 3.0 < v < 3.3, v
    assert v != 49


def test_unparseable_price_sorts_last():
    assert matcher._parse_price("") == float("inf")
    assert matcher._parse_price(None) == float("inf")
    assert matcher._parse_price("call for price") == float("inf")


# ---------- end-to-end filtering ----------

def test_below_4usd_returns_only_under_budget():
    res = matcher.top_seasonings("sesame below 4usd", CATALOG, limit=10)
    assert res, "budget search returned nothing"
    assert all(r["_price_num"] <= 4.0 for r in res)
    codes = {r["code"] for r in res}
    assert {"J-1", "B-1", "B-2"} <= codes      # IDR + THB items are in budget
    assert "S-2" not in codes                  # USD 12.50 is not
    assert "S-1" not in codes                  # SGD 9.90 (~$7.33) is not


def test_shorthand_lt_syntax_matches_verbose():
    a = {r["code"] for r in matcher.top_seasonings("sesame <4usd", CATALOG, limit=10)}
    b = {r["code"] for r in matcher.top_seasonings("sesame below 4usd", CATALOG, limit=10)}
    assert a == b and a


def test_results_sorted_cheapest_first_across_currencies():
    res = matcher.top_seasonings("sesame below 4usd", CATALOG, limit=10)
    prices = [r["_price_num"] for r in res]
    assert prices == sorted(prices), prices


def test_strict_filter_can_return_empty_then_soft_fallback_offers_options():
    """Below an impossible budget: strict returns nothing, soft still helps."""
    strict = matcher.top_seasonings("sesame below 0.5usd", CATALOG, limit=5)
    assert strict == []
    soft = matcher.top_seasonings(
        "sesame below 0.5usd", CATALOG, limit=5, strict_price=False
    )
    assert soft, "soft fallback must still surface the closest options"
    assert all("SESAME" in r["name"].upper() for r in soft)


def test_budget_does_not_leak_into_keyword_matching():
    """'below 4usd' must not be matched as product text."""
    res = matcher.top_seasonings("sesame below 4usd", CATALOG, limit=10)
    assert all("SESAME" in r["name"].upper() for r in res)


# ---------- factory code-prefix filter (S- / J- / B-) ----------

PREFIX_CATALOG = [
    {"name": "SESAME SEASONING",       "code": "S-100", "price": "USD 3.00", "category": "Snack"},
    {"name": "SESAME OIL SEASONING",   "code": "S-101", "price": "USD 3.50", "category": "Oil"},
    {"name": "BLACK SESAME SEASONING", "code": "J-200", "price": "IDR 45,021", "category": "Snack"},
    {"name": "SESAME PEANUT SEASONING","code": "J-201", "price": "IDR 49,810", "category": "Snack"},
    {"name": "SESAME BUTTER SEASONING","code": "B-300", "price": "THB 106.9", "category": "Snack"},
    {"name": "SINGAPORE LAKSA SEASONING","code": "B-301", "price": "THB 110.0", "category": "Snack"},
]


def test_prefix_parsed_from_query():
    assert matcher.parse_code_prefix("sesame j code") == ("sesame", "J")
    assert matcher.parse_code_prefix("S codes cheese") == ("cheese", "S")
    assert matcher.parse_code_prefix("sesame b-codes") == ("sesame", "B")
    assert matcher.parse_code_prefix("sesame indonesia codes") == ("sesame", "J")
    assert matcher.parse_code_prefix("sesame thailand code") == ("sesame", "B")


def test_bare_country_word_is_NOT_a_filter():
    """'singapore laksa' / 'thai tom yum' are FLAVOURS sold from every
    factory — treating them as filters would hide what the rep asked for."""
    for q in ("singapore laksa", "thai tom yum", "indonesia style sesame"):
        assert matcher.parse_code_prefix(q) == (q, None)


def test_search_filtered_to_each_factory():
    for pfx, expect in (("j", {"J-200", "J-201"}),
                        ("s", {"S-100", "S-101"}),
                        ("b", {"B-300"})):
        res = matcher.top_seasonings(f"sesame {pfx} code", PREFIX_CATALOG, limit=10)
        got = {r["code"] for r in res}
        assert got == expect, f"{pfx}: {got}"
        assert all(matcher.code_has_prefix(c, pfx) for c in got)


def test_prefix_combines_with_budget():
    """'sesame j code below 4usd' -> only J- codes, all under $4."""
    res = matcher.top_seasonings("sesame j code below 4usd", PREFIX_CATALOG, limit=10)
    assert res
    assert all(r["code"].startswith("J-") for r in res)
    assert all(r["_price_num"] <= 4.0 for r in res)


def test_prefix_via_argument_matches_inline_syntax():
    a = {r["code"] for r in matcher.top_seasonings("sesame", PREFIX_CATALOG, limit=10, prefix="J")}
    b = {r["code"] for r in matcher.top_seasonings("sesame j code", PREFIX_CATALOG, limit=10)}
    assert a == b == {"J-200", "J-201"}


def test_no_prefix_returns_all_factories():
    res = matcher.top_seasonings("sesame", PREFIX_CATALOG, limit=10)
    pfxs = {r["code"].split("-")[0] for r in res}
    assert {"S", "J", "B"} <= pfxs


# ---------- recency: newest sample wins (V1.17.18) ----------

from datetime import date as _d  # noqa: E402

RECENCY_CATALOG = [
    {"name": "CHEESE SEASONING", "code": "B-OLD", "price": "THB 100.0",
     "category": "Snack", "last_sent": _d(2010, 6, 16)},
    {"name": "CHEESE SEASONING", "code": "B-NEW", "price": "THB 200.0",
     "category": "Snack", "last_sent": _d(2026, 7, 16)},
    {"name": "CHEESE SEASONING", "code": "B-MID", "price": "THB 150.0",
     "category": "Snack", "last_sent": _d(2020, 1, 1)},
    {"name": "CHEESE SEASONING", "code": "B-NONE", "price": "THB 90.0",
     "category": "Snack"},
]


def test_newest_sample_ranks_first_even_when_pricier():
    """The 2026 product must lead, though it is the MOST expensive —
    recency beats price once relevance is tied."""
    res = matcher.top_seasonings("cheese", RECENCY_CATALOG, limit=4)
    assert [r["code"] for r in res][:3] == ["B-NEW", "B-MID", "B-OLD"]


def test_undated_product_sorts_last_not_first():
    res = matcher.top_seasonings("cheese", RECENCY_CATALOG, limit=4)
    assert res[-1]["code"] == "B-NONE"


def test_recency_does_not_override_relevance():
    """An ancient exact match still beats a fresh irrelevant one."""
    cat = [
        {"name": "SESAME SEASONING", "code": "S-OLD", "price": "USD 3.00",
         "category": "Snack", "last_sent": _d(2009, 1, 1)},
        {"name": "CHEESE SEASONING", "code": "S-NEW", "price": "USD 3.00",
         "category": "Snack", "last_sent": _d(2026, 7, 1)},
    ]
    res = matcher.top_seasonings("sesame", cat, limit=2)
    assert res[0]["code"] == "S-OLD"


def test_recency_survives_string_and_missing_dates():
    """Catalog can be built from tabs with no date column — must not crash."""
    cat = [
        {"name": "CHEESE SEASONING", "code": "X-1", "price": "USD 3.00",
         "category": "Snack", "last_sent": "2026-07-16"},
        {"name": "CHEESE SEASONING", "code": "X-2", "price": "USD 3.00",
         "category": "Snack", "last_sent": None},
        {"name": "CHEESE SEASONING", "code": "X-3", "price": "USD 3.00",
         "category": "Snack", "last_sent": "not a date"},
    ]
    res = matcher.top_seasonings("cheese", cat, limit=3)
    assert res[0]["code"] == "X-1"
    assert len(res) == 3


def test_recency_respects_budget_and_prefix_filters():
    res = matcher.top_seasonings("cheese b code below 4usd", RECENCY_CATALOG, limit=4)
    assert res
    assert all(r["code"].startswith("B-") for r in res)
    assert all(r["_price_num"] <= 4.0 for r in res)


# ---------- destination-country filter ("to vietnam") ----------

def test_country_filter_requires_preposition():
    # bare country words are flavours — pinned; the filter needs 'to'/'for'
    assert matcher.parse_country_filter("singapore laksa") == ("singapore laksa", None)
    assert matcher.parse_country_filter("thai tom yum") == ("thai tom yum", None)


def test_country_filter_parses_destinations():
    assert matcher.parse_country_filter("cheese to vietnam") == ("cheese", "Vietnam")
    assert matcher.parse_country_filter("rich sent to china") == ("rich", "China")
    assert matcher.parse_country_filter("to indonesia") == ("", "Indonesia")
    assert matcher.parse_country_filter("sesame for japan") == ("sesame", "Japan")


def test_country_filter_composes_with_budget():
    cleaned, country = matcher.parse_country_filter("cheese to vietnam <4.5usd")
    assert country == "Vietnam"
    kw, cap = matcher.parse_seasoning_query(cleaned)
    assert kw == "cheese" and cap == 4.5
