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
