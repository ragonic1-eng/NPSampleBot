# -*- coding: utf-8 -*-
"""Regressions for the V1.17.x full-bot audit (31 Aug 2026).

Pins the fixes for: PTB day-numbering (Sun-Thu crons), code-regex false
positives ('b-codes', 'T-bone'), semicolon truncation in /sr, mid-word
CONTACT/ADDRESS captures, dosage lines read as quantity, 'x 3 flavours'
read as sets, the draft-edit fallback hijacking searches, foreign-currency
price caps, pure-generic queries returning the cheapest catalogue items,
and _collapse_samples merging two reps' shipments.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "")

import matcher  # noqa: E402
import sample_request as srq  # noqa: E402
from bot import (_WEEKDAYS, _collapse_samples, _kw_all_words_match,  # noqa: E402
                 _match_rep_names, _smart_text_match)


# ---- PTB cron convention ---------------------------------------------------

def test_weekday_tuple_is_ptb_v20_monday_to_friday():
    # PTB v20+: run_daily days 0-6 = SUNDAY-Saturday. (0,1,2,3,4) ran the
    # "Mon-Fri" jobs Sun-Thu — no Friday digest/sync, Sunday nudges.
    assert _WEEKDAYS == (1, 2, 3, 4, 5)


# ---- code-shape false positives -------------------------------------------

def test_prefix_words_are_not_codes():
    a = srq.parse_ask("acme - T-bone steak seasoning for chips 100g")
    assert a.codes == []  # 'T-bone' has no digit → not an MMS code
    b = srq.parse_ask("acme - repeat S-18CS43-002 200g")
    assert b.codes == ["S-18CS43-002"]


def test_codes_inside_flavour_blocks_still_detected():
    a = srq.parse_ask("""acme - two flavours
1. BBQ Seasoning
please make it closer to S-18CS43-002 but reduce the salt
2. Sour Cream Seasoning
tangy""")
    assert a.structured
    assert "S-18CS43-002" in a.codes


# ---- parse_ask consumption bugs -------------------------------------------

def test_semicolons_do_not_truncate_the_request():
    a = srq.parse_ask(
        "pran food - Lemon Habanero; Jalapeno Ranch; Salsa Verde\n"
        "Budget <2 usd\n1kg for each mention flavor")
    assert "Jalapeno Ranch" in a.ask_text and "Salsa Verde" in a.ask_text
    assert a.overrides.get("budget") == "<2 usd"
    assert a.qty_g == 1000 and a.qty_each


def test_legacy_semicolon_overrides_still_work():
    a = srq.parse_ask("acme - bbq seasoning; budget: 3-4 usd; bag: NP bag")
    assert a.overrides.get("budget") == "3-4 usd"
    assert a.overrides.get("bag") == "NP bag"


def test_tel_does_not_match_inside_words():
    a = srq.parse_ask("acme - new flavour\nTellicherry black pepper base note")
    assert "contact" not in a.overrides
    assert "Tellicherry" in a.ask_text


def test_address_the_verb_is_not_an_address():
    a = srq.parse_ask("acme - new flavour\n"
                      "Please address the bitterness at the end")
    assert "addr" not in a.overrides
    b = srq.parse_ask("acme - new flavour\nAddress: 105 Pragati Swarani, Dhaka")
    assert b.overrides.get("addr") == "105 Pragati Swarani, Dhaka"


def test_dosage_line_is_not_the_sample_quantity():
    a = srq.parse_ask("acme - bbq seasoning\n"
                      "dosage 15g per kg of chips\n"
                      "1kg for each mention flavor")
    assert a.qty_g == 1000 and a.qty_each


def test_x_n_flavours_is_not_a_set_count():
    a = srq.parse_ask("acme - mixed request 200g x 3 flavours")
    assert a.sets is None
    b = srq.parse_ask("acme - bbq seasoning 200g x 3 sets")
    assert b.sets == 3


# ---- draft-edit fallback must not hijack searches --------------------------

def _dummy_draft():
    return {"ask": srq.Ask()}


def test_fallback_bare_price_with_product_words_is_unrelated():
    for t in ("cheese b code below 4usd", "rich <4.5 usd",
              "cheapest chicken powder", "laksa under 3 usd"):
        assert srq.fallback_update(_dummy_draft(), t)["action"] == "unrelated", t


def test_fallback_real_budget_edits_still_work():
    for t in ("<2 usd", "make it <2 usd", "budget 3-4 usd",
              "cheap as possible"):
        upd = srq.fallback_update(_dummy_draft(), t)
        assert upd["action"] == "modify" and upd["fields"].get("budget"), t


def test_fallback_confirm_and_discard_untouched():
    assert srq.fallback_update(_dummy_draft(), "ok raise it")["action"] == "confirm"
    assert srq.fallback_update(_dummy_draft(), "never mind")["action"] == "discard"


# ---- matcher: currency + generic-only queries ------------------------------

def test_foreign_currency_cap_is_converted_to_usd():
    kw, cap = matcher.parse_seasoning_query("cheese below 100 thb")
    assert kw == "cheese"
    assert cap is not None and 2.5 < cap < 3.5  # 100 THB ≈ $2.90


def test_generic_only_query_returns_no_matches():
    cat = [
        {"name": "CHEAP FILLER", "price": "USD 0.10", "code": "S-1"},
        {"name": "CHICKEN POWDER", "price": "USD 4.00", "code": "S-2"},
    ]
    assert matcher.top_seasonings("seasoning powder", cat) == []
    # …but a pure price query still returns the cheapest.
    hits = matcher.top_seasonings("below 1 usd", cat)
    assert [s["code"] for s in hits] == ["S-1"]


# ---- collapse key ----------------------------------------------------------

def test_collapse_keeps_two_reps_shipments_separate():
    d = dt.date(2026, 8, 12)
    rows = [
        {"Product Code": "S-668U1", "Sample Date Out": "12/Aug/2026",
         "Customer Name": "Hung Hau", "Sales": "Rich", "_date": d,
         "R&D Price": "USD 4.00", "Ingested At UTC": "2026-08-12T01"},
        {"Product Code": "S-668U1", "Sample Date Out": "12/Aug/2026",
         "Customer Name": "Hung Hau", "Sales": "Alex", "_date": d,
         "R&D Price": "USD 4.00", "Ingested At UTC": "2026-08-12T02"},
    ]
    assert len(_collapse_samples(rows)) == 2


def test_collapse_merges_date_format_drift():
    d = dt.date(2026, 4, 1)
    rows = [
        {"Product Code": "S-1", "Sample Date Out": "01/Apr/2026",
         "Customer Name": "Acme", "Sales": "Alex", "_date": d,
         "R&D Price": "USD 4.00", "Ingested At UTC": "a"},
        {"Product Code": "S-1", "Sample Date Out": "2026-04-01",
         "Customer Name": "Acme", "Sales": "Alex", "_date": d,
         "R&D Price": "USD 4.10", "Ingested At UTC": "b"},
    ]
    out = _collapse_samples(rows)
    assert len(out) == 1 and out[0]["_dup_count"] == 2


# ---- promo pseudo-customers must be findable by filter ---------------------

def test_multiword_filter_matches_promo_customers():
    # 'alex bangladesh promotion' → rep filter kw 'bangladesh promotion'.
    # _smart_text_match needs the words adjacent, so these grouped promo
    # rows were invisible; the all-words fallback finds them.
    target = "(Bangladesh August sample promotion)"
    assert not _smart_text_match("bangladesh promotion", target)  # the gap
    assert _kw_all_words_match("bangladesh promotion", target)
    assert _kw_all_words_match("bangladesh aug", target)
    assert _kw_all_words_match("bangladesh sample promotion", target)
    # …but wrong-country and single-word queries don't over-match
    assert not _kw_all_words_match("bangladesh promotion",
                                   "(Vietnam promotion samples July trip)")
    assert not _kw_all_words_match("promotion", target)  # 1 token → defer
    assert not _kw_all_words_match("", target)


# ---- 2-char rep names ------------------------------------------------------

def test_two_char_rep_exact_match_is_strong():
    hits, strong = _match_rep_names("nu", ["Nu", "Jang", "Ying"])
    assert strong and hits == ["Nu"]
    # …but 2 chars never fuzzy-matches anything.
    hits2, strong2 = _match_rep_names("nu", ["Jang", "Ying"])
    assert hits2 == [] and not strong2
