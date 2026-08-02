"""Tests for the V1.17.x universal smart text router building blocks.

The router itself needs Telegram + Sheets plumbing; these tests pin the
pure identification helpers that decide WHERE a typed message goes:
  • _route_strip_fillers — "samples for alex" → "alex"
  • _match_rep_names     — exact / first-name / fuzzy rep detection
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "")
os.environ.setdefault("GOOGLE_SHEETS_ID", "")

from bot import _match_rep_names, _route_strip_fillers  # noqa: E402

REPS = ["Alex Tan", "Leo", "William Susanto", "Heidy", "Freddy"]


def test_fillers_stripped_around_names():
    assert _route_strip_fillers("samples for alex") == "alex"
    assert _route_strip_fillers("show me datong") == "datong"
    assert _route_strip_fillers("what did leo send out") == "leo"


def test_filler_only_text_falls_back_to_original():
    # Everything is filler — return the original text rather than "".
    assert _route_strip_fillers("show me samples") == "show me samples"


def test_rep_exact_full_name_is_strong():
    hits, strong = _match_rep_names("alex tan", REPS)
    assert strong is True
    assert hits == ["Alex Tan"]


def test_rep_first_name_is_strong():
    hits, strong = _match_rep_names("alex", REPS)
    assert strong is True
    assert hits == ["Alex Tan"]


def test_rep_case_and_spacing_insensitive():
    hits, strong = _match_rep_names("  ALEX  ", REPS)
    assert strong is True and hits == ["Alex Tan"]


def test_rep_typo_is_weak_hit_not_strong():
    hits, strong = _match_rep_names("willium susanto", REPS)
    assert strong is False
    assert "William Susanto" in hits


def test_product_words_do_not_match_reps():
    for q in ("cheese", "bbq chicken", "masala noodle", "spicy squid"):
        hits, strong = _match_rep_names(q, REPS)
        assert hits == [], f"{q!r} unexpectedly matched {hits}"


def test_short_text_never_matches_reps():
    hits, strong = _match_rep_names("al", REPS)
    assert hits == [] and strong is False


def test_two_char_leo_requires_exact():
    # 'leo' is 3 chars — allowed, exact match.
    hits, strong = _match_rep_names("leo", REPS)
    assert strong is True and hits == ["Leo"]


# ---------- origin line: country + customer on product results (V1.17.x) ----------

from bot import _origin_line  # noqa: E402


def test_origin_line_shows_country_and_customer():
    out = _origin_line("Indonesia", "PT MAKMUR")
    assert "Indonesia" in out and "PT MAKMUR" in out
    assert out.startswith("📍")
    assert "🇮🇩" in out          # known country gets a flag


def test_origin_line_customer_only():
    out = _origin_line("", "ACME")
    assert "ACME" in out
    assert "from" not in out       # no country -> don't say "from"


def test_origin_line_country_only():
    out = _origin_line("Thailand", "")
    assert "Thailand" in out
    assert "🇹🇭" in out


def test_origin_line_empty_when_nothing_known():
    assert _origin_line("", "") == ""
    assert _origin_line(None, None) == ""


def test_origin_line_unknown_country_has_no_flag_but_still_shows():
    out = _origin_line("Narnia", "SOMECO")
    assert "Narnia" in out and "SOMECO" in out


# ---------- AWB price-leak guard (S-B25L4 bug) ----------

from bot import _awb_is_price_leak  # noqa: E402


def test_awb_price_leak_detects_costs_not_tracking_numbers():
    # Raw-material costs wrongly living in col K of the Singapore FSL tab.
    for leak in ("1.9232", "4.3635", "2.57", "123.45", "SGD 4.36",
                 "USD 3.10", "  2.57  "):
        assert _awb_is_price_leak(leak), leak


def test_awb_price_leak_passes_real_awbs():
    # Genuine tracking numbers and markers must NOT be flagged.
    for good in ("1234567890", "JD014600003217", "HAND CARRY", "HC",
                 "", "1Z999AA10123456784"):
        assert not _awb_is_price_leak(good), good


# ---------- transient Sheets outage detection (sales-name silent-fail) ----------

from bot import _is_transient_data_error  # noqa: E402


def test_transient_data_error_flags_google_outages():
    class APIError(Exception):
        pass
    for e in (APIError("[503]: The service is currently unavailable."),
              Exception("[429]: rateLimitExceeded"),
              Exception("500 backendError"),
              Exception("APIError: [502]")):
        assert _is_transient_data_error(e), e


def test_transient_data_error_ignores_real_bugs():
    for e in (ValueError("no such column"), KeyError("Sales"),
              TypeError("bad operand")):
        assert not _is_transient_data_error(e), e
