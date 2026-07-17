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
