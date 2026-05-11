"""Regression tests for bot._smart_text_match.

These pin the fuzzy-matcher behaviour against known false positives and
known typo-correction cases so future ratio tweaks don't silently regress.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "")
os.environ.setdefault("GOOGLE_SHEETS_ID", "")

from bot import _smart_text_match  # noqa: E402


def test_masala_does_not_match_mala_product_name():
    """V1.13.8 regression: searching 'masala' must not return 'MALA BEEF
    SEASONING'. The 4-char target token 'mala' must be rejected by the
    length guard against the 6-char query."""
    assert _smart_text_match("masala", "MALA BEEF SEASONING") is False
    assert _smart_text_match("masala", "MALA SEASONING") is False


def test_masala_still_matches_masala_products():
    assert _smart_text_match("masala", "SPICY MASALA SEASONING") is True
    assert _smart_text_match("masala", "MASALA SEASONING") is True


def test_intentional_mala_search_still_finds_mala():
    """Reps explicitly looking for mala (4 chars) must still find it."""
    assert _smart_text_match("mala", "MALA BEEF SEASONING") is True
    assert _smart_text_match("mala", "MALA SEASONING") is True


def test_typo_rendnag_still_corrects_to_rendang():
    """V1.12.6 typo case must keep working under the tighter guard."""
    assert _smart_text_match("rendnag", "rendang seasoning") is True
    assert _smart_text_match("rendng", "rendang seasoning") is True


def test_short_typos_still_match():
    assert _smart_text_match("cheez", "cheese seasoning") is True
    assert _smart_text_match("datng", "Da tong") is True


def test_malaysia_does_not_match_mala():
    """V1.13.3 regression case."""
    assert _smart_text_match("malaysia", "MALA BEEF SEASONING") is False


def test_peri_does_not_match_pepper():
    """V1.8.8 known false-positive case."""
    assert _smart_text_match("peri", "pepper seasoning") is False
