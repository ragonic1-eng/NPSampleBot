"""Regression tests for vision_scan catalog self-healing (V1.17.x).

Pinned against the REAL misreads from a rep's photo (2026-07-17):

    OCR read          printed on label
    S-643G1-06   →    S-633G1-06     (4 misread for 3)
    S-844AJ1     →    S-B4AJ1        (8 misread for B + doubled 4)
    S-44XG1-04   →    S-4AXG1-04     (4 misread for A)

All three must heal via the free catalog layer, without Haiku.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "")
os.environ.setdefault("GOOGLE_SHEETS_ID", "")

import matcher  # noqa: E402
from vision_scan import (  # noqa: E402
    _distance_snap,
    _heal_against_catalog,
    _merge_haiku_read,
    _plausible_edit_ops,
)

# The six codes actually printed on the rep's photo, plus decoys so the
# healer has to discriminate, not just pick the only option.
CATALOG = {
    "S-633G1-06", "S-YA351-11", "S-B4AJ1", "S-B1LJ1-03",
    "S-4AXG1-04", "S-71UH2-50",
    # decoys
    "S-62RG3-19", "S-668U1", "S-668U1-02", "S-Y9KY2",
    "J-61TS2-22", "B-A2K91-03", "S-B4AJ1-77",
}

RAW_FROM_PHOTO = [
    "S-643G1-06",  # wrong: 4 for 3
    "S-YA351-11",  # correct
    "S-844AJ1",    # wrong: 8 for B, doubled 4
    "S-B1LJ1-03",  # correct
    "S-44XG1-04",  # wrong: 4 for A
    "S-71UH2-50",  # correct
]

EXPECTED = [
    "S-633G1-06",
    "S-YA351-11",
    "S-B4AJ1",
    "S-B1LJ1-03",
    "S-4AXG1-04",
    "S-71UH2-50",
]


def test_photo_misreads_all_heal():
    final, corrections, unmatched, suggestions = _heal_against_catalog(
        RAW_FROM_PHOTO, CATALOG
    )
    assert final == EXPECTED
    assert unmatched == []
    assert corrections == {
        "S-643G1-06": "S-633G1-06",
        "S-844AJ1": "S-B4AJ1",
        "S-44XG1-04": "S-4AXG1-04",
    }
    assert suggestions == {}


def test_correct_codes_pass_through_untouched():
    final, corrections, unmatched, _ = _heal_against_catalog(list(EXPECTED), CATALOG)
    assert final == EXPECTED
    assert corrections == {}
    assert unmatched == []


def test_tie_between_twins_never_guesses():
    """S-668U3 sits at distance 1 from BOTH S-668U1 and S-668U2 — the
    healer must refuse to pick and surface both as suggestions."""
    catalog = {"S-668U1", "S-668U2"}
    snapped, candidates = _distance_snap("S-668U3", catalog)
    assert snapped is None
    assert set(candidates) == {"S-668U1", "S-668U2"}

    final, corrections, unmatched, suggestions = _heal_against_catalog(
        ["S-668U3"], catalog
    )
    assert final == ["S-668U3"]          # kept raw so /pp can still try MMS
    assert unmatched == ["S-668U3"]
    assert set(suggestions["S-668U3"]) == {"S-668U1", "S-668U2"}


def test_snap_never_crosses_prefix():
    """J- codes route to a different factory — a J- misread must not be
    'healed' into an S- code however close it is."""
    snapped, candidates = _distance_snap("J-633G1-06", {"S-633G1-06"})
    assert snapped is None
    assert candidates == []


def test_distance2_requires_plausible_ops():
    # 8→B swap + doubled-4 drop: plausible.
    assert _plausible_edit_ops("S-844AJ1", "S-B4AJ1") is True
    # Two arbitrary substitutions (W→K, J→Q): not a believable OCR slip.
    assert _plausible_edit_ops("S-W4AJ1", "S-K4AQ1") is False


def test_distance2_implausible_stays_unmatched():
    catalog = {"S-K4AQ1"}
    final, corrections, unmatched, _ = _heal_against_catalog(["S-W4AJ1"], catalog)
    assert final == ["S-W4AJ1"]
    assert unmatched == ["S-W4AJ1"]
    assert corrections == {}


def test_empty_catalog_passes_codes_through():
    final, corrections, unmatched, suggestions = _heal_against_catalog(
        ["S-123AB"], set()
    )
    assert final == ["S-123AB"]
    assert unmatched == ["S-123AB"]
    assert suggestions == {}


def test_merge_haiku_read_prefers_validated_counterpart():
    """Local OCR misread one line with TWO implausible slips (W for B and
    Q for J — neither is a confusable pair, so the local healer refuses to
    snap at distance 2); the Haiku re-read of the same physical line
    validates → Haiku's code wins."""
    catalog = {"S-B4AJ1", "S-71UH2-50"}
    l_raw = ["S-W4AQ1", "S-71UH2-50"]
    l_final, l_corr, l_unm, l_sugg = _heal_against_catalog(l_raw, catalog)
    assert "S-W4AQ1" in l_unm  # sanity: local couldn't heal it

    raw, final, corr, unm, sugg = _merge_haiku_read(
        l_raw, l_final, l_corr, l_unm, l_sugg,
        h_raw=["S-B4AJ1", "S-71UH2-50"],
        catalog_codes=catalog,
    )
    assert final == ["S-B4AJ1", "S-71UH2-50"]
    assert corr["S-W4AQ1"] == "S-B4AJ1"
    assert unm == []


def test_merge_haiku_read_never_collapses_distant_codes():
    """A local read ≥3 edits away from every Haiku code is a DIFFERENT
    line, not a re-read — both must be kept (nothing silently dropped)."""
    catalog = {"S-B4AJ1", "S-71UH2-50"}
    l_raw = ["S-8A4AJ7", "S-71UH2-50"]  # 3 edits from S-B4AJ1
    l_final, l_corr, l_unm, l_sugg = _heal_against_catalog(l_raw, catalog)

    raw, final, corr, unm, sugg = _merge_haiku_read(
        l_raw, l_final, l_corr, l_unm, l_sugg,
        h_raw=["S-B4AJ1", "S-71UH2-50"],
        catalog_codes=catalog,
    )
    assert "S-8A4AJ7" in final   # local kept (unmatched, still tried on MMS)
    assert "S-B4AJ1" in final    # haiku's extra line appended
    assert unm == ["S-8A4AJ7"]


def test_merge_haiku_read_appends_missed_lines():
    """Haiku sees a code the local engine missed entirely — appended."""
    catalog = {"S-633G1-06", "S-B4AJ1"}
    l_raw = ["S-633G1-06"]
    l_final, l_corr, l_unm, l_sugg = _heal_against_catalog(l_raw, catalog)
    # Local read fully validated, but pretend escalation ran anyway.
    raw, final, corr, unm, sugg = _merge_haiku_read(
        l_raw, l_final, l_corr, l_unm, l_sugg,
        h_raw=["S-633G1-06", "S-B4AJ1"],
        catalog_codes=catalog,
    )
    assert final == ["S-633G1-06", "S-B4AJ1"]
    assert unm == []


def test_merge_haiku_keeps_local_when_haiku_unvalidated_too():
    """Neither read validates (maybe a brand-new code not in the sheet yet)
    — keep the local read so /pp still tries MMS with it."""
    catalog = {"S-71UH2-50"}
    l_raw = ["S-999ZZ1"]
    l_final, l_corr, l_unm, l_sugg = _heal_against_catalog(l_raw, catalog)
    raw, final, corr, unm, sugg = _merge_haiku_read(
        l_raw, l_final, l_corr, l_unm, l_sugg,
        h_raw=["S-999ZZ7"],
        catalog_codes=catalog,
    )
    assert final == ["S-999ZZ1"]
    assert unm == ["S-999ZZ1"]


def test_close_code_matches_orders_by_distance():
    catalog = {"S-633G1-06", "S-633G1-07", "S-B4AJ1", "J-643G1-06"}
    out = matcher.close_code_matches("S-643G1-06", catalog, limit=3)
    codes = [c for c, _d in out]
    # Same-prefix candidates only (no J-), distance-1 hits first.
    assert "S-633G1-06" in codes
    assert "J-643G1-06" not in codes
    assert out[0][1] <= out[-1][1]
