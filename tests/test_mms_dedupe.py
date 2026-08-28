"""Regression test for the repeat-shipment data-loss bug (V1.17.31).

fetch_all_samples de-duplicates rows across overlapping monthly chunks. The
key used to be (sample_request_code, product_code) with NO ship date, so a
standing sample request that shipped the SAME product again on a later date
had every repeat silently discarded.

Real case that surfaced it: SR S-11RS43-002 (APACIFIC) shipped S-B18L9-01
HOKKAIDO MILK on 12/May/2026 and again on 24/Aug/2026 — only May survived,
so the bot reported the August sample had never gone out. 165 rows were
lost across one Mar-Aug sync.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "")
os.environ.setdefault("GOOGLE_SHEETS_ID", "")

import mms_client  # noqa: E402


def _row(code: str, product: str, ship: str) -> mms_client.SampleRow:
    return mms_client.SampleRow(
        sample_request_date="", sample_request_code=code, sales="Alex",
        country="SG (Singapore)", customer_code="S-TDA024",
        customer_name="APACIFIC GROUP PTE LTD", product_code=product,
        product_name="HOKKAIDO MILK SEASONING (IN DOUGH)",
        rd_price="SGD 8.88", sample_date_out=ship, feedback="", quantity_g="500",
    )


def test_repeat_shipment_of_same_product_is_kept(monkeypatch):
    """Same SR + same product, two different ship dates => BOTH kept."""
    may = _row("S-11RS43-002", "S-B18L9-01", "12/May/2026")
    aug = _row("S-11RS43-002", "S-B18L9-01", "24/Aug/2026")

    def fake_search(session, a, b):
        # One row per chunk, mimicking the real Mar-May / Jun-Aug split.
        return [may] if a.month < 6 else [aug]

    monkeypatch.setattr(mms_client, "search_samples", fake_search)
    out = mms_client.fetch_all_samples(None, dt.date(2026, 3, 1), dt.date(2026, 8, 28))
    dates = sorted(r.sample_date_out for r in out)
    assert dates == ["12/May/2026", "24/Aug/2026"], dates


def test_true_duplicate_from_overlapping_chunks_is_still_dropped(monkeypatch):
    """The dedupe must still collapse the SAME shipment seen twice."""
    same = _row("S-11RS43-002", "S-B18L9-01", "24/Aug/2026")

    def fake_search(session, a, b):
        return [same]          # every chunk returns the identical row

    monkeypatch.setattr(mms_client, "search_samples", fake_search)
    out = mms_client.fetch_all_samples(None, dt.date(2026, 3, 1), dt.date(2026, 8, 28))
    assert len(out) == 1, [r.sample_date_out for r in out]


def test_distinct_products_under_one_request_all_kept(monkeypatch):
    rows = [
        _row("S-11RS43-002", "S-B1WL1-02", "25/Aug/2026"),
        _row("S-11RS43-002", "S-B18L9-01", "24/Aug/2026"),
        _row("S-11RS43-002", "S-62RG3-19", "25/Aug/2026"),
    ]

    def fake_search(session, a, b):
        return rows if a.month >= 6 else []

    monkeypatch.setattr(mms_client, "search_samples", fake_search)
    out = mms_client.fetch_all_samples(None, dt.date(2026, 3, 1), dt.date(2026, 8, 28))
    assert len({r.product_code for r in out}) == 3
