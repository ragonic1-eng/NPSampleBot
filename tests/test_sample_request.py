# -*- coding: utf-8 -*-
"""/sr parsing regressions — the Pran 3-flavour message (31 Aug 2026):
numbered list starting at '2.' must not drop flavour 1, and an FSL alias
containing the customer's name must not force 'which one did you mean?'."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import sample_request as srq  # noqa: E402
import sheets  # noqa: E402

PRAN = """pran food - Lemon Habanero Seasoning.
Jalapeno Flavored Seasoning.
Salsa Verde Flavored Seasoning
Comment - no prefer code. Customer want Lemon Habanero Seasoning:
The flavor should have two clear characteristics: citrus freshness followed by strong habanero chilli heat.
Supporting notes: salt, garlic and a little onion.
Should not become excessively sour or bitter.

2. Jalapeño Flavored Seasoning

This should represent the fresh green chilli character of jalapeño, rather than simply making it very hot.
Garlic and onion as supporting notes.

3. Salsa Verde Flavored Seasoning

This should have a fresh, tangy green salsa character.
Tomatillo + green chilli + lime + coriander + onion + garlic.
Budget <2 usd
Compliance for banagldesh and mexico market
1kg for each mention flavor
Target base is on potato biscuit,win win type"""


def test_implicit_first_flavour_recovered():
    a = srq.parse_ask(PRAN)
    assert a.structured
    assert [f["name"] for f in a.flavours] == [
        "Lemon Habanero Seasoning",
        "Jalapeño Flavored Seasoning",
        "Salsa Verde Flavored Seasoning",
    ]
    # flavour 1 owns its spec lines, not the intro
    assert any("citrus freshness" in s for s in a.flavours[0]["spec"])
    assert a.qty_g == 1000 and a.qty_each
    assert a.overrides["budget"] == "<2 usd"
    assert a.overrides["compliance"] == "Bangladesh, Mexico"


def test_reqnote_counts_three_flavours():
    a = srq.parse_ask(PRAN)
    d = {"qty": a.qty_g, "sets": 1, "rtype": "new", "rtype_label": "New",
         "base_code": ""}
    draft = {"derived": d, "ask": a, "bag": "", "budget": "<2 usd",
             "compliance": "Bangladesh, Mexico", "need_by": "", "attn": "",
             "contact": "", "addr": ""}
    note = srq.render_reqnote(draft)
    assert "1. LEMON HABANERO SEASONING" in note
    assert "3. SALSA VERDE FLAVORED SEASONING" in note
    assert "3 flavours" in note and "2 flavours" not in note


def test_start_at_two_unrecoverable_stays_verbatim():
    a = srq.parse_ask("""acme - misc request
some intro text here about the project
2. Sour Cream Seasoning
tangy, creamy
3. Onion Seasoning
oniony""")
    assert not a.structured
    assert "2. Sour Cream Seasoning" in a.ask_text and "oniony" in a.ask_text


def test_normal_numbering_still_structures():
    a = srq.parse_ask("""acme - two flavours
1. BBQ Seasoning - smoky, sweet
2. Sour Cream Seasoning
tangy, creamy
200g each""")
    assert a.structured
    assert [f["name"] for f in a.flavours] == ["BBQ Seasoning",
                                               "Sour Cream Seasoning"]


def test_resolve_customer_exact_tokens_beat_alias(monkeypatch):
    monkeypatch.setattr(sheets, "load_merged_customers", lambda **k: [
        {"name": "Pran Foods Ltd", "code": "S-UDP041"},
        {"name": "Prime Taste Pte Ltd", "code": "S-XXX"},
    ])
    monkeypatch.setattr(sheets, "load_fsl_rows_all", lambda tab: [
        {"Customer Name": "(F.B.M Technologies Ltd (Pran Foods))"},
    ] * 3)
    best, cands = srq.resolve_customer("pran food")
    assert best is not None and best["name"] == "Pran Foods Ltd"
    # the alias is still visible as a runner-up, just not a blocker
    assert any("F.B.M" in c["name"] for c in cands)
