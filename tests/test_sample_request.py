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


def test_reqnote_has_no_thanks():
    a = srq.parse_ask(PRAN)
    d = {"qty": a.qty_g, "sets": 1, "rtype": "new", "rtype_label": "New",
         "base_code": ""}
    draft = {"derived": d, "ask": a, "bag": "", "budget": "<2 usd",
             "compliance": "Bangladesh, Mexico", "need_by": "", "attn": "",
             "contact": "", "addr": "105 Pragati Swarani, Dhaka"}
    note = srq.render_reqnote(draft)
    assert "THANKS" not in note
    assert note.rstrip().endswith("ADDRESS: 105 Pragati Swarani, Dhaka")


PRAN_SHIPTO = """pran food - Lemon Habanero Seasoning.
Jalapeno Flavored Seasoning.
Salsa Verde Flavored Seasoning
Comment - no prefer code. Customer want Lemon Habanero Seasoning:
The flavor should have two clear characteristics: citrus freshness followed by strong habanero chilli heat.
2. Jalapeño Flavored Seasoning
Garlic and onion as supporting notes.
3. Salsa Verde Flavored Seasoning
Tomatillo + green chilli + lime + coriander + onion + garlic.
PRAN FOODS LTD
MR SAJIB
+880 1704-158453
PRAN-RFL CENTRE, 105, MIDDLE BADDA,
GPO BOX #83, LEVEL 8,
1212 DHAKA
Bangladesh
Budget <2 usd
Compliance for banagldesh and mexico market
1kg for each mention flavor
Target base is on potato biscuit,win win type"""


def test_pasted_shipto_block_becomes_overrides_not_body():
    a = srq.parse_ask(PRAN_SHIPTO)
    # the NEW details the rep pasted win over anything remembered
    assert a.overrides.get("contact") == "+880 1704-158453"
    assert a.overrides.get("attn") == "Mr Sajib"
    assert a.overrides.get("addr") == (
        "PRAN FOODS LTD, PRAN-RFL CENTRE, 105, MIDDLE BADDA, "
        "GPO BOX #83, LEVEL 8, 1212 DHAKA, Bangladesh")
    # the block leaves the note body entirely — no duplicate in MMS
    joined = a.ask_text + " ".join(
        s for f in a.flavours for s in f["spec"])
    assert "+880" not in joined and "PRAN-RFL" not in joined
    assert "MR SAJIB" not in joined
    # flavour structuring is unaffected
    assert a.structured and len(a.flavours) == 3


def test_labelled_shipto_still_beats_pasted_block():
    a = srq.parse_ask("""acme - bbq seasoning
CONTACT NO.: 999888777
MR SAJIB
+880 1704-158453
105, MIDDLE BADDA, DHAKA
Bangladesh""")
    assert a.overrides["contact"] == "999888777"  # explicit label wins
    assert a.overrides["attn"] == "Mr Sajib"
    assert "+880" not in a.ask_text  # block still removed from the note


def test_lone_number_line_is_not_a_shipto_block():
    a = srq.parse_ask("""acme - bbq seasoning
target 1200000 scu
1704158453
smoky and sweet profile""")
    assert "contact" not in a.overrides
    assert "1704158453" in a.ask_text


def test_need_by_prepdate_formats():
    import datetime as dt
    today = dt.date(2026, 9, 1)
    f = srq.need_by_prepdate
    assert f("BY 13 SEP 2026", today) == "13/Sep/2026"
    assert f("13 sep", today) == "13/Sep/2026"
    assert f("Sep 13", today) == "13/Sep/2026"
    assert f("13/9", today) == "13/Sep/2026"
    assert f("by 5th sep", today) == "05/Sep/2026"
    assert f("15/1", today) == "15/Jan/2027"  # already passed -> next year
    assert f("9/13", today) == "13/Sep/2026"  # month-first numeric, swapped
    # vague deadlines never guess a date
    assert f("ASAP", today) == ""
    assert f("BY NEXT WEEK", today) == ""
    assert f("BY SEP 2026", today) == ""  # month+year only: no day
    assert f("", today) == ""


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
