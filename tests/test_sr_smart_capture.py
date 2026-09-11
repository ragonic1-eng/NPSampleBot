"""/sr smart capture (Alex 11-Sep-2026: "make the bot able to understand
user input more smarter. no repeat of content").

Rules-level coverage for what the live run showed printing twice or not
being read; the LLM-assisted parts (target base, one-line item lists) are
exercised through sr_live.py against the real provider."""
import datetime as _dt

import sample_request as srq


def _draft(a, **kw):
    d = {"derived": {"qty": a.qty_g or 100, "sets": 1, "rtype": "new",
                     "rtype_label": "New", "base_code": ""},
         "ask": a, "bag": a.overrides.get("bag", ""),
         "budget": a.overrides.get("budget", ""),
         "compliance": a.overrides.get("compliance", ""),
         "need_by": a.overrides.get("need_by", "").upper(),
         "attn": a.overrides.get("attn", ""),
         "contact": a.overrides.get("contact", ""),
         "addr": a.overrides.get("addr", "")}
    d.update(kw)
    return d


def test_casual_one_liner_fields_leave_the_comment():
    a = srq.parse_ask("pran, chilli seasoning and seafood seasoning 200g each "
                      "for potato chips, need by next friday, budget below 2 usd")
    assert a.customer_text == "pran"
    assert a.items == ["chilli seasoning", "seafood seasoning"]
    assert a.base == "potato chips"
    assert a.qty_g == 200 and a.qty_each
    assert a.overrides["need_by"] == "next friday"
    assert a.overrides["budget"] == "below 2 usd"
    assert a.ask_text == ""          # nothing left to print twice
    note = srq.render_reqnote(_draft(a))
    assert note.count("next friday") == 0 and "NEED BY: NEXT FRIDAY" in note
    assert note.count("below 2 usd") == 1


def test_one_liner_sentence_is_kept_when_it_is_not_a_list():
    a = srq.parse_ask("pran - make the bbq seasoning smokier and less sweet 200g")
    assert a.items == []
    assert "smokier and less sweet" in a.ask_text
    assert a.qty_g == 200


def test_single_item_head_line_splits_name_from_amount():
    a = srq.parse_ask("acme - bbq seasoning 200g")
    assert a.qty_g == 200
    note = srq.render_reqnote(_draft(a))
    assert "QTY: 200g x 1 set" in note
    assert note.count("200g") == 1


def test_send_by_carrier_line_is_the_method_not_comment():
    a = srq.parse_ask("Pran\nSeasoning name:\nCheese seasoning\n"
                      "Comment: more cheesy\nSend by DHL")
    assert a.delivery == "DHL"
    assert "dhl" not in a.ask_text.lower()
    a2 = srq.parse_ask("Pran - bbq seasoning 100g\nship via lalamove")
    assert a2.delivery == "Lalamove"


def test_labelled_need_by_is_consumed_after_a_prose_mention():
    a = srq.parse_ask("pran foods\nSeasoning name:\nTomato seasoning\n"
                      "Comment: customer wants tomato seasoning for potato "
                      "crackers, need by 20 Sept.\nNeed by: 20 Sept\nQty: 500g")
    assert a.overrides["need_by"] == "20 Sept"
    note = srq.render_reqnote(_draft(a))
    assert note.count("Need by: 20 Sept") == 0
    assert note.count("NEED BY: 20 SEPT") == 1


def test_labelled_budget_is_consumed_after_a_prose_mention():
    a = srq.parse_ask("pran - bbq seasoning 100g\n"
                      "customer says budget must be below 2 usd for this one and it is urgent for them\n"
                      "Budget: below 2 usd")
    assert a.overrides["budget"] == "below 2 usd"
    assert "Budget: below 2 usd" not in a.ask_text


def test_quantity_words_travel_with_the_figure():
    a = srq.parse_ask("Quasem\nNaga seasoning\nChilli seasoning\n"
                      "100g each no application")
    assert a.qty_g == 100 and a.qty_each
    assert a.qty_text == "100g each no application"
    a2 = srq.parse_ask("Quasem\nNaga seasoning\n200g each")
    assert a2.qty_text == ""          # bare figure keeps the derived form


def test_form_quantity_phrase_is_not_repeated_under_items():
    msg = """SEASONING NAME: SAMBAL CHILLI SEASONING
COMMENT: MORE HEAT
50G SEASONING WITH NO APPLIED SAMPLES

SEASONING NAME: TAKOYAKI SEASONING
COMMENT: CHECK PH RA
100G SEASONING WITH NO APPLIED SAMPLES

Customer: Apacific"""
    a = srq.parse_ask(msg)
    assert a.form_mode
    assert all("APPLIED SAMPLES" not in s for f in a.flavours for s in f["spec"])
    note = srq.render_reqnote(_draft(a, derived={"qty": 50, "sets": 1, "rtype": "new",
                                                 "rtype_label": "New", "base_code": ""}))
    head, _, qty = note.partition("QTY:")
    assert "APPLIED SAMPLES" not in head
    assert qty.count("SEASONING WITH NO APPLIED SAMPLES") == 2


def test_relative_need_by_becomes_a_date():
    today = _dt.date(2026, 9, 11)   # a Friday
    assert srq._relative_need_by("next friday", today) == _dt.date(2026, 9, 18)
    assert srq._relative_need_by("next monday", today) == _dt.date(2026, 9, 14)
    assert srq._relative_need_by("this week", today) == _dt.date(2026, 9, 18)
    assert srq._relative_need_by("next week", today) == _dt.date(2026, 9, 18)
    assert srq._relative_need_by("in 2 weeks", today) == _dt.date(2026, 9, 25)
    assert srq._relative_need_by("end of month", today) == _dt.date(2026, 9, 30)
    assert srq._relative_need_by("tomorrow", today) == _dt.date(2026, 9, 14)  # Sat -> Mon
    assert srq._relative_need_by("asap", today) is None
    assert srq._relative_need_by("16 Sept", today) is None


def test_need_by_text_labels_the_resolved_date():
    src = {}
    out = srq._need_by_text("next week", src)
    assert out.startswith("BY ") and src["need_by"] == "from 'next week'"
    src = {}
    assert srq._need_by_text("16 Sept", src) == "16 SEPT" and "need_by" not in src
