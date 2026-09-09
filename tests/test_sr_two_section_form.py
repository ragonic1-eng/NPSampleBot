"""Alex's two-section form (Pran Foods, 09-Sep-2026): a SEASONING NAME
section listing items (name, then code on the next line, or 'NAME CODE'
on one line) and a COMMENT section repeating each item with ' - note'
after its code. The bot had merged the two sections into one item list
and left the first two notes floating.

Rendering rules he confirmed the same night: the sections mirror his
lines as typed (code on its own line stays on its own line), and
quantities live in a per-item QTY block, never in the Comment headers."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sample_request as srq  # noqa: E402

MSG = """customer: Pran foods
SEASONING NAME:
CHILLI SEASONING
S-83EH5-08

ROASTED CORN SEASONING S-83NJ1-11

SEAFOOD SEASONING
S-J2M53-26-07

CORN BBQ SEASONING
S-B8SL1

COMMENT:
CHILLI SEASONING
S-83EH5-08 -short listed. Same code same profile repeat sample

ROASTED CORN SEASONING S-83NJ1-11- Decrease salt by 20%

SEAFOOD SEASONING
S-J2M53-26-07- Increase seafood taste by 20%

CORN BBQ SEASONING
S-B8SL1 - Reduce salt by 20%

Qty: each sample 200g no need application

Budget: below <2.2 usd
Compliance: Bangladesh
Target base: corn curl
Send method: DHL
Attention: Mr Sajib
"""


def _draft(a):
    return {"derived": {"qty": a.qty_g or 200, "sets": 1, "rtype": "rep",
                        "rtype_label": "Repeat", "base_code": ""},
            "ask": a, "bag": "NP bag", "budget": a.overrides.get("budget", ""),
            "compliance": a.overrides.get("compliance", ""), "need_by": "",
            "attn": a.overrides.get("attn", ""), "contact": "", "addr": ""}


def test_names_section_gives_four_items_as_typed():
    a = srq.parse_ask(MSG)
    assert [(f["name"], f["code"], bool(f.get("code_line"))) for f in a.flavours] == [
        ("CHILLI SEASONING", "S-83EH5-08", True),
        ("ROASTED CORN SEASONING S-83NJ1-11", "S-83NJ1-11", False),
        ("SEAFOOD SEASONING", "S-J2M53-26-07", True),
        ("CORN BBQ SEASONING", "S-B8SL1", True),
    ]
    assert a.structured and a.form_mode
    assert a.items == []            # nothing left over for the flat list


def test_comment_section_notes_land_on_their_item():
    a = srq.parse_ask(MSG)
    specs = {f["code"]: f["spec"] for f in a.flavours}
    assert specs["S-83EH5-08"] == ["S-83EH5-08 - short listed. Same code same profile repeat sample"]
    assert specs["S-83NJ1-11"] == ["Decrease salt by 20%"]   # code already in the header
    assert specs["S-J2M53-26-07"] == ["S-J2M53-26-07 - Increase seafood taste by 20%"]
    assert specs["S-B8SL1"] == ["S-B8SL1 - Reduce salt by 20%"]
    assert a.ask_text == ""         # no floating comment lines


def test_global_fields_are_untouched():
    a = srq.parse_ask(MSG)
    assert a.customer_text.lower() == "pran foods"
    assert a.qty_text == "each sample 200g no need application"
    assert a.overrides["budget"] == "below <2.2 usd"
    assert a.overrides["compliance"] == "Bangladesh"
    assert a.overrides["attn"] == "Mr Sajib"
    assert a.delivery.lower() == "dhl"
    assert a.base.lower().startswith("corn curl")


def test_rendered_note_mirrors_his_lines_and_puts_qty_in_its_own_block():
    a = srq.parse_ask(MSG)
    note = srq.render_reqnote(_draft(a))
    assert "Seasoning name:\nCHILLI SEASONING\nS-83EH5-08\nROASTED CORN SEASONING S-83NJ1-11\n" in note
    assert "1. CHILLI SEASONING\nS-83EH5-08 - short listed. Same code same profile repeat sample" in note
    assert "2. ROASTED CORN SEASONING S-83NJ1-11\nDecrease salt by 20%" in note
    assert "x 1 set" not in note                       # no qty in Comment
    assert "QTY:\nCHILLI SEASONING- 200g no need application\n" in note
    assert "CORN BBQ SEASONING- 200g no need application" in note
    assert note.count("200g no need application") == 4


def test_other_layouts_do_not_trigger_the_pre_pass():
    # value on the header line (Jordan / Apacific style) → not this form
    blocks, rem = srq._two_section_form(
        ["Seasoning name: 1. Spicy Korean Gochujang", "Comment: corn puff base"])
    assert blocks == [] and len(rem) == 2


def test_llm_addr_that_is_an_attention_line_is_dropped():
    d = srq._ground({"addr": "Attention: Mr Sajib 200g", "attn": "Mr Sajib"}, MSG)
    assert d["addr"] is None
    assert d["attn"] == "Mr Sajib"
