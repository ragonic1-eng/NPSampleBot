"""Alex's two-section form (Pran Foods, 09-Sep-2026): a SEASONING NAME
section listing items (name, then code on the next line, or 'NAME CODE'
on one line) and a COMMENT section repeating each item with ' - note'
after its code. The bot had merged the two sections into one item list
and left the first two notes floating.

Rendering (Alex 10-Sep, Liwayway: "the bot is suppose to copy the
generated text"): Seasoning name as typed without codes, the Comment
section verbatim with a blank line before each item paragraph, one QTY
line in his words when he gave one figure for all."""
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
    return {"derived": {"qty": a.qty_g or 200, "sets": 1, "rtype": "new",
                        "rtype_label": "New", "base_code": ""},
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


def test_comment_section_is_kept_verbatim_and_notes_land_on_their_item():
    a = srq.parse_ask(MSG)
    assert a.comment_verbatim == [
        "CHILLI SEASONING",
        "S-83EH5-08 -short listed. Same code same profile repeat sample",
        "ROASTED CORN SEASONING S-83NJ1-11- Decrease salt by 20%",
        "SEAFOOD SEASONING",
        "S-J2M53-26-07- Increase seafood taste by 20%",
        "CORN BBQ SEASONING",
        "S-B8SL1 - Reduce salt by 20%",
    ]
    specs = {f["code"]: f["spec"] for f in a.flavours}
    assert specs["S-83EH5-08"] == ["S-83EH5-08 - short listed. Same code same profile repeat sample"]
    assert specs["S-83NJ1-11"] == ["Decrease salt by 20%"]
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


def test_rendered_note_copies_his_sections():
    a = srq.parse_ask(MSG)
    note = srq.render_reqnote(_draft(a))
    head, rest = note.split("Comment:")
    assert head == "Seasoning name:\nCHILLI SEASONING\nROASTED CORN SEASONING\nSEAFOOD SEASONING\nCORN BBQ SEASONING\n\n"
    comment = rest.split("TARGET BASE")[0]
    assert comment == ("\nCHILLI SEASONING\nS-83EH5-08 -short listed. Same code same profile repeat sample\n\n"
                       "ROASTED CORN SEASONING S-83NJ1-11- Decrease salt by 20%\n\n"
                       "SEAFOOD SEASONING\nS-J2M53-26-07- Increase seafood taste by 20%\n\n"
                       "CORN BBQ SEASONING\nS-B8SL1 - Reduce salt by 20%\n\n")
    assert "1. CHILLI" not in note and "x 1 set" not in note
    assert "QTY: each sample 200g no need application\n" in note
    assert note.count("200g no need application") == 1


def test_other_layouts_do_not_trigger_the_pre_pass():
    # value on the header line (Jordan / Apacific style) → not this form
    blocks, rem = srq._two_section_form(
        ["Seasoning name: 1. Spicy Korean Gochujang", "Comment: corn puff base"])
    assert blocks == [] and len(rem) == 2


def test_llm_addr_that_is_an_attention_line_is_dropped():
    d = srq._ground({"addr": "Attention: Mr Sajib 200g", "attn": "Mr Sajib"}, MSG)
    assert d["addr"] is None
    assert d["attn"] == "Mr Sajib"
