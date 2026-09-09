"""Alex's two-section form (Pran Foods, 09-Sep-2026): a SEASONING NAME
section listing items (name, then code on the next line, or 'NAME CODE'
on one line) and a COMMENT section repeating each item with ' - note'
after its code. The bot had merged the two sections into one item list
and left the first two notes floating."""
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


def test_names_section_gives_four_items_with_codes():
    a = srq.parse_ask(MSG)
    assert [f["name"] for f in a.flavours] == [
        "CHILLI SEASONING S-83EH5-08",
        "ROASTED CORN SEASONING S-83NJ1-11",
        "SEAFOOD SEASONING S-J2M53-26-07",
        "CORN BBQ SEASONING S-B8SL1",
    ]
    assert a.structured and a.form_mode
    assert a.items == []            # nothing left over for the flat list


def test_comment_section_notes_land_on_their_item():
    a = srq.parse_ask(MSG)
    specs = {f["code"]: f["spec"] for f in a.flavours}
    assert specs["S-83EH5-08"] == ["short listed. Same code same profile repeat sample"]
    assert specs["S-83NJ1-11"] == ["Decrease salt by 20%"]
    assert specs["S-J2M53-26-07"] == ["Increase seafood taste by 20%"]
    assert specs["S-B8SL1"] == ["Reduce salt by 20%"]
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


def test_other_layouts_do_not_trigger_the_pre_pass():
    # value on the header line (Jordan / Apacific style) → not this form
    blocks, rem = srq._two_section_form(
        ["Seasoning name: 1. Spicy Korean Gochujang", "Comment: corn puff base"])
    assert blocks == [] and len(rem) == 2


def test_llm_addr_that_is_an_attention_line_is_dropped():
    d = srq._ground({"addr": "Attention: Mr Sajib 200g", "attn": "Mr Sajib"}, MSG)
    assert d["addr"] is None
    assert d["attn"] == "Mr Sajib"
