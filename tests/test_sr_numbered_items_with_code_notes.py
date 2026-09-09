"""Pran Foods, 09-Sep-2026: a 'Seasoning name:' header, then the rep's own
numbering with a 'S-CODE - note' line under each item, plus global Qty /
Compliance lines. The form splitter had turned every code line into its
own item (6 items for a 3-item list) and 'Compliance:' never matched the
global-field pattern (complian\\b can't match inside 'compliance')."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sample_request as srq  # noqa: E402

MSG = """Pran Foods
Seasoning name:
1. CHILLI SEASONING
S-83EH5-08 -SHORT LISTED. SAME CODE SAME PROFILE REPEAT SAMPLE
ROASTED CORN SEASONING S-83NJ1-11 - Decrease salt by 20%
2. SEAFOOD SEASONING
S-J2M53-26-07 - Increase seafood taste by 20%
3. CORN BBQ SEASONING
S-B8SL1 - REDUCE SALT BY 20%
Qty: each sample 200g no need application
Compliance: Bangladesh
TARGET BASE: corn curl
BAG: NP BAG
BUDGET: below <2.2 usd
Delivery method: DHL
Delivery address: PRAN FOODS LTD, PRAN-RFL CENTRE, 105, MIDDLE BADDA, GPO BOX #83, LEVEL 8, 1212 DHAKA, Bangladesh
RECEIVER NAME: Mr Sajib
CONTACT NO.: +880 1704-158453"""


def test_code_note_lines_attach_to_the_numbered_item_above():
    a = srq.parse_ask(MSG)
    assert [f["name"] for f in a.flavours] == [
        "CHILLI SEASONING", "SEAFOOD SEASONING", "CORN BBQ SEASONING"]
    assert a.flavours[0]["spec"][0].startswith("S-83EH5-08")
    assert a.flavours[1]["spec"] == ["S-J2M53-26-07 - Increase seafood taste by 20%"]
    assert a.flavours[2]["spec"] == ["S-B8SL1 - REDUCE SALT BY 20%"]
    assert a.codes == ["S-83EH5-08", "S-83NJ1-11", "S-J2M53-26-07", "S-B8SL1"]


def test_global_qty_and_compliance_do_not_join_the_last_item():
    a = srq.parse_ask(MSG)
    assert a.qty_text == "each sample 200g no need application"
    assert a.overrides["compliance"] == "Bangladesh"
    assert not any("Qty:" in x or "Compliance" in x
                   for f in a.flavours for x in f["spec"])


def test_code_then_name_line_still_starts_an_item():
    # Apacific-style 'S-CODE NAME' (no dash) is an item, not a note.
    blocks, _ = srq._form_blocks([
        "SEASONING NAME: SAMBAL CHILLI SEASONING",
        "S-K9U15-08 TAKOYAKI SEASONING",
        "S-B5KL2 CHEESE MUSTARD SEASONING",
    ])
    assert [b["name"] for b in blocks] == [
        "SAMBAL CHILLI SEASONING", "S-K9U15-08 TAKOYAKI SEASONING",
        "S-B5KL2 CHEESE MUSTARD SEASONING"]


def test_ground_keeps_a_short_token_value_that_appears_verbatim():
    d = srq._ground({"bag": "NP BAG", "compliance": "Mars"}, MSG)
    assert d["bag"] == "NP BAG"        # 'np' and 'bag' are both < 4 chars
    assert d["compliance"] is None     # never in the message → dropped
