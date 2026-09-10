"""Alex 10-Sep, Liwayway Bangladesh: 'For one last time below should be
the correct output.' The note mirrors his input - Seasoning name as typed
without codes, his Comment section verbatim with a blank line before each
item paragraph, one QTY line in his words, 'Expected send out' as the date."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sample_request as srq  # noqa: E402

MSG = """Customer: liwayway bangladesh

Seasoning name:
BEEF BBQ CONCENTRATE
S-D3J23-02-Y2
Milk seasoning
S-AA7L4-Y2

Comment:
BEEF BBQ CONCENTRATE-
Customer wants to do final trial. So requesting more seasoning.
Milk seasoning- customer wants to run market trial also. Shortlisted seasoning.

Qty: each 400g powder no application needed.

Expected send out: 16 Sept

Attention: Ms Noralyn
Mobile:+880 1958-294624
Address: Holding- 102, Block- B, Jolarpar Kaoultia, Gazipur Sadar, Gazipur- 1703
"""

EXPECTED = """Seasoning name:
BEEF BBQ CONCENTRATE
Milk seasoning

Comment:
BEEF BBQ CONCENTRATE S-D3J23-02-Y2 -
Customer wants to do final trial. So requesting more seasoning.

Milk seasoning S-AA7L4-Y2 -
customer wants to run market trial also. Shortlisted seasoning.

TARGET BASE: Corn puff
BAG: NP BAG
COMPLIANCE: Bangladesh
QTY: each 400g powder no application needed
NEED BY: 16 SEPT

Delivery method: DHL
Delivery address: Holding- 102, Block- B, Jolarpar Kaoultia, Gazipur Sadar, Gazipur- 1703

RECEIVER NAME: Ms Noralyn
CONTACT NO.: +880 1958-294624"""


def _draft(a):
    # what the bot fills after he answers the two starred questions
    a.base = "Corn puff"
    a.delivery = "DHL"
    return {"derived": {"qty": 400, "sets": 1, "rtype": "new",
                        "rtype_label": "New", "base_code": ""},
            "ask": a, "bag": "NP bag", "budget": "",
            "compliance": "Bangladesh",
            "need_by": a.overrides.get("need_by", "").upper(),
            "attn": a.overrides.get("attn", ""),
            "contact": a.overrides.get("contact", ""),
            "addr": a.overrides.get("addr", "")}


def test_parse_two_items_comment_verbatim_and_fields():
    a = srq.parse_ask(MSG)
    assert a.customer_text.lower() == "liwayway bangladesh"
    assert [f["name"] for f in a.flavours] == ["BEEF BBQ CONCENTRATE", "Milk seasoning"]
    assert a.codes == ["S-D3J23-02-Y2", "S-AA7L4-Y2"]
    assert a.comment_verbatim == [
        "BEEF BBQ CONCENTRATE-",
        "Customer wants to do final trial. So requesting more seasoning.",
        "Milk seasoning- customer wants to run market trial also. Shortlisted seasoning.",
    ]
    assert a.qty_text == "each 400g powder no application needed"
    assert a.overrides["need_by"] == "16 Sept"
    assert a.overrides["attn"] == "Ms Noralyn"
    assert a.overrides["contact"] == "+880 1958-294624"
    assert a.overrides["addr"].startswith("Holding- 102")
    assert a.ask_text == ""


def test_note_matches_alex_expected_output_exactly():
    a = srq.parse_ask(MSG)
    assert srq.render_reqnote(_draft(a)) == EXPECTED


def test_country_in_his_words_never_silently_matches_another_country():
    assert srq._country_tokens("liwayway bangladesh") == {"Bangladesh"}
    assert srq._country_tokens("Myanmar Liwayway Food Industries Ltd") == {"Myanmar"}
    assert not (srq._country_tokens("liwayway bangladesh")
                & srq._country_tokens("Myanmar Liwayway Food Industries Ltd"))


def test_customer_wants_sentence_is_not_a_customer_label():
    assert not srq._GLOBAL_FIELD_RE.match("Customer wants to do final trial.")
    assert srq._GLOBAL_FIELD_RE.match("Customer: liwayway bangladesh")
    assert srq._GLOBAL_FIELD_RE.match("Customer name: Apacific")


def test_comment_header_carries_the_code_and_the_note_starts_below():
    """Alex 10-Sep: "only comment is required to include the product code"."""
    a = srq.parse_ask(MSG)
    body = srq.render_reqnote(_draft(a)).split("Comment:")[1].split("TARGET BASE")[0]
    assert "BEEF BBQ CONCENTRATE S-D3J23-02-Y2 -" in body
    assert "Milk seasoning S-AA7L4-Y2 -" in body
    # the note is never left on the header line
    assert "S-AA7L4-Y2 - customer wants" not in body
    # and the codes stay OUT of the Seasoning name section
    head = srq.render_reqnote(_draft(a)).split("Comment:")[0]
    assert "S-D3J23-02-Y2" not in head and "S-AA7L4-Y2" not in head
