"""/sr: a previous 'Will write into MMS' note pasted back in must round-trip
cleanly. Alex 03-Sep, Abul Khair ('why got repeat??'): NEED BY printed
twice, 'fedex' and 'Comment: Seasoning:' leaked into the comment, and
'Mobile:' / 'Company:' / 'Lemon seasoning- no specific code' were listed as
seasoning names.
"""
import sample_request as srq

PASTED = """Company: Abul khair consumer products ltd
Seasoning name:
Lemon flavor
Chilli seasoning
Lemon seasoning- no specific code
Chilli seasoning- no specific code
Mobile: +880 1912-643856
Comment: Seasoning:
Comment:
NEED BY: BY 11 SEP 2026
fedex
TARGET BASE: Charnachur
BAG: NP BAG
BUDGET: <2usd
COMPLIANCE: Bangladesh
QTY: 100g x 1 set each
Delivery address: D.T. Road, Pahartali, Chittagong 4202
RECEIVER NAME: Mr Hasnat"""


def _draft(a):
    return {"derived": {"qty": a.qty_g or 100, "sets": 1, "rtype": "new",
                        "rtype_label": "New", "base_code": ""},
            "ask": a, "bag": a.overrides.get("bag", ""),
            "budget": a.overrides.get("budget", ""),
            "compliance": a.overrides.get("compliance", ""),
            "need_by": a.overrides.get("need_by", ""),
            "attn": a.overrides.get("attn", ""),
            "contact": a.overrides.get("contact", ""),
            "addr": a.overrides.get("addr", "")}


def test_pasted_note_fields_land_where_they_belong():
    a = srq.parse_ask(PASTED)
    assert a.customer_text == "Abul khair consumer products ltd"
    assert a.items == ["Lemon flavor", "Chilli seasoning"]
    assert a.no_prefer_code
    o = a.overrides
    assert o["contact"] == "+880 1912-643856"
    assert o["need_by"] == "BY 11 SEP 2026"
    assert o["attn"] == "Mr Hasnat"
    assert o["addr"] == "D.T. Road, Pahartali, Chittagong 4202"
    assert o["compliance"] == "Bangladesh" and o["budget"] == "<2usd"
    assert o["bag"] == "NP bag"
    assert a.base == "Charnachur"
    assert a.delivery == "FedEx"
    assert a.qty_g == 100 and a.qty_each


def test_pasted_note_leaves_nothing_in_the_comment():
    a = srq.parse_ask(PASTED)
    low = a.ask_text.lower()
    for leaked in ("need by", "fedex", "comment", "seasoning:", "mobile",
                   "company", "no specific code"):
        assert leaked not in low, (leaked, a.ask_text)


def test_rendered_note_has_no_repeats():
    a = srq.parse_ask(PASTED)
    note = srq.render_reqnote(_draft(a))
    assert note.count("Comment:") == 1
    assert note.count("NEED BY") == 1
    assert note.count("Lemon flavor") == 1
    assert note.count("Chilli seasoning") == 1
    assert "Mobile" not in note and "Company" not in note
    assert note.count("No prefer code.") == 1
    assert note.count("Seasoning name:") == 1


def test_llm_canonical_form_is_also_clean():
    # Production runs parse_ask on the LLM's rebuilt 'customer - ask' text.
    text = ("Abul khair consumer products ltd — Lemon flavor\n"
            + "\n".join(PASTED.splitlines()[2:]))
    a = srq.parse_ask(text)
    assert a.customer_text == "Abul khair consumer products ltd"
    assert a.items == ["Lemon flavor", "Chilli seasoning"]
    assert "need by" not in a.ask_text.lower()
    assert "fedex" not in a.ask_text.lower()


def test_code_note_is_a_note_not_a_second_item():
    a = srq.parse_ask("PRAN — snacks\nSeasoning name:\n"
                      "Lemon seasoning- no specific code")
    assert a.items == ["Lemon seasoning"] and a.no_prefer_code


def test_similar_but_different_items_are_both_kept():
    a = srq.parse_ask("PRAN — snacks\nSeasoning name:\n"
                      "Chilli seasoning\nChilli lime seasoning")
    assert a.items == ["Chilli seasoning", "Chilli lime seasoning"]


def test_bare_carrier_line_is_the_method():
    a = srq.parse_ask("PRAN — bbq seasoning 100g\nfedex")
    assert a.delivery == "FedEx" and "fedex" not in a.ask_text.lower()
    a2 = srq.parse_ask("PRAN — bbq seasoning 100g\n"
                       "Delivery method: Courier\ndhl")
    assert a2.delivery == "Courier"   # an explicit method beats a stray word


def test_need_by_with_colon_is_consumed_once():
    a = srq.parse_ask("PRAN — bbq seasoning 100g\nNEED BY: BY 11 SEP 2026")
    assert a.overrides["need_by"] == "BY 11 SEP 2026"
    assert "need by" not in a.ask_text.lower()


def test_empty_comment_headers_are_scaffolding():
    a = srq.parse_ask("PRAN — bbq seasoning 100g\nComment:\n"
                      "Comment: Seasoning:\nComment: extra hot please")
    assert a.ask_text.strip() == "Comment: extra hot please"


def test_mobile_is_a_contact_and_company_is_the_customer():
    a = srq.parse_ask("Company: PRAN Foods\nSeasoning name:\nBBQ seasoning\n"
                      "Mobile: +880 1700-000000")
    assert a.customer_text == "PRAN Foods"
    assert a.items == ["BBQ seasoning"]
    assert a.overrides["contact"] == "+880 1700-000000"
