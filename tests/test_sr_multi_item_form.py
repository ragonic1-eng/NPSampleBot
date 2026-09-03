"""/sr multi-item form (Alex 03-Sep, Apacific): 'when it comes to multiple
seasoning sample request the output become very inconsistent'.

His own layout - repeated SEASONING NAME: / COMMENT: / '50G ...' blocks,
code-led items, NEW SAMPLE markers - must become one clean numbered item
per block, each with its own comment and quantity; and 'Lala move to
Geylang' is a delivery TO Geylang, so nothing from the customer's last
Philippines request may be pasted in as receiver/contact/address.
"""
import sample_request as srq

APACIFIC = """SEASONING NAME: SAMBAL CHILLI SEASONING
COMMENT: : MODIFY S-B5KL3 SAMBAL CHILLI SEASONING, REMOVE CHILLI AND HEAT
BUDGET: < 2.5USD
50G SEASONING WITH NO APPLIED SAMPLES


SEASONING NAME: S-K9U15-08 TAKOYAKI SEASONING
COMMENT: CHECK PHILIPPINES RA
100G SEASONING WITH NO APPLIED SAMPLES

NEW SAMPLE
SEASONING NAME: TAIWAN SAUSAGE SEASONING
COMMENT: : TAKE FROM LIBRARY IF HAVE
100G SEASONING WITH NO APPLIED SAMPLES

S-T4VG1-09 CUCUMBER SEASONING (CHECK PH RA)
50 GRAMS AND NO APPLICATIONS
S-B5KL2 CHEESE MUSTARD SEASONING
50 GRAMS AND NO APPLICATIONS
S-B5KL1 ONION MUSTARD SEASONING
50 GRAMS AND NO APPLICATIONS
NO PH CODE – PENDING FOR REGENT SALTED EGG CODE.
SALTED EGG SEASONING

Customer: Apacific
Delivery method: Lala move to Geylang




NEW SAMPLE
SEASONING NAME:
AMERICAN DORITOS CORIANDER FLAVOR CORN CHIPS
COMMENT: : TAKE FROM LIBRARY IF HAVE

50G SEASONING WITH NO APPLIED SAMPLE"""

NAMES = [
    "SAMBAL CHILLI SEASONING",
    "S-K9U15-08 TAKOYAKI SEASONING",
    "TAIWAN SAUSAGE SEASONING",
    "S-T4VG1-09 CUCUMBER SEASONING (CHECK PH RA)",
    "S-B5KL2 CHEESE MUSTARD SEASONING",
    "S-B5KL1 ONION MUSTARD SEASONING",
    "SALTED EGG SEASONING",
    "AMERICAN DORITOS CORIANDER FLAVOR CORN CHIPS",
]


def _draft(a):
    return {"derived": {"qty": a.qty_g or 50, "sets": 1, "rtype": "mod",
                        "rtype_label": "Modify", "base_code": "S-B5KL3"},
            "ask": a, "bag": "NP bag", "budget": a.overrides.get("budget", ""),
            "compliance": "", "need_by": "", "attn": "", "contact": "",
            "addr": a.delivery_addr}


def test_every_block_becomes_one_item_in_order():
    a = srq.parse_ask(APACIFIC)
    assert a.form_mode and a.structured
    assert [f["name"] for f in a.flavours] == NAMES
    assert a.items == []            # items ARE the blocks, nothing doubled


def test_each_item_keeps_its_own_comment_and_quantity():
    a = srq.parse_ask(APACIFIC)
    f = {b["name"]: b for b in a.flavours}
    assert f["SAMBAL CHILLI SEASONING"]["qty"] == "50g"
    assert "MODIFY S-B5KL3 SAMBAL CHILLI SEASONING, REMOVE CHILLI AND HEAT" \
        in f["SAMBAL CHILLI SEASONING"]["spec"]
    assert f["S-K9U15-08 TAKOYAKI SEASONING"]["qty"] == "100g"
    assert "CHECK PHILIPPINES RA" in f["S-K9U15-08 TAKOYAKI SEASONING"]["spec"]
    assert f["TAIWAN SAUSAGE SEASONING"]["qty"] == "100g"
    assert f["TAIWAN SAUSAGE SEASONING"]["spec"][0] == "NEW SAMPLE"
    assert f["S-B5KL1 ONION MUSTARD SEASONING"]["qty"] == "50g"
    assert f["S-B5KL1 ONION MUSTARD SEASONING"]["spec"] == ["NO APPLICATIONS"]
    # the PH-code note names the salted egg item -> travels with it
    assert f["SALTED EGG SEASONING"]["spec"] == [
        "NO PH CODE – PENDING FOR REGENT SALTED EGG CODE."]
    assert f["AMERICAN DORITOS CORIANDER FLAVOR CORN CHIPS"]["qty"] == "50g"


def test_global_fields_still_land_and_nothing_leaks_into_the_comment():
    a = srq.parse_ask(APACIFIC)
    assert a.customer_text == "Apacific"
    assert a.overrides["budget"] == "< 2.5USD"
    assert a.delivery == "Lala move" and a.delivery_addr == "Geylang"
    low = a.ask_text.lower()
    for leaked in ("seasoning name", "comment", "new sample", "budget",
                   "customer:", "delivery"):
        assert leaked not in low, (leaked, a.ask_text)


def test_rendered_note_is_one_numbered_item_per_block():
    a = srq.parse_ask(APACIFIC)
    note = srq.render_reqnote(_draft(a))
    assert note.count("Seasoning name:") == 1
    assert note.count("Comment:") == 1
    assert "1. SAMBAL CHILLI SEASONING - 50g x 1 set" in note
    assert "2. S-K9U15-08 TAKOYAKI SEASONING - 100g x 1 set" in note
    assert "7. SALTED EGG SEASONING - 50g x 1 set" in note   # request default
    assert "8. AMERICAN DORITOS CORIANDER FLAVOR CORN CHIPS - 50g x 1 set" in note
    assert "QTY:" not in note                       # headers carry it
    assert "SEASONING NAME:" not in note and "NEW SAMPLE\nSEASONING" not in note
    assert note.count("TAKE FROM LIBRARY IF HAVE") == 2
    assert "Delivery method: Lala move" in note
    assert "Delivery address: Geylang" in note
    assert "CAVITE" not in note


def test_llm_canonical_head_line_is_split_off():
    # Production feeds 'CUSTOMER - <first ask line>' as line 0.
    text = "Apacific — " + APACIFIC
    a = srq.parse_ask(text)
    assert a.customer_text == "Apacific"
    assert [f["name"] for f in a.flavours] == NAMES


def test_numbered_only_lists_are_left_to_structure_body():
    a = srq.parse_ask("""acme - two flavours
1. BBQ Seasoning - smoky, sweet
2. Sour Cream Seasoning
tangy, creamy
200g each""")
    assert a.structured and not a.form_mode
    assert [f["name"] for f in a.flavours] == ["BBQ Seasoning",
                                               "Sour Cream Seasoning"]


def test_single_header_is_not_the_form():
    a = srq.parse_ask("PRAN — snacks\nSeasoning name: BBQ seasoning\n"
                      "Comment: extra smoky")
    assert not a.form_mode


def test_lalamove_to_place_is_a_destination_not_a_method_blob():
    a = srq.parse_ask("Apacific — bbq seasoning 100g\n"
                      "Delivery method: Lala move to Geylang")
    assert a.delivery == "Lala move" and a.delivery_addr == "Geylang"


def test_receiver_from_history_must_look_like_a_person():
    assert srq._looks_like_person("Mr Hasnat")
    assert srq._looks_like_person("MR SAJIB")
    assert not srq._looks_like_person(
        "ached the ingredients list for the Sweet Corn seasoning that must "
        "be followed. Ingredients listed must be present on the sample")
    assert srq._person("") == ""
