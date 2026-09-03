"""Regression tests for the /sr fixes of 03-Sep (Alex's Quasem request).

Three failures on one draft:
  * the card said 'Still missing: Target base - just tell me', he said
    'potato chip', and the bot ran a product search instead;
  * Compliance was proposed from Quasem's previous SR with no way to take
    it back (and an empty value from the editor was silently ignored, so
    even 'no compliance' could never have cleared it);
  * Target base wasn't in the editor's vocabulary at all after the build.
"""
from sample_request import Ask, answer_gap, apply_fields, fallback_update


def _draft(**over):
    d = {
        "ask": Ask(ask_text="naga seasoning"),
        "derived": {"qty": 100, "sets": 1, "rtype": "new", "base_code": "",
                    "qty_src": ""},
        "bag": "NP bag", "budget": "<2usd", "compliance": "for Bangladesh",
        "attn": "Mr Monshin Ali", "contact": "+880 1713-096222",
        "addr": "Quasem Food Products Limited, Dhaka-1212",
        "need_by": "BY 11 SEP 2026", "assignee": "Jessie",
        "src": {"compliance": "their last request"},
        "fields": {"Target base": ""}, "gaps": ["Target base"], "missing": [],
    }
    d.update(over)
    return d


# ---------------------------------------------------- bare reply -> open slot

def test_bare_reply_fills_the_one_open_slot():
    d = _draft()
    assert answer_gap(d, "potato chip") == "Target base"
    assert d["ask"].base == "potato chip"
    assert d["src"]["base"] == "you"
    assert d["fields"]["Target base"] == "you"


def test_bare_reply_is_not_an_answer_when_two_slots_are_open():
    d = _draft(gaps=["Target base", "Budget"])
    assert answer_gap(d, "potato chip") == ""
    assert d["ask"].base == ""


def test_confirm_code_and_keyword_replies_are_not_answers():
    for reply in ("raise it", "S-17AS42-002", "budget <3 usd", "cancel",
                  "no compliance"):
        d = _draft()
        assert answer_gap(d, reply) == "", reply
        assert d["ask"].base == ""


def test_other_scalar_slots_take_a_bare_answer_too():
    d = _draft(gaps=["Receiver name"], attn="")
    assert answer_gap(d, "Mr Monshin Ali") == "Receiver name"
    assert d["attn"] == "Mr Monshin Ali" and d["src"]["attn"] == "you"


# ------------------------------------------------------- removing a proposal

def test_typed_removal_clears_a_proposed_field_and_pins_it():
    d = _draft()
    upd = fallback_update(d, "no compliance")
    assert upd["action"] == "modify" and upd["clear"] == ["compliance"]
    apply_fields(d, upd["fields"], clear=upd["clear"])
    assert d["compliance"] == ""
    assert d["src"]["compliance"] == "you"   # his word - never re-derived


def test_clear_synonyms_map_to_canonical_keys():
    d = _draft()
    assert fallback_update(d, "remove the address")["clear"] == ["addr"]
    assert fallback_update(d, "drop the deadline")["clear"] == ["need_by"]
    assert fallback_update(d, "without target base")["clear"] == ["base"]
    assert fallback_update(d, "Clear the phone.")["clear"] == ["contact"]


def test_no_budget_stays_a_value_not_a_removal():
    # 'BUDGET: no budget' is something R&D reads - only remove/drop clears.
    d = _draft()
    assert "clear" not in fallback_update(d, "no budget")
    assert fallback_update(d, "remove the budget")["clear"] == ["budget"]


def test_empty_editor_values_never_wipe_but_clear_does():
    d = _draft()
    apply_fields(d, {"compliance": "", "budget": None})   # LLM 'unchanged'
    assert d["compliance"] == "for Bangladesh" and d["budget"] == "<2usd"
    apply_fields(d, {}, clear=["budget"])
    assert d["budget"] == "" and d["src"]["budget"] == "you"


# ---------------------------------------------------- target base editable

def test_target_base_keyword_reply_is_read_and_applied():
    d = _draft()
    upd = fallback_update(d, "target base: potato chip")
    assert upd["fields"]["base"] == "potato chip"
    apply_fields(d, upd["fields"])
    assert d["ask"].base == "potato chip" and d["src"]["base"] == "you"


def test_based_on_prose_does_not_hijack_target_base():
    d = _draft()
    assert "base" not in fallback_update(d, "based on last time")["fields"]
