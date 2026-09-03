"""/sr Compliance: ask, don't assume (Alex 03-Sep).

Compliance is a regulatory field. It comes ONLY from what the rep states in
this request - never an old SR's value, never the customer's country, and
not memory (memory was written from every submit, so a guessed value that
slipped through one tap would have been re-proposed forever).
"""
import sample_request as srq
from sample_request import Ask, compliance_for


def test_stated_compliance_is_his():
    a = Ask()
    a.overrides["compliance"] = "Bangladesh"
    assert compliance_for(a) == ("Bangladesh", "you")


def test_mentioned_but_unreadable_asks_to_confirm():
    a = Ask()
    a.hints.add("compliance")
    assert compliance_for(a) == ("", "confirm")


def test_unstated_compliance_is_blank_and_asked_not_guessed():
    # No fallback of any kind - the scoreboard lists it under 'Still missing'.
    assert compliance_for(Ask()) == ("", "")


def test_parsed_compliance_flows_through():
    a = srq.parse_ask("PRAN — bbq seasoning 100g\nCompliance for Bangladesh")
    assert compliance_for(a) == ("Bangladesh", "you")


def test_submit_never_remembers_compliance(monkeypatch):
    calls = []
    monkeypatch.setattr(srq, "mem_set", lambda c, k, v: calls.append((k, v)))
    srq.remember_submitted({"customer": "Quasem", "bag": "NP bag",
                            "budget": "<2usd", "compliance": "Bangladesh",
                            "attn": "Mr Ali", "contact": "", "addr": ""})
    assert ("bag", "NP bag") in calls and ("budget", "<2usd") in calls
    assert ("attn", "Mr Ali") in calls
    assert not any(k == "compliance" for k, _ in calls)
