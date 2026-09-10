"""A customer that already has an SR in MMS (created through the bot as a
temporary-name customer, or raised there by hand) must be recognised
before the bot asks 'existing or new?' (Alex 10-Sep: liwayway bangladesh,
S-19AS43-001, was asked to be created again)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sample_request as srq  # noqa: E402

TEMP_PAGE = """<html><body>Sample Request S-19AS43-001 (20649) 10/Sep/2026
<b>Customer ID</b> (liwayway bangladesh) <b>Customer List</b> J-C1102 : CTS Group</body></html>"""
REG_PAGE = """<html>Sample Request S-18CS43-002 (1) 01/Sep/2026 Customer ID S-UDP041 Pran Foods Ltd Customer List J-C1 : X</html>"""


def test_sr_customer_label_reads_temporary_and_registered_names():
    assert srq._sr_customer_label(TEMP_PAGE) == "liwayway bangladesh"
    assert srq._sr_customer_label(REG_PAGE) == "Pran Foods Ltd"


class _FakeWriter:
    def __init__(self, found, page):
        self._found, self._page = found, page

    def login(self):
        return True

    def find_sr_by_name(self, name):
        return self._found

    def get_page(self, code):
        return self._page


def test_known_in_mms_memory_first(monkeypatch):
    monkeypatch.setattr(srq, "mem_get", lambda c, k: "S-19AS43-001" if k == "sr_code" else "")
    monkeypatch.setattr(srq, "SRWriter", lambda: (_ for _ in ()).throw(AssertionError("MMS must not be touched")))
    assert srq._known_in_mms("liwayway bangladesh") == ("liwayway bangladesh", "S-19AS43-001", None)


def test_known_in_mms_exact_name_from_sr_list(monkeypatch):
    saved = {}
    monkeypatch.setattr(srq, "mem_get", lambda c, k: "")
    monkeypatch.setattr(srq, "mem_set", lambda c, k, v: saved.setdefault((c, k), v))
    fw = _FakeWriter("S-19AS43-001", TEMP_PAGE)
    monkeypatch.setattr(srq, "SRWriter", lambda: fw)
    label, code, w = srq._known_in_mms("Liwayway Bangladesh")
    assert (label, code) == ("liwayway bangladesh", "S-19AS43-001") and w is fw
    assert saved[("Liwayway Bangladesh", "sr_code")] == "S-19AS43-001"


def test_known_in_mms_partial_name_still_asks(monkeypatch):
    monkeypatch.setattr(srq, "mem_get", lambda c, k: "")
    monkeypatch.setattr(srq, "SRWriter", lambda: _FakeWriter("J-199J45-004", REG_PAGE.replace("Pran Foods Ltd", "Liwayway PT")))
    assert srq._known_in_mms("liwayway") is None
