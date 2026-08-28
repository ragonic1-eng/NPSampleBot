"""Tests for the V1.17.x universal smart text router building blocks.

The router itself needs Telegram + Sheets plumbing; these tests pin the
pure identification helpers that decide WHERE a typed message goes:
  • _route_strip_fillers — "samples for alex" → "alex"
  • _match_rep_names     — exact / first-name / fuzzy rep detection
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "")
os.environ.setdefault("GOOGLE_SHEETS_ID", "")

from bot import _match_rep_names, _route_strip_fillers  # noqa: E402

REPS = ["Alex Tan", "Leo", "William Susanto", "Heidy", "Freddy"]


def test_fillers_stripped_around_names():
    assert _route_strip_fillers("samples for alex") == "alex"
    assert _route_strip_fillers("show me datong") == "datong"
    assert _route_strip_fillers("what did leo send out") == "leo"


def test_filler_only_text_falls_back_to_original():
    # Everything is filler — return the original text rather than "".
    assert _route_strip_fillers("show me samples") == "show me samples"


def test_rep_exact_full_name_is_strong():
    hits, strong = _match_rep_names("alex tan", REPS)
    assert strong is True
    assert hits == ["Alex Tan"]


def test_rep_first_name_is_strong():
    hits, strong = _match_rep_names("alex", REPS)
    assert strong is True
    assert hits == ["Alex Tan"]


def test_rep_case_and_spacing_insensitive():
    hits, strong = _match_rep_names("  ALEX  ", REPS)
    assert strong is True and hits == ["Alex Tan"]


def test_rep_typo_is_weak_hit_not_strong():
    hits, strong = _match_rep_names("willium susanto", REPS)
    assert strong is False
    assert "William Susanto" in hits


def test_product_words_do_not_match_reps():
    for q in ("cheese", "bbq chicken", "masala noodle", "spicy squid"):
        hits, strong = _match_rep_names(q, REPS)
        assert hits == [], f"{q!r} unexpectedly matched {hits}"


def test_short_text_never_matches_reps():
    hits, strong = _match_rep_names("al", REPS)
    assert hits == [] and strong is False


def test_two_char_leo_requires_exact():
    # 'leo' is 3 chars — allowed, exact match.
    hits, strong = _match_rep_names("leo", REPS)
    assert strong is True and hits == ["Leo"]


# ---------- origin line: country + customer on product results (V1.17.x) ----------

from bot import _origin_line  # noqa: E402


def test_origin_line_shows_country_and_customer():
    out = _origin_line("Indonesia", "PT MAKMUR")
    assert "Indonesia" in out and "PT MAKMUR" in out
    assert out.startswith("📍")
    assert "🇮🇩" in out          # known country gets a flag


def test_origin_line_customer_only():
    out = _origin_line("", "ACME")
    assert "ACME" in out
    assert "from" not in out       # no country -> don't say "from"


def test_origin_line_country_only():
    out = _origin_line("Thailand", "")
    assert "Thailand" in out
    assert "🇹🇭" in out


def test_origin_line_empty_when_nothing_known():
    assert _origin_line("", "") == ""
    assert _origin_line(None, None) == ""


def test_origin_line_unknown_country_has_no_flag_but_still_shows():
    out = _origin_line("Narnia", "SOMECO")
    assert "Narnia" in out and "SOMECO" in out


# ---------- AWB price-leak guard (S-B25L4 bug) ----------

from bot import _awb_is_price_leak  # noqa: E402


def test_awb_price_leak_detects_costs_not_tracking_numbers():
    # Raw-material costs wrongly living in col K of the Singapore FSL tab.
    for leak in ("1.9232", "4.3635", "2.57", "123.45", "SGD 4.36",
                 "USD 3.10", "  2.57  "):
        assert _awb_is_price_leak(leak), leak


def test_awb_price_leak_passes_real_awbs():
    # Genuine tracking numbers and markers must NOT be flagged.
    for good in ("1234567890", "JD014600003217", "HAND CARRY", "HC",
                 "", "1Z999AA10123456784"):
        assert not _awb_is_price_leak(good), good


# ---------- transient Sheets outage detection (sales-name silent-fail) ----------

from bot import _is_transient_data_error  # noqa: E402


def test_transient_data_error_flags_google_outages():
    class APIError(Exception):
        pass
    for e in (APIError("[503]: The service is currently unavailable."),
              Exception("[429]: rateLimitExceeded"),
              Exception("500 backendError"),
              Exception("APIError: [502]")):
        assert _is_transient_data_error(e), e


def test_transient_data_error_ignores_real_bugs():
    for e in (ValueError("no such column"), KeyError("Sales"),
              TypeError("bad operand")):
        assert not _is_transient_data_error(e), e


# ---------- rep-name recognition survives a Sheets outage (LKG cache) ----------

import asyncio  # noqa: E402
import bot as _botmod  # noqa: E402


def test_rep_names_last_known_good_survives_outage(monkeypatch):
    import sheets

    good = [
        {"MMS Name": "Alex", "Active": "Y"},
        {"MMS Name": "Leo", "Active": "Y"},
    ]
    monkeypatch.setattr(sheets, "load_users", lambda *a, **k: good)
    monkeypatch.setattr(sheets, "load_fsl_rows_all", lambda *a, **k: [])
    _botmod._REP_NAMES_LKG = []

    # 1) healthy read seeds the cache
    names = asyncio.get_event_loop().run_until_complete(_botmod._active_rep_names([]))
    assert "Alex" in names and "Leo" in names

    # 2) every read now 503s — must still serve the cached names, not []
    def boom(*a, **k):
        raise Exception("APIError: [503]: The service is currently unavailable.")
    monkeypatch.setattr(sheets, "load_users", boom)
    monkeypatch.setattr(sheets, "load_fsl_rows_all", boom)
    errs = []
    degraded = asyncio.get_event_loop().run_until_complete(
        _botmod._active_rep_names(errs)
    )
    assert "Alex" in degraded, "outage wiped rep-name recognition"
    assert errs, "transient error should still be flagged for the router"


# ---------- rep view: small customers are never capped (Ikan Mas bug) ----------

import datetime as _dt2  # noqa: E402


def _mkrow(cust, i, days):
    d = _dt2.date(2026, 8, 2) - _dt2.timedelta(days=days)
    return {"Customer Name": cust, "Product Code": f"S-{i:04d}",
            "Product Name": f"SHRIMP POWDER {i}", "R&D Price": "IDR 242,810",
            "Sample Date Out": d.strftime("%d/%b/%Y"), "_date": d}


def test_small_customer_not_capped_when_sharing_page_with_big_one(monkeypatch):
    """A 7-sample customer must show all 7 and get NO button, even when it
    shares a rep's page with a 90-sample customer."""
    rows = [_mkrow("Ikan Mas (UD)", i, 3 + i) for i in range(7)]
    rows += [_mkrow("Big Co", 500 + i, i) for i in range(90)]

    async def fl(scope, name):
        return rows

    async def pref(u):
        return None

    async def foot():
        return ""

    monkeypatch.setattr(_botmod, "_load_lastsample_rows", fl)
    monkeypatch.setattr(_botmod, "_user_pref_currency", pref)
    monkeypatch.setattr(_botmod, "_last_sync_footer", foot)
    monkeypatch.setattr(_botmod, "_sgt_now", lambda: _dt2.datetime(2026, 8, 2))

    captured = {}

    async def cap_send(update, text, markup=None):
        captured["text"] = text
        captured["btns"] = [
            b.text for row in (markup.inline_keyboard if markup else []) for b in row
        ]

    monkeypatch.setattr(_botmod, "send", cap_send)

    class Chat:
        async def send_action(self, *a):
            pass

    class Upd:
        effective_chat = Chat()
        effective_user = type("U", (), {"id": 1, "username": "a"})()
        effective_message = None

    class Ctx:
        user_data = {}

    asyncio.get_event_loop().run_until_complete(
        _botmod._show_rep_samples(Upd(), Ctx(), "Leo", page=0)
    )
    # Ikan Mas must have NO button (all 7 fit inline) ...
    assert not any("Ikan" in b for b in captured["btns"]), captured["btns"]
    # ... and its 7th sample line must be present.
    assert "7. <b>SHRIMP POWDER 6</b>" in captured["text"]
    # The big customer DOES get a button.
    assert any("Big Co" in b for b in captured["btns"]), captured["btns"]


# ---------- collapse same-day price fluctuation to latest (J-YC381-03) --------

def test_collapse_samples_keeps_latest_price_per_code_date():
    import datetime as d
    day = d.date(2026, 3, 9)
    rows = [
        {"Product Code": "J-YC381-03", "Product Name": "TOASTED ONION",
         "Sample Date Out": "9/Mar/2026", "R&D Price": "IDR 64,558",
         "Ingested At UTC": "2026-05-24 09:41 UTC", "_date": day},
        {"Product Code": "J-YC381-03", "Product Name": "TOASTED ONION",
         "Sample Date Out": "9/Mar/2026", "R&D Price": "IDR 66,250",
         "Ingested At UTC": "2026-05-27 10:01 UTC", "_date": day},   # latest sync
        {"Product Code": "J-YC381-03", "Product Name": "TOASTED ONION",
         "Sample Date Out": "9/Mar/2026", "R&D Price": "IDR 65,000",
         "Ingested At UTC": "2026-05-25 09:41 UTC", "_date": day},
    ]
    out = _botmod._collapse_samples(rows)
    assert len(out) == 1
    assert out[0]["R&D Price"] == "IDR 66,250"   # newest ingested wins
    assert out[0]["_dup_count"] == 3


def test_collapse_keeps_different_dates_and_codes_separate():
    import datetime as d
    rows = [
        {"Product Code": "J-1", "Sample Date Out": "9/Mar/2026",
         "R&D Price": "IDR 1", "Ingested At UTC": "2026-05-01 00:00 UTC",
         "_date": d.date(2026, 3, 9)},
        {"Product Code": "J-1", "Sample Date Out": "10/Mar/2026",
         "R&D Price": "IDR 2", "Ingested At UTC": "2026-05-01 00:00 UTC",
         "_date": d.date(2026, 3, 10)},           # same code, diff day -> separate
        {"Product Code": "J-2", "Sample Date Out": "9/Mar/2026",
         "R&D Price": "IDR 3", "Ingested At UTC": "2026-05-01 00:00 UTC",
         "_date": d.date(2026, 3, 9)},            # diff code -> separate
    ]
    out = _botmod._collapse_samples(rows)
    assert len(out) == 3
    assert all(r["_dup_count"] == 1 for r in out)


def test_collapse_normalises_date_format():
    """'9/Mar' and '09/Mar' are the same day -> collapse together."""
    import datetime as d
    day = d.date(2026, 3, 9)
    rows = [
        {"Product Code": "J-1", "Sample Date Out": "9/Mar/2026", "R&D Price": "IDR 1",
         "Ingested At UTC": "2026-05-01 00:00 UTC", "_date": day},
        {"Product Code": "J-1", "Sample Date Out": "09/Mar/2026", "R&D Price": "IDR 2",
         "Ingested At UTC": "2026-05-02 00:00 UTC", "_date": day},
    ]
    out = _botmod._collapse_samples(rows)
    assert len(out) == 1 and out[0]["_dup_count"] == 2


# ---------- code search collapses same-day snapshots (J-YC381-03 UX) ---------

def test_lastsample_product_filter_collapses_same_day_rows():
    import datetime as d
    day = d.date(2026, 3, 9)
    rows = [
        {"Product Code": "J-YC381-03", "Product Name": "TOASTED ONION POWDER",
         "Customer Name": "Intika (CV)", "Sample Date Out": "9/Mar/2026",
         "R&D Price": f"IDR {65000 + i}", "Ingested At UTC": f"2026-05-{20+i:02d}",
         "_date": day}
        for i in range(5)
    ] + [
        {"Product Code": "J-YC381-03", "Product Name": "TOASTED ONION POWDER",
         "Customer Name": "OTHER CUSTOMER", "Sample Date Out": "9/Mar/2026",
         "R&D Price": "IDR 60,000", "Ingested At UTC": "2026-05-20",
         "_date": day},
    ]
    out = _botmod._filter_lastsample_products(rows, "J-YC381-03")
    # 5 same-customer snapshots -> 1; different customer stays separate.
    assert len(out) == 2
    intika = [r for r in out if r["Customer Name"] == "Intika (CV)"][0]
    assert intika["_dup_count"] == 5
    assert intika["R&D Price"] == "IDR 65004"   # latest-synced snapshot wins


# ---------- 'eric noodle' = noodle PRODUCTS, not noodle-named customers ------

def test_rep_keyword_prefers_product_name_over_customer(monkeypatch):
    import datetime as d

    def mk(cust, code, name):
        return {"Customer Name": cust, "Product Code": code,
                "Product Name": name, "R&D Price": "USD 4.00",
                "Sample Date Out": "1/Jul/2026", "Sales": "Eric",
                "Country": "India", "_date": d.date(2026, 7, 1),
                "Ingested At UTC": "x"}

    rows = [
        mk("Excel Foods", "S-1", "CHICKEN NOODLE SOUP SEASONING"),
        mk("Kwality Noodles Industries", "S-2", "CHEESE SEASONING"),
    ]

    async def fl(scope, name=""):
        return rows

    async def pref(u):
        return None

    monkeypatch.setattr(_botmod, "_load_lastsample_rows", fl)
    monkeypatch.setattr(_botmod, "_user_pref_currency", pref)
    monkeypatch.setattr(_botmod, "_sgt_now", lambda: __import__("datetime").datetime(2026, 8, 2))
    captured = {}

    async def cs(update, text, markup=None):
        captured["text"] = text

    monkeypatch.setattr(_botmod, "send", cs)

    class Chat:
        async def send_action(self, *a):
            pass

    class Upd:
        effective_chat = Chat()
        effective_user = type("U", (), {"id": 1, "username": "a"})()
        effective_message = None
        callback_query = None

    class Ctx:
        user_data = {}

    # 'noodle' matches a product name -> customer-name matches must be excluded
    asyncio.get_event_loop().run_until_complete(
        _botmod._show_rep_samples_filtered(Upd(), Ctx(), "Eric", "noodle")
    )
    assert "CHICKEN NOODLE SOUP" in captured["text"]
    assert "CHEESE SEASONING" not in captured["text"]

    # 'kwality' matches no product -> falls back to customer, labelled so
    asyncio.get_event_loop().run_until_complete(
        _botmod._show_rep_samples_filtered(Upd(), Ctx(), "Eric", "kwality")
    )
    assert "CHEESE SEASONING" in captured["text"]
    assert "customer" in captured["text"]


def test_origin_line_includes_sent_date():
    import datetime as d
    out = _origin_line("China", "FUJIAN ZHAOLU", d.date(2026, 7, 14))
    assert "14 Jul 2026" in out and "📅" in out
    # date-less rows keep the old shape, no dangling separator
    assert "📅" not in _origin_line("China", "FUJIAN ZHAOLU")


# ---------- FSL-only customers are found and route directly (HungHau) --------

def test_fsl_only_customer_routes_direct_despite_weak_product_noise(monkeypatch):
    """'hung hau' must reach '(HungHau Foods Vietnam)' — a customer that
    exists ONLY in the FSL (not the customer master) — and a threshold-noise
    product match (score 60) must not force a disambiguation screen."""
    import datetime as d
    import sheets

    fsl_rows = [
        {"Customer Name": "(HungHau Foods Vietnam)", "Product Code": f"S-{i:03d}",
         "Product Name": "FISH SEASONING", "Sales": "Leo",
         "Sample Date Out": "1/Jul/2026", "R&D Price": "USD 4.00",
         "_date": d.date(2026, 7, 1), "Ingested At UTC": "x"}
        for i in range(5)
    ]
    monkeypatch.setattr(sheets, "load_merged_customers", lambda: [])
    monkeypatch.setattr(sheets, "load_fsl_rows_all", lambda tab=None: fsl_rows)
    # Product probe returns one barely-over-threshold accident, like UNAGI.
    monkeypatch.setattr(
        sheets, "load_seasonings",
        lambda force=False: [{"name": "UNAGI SEASONING", "code": "S-B6TF1",
                              "price": "SGD 6.42", "category": "Snack"}],
    )

    async def reps(errors=None):
        return ["Leo", "Alex"]
    monkeypatch.setattr(_botmod, "_active_rep_names", reps)

    routed = {}

    async def fake_ls(update, ctx, mms_name, query, prev, mode=None,
                      scope=None, **kw):
        routed.update(query=query, mode=mode, scope=scope)
    monkeypatch.setattr(_botmod, "_run_lastsample_search", fake_ls)

    async def fake_send(u, text, markup=None):
        routed.setdefault("sent", []).append(text)
    monkeypatch.setattr(_botmod, "send", fake_send)

    class Chat:
        async def send_action(self, *a):
            pass

    class Upd:
        effective_chat = Chat()
        effective_user = type("U", (), {"id": 1, "username": "a"})()
        effective_message = None
        callback_query = None

    class Ctx:
        user_data = {}

    asyncio.get_event_loop().run_until_complete(
        _botmod._smart_route_text(Upd(), Ctx(), "Hung hau")
    )
    assert routed.get("query") == "(HungHau Foods Vietnam)", routed
    assert routed.get("mode") == "customer" and routed.get("scope") == "all"
