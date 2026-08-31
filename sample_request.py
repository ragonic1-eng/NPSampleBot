"""Sample-request raising bot — v1, Alex-only (2026-08-31 build go-ahead).

One Telegram message ("haritage — mala crawfish 200g") becomes a complete
DRAFT sample request with every derived value shown with its provenance;
nothing touches MMS until Alex taps confirm. Design, evidence and the
derivability analysis live in the research artifact
(claude.ai/code/artifact/3654b826-7074-4f1f-850e-5a15f2fceb88).

Hard rules encoded here:
  * The request-type radio (sreq1[N].rtype new/rep/mod) is ALWAYS set
    from the ask — never left on a default (49% of historical requests
    were mistyped "New").
  * The assignee (sreq1[N].nextActUserId) is ALWAYS set — the MMS
    default is blank, which parks the request in NO ONE's queue.
    Territory rule per Alex: S→Jessie, B→Ple, J→(Iqlima default,
    split territory — confirm with Alex). Assignee user-ids are
    resolved from the live page's own <option> list at submit time,
    never hardcoded.
  * MMS has ZERO client/server-side validation we can rely on, so every
    write step is verified by re-reading the page; a failed step after
    additem triggers command=clear (which only removes EMPTY items) so
    nothing half-written is left behind.
  * Multi-user later is additive: everything is keyed by the requesting
    Telegram user; only the /sr gate is Alex-only today.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
import re
import secrets
import statistics
from collections import Counter
from dataclasses import dataclass, field

import requests
from bs4 import BeautifulSoup

import config
import matcher
import mms_client
import sheets
from mms_product import _form_payload, _override

log = logging.getLogger("npsamplebot.sr")

# ---------------------------------------------------------------- constants

# Territory front doors. v1 scope (Alex, 31 Aug 2026): "ignore thailand and
# Indonesia sample requesting ill use it for SG only" — so ONLY Singapore is
# active; a J/B/C-prefix SR is refused outright rather than guessed at (a
# wrong assignee parks the request in the wrong queue, the exact failure
# this bot exists to fix). To switch a market on later, add its entry here:
#   "B": "Ple"     (Alex: "thailand is to ple"; data: Boong compounds)
#   "J": ?          (split territory — Iqlima/Takenori/Rafly by rep; was
#                    never confirmed, which is why it stays off)
# His own history says the cost is nil: all 32 of Alex's SRs in the last
# ~180 days (356 submission rows) are S-prefix Singapore.
TERRITORY_ASSIGNEE = {"S": "Jessie"}

_CODE_RE = re.compile(
    r"\b[SJTB]-[A-Za-z0-9]{3,}(?:-[A-Za-z0-9]{1,6}){0,6}\b", re.IGNORECASE
)
_QTY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(kg|g)\b", re.IGNORECASE)
_SETS_RE = re.compile(r"\b[xX]\s*(\d+)\b|\b(\d+)\s*sets?\b", re.IGNORECASE)
_MOD_RE = re.compile(
    r"\bmodif|\bchange\b|\badjust|\bimprove|\breduce\b|\bincrease\b|"
    r"\bless\b|\bmore\b|\bthicker|\bthinner|\bcloser to\b", re.IGNORECASE
)

MEMORY_TAB = "SR Bot Memory"
MEMORY_HEADER = ["Customer", "Key", "Value", "Updated"]

DRAFTS: dict[str, dict] = {}  # token -> draft (in-memory; single-replica v1)

SUBMISSION_TABS = ("Submissions SG", "Submissions Jakarta", "Submissions Thailand")


# ------------------------------------------------------------- prefs memory

def _mem_ws():
    sh = sheets._open_ops()  # noqa: SLF001 — same internal reuse as sibling_api
    try:
        ws = sh.worksheet(MEMORY_TAB)
    except Exception:  # noqa: BLE001 — WorksheetNotFound (gspread version safe)
        ws = sh.add_worksheet(title=MEMORY_TAB, rows=500, cols=4)
        ws.update(values=[MEMORY_HEADER], range_name="A1")
    return ws

def mem_get(customer: str, key: str) -> str:
    try:
        for row in _mem_ws().get_all_values()[1:]:
            if len(row) >= 3 and row[0].strip().lower() == customer.strip().lower() \
                    and row[1].strip() == key:
                return row[2].strip()
    except Exception as e:  # noqa: BLE001
        log.warning("SR mem_get(%s,%s) failed: %s", customer, key, e)
    return ""

def mem_set(customer: str, key: str, value: str) -> None:
    try:
        ws = _mem_ws()
        rows = ws.get_all_values()
        now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        for i, row in enumerate(rows[1:], start=2):
            if len(row) >= 2 and row[0].strip().lower() == customer.strip().lower() \
                    and row[1].strip() == key:
                ws.update(values=[[customer, key, value, now]],
                          range_name=f"A{i}:D{i}")
                return
        ws.append_row([customer, key, value, now])
    except Exception as e:  # noqa: BLE001
        log.warning("SR mem_set(%s,%s) failed: %s", customer, key, e)


# ------------------------------------------------------------------ parsing

@dataclass
class Ask:
    customer_text: str = ""
    ask_text: str = ""
    codes: list[str] = field(default_factory=list)
    qty_g: int | None = None
    sets: int | None = None
    overrides: dict[str, str] = field(default_factory=dict)

_OVR_KEYS = {"bag", "budget", "compliance", "attn", "addr", "address",
             "contact", "qty", "sets", "assignee", "type", "base"}

def parse_ask(text: str) -> Ask:
    """'haritage — mala crawfish 200g; bag: empty; budget: usd 4-5'"""
    a = Ask()
    parts = [p.strip() for p in text.split(";")]
    head = parts[0]
    for seg in parts[1:]:
        if ":" in seg:
            k, v = seg.split(":", 1)
            k = k.strip().lower()
            if k in _OVR_KEYS:
                a.overrides["addr" if k == "address" else k] = v.strip()
    # customer — ask split: em-dash, ' - ', or first comma
    for sep in ("—", " - ", "–"):
        if sep in head:
            a.customer_text, a.ask_text = [s.strip() for s in head.split(sep, 1)]
            break
    else:
        if "," in head:
            a.customer_text, a.ask_text = [s.strip() for s in head.split(",", 1)]
        else:
            words = head.split()
            a.customer_text, a.ask_text = " ".join(words[:2]), " ".join(words[2:])
    a.codes = [c.upper() for c in _CODE_RE.findall(a.ask_text)]
    m = _QTY_RE.search(a.ask_text)
    if m:
        n = float(m.group(1))
        a.qty_g = int(n * 1000) if m.group(2).lower() == "kg" else int(n)
    m = _SETS_RE.search(a.ask_text)
    if m:
        a.sets = int(m.group(1) or m.group(2))
    return a


# ----------------------------------------------------------- customer + SR

def resolve_customer(q: str) -> tuple[dict | None, list[dict]]:
    """Best customer + runner-up candidates from master + FSL names."""
    merged = sheets.load_merged_customers()
    hits = matcher.top_customer_master(q, merged, limit=3)
    qsq = re.sub(r"[^a-z0-9]", "", q.lower())
    fsl_names: Counter = Counter()
    for tab in (sheets.FSL_TAB, sheets.JAKARTA_FSL_TAB, sheets.BANGKOK_FSL_TAB):
        try:
            for r in sheets.load_fsl_rows_all(tab):
                c = (r.get("Customer Name") or "").strip()
                if c:
                    fsl_names[c] += 1
        except Exception as e:  # noqa: BLE001
            log.warning("SR resolve_customer FSL read failed: %s", e)
    if len(qsq) >= 4:
        seen = {h["name"].strip().lower() for h in hits}
        for name, n in fsl_names.most_common():
            nsq = re.sub(r"[^a-z0-9]", "", name.lower())
            if qsq in nsq and (len(qsq) / max(len(nsq), 1) >= 0.3 or len(qsq) >= 8):
                if name.strip().lower() not in seen:
                    hits.append({"name": name, "score": 95, "code": ""})
    hits.sort(key=lambda x: -(x.get("score") or 0))
    if not hits:
        return None, []
    strong = hits[0].get("score", 0) >= 90
    ambiguous = len(hits) > 1 and hits[1].get("score", 0) >= 90
    return (hits[0] if strong and not ambiguous else None), hits[:3]

def _sr_series_key(code: str) -> tuple:
    """Sortable (series, seq) from an SR code: SR series numbers increase
    over time (S-11.. 2024 → S-18.. 2026), which the live MMS list confirms
    (it ordered Pran's S-18CS43-002 above S-16PS43-003). More reliable than
    the Submissions tab's request DATE, which reflects synced product rows,
    not the SR itself."""
    m = re.match(r"^[SJBC]-(\d+)[A-Z]*\d*[A-Z]*-?(\d*)", code.upper())
    if not m:
        return (0, 0)
    return (int(m.group(1)), int(m.group(2) or 0))


def find_sr_code(customer: str) -> str:
    """Newest SR code for this customer from the Submissions tabs."""
    codes: set[str] = set()
    target = re.sub(r"[^a-z0-9]", "", customer.lower())
    sh = sheets._open_seasoning_master()  # noqa: SLF001
    for tab in SUBMISSION_TABS:
        try:
            data = sh.worksheet(tab).get_all_values()
        except Exception as e:  # noqa: BLE001
            log.warning("SR find_sr_code(%s) failed: %s", tab, e)
            continue
        if not data:
            continue
        idx = {n: i for i, n in enumerate(data[0])}
        for row in data[1:]:
            cust = re.sub(r"[^a-z0-9]", "",
                          row[idx.get("Customer Name", 4)].lower())
            if not cust or (target not in cust and cust not in target):
                continue
            code = row[idx.get("Sample Request Code", 1)].strip()
            # sheet renders 'S-18CS43-002 (3)' — strip the item marker
            code = re.sub(r"\s*\(\d+\)\s*$", "", code)
            if code:
                codes.add(code)
    if not codes:
        return ""
    return max(codes, key=_sr_series_key)

def customer_history(customer: str) -> list[dict]:
    target = re.sub(r"[^a-z0-9]", "", customer.lower())
    out = []
    for tab in (sheets.FSL_TAB, sheets.JAKARTA_FSL_TAB, sheets.BANGKOK_FSL_TAB):
        try:
            for r in sheets.load_fsl_rows_all(tab):
                c = re.sub(r"[^a-z0-9]", "",
                           (r.get("Customer Name") or "").lower())
                if c and (target in c or c in target):
                    out.append(r)
        except Exception as e:  # noqa: BLE001
            log.warning("SR customer_history(%s) failed: %s", tab, e)
    return out


# -------------------------------------------------------------- derivation

def derive_defaults(hist: list[dict], ask: Ask) -> dict:
    d: dict = {}
    # quantity: explicit > customer mode > 100 g
    if ask.qty_g:
        d["qty"], d["qty_src"] = ask.qty_g, "you specified"
    else:
        qs = [q for q in (str(r.get("Quantity (g)") or "").strip() for r in hist)
              if q.isdigit()]
        if qs:
            mode, n = Counter(qs).most_common(1)[0]
            d["qty"], d["qty_src"] = int(mode), f"their usual ({n}× before)"
        else:
            d["qty"], d["qty_src"] = 100, "house default"
    d["sets"] = ask.sets or 1
    # budget band from realised USD prices
    prices = []
    for r in hist:
        m = re.match(r"^USD\s*([\d.,]+)", str(r.get("R&D Price") or ""))
        if m:
            try:
                p = float(m.group(1).replace(",", ""))
                if 0.5 < p < 50:
                    prices.append(p)
            except ValueError:
                pass
    if len(prices) >= 3:
        med = statistics.median(prices)
        d["budget"] = f"USD {min(prices):.2f}-{max(prices):.2f} (median {med:.2f})"
        d["budget_src"] = f"their realised band, {len(prices)} samples"
    else:
        d["budget"], d["budget_src"] = "", ""
    # country from history
    countries = Counter((r.get("Country") or "").strip()
                        for r in hist if (r.get("Country") or "").strip())
    d["country"] = countries.most_common(1)[0][0] if countries else ""
    # request type — inferred, never defaulted (the 49% fix)
    hist_codes = {str(r.get("Product Code") or "").strip().upper() for r in hist}
    if ask.codes and all(c in hist_codes for c in ask.codes) \
            and not _MOD_RE.search(ask.ask_text):
        d["rtype"], d["rtype_label"] = "rep", "Repeat"
        d["base_code"] = ask.codes[0]
    elif ask.codes and _MOD_RE.search(ask.ask_text):
        d["rtype"], d["rtype_label"] = "mod", "Modify"
        d["base_code"] = ask.codes[0]
    else:
        d["rtype"], d["rtype_label"], d["base_code"] = "new", "New", ""
    return d


def shipto_from_sr_page(html: str) -> dict:
    """Latest ATTN/CONTACT/ADDRESS block + latest COMPLIANCE line from the
    SR's own request logs — the strongest ship-to source (91% of requests
    carry the block, near-identical per customer)."""
    text = BeautifulSoup(html, "html.parser").get_text("\n", strip=True)
    out = {"attn": "", "contact": "", "addr": "", "compliance": ""}
    for m in re.finditer(r"ATTN\s*:?\s*([^\n]+)", text, re.IGNORECASE):
        out["attn"] = m.group(1).strip()
    for m in re.finditer(r"CONTACT(?:\s*NO\.?)?\s*:?\s*([^\n]+)", text, re.IGNORECASE):
        out["contact"] = m.group(1).strip()
    for m in re.finditer(r"ADDRESS\s*:?\s*([^\n]+)", text, re.IGNORECASE):
        out["addr"] = m.group(1).strip()
    for m in re.finditer(r"COMPLIAN\w*\s*:?\s*([^\n]+)", text, re.IGNORECASE):
        out["compliance"] = m.group(1).strip()
    return out


# ----------------------------------------------------------------- MMS I/O

class SRWriter:
    """One authenticated MMS session; verified, reversible-where-possible
    writes. NO step is retried on auth failure — a bad login must never
    loop (account lockout risk, password rotates monthly)."""

    def __init__(self):
        self.session = requests.Session()
        self._logged_in = False

    def login(self) -> bool:
        if self._logged_in:
            return True
        ok = mms_client.login(
            self.session, config.MMS_USER, config.MMS_PASSWORD
        )
        self._logged_in = bool(ok)
        return self._logged_in

    def _url(self, sr_code: str) -> str:
        return f"{mms_client.BASE_URL}/master/sampleRequestUpdate.do?code={sr_code}"

    def get_page(self, sr_code: str) -> str:
        r = self.session.get(self._url(sr_code),
                             headers=mms_client.HEADERS_BASE, timeout=60)
        r.raise_for_status()
        r.encoding = "utf-8"
        return r.text

    def _post(self, sr_code: str, payload) -> str:
        r = self.session.post(
            self._url(sr_code), data=payload,
            headers={**mms_client.HEADERS_BASE,
                     "Content-Type": "application/x-www-form-urlencoded",
                     "Referer": self._url(sr_code)},
            timeout=90,
        )
        r.raise_for_status()
        r.encoding = "utf-8"
        return r.text

    @staticmethod
    def _sections(html: str) -> int:
        return len(set(re.findall(r'name="sreq1\[(\d+)\]\.seq"', html)))

    @staticmethod
    def _assignee_id(html: str, name: str) -> str:
        """Resolve the assignee's user-id from the live select options."""
        m = re.search(
            r'<option value="(\d+)"[^>]*>\s*' + re.escape(name) + r"\s*</option>",
            html, re.IGNORECASE,
        )
        return m.group(1) if m else ""

    def add_item_and_request(self, sr_code: str, rtype: str, base_code: str,
                             reqnote: str, assignee: str) -> dict:
        """additem → fill new section → request{N} → assign → save.
        Every step verified; cleanup via command=clear if we bail after
        additem (clear only removes EMPTY items — exactly our failure
        window). Returns {'ok':bool, 'detail':str, 'section':int}."""
        html = self.get_page(sr_code)
        before = self._sections(html)
        aid = self._assignee_id(html, assignee)
        if not aid:
            return {"ok": False,
                    "detail": f"assignee '{assignee}' not in the page's dropdown"}

        form = BeautifulSoup(html, "html.parser").find("form")
        if form is None:
            return {"ok": False, "detail": "no form on SR page"}
        html2 = self._post(sr_code, _override(_form_payload(form),
                                              {"command": "additem"}))
        after = self._sections(html2)
        if after != before + 1:
            return {"ok": False,
                    "detail": f"additem: expected {before + 1} sections, got {after}"}
        n = after - 1  # 0-based index of the new section

        try:
            form2 = BeautifulSoup(html2, "html.parser").find("form")
            fill = {
                f"sreq1[{n}].rtype": rtype,
                f"sreq1[{n}].reqnote": reqnote,
                "command": f"request{n}",
            }
            if rtype in ("rep", "mod") and base_code:
                fill[f"reqProductCode[{n}]"] = base_code
            html3 = self._post(sr_code, _override(_form_payload(form2), fill))
            # verify: our text is on the page and the section saved
            probe = reqnote.strip().splitlines()[0][:40]
            if probe and probe not in html3:
                return self._bail(sr_code,
                                  f"request{n}: submitted text not found on page")
            # assign + save
            form3 = BeautifulSoup(html3, "html.parser").find("form")
            html4 = self._post(sr_code, _override(_form_payload(form3), {
                f"sreq1[{n}].nextActUserId": aid,
                "command": "save",
            }))
            sel = re.search(
                rf'name="sreq1\[{n}\]\.nextActUserId".*?'
                rf'<option value="{aid}" selected',
                html4, re.S,
            )
            if not sel:
                return {"ok": False, "section": n + 1,
                        "detail": ("request saved but assignee NOT confirmed "
                                   "selected — set it manually in MMS")}
            return {"ok": True, "section": n + 1,
                    "detail": f"item ({n + 1}) raised, assigned to {assignee}"}
        except Exception as e:  # noqa: BLE001
            log.exception("SR submit failed mid-flight")
            return self._bail(sr_code, f"submit error: {e}")

    def _bail(self, sr_code: str, why: str) -> dict:
        """Best-effort cleanup of an empty just-added item."""
        try:
            html = self.get_page(sr_code)
            form = BeautifulSoup(html, "html.parser").find("form")
            if form is not None:
                self._post(sr_code, _override(_form_payload(form),
                                              {"command": "clear"}))
        except Exception:  # noqa: BLE001
            log.exception("SR clear-after-fail also failed")
        return {"ok": False, "detail": why + " (empty item cleared)"}

    # -- live SR lookup ---------------------------------------------------
    _cl_map: dict[str, str] | None = None  # squished customer name -> modal id

    def _customer_modal_map(self) -> dict[str, str]:
        """name→customer_id from the SR search page's modal — the complete
        MMS customer list (2,333 entities vs the master's 522), cached for
        the process lifetime."""
        if SRWriter._cl_map is not None:
            return SRWriter._cl_map
        r = self.session.get(
            f"{mms_client.BASE_URL}/master/sampleRequestSearch.do",
            headers=mms_client.HEADERS_BASE, timeout=60,
        )
        r.encoding = "utf-8"
        mapping: dict[str, str] = {}
        for m in re.finditer(
            r'name="cl" value="(\d+)"[^>]*>\s*'
            r'<label for="customer-\1">([^<]+)</label>', r.text,
        ):
            label = m.group(2)
            name = label.split(":", 1)[1] if ":" in label else label
            key = re.sub(r"[^a-z0-9]", "", name.lower())
            if key:
                mapping[key] = m.group(1)
        SRWriter._cl_map = mapping
        return mapping

    def newest_sr_for(self, customer_name: str) -> str:
        """First sampleRequestUpdate link in MMS's own list for this
        customer — MMS orders it most-recently-active first, which beats
        any series/date heuristic."""
        cmap = self._customer_modal_map()
        key = re.sub(r"[^a-z0-9]", "", customer_name.lower())
        cid = cmap.get(key) or next(
            (v for k, v in cmap.items() if key in k or k in key), "")
        if not cid:
            return ""
        url = f"{mms_client.BASE_URL}/master/sampleRequestSearch.do"
        common = {"code": "", "customer_id": cid, "customer_name": customer_name}
        hdrs = {**mms_client.HEADERS_BASE,
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": url}
        self.session.post(url, data={**common, "command": "find"},
                          headers=hdrs, timeout=60)
        r = self.session.post(url, data={**common, "command": "list"},
                              headers=hdrs, timeout=60)
        r.encoding = "utf-8"
        m = re.search(r"sampleRequestUpdate\.do\?code=([A-Za-z0-9\-]+)", r.text)
        return m.group(1) if m else ""

    def test_cycle(self, sr_code: str) -> str:
        """Reversible write test: additem → verify → clear → verify.
        Leaves the SR byte-identical (clear removes only empty items)."""
        html = self.get_page(sr_code)
        before = self._sections(html)
        form = BeautifulSoup(html, "html.parser").find("form")
        html2 = self._post(sr_code, _override(_form_payload(form),
                                              {"command": "additem"}))
        mid = self._sections(html2)
        form2 = BeautifulSoup(html2, "html.parser").find("form")
        html3 = self._post(sr_code, _override(_form_payload(form2),
                                              {"command": "clear"}))
        after = self._sections(html3)
        ok = (mid == before + 1) and (after == before)
        return (f"{'PASS' if ok else 'FAIL'}: sections {before} → additem "
                f"{mid} → clear {after} on {sr_code}")


# ------------------------------------------------------------ draft object

def build_draft(user_id: int, text: str) -> dict:
    """Everything needed to render + submit. Fetches the SR page once for
    ship-to/compliance provenance and section count."""
    ask = parse_ask(text)
    best, candidates = resolve_customer(ask.customer_text)
    if best is None:
        return {"error": "ambiguous", "candidates": candidates,
                "customer_text": ask.customer_text}
    customer = best["name"]
    hist = customer_history(customer)
    d = derive_defaults(hist, ask)

    # SR selection: MMS's own most-recently-active list first (authoritative
    # ordering), Submissions-tab series heuristic as offline fallback.
    ship = {"attn": "", "contact": "", "addr": "", "compliance": ""}
    page_err = ""
    sr_code = ""
    try:
        w = SRWriter()
        if w.login():
            sr_code = w.newest_sr_for(customer)
            if sr_code:
                ship = shipto_from_sr_page(w.get_page(sr_code))
        else:
            page_err = "MMS login failed (no retry — check password rotation)"
    except Exception as e:  # noqa: BLE001
        page_err = f"MMS lookup failed: {e}"
    if not sr_code:
        sr_code = find_sr_code(customer)
    prefix = (sr_code or "S")[0].upper()
    territory = {"S": "Singapore", "J": "Indonesia",
                 "B": "Thailand", "C": "C-factory"}.get(prefix, "?")

    # v1 is Singapore-only: refuse anything else instead of guessing the
    # assignee. (Asked codes are checked too — an S customer being asked
    # for a J/B product still routes to a non-SG factory.)
    non_sg = prefix != "S" or any(not c.upper().startswith("S-")
                                  for c in ask.codes)
    if non_sg:
        return {"error": "territory", "customer": customer,
                "sr_code": sr_code, "territory": territory,
                "codes": ask.codes}

    # memory + overrides beat page-derived values
    bag = ask.overrides.get("bag") or mem_get(customer, "bag")
    compliance = (ask.overrides.get("compliance")
                  or mem_get(customer, "compliance")
                  or ship["compliance"] or d["country"])
    budget = ask.overrides.get("budget") or mem_get(customer, "budget") or d["budget"]
    attn = ask.overrides.get("attn") or ship["attn"] or mem_get(customer, "attn")
    contact = ask.overrides.get("contact") or ship["contact"] or mem_get(customer, "contact")
    addr = ask.overrides.get("addr") or ship["addr"] or mem_get(customer, "addr")
    if ask.overrides.get("qty"):
        m = _QTY_RE.search(ask.overrides["qty"] + "g")
        if m:
            d["qty"] = int(float(m.group(1)))
    if ask.overrides.get("type") in ("new", "rep", "mod"):
        d["rtype"] = ask.overrides["type"]
        d["rtype_label"] = {"new": "New", "rep": "Repeat", "mod": "Modify"}[d["rtype"]]
    if ask.overrides.get("base"):
        d["base_code"] = ask.overrides["base"].upper()
    # prefix is guaranteed 'S' here (non-SG refused above) — no fallback
    # guessing; an explicit override still wins for flexibility.
    assignee = ask.overrides.get("assignee") or TERRITORY_ASSIGNEE[prefix]

    token = secrets.token_hex(3)
    draft = {
        "token": token, "user_id": user_id, "customer": customer,
        "sr_code": sr_code, "territory": territory, "prefix": prefix,
        "ask": ask, "derived": d, "bag": bag, "budget": budget,
        "compliance": compliance, "attn": attn, "contact": contact,
        "addr": addr, "assignee": assignee, "need_by": "",
        "page_err": page_err,
        "missing": [k for k, v in
                    (("bag", bag), ("ship-to", attn or addr))
                    if not v],
    }
    DRAFTS[token] = draft
    return draft


def render_reqnote(draft: dict) -> str:
    """The text that will be written into MMS — house style (Rich/Alex
    convention), fully visible in the draft before confirm."""
    d = draft["derived"]
    ask = draft["ask"]
    lines = []
    if ask.ask_text:
        lines.append(ask.ask_text.upper() if ask.ask_text.islower()
                     else ask.ask_text)
    lines.append(f"QTY: {d['qty']}G X {d['sets']} SET"
                 + ("S" if d["sets"] != 1 else ""))
    if draft["bag"]:
        lines.append(f"BAG: {draft['bag'].upper()}")
    if draft["budget"]:
        lines.append(f"BUDGET: {draft['budget']}")
    if draft["compliance"]:
        lines.append(f"COMPLIANCE: {draft['compliance']}")
    if draft["need_by"]:
        lines.append(f"NEED BY: {draft['need_by']}")
    if draft["attn"]:
        lines.append(f"ATTN: {draft['attn']}")
    if draft["contact"]:
        lines.append(f"CONTACT NO.: {draft['contact']}")
    if draft["addr"]:
        lines.append(f"ADDRESS: {draft['addr']}")
    lines.append("THANKS")
    return "\n".join(lines)


def remember_submitted(draft: dict) -> None:
    """Every submitted value becomes the customer's new default — this is
    the 'learn from every correction' loop: whatever Alex overrode this
    time is proposed next time."""
    c = draft["customer"]
    for key in ("bag", "budget", "compliance", "attn", "contact", "addr"):
        if draft.get(key):
            mem_set(c, key, draft[key])
