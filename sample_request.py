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
    ask_text: str = ""      # the request body — everything the rep wrote
    codes: list[str] = field(default_factory=list)
    qty_g: int | None = None
    qty_each: bool = False  # "1kg for each sample"
    sets: int | None = None
    overrides: dict[str, str] = field(default_factory=dict)  # EXPLICIT values
    hints: set = field(default_factory=set)  # field mentioned, value unreadable

_OVR_KEYS = {"bag", "budget", "compliance", "attn", "addr", "address",
             "contact", "qty", "sets", "assignee", "type", "base"}

# Markets the fallback normalizer knows (typo-tolerant via fuzzy match).
_MARKETS = [
    "Bangladesh", "Mexico", "Singapore", "Malaysia", "Indonesia", "Thailand",
    "Vietnam", "Japan", "Korea", "China", "India", "Philippines", "Australia",
    "New Zealand", "USA", "Canada", "EU", "UK", "Middle East", "Dubai", "UAE",
    "Taiwan", "Hong Kong", "Nepal", "Myanmar", "Sri Lanka", "Pakistan",
    "Saudi Arabia", "CODEX", "FDA", "FSANZ",
]

def _normalize_markets(s: str) -> str:
    """'banagldesh and mexico market' → 'Bangladesh, Mexico'. Unknown tokens
    survive title-cased rather than being dropped."""
    from rapidfuzz import fuzz
    s = re.sub(r"\bmarkets?\b", " ", s, flags=re.I)
    tokens = [t.strip() for t in re.split(r",|/|&|\band\b|\+", s, flags=re.I)
              if t.strip()]
    out = []
    for t in tokens:
        best, score = "", 0
        for m in _MARKETS:
            sc = fuzz.ratio(t.lower(), m.lower())
            if sc > score:
                best, score = m, sc
        out.append(best if score >= 80 else t.title())
    seen, uniq = set(), []
    for m in out:
        if m.lower() not in seen:
            seen.add(m.lower())
            uniq.append(m)
    return ", ".join(uniq)


_BUDGET_LINE = re.compile(
    r"budget\s*[:\-]?\s*(.+)|"
    r"((?:<|under|below|max|less than|around|about)\s*(?:usd\s*)?\$?"
    r"\d+(?:\.\d+)?(?:\s*-\s*\d+(?:\.\d+)?)?\s*(?:usd)?)|"
    r"(cheap(?:est)?(?:\s+as\s+possible)?|no budget)", re.IGNORECASE)
_COMPLIANCE_LINE = re.compile(
    r"complian\w*\s*(?:for|:)?\s*(.+)", re.IGNORECASE)
_BAG_LINE = re.compile(r"\b(np|empty)\s*(?:sample\s*)?bags?\b", re.IGNORECASE)
_NEEDBY_LINE = re.compile(
    r"need(?:ed)?(?:\s+it)?\s+by\s+(.+)|\b(next week|this week|asap|urgent)\b",
    re.IGNORECASE)
_ATTN_LINE = re.compile(r"\battn\.?\s*[:\-]?\s*(.+)", re.IGNORECASE)
_CONTACT_LINE = re.compile(
    r"\b(?:contact(?:\s*no\.?)?|phone|tel)\s*[:\-]?\s*(.+)", re.IGNORECASE)
_ADDR_LINE = re.compile(r"\baddress\s*[:\-]?\s*(.+)", re.IGNORECASE)


def parse_ask(text: str) -> Ask:
    """No-LLM fallback parser. Handles MULTI-LINE messages: the first line
    (or first comma/dash segment) names the customer; every line is scanned
    for explicit field values (budget, compliance, qty, bag, ship-to,
    need-by) which are EXPLICIT — they always beat derived values. A field
    keyword we can see but can't read becomes a HINT: the draft shows
    'please confirm' instead of a confidently wrong derived number.
    The old ';key: value' syntax still works, silently."""
    a = Ask()
    # legacy ';key: value' segments first (silent compatibility)
    semi = [p.strip() for p in text.split(";")]
    for seg in semi[1:]:
        if ":" in seg and "\n" not in seg:
            k, v = seg.split(":", 1)
            k = k.strip().lower()
            if k in _OVR_KEYS:
                a.overrides["addr" if k == "address" else k] = v.strip()
    text = semi[0] if len(semi) > 1 else text

    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if not lines:
        return a
    head = lines[0]
    # customer — ask split: em/en dash, ' - ', 'Ltd- X', or first comma
    m = re.split(r"\s*[–—]\s*|\s+-\s*|-\s+|,", head, 1)
    if len(m) == 2:
        a.customer_text, first_ask = m[0].strip(), m[1].strip()
    else:
        words = head.split()
        a.customer_text, first_ask = " ".join(words[:3]), " ".join(words[3:])

    body: list[str] = [first_ask] if first_ask else []
    for line in lines[1:]:
        consumed = False
        bm = _BUDGET_LINE.search(line)
        if bm and "budget" not in a.overrides:
            val = (bm.group(1) or bm.group(2) or bm.group(3) or "").strip()
            if val:
                a.overrides["budget"] = val
                consumed = True
            else:
                a.hints.add("budget")
        cm = _COMPLIANCE_LINE.search(line)
        if cm and "compliance" not in a.overrides:
            val = _normalize_markets(cm.group(1))
            if val:
                a.overrides["compliance"] = val
                consumed = True
            else:
                a.hints.add("compliance")
        gm = _BAG_LINE.search(line)
        if gm and "bag" not in a.overrides:
            a.overrides["bag"] = ("NP bag" if gm.group(1).lower() == "np"
                                  else "Empty bag")
            consumed = consumed or len(line) < 40
        nm = _NEEDBY_LINE.search(line)
        if nm and "need_by" not in a.overrides:
            a.overrides["need_by"] = (nm.group(1) or nm.group(2)).strip()
        am = _ATTN_LINE.search(line)
        if am:
            a.overrides["attn"] = am.group(1).strip()
            consumed = True
        km = _CONTACT_LINE.search(line)
        if km:
            a.overrides["contact"] = km.group(1).strip()
            consumed = True
        dm = _ADDR_LINE.search(line)
        if dm:
            a.overrides["addr"] = dm.group(1).strip()
            consumed = True
        qm = _QTY_RE.search(line)
        if qm and a.qty_g is None:
            n = float(qm.group(1))
            a.qty_g = int(n * 1000) if qm.group(2).lower() == "kg" else int(n)
            if re.search(r"\beach\b|\bper\b", line, re.I):
                a.qty_each = True
            consumed = consumed or len(line) < 30
        if not consumed:
            body.append(line)

    a.ask_text = "\n".join(body).strip()
    blob = a.ask_text + " "
    a.codes = [c.upper() for c in _CODE_RE.findall(blob)]
    if a.qty_g is None:
        m2 = _QTY_RE.search(blob)
        if m2:
            n = float(m2.group(1))
            a.qty_g = int(n * 1000) if m2.group(2).lower() == "kg" else int(n)
            if re.search(r"\beach\b", blob, re.I):
                a.qty_each = True
    m3 = _SETS_RE.search(blob)
    if m3:
        a.sets = int(m3.group(1) or m3.group(2))
    # keyword-without-value → hint, never a derived fill-in
    full = text.lower()
    if "budget" not in a.overrides and "budget" not in a.hints \
            and re.search(r"budget|cheap", full):
        a.hints.add("budget")
    if "compliance" not in a.overrides \
            and re.search(r"complian|regulat", full):
        a.hints.add("compliance")
    return a


# ------------------------------------------------------------- LLM parsing

_PARSE_PROMPT = """You turn a salesperson's casual message into a sample-request JSON.
The message names a customer and what seasoning/sample they want, plus any of:
quantity (grams), sets, bag type (NP bag / empty bag), budget, compliance
markets, ship-to contact/address, need-by timing, request type.

Message: {text}

Reply with ONLY a JSON object (no prose) with exactly these keys, null when
not stated:
{{"customer": str, "ask": str (what they want, short, keep product codes
verbatim), "qty_g": int|null, "sets": int|null, "bag": str|null,
"budget": str|null, "compliance": str|null, "attn": str|null,
"contact": str|null, "addr": str|null, "need_by": str|null,
"rtype": "new"|"rep"|"mod"|null, "base_code": str|null}}

Rules: 'repeat X' → rtype rep, base_code X. 'modify/change X' → rtype mod.
"cheap as possible" etc → budget as stated. Never invent values."""

_UPDATE_PROMPT = """A salesperson is editing a draft sample request by chatting.
Current draft:
{draft}

Their new message: {text}

Classify and reply with ONLY JSON:
{{"action": "modify"|"confirm"|"discard"|"new_request"|"unrelated",
"fields": {{...only the draft keys that change...}},
"question": str|null}}

Draft keys allowed in fields: ask, qty (int, grams), sets (int), bag, budget,
compliance, attn, contact, addr, need_by, assignee, rtype ("new"/"rep"/"mod"),
base_code.
- "confirm" = they clearly say to raise/submit/send it (e.g. "yes go ahead",
  "raise it", "confirm", "ok send").
- "discard" = cancel / never mind / drop it.
- "new_request" = a different customer + different ask (a fresh request, not
  an edit).
- "unrelated" = clearly not about this draft (a product search, a greeting,
  another bot task).
- otherwise "modify" with the changed fields. "make it 200g" → qty 200.
  "use empty bags" → bag "Empty bag". "send to the KL office ..." → addr.
- If their edit is ambiguous, action "modify", empty fields, and put a short
  plain-language question in "question" (like a colleague would ask,
  e.g. "How many grams — 200?"). Never invent values."""


def _json_from(text: str) -> dict | None:
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        return None
    try:
        import json
        return json.loads(m.group(0))
    except Exception:  # noqa: BLE001
        return None


async def llm_parse(text: str) -> dict | None:
    """Natural-language → request dict. None on any failure (caller falls
    back to the regex parser, which also keeps the old ';key: value' syntax
    working silently)."""
    import ai
    try:
        out, _, _ = await ai._ask(  # noqa: SLF001 — house helper
            _PARSE_PROMPT.format(text=text), max_tokens=300)
        d = _json_from(out)
        if d and d.get("customer") and d.get("ask"):
            return d
    except Exception as e:  # noqa: BLE001
        log.warning("SR llm_parse failed: %s", e)
    return None


async def llm_update(draft: dict, text: str) -> dict:
    """Interpret a reply to an active draft. Always returns an action dict;
    on LLM failure the reply is treated as unrelated (normal routing)."""
    import ai
    import json as _json
    snapshot = {
        "customer": draft["customer"], "ask": draft["ask"].ask_text,
        "qty": draft["derived"]["qty"], "sets": draft["derived"]["sets"],
        "rtype": draft["derived"]["rtype"],
        "base_code": draft["derived"]["base_code"],
        "bag": draft["bag"], "budget": draft["budget"],
        "compliance": draft["compliance"], "attn": draft["attn"],
        "contact": draft["contact"], "addr": draft["addr"],
        "assignee": draft["assignee"], "need_by": draft["need_by"],
    }
    try:
        out, _, _ = await ai._ask(  # noqa: SLF001
            _UPDATE_PROMPT.format(draft=_json.dumps(snapshot), text=text),
            max_tokens=300)
        d = _json_from(out)
        if d and d.get("action") in ("modify", "confirm", "discard",
                                     "new_request", "unrelated"):
            return d
    except Exception as e:  # noqa: BLE001
        log.warning("SR llm_update failed: %s", e)
    return {"action": "unrelated", "fields": {}, "question": None}


_CONFIRM_RE = re.compile(
    r"^(ok(ay)?\b[\s,.!]*)?(yes\b[\s,.!]*)?"
    r"(raise|submit|send|confirm|go ahead|proceed)(\s+it)?[\s.!]*$",
    re.IGNORECASE)
_DISCARD_RE = re.compile(
    r"^(cancel|discard|never\s?mind|drop it|forget it)\b", re.IGNORECASE)
_QTY_REPLY_RE = re.compile(
    r"^(?:make it |change (?:it )?to )?(\d+(?:\.\d+)?)\s*(kg|g)\b"
    r"(\s*each)?[\s.!]*$", re.IGNORECASE)


def fallback_update(draft: dict, text: str) -> dict:
    """No-LLM interpretation of a reply to an active draft — keeps the
    conversational loop alive when the API is down or out of credits.
    Deterministic and conservative: anything it can't clearly match is
    'unrelated' (falls through to normal routing), never a guess."""
    t = text.strip()
    if _CONFIRM_RE.match(t):
        return {"action": "confirm", "fields": {}, "question": None}
    if _DISCARD_RE.match(t):
        return {"action": "discard", "fields": {}, "question": None}
    fields: dict = {}
    qm = _QTY_REPLY_RE.match(t)
    if qm:
        n = float(qm.group(1))
        fields["qty"] = int(n * 1000) if qm.group(2).lower() == "kg" else int(n)
        if qm.group(3):
            draft["ask"].qty_each = True
    gm = _BAG_LINE.search(t)
    if gm and len(t) < 60:
        fields["bag"] = "NP bag" if gm.group(1).lower() == "np" else "Empty bag"
    bm = _BUDGET_LINE.search(t)
    if bm and re.search(r"budget|usd|cheap", t, re.I) and len(t) < 80:
        val = (bm.group(1) or bm.group(2) or bm.group(3) or "").strip()
        if val:
            fields["budget"] = val
    cm = _COMPLIANCE_LINE.search(t)
    if cm:
        val = _normalize_markets(cm.group(1))
        if val:
            fields["compliance"] = val
    nm = _NEEDBY_LINE.search(t)
    if nm and len(t) < 60:
        fields["need_by"] = ((nm.group(1) or nm.group(2)) or "").strip().upper()
    am = _ATTN_LINE.search(t)
    if am:
        fields["attn"] = am.group(1).strip()
    km = _CONTACT_LINE.search(t)
    if km:
        fields["contact"] = km.group(1).strip()
    dm = _ADDR_LINE.search(t)
    if dm:
        fields["addr"] = dm.group(1).strip()
    if fields:
        return {"action": "modify", "fields": fields, "question": None}
    return {"action": "unrelated", "fields": {}, "question": None}


def apply_fields(draft: dict, fields: dict) -> None:
    """Merge an LLM 'modify' result into the draft in place."""
    d = draft["derived"]
    for k, v in (fields or {}).items():
        if v in (None, ""):
            continue
        if k == "qty":
            try:
                d["qty"], d["qty_src"] = int(v), "you said"
            except (TypeError, ValueError):
                pass
        elif k == "sets":
            try:
                d["sets"] = int(v)
            except (TypeError, ValueError):
                pass
        elif k == "rtype" and v in ("new", "rep", "mod"):
            d["rtype"] = v
            d["rtype_label"] = {"new": "New", "rep": "Repeat",
                                "mod": "Modify"}[v]
        elif k == "base_code":
            d["base_code"] = str(v).upper()
        elif k == "ask":
            draft["ask"].ask_text = str(v)
        elif k in ("bag", "budget", "compliance", "attn", "contact",
                   "addr", "assignee", "need_by"):
            draft[k] = str(v)
            # a reply IS an explicit statement — it must never be
            # re-overridden by derived values, and clears 'confirm' flags
            draft.setdefault("src", {})[k] = "you"
    draft["missing"] = [m for m, val in
                        (("bag", draft["bag"]),
                         ("ship-to", draft["attn"] or draft["addr"]))
                        if not val]


def parsed_to_text(parsed: dict) -> str:
    """Rebuild a canonical '/sr' line from an LLM parse so build_draft's
    existing pipeline (regex overrides included) stays the single path."""
    bits = [f"{parsed['customer']} — {parsed['ask']}"]
    if parsed.get("qty_g"):
        bits[0] += f" {parsed['qty_g']}g"
    if parsed.get("sets"):
        bits[0] += f" x {parsed['sets']}"
    keymap = {"bag": "bag", "budget": "budget", "compliance": "compliance",
              "attn": "attn", "contact": "contact", "addr": "addr",
              "rtype": "type", "base_code": "base"}
    for k, ov in keymap.items():
        if parsed.get(k):
            bits.append(f"{ov}: {parsed[k]}")
    return "; ".join(bits)


# ----------------------------------------------------------- customer + SR

_GENERIC_TOKENS = {
    "food", "foods", "ltd", "limited", "co", "company", "pte", "sdn", "bhd",
    "inc", "corp", "corporation", "industries", "industry", "group",
    "global", "international", "trading", "enterprise", "enterprises",
    "manufacturing", "the", "and", "of",
}

def _plausible(query: str, name: str) -> bool:
    """A candidate is only plausible if a DISTINCTIVE query token matches a
    distinctive name token. Kills 'pran food' → 'AKIJ FOOD AND BEVERAGE'
    (they share only the generic 'food') — same bug class as Prantalay."""
    from rapidfuzz import fuzz
    q_toks = [t for t in re.findall(r"[a-z0-9]+", query.lower())
              if t not in _GENERIC_TOKENS and len(t) >= 3]
    n_toks = [t for t in re.findall(r"[a-z0-9]+", name.lower())
              if t not in _GENERIC_TOKENS and len(t) >= 3]
    if not q_toks or not n_toks:
        return True  # nothing distinctive to judge by — don't over-filter
    for qt in q_toks:
        for nt in n_toks:
            if qt in nt or nt in qt or fuzz.ratio(qt, nt) >= 80:
                return True
    return False


def resolve_customer(q: str) -> tuple[dict | None, list[dict]]:
    """Best customer + runner-up candidates from master + FSL names."""
    merged = sheets.load_merged_customers()
    hits = matcher.top_customer_master(q, merged, limit=5)
    hits = [h for h in hits if _plausible(q, h.get("name", ""))][:3]
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

def build_draft(user_id: int, text: str, force_customer: str = "") -> dict:
    """Everything needed to render + submit. Fetches the SR page once for
    ship-to/compliance provenance and section count. `force_customer`
    skips name resolution (the 'which one did you mean' button flow)."""
    ask = parse_ask(text)
    if force_customer:
        customer = force_customer
    else:
        best, candidates = resolve_customer(ask.customer_text)
        if best is None:
            return {"error": "ambiguous", "candidates": candidates,
                    "customer_text": ask.customer_text, "raw_text": text}
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

    # ABSOLUTE RULE (Alex, 31 Aug — after his '<2 usd' was silently
    # replaced by the derived 'USD 3.62-9.07' band): anything the user
    # EXPLICITLY stated always wins over derived/remembered values. A field
    # keyword we saw but couldn't read (hint) shows 'please confirm' and
    # suppresses the derived proposal — a wrong confident number is far
    # worse than a blank.
    src: dict[str, str] = {}

    def pick(key, explicit, *fallbacks):
        if explicit:
            src[key] = "you"
            return explicit
        if key in ask.hints:
            src[key] = "confirm"
            return ""
        for label, val in fallbacks:
            if val:
                src[key] = label
                return val
        src[key] = ""
        return ""

    bag = pick("bag", ask.overrides.get("bag"),
               ("remembered", mem_get(customer, "bag")))
    compliance = pick("compliance", ask.overrides.get("compliance"),
                      ("remembered", mem_get(customer, "compliance")),
                      ("their last request", ship["compliance"]),
                      ("their country", d["country"]))
    budget = pick("budget", ask.overrides.get("budget"),
                  ("remembered", mem_get(customer, "budget")),
                  (d.get("budget_src") or "history", d["budget"]))
    # Ship-to stacks every source we hold (Alex: propose and confirm, don't
    # say 'not known yet'): explicit > SR-page logs > memory > customer
    # master (address/receiver/phone).
    master_rec = best if not force_customer else next(
        (c for c in sheets.load_merged_customers()
         if c.get("name", "").strip().lower() == customer.strip().lower()), {})
    attn = pick("attn", ask.overrides.get("attn"),
                ("their last request", ship["attn"]),
                ("remembered", mem_get(customer, "attn")),
                ("customer master", (master_rec or {}).get("receiving_person", "")))
    contact = pick("contact", ask.overrides.get("contact"),
                   ("their last request", ship["contact"]),
                   ("remembered", mem_get(customer, "contact")),
                   ("customer master", (master_rec or {}).get("receiver_number", "")))
    addr = pick("addr", ask.overrides.get("addr"),
                ("their last request", ship["addr"]),
                ("remembered", mem_get(customer, "addr")),
                ("customer master", (master_rec or {}).get("address", "")))
    if ask.overrides.get("qty"):
        m = _QTY_RE.search(ask.overrides["qty"] + "g")
        if m:
            d["qty"], d["qty_src"] = int(float(m.group(1))), "you specified"
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
        "addr": addr, "assignee": assignee,
        "need_by": (ask.overrides.get("need_by") or "").upper(),
        "page_err": page_err, "src": src,
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
    # "1kg for each sample" (multi-flavour asks) → EACH, matching the house
    # convention: multi-flavour requests are ONE item with a flavour list
    # (verified across the corpus — Rich's 2026 butter-range and nasi-lemak
    # requests list up to 6 flavours in a single section).
    if ask.qty_each:
        lines.append(f"QTY: {d['qty']}G EACH")
    else:
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
