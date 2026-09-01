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
# 'x N' means sets ONLY when N isn't counting flavours — '200g x 3 flavours'
# used to read as 3 SETS (a 9× over-order on a 3-flavour ask).
_SETS_RE = re.compile(
    r"\b[xX]\s*(\d+)\b(?!\s*flavou?rs?)|\b(\d+)\s*sets?\b", re.IGNORECASE)
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
    # Structured body (Alex 31 Aug: 'paragraphs the comment nicely so rnd
    # knows what comment for which seasoming'):
    flavours: list = field(default_factory=list)  # [{"name":…, "spec":[…]}]
    base: str = ""          # TARGET BASE / application
    restriction: str = ""   # halal / gluten-free / non-GMO …
    structured: bool = False  # True only when block detection is CONFIDENT
    # Alex 02-Sep: never fabricate 'No prefer code.' — say it only when he
    # actually wrote it, and then only inside the Comment block.
    no_prefer_code: bool = False
    # Alex 02-Sep: delivery METHOD and delivery ADDRESS are two separate
    # inputs. A stated delivery address is an EXPLICIT ship-to and must
    # replace the derived customer-master address, never sit beside it
    # contradicting it (courier-to-Geylang vs the Dhaka master address).
    delivery: str = ""          # method: courier / self-collect / DHL…
    delivery_addr: str = ""     # where it actually goes
    # Per-item quantities: [(qty_text, item_name)] e.g. [('500g',
    # 'Texture improver 2')]. Rendered as 'QTY: 100g - Tomato seasoning,
    # 500g - Texture improver 2' so R&D can't misread which is which.
    item_qty: list = field(default_factory=list)
    items: list = field(default_factory=list)  # bare product/item names

    def body_text(self) -> str:
        """ask_text PLUS the structured flavour blocks — for detection scans
        (modify-keywords etc.) that must see the whole request, not just the
        intro left behind after _structure_body moved the blocks out."""
        parts = [self.ask_text]
        for f in self.flavours:
            parts.append(f.get("name", ""))
            parts.extend(f.get("spec", []))
        return "\n".join(p for p in parts if p)

_OVR_KEYS = {"bag", "budget", "compliance", "attn", "addr", "address",
             "contact", "qty", "sets", "assignee", "type", "base", "need_by"}

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
    r"need(?:ed)?(?:\s+it)?\s+by\s+(.+)|"
    # 'Expected to be send by 9sept', 'target to send by 19 aug'
    r"(?:expect(?:ed)?|target)\s+to\s+(?:be\s+)?(?:send|sent|ship)\s*"
    r"(?:by|on)?\s+(.+)|"
    r"\b(next week|this week|asap|urgent)\b",
    re.IGNORECASE)
_BASE_LINE = re.compile(
    r"(?:target\s+)?base\s*(?:is)?\s*(?:on)?\s*[:\-]?\s*(.+)|"
    r"application\s*[:\-]?\s*(.+)", re.IGNORECASE)
_RESTRICTION_LINE = re.compile(
    r"restrictions?\s*[:\-]?\s*(.+)|"
    r"\b((?:must be |no )?(?:halal|gluten.?free|non.?gmo|msg.?free|vegan|"
    r"non.?irradiated)[^\n]*)", re.IGNORECASE)
_NUM_ITEM = re.compile(r"^(\d+)[.)]\s*(.+)$")
# Alex's standard request form marks required fields with '*'
# ('Budget* for ...', 'Contact*: +86...', 'Expected to be send by* 9sept').
# The star is form notation, not content — strip it before field matching
# or it lands inside every captured value ('* For China').
_STAR = re.compile(r"(?<=[A-Za-z一-鿿)])\*+")
# Section headers whose CONTENT is the lines beneath them.
_HDR_SEASONING = re.compile(r"^seasoning\s*names?\s*[:\-]?\s*$", re.IGNORECASE)
_HDR_QTY = re.compile(
    r"^(?:qty|quantity)\s*(?:of\s*samples?)?\s*[:\-]?\s*$|"
    # Alex's own wording on his existing SRs
    r"^sample\s*(?:to\s*be\s*given|size|qty)[^:]*:?\s*$", re.IGNORECASE)
_HDR_COMMENT = re.compile(r"^comments?\s*[:\-]\s*(.*)$", re.IGNORECASE)
_RECEIVER_LINE = re.compile(
    r"^(?:receiver|recipient|attention)\s*(?:name)?\s*[:\-]?\s*(.+)$",
    re.IGNORECASE)
# A line that is (mostly) CJK is part of a Chinese company / address block.
_CJK = re.compile(r"[一-鿿]")
# 'no prefer code' / 'no preferred code' / 'no code preference' — EXPLICIT only.
_NO_PREFER_CODE = re.compile(
    r"\bno\s+(?:prefer(?:red|ence)?|preference)\s+code\b|"
    r"\bno\s+code\s+preference\b", re.IGNORECASE)
# 'Send method:' / 'Delivery method:' / 'to courier to Geylang'
_DELIVERY_LINE = re.compile(
    r"^(?:send|delivery|shipping)\s*method\s*[:\-]?\s*(.*)$|"
    r"^((?:to\s+)?(?:courier|self.?collect|collection|hand.?carry|deliver)\b.*)$",
    re.IGNORECASE)
# explicit 'Delivery address: X' / 'Send to: X' / 'Ship to: X'
_DELIVERY_ADDR_LINE = re.compile(
    r"^(?:delivery|shipping|send|ship)\s*(?:address|to)\s*[:\-]?\s*(.+)$",
    re.IGNORECASE)
# the bare method verbs, so 'to courier to Geylang' splits into
# method='Courier' + address='Geylang'
_METHOD_WORD = re.compile(
    r"\b(courier|self.?collect(?:ion)?|hand.?carry|dhl|fedex|ups|deliver\w*|"
    r"collect\w*)\b", re.IGNORECASE)
_METHOD_DEST = re.compile(
    r"\b(?:courier|deliver\w*|send|ship|collect\w*)\b[^,]*?\bto\s+(.+)$",
    re.IGNORECASE)
# What a sample quantity actually IS (Alex 02-Sep, confirmed against the
# corpus): an AMOUNT PHRASE — a leading number followed by an optional short
# unit/qualifier. The vocabulary is open and always will be: '1000g', '50kg',
# '1 pkt', '1 small bottle', '1pcs Noodle Cake', '200g each', '500G EA'. So
# the rule is 'starts with a number', NOT membership of a unit list — a fixed
# list silently drops every phrasing nobody thought to enumerate.
#
# Guards that keep it from eating other lines:
#   • the trailing phrase is capped short, so a product name can't pass as
#     a unit ('1 Lemon Habanero Seasoning' is not a quantity)
#   • a numbered list marker is excluded for free: '1.' / '2)' put
#     punctuation straight after the digit, where a unit letter must be.
_AMOUNT = r"\d+(?:\.\d+)?\s*(?:[A-Za-z][A-Za-z.]{0,9}(?:\s+[A-Za-z]{1,8})?)?"
_QTY_SEP = r"(?:[-–—:]|\s+of\b)"
# '500g - texture improver 2' | '1 pkt- X' | '1 pkt of application'
_QTY_ITEM_RE = re.compile(
    rf"^({_AMOUNT})\s*{_QTY_SEP}\s*(.+)$", re.IGNORECASE)
# 'texture improver 2 - 500g'
_ITEM_QTY_RE = re.compile(
    rf"^(.+?)\s*[-–—:]\s*({_AMOUNT})\s*$", re.IGNORECASE)
# A bare amount on its own line ('1 small bottle', '100g sample',
# '1pcs Noodle Cake'). Only used UNDER a quantity heading — in that
# context the whole line is the quantity, so 'starts with a number' is
# the honest rule and no unit vocabulary is needed. Numbered list markers
# ('1.' / '2)') are still excluded.
_BARE_AMOUNT_RE = re.compile(r"^\d+(?:\.\d+)?\s*[A-Za-z][\w.,/ ]{0,40}$")


def _norm_qty(s: str) -> str:
    """Tidy spacing only — never re-word the rep's own phrasing.
    '500 g' → '500g'; '1 pkt', '1 small bottle' pass through intact."""
    s = " ".join(s.split())
    return re.sub(r"^(\d+(?:\.\d+)?)\s+(kgs?|g|gm)\b", r"\1\2", s,
                  flags=re.IGNORECASE)
_ATTN_LINE = re.compile(r"\battn\.?\s*[:\-]?\s*(.+)", re.IGNORECASE)
# (?![A-Za-z]) — without it 'tel' matched INSIDE 'Tellicherry'/'Telur' and
# the rest of a spec line was consumed as CONTACT NO. and written into MMS.
_CONTACT_LINE = re.compile(
    r"\b(?:contact(?:\s*no\.?)?|phone|tel)(?![A-Za-z])\s*[:\-]?\s*(.+)",
    re.IGNORECASE)
# Line-start OR explicit colon — 'Please address the bitterness at the end'
# is the verb, not a shipping address. Value is group(1) or group(2).
_ADDR_LINE = re.compile(
    r"^\s*address\b\s*[:\-]?\s*(.+)|\baddress\s*:\s*(.+)", re.IGNORECASE)
# Lines that give a USAGE rate, not a sample size — 'dosage 15g per kg of
# chips' must never become the request quantity.
_DOSAGE_LINE = re.compile(
    r"\bdosage|\busage|\bper\s+kg\b|\bper\s+100\s*g\b", re.IGNORECASE)

# --- unlabelled pasted ship-to blocks ---------------------------------
# Reps paste the customer's mailing block verbatim (company / MR X /
# phone / address lines / country) with no ATTN:/ADDRESS: labels. If it
# isn't recognised it stays in the note body AND the footer appends the
# stale remembered ship-to — a duplicate in the note, and worse, the
# NEW phone/address the rep just gave is silently ignored. Anchored on
# a standalone phone-number line so prose never matches.
_PHONE_ONLY_LINE = re.compile(r"\+?\(?\d[\d\s\-().]{6,}")
_ATTN_NAME_LINE = re.compile(
    r"(?:mr|ms|mrs|mdm|dr|attn)\.?\s+[a-zA-Z][a-zA-Z .'\-]{1,40}", re.IGNORECASE)
# 'MR SAJIB +880 1704-158453' — Telegram often joins the name and phone
# onto one line; group 1 = the person, group 2 = the number.
_ATTN_PHONE_LINE = re.compile(
    r"((?:mr|ms|mrs|mdm|dr|attn)\.?\s+[a-zA-Z][a-zA-Z .'\-]{1,40}?)\s+"
    r"(\+?\(?\d[\d\s\-().]{6,})", re.IGNORECASE)
_COMPANY_TAIL = re.compile(
    r"\b(?:ltd|limited|pte|inc|corp|co|llc|bhd|gmbh|company)\b\.?\s*$",
    re.IGNORECASE)
_ADDR_WORDS = re.compile(
    r"\b(?:p\.?o\.?\s*box|box|level|floor|road|street|jalan|centre|center|"
    r"building|tower|block|district|industrial|estate|zone)\b|#\s*\d",
    re.IGNORECASE)
_POSTAL_CITY = re.compile(r"\d{3,6}\s+[a-zA-Z][a-zA-Z .\-]+")
_SHIPTO_COUNTRIES = frozenset((
    "bangladesh", "mexico", "vietnam", "viet nam", "singapore", "malaysia",
    "indonesia", "thailand", "india", "china", "japan", "philippines",
    "myanmar", "cambodia", "laos", "korea", "south korea", "taiwan",
    "hong kong", "sri lanka", "pakistan", "nepal", "uae",
    "united arab emirates", "saudi arabia", "australia", "new zealand",
    "usa", "united states", "uk", "united kingdom",
))


def _shipto_kind(line: str) -> str:
    """Classify one line of a pasted mailing block ('' = not one)."""
    s = line.strip()
    if not s or len(s) > 70:
        return ""
    if _PHONE_ONLY_LINE.fullmatch(s):
        return "phone"
    if _ATTN_PHONE_LINE.fullmatch(s):
        return "attn_phone"
    if _ATTN_NAME_LINE.fullmatch(s):
        return "attn"
    if s.lower().rstrip(".") in _SHIPTO_COUNTRIES:
        return "country"
    if _COMPANY_TAIL.search(s):
        return "company"
    st = s.rstrip(",").strip()
    if (_ADDR_WORDS.search(s) or _POSTAL_CITY.fullmatch(st)
            or ("," in st and any(ch.isdigit() for ch in st))):
        return "addr"
    return ""


def _extract_shipto(a: Ask, body: list[str]) -> None:
    """Pull an unlabelled pasted mailing block out of the body into the
    ship-to overrides. Needs a standalone phone line plus ≥1 adjacent
    address-shaped line; explicit ATTN:/CONTACT:/ADDRESS: labels still
    win (setdefault), but the block always leaves the note — a mailing
    block inside a flavour spec is never what R&D should read."""
    pi = next((i for i, l in enumerate(body)
               if _shipto_kind(l) in ("phone", "attn_phone")), None)
    if pi is None:
        return
    lo = pi
    while lo > 0 and _shipto_kind(body[lo - 1]):
        lo -= 1
    hi = pi
    while hi + 1 < len(body) and _shipto_kind(body[hi + 1]):
        hi += 1
    if hi == lo and _shipto_kind(body[pi]) == "phone":
        return  # a lone number is not a mailing block ('MR X +65…' is)
    block = body[lo:hi + 1]
    ap = _ATTN_PHONE_LINE.fullmatch(body[pi].strip())
    contact = ap.group(2).strip() if ap else body[pi].strip()
    attn = (ap.group(1).strip() if ap else
            next((l.strip() for l in block
                  if _shipto_kind(l) == "attn"), ""))
    addr_ls = [l.strip().rstrip(",").strip() for l in block
               if _shipto_kind(l) not in ("phone", "attn", "attn_phone")]
    a.overrides.setdefault("contact", contact)
    if attn:
        a.overrides.setdefault("attn",
                               attn.title() if attn.isupper() else attn)
    if addr_ls:
        a.overrides.setdefault("addr", ", ".join(addr_ls))
    del body[lo:hi + 1]


def _structure_body(a: Ask, body: list[str]) -> None:
    """Split the request body into per-flavour blocks so R&D can see which
    comment belongs to which seasoning. CONSERVATIVE: it only claims
    'structured' when the rep himself numbered the flavours (1. / 2) …);
    anything else keeps the raw text unaltered — a mangled half-structure
    is worse than his own words verbatim."""
    blocks: list[dict] = []
    current: dict | None = None
    intro: list[str] = []
    first_num = 0
    for line in body:
        m = _NUM_ITEM.match(line)
        if m:
            if not blocks:
                first_num = int(m.group(1))
            # 'name - spec' on the same numbered line → split once
            rest = m.group(2).strip()
            parts = re.split(r"\s+[-–—]\s+", rest, 1)
            current = {"name": parts[0].strip(),
                       "spec": [parts[1].strip()] if len(parts) > 1 else []}
            blocks.append(current)
        elif current is not None:
            current["spec"].append(line)
        else:
            intro.append(line)
    if blocks and first_num == 2 and intro:
        # The rep numbered from 2 — flavour 1 exists but unnumbered (its spec
        # follows a 'Customer want X:' style line). Recover it, or refuse to
        # structure at all: a 2-of-3 block list writes the wrong flavour
        # count into MMS (the Pran 3-flavour request shipped as '2 flavours').
        from rapidfuzz import fuzz
        blk_sq = [re.sub(r"[^a-z0-9]", "", b["name"].lower()) for b in blocks]
        missing = []  # leading short name-list lines matching no block
        for line in intro:
            s = line.strip().rstrip(".")
            if not (0 < len(s) <= 48):
                break
            sq = re.sub(r"[^a-z0-9]", "", s.lower())
            if sq and not any(fuzz.ratio(sq, b) >= 90 for b in blk_sq):
                missing.append((s, sq))
        hdr_idx = -1
        if len(missing) == 1:
            name, name_sq = missing[0]
            for i in range(len(intro) - 1, 0, -1):
                if name_sq in re.sub(r"[^a-z0-9]", "", intro[i].lower()):
                    hdr_idx = i
                    break
        if hdr_idx > 0:
            blocks.insert(0, {"name": name, "spec": intro[hdr_idx + 1:]})
            intro = intro[:hdr_idx + 1]
        else:
            return  # can't recover flavour 1 — verbatim beats a wrong count
    if len(blocks) < 2:
        return  # nothing to structure confidently
    # Drop an intro line that just re-lists the flavour names (Alex's head
    # line 'Lemon Habanero Seasoning. Jalapeno … . Salsa Verde …').
    names_sq = re.sub(r"[^a-z0-9]", "", " ".join(b["name"] for b in blocks).lower())
    kept_intro = []
    for line in intro:
        line_sq = re.sub(r"[^a-z0-9]", "", line.lower())
        if line_sq and names_sq:
            overlap = sum(1 for i in range(0, len(line_sq) - 3, 4)
                          if line_sq[i:i + 4] in names_sq)
            if overlap / max(len(line_sq) // 4, 1) >= 0.6:
                continue  # redundant re-listing — the blocks carry the names
        kept_intro.append(line)
    a.flavours = blocks
    a.structured = True
    body[:] = kept_intro  # remaining ask_text = intro only; blocks render
    #                        separately in render_reqnote


def parse_ask(text: str) -> Ask:
    """No-LLM fallback parser. Handles MULTI-LINE messages: the first line
    (or first comma/dash segment) names the customer; every line is scanned
    for explicit field values (budget, compliance, qty, bag, ship-to,
    need-by) which are EXPLICIT — they always beat derived values. A field
    keyword we can see but can't read becomes a HINT: the draft shows
    'please confirm' instead of a confidently wrong derived number.
    The old ';key: value' syntax still works, silently."""
    a = Ask()
    # legacy ';key: value' segments first (silent compatibility). ONLY a
    # segment that actually looks like an override is consumed — any other
    # ';' is ordinary punctuation and its text is KEPT. (This used to keep
    # semi[0] only, so 'Lemon Habanero; Jalapeno; Salsa Verde' silently
    # truncated the request to one flavour and dropped every later line.)
    semi = text.split(";")
    kept = [semi[0]]
    for seg in semi[1:]:
        k, sep, v = seg.partition(":")
        key = k.strip().lower()
        if sep and "\n" not in seg and key in _OVR_KEYS:
            a.overrides["addr" if key == "address" else key] = v.strip()
        else:
            kept.append(seg)
    text = ";".join(kept)

    # Strip the form's required-field stars before ANY field matching.
    text = _STAR.sub("", text)
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if not lines:
        return a
    # Section headers ('Seasoning name' / 'Qty of sample') own the lines
    # beneath them until the next labelled line — Alex's standard form.
    flat: list[str] = []
    section = ""
    for ln in lines:
        if _HDR_SEASONING.match(ln):
            section = "seasoning"
            continue
        if _HDR_QTY.match(ln):
            section = "qty"
            continue
        if section and re.match(
                r"^(comment|budget|complian|target\s*base|send\s*method|"
                r"delivery|expected|need|receiver|contact|address|restriction)",
                ln, re.I):
            section = ""
        if section == "seasoning":
            # A quantity line inside the seasoning list is still a
            # quantity ('1 pkt- Texture 2 wheat flour pellets'), not the
            # name of a seasoning — otherwise it becomes an item AND
            # picks up a fabricated default amount.
            _qi = _QTY_ITEM_RE.match(ln)
            _iq = _ITEM_QTY_RE.match(ln) if not _qi else None
            if _qi or _iq:
                _q = (_qi.group(1) if _qi else _iq.group(2)).strip()
                _n = (_qi.group(2) if _qi else _iq.group(1)).strip()
                a.item_qty.append((_norm_qty(_q), _n))
                continue
            a.items.append(ln.rstrip("."))
            continue
        if section == "qty":
            qi = _QTY_ITEM_RE.match(ln)
            iq = _ITEM_QTY_RE.match(ln) if not qi else None
            if qi or iq:
                q = (qi.group(1) if qi else iq.group(2)).strip()
                n = (qi.group(2) if qi else iq.group(1)).strip()
                a.item_qty.append((_norm_qty(q), n))
                continue
            # A bare amount under the heading ('1 small bottle', '100g
            # sample') is the quantity itself — it belongs to the item
            # being discussed, not to a name on the same line.
            if _BARE_AMOUNT_RE.match(ln):
                a.item_qty.append((_norm_qty(ln), ""))
                continue
            section = ""
        flat.append(ln)
    lines = flat or lines
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
            a.overrides["need_by"] = (
                nm.group(1) or nm.group(2) or nm.group(3) or "").strip()
            consumed = consumed or len(line) < 60
        rc = _RECEIVER_LINE.match(line)
        if rc and len(line) < 90 and "attn" not in a.overrides:
            a.overrides["attn"] = rc.group(1).strip()
            consumed = True
        am = _ATTN_LINE.search(line)
        if am and "attn" not in a.overrides:
            a.overrides["attn"] = am.group(1).strip()
            consumed = True
        km = _CONTACT_LINE.search(line)
        if km:
            a.overrides["contact"] = km.group(1).strip()
            consumed = True
        dm = _ADDR_LINE.search(line)
        if dm:
            a.overrides["addr"] = (dm.group(1) or dm.group(2) or "").strip()
            consumed = True
        # Per-item quantity FIRST ('500g - texture improver 2'): the
        # generic scan below used to swallow these short lines as a bare
        # qty, losing which item they belonged to (Alex 02-Sep).
        _qi = _QTY_ITEM_RE.match(line)
        _iq = _ITEM_QTY_RE.match(line) if not _qi else None
        if (_qi or _iq) and len(line) < 90 and not _DOSAGE_LINE.search(line):
            _qty_txt = (_qi.group(1) if _qi else _iq.group(2)).strip()
            _name = (_qi.group(2) if _qi else _iq.group(1)).strip()
            if _name and len(_name) < 60 and not _QTY_RE.fullmatch(_name):
                a.item_qty.append((_norm_qty(_qty_txt), _name))
                continue
        qm = None if _DOSAGE_LINE.search(line) else _QTY_RE.search(line)
        if qm and a.qty_g is None:
            n = float(qm.group(1))
            a.qty_g = int(n * 1000) if qm.group(2).lower() == "kg" else int(n)
            if re.search(r"\beach\b|\bper\b", line, re.I):
                a.qty_each = True
            consumed = consumed or len(line) < 30
        if not consumed:
            body.append(line)

    # base / restriction / delivery / per-item qty — corpus-standard fields
    kept: list[str] = []
    for line in body:
        bm2 = _BASE_LINE.match(line)
        if bm2 and not a.base:
            a.base = (bm2.group(1) or bm2.group(2) or "").strip()
            continue
        rm2 = _RESTRICTION_LINE.match(line)
        if rm2 and not a.restriction and len(line) < 90:
            a.restriction = (rm2.group(1) or rm2.group(2) or "").strip()
            continue
        # Alex 02-Sep: delivery METHOD and delivery ADDRESS are separate
        # inputs. 'To courier to Geylang.' → method 'Courier',
        # address 'Geylang'.
        da2 = _DELIVERY_ADDR_LINE.match(line)
        if da2 and len(line) < 200 and not _METHOD_WORD.match(da2.group(1)):
            a.delivery_addr = da2.group(1).strip().rstrip(".")
            continue
        dm2 = _DELIVERY_LINE.match(line)
        if dm2 and len(line) < 200:
            val = (dm2.group(1) or dm2.group(2) or "").strip()
            if val:
                dest = _METHOD_DEST.search(val)
                if dest and not a.delivery_addr:
                    a.delivery_addr = dest.group(1).strip().rstrip(".")
                mw = _METHOD_WORD.search(val)
                if mw:
                    meth = mw.group(1).strip().rstrip(".")
                    a.delivery = (meth[:1].upper() + meth[1:]) or a.delivery
                elif not dest:
                    a.delivery = (a.delivery + " " + val).strip()
            continue
        # per-item quantity, either order ('500g - X' / 'X - 500g')
        qi = _QTY_ITEM_RE.match(line)
        iq = _ITEM_QTY_RE.match(line) if not qi else None
        if (qi or iq) and len(line) < 90:
            qty_txt = (qi.group(1) if qi else iq.group(2)).strip()
            name = (qi.group(2) if qi else iq.group(1)).strip()
            # guard: don't swallow spec prose ('lemon - sharp citrus kick')
            if name and len(name) < 60:
                a.item_qty.append((_norm_qty(qty_txt), name))
                continue
        kept.append(line)
    body = kept
    # Trailing unlabelled block after the contact line is the ship-to
    # address (Alex's form ends with the company name + street address,
    # often in Chinese). Only claimed when we have a contact/receiver and
    # no address yet — otherwise ordinary prose would be swallowed.
    if ("addr" not in a.overrides
            and ("contact" in a.overrides or "attn" in a.overrides)):
        tail: list[str] = []
        for line in reversed(body):
            s = line.strip()
            if not s:
                break
            if _CJK.search(s) or re.search(r"\d{3,}|road|rd\.|street|st\.|"
                                           r"ave|avenue|jalan|lorong|no\.|"
                                           r"district|province|city", s, re.I):
                tail.insert(0, s)
            else:
                break
        if tail:
            a.overrides["addr"] = "\n".join(tail)
            body = body[:len(body) - len(tail)]
    # explicit 'no prefer code' anywhere in the message (never inferred)
    if _NO_PREFER_CODE.search(text):
        a.no_prefer_code = True
    body = [l for l in body if not _NO_PREFER_CODE.match(l.strip())]
    # bare item names: short lines that aren't sentences — the unnumbered
    # equivalent of a flavour list ('Tomato seasoning' / 'Texture improver 2')
    for line in body:
        s = line.strip().rstrip(".")
        if (2 <= len(s.split()) <= 6 and len(s) < 55
                and not s.endswith((":", ","))
                and not re.search(r"[.!?]\s", s)
                and not re.match(r"^(comment|budget|compliance|target|note)",
                                 s, re.I)):
            a.items.append(s)

    _extract_shipto(a, body)
    _structure_body(a, body)
    a.ask_text = "\n".join(body).strip()
    # Codes/qty are scanned over the FULL original text, not the post-
    # structure ask_text — structuring moves flavour blocks out of ask_text,
    # which blinded the code/'modify' detection ('closer to S-18CS43-002'
    # inside a numbered block yielded rtype=New with no base code). Digit
    # requirement: every real MMS code has one; 'T-bone' does not, and it
    # used to trigger a bogus Singapore-only refusal.
    blob = text + " "
    a.codes = [c.upper() for c in _CODE_RE.findall(blob)
               if any(ch.isdigit() for ch in c)]
    if a.qty_g is None:
        for _bl in blob.splitlines():
            if _DOSAGE_LINE.search(_bl):
                continue
            # A per-item line ('500g - texture improver 2') belongs to THAT
            # item — it must not become the request-wide default, or the
            # other item silently inherits the wrong figure (Alex 02-Sep).
            _s = _bl.strip()
            if _QTY_ITEM_RE.match(_s) or _ITEM_QTY_RE.match(_s):
                continue
            m2 = _QTY_RE.search(_bl)
            if m2:
                n = float(m2.group(1))
                a.qty_g = int(n * 1000) if m2.group(2).lower() == "kg" else int(n)
                if re.search(r"\beach\b", blob, re.I):
                    a.qty_each = True
                break
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
"cheap as possible" etc → budget as stated. Never invent values.
Keep the rep's line breaks in "ask" (use \n) — R&D reads it as written,
and numbered flavour lists must stay numbered lines. If he numbers
multiple flavours (1. name - spec, 2. name - spec...), extract each as
a distinct name+spec pair — the renderer builds the 'Seasoning name:' /
'Comment:' blocks itself; don't rewrite them into that shape yourself."""

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
        # 1000, not 300: the JSON echoes the whole ask, and a 3-flavour spec
        # alone is ~400 tokens — truncation made _json_from fail on exactly
        # the long messages this parser exists for (regex fallback hid it).
        out, _, _ = await ai._ask(  # noqa: SLF001 — house helper
            _PARSE_PROMPT.format(text=text), max_tokens=1000)
        d = _json_from(out)
        if d and d.get("customer") and d.get("ask"):
            return d
    except Exception as e:  # noqa: BLE001
        log.warning("SR llm_parse failed: %s", e)
    return None


async def llm_update(draft: dict, text: str) -> dict | None:
    """Interpret a reply to an active draft. Returns None when the LLM is
    UNAVAILABLE (caller may then try the regex fallback); a real LLM verdict
    of 'unrelated' is returned as-is and must be respected — re-running the
    loose regex fallback on top of it hijacked ordinary product searches
    ('cheese b code below 4usd' became a silent budget edit)."""
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
    return None


_CONFIRM_RE = re.compile(
    r"^(ok(ay)?\b[\s,.!]*)?(yes\b[\s,.!]*)?"
    r"(raise|submit|send|confirm|go ahead|proceed)(\s+it)?[\s.!]*$",
    re.IGNORECASE)
_DISCARD_RE = re.compile(
    r"^(cancel|discard|never\s?mind|drop it|forget it)\b", re.IGNORECASE)
_QTY_REPLY_RE = re.compile(
    r"^(?:make it |change (?:it )?to )?(\d+(?:\.\d+)?)\s*(kg|g)\b"
    r"(\s*each)?[\s.!]*$", re.IGNORECASE)


# What may sit BEFORE a bare value for the fallback to treat it as a draft
# edit ("make it <2 usd", "budget <2 usd", "<2 usd") — a product keyword
# there ("cheese b code below 4usd") means it's a SEARCH, not an edit.
_EDIT_PREFIX_RE = re.compile(
    r"^\s*(?:ok(?:ay)?\b[\s,]*)?(?:please\b\s*)?"
    r"(?:make\s+(?:it|the\s+budget)|set(?:\s+it)?|budget|"
    r"change\s+(?:it\s+)?to)?\s*$", re.IGNORECASE)


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
        # Bare forms ("<2 usd" / "cheap as possible") count as an edit ONLY
        # when nothing search-like precedes them, and the cheap-form must BE
        # the whole message ('cheapest chicken powder' is a search).
        ok = bool(bm.group(1))
        if not ok and val:
            ok = bool(_EDIT_PREFIX_RE.match(t[:bm.start()]))
            if ok and bm.group(3):
                ok = bm.end() >= len(t) - 2
        if ok and val:
            fields["budget"] = val
    cm = _COMPLIANCE_LINE.search(t)
    if cm:
        val = _normalize_markets(cm.group(1))
        if val:
            fields["compliance"] = val
    nm = _NEEDBY_LINE.search(t)
    if nm and len(t) < 60:
        # A bare urgency word ('asap', 'next week') is only an edit when it
        # is essentially the whole message — inside a longer sentence it's
        # probably a search or small talk.
        if nm.group(1) or len(t) < 25:
            fields["need_by"] = ((nm.group(1) or nm.group(2)) or "").strip().upper()
    am = _ATTN_LINE.search(t)
    if am and len(t) < 60:
        fields["attn"] = am.group(1).strip()
    km = _CONTACT_LINE.search(t)
    if km and len(t) < 60:
        fields["contact"] = km.group(1).strip()
    dm = _ADDR_LINE.search(t)
    if dm:
        fields["addr"] = (dm.group(1) or dm.group(2) or "").strip()
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
                         ("ship-to", draft["attn"] or draft["addr"]),
                         ("need-by", draft["need_by"]))
                        if not val]


def parsed_to_text(parsed: dict) -> str:
    """Rebuild a canonical '/sr' line from an LLM parse so build_draft's
    existing pipeline (regex overrides included) stays the single path."""
    bits = [f"{parsed['customer']} — {parsed['ask']}"]
    if parsed.get("qty_g"):
        bits[0] += f" {parsed['qty_g']}g"
    if parsed.get("sets"):
        bits[0] += f" x {parsed['sets']}"
    # need_by included: it used to survive only via cmd_sr's post-build
    # fix-up, so the pending-pick rebuild silently dropped 'need it next
    # week' back to STANDARD.
    keymap = {"bag": "bag", "budget": "budget", "compliance": "compliance",
              "attn": "attn", "contact": "contact", "addr": "addr",
              "rtype": "type", "base_code": "base", "need_by": "need_by"}
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


# Geography a rep adds to place a customer ('Fujian Pan pan', 'PAN PAN
# CHINA'). Real but weak identity: the BRAND token is what distinguishes
# one customer from another, so place words must never outrank it.
_PLACE_TOKENS = {
    "china", "prc", "fujian", "guangdong", "shanghai", "beijing", "jiangsu",
    "zhejiang", "shandong", "quanzhou", "jinjiang", "xiamen", "singapore",
    "malaysia", "indonesia", "thailand", "vietnam", "japan", "korea",
    "india", "bangladesh", "philippines", "myanmar", "nepal", "taiwan",
    "hongkong", "dubai", "uae", "australia", "jakarta", "bangkok", "hanoi",
    "saigon", "penang", "selangor", "johor", "klang", "dhaka", "mumbai",
    "delhi", "asia", "asian", "pacific", "sdn", "bhd",
}


def _distinct_tokens(s: str) -> frozenset:
    return frozenset(t for t in re.findall(r"[a-z0-9]+", s.lower())
                     if t not in _GENERIC_TOKENS and len(t) >= 3)


def _brand_tokens(s: str) -> frozenset:
    """Distinctive tokens with geography removed — the customer's identity."""
    return frozenset(t for t in _distinct_tokens(s) if t not in _PLACE_TOKENS)


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
    # Secondary sweep: when the fuzzy pool covers the rep's words poorly,
    # scan ALL known names for ones covering more of them. 'Fujian Pan pan'
    # never surfaced '(PAN PAN CHINA)' because no single fuzzy/containment
    # rule fires — the distinctive words are there, just worded differently.
    # Score on BRAND tokens (geography stripped) so 'Fujian Pan pan' is
    # judged on 'pan', not on the province it shares with unrelated firms.
    _q_all = _brand_tokens(q) or _distinct_tokens(q)
    if _q_all:
        from rapidfuzz import fuzz as _fz

        def _cov_of(name: str) -> int:
            n_toks = _brand_tokens(name) or _distinct_tokens(name)
            return sum(1 for qt in _q_all
                       if any(qt in nt or nt in qt or _fz.ratio(qt, nt) >= 85
                              for nt in n_toks))

        best_cov = max((_cov_of(h.get("name", "")) for h in hits), default=0)
        # Sweep when the pool covers the rep's words poorly OR when nothing
        # in it is an exact brand match — 'Fujian Pan pan' scored a full 1/1
        # via 'Pan Seas Enterprises', which merely shares the word 'pan',
        # so a coverage test alone never looked for the real '(Pan Pan)'.
        _has_exact = any(
            (_brand_tokens(h.get("name", ""))
             or _distinct_tokens(h.get("name", ""))) == _q_all for h in hits)
        if best_cov < len(_q_all) or not _has_exact:
            seen_l = {h["name"].strip().lower() for h in hits}
            pool = [(c["name"], c) for c in merged]
            pool += [(n, {"name": n, "code": ""}) for n in fsl_names]
            extra = []
            for name, rec in pool:
                if name.strip().lower() in seen_l:
                    continue
                cov = _cov_of(name)
                # Better coverage, OR the same coverage with an EXACT brand
                # identity ('Fujian Pan pan' → '(Pan Pan)' is {pan} == {pan},
                # while 'Pan Seas Enterprises' is {pan, seas} — a different
                # company that merely shares the word).
                exact_brand = (_brand_tokens(name) or _distinct_tokens(name)) \
                    == _q_all
                if cov > best_cov or (cov >= best_cov and exact_brand):
                    extra.append((cov + (1 if exact_brand else 0),
                                  {**rec, "score": 95 if exact_brand else 90}))
            extra.sort(key=lambda t: -t[0])
            for _c, rec in extra[:3]:
                hits.append(rec)

    # Coverage re-rank: prefer the candidate that accounts for MORE of the
    # rep's distinctive words. 'Fujian Pan pan' → '(PAN PAN CHINA)' covers
    # {pan}; 'FUJIAN ZHAOLU TRADING' covers only {fujian} while scoring
    # higher on raw fuzz because the geographic word is long. Repeated
    # words count once, so 'pan pan' doesn't outrank on repetition alone.
    _q_toks = _brand_tokens(q) or _distinct_tokens(q)

    def _coverage(name: str) -> int:
        from rapidfuzz import fuzz as _f
        n_toks = _brand_tokens(name) or _distinct_tokens(name)
        hit = 0
        for qt in _q_toks:
            if any(qt in nt or nt in qt or _f.ratio(qt, nt) >= 85
                   for nt in n_toks):
                hit += 1
        return hit

    if _q_toks:
        for x in hits:
            x["_cov"] = _coverage(x.get("name", ""))
        hits.sort(key=lambda x: (-(x.get("_cov") or 0),
                                 -(x.get("score") or 0)))
    else:
        hits.sort(key=lambda x: -(x.get("score") or 0))
    if not hits:
        return None, []
    # Dominant match: a candidate whose DISTINCTIVE tokens equal the query's
    # exactly IS the customer — never ask. 'pran food' → {'pran'} equals
    # 'Pran Foods Ltd' → {'pran'}, so the FSL alias '(F.B.M Technologies
    # Ltd (Pran Foods))' ({'technologies','pran'}) can't force a pointless
    # 'which one did you mean?'. Ties (spelling variants of the same entity)
    # go to the best fuzzy score, then to the entry with a customer code.
    q_set = _brand_tokens(q) or _distinct_tokens(q)
    if q_set:
        exact = [x for x in hits
                 if (_brand_tokens(x.get("name", ""))
                     or _distinct_tokens(x.get("name", ""))) == q_set]
        if exact:
            exact.sort(key=lambda x: (-(x.get("score") or 0),
                                      not x.get("code")))
            return exact[0], hits[:3]
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
            and not _MOD_RE.search(ask.body_text()):
        d["rtype"], d["rtype_label"] = "rep", "Repeat"
        d["base_code"] = ask.codes[0]
    elif ask.codes and _MOD_RE.search(ask.body_text()):
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
                             reqnote: str, assignee: str,
                             prepdate: str = "") -> dict:
        """additem → fill new section → request{N} → assign → save.
        Every step verified; cleanup via command=clear if we bail after
        additem (clear only removes EMPTY items — exactly our failure
        window). `prepdate` ('13/Sep/2026') sets the section's 'until'
        target-date dropdown when MMS offers that date.
        Returns {'ok':bool, 'detail':str, 'section':int}."""
        # MMS turns CRLF into <BR> on save — that's how browser-typed notes
        # get their line breaks. Bare \n is stored raw and the rendered
        # page collapses it into one unreadable paragraph.
        reqnote = reqnote.replace("\r\n", "\n").replace("\n", "\r\n")
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
            # verify: our text is on the page and the section saved.
            # Compare against the UNESCAPED page ('&' arrives as '&amp;',
            # which made every ask containing & fail verification and
            # invite a duplicate raise), and probe with a line that is
            # unique to THIS request — 'No prefer code.' appears in older
            # items on the same SR, so it proved nothing.
            import html as _html
            probe_lines = [l.strip() for l in reqnote.splitlines()
                           if l.strip() and l.strip() != "No prefer code."]
            probe = (probe_lines[0] if probe_lines else "")[:40]
            if probe and probe not in _html.unescape(html3):
                return self._bail(sr_code,
                                  f"request{n}: submitted text not found on page")
            # assign + save (+ the 'until' target-date dropdown)
            form3 = BeautifulSoup(html3, "html.parser").find("form")
            fill3 = {f"sreq1[{n}].nextActUserId": aid, "command": "save"}
            if prepdate:
                blk = re.search(
                    rf'name="sreq1\[{n}\]\.prepdateString".*?</select>',
                    html3, re.S)
                if blk and f'value="{prepdate}"' in blk.group(0):
                    fill3[f"sreq1[{n}].prepdateString"] = prepdate
            html4 = self._post(sr_code, _override(_form_payload(form3), fill3))
            sel = re.search(
                rf'name="sreq1\[{n}\]\.nextActUserId".*?'
                rf'<option value="{aid}" selected',
                html4, re.S,
            )
            if not sel:
                return {"ok": False, "section": n + 1,
                        "detail": ("request saved but assignee NOT confirmed "
                                   "selected — set it manually in MMS")}
            prep_note = ""
            if prepdate:
                blk4 = re.search(
                    rf'name="sreq1\[{n}\]\.prepdateString".*?</select>',
                    html4, re.S)
                if blk4 and re.search(
                        rf'value="{re.escape(prepdate)}" selected',
                        blk4.group(0)):
                    prep_note = f", until {prepdate}"
                else:
                    prep_note = (f" — ⚠ couldn't set the until-date "
                                 f"({prepdate}), pick it in MMS")
            return {"ok": True, "section": n + 1,
                    "detail": (f"item ({n + 1}) raised, assigned to "
                               f"{assignee}{prep_note}")}
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
        # 'clear' removes only EMPTY items — if the reqnote had already
        # saved, the item is still there. Never claim it was removed.
        return {"ok": False, "detail": why + " (cleanup attempted — check "
                                             "the SR in MMS before retrying)"}

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
        if mapping:
            # Never cache an EMPTY map — one bad fetch (login redirect,
            # 5xx body) used to disable live SR lookup for the whole
            # process lifetime with no visible error.
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


async def screenshot_sr(session, sr_code: str, section: int | None = None):
    """Best-effort PNG of the SR page after a submit — visual proof for
    the rep that the item really is in MMS.

    Reuses the ALREADY-LOGGED-IN requests session's cookies in a headless
    Chromium (the same binary the AWB scrape ships) — no credential entry
    anywhere. `section` (1-based, from add_item_and_request's result)
    clips the shot to that item: from its "Next action by" select down to
    its "until" dropdown. Falls back to the full page, then to None.
    Never raises — a screenshot hiccup must not touch the submit flow.
    """
    try:
        from playwright.async_api import async_playwright  # noqa: WPS433
    except ImportError:
        log.info("playwright unavailable — skipping SR screenshot")
        return None
    url = f"{mms_client.BASE_URL}/master/sampleRequestUpdate.do?code={sr_code}"
    try:
        cookies = [{
            "name": c.name, "value": c.value,
            "domain": c.domain or "www.npsin.com",
            "path": c.path or "/",
        } for c in session.cookies]
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True,
                                              args=["--no-sandbox"])
            try:
                ctx = await browser.new_context(
                    viewport={"width": 1280, "height": 1400})
                await ctx.add_cookies(cookies)
                page = await ctx.new_page()
                await page.goto(url, timeout=30000,
                                wait_until="domcontentloaded")
                clip = None
                if section is not None:
                    n = section - 1
                    try:
                        top = await page.locator(
                            f'select[name="sreq1[{n}].nextActUserId"]'
                        ).bounding_box(timeout=5000)
                        low = await page.locator(
                            f'select[name="sreq1[{n}].prepdateString"]'
                        ).bounding_box(timeout=5000)
                        if top and low:
                            y0 = max(0.0, top["y"] - 60)
                            y1 = low["y"] + low["height"] + 40
                            clip = {"x": 0, "y": y0, "width": 1280,
                                    "height": max(200.0, y1 - y0)}
                    except Exception:  # noqa: BLE001 — layout drift → full page
                        clip = None
                return await page.screenshot(clip=clip,
                                             full_page=clip is None,
                                             timeout=15000)
            finally:
                await browser.close()
    except Exception as e:  # noqa: BLE001
        log.warning("SR screenshot failed (submit unaffected): %s", e)
        return None


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
    # A stated delivery address IS the ship-to — explicit always wins over
    # the customer-master default (Alex 02-Sep: courier-to-Geylang must not
    # sit next to the Dhaka master address).
    addr = pick("addr", ask.overrides.get("addr") or ask.delivery_addr,
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

    import time as _time
    token = secrets.token_hex(3)
    draft = {
        "token": token, "user_id": user_id, "customer": customer,
        "created_at": _time.time(),
        "sr_code": sr_code, "territory": territory, "prefix": prefix,
        "ask": ask, "derived": d, "bag": bag, "budget": budget,
        "compliance": compliance, "attn": attn, "contact": contact,
        "addr": addr, "assignee": assignee,
        "need_by": (ask.overrides.get("need_by") or "").upper(),
        "page_err": page_err, "src": src,
        # need-by has NO default (Alex, 01 Sep): the rep keys in when the
        # customer expects the seasoning; the bot writes it into the note
        # and sets the SR's until dropdown. Submit refuses while missing.
        "missing": [k for k, v in
                    (("bag", bag), ("ship-to", attn or addr),
                     ("need-by", ask.overrides.get("need_by")))
                    if not v],
        # Smart-fill status for Alex's standard form (each '*' field):
        # 'you' = he wrote it · a source label = the bot filled it from
        # history/master · '' = genuinely missing, must be asked.
        "fields": {
            "Seasoning name": ("you" if (ask.items or ask.flavours
                                         or ask.ask_text) else ""),
            "Comment": "you" if ask.ask_text else "",
            "Qty of sample": ("you" if (ask.item_qty or ask.qty_g)
                              else (d.get("qty_src") or "")),
            "Budget": src.get("budget", ""),
            "Compliance": src.get("compliance", ""),
            "Target base": "you" if ask.base else "",
            "Send method": "you" if ask.delivery else "",
            "Expected send by": "you" if ask.overrides.get("need_by") else "",
            "Receiver name": src.get("attn", ""),
            "Contact": src.get("contact", ""),
            "Address": src.get("addr", ""),
        },
        # Alex 02-Sep: 'for the qty segment need to make it clear if not
        # ask user again'. Several named items but not every one has a
        # stated quantity → ASK rather than silently applying one figure.
        "qty_ambiguous": bool(
            ask.item_qty and ask.items
            and len(ask.item_qty) < len({i.lower() for i in ask.items})
        ) or bool(
            len(ask.items) > 1 and not ask.item_qty and not ask.qty_each
            and not (ask.structured and ask.flavours)
        ),
    }
    DRAFTS[token] = draft
    return draft


# Intro lines the rendered note already states, so repeating them is the
# 01-Sep duplicate bug: 'Comment - no prefer code. Customer want X:' next
# to the renderer's own 'Comment: No prefer code.' header.
_NO_PREFER_RESTATE = re.compile(
    r"^\s*(?:comment\s*[-:–—]*\s*)?no\s+prefer(?:red)?\s+code[.\s]*"
    r"(?:customer\s+wants?\s+[^:]{0,80}:?\s*)?$", re.IGNORECASE)
_CUSTOMER_WANT_LINE = re.compile(
    r"^\s*(?:comment\s*[-:–—]*\s*)?customer\s+wants?\s+([^:]{1,80}):?\s*$",
    re.IGNORECASE)


def _filter_ask_text(ask_text: str, flavours: list, no_code: bool) -> str:
    """Drop intro lines the note already carries elsewhere: 'no prefer
    code' restatements (the Comment:/boilerplate line owns that) and
    'Customer want X:' headers where X is one of the numbered flavours."""
    names_sq = re.sub(r"[^a-z0-9]", "",
                      " ".join(f["name"] for f in flavours).lower())
    out = []
    for line in ask_text.splitlines():
        if no_code and _NO_PREFER_RESTATE.match(line):
            continue
        cw = _CUSTOMER_WANT_LINE.match(line)
        if cw and names_sq:
            want_sq = re.sub(r"[^a-z0-9]", "", cw.group(1).lower())
            if want_sq and want_sq in names_sq:
                continue
        out.append(line)
    return "\n".join(out).strip()


def render_reqnote(draft: dict) -> str:
    """The text written into MMS — the thing R&D actually reads.

    Multi-flavour asks are ONE item with numbered per-flavour blocks (the
    corpus convention: sections routinely carry 2-6 flavours). Layout
    follows Alex's requested shape: flavour blocks first — each owning its
    spec and its quantity — then the constraint footer (TARGET BASE /
    RESTRICTION / BUDGET / COMPLIANCE / QTY / ship-to). Note: Rich's
    corpus style puts constraints ABOVE the flavour list; we deviate to
    Alex's sketch since he's the author R&D reads and both layouts appear
    in the corpus. If the rep didn't number his flavours we DON'T guess a
    structure — his text goes through verbatim (never a mangled half-
    structure).
    """
    d = draft["derived"]
    ask = draft["ask"]
    qty_str = f"{d['qty']}g x {d['sets']} set" + ("s" if d["sets"] != 1 else "")
    # Alex 02-Sep: 'No prefer code.' is HIS phrase, not our inference —
    # emit it only when he actually wrote it, and only inside Comment.
    no_code = ask.no_prefer_code
    lines: list[str] = []
    if ask.structured and ask.flavours:
        # Alex's OWN convention, confirmed from his prior request already
        # on this SR ('Seasoning name: X / Comment: Y / Budget: Z /
        # Sample to be given to customer: Q', repeated per flavour when
        # budget/qty genuinely differ per flavour). Here budget/qty/
        # compliance are IDENTICAL across all flavours, so per his
        # correction they're consolidated once instead of repeated:
        # one 'Seasoning name:' block listing every name, one 'Comment:'
        # block holding the numbered spec list (with 'No prefer code.'
        # folded in as its opening line), shared footer once at the end.
        lines.append("Seasoning name:")
        lines.extend(f["name"].upper() for f in ask.flavours)
        lines.append("")
        comment_lead = "No prefer code." if no_code else ""
        lines.append(f"Comment: {comment_lead}".rstrip())
        lines.append("")
        for i, f in enumerate(ask.flavours, 1):
            lines.append(f"{i}. {f['name'].upper()} - {qty_str}")
            lines.extend(s for s in f["spec"] if s.strip())
            lines.append("")
        ask_txt = _filter_ask_text(ask.ask_text, ask.flavours, no_code)
        if ask_txt:
            lines.append(ask_txt)
            lines.append("")
    else:
        ask_txt = _filter_ask_text(ask.ask_text, [], no_code)
        # Alex's standard form opens with the seasoning list; keep that
        # header even when he didn't number them (unnumbered items are
        # still a list, just not per-flavour specs).
        if ask.items:
            lines.append("Seasoning name:")
            lines.extend(ask.items)
            lines.append("")
        if no_code:
            # Inside Comment, never as a bare first line. If he wrote his
            # own 'Comment-' line the body already carries it, so just
            # prefix; otherwise open the Comment block ourselves.
            if re.search(r"^\s*comment\s*[-:]", ask_txt, re.I | re.M):
                lines.append("Comment: No prefer code.")
            else:
                lines.append(f"Comment: No prefer code.")
            lines.append("")
        if ask_txt:
            lines.append(ask_txt)
            lines.append("")
    if ask.base:
        lines.append(f"TARGET BASE: {ask.base}")
    if ask.restriction:
        lines.append(f"RESTRICTION: {ask.restriction}")
    if draft["bag"]:
        lines.append(f"BAG: {draft['bag'].upper()}")
    if draft["budget"]:
        lines.append(f"BUDGET: {draft['budget']}")
    if draft["compliance"]:
        lines.append(f"COMPLIANCE: {draft['compliance']}")
    # QTY: each numbered header already carries '- {qty}' for structured
    # multi-flavour notes — repeating it in the footer was Alex's 01-Sep
    # duplicate complaint. Footer QTY only when the headers don't show it.
    if ask.item_qty:
        # Per-item quantities — Alex 02-Sep: '100g - Tomato seasoning,
        # 500g - Texture improver 2', never a bare figure that reads as
        # the whole request. Items he didn't price get the default qty so
        # every named item is accounted for explicitly.
        paired = [(q, n) for q, n in ask.item_qty if n]
        bare = [q for q, n in ask.item_qty if not n]
        named = {n.lower() for _, n in paired}

        def _claimed(item: str) -> bool:
            il = item.lower()
            return il in named or any(il in n or n in il for n in named)

        # Attach bare amounts ('1 small bottle' under the Qty heading) to
        # the items that don't yet have one, in the order both were given —
        # that IS the quantity for that item, not a loose figure.
        unpriced = [it for it in ask.items if not _claimed(it)]
        for it in list(unpriced):
            if not bare:
                break
            paired.append((bare.pop(0), it))
            unpriced.remove(it)
        parts = [f"{q} - {n}" for q, n in paired]
        parts += bare  # anything still unattached, kept verbatim
        # Only now fall back to the derived default, and only for items
        # the rep never quantified at all.
        for it in unpriced:
            parts.append(f"{d['qty']}g - {it}")
        lines.append("QTY: " + ", ".join(parts))
    elif not (ask.structured and len(ask.flavours) > 1):
        if ask.qty_each:
            lines.append(f"QTY: {qty_str} each")
        else:
            lines.append(f"QTY: {qty_str}")
    if draft["need_by"]:
        lines.append(f"NEED BY: {draft['need_by']}")
    # Delivery method / address are separate inputs. A courier drop-point
    # ('to Geylang') and the final receiver address can BOTH be given —
    # they're different places, so never collapse one into the other.
    deliv_addr = ask.delivery_addr or (draft["addr"] if ask.delivery else "")
    if ask.delivery or ask.delivery_addr:
        lines.append("")
        if ask.delivery:
            lines.append(f"Delivery method: {ask.delivery}")
        if deliv_addr:
            lines.append(f"Delivery address: {deliv_addr}")
        lines.append("")
    if draft["attn"]:
        lines.append(f"RECEIVER NAME: {draft['attn']}")
    if draft["contact"]:
        lines.append(f"CONTACT NO.: {draft['contact']}")
    # Print the receiver address unless the delivery block already showed
    # this exact text (then it would just be a duplicate).
    if draft["addr"] and draft["addr"].strip() != deliv_addr.strip():
        lines.append(f"ADDRESS: {draft['addr']}")
    # collapse any doubled blanks from empty optional groups
    out: list[str] = []
    for l in lines:
        if l == "" and (not out or out[-1] == ""):
            continue
        out.append(l)
    return "\n".join(out)


_MONTH_ABBR = ("jan", "feb", "mar", "apr", "may", "jun",
               "jul", "aug", "sep", "oct", "nov", "dec")


def need_by_prepdate(need_by: str, today=None) -> str:
    """Free-text NEED BY → MMS's 'until' dropdown format (13/Sep/2026).

    The SR form's target-date select (sreq1[N].prepdateString) only offers
    dates ~4 weeks out in DD/Mon/YYYY. Handles '13 SEP 2026', 'Sep 13',
    '13/9' and friends; anything vaguer ('ASAP', 'next week') returns ''
    and the dropdown is left alone — never guess a deadline. A missing
    year means the nearest upcoming occurrence."""
    import datetime as _dt
    s = (need_by or "").lower()
    today = today or _dt.date.today()
    months = "|".join(_MONTH_ABBR)
    d = mo = yr = None
    m = re.search(rf"(?<!\d)(\d{{1,2}})(?:st|nd|rd|th)?[\s/.\-]*({months})"
                  rf"[a-z]*[\s/.\-,]*(\d{{4}})?", s)
    if m:
        d, mo = int(m.group(1)), _MONTH_ABBR.index(m.group(2)) + 1
        yr = int(m.group(3)) if m.group(3) else None
    else:
        m = re.search(rf"\b({months})[a-z]*[\s/.\-,]*(?<!\d)(\d{{1,2}})"
                      rf"(?:st|nd|rd|th)?(?!\d)(?:[\s/.\-,]*(\d{{4}}))?", s)
        if m:
            mo, d = _MONTH_ABBR.index(m.group(1)) + 1, int(m.group(2))
            yr = int(m.group(3)) if m.group(3) else None
        else:
            m = re.search(r"(?<!\d)(\d{1,2})[/\-](\d{1,2})"
                          r"(?:[/\-](\d{2,4}))?(?!\d)", s)
            if m:  # day-first (SG convention); swap if only month-first fits
                d, mo = int(m.group(1)), int(m.group(2))
                yr = int(m.group(3)) if m.group(3) else None
                if yr is not None and yr < 100:
                    yr += 2000
                if mo > 12 >= d:
                    d, mo = mo, d
    if not d or not mo:
        return ""
    try:
        date = _dt.date(yr or today.year, mo, d)
        if yr is None and date < today:
            date = date.replace(year=today.year + 1)
    except ValueError:
        return ""
    return f"{date.day:02d}/{_MONTH_ABBR[date.month - 1].capitalize()}/{date.year}"


def remember_submitted(draft: dict) -> None:
    """Every submitted value becomes the customer's new default — this is
    the 'learn from every correction' loop: whatever Alex overrode this
    time is proposed next time."""
    c = draft["customer"]
    for key in ("bag", "budget", "compliance", "attn", "contact", "addr"):
        if draft.get(key):
            mem_set(c, key, draft[key])
