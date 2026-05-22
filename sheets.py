"""Google Sheets wrapper.

Lazy-loads gspread client, caches the seasoning master list for 1 hour,
exposes helpers for customer lookup / upsert and sample-log append.
"""
from __future__ import annotations

import json as _json
import logging
import os
import re
import time
from typing import Any


# Audit V1.13.14 — restore Chinese characters that were mangled by the
# old MMS scraper (mms_client._parse_js_object_body) before that bug
# was fixed. The scraper used to strip the leading backslash off
# `\uXXXX` escapes, leaving raw `uXXXX` ASCII in the sheet. Mojibake
# in MMS arrives as a CONTIGUOUS run (e.g. `u9C9Cu9999u6D77u82D4u5473`
# for 鲜香海苔味), so we match the whole run with one regex and decode
# every 5-char escape inside it. The lookbehind guards against false
# positives in legitimate text like "Yu1234" type strings; once the
# run is matched, we don't need to re-check the lookbehind for inner
# escapes.
#
# Audit fix #12 — require AT LEAST TWO consecutive escapes ({2,}).
# Previously {1,} matched single tokens like "u2024" inside legitimate
# product/customer names (e.g. a description that happens to mention
# year "u2024" or a unit code "u3000") and silently mojibake'd them
# into bizarre Unicode characters. All real MMS mojibake we've seen
# is multi-char Chinese (鲜香海苔味 etc.), so requiring ≥2 escapes
# kills the false-positive surface without losing any known case.
_MANGLED_UNICODE_RUN_RE = re.compile(
    r"(?<![A-Za-z0-9])((?:u[0-9A-Fa-f]{4}){2,})"
)


def _decode_mangled_unicode_run(m: "re.Match[str]") -> str:
    """Decode each `uXXXX` escape inside a contiguous matched run."""
    raw = m.group(1)
    chars = []
    # Each escape is exactly 5 chars: 'u' + 4 hex digits.
    for i in range(0, len(raw), 5):
        hex_str = raw[i + 1:i + 5]
        try:
            chars.append(chr(int(hex_str, 16)))
        except (ValueError, OverflowError):
            # Defensive — should never trip given the regex, but if we
            # ever encounter an invalid codepoint, keep the original.
            chars.append(raw[i:i + 5])
    return "".join(chars)


def restore_mangled_unicode(s: str) -> str:
    """Decode orphaned `uXXXX` fragments back to the original characters.

    Idempotent (a run of legitimate ASCII has no escapes to decode).
    Used at row-read time so every consumer (digest, search, /pp,
    /lastsample) sees clean names without needing a one-time backfill
    of the sheet.
    """
    if not s or "u" not in s:
        return s
    return _MANGLED_UNICODE_RUN_RE.sub(_decode_mangled_unicode_run, s)

import gspread
from google.oauth2.service_account import Credentials

import config

log = logging.getLogger(__name__)

_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

_client: gspread.Client | None = None
_seasoning_cache: tuple[float, list[dict[str, Any]]] | None = None
_SEASONING_TTL = 60 * 60  # 1 hour
_users_cache: tuple[float, list[dict[str, str]]] | None = None
_USERS_TTL = 5 * 60  # 5 min — short so added/removed users pick up quickly
_customers_cache: tuple[float, list[dict[str, str]]] | None = None
_CUSTOMERS_TTL = 5 * 60  # 5 min — invalidated immediately on upsert
_samples_cache: tuple[float, list[dict[str, Any]]] | None = None
_SAMPLES_TTL = 2 * 60  # 2 min — invalidated immediately on append
_past_submissions_cache: tuple[float, list[dict[str, str]]] | None = None
_PAST_SUBMISSIONS_TTL = 30 * 60  # 30 min — refresh sometimes, not too often


def _get_client() -> gspread.Client:
    global _client
    if _client is None:
        # On cloud deployments (e.g. Railway) the service-account file isn't
        # present — the full JSON is passed as the env var
        # GOOGLE_SERVICE_ACCOUNT_JSON instead.
        sa_json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
        if sa_json_str:
            sa_info = _json.loads(sa_json_str)
            creds = Credentials.from_service_account_info(sa_info, scopes=_SCOPES)
        else:
            creds = Credentials.from_service_account_file(
                config.GOOGLE_SERVICE_ACCOUNT_FILE, scopes=_SCOPES
            )
        _client = gspread.authorize(creds)
    return _client


def _open_ops():
    return _get_client().open_by_key(config.OPS_SHEET_ID)


def ensure_ops_tabs() -> None:
    """Create Customers / Sales Log / Users tabs with headers if missing.

    Also self-heals: when a new column is added to the code-side constants
    (e.g. V0.4.0 added "Deadline" to SALES_LOG_COLS) the matching column is
    appended at the END of the existing header row. We never insert mid-way
    or reorder — doing so would misalign every existing row.
    """
    sh = _open_ops()
    existing = {ws.title for ws in sh.worksheets()}

    specs = [
        (config.TAB_CUSTOMERS, config.CUSTOMER_COLS),
        (config.TAB_SALES_LOG, config.SALES_LOG_COLS),
        (config.TAB_USERS, config.USER_COLS),
    ]
    for title, cols in specs:
        if title in existing:
            ws = sh.worksheet(title)
            first = ws.row_values(1)
            if not first:
                ws.append_row(cols)
            else:
                current = [str(c).strip() for c in first]
                missing = [c for c in cols if c not in current]
                if missing:
                    new_header = current + missing
                    # Make sure the sheet is wide enough for the extra columns.
                    if ws.col_count < len(new_header):
                        ws.add_cols(len(new_header) - ws.col_count)
                    ws.update("A1", [new_header])
                    log.info(
                        "Added %d missing column(s) to %r: %s",
                        len(missing), title, ", ".join(missing),
                    )
        else:
            ws = sh.add_worksheet(title=title, rows=1000, cols=max(len(cols), 10))
            ws.append_row(cols)

    # Delete the default "Sheet1" if it's empty and we just populated tabs.
    try:
        default = sh.worksheet("Sheet1")
        if default.row_count and not any(default.row_values(1)):
            sh.del_worksheet(default)
    except gspread.WorksheetNotFound:
        pass


# ---------- Seasoning master ----------

def load_seasonings(force: bool = False) -> list[dict[str, Any]]:
    """Return the seasoning master list — all tabs merged, copies skipped.

    Each item gets a `category` (the tab name) so suggestions can show it.
    """
    global _seasoning_cache
    now = time.time()
    if not force and _seasoning_cache and now - _seasoning_cache[0] < _SEASONING_TTL:
        return _seasoning_cache[1]

    sh = _get_client().open_by_key(config.SEASONING_SHEET_ID)
    cleaned: list[dict[str, Any]] = []
    per_tab: list[tuple[str, int]] = []
    for ws in sh.worksheets():
        if "copy" in ws.title.lower():
            continue
        try:
            rows = ws.get_all_records()
        except Exception as e:  # noqa: BLE001
            log.warning("Skipping tab %r: %s", ws.title, e)
            continue
        count = 0
        for r in rows:
            norm = {str(k).strip(): v for k, v in r.items()}
            name = str(norm.get(config.SEASONING_COL_NAME, "")).strip()
            if not name:
                continue
            cleaned.append(
                {
                    "name": name,
                    "price": str(norm.get(config.SEASONING_COL_PRICE, "")).strip(),
                    "code": str(norm.get(config.SEASONING_COL_CODE, "")).strip(),
                    "category": ws.title,
                }
            )
            count += 1
        per_tab.append((ws.title, count))
    _seasoning_cache = (now, cleaned)
    log.info(
        "Loaded %d seasonings across %d tabs (%s)",
        len(cleaned),
        len(per_tab),
        ", ".join(f"{t}:{c}" for t, c in per_tab),
    )
    return cleaned


# ---------- Full Sample Listing (transactional sample-out log) ----------

FSL_TAB = "Full Sample Listing"                  # Singapore / S- codes (default)
JAKARTA_FSL_TAB = "Full Sample Listing Jakarta"  # Indonesia / J- codes (V1.11.0)
BANGKOK_FSL_TAB = "Full Sample Listing Thailand" # Thailand / B- codes (V1.13.0)
FSL_HEADER = [
    "Sales", "Customer Name", "Country", "Product Code", "Product Name",
    "Quantity (g)", "Sample Date Out", "Taste describe", "Category",
    "R&D Price",
    # V1.16.0 — filled by the twice-daily AWB sync (awb_sync.py) from the
    # DHL Express + FedEx shipping portals. Matched by customer name +
    # sample date out (±2 days). Initial value on append is "".
    "AWB",
    # V1.17.0 — customer mailing address as printed on the DHL Ship-To
    # label. Captured opportunistically during AWB sync (DHL only — the
    # FedEx grid doesn't expose full address). Drives the quote_web
    # form's address auto-fill: when a rep picks a customer in the
    # quotation builder, this value pre-populates the address textarea.
    # Latest shipment wins (overwrites on each match).
    "Customer Address",
]

# Code-prefix → tab routing. Used by /pp's FSL fallback and by the sync
# engine to send each MMS row to the right tab. Single source of truth so
# we don't have to teach every helper the prefix rules.
#
# V1.13.0: B-prefix codes were previously assumed to be legacy Singapore
# (defaulted to FSL_TAB). User clarified that B- = Thailand factory, so
# the routing now sends them to BANGKOK_FSL_TAB. The 205 stranded B-rows
# that lived in FSL_TAB before this change were migrated by
# _thailand_migrate_stranded.py.
FSL_TAB_BY_PREFIX = {
    "S": FSL_TAB,
    "J": JAKARTA_FSL_TAB,
    "B": BANGKOK_FSL_TAB,
}


def fsl_tab_for_code(code: str) -> str:
    """Return the FSL tab name a product code belongs to, defaulting to
    the main FSL_TAB when the prefix is unknown."""
    code = (code or "").strip().upper()
    if not code:
        return FSL_TAB
    return FSL_TAB_BY_PREFIX.get(code.split("-", 1)[0], FSL_TAB)
# Index positions for the 10-col layout (0-indexed in the values matrix).
FSL_COL_SALES = 0
FSL_COL_CUSTOMER = 1
FSL_COL_COUNTRY = 2
FSL_COL_CODE = 3
FSL_COL_NAME = 4
FSL_COL_QTY = 5
FSL_COL_DATE = 6
FSL_COL_TASTE = 7
FSL_COL_CATEGORY = 8
FSL_COL_RD_PRICE = 9
FSL_COL_AWB = 10
FSL_COL_ADDR = 11  # V1.17.0 — Customer Address (col L), filled by AWB sync from DHL ship-to


def _open_seasoning_master():
    """Open the workbook that holds Full Sample Listing + the 6 category tabs."""
    return _get_client().open_by_key(config.SEASONING_SHEET_ID)


def _norm_customer(name: str) -> str:
    return " ".join((name or "").lower().split())


def load_fsl_dedupe_keys(tab: str = FSL_TAB) -> set[tuple[str, str, str]]:
    """Return the set of (sample_date_out, product_code, normalized_customer)
    tuples for every row already in the target FSL tab.

    Kept as a thin wrapper so callers that only want dedupe don't have to
    pay for the full state load. sync_engine prefers ``load_fsl_state()``.
    """
    return load_fsl_state(tab=tab)["dedupe_keys"]


def load_fsl_customer_country_map(tab: str = FSL_TAB) -> dict[str, str]:
    """Back-compat wrapper. Prefer ``load_fsl_state()`` which returns this
    plus the taste/category maps in a single sheet read."""
    return load_fsl_state(tab=tab)["customer_country"]


def load_fsl_state(tab: str = FSL_TAB) -> dict:
    """Single FSL read returning every map sync_engine needs.

    Returns:
        {
          "dedupe_keys":      set[(date, code_upper, customer_norm)],
          "customer_country": {customer_norm: country},
          "code_taste":       {CODE_UPPER: taste_describe},
          "code_category":    {CODE_UPPER: category},
        }

    Why this exists: enriched FSL rows are the cheapest possible cache.
    Railway wipes the on-disk JSON caches every redeploy, but FSL lives in
    Google Sheets and persists forever. Reading it on every sync gives
    each new MMS row a free shot at "we already know the taste/category for
    this product code, no Haiku needed."

    Conflict resolution:
      - customer_country: most-common country across all rows for that customer
      - code_taste / code_category: first non-empty value wins (deterministic
        per code, so first-seen-from-FSL is fine)
    """
    from collections import Counter
    sh = _open_seasoning_master()
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        return {
            "dedupe_keys": set(),
            "customer_country": {},
            "code_taste": {},
            "code_category": {},
        }

    rows = ws.get_all_values()[1:]  # skip header
    dedupe_keys: set[tuple[str, str, str]] = set()
    by_customer: dict[str, Counter] = {}
    code_taste: dict[str, str] = {}
    code_category: dict[str, str] = {}

    for r in rows:
        date = (r[FSL_COL_DATE] if len(r) > FSL_COL_DATE else "").strip()
        code = (r[FSL_COL_CODE] if len(r) > FSL_COL_CODE else "").strip().upper()
        cust = (r[FSL_COL_CUSTOMER] if len(r) > FSL_COL_CUSTOMER else "").strip()
        country = (r[FSL_COL_COUNTRY] if len(r) > FSL_COL_COUNTRY else "").strip()
        taste = (r[FSL_COL_TASTE] if len(r) > FSL_COL_TASTE else "").strip()
        category = (r[FSL_COL_CATEGORY] if len(r) > FSL_COL_CATEGORY else "").strip()

        if date and code and cust:
            dedupe_keys.add((date, code, _norm_customer(cust)))
        if cust and country:
            by_customer.setdefault(_norm_customer(cust), Counter())[country] += 1
        if code and taste and code not in code_taste:
            code_taste[code] = taste
        if code and category and code not in code_category:
            code_category[code] = category

    return {
        "dedupe_keys": dedupe_keys,
        "customer_country": {k: ctr.most_common(1)[0][0] for k, ctr in by_customer.items()},
        "code_taste": code_taste,
        "code_category": code_category,
    }


def load_fsl_category_tab_map() -> dict[str, str]:
    """Build a {PRODUCT_CODE_UPPER: category} map by reading the 6 category
    tabs (Snack, Noodle & Instant Soup, etc.). First tab to claim a code wins.
    """
    sh = _open_seasoning_master()
    out: dict[str, str] = {}
    for tab in (
        "Snack", "Noodle & Instant Soup", "Sauces & Mixes",
        "Marinades", "Oil", "Beverage",
    ):
        try:
            ws = sh.worksheet(tab)
        except gspread.WorksheetNotFound:
            continue
        for r in ws.get_all_values()[1:]:
            for cell in r:
                c = (cell or "").strip().upper()
                if c.startswith("S-") and len(c) >= 5:
                    out.setdefault(c, tab)
                    break
    return out


def sort_fsl_by_date(tab: str = FSL_TAB) -> int:
    """Resort the target FSL tab by Sample Date Out (asc).

    Late-arriving MMS rows can be older than rows already in the sheet
    (e.g. a March entry showing up after April was already written). After
    every append we re-sort the whole table so the user always sees a
    chronological log. Unparseable dates sink to the bottom so they're easy
    to spot and fix.

    Returns the number of data rows written back. Returns 0 if the tab is
    missing or has no data rows. Header (row 1) is left untouched.
    """
    import datetime as _d
    sh = _open_seasoning_master()
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        return 0
    values = ws.get_all_values()
    if len(values) < 2:
        return 0
    data = values[1:]
    width = len(FSL_HEADER)
    data = [r + [""] * (width - len(r)) if len(r) < width else r[:width] for r in data]

    SENTINEL = _d.date(9999, 12, 31)

    def _key(r):
        d = _parse_iso_date(r[FSL_COL_DATE])
        return (1, SENTINEL) if d is None else (0, d)

    sorted_data = sorted(data, key=_key)
    if sorted_data == data:
        return 0  # already in order — skip the write

    last_row = len(sorted_data) + 1
    ws.update(
        range_name=f"A2:J{last_row}",
        values=sorted_data,
        value_input_option="USER_ENTERED",
    )
    return len(sorted_data)


def append_fsl_rows(rows: list[list[str]], tab: str = FSL_TAB) -> int:
    """Append rows to the bottom of the target FSL tab.

    Each row should match FSL_HEADER's column order. Rows shorter than
    len(FSL_HEADER) are right-padded with "" (so callers that still hand
    over 10-element rows from before V1.16.0 keep working — AWB starts
    empty and gets filled later by awb_sync). Returns the number of rows
    actually appended. Header is refreshed if missing. The tab is
    auto-created if it doesn't yet exist (used by the Indonesia backfill
    the first time it runs).
    """
    if not rows:
        return 0
    sh = _open_seasoning_master()
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab, rows=2000, cols=len(FSL_HEADER))

    # Ensure header. V1.16.0 added a new AWB col to FSL_HEADER — existing
    # tabs may still have the 10-col layout, so this also widens them.
    first = ws.row_values(1)
    if first != FSL_HEADER:
        if ws.col_count < len(FSL_HEADER):
            ws.add_cols(len(FSL_HEADER) - ws.col_count)
        ws.update(values=[FSL_HEADER], range_name="A1")

    # Pad each row up to the full FSL_HEADER width so callers from before
    # V1.16.0 (which still produce 10-element rows) keep working.
    width = len(FSL_HEADER)
    padded = [list(r) + [""] * (width - len(r)) if len(r) < width else list(r)
              for r in rows]

    # Find the first empty row at the bottom of the existing data.
    existing = ws.get_all_values()
    next_row = max(2, len(existing) + 1)

    # Make sure the sheet is tall enough.
    needed = next_row + len(padded) - 1
    if ws.row_count < needed:
        ws.add_rows(needed - ws.row_count)

    # _col_letter(11) → "K". Lifted to a helper so future column additions
    # don't need a fresh end-letter constant each time.
    end_col_letter = _col_letter(width)
    ws.update(
        range_name=f"A{next_row}:{end_col_letter}{next_row + len(padded) - 1}",
        values=padded,
        value_input_option="USER_ENTERED",
    )
    _invalidate_fsl_cache()
    return len(padded)


def _col_letter(col_1_indexed: int) -> str:
    """Convert 1-indexed column number to A1 column letter (1 → 'A', 11 → 'K')."""
    result = ""
    n = col_1_indexed
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(ord("A") + rem) + result
    return result


# ---------- AWB customer-name aliases (V1.16.0) ----------

# Tab name on OPS_SHEET_ID. Reps maintain it manually whenever a
# carrier label name doesn't match the FSL Customer Name (e.g. the
# DHL waybill says "SARL HYGIENIX MANUFACTURE COMPANY" but in your
# FSL the customer is "Daiya Food" — same shipment, different
# label conventions). One row per alias.
TAB_AWB_ALIASES = "AWB Customer Aliases"
AWB_ALIAS_HEADER = ["Carrier Name", "FSL Customer Name", "Notes"]

# 10-min TTL cache so the twice-daily sync hits Sheets at most once.
_AWB_ALIASES_CACHE: dict = {}


def load_customer_aliases() -> dict[str, str]:
    """Return a {carrier_name: fsl_customer_name} mapping from the
    'AWB Customer Aliases' tab in OPS_SHEET_ID.

    Auto-creates the tab (with the header row + a one-line example
    comment) the first time it's called against a workbook that
    doesn't have it yet. Empty mapping returned when the tab exists
    but contains only the header.

    awb_sync calls this once per sync run and uses it to rewrite each
    inbound shipment's recipient_name BEFORE the fuzzy matcher runs.
    Keys are case-preserved here; awb_sync normalises both sides
    before comparing, so capitalisation in the sheet doesn't matter.
    """
    import time as _time
    now = _time.time()
    cached = _AWB_ALIASES_CACHE.get("data")
    if cached and now - cached["ts"] < 600:
        return cached["map"]

    sh = _open_ops()
    try:
        ws = sh.worksheet(TAB_AWB_ALIASES)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=TAB_AWB_ALIASES, rows=200, cols=3)
        ws.update(values=[AWB_ALIAS_HEADER], range_name="A1")
        # Add a single illustrative example row that's clearly a
        # placeholder. Reps can delete or overwrite it.
        ws.update(
            values=[[
                "SARL HYGIENIX MANUFACTURE COMPANY",
                "Daiya Food",
                "Example — distributor → end customer brand",
            ]],
            range_name="A2",
        )
        _AWB_ALIASES_CACHE["data"] = {
            "ts": now,
            "map": {"SARL HYGIENIX MANUFACTURE COMPANY": "Daiya Food"},
        }
        return _AWB_ALIASES_CACHE["data"]["map"]

    rows = ws.get_all_values()
    result: dict[str, str] = {}
    for r in rows[1:]:
        if len(r) < 2:
            continue
        carrier = (r[0] or "").strip()
        fsl = (r[1] or "").strip()
        if carrier and fsl:
            result[carrier] = fsl
    _AWB_ALIASES_CACHE["data"] = {"ts": now, "map": result}
    return result


# ---------- Unmatched AWBs (V1.17.x — persistent, drives digest footer) ----------

TAB_UNMATCHED_AWBS = "Unmatched AWBs"
UNMATCHED_AWB_HEADER = [
    "AWB", "Carrier", "Recipient Name (carrier label)",
    "Ship Date", "Last Updated UTC",
]


def write_unmatched_awbs(rows: list[dict]) -> int:
    """MERGE this run's unmatched list with the existing OPS tab content.

    `rows` is a list of dicts with keys: awb, carrier, recipient_name,
    ship_date (str or date). Existing rows in the tab are preserved
    UNLESS the same AWB appears in `rows` (in which case the newer
    entry wins — refresh stamp + any updated metadata). Reps can also
    MANUALLY append rows to the tab (e.g. FedEx shipments the scraper
    can't see) and those stay across sync runs.

    Why merge instead of overwrite: the bot's twice-daily DHL scrape
    only sees the last ~5 weeks of shipments. If today's sync finds
    only 2 unmatched (because DHL was rate-limited, or all recent
    shipments matched FSL), an overwrite would wipe yesterday's 30+
    unresolved entries — exactly the list reps need to triage. Merge
    preserves the rolling triage list.

    Returns the total number of rows written (including merged-in
    historicals + new entries).
    """
    from datetime import datetime as _dt, timezone as _tz, date as _date
    sh = _open_ops()
    try:
        ws = sh.worksheet(TAB_UNMATCHED_AWBS)
        existing_values = ws.get_all_values()
        # Skip header row when parsing existing data.
        existing_rows: list[dict] = []
        for r in existing_values[1:]:
            if not r or not (r[0] or "").strip():
                continue
            existing_rows.append({
                "awb": (r[0] or "").strip(),
                "carrier": (r[1] or "").strip() if len(r) > 1 else "",
                "recipient_name": (r[2] or "").strip() if len(r) > 2 else "",
                "ship_date": (r[3] or "").strip() if len(r) > 3 else "",
                "last_updated_utc": (r[4] or "").strip() if len(r) > 4 else "",
            })
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(
            title=TAB_UNMATCHED_AWBS,
            rows=max(200, len(rows) + 50),
            cols=len(UNMATCHED_AWB_HEADER),
        )
        existing_rows = []

    now_utc = _dt.now(_tz.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Normalise an incoming row to the dict shape we store on the sheet.
    def _to_sheet_row(r: dict) -> dict:
        ship_date = r.get("ship_date", "")
        if isinstance(ship_date, _date):
            ship_date = ship_date.strftime("%Y-%m-%d")
        return {
            "awb": str(r.get("awb", "")).strip(),
            "carrier": str(r.get("carrier", "")).strip(),
            "recipient_name": str(r.get("recipient_name", "")).strip(),
            "ship_date": str(ship_date or ""),
            "last_updated_utc": now_utc,
        }

    # Merge by AWB. Newer wins.
    by_awb: dict[str, dict] = {}
    for r in existing_rows:
        awb = (r.get("awb") or "").strip()
        if awb:
            by_awb[awb] = r
    for r in rows:
        norm = _to_sheet_row(r)
        if norm["awb"]:
            by_awb[norm["awb"]] = norm

    # Sort by ship_date desc (newest first) for readability in the sheet.
    merged = sorted(
        by_awb.values(),
        key=lambda r: str(r.get("ship_date") or ""),
        reverse=True,
    )

    ws.clear()
    body: list[list[str]] = [UNMATCHED_AWB_HEADER]
    for r in merged:
        body.append([
            r.get("awb", ""),
            r.get("carrier", ""),
            r.get("recipient_name", ""),
            r.get("ship_date", ""),
            r.get("last_updated_utc", ""),
        ])
    ws.update(values=body, range_name="A1")
    return len(merged)


def load_unmatched_awbs() -> list[dict]:
    """Read persisted unmatched AWBs from the OPS tab. Returns [] when the
    tab doesn't exist or has only the header. Each dict has the same shape
    as the input to `write_unmatched_awbs`. Used by the daily digest to
    surface the 'AWBs not in FSL' footer reliably."""
    sh = _open_ops()
    try:
        ws = sh.worksheet(TAB_UNMATCHED_AWBS)
    except gspread.WorksheetNotFound:
        return []
    rows = ws.get_all_values()
    out: list[dict] = []
    for r in rows[1:]:
        if not r or not (r[0] or "").strip():
            continue
        out.append({
            "awb": (r[0] or "").strip(),
            "carrier": (r[1] or "").strip() if len(r) > 1 else "",
            "recipient_name": (r[2] or "").strip() if len(r) > 2 else "",
            "ship_date": (r[3] or "").strip() if len(r) > 3 else "",
            "last_updated_utc": (r[4] or "").strip() if len(r) > 4 else "",
        })
    return out


# ---------- AWB column updates (V1.16.0 — driven by awb_sync.py) ----------

def ensure_awb_column(tab: str = FSL_TAB) -> bool:
    """Make sure the target FSL tab has the AWB column header in place.

    Idempotent — safe to call before every sync run. Returns True if the
    sheet was modified (column added or header rewritten), False if the
    header was already correct. Used by awb_sync so the first run on a
    pre-V1.16.0 tab self-heals before we try to write AWB cells.
    """
    sh = _open_seasoning_master()
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        return False
    first = ws.row_values(1)
    if first == FSL_HEADER:
        return False
    if ws.col_count < len(FSL_HEADER):
        ws.add_cols(len(FSL_HEADER) - ws.col_count)
    ws.update(values=[FSL_HEADER], range_name="A1")
    return True


def load_fsl_rows_with_row_numbers(tab: str = FSL_TAB) -> list[dict]:
    """Like load_fsl_rows_all() but also stamps each dict with `_row` =
    the 1-indexed row number in the sheet (so an AWB update can target
    the exact cell). Used by awb_sync's matcher.
    """
    sh = _open_seasoning_master()
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        return []
    values = ws.get_all_values()
    if len(values) < 2:
        return []
    out: list[dict] = []
    for i, r in enumerate(values[1:], start=2):
        padded = r + [""] * (len(FSL_HEADER) - len(r))
        row = {hdr: restore_mangled_unicode(padded[idx])
               for idx, hdr in enumerate(FSL_HEADER)}
        row["_date"] = _parse_iso_date(row.get("Sample Date Out", ""))
        row["_row"] = i
        out.append(row)
    return out


def write_awb_updates(tab: str, updates: list[tuple[int, str, str]]) -> int:
    """Write AWB + (optional) Customer Address into K + L of `tab`.

    `updates` is a list of (row_number, awb, address) triples — row_number
    is the 1-indexed sheet row (as returned by load_fsl_rows_with_row_numbers).
    Rows with an empty AWB are skipped entirely. Empty address values are
    written as no-ops (the L cell is left untouched). Returns the number
    of rows whose K cell was written. Uses a single batch_update call so
    the whole sync is one Sheets API round-trip regardless of how many
    rows match.

    Backwards-compatible at the call-site level: older code that passes
    2-tuples still works because we degrade gracefully on a short tuple
    (treating address as empty).
    """
    clean: list[tuple[int, str, str]] = []
    for u in updates:
        # Accept (row, awb) for backwards-compat with any caller that
        # hasn't been updated yet. Tuple-of-three is the new shape.
        if len(u) == 2:
            r, awb = u  # type: ignore[misc]
            addr = ""
        else:
            r, awb, addr = u
        awb_clean = (awb or "").strip()
        addr_clean = (addr or "").strip()
        # Keep the row if EITHER cell needs writing. Empty awb here is
        # a deliberate signal from the matcher ("AWB already filled, but
        # please write the address") — see build_updates_for_tab.
        if not awb_clean and not addr_clean:
            continue
        clean.append((r, awb_clean, addr_clean))
    if not clean:
        return 0
    sh = _open_seasoning_master()
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        return 0
    awb_col = _col_letter(FSL_COL_AWB + 1)   # "K"
    addr_col = _col_letter(FSL_COL_ADDR + 1)  # "L"
    body: list[dict] = []
    for row, awb, addr in clean:
        if awb:
            body.append({"range": f"{awb_col}{row}", "values": [[awb]]})
        if addr:
            body.append({"range": f"{addr_col}{row}", "values": [[addr]]})
    ws.batch_update(body, value_input_option="USER_ENTERED")
    _invalidate_fsl_cache()
    return len(clean)


# ---------- /pp query log (audit trail of product price lookups) ----------

PP_QUERY_TAB = "Query"
PP_QUERY_HEADER = [
    "Timestamp",
    "Telegram Username",
    "Telegram User ID",
    "Query",
    "Result",
    "Matched Code",
    "Name",
    "R&D Price (USD)",
    "Raw Material Cost (USD)",
    "Error",
]


def log_pp_query(
    *,
    username: str,
    user_id: int | str,
    query: str,
    result: str,
    matched_code: str = "",
    name: str = "",
    rd_price_usd: float | None = None,
    raw_material_cost_usd: float | None = None,
    error: str = "",
) -> None:
    """Append one row to the 'Query' tab inside the seasoning master sheet.

    Logs every /pp lookup so the user can see what's been searched. Best-effort:
    any failure is logged and swallowed so the bot's reply to the user isn't blocked.
    """
    try:
        sh = _get_client().open_by_key(config.SEASONING_SHEET_ID)
        try:
            ws = sh.worksheet(PP_QUERY_TAB)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=PP_QUERY_TAB, rows=1000, cols=len(PP_QUERY_HEADER))

        # Make sure the header is in place — first time the bot writes here it'll be empty.
        first = ws.row_values(1)
        if not first:
            ws.update("A1", [PP_QUERY_HEADER])
        elif first != PP_QUERY_HEADER and all(not c.strip() for c in first):
            ws.update("A1", [PP_QUERY_HEADER])

        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        row = [
            ts,
            f"@{username}" if username else "",
            str(user_id),
            query,
            result,
            matched_code,
            name,
            "" if rd_price_usd is None else f"{rd_price_usd:.4f}",
            "" if raw_material_cost_usd is None else f"{raw_material_cost_usd:.4f}",
            error,
        ]
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:  # noqa: BLE001
        log.warning("log_pp_query failed: %s", e)


# ---------- Past sample submissions (for smart seasoning suggestions) ----------

def load_past_submissions(force: bool = False) -> list[dict[str, str]]:
    """Past entries from the Sample Log, used to boost seasoning suggestions.

    Each row becomes ``{"query_text": <free-text the salesperson wrote>,
    "matched_code": <the seasoning code that ended up on the request>}``.

    The query_text concatenates the user-typed seasoning request plus
    surrounding context (comment / requirement / market / customer base) so
    fuzzy-matching catches phrases like "korea spicy noodle" even when those
    words don't appear in the seasoning name itself.
    """
    global _past_submissions_cache
    now = time.time()
    if (
        not force
        and _past_submissions_cache
        and now - _past_submissions_cache[0] < _PAST_SUBMISSIONS_TTL
    ):
        return _past_submissions_cache[1]

    try:
        ws = _open_ops().worksheet(config.TAB_SALES_LOG)
    except Exception as e:  # noqa: BLE001
        log.warning("load_past_submissions: %s tab missing (%s)", config.TAB_SALES_LOG, e)
        _past_submissions_cache = (now, [])
        return []

    try:
        rows = ws.get_all_records()
    except Exception as e:  # noqa: BLE001
        log.warning("load_past_submissions: failed to read rows: %s", e)
        _past_submissions_cache = (now, [])
        return []

    parts_keys = (
        "Seasoning Requested",
        "Comment",
        "Requirement",
        "Market",
        "Customer Base",
    )
    out: list[dict[str, str]] = []
    for r in rows:
        norm = {str(k).strip(): str(v).strip() for k, v in r.items()}
        code = norm.get("Matched Code", "").strip()
        if not code:
            continue  # rows without a confirmed seasoning code aren't useful
        bits = [norm.get(k, "") for k in parts_keys]
        query_text = " | ".join(b for b in bits if b)
        if not query_text:
            continue
        out.append({"query_text": query_text, "matched_code": code})

    _past_submissions_cache = (now, out)
    log.info("Loaded %d past submission entries for smart matching", len(out))
    return out


# ---------- Customer master (external sheet: code + name only) ----------

_customer_master_cache: tuple[float, list[dict[str, str]]] | None = None
_CUSTOMER_MASTER_TTL = 60 * 60  # 1 hour


def load_customer_master(force: bool = False) -> list[dict[str, str]]:
    """Authoritative customer list. Each entry: {'name': ..., 'code': ...}."""
    global _customer_master_cache
    now = time.time()
    if (
        not force
        and _customer_master_cache
        and now - _customer_master_cache[0] < _CUSTOMER_MASTER_TTL
    ):
        return _customer_master_cache[1]

    sh = _get_client().open_by_key(config.CUSTOMER_MASTER_SHEET_ID)
    try:
        ws = sh.worksheet(config.CUSTOMER_MASTER_WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.get_worksheet(0)
    # Position-based read: col A code, col B name, col C address.
    rows = ws.get_all_values()
    out: list[dict[str, str]] = []
    for r in rows[1:]:  # skip header
        code = (r[0] if len(r) > 0 else "").strip()
        name = (r[1] if len(r) > 1 else "").strip()
        addr = (r[2] if len(r) > 2 else "").strip()
        if name:
            out.append({"name": name, "code": code, "address": addr})
    _customer_master_cache = (now, out)
    log.info("Loaded %d customers from master (%s)", len(out), ws.title)
    return out


# ---------- Customers (OPS contact cache) ----------

def load_customers(force: bool = False) -> list[dict[str, str]]:
    global _customers_cache
    now = time.time()
    if not force and _customers_cache and now - _customers_cache[0] < _CUSTOMERS_TTL:
        return _customers_cache[1]
    ws = _open_ops().worksheet(config.TAB_CUSTOMERS)
    out = ws.get_all_records()
    _customers_cache = (now, out)
    return out


def find_customer(company_name: str) -> dict[str, str] | None:
    target = company_name.strip().lower()
    for row in load_customers():
        if str(row.get("Company Name", "")).strip().lower() == target:
            return row
    return None


def load_merged_customers(force: bool = False) -> list[dict[str, str]]:
    """Merge customer master (name+code+address) with OPS Customers (contacts).

    Keyed on case-insensitive name. Master wins on code / address; OPS fills
    receiver / receiving person / courier. Used by the 12/15 name matcher so
    customers from either source show up as suggestions.
    """
    master = load_customer_master(force=force)
    ops = load_customers(force=force)

    merged: dict[str, dict[str, str]] = {}

    for c in master:
        key = c["name"].strip().lower()
        if not key:
            continue
        merged[key] = {
            "name": c["name"],
            "code": c.get("code", ""),
            "address": c.get("address", ""),
            "receiver_number": "",
            "receiving_person": "",
            "courier": "",
            "source": "master",
        }

    for r in ops:
        name = str(r.get("Company Name", "")).strip()
        if not name:
            continue
        key = name.lower()
        entry = merged.get(key)
        if entry is None:
            entry = {
                "name": name,
                "code": "",
                "address": str(r.get("Address", "")).strip(),
                "receiver_number": "",
                "receiving_person": "",
                "courier": "",
                "source": "ops",
            }
            merged[key] = entry
        else:
            entry["source"] = "both"
            # Fall back to OPS address only when master didn't have one.
            if not entry["address"]:
                entry["address"] = str(r.get("Address", "")).strip()
        entry["receiver_number"] = str(r.get("Receiver Number", "")).strip()
        entry["receiving_person"] = str(r.get("Receiving Person", "")).strip()
        entry["courier"] = str(r.get("Preferred Courier", "")).strip()

    return list(merged.values())


def upsert_customer(data: dict[str, str]) -> None:
    """Insert or update a customer by Company Name (case-insensitive)."""
    global _customers_cache
    ws = _open_ops().worksheet(config.TAB_CUSTOMERS)
    records = ws.get_all_records()
    name = data.get("Company Name", "").strip()
    if not name:
        return
    target = name.lower()
    for idx, row in enumerate(records, start=2):  # header is row 1
        if str(row.get("Company Name", "")).strip().lower() == target:
            values = [data.get(col, row.get(col, "")) for col in config.CUSTOMER_COLS]
            ws.update(f"A{idx}:E{idx}", [values])
            _customers_cache = None  # next merged-load picks up fresh contacts
            return
    ws.append_row([data.get(col, "") for col in config.CUSTOMER_COLS])
    _customers_cache = None


# ---------- Users (whitelist) ----------

def load_users(force: bool = False) -> list[dict[str, str]]:
    global _users_cache
    now = time.time()
    if not force and _users_cache and now - _users_cache[0] < _USERS_TTL:
        return _users_cache[1]
    ws = _open_ops().worksheet(config.TAB_USERS)
    out = ws.get_all_records()
    _users_cache = (now, out)
    return out


def invalidate_caches() -> None:
    """Force-refresh every cache on next read. Used by /reload."""
    global _seasoning_cache, _users_cache, _customer_master_cache, _customers_cache, _samples_cache
    _seasoning_cache = None
    _users_cache = None
    _customer_master_cache = None
    _customers_cache = None
    _samples_cache = None


def _norm_tg_id(v) -> str:
    """Normalize a Telegram User ID coming from the sheet for comparison.

    gspread's get_all_records returns numeric-looking cells as int or float.
    A cell typed as `1039554107` arrives as the int 1039554107, but if any
    spreadsheet logic ever turned it into a float we'd get
    `1039554107.0` — and str(...) would no longer match str(user.id).
    Strip the trailing `.0`.
    """
    s = str(v).strip()
    if not s:
        return ""
    try:
        return str(int(float(s)))
    except (ValueError, TypeError):
        return s


def _row_get_loose(row: dict, key: str) -> str:
    """Get a value from a row dict tolerant of header whitespace and case.

    A header typed as 'MMS Name ' (trailing space) or 'mms name' would
    otherwise cause row.get('MMS Name') to silently return None, leading
    to mysterious 'your MMS name isn't set' errors even when the cell is
    populated. This walks the dict's keys and matches case-/whitespace-
    insensitively, so we tolerate copy-paste imperfections from the sheet.
    """
    target = key.lower().strip()
    for k, v in row.items():
        if str(k).lower().strip() == target:
            return v if v is not None else ""
    return ""


def get_user_mms_name(tg_user_id: int, username: str | None) -> str:
    """Return the MMS Name configured for this Telegram user, or "" if none.

    Auto-retries against a force-refreshed user list on miss, so admins
    don't have to remember /reload after editing the Authorized Users tab.
    The first attempt uses the 5-minute cache (fast); if no row matches OR
    the matched row has an empty MMS Name, we re-read the sheet and try
    once more.

    Column lookups are header-tolerant via ``_row_get_loose`` — copy-paste
    artefacts like ``MMS Name `` (trailing space) or ``mms name`` no longer
    silently break the lookup.
    """
    target_id = _norm_tg_id(tg_user_id)
    target_uname = (username or "").lstrip("@").lower()

    def _scan(users) -> str:
        for row in users:
            active = str(_row_get_loose(row, "Active")).strip().lower()
            if active not in {"y", "yes", "true", "1"}:
                continue
            row_id = _norm_tg_id(_row_get_loose(row, "Telegram User ID"))
            row_uname = str(_row_get_loose(row, "Telegram Username")).lstrip("@").lower()
            if (row_id and row_id == target_id) or (row_uname and row_uname == target_uname):
                return str(_row_get_loose(row, "MMS Name")).strip()
        return ""

    users = load_users(force=False)
    name = _scan(users)
    if name:
        return name
    # Cache may be stale (user just filled in MMS Name). Re-read once.
    users = load_users(force=True)
    name = _scan(users)
    if name:
        return name
    # Hard miss — log the headers we actually saw + the first few rows so
    # we can diagnose 'MMS name isn't set' complaints from Railway logs.
    sample_headers = list(users[0].keys()) if users else []
    log.warning(
        "get_user_mms_name MISS for tg_user_id=%s username=%s. "
        "Saw %d users with headers %s",
        target_id, target_uname, len(users), sample_headers,
    )
    return ""


def find_fsl_product_by_code(code: str) -> dict | None:
    """Return the most-recent Full Sample Listing row whose Product Code
    EXACTLY matches ``code`` (case-insensitive), or ``None`` if no row
    matches.

    Used by /pp as a fallback when MMS returns a parent code (e.g. user
    asked for ``S-TXF06-00-03`` but MMS only has ``S-TXF06-00``) — FSL
    holds variant-specific R&D prices that the parent's MMS row doesn't.

    The returned dict carries every FSL_HEADER column plus a parsed
    ``_date`` (datetime.date or None) so the caller can render the
    sample-out date nicely.
    """
    import datetime as _d
    code_upper = (code or "").strip().upper()
    if not code_upper:
        return None
    # Route by prefix: J- → Jakarta tab, otherwise → main FSL tab.
    tab = fsl_tab_for_code(code_upper)
    sh = _open_seasoning_master()
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        return None
    values = ws.get_all_values()
    if len(values) < 2:
        return None
    matches: list[dict] = []
    for r in values[1:]:
        if len(r) <= FSL_COL_CODE:
            continue
        row_code = (r[FSL_COL_CODE] or "").strip().upper()
        if row_code != code_upper:
            continue
        padded = r + [""] * (len(FSL_HEADER) - len(r))
        row = {hdr: restore_mangled_unicode(padded[i]) for i, hdr in enumerate(FSL_HEADER)}
        row["_date"] = _parse_iso_date(row.get("Sample Date Out", ""))
        matches.append(row)
    if not matches:
        return None
    SENTINEL = _d.date(1900, 1, 1)
    matches.sort(key=lambda r: r.get("_date") or SENTINEL, reverse=True)
    return matches[0]


# 90-second TTL cache for the read-heavy FSL paths. /lastsample and
# /alllastsample each pull all 3 region tabs from scratch; a single
# search burst (rep types a query, paginates twice, then refines)
# easily blew through the Sheets API's 60-reads-per-minute-per-user
# quota and returned silent "no match" errors to the user. 90s keeps
# searches snappy without hiding genuine data updates for long.
# Writes (append_fsl_rows, write_awb_updates) call _invalidate_fsl_cache.
_FSL_ROWS_CACHE: dict[tuple, tuple[float, list]] = {}
_FSL_CACHE_TTL_SEC = 90


def _invalidate_fsl_cache() -> None:
    """Clear the FSL row cache. Called after any write so the next read
    sees fresh data instead of pre-write snapshot."""
    _FSL_ROWS_CACHE.clear()


def load_fsl_rows_all(tab: str = FSL_TAB) -> list[dict[str, str]]:
    """Return every row from the target FSL tab, regardless of Sales rep.

    Admin-only path: /alllastsample uses this to search across all reps,
    in contrast to load_fsl_rows_for_sales() which scopes to one rep.
    Each row includes a parsed ``_date`` for sorting.

    90s TTL cache — see _FSL_ROWS_CACHE docstring.
    """
    import time as _time
    cache_key = ("all", tab)
    cached = _FSL_ROWS_CACHE.get(cache_key)
    now = _time.time()
    if cached and now - cached[0] < _FSL_CACHE_TTL_SEC:
        return cached[1]

    sh = _open_seasoning_master()
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        return []
    values = ws.get_all_values()
    if len(values) < 2:
        return []
    out: list[dict[str, str]] = []
    for r in values[1:]:
        padded = r + [""] * (len(FSL_HEADER) - len(r))
        row = {hdr: restore_mangled_unicode(padded[i]) for i, hdr in enumerate(FSL_HEADER)}
        row["_date"] = _parse_iso_date(row.get("Sample Date Out", ""))
        out.append(row)
    _FSL_ROWS_CACHE[cache_key] = (now, out)
    return out


def load_fsl_rows_for_sales(sales_name: str, tab: str = FSL_TAB) -> list[dict[str, str]]:
    """Return every FSL row whose Sales col matches sales_name in the target
    tab (case-insensitive, whitespace-collapsed). Each row is a dict with
    the FSL_HEADER columns as keys + a parsed `_date` (datetime.date | None)
    for sorting. Returns [] if the sales name is empty or the tab is missing.

    90s TTL cache keyed by (sales_name lowered, tab) — see
    _FSL_ROWS_CACHE docstring above.
    """
    if not (sales_name or "").strip():
        return []
    target = " ".join(sales_name.lower().split())

    import time as _time
    cache_key = ("sales", target, tab)
    cached = _FSL_ROWS_CACHE.get(cache_key)
    now = _time.time()
    if cached and now - cached[0] < _FSL_CACHE_TTL_SEC:
        return cached[1]

    sh = _open_seasoning_master()
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        return []
    values = ws.get_all_values()
    if len(values) < 2:
        return []
    header, data = values[0], values[1:]

    out: list[dict[str, str]] = []
    for r in data:
        if len(r) <= FSL_COL_SALES:
            continue
        sales = " ".join((r[FSL_COL_SALES] or "").lower().split())
        if sales != target:
            continue
        # Pad to header width for safe column access.
        padded = r + [""] * (len(FSL_HEADER) - len(r))
        row = {h: restore_mangled_unicode(padded[i]) for i, h in enumerate(FSL_HEADER)}
        row["_date"] = _parse_iso_date(row.get("Sample Date Out", ""))
        out.append(row)
    _FSL_ROWS_CACHE[cache_key] = (now, out)
    return out


def is_user_authorized(tg_user_id: int, username: str | None) -> bool:
    uname = (username or "").lstrip("@").lower()
    for row in load_users():
        if str(row.get("Active", "")).strip().lower() not in {"y", "yes", "true", "1"}:
            continue
        row_id = str(row.get("Telegram User ID", "")).strip()
        row_uname = str(row.get("Telegram Username", "")).lstrip("@").lower()
        if row_id and row_id == str(tg_user_id):
            return True
        if row_uname and row_uname == uname:
            return True
    return False


# ---------- Sales log ----------

def append_sample_request(row: dict[str, str]) -> None:
    """Append a sales-log row, aligned to the sheet's actual header order.

    If the sheet is missing any column listed in ``SALES_LOG_COLS``, the
    column is appended to the header first so the new value doesn't get
    jammed into a column that already means something else. This prevents
    the V0.3→V0.4 Deadline shift bug from recurring next time we add a
    field.
    """
    global _samples_cache
    ws = _open_ops().worksheet(config.TAB_SALES_LOG)
    header = [str(c).strip() for c in ws.row_values(1)]
    missing = [c for c in config.SALES_LOG_COLS if c not in header]
    if missing:
        new_header = header + missing
        if ws.col_count < len(new_header):
            ws.add_cols(len(new_header) - ws.col_count)
        ws.update("A1", [new_header])
        header = new_header
        log.info("append: added missing column(s): %s", ", ".join(missing))
    # Only write columns the sheet knows about — never append extras past the
    # header, which was the bug that shifted the previous row.
    ws.append_row([row.get(col, "") for col in header])
    _samples_cache = None  # so /samples shows the just-submitted row


def load_sample_log(force: bool = False) -> list[dict[str, Any]]:
    """Return every row in the Sales Log tab. Used by /samples."""
    global _samples_cache
    now = time.time()
    if not force and _samples_cache and now - _samples_cache[0] < _SAMPLES_TTL:
        return _samples_cache[1]
    ws = _open_ops().worksheet(config.TAB_SALES_LOG)
    out = ws.get_all_records()
    _samples_cache = (now, out)
    return out


# ---------- Sample Master List 2024-Present — DEPRECATED ----------
#
# This tab has been retired in favour of the transactional "Full Sample
# Listing" tab, populated by the PDF consolidation pipeline:
#     _consolidate_full_sample_listing.py
#     _propagate_manual_countries.py
#     _add_taste_describe.py
#     _add_category.py
#
# The /updatesamplelist command that used to write here is currently
# stubbed (see bot.cmd_update_sample_list) and will be rewired to target
# "Full Sample Listing" with the new schema. Until then, the helpers below
# stay in source for reference but should NOT auto-create the deleted tab.

SAMPLE_MASTER_TAB = "Sample Master List 2024-Present"  # deprecated
SAMPLE_MASTER_COLS = [
    "Seasoning Name",
    "Code",
    "Country",
    "Sales",
    "R&D Price (USD)",
    "Sample Date Out",
    "Flavour Profile",
    "Taste describe",
]


def _parse_iso_date(s: str):
    """Best-effort parse of Sample Date Out strings. Returns ``date`` or None.

    Accepts every shape we've seen in Full Sample Listing in the wild:
      - ISO          ``2026-04-29``
      - MMS style    ``02-Apr-2024``        (dash + abbreviated month)
      - Slash + abbr ``01/Apr/2026``        (FSL via PDF backfill)
      - Slash + full ``01/April/2026``
      - Numeric      ``01/04/2026`` / ``2026/04/29`` / ``01-04-2026``
      - Spaced       ``02 Apr 2024``

    Critical for /lastsample sorting and the FSL sort job — if a row's date
    fails to parse, it falls back to a sentinel date (1900-01-01) which can
    cause newer rows to sort below older ones when scores tie. Bug found
    V1.8.4: ``01/Apr/2026`` wasn't covered, leaving 2026 honey samples
    sorting behind 2024 ones.
    """
    import datetime as _d
    s = (s or "").strip()
    if not s:
        return None
    for fmt in (
        "%Y-%m-%d",
        "%d-%b-%Y", "%d-%B-%Y",
        "%d/%b/%Y", "%d/%B/%Y",
        "%d/%m/%Y", "%Y/%m/%d",
        "%d %b %Y", "%d %B %Y",
        "%d-%m-%Y",
    ):
        try:
            return _d.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _open_master():
    """The sample-master tab lives inside the same file as SEASONING_SHEET_ID."""
    return _get_client().open_by_key(config.SEASONING_SHEET_ID)


# ---- Sync metadata (last sync run, per tab) ----

_SYNC_META_TAB = "_sync_meta"
_SYNC_KEY_LAST_RUN = "sample_master_last_sync_utc"

# Per-tab last-sync key. Indonesia gets its own so its cooldown / catch-up
# logic doesn't trip on the Singapore timestamp (and vice versa).
_SYNC_KEY_BY_TAB = {
    FSL_TAB: _SYNC_KEY_LAST_RUN,
    JAKARTA_FSL_TAB: "sample_master_last_sync_utc_jakarta",
    BANGKOK_FSL_TAB: "sample_master_last_sync_utc_thailand",
}


def ensure_bangkok_tab() -> None:
    """Make sure the Thailand FSL tab exists with the right header.

    V1.13.0 — added when Bangkok factory data was backfilled. Same
    pattern as ensure_jakarta_tab: rename common typo'd variants in
    place, otherwise leave alone if already correct, otherwise create
    fresh. Singapore + Indonesia tabs are NEVER touched.
    """
    sh = _open_seasoning_master()
    existing = {ws.title: ws for ws in sh.worksheets()}
    if BANGKOK_FSL_TAB in existing:
        ws = existing[BANGKOK_FSL_TAB]
        first = ws.row_values(1)
        if first != FSL_HEADER:
            if ws.col_count < len(FSL_HEADER):
                ws.add_cols(len(FSL_HEADER) - ws.col_count)
            ws.update(values=[FSL_HEADER], range_name="A1")
        return
    # Common typo variants to rename in place.
    for typo in ("Full sample list thailand", "Full Sample List Thailand",
                 "Full sample listing Thailand", "Full Sample listing thailand",
                 "Full sample list bangkok", "Full Sample Listing Bangkok"):
        if typo in existing:
            existing[typo].update_title(BANGKOK_FSL_TAB)
            log.info("Renamed tab %r -> %r", typo, BANGKOK_FSL_TAB)
            ws = sh.worksheet(BANGKOK_FSL_TAB)
            first = ws.row_values(1)
            if first != FSL_HEADER:
                if ws.col_count < len(FSL_HEADER):
                    ws.add_cols(len(FSL_HEADER) - ws.col_count)
                ws.update(values=[FSL_HEADER], range_name="A1")
            return
    # Fresh create.
    ws = sh.add_worksheet(title=BANGKOK_FSL_TAB, rows=2000, cols=len(FSL_HEADER))
    ws.update(values=[FSL_HEADER], range_name="A1")
    log.info("Created tab %r", BANGKOK_FSL_TAB)


def ensure_jakarta_tab() -> None:
    """Make sure the Indonesia FSL tab exists with the right header.

    Called from bot startup. Idempotent — does nothing if the tab is
    already correct. If a typo'd 'Full sample list jakatar' tab exists
    (e.g. left over from manual sheet creation), we rename it in place
    so any links / bookmarks the user had remain valid. Otherwise create
    a fresh empty tab with the standard FSL_HEADER.

    Critically: does NOT touch the main FSL tab. Singapore data is
    completely untouched by this bootstrap.
    """
    sh = _open_seasoning_master()
    existing = {ws.title: ws for ws in sh.worksheets()}
    if JAKARTA_FSL_TAB in existing:
        ws = existing[JAKARTA_FSL_TAB]
        first = ws.row_values(1)
        if first != FSL_HEADER:
            if ws.col_count < len(FSL_HEADER):
                ws.add_cols(len(FSL_HEADER) - ws.col_count)
            ws.update(values=[FSL_HEADER], range_name="A1")
        return
    # Look for the typo'd version and rename it in place.
    for typo in ("Full sample list jakatar", "Full Sample List Jakatar",
                 "Full sample list Jakarta", "Full Sample list jakarta"):
        if typo in existing:
            existing[typo].update_title(JAKARTA_FSL_TAB)
            log.info("Renamed tab %r -> %r", typo, JAKARTA_FSL_TAB)
            ws = sh.worksheet(JAKARTA_FSL_TAB)
            first = ws.row_values(1)
            if first != FSL_HEADER:
                if ws.col_count < len(FSL_HEADER):
                    ws.add_cols(len(FSL_HEADER) - ws.col_count)
                ws.update(values=[FSL_HEADER], range_name="A1")
            return
    # Fresh create.
    ws = sh.add_worksheet(title=JAKARTA_FSL_TAB, rows=2000, cols=len(FSL_HEADER))
    ws.update(values=[FSL_HEADER], range_name="A1")
    log.info("Created tab %r", JAKARTA_FSL_TAB)


def _open_sync_meta_ws():
    """Get (or create+hide) the tiny _sync_meta tab used for cooldown state.

    Two-column layout: col A = key, col B = ISO UTC timestamp. We hide it so
    it doesn't clutter the user's tab bar but still accepts programmatic R/W.
    """
    sh = _open_master()
    try:
        return sh.worksheet(_SYNC_META_TAB)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=_SYNC_META_TAB, rows=20, cols=2)
        ws.update(range_name="A1", values=[["key", "value"]])
        try:
            # Hide the tab from the user's UI — cosmetic only.
            sh.batch_update(
                {
                    "requests": [
                        {
                            "updateSheetProperties": {
                                "properties": {
                                    "sheetId": ws.id,
                                    "hidden": True,
                                },
                                "fields": "hidden",
                            }
                        }
                    ]
                }
            )
        except Exception as e:  # noqa: BLE001
            log.debug("Could not hide _sync_meta tab: %s", e)
        return ws


def get_last_sample_sync(tab: str = FSL_TAB):
    """Return the UTC datetime of the last successful sync for ``tab``,
    or ``None`` if we've never recorded one. Singapore (FSL_TAB) and
    Indonesia (JAKARTA_FSL_TAB) each have their own meta key."""
    import datetime as _d
    target_key = _SYNC_KEY_BY_TAB.get(tab, _SYNC_KEY_LAST_RUN)
    try:
        ws = _open_sync_meta_ws()
        for row in ws.get_all_values()[1:]:  # skip header
            if len(row) >= 2 and row[0].strip() == target_key:
                raw = row[1].strip()
                if not raw:
                    return None
                try:
                    return _d.datetime.fromisoformat(raw)
                except ValueError:
                    return None
    except Exception as e:  # noqa: BLE001
        log.warning("get_last_sample_sync(%s) failed: %s", tab, e)
    return None


def set_last_sample_sync(when, tab: str = FSL_TAB) -> None:
    """Upsert the last-run timestamp (UTC ISO) into _sync_meta for ``tab``."""
    import datetime as _d
    target_key = _SYNC_KEY_BY_TAB.get(tab, _SYNC_KEY_LAST_RUN)
    try:
        ws = _open_sync_meta_ws()
        iso = (
            when.astimezone(_d.timezone.utc).replace(microsecond=0).isoformat()
            if when.tzinfo
            else when.replace(microsecond=0).isoformat() + "+00:00"
        )
        rows = ws.get_all_values()
        for i, row in enumerate(rows[1:], start=2):  # header is row 1
            if len(row) >= 1 and row[0].strip() == target_key:
                ws.update(range_name=f"A{i}:B{i}", values=[[target_key, iso]])
                return
        ws.append_row([target_key, iso])
    except Exception as e:  # noqa: BLE001
        # Never let meta-write failure abort a successful sync.
        log.warning("set_last_sample_sync(%s) failed: %s", tab, e)


def load_sample_master() -> tuple[list[str], list[list[str]]]:
    """Return (header, rows) for the deprecated master tab.

    No longer auto-creates the tab if missing — the user deleted it and we
    don't want to silently resurrect it. Callers that hit this path now get
    an empty result; the legacy /updatesamplelist command is stubbed at the
    handler level so this should never be reached in practice.
    """
    sh = _open_master()
    try:
        ws = sh.worksheet(SAMPLE_MASTER_TAB)
    except gspread.WorksheetNotFound:
        log.warning(
            "load_sample_master: %r tab not found — it's been deprecated. "
            "Returning empty result. The PDF consolidation pipeline writes "
            "to 'Full Sample Listing' instead.",
            SAMPLE_MASTER_TAB,
        )
        return SAMPLE_MASTER_COLS[:], []

    vals = ws.get_all_values()
    if not vals:
        ws.update(range_name="A1", values=[SAMPLE_MASTER_COLS])
        return SAMPLE_MASTER_COLS[:], []

    header = [str(c).strip() for c in vals[0]]
    # Self-heal missing columns.
    missing = [c for c in SAMPLE_MASTER_COLS if c not in header]
    if missing:
        header = header + missing
        if ws.col_count < len(header):
            ws.add_cols(len(header) - ws.col_count)
        ws.update(range_name="A1", values=[header])

    rows = [list(r) + [""] * (len(header) - len(r)) for r in vals[1:]]
    return header, rows


def upsert_sample_master(
    incoming: list[dict[str, str]],
    blurb_fn=None,
) -> tuple[int, int]:
    """DEPRECATED — upserts to the deleted 'Sample Master List 2024-Present'.

    Kept for reference until the /updatesamplelist command is rewired to
    target 'Full Sample Listing' with the transactional schema. If invoked
    today, will hit `load_sample_master`'s missing-tab branch and short-
    circuit without writing.

    Original docstring follows.

    Upsert rows into 'Sample Master List 2024-Present' keyed on Code.

    ``incoming`` items look like::
        {
          "Seasoning Name": ..., "Code": ...,
          "Country": ..., "Sales": ..., "R&D Price (USD)": ...,
        }

    Behaviour:
      - Existing Code → refresh Seasoning Name / Country / Sales / Price with
        the latest values. Flavour Profile and Taste describe are preserved
        if already populated; filled via ``blurb_fn(code, name)`` if blank.
      - New Code → append with a fresh Flavour Profile + Taste describe.
      - Order in the sheet is preserved; new rows append at the bottom.

    ``blurb_fn(code, name) -> (flavour_profile, taste_describe)`` is called
    only when we actually need a blurb. Pass ``None`` to leave blanks.

    Returns ``(added, updated)``.
    """
    header, existing_rows = load_sample_master()
    # Ensure every canonical column has an index in the header.
    idx = {name: header.index(name) for name in SAMPLE_MASTER_COLS}

    # Build a lookup of existing rows by Code.
    by_code: dict[str, int] = {}
    for i, row in enumerate(existing_rows):
        code = (row[idx["Code"]] if idx["Code"] < len(row) else "").strip()
        if code:
            by_code[code.upper()] = i

    added = 0
    updated = 0
    for item in incoming:
        code = (item.get("Code") or "").strip()
        if not code:
            continue
        name = (item.get("Seasoning Name") or "").strip()
        country = (item.get("Country") or "").strip()
        sales = (item.get("Sales") or "").strip()
        price = (item.get("R&D Price (USD)") or "").strip()
        date_out = (item.get("Sample Date Out") or "").strip()

        key = code.upper()
        if key in by_code:
            r = existing_rows[by_code[key]]
            # Widen row to header length if needed.
            if len(r) < len(header):
                r = r + [""] * (len(header) - len(r))
                existing_rows[by_code[key]] = r
            changed = False
            for col, val in (
                ("Seasoning Name", name),
                ("Country", country),
                ("Sales", sales),
                ("R&D Price (USD)", price),
            ):
                if val and r[idx[col]] != val:
                    r[idx[col]] = val
                    changed = True
            # Sample Date Out: only overwrite if incoming is strictly newer —
            # prevents an older PDF re-parse from clobbering a newer MMS date.
            if date_out:
                existing_date = _parse_iso_date(r[idx["Sample Date Out"]])
                new_date = _parse_iso_date(date_out)
                if new_date is not None and (existing_date is None or new_date > existing_date):
                    if r[idx["Sample Date Out"]] != date_out:
                        r[idx["Sample Date Out"]] = date_out
                        changed = True
            # Fill blurb fields only if currently blank.
            if blurb_fn is not None and name:
                if not r[idx["Flavour Profile"]].strip() or not r[idx["Taste describe"]].strip():
                    fp, td = blurb_fn(code, name)
                    if fp and not r[idx["Flavour Profile"]].strip():
                        r[idx["Flavour Profile"]] = fp
                        changed = True
                    if td and not r[idx["Taste describe"]].strip():
                        r[idx["Taste describe"]] = td
                        changed = True
            if changed:
                updated += 1
        else:
            new_row = [""] * len(header)
            new_row[idx["Seasoning Name"]] = name
            new_row[idx["Code"]] = code
            new_row[idx["Country"]] = country
            new_row[idx["Sales"]] = sales
            new_row[idx["R&D Price (USD)"]] = price
            new_row[idx["Sample Date Out"]] = date_out
            if blurb_fn is not None and name:
                fp, td = blurb_fn(code, name)
                new_row[idx["Flavour Profile"]] = fp
                new_row[idx["Taste describe"]] = td
            existing_rows.append(new_row)
            by_code[key] = len(existing_rows) - 1
            added += 1

    # Single batch write covering every data row.
    sh = _open_master()
    ws = sh.worksheet(SAMPLE_MASTER_TAB)
    needed = len(existing_rows) + 1
    if ws.row_count < needed:
        ws.add_rows(needed - ws.row_count)

    end_col = gspread.utils.rowcol_to_a1(1, len(header)).rstrip("1")
    if existing_rows:
        ws.update(
            range_name=f"A2:{end_col}{len(existing_rows) + 1}",
            values=existing_rows,
        )
    return added, updated
