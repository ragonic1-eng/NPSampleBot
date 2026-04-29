"""Google Sheets wrapper.

Lazy-loads gspread client, caches the seasoning master list for 1 hour,
exposes helpers for customer lookup / upsert and sample-log append.
"""
from __future__ import annotations

import json as _json
import logging
import os
import time
from typing import Any

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

FSL_TAB = "Full Sample Listing"
FSL_HEADER = [
    "Sales", "Customer Name", "Country", "Product Code", "Product Name",
    "Quantity (g)", "Sample Date Out", "Taste describe", "Category",
    "R&D Price",
]
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


def _open_seasoning_master():
    """Open the workbook that holds Full Sample Listing + the 6 category tabs."""
    return _get_client().open_by_key(config.SEASONING_SHEET_ID)


def _norm_customer(name: str) -> str:
    return " ".join((name or "").lower().split())


def load_fsl_dedupe_keys() -> set[tuple[str, str, str]]:
    """Return the set of (sample_date_out, product_code, normalized_customer)
    tuples for every row already in Full Sample Listing.

    /updatesamplelist uses this to skip rows it's already imported from MMS.
    """
    sh = _open_seasoning_master()
    try:
        ws = sh.worksheet(FSL_TAB)
    except gspread.WorksheetNotFound:
        return set()
    rows = ws.get_all_values()[1:]  # skip header
    out: set[tuple[str, str, str]] = set()
    for r in rows:
        date = (r[FSL_COL_DATE] if len(r) > FSL_COL_DATE else "").strip()
        code = (r[FSL_COL_CODE] if len(r) > FSL_COL_CODE else "").strip().upper()
        cust = (r[FSL_COL_CUSTOMER] if len(r) > FSL_COL_CUSTOMER else "").strip()
        if date and code and cust:
            out.add((date, code, _norm_customer(cust)))
    return out


def load_fsl_customer_country_map() -> dict[str, str]:
    """Build a {normalized_customer_name: country} map from existing FSL rows.

    Used by /updatesamplelist to back-fill Country when MMS returns it blank
    (most legacy MMS rows have no country) — your own data is the source of
    truth.
    """
    from collections import Counter
    sh = _open_seasoning_master()
    try:
        ws = sh.worksheet(FSL_TAB)
    except gspread.WorksheetNotFound:
        return {}
    rows = ws.get_all_values()[1:]
    by_customer: dict[str, Counter] = {}
    for r in rows:
        name = (r[FSL_COL_CUSTOMER] if len(r) > FSL_COL_CUSTOMER else "").strip()
        country = (r[FSL_COL_COUNTRY] if len(r) > FSL_COL_COUNTRY else "").strip()
        if not name or not country:
            continue
        by_customer.setdefault(_norm_customer(name), Counter())[country] += 1
    return {k: ctr.most_common(1)[0][0] for k, ctr in by_customer.items()}


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


def append_fsl_rows(rows: list[list[str]]) -> int:
    """Append rows to the bottom of Full Sample Listing.

    Each row must already be a 10-element list matching FSL_HEADER. Returns
    the number of rows actually appended. Header is refreshed if missing.
    """
    if not rows:
        return 0
    sh = _open_seasoning_master()
    try:
        ws = sh.worksheet(FSL_TAB)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=FSL_TAB, rows=2000, cols=len(FSL_HEADER))

    # Ensure header.
    first = ws.row_values(1)
    if first != FSL_HEADER:
        if ws.col_count < len(FSL_HEADER):
            ws.add_cols(len(FSL_HEADER) - ws.col_count)
        ws.update(values=[FSL_HEADER], range_name="A1")

    # Find the first empty row at the bottom of the existing data.
    existing = ws.get_all_values()
    next_row = max(2, len(existing) + 1)

    # Make sure the sheet is tall enough.
    needed = next_row + len(rows) - 1
    if ws.row_count < needed:
        ws.add_rows(needed - ws.row_count)

    end_col_letter = "J"  # 10th column
    ws.update(
        range_name=f"A{next_row}:{end_col_letter}{next_row + len(rows) - 1}",
        values=rows,
        value_input_option="USER_ENTERED",
    )
    return len(rows)


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

    Accepts ISO (YYYY-MM-DD), ``dd-MMM-yyyy`` (MMS style like ``02-Apr-2024``),
    ``dd/mm/yyyy`` and a couple of other common shapes. Never raises.
    """
    import datetime as _d
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d/%m/%Y", "%Y/%m/%d", "%d %b %Y", "%d-%m-%Y"):
        try:
            return _d.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _open_master():
    """The sample-master tab lives inside the same file as SEASONING_SHEET_ID."""
    return _get_client().open_by_key(config.SEASONING_SHEET_ID)


# ---- Sync metadata (last /updatesamplelist run) ----

_SYNC_META_TAB = "_sync_meta"
_SYNC_KEY_LAST_RUN = "sample_master_last_sync_utc"


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


def get_last_sample_sync():
    """Return the UTC datetime of the last successful /updatesamplelist run,
    or ``None`` if we've never recorded one."""
    import datetime as _d
    try:
        ws = _open_sync_meta_ws()
        for row in ws.get_all_values()[1:]:  # skip header
            if len(row) >= 2 and row[0].strip() == _SYNC_KEY_LAST_RUN:
                raw = row[1].strip()
                if not raw:
                    return None
                try:
                    return _d.datetime.fromisoformat(raw)
                except ValueError:
                    return None
    except Exception as e:  # noqa: BLE001
        log.warning("get_last_sample_sync failed: %s", e)
    return None


def set_last_sample_sync(when) -> None:
    """Upsert the last-run timestamp (UTC ISO) into _sync_meta."""
    import datetime as _d
    try:
        ws = _open_sync_meta_ws()
        iso = (
            when.astimezone(_d.timezone.utc).replace(microsecond=0).isoformat()
            if when.tzinfo
            else when.replace(microsecond=0).isoformat() + "+00:00"
        )
        rows = ws.get_all_values()
        for i, row in enumerate(rows[1:], start=2):  # header is row 1
            if len(row) >= 1 and row[0].strip() == _SYNC_KEY_LAST_RUN:
                ws.update(range_name=f"A{i}:B{i}", values=[[_SYNC_KEY_LAST_RUN, iso]])
                return
        ws.append_row([_SYNC_KEY_LAST_RUN, iso])
    except Exception as e:  # noqa: BLE001
        # Never let meta-write failure abort a successful sync.
        log.warning("set_last_sample_sync failed: %s", e)


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
