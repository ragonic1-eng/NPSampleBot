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
