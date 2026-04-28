"""NPSampleBot — Telegram bot for raising seasoning sample requests.

Module 1:
  - guided form with forward/back navigation
  - fuzzy seasoning suggestions (top 3) from master sheet
  - company lookup with auto-fill of contact details
  - final draft preview with edit-any-field
  - on confirm: append to "Sample request list from sales"
"""
from __future__ import annotations

import asyncio
import html
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from telegram import (
    BotCommand,
    BotCommandScopeDefault,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

import ai
import config
import matcher
import mms_product
import sheets
import state
import vision_scan
from state import FIELDS, FIELD_LABELS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("npsamplebot")


# --------------------------- helpers ---------------------------

COURIERS = ["DHL", "FedEx", "Airpak", "China Man"]
APP_METHODS = ["Dusting", "Slurry", "3DF"]
CUSTOMER_BASES = [
    "Potato chips",
    "Potato pellet",
    "Corn Pellet",
    "Corn Puffs",
    "Popcorn",
    "Jhalmuri",
    "Charnachur",
    "Popchips",
    "Instant noodles",
    "Wheat flour biscuit",
]

# Seasoning-weight presets for 3/15 main quantity.
SEASONING_WEIGHTS = ["30g", "50g", "100g", "200g", "300g", "500g", "1kg"]
# Bottle count presets when the selected product is an oil.
OIL_BOTTLES = ["1", "2", "3", "5"]
# Application base-product presets (different list from customer base @ 10/15).
APP_BASES = ["Potato chips", "Corn puff", "Corn curl (Twisties)", "Wheat flour base pellets", "Wheat flour biscuit"]
# Set-count presets for seasoning qty + application sample.
SET_COUNTS = ["1", "2", "3", "5"]

_QTY_SUBS = {
    "main", "main_manual",
    "main_sets", "main_sets_manual",
    "need_app",
    "app_amount",
    "app_sets", "app_sets_manual",
    "app_base", "app_base_manual",
}

# /samples (V0.3.0) — Singapore time, 5 rows per page.
SGT_OFFSET_HOURS = 8
SAMPLES_PAGE_SIZE = 5


def _sgt_now() -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=SGT_OFFSET_HOURS)


def _parse_log_ts_utc(s: str) -> datetime | None:
    """Parse the log's 'YYYY-MM-DD HH:MM:SS UTC' timestamp back to a UTC datetime."""
    s = (s or "").strip()
    if not s:
        return None
    if s.endswith(" UTC"):
        s = s[:-4]
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _log_ts_to_sgt(s: str) -> datetime | None:
    ts = _parse_log_ts_utc(s)
    return ts + timedelta(hours=SGT_OFFSET_HOURS) if ts else None


def _mine_only(rows: list[dict[str, Any]], user_id: int) -> list[dict[str, Any]]:
    uid = str(user_id)
    return [r for r in rows if str(r.get("Telegram User ID", "")).strip() == uid]


def _filter_today_sgt(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    today = _sgt_now().date()
    out = []
    for r in rows:
        sgt = _log_ts_to_sgt(r.get("Timestamp", ""))
        if sgt and sgt.date() == today:
            out.append(r)
    return out


def _filter_month_sgt(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    now = _sgt_now()
    y, m = now.year, now.month
    out = []
    for r in rows:
        sgt = _log_ts_to_sgt(r.get("Timestamp", ""))
        if sgt and sgt.year == y and sgt.month == m:
            out.append(r)
    return out


def _sort_by_ts_desc(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda r: _parse_log_ts_utc(r.get("Timestamp", "")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )


def _group_by_customer(rows: list[dict[str, Any]]) -> list[tuple[str, list[dict[str, Any]]]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        name = str(r.get("Customer Company Name", "")).strip() or "(unknown)"
        buckets.setdefault(name, []).append(r)
    # Busiest customer first, ties by name.
    return sorted(buckets.items(), key=lambda kv: (-len(kv[1]), kv[0].lower()))


def _page_slice(items: list, page: int, size: int = SAMPLES_PAGE_SIZE) -> tuple[list, int, int]:
    total_pages = max(1, (len(items) + size - 1) // size)
    page = max(0, min(page, total_pages - 1))
    start = page * size
    return items[start:start + size], page, total_pages


def _page_nav_row(page: int, total_pages: int, cb_prefix: str) -> list[tuple[str, str]]:
    row: list[tuple[str, str]] = []
    if page > 0:
        row.append(("◀ Prev", f"{cb_prefix}:page:{page - 1}"))
    row.append((f"Page {page + 1}/{total_pages}", "samp:noop"))
    if page < total_pages - 1:
        row.append(("Next ▶", f"{cb_prefix}:page:{page + 1}"))
    return row


def _fmt_sample_summary(r: dict[str, Any]) -> str:
    """Render a sales-log row in the same style as the Draft review screen.

    The sheet column names match FIELDS labels exactly (see SALES_LOG_COLS),
    so we can iterate FIELDS and look up each value by its label.
    """
    lines = []
    for _key, label in FIELDS:
        val = str(r.get(label, "")).strip()
        val_str = h(val) if val else "<i>(empty)</i>"
        lines.append(f"<b>{h(label)}:</b> {val_str}")
    return "\n".join(lines)


def _is_oil_product(d: state.Draft) -> bool:
    return (d.matched_category or "").strip().lower() == "oil"


def _combine_main_label(d: state.Draft) -> None:
    """For seasoning flow: merge weight + sets into _qty_main_label.
    Oil flow sets _qty_main_label directly when the bottle count is picked.
    """
    w = d.data.get("_qty_main_weight", "").strip()
    s = d.data.get("_qty_main_sets", "").strip()
    if w and s:
        d.data["_qty_main_label"] = f"{s} x {w} seasoning"


def _combine_app_label(d: state.Draft) -> None:
    amt = d.data.get("_qty_app_amount", "").strip()
    s = d.data.get("_qty_app_sets", "").strip()
    if amt and s:
        d.data["_qty_app"] = f"{s} x {amt}"


def _finalize_quantity(d: state.Draft) -> None:
    """Combine the sub-answers into the single Quantity string we log."""
    main = d.data.get("_qty_main_label", "").strip()
    app = d.data.get("_qty_app", "").strip()
    base = d.data.get("_qty_base", "").strip()
    if app and base:
        d.data["quantity"] = f"{main} + {app} on {base}"
    elif app:
        d.data["quantity"] = f"{main} + {app}"
    else:
        d.data["quantity"] = main
    for k in (
        "_qty_main_label", "_qty_main_weight", "_qty_main_sets",
        "_qty_app", "_qty_app_amount", "_qty_app_sets", "_qty_base",
    ):
        d.data.pop(k, None)


def h(s: Any) -> str:
    """Escape user-supplied or sheet-supplied text so it's safe inside HTML messages."""
    return html.escape(str(s or ""), quote=False)


def kb(rows: list[list[tuple[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(text, callback_data=data) for text, data in row] for row in rows]
    )


def nav_row(include_back: bool = True, include_skip: bool = False) -> list[tuple[str, str]]:
    row: list[tuple[str, str]] = []
    if include_back:
        row.append(("◀ Back", "nav:back"))
    if include_skip:
        row.append(("⏭ Skip", "nav:skip"))
    row.append(("✖ Cancel", "nav:cancel"))
    return row


def _footer(update: Update) -> str:
    """Tidy footer on every bot reply.

    Format:
      <i>V1.4.x</i>                          ← no tokens consumed yet
      <i>V1.4.x · 🧠 1,234 tokens</i>        ← tokens consumed in this draft

    One line, italic, comma-separated thousands. No in/out breakdown — sales
    reps don't need that level of detail; one total is enough.
    """
    parts = [config.BOT_VERSION]
    user = update.effective_user
    if user:
        d = state.get(user.id)
        if d and d.tokens_total:
            parts.append(f"🧠 {d.tokens_total:,} tokens")
    return "<i>" + " · ".join(parts) + "</i>"


# User IDs we last told "please tap the button above". When the callback
# eventually arrives, we force a NEW message instead of editing the stale
# picker (which is now scrolled above the reminder and looks silent).
_stuck_reminder_users: set[int] = set()


def _mark_stuck_reminder(user_id: int) -> None:
    _stuck_reminder_users.add(user_id)


# --- group-chat hijack protection ---
# When the bot replies with inline buttons in a group, we remember which user
# the message was sent FOR. If a different user taps those buttons, we refuse
# the click and send a fresh nudge instead of clobbering the original user's
# UI. Bounded so memory doesn't grow without limit; cleared on bot restart
# (acceptable — owners get repopulated as users interact again).
_KB_OWNER_CAP = 500
_kb_owners: dict[tuple[int, int], int] = {}


def _register_kb_owner(chat_id: int | None, message_id: int | None, user_id: int | None) -> None:
    if chat_id is None or message_id is None or user_id is None:
        return
    _kb_owners[(chat_id, message_id)] = user_id
    while len(_kb_owners) > _KB_OWNER_CAP:
        _kb_owners.pop(next(iter(_kb_owners)), None)


def _kb_owner(chat_id: int, message_id: int) -> int | None:
    return _kb_owners.get((chat_id, message_id))


async def send(
    update: Update,
    text: str,
    markup: InlineKeyboardMarkup | None = None,
    *,
    with_footer: bool = True,
):
    """Send/edit a message to the user.

    The version + token footer is ON by default. Pass `with_footer=False` only
    when stacking multiple bot replies for one logical action and you don't
    want the footer to repeat on each fragment.

    If the message has inline buttons, the originating user is recorded so
    on_callback can refuse cross-user clicks in group chats.
    """
    full = f"{text}\n\n{_footer(update)}" if with_footer else text
    user = update.effective_user
    stuck = bool(user and user.id in _stuck_reminder_users)
    sent_msg = None
    if update.callback_query and not stuck:
        try:
            await update.callback_query.edit_message_text(
                full, reply_markup=markup, parse_mode=ParseMode.HTML
            )
            sent_msg = update.callback_query.message
        except Exception:  # noqa: BLE001 — fall through and send new message
            pass
    if sent_msg is None:
        if stuck and user:
            _stuck_reminder_users.discard(user.id)
        chat = update.effective_chat
        sent_msg = await chat.send_message(
            full, reply_markup=markup, parse_mode=ParseMode.HTML
        )
    if markup is not None and sent_msg is not None and user is not None:
        _register_kb_owner(
            getattr(getattr(sent_msg, "chat", None), "id", None),
            getattr(sent_msg, "message_id", None),
            user.id,
        )


def _effective_comment(d: state.Draft) -> str:
    """Comment that actually gets shown/saved.

    If the user picked a catalog match at 1/15, we guarantee the code + name
    are present in the comment — even if they later typed something different
    at 2/15. The code is prepended in brackets unless already present.
    """
    user_comment = (d.data.get("comment") or "").strip()
    if not d.matched_code:
        return user_comment
    code = d.matched_code
    name = d.data.get("seasoning", "") or ""
    if code and code in user_comment:
        return user_comment
    prefix = f"[{code} — {name}]" if name else f"[{code}]"
    return f"{prefix} {user_comment}".strip()


def field_index(key: str) -> int:
    for i, (k, _) in enumerate(FIELDS):
        if k == key:
            return i
    return -1


def next_stage(current: str) -> str:
    i = field_index(current)
    if i == -1 or i == len(FIELDS) - 1:
        return "review"
    return FIELDS[i + 1][0]


def prev_stage(current: str) -> str:
    i = field_index(current)
    if i <= 0:
        return FIELDS[0][0]
    return FIELDS[i - 1][0]


# --------------------------- authorization ---------------------------

async def _authorized(update: Update) -> bool:
    user = update.effective_user
    if not user:
        return False
    try:
        # Runs off-thread so gspread's blocking call doesn't freeze the loop.
        # load_users() inside is cached (5 min), so this is near-instant after
        # first hit.
        ok = await asyncio.to_thread(sheets.is_user_authorized, user.id, user.username)
    except Exception as e:  # noqa: BLE001
        log.exception("auth check failed for uid=%s uname=%s: %s", user.id, user.username, e)
        ok = False
    if not ok:
        log.warning("auth denied: uid=%s uname=%s", user.id, user.username)
        await send(
            update,
            "🔒 <b>You're not authorized to use this bot.</b>\n\n"
            "Please ask the admin to add you, and share these details:\n"
            f"• Username: <code>@{h(user.username or '(none)')}</code>\n"
            f"• Telegram ID: <code>{user.id}</code>",
        )
    return ok


# --------------------------- commands ---------------------------

def _is_update_sample_owner(user) -> bool:
    """True only for the Telegram username allowed to run /updatesamplelist."""
    uname = (getattr(user, "username", "") or "").lstrip("@").lower()
    return bool(uname) and uname == config.UPDATE_SAMPLE_OWNER


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await _authorized(update):
        return
    user = update.effective_user
    # Don't start a draft yet — wait for the user to pick "new request".
    state.clear(user.id)
    menu = [
        [("🌶 Find a seasoning & raise request", "menu:new")],
        [("📄 Paste a multi-seasoning email", "menu:bulk")],
        [("📷 Scan a product photo", "menu:scan")],
        [("✏️ Enter a code (price lookup)", "menu:code")],
        [("📋 My sample requests", "menu:samples")],
    ]
    if _is_update_sample_owner(user):
        menu.append([("🔄 Sync MMS Sample Master List", "menu:updsample")])
    await send(
        update,
        "👋 <b>Hi there — what can I help with?</b>",
        kb(menu),
    )


async def cmd_bulk(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await _authorized(update):
        return
    await _start_bulk(update, ctx)


async def cmd_samples(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await _authorized(update):
        return
    await show_samples_menu(update, ctx)


async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state.clear(update.effective_user.id)
    for k in ("seasoning_queries", "seasoning_candidates", "seasoning_query"):
        ctx.user_data.pop(k, None)
    await send(
        update,
        "✖ Draft cancelled.",
        kb([[("🏠 Main menu", "menu:home")]]),
    )


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    is_admin = _is_update_sample_owner(user)
    lines = [
        "<b>📚 NPSampleBot — commands</b>",
        "",
        "<b>Sample requests</b>",
        "/start — main menu",
        "/bulk — paste a multi-seasoning email, I split it for you",
        "/samples — review the requests you've raised",
        "",
        "<b>While drafting</b>",
        "/edit — jump back to the review to change any field",
        "/cancel — discard the current draft",
        "",
        "<b>Product lookup</b>",
        "/pp <code> — fetch price (Code · Name · R&amp;D Price · Raw Material Cost)",
        "/scan — send a photo, I OCR codes and run /pp on each",
        "",
        "<b>Account</b>",
        "/whoami — your Telegram ID and username",
        "/help — this message",
    ]
    if is_admin:
        lines += [
            "",
            "<b>🔧 Admin</b>",
            "/reload — refresh seasoning &amp; customer lists from Sheets",
            "/diag — diagnostics (auth / sheet visibility)",
            "/updatesamplelist — sync Sample Master List from MMS (24h cooldown; "
            "append <code>force</code> to override)",
        ]
    await send(update, "\n".join(lines))


SAMPLE_SYNC_COOLDOWN_HOURS = 24


async def cmd_update_sample_list(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Sync 'Sample Master List 2024-Present' from MMS (ragonic-only).

    Fetches Sample Date Out from SAMPLE_UPDATE_START (default 2026-03-01)
    through today, upserts by Product Code, and fills Flavour Profile +
    Taste describe via Claude for genuinely new products only.

    Rate-limited: refuses to re-sync within 24h of the last successful run
    unless invoked as ``/updatesamplelist force``.
    """
    if not await _authorized(update):
        return
    user = update.effective_user
    if not _is_update_sample_owner(user):
        await send(update, "🔒 This command is restricted.")
        return
    args = (ctx.args if hasattr(ctx, "args") else []) or []
    force = any(a.lower() in {"force", "now", "--force"} for a in args)
    await _run_update_sample_list(update, ctx, force=force)


async def _run_update_sample_list(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE, force: bool = False
):
    import datetime as _dt

    import mms_client
    import requests

    if not config.MMS_PASSWORD:
        await send(update, "⚠️ MMS_PASSWORD is not configured. Set it on Railway and redeploy.")
        return

    # 24h cooldown — skip only if not forced.
    if not force:
        last = await asyncio.to_thread(sheets.get_last_sample_sync)
        if last is not None:
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            now_utc = datetime.now(timezone.utc)
            age = now_utc - last
            cooldown = _dt.timedelta(hours=SAMPLE_SYNC_COOLDOWN_HOURS)
            if age < cooldown:
                remaining = cooldown - age
                hrs = int(remaining.total_seconds() // 3600)
                mins = int((remaining.total_seconds() % 3600) // 60)
                await send(
                    update,
                    "ℹ️ <b>Sample Master List is already up to date.</b>\n\n"
                    f"Last synced: <b>{last.strftime('%d %b %Y %H:%M UTC')}</b>\n"
                    f"Next auto-sync allowed in: <b>{hrs}h {mins}m</b>\n\n"
                    "<i>MMS rarely changes within 24h, so we skip to save your "
                    "API quota. If you truly need a fresh pull (e.g. you just "
                    "edited MMS), run:</i>\n"
                    "<code>/updatesamplelist force</code>",
                )
                return

    try:
        start_date = _dt.datetime.strptime(config.SAMPLE_UPDATE_START, "%Y-%m-%d").date()
    except ValueError:
        start_date = _dt.date(2026, 3, 1)
    end_date = _dt.date.today()

    status = await update.effective_chat.send_message(
        f"⏳ Logging into MMS…\nSyncing <b>{start_date.strftime('%d %b %Y')}</b> → "
        f"<b>{end_date.strftime('%d %b %Y')}</b>\n\n<i>{config.BOT_VERSION}</i>",
        parse_mode=ParseMode.HTML,
    )

    async def _set(msg: str) -> None:
        try:
            await status.edit_text(msg + f"\n\n<i>{config.BOT_VERSION}</i>", parse_mode=ParseMode.HTML)
        except Exception:  # noqa: BLE001
            pass

    session = requests.Session()
    try:
        ok = await asyncio.to_thread(
            mms_client.login, session, config.MMS_USER, config.MMS_PASSWORD
        )
    except Exception as e:  # noqa: BLE001
        log.exception("MMS login error: %s", e)
        await _set(f"❌ MMS login error: <code>{h(str(e)[:200])}</code>")
        return
    if not ok:
        await _set("❌ MMS login failed — check MMS_USER / MMS_PASSWORD env vars.")
        return

    await _set("✅ Logged into MMS.\n⏳ Fetching sample submissions…")
    t0 = datetime.now(timezone.utc)
    try:
        rows = await asyncio.to_thread(
            mms_client.fetch_all_samples, session, start_date, end_date
        )
    except Exception as e:  # noqa: BLE001
        log.exception("MMS fetch error: %s", e)
        await _set(f"❌ MMS fetch failed: <code>{h(str(e)[:200])}</code>")
        return

    fetch_secs = (datetime.now(timezone.utc) - t0).total_seconds()
    await _set(
        f"✅ Pulled <b>{len(rows)}</b> MMS rows in {fetch_secs:.0f}s.\n"
        "⏳ Dedup + upsert into Google Sheets…"
    )

    # Dedupe by Product Code — latest Sample Date Out wins (same rule as backfill).
    best: dict[str, mms_client.SampleRow] = {}
    for r in rows:
        code = (r.product_code or "").strip()
        if not code:
            continue
        d = r.sample_date_out_as_date() or _dt.date(1900, 1, 1)
        prev = best.get(code)
        if prev is None or (prev.sample_date_out_as_date() or _dt.date(1900, 1, 1)) < d:
            best[code] = r

    def _iso(r: mms_client.SampleRow) -> str:
        d = r.sample_date_out_as_date()
        return d.isoformat() if d else (r.sample_date_out or "").strip()

    incoming = [
        {
            "Seasoning Name": r.product_name,
            "Code": r.product_code,
            "Country": r.country,
            "Sales": r.sales,
            "R&D Price (USD)": r.rd_price,
            "Sample Date Out": _iso(r),
        }
        for r in best.values()
    ]

    t1 = datetime.now(timezone.utc)
    ai.reset_blurb_usage()  # so the totals below only reflect THIS run
    try:
        added, updated = await asyncio.to_thread(
            sheets.upsert_sample_master, incoming, ai.taste_blurb_sync
        )
    except Exception as e:  # noqa: BLE001
        log.exception("Sheet upsert failed: %s", e)
        await _set(f"❌ Sheet upsert failed: <code>{h(str(e)[:200])}</code>")
        return

    upsert_secs = (datetime.now(timezone.utc) - t1).total_seconds()
    total_secs = (datetime.now(timezone.utc) - t0).total_seconds()

    # Claude Haiku token accounting (only blurb calls — we don't hit Claude
    # anywhere else in this flow). Haiku 4.5 rates are USD 1 / 1M input,
    # USD 5 / 1M output. Show an estimated cost so the user sees the spend.
    usage = ai.get_blurb_usage()
    t_in = usage["input_tokens"]
    t_out = usage["output_tokens"]
    cost_usd = (t_in / 1_000_000) * 1.0 + (t_out / 1_000_000) * 5.0

    # Record the successful run so the 24h cooldown starts ticking.
    sync_time = datetime.now(timezone.utc)
    try:
        await asyncio.to_thread(sheets.set_last_sample_sync, sync_time)
    except Exception as e:  # noqa: BLE001
        log.warning("Could not record last-sync timestamp: %s", e)

    lines = [
        "✅ <b>Sample Master List updated.</b>",
        "",
        f"Window: {start_date.strftime('%d %b %Y')} → {end_date.strftime('%d %b %Y')}",
        f"MMS rows pulled: <b>{len(rows)}</b>",
        f"Unique products: <b>{len(incoming)}</b>",
        f"➕ Added: <b>{added}</b>",
        f"🔁 Refreshed: <b>{updated}</b>",
        f"⏱ Fetch {fetch_secs:.0f}s · Upsert {upsert_secs:.0f}s · Total {total_secs:.0f}s",
        "",
        "<b>Anthropic API usage (Haiku 4.5)</b>",
        f"🤖 Claude calls: <b>{usage['calls']}</b>",
        f"↳ Input tokens: <b>{t_in:,}</b>",
        f"↳ Output tokens: <b>{t_out:,}</b>",
        f"💵 Est. cost: <b>US$ {cost_usd:.4f}</b>",
        "",
        f"<i>Next auto-sync allowed after "
        f"{(sync_time + _dt.timedelta(hours=SAMPLE_SYNC_COOLDOWN_HOURS)).strftime('%d %b %Y %H:%M UTC')} "
        f"· use /updatesamplelist force to bypass.</i>",
    ]
    await _set("\n".join(lines))


async def cmd_whoami(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await send(
        update,
        f"Username: <code>@{h(u.username or '(none)')}</code>\nID: <code>{u.id}</code>",
        with_footer=True,
    )


_PP_CODE_RE = re.compile(r"\bS-[A-Za-z0-9]{3,}(?:-[A-Za-z0-9]{1,4})?\b", re.IGNORECASE)


def _dedupe_codes(codes: list[str], cap: int = 5) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for c in codes:
        u = c.upper()
        if u not in seen:
            seen.add(u)
            out.append(u)
        if len(out) >= cap:
            break
    return out


async def _run_pp_for_codes(update: Update, codes: list[str]) -> None:
    """Fetch /pp for each code, edit-in-place loader, audit-log every result.

    Used by both `/pp <code>` and the photo-scan flow. Caller passes already
    deduplicated, capped, uppercase codes.
    """
    if not codes:
        return
    client = mms_product.get_client()
    chat = update.effective_chat
    user = update.effective_user
    uname = (user.username or user.full_name or "") if user else ""
    uid = user.id if user else ""

    def _audit(**kw):
        asyncio.create_task(
            asyncio.to_thread(
                sheets.log_pp_query,
                username=uname,
                user_id=uid,
                **kw,
            )
        )

    for code in codes:
        placeholder = await chat.send_message(
            f"⏳ Fetching <code>{h(code)}</code> from MMS…",
            parse_mode=ParseMode.HTML,
        )
        try:
            await chat.send_action("typing")
        except Exception:  # noqa: BLE001
            pass

        async def _replace(text: str) -> None:
            try:
                await placeholder.edit_text(text, parse_mode=ParseMode.HTML)
            except Exception:  # noqa: BLE001 — message may be too old to edit
                await chat.send_message(text, parse_mode=ParseMode.HTML)

        try:
            product = await asyncio.to_thread(client.fetch_product, code)
        except mms_product.ProductNotFound:
            await _replace(f"😕 No product found for <code>{h(code)}</code>.")
            _audit(query=code, result="Not Found")
            continue
        except mms_product.MMSError as e:
            log.warning("MMS error for %s: %s", code, e)
            await _replace(
                f"😬 MMS error for <code>{h(code)}</code>: {h(str(e))}"
            )
            _audit(query=code, result="MMS Error", error=str(e))
            continue
        except Exception as e:  # noqa: BLE001
            log.exception("Unexpected /pp error for %s", code)
            await _replace(
                f"😵 Couldn't fetch <code>{h(code)}</code>: {h(str(e))}"
            )
            _audit(query=code, result="Error", error=str(e))
            continue
        # Apply the standing markup (config.RMC_MARKUP_USD) before showing
        # OR logging the cost — every customer-facing surface (/pp, /scan,
        # "✏️ Enter a code") and the Query-tab audit log stay consistent.
        adj_rmc = product.raw_material_cost_usd + config.RMC_MARKUP_USD
        rd = (
            f"USD {product.rd_price_usd:.2f}"
            if product.rd_price_usd is not None else "—"
        )
        body = (
            f"<b>Code:</b> <code>{h(product.code)}</code>\n"
            f"<b>Name:</b> {h(product.name)}\n"
            f"<b>R&amp;D Price:</b> {h(rd)}\n"
            f"<b>Raw Material Cost:</b> USD {adj_rmc:.4f}"
        )
        await _replace(body)
        _audit(
            query=code,
            result="Found",
            matched_code=product.code,
            name=product.name,
            rd_price_usd=product.rd_price_usd,
            raw_material_cost_usd=adj_rmc,
        )


async def cmd_pp(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """`/pp <code>` — fetch product price summary from MMS.

    Returns: Code, Name, R&D Price (USD), Raw Material Cost (USD).
    Pricing-only — does NOT fetch the full ingredient table.
    Goes straight to MMS each call (no caching) so the price stays fresh.
    """
    if not await _authorized(update):
        return
    msg = update.effective_message
    raw = " ".join(ctx.args) if ctx.args else (msg.text or "").partition(" ")[2]
    codes = _PP_CODE_RE.findall(raw)
    if not codes:
        await send(
            update,
            "💲 <b>Product price lookup</b>\n\n"
            "Send a product code, e.g. <code>/pp S-62RG3-19</code>.",
        )
        return
    unique = _dedupe_codes(codes, cap=5)
    if len(codes) > 5:
        await send(update, f"🙏 Max 5 codes per /pp — running first 5: {', '.join(unique)}")
    await _run_pp_for_codes(update, unique)


async def cmd_scan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """`/scan` — prompt the user to send a photo of product code(s)."""
    if not await _authorized(update):
        return
    ctx.user_data["awaiting_scan_photo"] = True
    await send(
        update,
        "📷 <b>Scan a product photo</b>\n\n"
        "Send a photo of one or more product code labels "
        "(<code>S-XXXXX-XX</code>). I'll read them and pull the price for each.",
    )


async def on_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle a photo upload — OCR for product codes, then auto-/pp each one.

    GATED: only fires if the user explicitly opted into a scan via /scan or the
    main-menu Scan button. Without this gate, every photo any user posts in a
    group chat would trigger OCR (token spam, scan-result spam).
    """
    if not await _authorized(update):
        return
    msg = update.effective_message
    if not msg or not msg.photo:
        return
    # Opt-in gate — pop is single-use, so each /scan accepts ONE photo.
    if not ctx.user_data.pop("awaiting_scan_photo", None):
        return

    chat = update.effective_chat
    notice = await chat.send_message(
        "🔍 Reading product code(s) from your photo… one sec!",
        parse_mode=ParseMode.HTML,
    )

    async def _cleanup() -> None:
        # Drop the loading notice + the user's original photo so the group
        # chat doesn't fill up with scanned images. In groups the bot needs
        # the "delete messages" admin permission for the photo delete to
        # actually take effect — failures are swallowed silently.
        for target in (notice, msg):
            try:
                await target.delete()
            except Exception:  # noqa: BLE001 — message may already be gone / no permission
                pass

    try:
        await chat.send_action("typing")
    except Exception:  # noqa: BLE001
        pass

    try:
        photo = msg.photo[-1]  # largest size for best OCR
        tg_file = await ctx.bot.get_file(photo.file_id)
        buf = await tg_file.download_as_bytearray()
    except Exception as e:  # noqa: BLE001
        log.exception("Photo download failed")
        await _cleanup()
        await send(update, f"😕 Couldn't read that photo: {h(str(e))}")
        return

    # Build the catalog set (uppercase) once, used by the self-healer.
    try:
        seasonings = await asyncio.to_thread(sheets.load_seasonings)
        catalog_codes = {
            str(s.get("code", "")).strip().upper()
            for s in seasonings
            if s.get("code")
        }
    except Exception as e:  # noqa: BLE001
        log.warning("Catalog load failed for scan: %s", e)
        catalog_codes = set()

    try:
        result = await vision_scan.scan_image(bytes(buf), catalog_codes)
    except Exception as e:  # noqa: BLE001
        log.exception("OCR failed")
        await _cleanup()
        await send(update, f"😵 OCR failed: {h(str(e))}")
        return

    # OCR done — drop the loading notice AND the user's original photo so
    # the group chat doesn't accumulate clutter.
    await _cleanup()

    if not result.codes:
        await send(
            update,
            "🙈 Couldn't spot a product code in that photo.\n"
            "Try again with better lighting or a closer crop — codes look like "
            "<code>S-XXXXX-XX</code> or <code>S-XXXXXX</code>.",
        )
        return

    # Build a summary so the user sees what we detected (and any auto-corrections).
    lines = [f"🎯 Detected <b>{len(result.codes)}</b> code(s):"]
    for raw, final in zip(result.raw_codes, result.codes):
        if raw != final:
            lines.append(f"  • <code>{h(raw)}</code> → <code>{h(final)}</code> 🩹 auto-corrected")
        else:
            lines.append(f"  • <code>{h(final)}</code>")
    if result.unmatched:
        lines.append(
            f"\n⚠️ Not in catalog (will still try MMS): "
            + ", ".join(f"<code>{h(c)}</code>" for c in result.unmatched)
        )
    lines.append("\nFetching prices now…")
    await send(update, "\n".join(lines))

    # Cap at 5 to match the /pp ceiling and avoid spamming MMS.
    unique = _dedupe_codes(result.codes, cap=5)
    await _run_pp_for_codes(update, unique)


async def cmd_diag(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Diagnostic: bypasses auth, directly reports what the bot can read
    from the Authorized Users tab. Used to debug 'not authorized' problems."""
    u = update.effective_user
    your_id = str(u.id)
    your_uname = (u.username or "").lstrip("@").lower()

    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    lines = [
        "<b>🩺 Diagnostic report</b>",
        f"Bot version: <code>{h(config.BOT_VERSION)}</code>",
        f"Your ID: <code>{h(your_id)}</code>",
        f"Your username: <code>@{h(your_uname or '(none)')}</code>",
        "",
        f"SA JSON env: <code>{'SET' if sa_json else 'MISSING'} (len={len(sa_json)})</code>",
        f"OPS_SHEET_ID: <code>{h(config.OPS_SHEET_ID[:12] + '…' if config.OPS_SHEET_ID else 'MISSING')}</code>",
    ]

    try:
        users = await asyncio.to_thread(sheets.load_users, True)
    except Exception as e:  # noqa: BLE001
        lines.append(f"\n❌ <b>load_users() failed:</b> <code>{h(str(e)[:300])}</code>")
        await send(update, "\n".join(lines))
        return

    lines.append(f"\n✅ Loaded <b>{len(users)}</b> row(s) from Authorized Users tab.")

    if users:
        # Show column headers actually present in the sheet.
        headers = list(users[0].keys())
        lines.append(f"Columns: <code>{h(', '.join(headers))}</code>")

    # Look for matching row.
    match_idx = -1
    for i, row in enumerate(users, start=2):  # sheet row numbers start at 2
        rid = str(row.get("Telegram User ID", "")).strip()
        rname = str(row.get("Telegram Username", "")).lstrip("@").lower().strip()
        if rid == your_id or (rname and rname == your_uname):
            match_idx = i
            active = str(row.get("Active", "")).strip()
            lines.append(
                f"\n🎯 Found you at sheet row <b>{i}</b>:\n"
                f"  • ID cell: <code>{h(rid or '(empty)')}</code>\n"
                f"  • Username cell: <code>{h(rname or '(empty)')}</code>\n"
                f"  • Active cell: <code>{h(active or '(empty)')}</code>"
            )
            if active.lower() not in {"y", "yes", "true", "1"}:
                lines.append(f"⚠️ Active is <b>{h(active)}</b> — must be <code>Y</code> to authorize.")
            break
    if match_idx == -1:
        lines.append(
            "\n❌ <b>Your ID/username is NOT in the sheet.</b>\n"
            "Add a row to the 'Authorized Users' tab with:\n"
            f"  • Telegram Username: <code>@{h(your_uname)}</code>\n"
            f"  • Telegram User ID: <code>{h(your_id)}</code>\n"
            "  • Active: <code>Y</code>"
        )

    await send(update, "\n".join(lines))


async def cmd_reload(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await _authorized(update):
        return
    sheets.invalidate_caches()
    try:
        await update.effective_chat.send_action("typing")
    except Exception:  # noqa: BLE001
        pass
    # Re-warm so the next user of the day doesn't eat the cold-load penalty.
    try:
        await asyncio.gather(
            asyncio.to_thread(sheets.load_seasonings),
            asyncio.to_thread(sheets.load_customer_master),
            asyncio.to_thread(sheets.load_customers),
            asyncio.to_thread(sheets.load_users),
        )
    except Exception as e:  # noqa: BLE001
        log.warning("reload warmup failed: %s", e)
    await send(update, "🔄 Caches refreshed from Google Sheets.")


async def cmd_edit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await _authorized(update):
        return
    user_id = update.effective_user.id
    d = state.get(user_id)
    if not d:
        if state.consume_expired_flag(user_id):
            await send(
                update,
                f"⏰ <b>Your draft expired</b> after {config.DRAFT_TIMEOUT_MINUTES} min of no input.\n\n"
                "Tap below to start fresh:",
                kb([[("➕ New request", "menu:new"), ("🏠 Main menu", "menu:home")]]),
            )
        else:
            await send(
                update,
                "🤔 <b>I don't have an active draft for you.</b>\n\n"
                "<i>This sometimes happens after a bot update — your in-progress "
                "draft gets reset when the bot redeploys.</i>\n\n"
                "Tap below to start a new one:",
                kb([[("➕ New request", "menu:new"), ("🏠 Main menu", "menu:home")]]),
            )
        return
    d.stage = "review"
    await ask(update, ctx, d)


# --------------------------- question dispatch ---------------------------

async def ask(update: Update, ctx: ContextTypes.DEFAULT_TYPE, d: state.Draft):
    d.touch()
    handler = _QUESTIONS.get(d.stage)
    if handler is None:
        await send(update, f"Unknown stage: {d.stage}. Send /start.")
        return
    await handler(update, ctx, d)


async def q_seasoning(update, ctx, d: state.Draft):
    current = d.data.get("seasoning", "")
    hint = f"\n\nCurrent: <i>{h(current)}</i>" if current else ""
    await send(
        update,
        "🌶 <b>Seasoning Requested</b>\n\n"
        "Type what you're looking for — I'll suggest the closest matches.\n\n"
        "💡 You can add filters: "
        "<i>cheese for bangladesh</i> · <i>bbq under $3</i> · "
        "<i>spicy chinese style</i>" + hint,
        kb([nav_row(include_back=False)]),
    )
    d.sub = "ask"


async def q_comment(update, ctx, d):
    existing = d.data.get("comment", "")
    hint = f"\n\nCurrent: <i>{h(existing)}</i>" if existing else ""
    await send(
        update,
        "💬 <b>Comment to R&amp;D</b>\n\n"
        "What should R&amp;D do — use an existing code, or develop a new one?\n"
        "<i>Examples:</i>\n"
        "• Use code S-WCFG2-10 as a snack seasoning\n"
        "• New code needed — peppery, less spicy" + hint,
        kb([nav_row()]),
    )


async def q_quantity(update, ctx, d):
    existing = d.data.get("quantity", "")
    hint = f"\n\nCurrent: <i>{h(existing)}</i>" if existing else ""

    # Entering the stage fresh — reset scratch and start at "main".
    if d.sub not in _QTY_SUBS:
        for k in (
            "_qty_main_label", "_qty_main_weight", "_qty_main_sets",
            "_qty_app", "_qty_app_amount", "_qty_app_sets", "_qty_base",
        ):
            d.data.pop(k, None)
        d.sub = "main"

    is_oil = _is_oil_product(d)

    if d.sub == "main":
        if is_oil:
            prompt = (
                "🛢 <b>Quantity</b>\n\n"
                "This is an <b>oil</b>. How many small bottles are needed?"
            )
            buttons: list[list[tuple[str, str]]] = []
            row: list[tuple[str, str]] = []
            for b in OIL_BOTTLES:
                row.append((f"{b} bottle{'s' if b != '1' else ''}", f"qm:b:{b}"))
                if len(row) == 2:
                    buttons.append(row)
                    row = []
            if row:
                buttons.append(row)
        else:
            prompt = (
                "⚖️ <b>Quantity</b>\n\n"
                "How much seasoning is required?"
            )
            buttons = []
            row = []
            for w in SEASONING_WEIGHTS:
                row.append((w, f"qm:w:{w}"))
                if len(row) == 3:
                    buttons.append(row)
                    row = []
            if row:
                buttons.append(row)
        buttons.append([("⌨️ Type it manually", "qm:manual")])
        buttons.append(nav_row())
        await send(update, prompt + hint, kb(buttons))
        return

    if d.sub == "main_manual":
        label = "bottle count / amount" if is_oil else "weight (e.g. 250g)"
        await send(
            update,
            f"⌨️ Type the {label} you need." + hint,
            kb([nav_row()]),
        )
        return

    if d.sub == "main_sets":
        weight = d.data.get("_qty_main_weight", "")
        buttons = []
        row: list[tuple[str, str]] = []
        for n in SET_COUNTS:
            row.append((n, f"qs:{n}"))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([("⌨️ Type it manually", "qs:manual")])
        buttons.append(nav_row())
        await send(
            update,
            f"🔢 <b>How many sets of {h(weight)} seasoning?</b>",
            kb(buttons),
        )
        return

    if d.sub == "main_sets_manual":
        await send(
            update,
            "⌨️ Type the number of sets (e.g. <i>4</i>).",
            kb([nav_row()]),
        )
        return

    if d.sub == "need_app":
        picked = d.data.get("_qty_main_label", "")
        await send(
            update,
            f"🧪 <b>Application sample needed?</b>\n\n"
            f"Main quantity: <b>{h(picked)}</b>",
            kb([[("✅ Yes", "qa:Y"), ("❌ No", "qa:N")], nav_row()]),
        )
        return

    if d.sub == "app_amount":
        await send(
            update,
            "⚖️ <b>Application gram needed?</b>\n\n"
            "Type the weight, e.g. <i>20g</i>.",
            kb([nav_row()]),
        )
        return

    if d.sub == "app_sets":
        amt = d.data.get("_qty_app_amount", "")
        buttons = []
        row = []
        for n in SET_COUNTS:
            row.append((n, f"qas:{n}"))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([("⌨️ Type it manually", "qas:manual")])
        buttons.append(nav_row())
        await send(
            update,
            f"🔢 <b>How many sets of {h(amt)} application sample?</b>",
            kb(buttons),
        )
        return

    if d.sub == "app_sets_manual":
        await send(
            update,
            "⌨️ Type the number of application sets (e.g. <i>2</i>).",
            kb([nav_row()]),
        )
        return

    if d.sub == "app_base":
        buttons = [[(b, f"qb:{i}")] for i, b in enumerate(APP_BASES)]
        buttons.append([("⌨️ Type it manually", "qb:manual")])
        buttons.append(nav_row())
        await send(
            update,
            "🎯 <b>Application Base Product</b>\n\nWhat base will the sample be applied on?",
            kb(buttons),
        )
        return

    if d.sub == "app_base_manual":
        await send(update, "⌨️ Type the base product.", kb([nav_row()]))
        return


async def q_price_budget(update, ctx, d):
    existing = d.data.get("price_budget", "")
    hint = f"\n\nCurrent: <i>{h(existing)}</i>" if existing else ""
    if d.sub not in {"currency", "amount"}:
        d.sub = "currency"
    if d.sub == "currency":
        await send(
            update,
            "💰 <b>Selling Price Budget</b>\n\n"
            "Pick a currency:" + hint,
            kb([[("USD", "cur:USD"), ("SGD", "cur:SGD")], nav_row()]),
        )
    else:
        cur = d.data.get('_currency', 'USD')
        await send(
            update,
            f"💰 <b>Selling Price Budget</b>\n\n"
            f"Type the max budget in {cur} (e.g. <i>3.00</i>)." + hint,
            kb([nav_row()]),
        )


async def q_app_method(update, ctx, d):
    existing = d.data.get("app_method", "")
    hint = f"\n\nCurrent: <i>{h(existing)}</i>" if existing else ""
    buttons = [[(m, f"app:{m}")] for m in APP_METHODS]
    buttons.append(nav_row())
    await send(
        update,
        "🧪 <b>Application Method</b>\n\nPick one:" + hint,
        kb(buttons),
    )


async def q_dosage(update, ctx, d):
    existing = d.data.get("dosage", "")
    hint = f"\n\nCurrent: <i>{h(existing)}</i>" if existing else ""
    await send(
        update,
        "📏 <b>Dosage</b>\n\n"
        "Customer-suggested dosage (e.g. <i>7%</i>). Tap Skip if not sure." + hint,
        kb([nav_row(include_skip=True)]),
    )


async def q_requirement(update, ctx, d):
    existing = d.data.get("requirement", "")
    hint = f"\n\nCurrent: <i>{h(existing)}</i>" if existing else ""
    await send(
        update,
        "📜 <b>Requirement</b>\n\n"
        "Any specific regulations? Example: <i>NO MSG / GMO FREE / HALAL</i>. "
        "Skip if none." + hint,
        kb([nav_row(include_skip=True)]),
    )


async def q_market(update, ctx, d):
    existing = d.data.get("market", "")
    hint = f"\n\nCurrent: <i>{h(existing)}</i>" if existing else ""
    await send(
        update,
        "🌏 <b>Market</b>\n\nFor which market? (e.g. <i>Vietnam</i>)" + hint,
        kb([nav_row()]),
    )


async def q_deadline(update, ctx, d):
    existing = d.data.get("deadline", "")
    hint = f"\n\nCurrent: <i>{h(existing)}</i>" if existing else ""
    await send(
        update,
        "⏰ <b>Deadline</b>\n\n"
        "When does the customer need the sample by? "
        "Example: <i>30 April 2026</i> · <i>next Friday</i> · <i>2 weeks</i>." + hint,
        kb([nav_row()]),
    )


async def q_taste_check(update, ctx, d):
    existing = d.data.get("taste_check", "")
    hint = f"\n\nCurrent: <i>{h(existing)}</i>" if existing else ""
    await send(
        update,
        "👅 <b>Need to Check Taste?</b>" + hint,
        kb([[("✅ Yes", "yn:Y"), ("❌ No", "yn:N")], nav_row()]),
    )


async def q_customer_base(update, ctx, d):
    existing = d.data.get("customer_base", "")
    hint = f"\n\nCurrent: <i>{h(existing)}</i>" if existing else ""
    if d.sub == "manual":
        await send(
            update,
            "⌨️ Type the customer base (e.g. <i>crab-shape pellet</i>).",
            kb([nav_row()]),
        )
        return
    # Two-column grid of preset bases + an explicit Manual button.
    buttons = []
    row: list[tuple[str, str]] = []
    for i, b in enumerate(CUSTOMER_BASES):
        row.append((b, f"cb:{i}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([("⌨️ Type it manually", "cb:manual")])
    buttons.append(nav_row())
    await send(
        update,
        "🍿 <b>Customer Base</b>\n\n"
        "Pick one, or tap <i>Type it manually</i> to enter your own." + hint,
        kb(buttons),
    )


async def q_courier(update, ctx, d):
    existing = d.data.get("courier", "")
    hint = f"\n\nCurrent: <i>{h(existing)}</i>" if existing else ""
    buttons = [[(c, f"cou:{c}")] for c in COURIERS]
    buttons.append(nav_row())
    await send(
        update,
        "🚚 <b>Preferred Courier</b>\n\nPick one:" + hint,
        kb(buttons),
    )


async def q_company_name(update, ctx, d):
    existing = d.data.get("company_name", "")
    hint = f"\n\nCurrent: <i>{h(existing)}</i>" if existing else ""
    await send(
        update,
        "🏢 <b>Customer Company Name</b>\n\n"
        "Type the company name. If we already have them, I'll auto-fill the rest." + hint,
        kb([nav_row()]),
    )
    d.sub = "ask"


async def q_receiver_number(update, ctx, d):
    existing = d.data.get("receiver_number", "")
    hint = f"\n\nCurrent: <i>{h(existing)}</i>" if existing else ""
    await send(
        update,
        "📞 <b>Receiver Number</b>\n\n"
        "Phone number for the courier (e.g. <i>+65 9123 4567</i>)." + hint,
        kb([nav_row()]),
    )


async def q_address(update, ctx, d):
    existing = d.data.get("address", "")
    hint = f"\n\nCurrent: <i>{h(existing)}</i>" if existing else ""
    await send(
        update,
        "📍 <b>Address</b>\n\n"
        "Where the sample should be shipped." + hint,
        kb([nav_row()]),
    )


async def q_receiving_person(update, ctx, d):
    existing = d.data.get("receiving_person", "")
    hint = f"\n\nCurrent: <i>{h(existing)}</i>" if existing else ""
    await send(
        update,
        "🙋 <b>Receiving Person</b>\n\nWho should the courier ask for? (e.g. <i>Ms Jenny</i>)" + hint,
        kb([nav_row()]),
    )


async def q_review(update, ctx, d: state.Draft):
    lines = ["<b>📝 Review your request</b>\n"]
    for key, label in FIELDS:
        if key == "comment":
            val = _effective_comment(d)
        else:
            val = d.data.get(key, "")
        val_str = h(val) if val else "<i>(empty)</i>"
        lines.append(f"<b>{h(label)}:</b> {val_str}")
    lines.append("\nAll good?")
    # Cancel separated from the primary actions to avoid one-tap mishaps.
    buttons = [
        [("✅ Submit", "rev:confirm")],
        [("✏️ Edit a field", "rev:edit")],
        [("✖ Discard draft", "nav:cancel")],
    ]
    await send(update, "\n".join(lines), kb(buttons))


_QUESTIONS = {
    "seasoning": q_seasoning,
    "comment": q_comment,
    "quantity": q_quantity,
    "price_budget": q_price_budget,
    "app_method": q_app_method,
    "dosage": q_dosage,
    "requirement": q_requirement,
    "market": q_market,
    "deadline": q_deadline,
    "taste_check": q_taste_check,
    "customer_base": q_customer_base,
    "courier": q_courier,
    "company_name": q_company_name,
    "receiver_number": q_receiver_number,
    "address": q_address,
    "receiving_person": q_receiving_person,
    "review": q_review,
}


# --------------------------- text handler ---------------------------

async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await _authorized(update):
        return
    user = update.effective_user
    text = (update.message.text or "").strip()
    if not text:
        return

    # Bulk-paste flow text states (await_paste, ask_budget_amt) run with no
    # active Draft — check those FIRST before the "no draft" guard.
    if await _handle_bulk_text(update, ctx, text):
        return

    # Manual code-entry flow ("✏️ Enter a code" on the main menu): user
    # opted into a one-shot price lookup. Single-use — pop the flag so
    # subsequent texts go through the normal flow.
    if ctx.user_data.pop("awaiting_code_text", None):
        codes = _PP_CODE_RE.findall(text)
        if not codes:
            await send(
                update,
                "🤔 That doesn't look like a product code (expected something "
                "like <code>S-668U1</code> or <code>S-62RG3-19</code>). Tap "
                "<i>Enter a code</i> on the main menu to try again.",
                kb([[("🏠 Main menu", "menu:home")]]),
            )
            return
        unique = _dedupe_codes(codes, cap=5)
        await _run_pp_for_codes(update, unique)
        return

    d = state.get(user.id)
    if not d:
        if state.consume_expired_flag(user.id):
            await send(
                update,
                f"⏰ <b>Your draft expired</b> after {config.DRAFT_TIMEOUT_MINUTES} min of no input.\n\n"
                "Tap below to start fresh:",
                kb([[("➕ New request", "menu:new"), ("🏠 Main menu", "menu:home")]]),
            )
        else:
            await send(
                update,
                "🤔 <b>I don't have an active draft for you.</b>\n\n"
                "<i>This sometimes happens after a bot update — your in-progress "
                "draft gets reset when the bot redeploys.</i>\n\n"
                "Tap below to start a new one:",
                kb([[("➕ New request", "menu:new"), ("🏠 Main menu", "menu:home")]]),
            )
        return
    d.touch()

    stage = d.stage

    # --- special inline flows ---
    if stage == "seasoning":
        await _handle_seasoning_text(update, ctx, d, text)
        return

    if stage == "company_name":
        await _handle_company_text(update, ctx, d, text)
        return

    if stage == "quantity":
        if d.sub == "main_manual":
            if _is_oil_product(d):
                # Oil manual entry — whatever they typed is the whole main
                # quantity; no "sets" question for oil.
                d.data["_qty_main_label"] = text
                d.sub = "need_app"
            else:
                d.data["_qty_main_weight"] = text
                d.sub = "main_sets"
            await ask(update, ctx, d)
            return
        if d.sub == "main_sets_manual":
            d.data["_qty_main_sets"] = text
            _combine_main_label(d)
            d.sub = "need_app"
            await ask(update, ctx, d)
            return
        if d.sub == "app_amount":
            d.data["_qty_app_amount"] = text
            d.sub = "app_sets"
            await ask(update, ctx, d)
            return
        if d.sub == "app_sets_manual":
            d.data["_qty_app_sets"] = text
            _combine_app_label(d)
            d.sub = "app_base"
            await ask(update, ctx, d)
            return
        if d.sub == "app_base_manual":
            d.data["_qty_base"] = text
            _finalize_quantity(d)
            await _advance(update, ctx, d)
            return
        _mark_stuck_reminder(user.id)
        await send(update, "👆 Tap a button above, or use <i>Type it manually</i>.")
        return

    if stage == "price_budget" and d.sub == "amount":
        cur = d.data.get("_currency", "USD")
        d.data["price_budget"] = f"{text} {cur}"
        d.data.pop("_currency", None)
        d.sub = ""
        await _advance(update, ctx, d)
        return

    if stage == "price_budget" and d.sub == "currency":
        _mark_stuck_reminder(user.id)
        await send(update, "👆 Tap USD or SGD above.")
        return

    # Fields that accept a button answer — remind user.
    if stage in {"app_method", "taste_check", "courier"}:
        _mark_stuck_reminder(user.id)
        await send(update, "👆 Tap one of the buttons above.")
        return

    # Customer base manual entry — sub-state set by tapping "Enter manually".
    if stage == "customer_base" and d.sub == "manual":
        d.data["customer_base"] = text
        d.sub = ""
        await _advance(update, ctx, d)
        return

    # Plain free-text fields
    d.data[stage] = text
    await _advance(update, ctx, d)


async def _handle_seasoning_text(update, ctx, d: state.Draft, text: str):
    chat = update.effective_chat

    # Conversational refinement: keep a short query history within the same
    # seasoning-stage session. Each new line the user types ADDS to their
    # search intent rather than replacing it. So:
    #   "instant soup seasoning"  → search "instant soup seasoning"
    #   then "i want more spicy"  → search "instant soup seasoning · spicy"
    # Cleared when the user picks a candidate, taps "🔄 Start over", or
    # cancels the draft.
    history: list[str] = list(ctx.user_data.get("seasoning_queries", []))
    history.append(text)
    # Cap history so it doesn't grow forever — only the last 4 turns count.
    if len(history) > 4:
        history = history[-4:]
    ctx.user_data["seasoning_queries"] = history
    combined_query = " · ".join(history)

    try:
        await chat.send_action("typing")
    except Exception:  # noqa: BLE001
        pass
    # Visible loader so the user knows we're working.
    placeholder = None
    try:
        placeholder = await chat.send_message(
            "🔍 Searching the catalog…",
            parse_mode=ParseMode.HTML,
        )
    except Exception:  # noqa: BLE001
        placeholder = None

    async def _drop_loader() -> None:
        if placeholder is not None:
            try:
                await placeholder.delete()
            except Exception:  # noqa: BLE001 — message may have been edited/deleted
                pass

    try:
        seasonings = await asyncio.to_thread(sheets.load_seasonings)
    except Exception as e:  # noqa: BLE001
        log.exception("load_seasonings failed: %s", e)
        await _drop_loader()
        await send(update, "⚠️ Couldn't read the seasoning master sheet. Continuing with your text.")
        d.data["seasoning"] = text
        d.matched_code = ""
        d.matched_price = ""
        ctx.user_data.pop("seasoning_queries", None)
        await _advance(update, ctx, d)
        return

    # Code-first match: if the user pasted a product code (exact, prefix or
    # suffix-trim variant), skip fuzzy name search and ask them to confirm.
    # `find_codes_matching` returns multiple if the user typed a base (e.g.
    # "S-668U1") that has several catalog variants ("S-668U1-02",
    # "S-668U1-03"), so the user can pick the right SKU.
    code_matches = matcher.find_codes_matching(text, seasonings)
    if code_matches:
        ctx.user_data["seasoning_candidates"] = code_matches[:5]
        ctx.user_data["seasoning_query"] = text
        if len(code_matches) == 1:
            c = code_matches[0]
            cat = c.get("category") or ""
            cat_str = f" · <i>{h(cat)}</i>" if cat else ""
            price = c.get("price") or "—"
            code = c.get("code") or "—"
            msg = (
                f"🎯 <b>Code match</b> for <code>{h(text)}</code>:\n\n"
                f"<b>{h(c['name'])}</b>{cat_str}\n"
                f"    code <code>{h(code)}</code> · {h(price)}\n\n"
                "Use this product?"
            )
            buttons = [
                [("✅ Yes, use it", "ssn:0")],
                [("🔍 No, search by name", "ssn:retry")],
                nav_row(include_back=False),
            ]
        else:
            lines = [
                f"🎯 <b>Found {len(code_matches)} matches</b> for <code>{h(text)}</code>:",
                "",
            ]
            buttons = []
            shown = code_matches[:5]
            for i, c in enumerate(shown):
                cat = c.get("category") or ""
                cat_str = f" · <i>{h(cat)}</i>" if cat else ""
                price = c.get("price") or "—"
                code = c.get("code") or "—"
                lines.append(
                    f"<b>{i+1}. {h(c['name'])}</b>{cat_str}\n"
                    f"    code <code>{h(code)}</code> · {h(price)}"
                )
                label = f"{i+1}. {c.get('code', '')} · {c['name']}"
                if len(label) > 40:
                    label = label[:38] + "…"
                buttons.append([(label, f"ssn:{i}")])
            lines.append("\nPick the one you want, or refine your code.")
            buttons.append([("🔍 Search by name instead", "ssn:retry")])
            buttons.append(nav_row(include_back=False))
            msg = "\n".join(lines)
        await _drop_loader()
        await send(update, msg, kb(buttons))
        return

    # Smart match: fuzzy-pool (name + category) + past-submissions boost,
    # then Claude rerank on the top 10. matcher also parses price filters
    # like "below 4.5 usd" / "under $3" out of the query. We feed it the
    # COMBINED query so multi-turn refinement keeps prior context.
    try:
        past = await asyncio.to_thread(sheets.load_past_submissions)
    except Exception as e:  # noqa: BLE001
        log.warning("load_past_submissions failed: %s", e)
        past = []

    # Pure local search — no AI tokens consumed. The matcher does the work:
    #   • token_set_ratio + WRatio scorer (order-insensitive, typo-tolerant)
    #   • generic-term stripping ("seasoning"/"powder"/"flavour" don't dominate)
    #   • category fold (catalog tab name is part of the choice string)
    #   • past-submissions boost (codes used for similar past queries lift)
    #   • code dedupe (same code in two tabs collapses to one row)
    # On the test set this ranks the right items at score 100 without help
    # from Claude. We keep ai.rerank_seasonings around in case we ever want
    # to re-enable it for harder queries.
    pool_candidates = matcher.top_seasonings(
        combined_query, seasonings, limit=10, pool=40, past_submissions=past
    )
    _cleaned, max_price = matcher.parse_seasoning_query(combined_query)

    # If a price cap was set but it killed the pool entirely, fall back to
    # the same search WITHOUT the cap so the user still gets the closest
    # matches (just above their budget). We surface the trade-off in the
    # message so the user can decide whether to refine.
    soft_price = False
    if not pool_candidates and max_price is not None:
        pool_candidates = matcher.top_seasonings(
            combined_query, seasonings, limit=10, pool=40,
            past_submissions=past, strict_price=False,
        )
        soft_price = True

    top = pool_candidates[:5]

    ctx.user_data["seasoning_candidates"] = top
    ctx.user_data["seasoning_query"] = text  # latest only — used by "Use my text"

    # Surface the price filter status so the user always knows what's been applied.
    if soft_price and not top:
        # Even the soft fallback found nothing — query has no matches at any price.
        cap_note = (
            f"\n⚠️ <i>No matches in the catalog for that query — even at any price.</i>"
        )
    elif soft_price:
        cap_note = (
            f"\n⚠️ <i>No matches under <b>${max_price:g} USD</b> — "
            "showing the closest above-budget options instead.</i>"
        )
    elif max_price is not None:
        cap_note = f"\n<i>Filtered to ≤ ${max_price:g} USD.</i>"
    else:
        cap_note = ""

    # Header reflects the running search context. Multi-turn → show history.
    if len(history) > 1:
        header = (
            f"🔍 <b>Searching:</b> "
            + " <b>+</b> ".join(f"<i>{h(q)}</i>" for q in history)
            + cap_note
        )
    else:
        header = f"You typed: <b>{h(text)}</b>{cap_note}"

    if not top:
        msg = (
            f"{header}\n\nNo close matches in the catalog. "
            "You can keep your text as-is, refine it, or start over."
        )
        buttons = [
            [("📝 Use my latest text as-is", "ssn:raw")],
            [("🔄 Start search over", "ssn:reset")],
            nav_row(include_back=False),
        ]
    else:
        lines = [header, "\nClosest matches:"]
        buttons = []
        for i, s in enumerate(top):
            cat = s.get("category") or ""
            cat_str = f" · <i>{h(cat)}</i>" if cat else ""
            price = s.get("price") or "—"
            code = s.get("code") or "—"
            lines.append(
                f"<b>{i+1}. {h(s['name'])}</b>{cat_str}\n"
                f"    code {h(code)} · {h(price)}"
            )
            label = f"{i+1}. {s['name']}"
            if len(label) > 40:
                label = label[:38] + "…"
            buttons.append([(label, f"ssn:{i}")])
        if len(history) > 1:
            lines.append("\n<i>💡 Type more to refine further, or tap Start over to reset.</i>")
        else:
            lines.append("\n<i>💡 Type more to refine, or pick a match above.</i>")
        buttons.append([("📝 Use my latest text as-is", "ssn:raw")])
        buttons.append([("🔄 Start search over", "ssn:reset")])
        buttons.append(nav_row(include_back=False))
        msg = "\n".join(lines)
    await _drop_loader()
    await send(update, msg, kb(buttons))


async def _handle_company_text(update, ctx, d: state.Draft, text: str):
    # Sub-state after user confirmed "new customer" — they now type the
    # correct full name, we store it and move on.
    if d.sub == "new_name":
        d.data["company_name"] = text
        d.sub = ""
        await send(update, f"Saved as new customer: <b>{h(text)}</b>.")
        await _advance(update, ctx, d)
        return

    if d.sub == "confirm_address":
        _mark_stuck_reminder(d.user_id)
        await send(update, "👆 Tap ✅ Yes or ❌ No above.")
        return

    try:
        await update.effective_chat.send_action("typing")
    except Exception:  # noqa: BLE001
        pass
    try:
        master = await asyncio.to_thread(sheets.load_merged_customers)
    except Exception as e:  # noqa: BLE001
        log.exception("load_merged_customers failed: %s", e)
        master = []

    top = matcher.top_customer_master(text, master, limit=5)
    ctx.user_data["company_candidates"] = top
    ctx.user_data["company_query"] = text

    if not top:
        d.sub = "new_name"
        await send(
            update,
            f"I couldn't find <b>{h(text)}</b> in the customer master list.\n\n"
            "Type the <b>correct full customer name</b> to continue "
            "(I'll treat this as a new customer).",
            kb([nav_row()]),
        )
        return

    lines = [f"You typed: <b>{h(text)}</b>\n\nClosest matches from the customer master — tap one:"]
    buttons = []
    for i, c in enumerate(top):
        code = c.get("code", "")
        code_str = f" · <code>{h(code)}</code>" if code else ""
        lines.append(f"<b>{i+1}. {h(c['name'])}</b>{code_str}")
        label = f"{i+1}. {c['name']}"
        if len(label) > 40:
            label = label[:38] + "…"
        buttons.append([(label, f"co:{i}")])
    buttons.append([("➕ New customer — not in the list", "co:new")])
    buttons.append(nav_row())
    await send(update, "\n".join(lines), kb(buttons))


# --------------------------- callback handler ---------------------------

async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await _authorized(update):
        return
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    user_id = update.effective_user.id
    chat = update.effective_chat

    # NOTE: a kb_owner ownership check used to live here to refuse cross-user
    # button taps in groups. It was firing false positives — legitimate
    # owners getting rejected on their own buttons — so it's disabled.
    # State isolation per-user_id (state._drafts) is enough to prevent real
    # corruption; a mistaken click only causes visual confusion, which is
    # acceptable. The kb_owners dict stays populated by send() in case we
    # want to re-enable a smarter version later.

    # "Add another seasoning" fires right after submit, when the draft is gone.
    # Handle before the no-draft guard.
    if data.startswith("again:"):
        await _handle_again(update, ctx, data.split(":", 1)[1])
        return

    # Main menu and /samples browsing work with or without a draft.
    if data.startswith("menu:"):
        await _handle_menu_callback(update, ctx, data.split(":", 1)[1])
        return
    if data.startswith("samp:"):
        await _handle_samples_callback(update, ctx, data.split(":", 1)[1])
        return

    # Bulk-paste session controls (cancel/retry/finish/list). These work with
    # or without an open draft.
    if data.startswith("bulk:"):
        await _handle_bulk_callback(update, ctx, data.split(":", 1)[1])
        return
    # Bulk-paste shared-value picks (taste/base/courier/currency).
    if data.startswith("bsh:"):
        await _handle_bulk_shared_callback(update, ctx, data.split(":", 1)[1])
        return
    # Tap on an item in the bulk list → open it as a Draft for review.
    if data.startswith("bitem:"):
        try:
            idx = int(data.split(":", 1)[1])
        except ValueError:
            return
        await _open_bulk_item(update, ctx, idx)
        return
    # V1.0.1 — bulk cross-fill Yes/No reply after an edit fills an empty field.
    if data.startswith("bxf:"):
        await _handle_bulk_crossfill_callback(update, ctx, data.split(":", 1)[1])
        return

    d = state.get(user_id)
    if not d:
        # In a group chat, this branch fires when user B taps user A's
        # buttons. We must NOT edit the original message (would clobber A's
        # view) — send a fresh nudge instead so A's draft stays visible.
        chat = update.effective_chat
        # NOTE: q.answer() was already called at the top, so we can't show a
        # popup. A new message is the cleanest fallback.
        if state.consume_expired_flag(user_id):
            await chat.send_message(
                f"⏰ Your draft expired after {config.DRAFT_TIMEOUT_MINUTES} min of no input. "
                "Type /start to begin a new one.\n\n<i>{footer}</i>".format(
                    footer=h(config.BOT_VERSION)
                ),
                parse_mode=ParseMode.HTML,
            )
        else:
            await chat.send_message(
                "🤔 No active draft — your buttons may belong to someone else, "
                "or the bot just redeployed. Type /start to begin a new one.\n\n"
                f"<i>{h(config.BOT_VERSION)}</i>",
                parse_mode=ParseMode.HTML,
            )
        return
    d.touch()

    if data.startswith("nav:"):
        await _handle_nav(update, ctx, d, data.split(":", 1)[1])
        return

    if data.startswith("ssn:"):
        await _handle_seasoning_pick(update, ctx, d, data.split(":", 1)[1])
        return

    if data.startswith("co:"):
        await _handle_company_pick(update, ctx, d, data.split(":", 1)[1])
        return

    if data.startswith("ca:"):
        ans = data.split(":", 1)[1]
        if ans == "yes":
            addr = ctx.user_data.pop("linked_address", "")
            if addr:
                d.data["address"] = addr
                d.data["_address_linked"] = "1"
        else:
            ctx.user_data.pop("linked_address", None)
            d.data.pop("_address_linked", None)
        d.sub = ""
        await _advance(update, ctx, d)
        return

    if data.startswith("cur:"):
        d.data["_currency"] = data.split(":", 1)[1]
        d.sub = "amount"
        await ask(update, ctx, d)
        return

    if data.startswith("app:"):
        d.data["app_method"] = data.split(":", 1)[1]
        await _advance(update, ctx, d)
        return

    if data.startswith("yn:"):
        d.data["taste_check"] = "Yes" if data.split(":", 1)[1] == "Y" else "No"
        await _advance(update, ctx, d)
        return

    if data.startswith("cou:"):
        d.data["courier"] = data.split(":", 1)[1]
        await _advance(update, ctx, d)
        return

    if data.startswith("qm:"):
        payload = data.split(":", 1)[1]
        if payload == "manual":
            d.sub = "main_manual"
            await ask(update, ctx, d)
            return
        # payload is "w:100g" (weight) or "b:2" (bottles)
        kind, _, val = payload.partition(":")
        if kind == "w":
            d.data["_qty_main_weight"] = val
            d.sub = "main_sets"
        elif kind == "b":
            d.data["_qty_main_label"] = f"{val} bottle{'s' if val != '1' else ''}"
            d.sub = "need_app"
        else:
            return
        await ask(update, ctx, d)
        return

    if data.startswith("qs:"):
        payload = data.split(":", 1)[1]
        if payload == "manual":
            d.sub = "main_sets_manual"
            await ask(update, ctx, d)
            return
        d.data["_qty_main_sets"] = payload
        _combine_main_label(d)
        d.sub = "need_app"
        await ask(update, ctx, d)
        return

    if data.startswith("qas:"):
        payload = data.split(":", 1)[1]
        if payload == "manual":
            d.sub = "app_sets_manual"
            await ask(update, ctx, d)
            return
        d.data["_qty_app_sets"] = payload
        _combine_app_label(d)
        d.sub = "app_base"
        await ask(update, ctx, d)
        return

    if data.startswith("qa:"):
        ans = data.split(":", 1)[1]
        if ans == "N":
            _finalize_quantity(d)
            await _advance(update, ctx, d)
            return
        d.sub = "app_amount"
        await ask(update, ctx, d)
        return

    if data.startswith("qb:"):
        payload = data.split(":", 1)[1]
        if payload == "manual":
            d.sub = "app_base_manual"
            await ask(update, ctx, d)
            return
        try:
            idx = int(payload)
        except ValueError:
            return
        if 0 <= idx < len(APP_BASES):
            d.data["_qty_base"] = APP_BASES[idx]
            _finalize_quantity(d)
            await _advance(update, ctx, d)
        return

    if data.startswith("cb:"):
        payload = data.split(":", 1)[1]
        if payload == "manual":
            d.sub = "manual"
            await ask(update, ctx, d)
            return
        try:
            idx = int(payload)
        except ValueError:
            return
        if 0 <= idx < len(CUSTOMER_BASES):
            d.data["customer_base"] = CUSTOMER_BASES[idx]
            d.sub = ""
            await _advance(update, ctx, d)
        return

    if data.startswith("rev:"):
        await _handle_review(update, ctx, d, data.split(":", 1)[1])
        return

    if data.startswith("edit:"):
        key = data.split(":", 1)[1]
        d.stage = key
        d.return_to_review = True
        d.sub = ""
        await ask(update, ctx, d)
        return


async def _handle_menu_callback(update, ctx, action: str):
    """Top-level /start menu — pick what the user wants to do."""
    user = update.effective_user
    if action == "home":
        await cmd_start(update, ctx)
        return
    if action == "new":
        state.clear(user.id)
        # Clean up any leftover per-user search context from a previous draft.
        for k in ("seasoning_queries", "seasoning_candidates", "seasoning_query"):
            ctx.user_data.pop(k, None)
        d = state.start(user.id, user.username or user.first_name or "")
        await send(update, "Let's begin! 🌶")
        await ask(update, ctx, d)
        return
    if action == "samples":
        await show_samples_menu(update, ctx)
        return
    if action == "bulk":
        await _start_bulk(update, ctx)
        return
    if action == "scan":
        ctx.user_data["awaiting_scan_photo"] = True
        await send(
            update,
            "📷 <b>Scan a product photo</b>\n\n"
            "Send a photo of one or more product code labels "
            "(<code>S-XXXXX-XX</code>). I'll read them and pull the price for each.\n\n"
            "<i>Tip:</i> tap 📎 → Camera for the sharpest shot.",
        )
        return
    if action == "code":
        ctx.user_data["awaiting_code_text"] = True
        await send(
            update,
            "✏️ <b>Enter a product code</b>\n\n"
            "Type one or more product codes and I'll pull the price for each. "
            "You can paste a base code like <code>S-668U1</code> and I'll list "
            "all its variants — or paste up to 5 full codes separated by spaces.",
        )
        return
    if action == "updsample":
        # Ragonic-only guard — defense in depth; button is already hidden
        # for everyone else at cmd_start.
        if not _is_update_sample_owner(user):
            await send(update, "🔒 This command is restricted.")
            return
        await _run_update_sample_list(update, ctx)
        return


async def _handle_again(update, ctx, action: str):
    """After a submit, let the user raise another request for the same customer."""
    user = update.effective_user
    if action == "fresh":
        ctx.user_data.pop("last_submission", None)
        state.clear(user.id)
        await send(update, "Send /start to begin a new request.")
        return
    if action == "samples":
        await show_samples_menu(update, ctx)
        return
    if action != "same":
        return
    carry = ctx.user_data.get("last_submission") or {}
    if not carry:
        await send(update, "Nothing to carry over. Send /start to begin a new request.")
        return
    d = state.start(user.id, user.username or user.first_name or "")
    # Copy every shared field, then clear the seasoning-specific ones so the
    # user is asked only what actually changes.
    d.data = dict(carry)
    for k in ("seasoning", "comment", "_currency"):
        d.data.pop(k, None)
    d.matched_code = ""
    d.matched_price = ""
    d.stage = "seasoning"
    # After seasoning pick we jump straight to review; they can tweak any
    # carried-over field from there.
    d.return_to_review = True
    await send(
        update,
        "🔁 Carrying over company + shipping details. "
        "Type the next seasoning — I'll take you straight to review after you pick.",
    )
    await ask(update, ctx, d)


async def _handle_nav(update, ctx, d: state.Draft, action: str):
    if action == "cancel":
        # Confirm before discarding — single tap kills 16 fields of work otherwise.
        await send(
            update,
            "⚠️ <b>Cancel this draft?</b>\n\n"
            "All entered fields will be discarded. This cannot be undone.",
            kb([
                [("🗑 Yes, discard", "nav:cancel_yes")],
                [("◀ Keep editing", "nav:cancel_no")],
            ]),
        )
        return
    if action == "cancel_yes":
        state.clear(d.user_id)
        await send(
            update,
            "✖ Draft discarded.",
            kb([[("🏠 Main menu", "menu:home")]]),
        )
        return
    if action == "cancel_no":
        # Bounce back to whatever question we were on.
        await ask(update, ctx, d)
        return
    if action == "skip":
        d.data[d.stage] = ""
        await _advance(update, ctx, d)
        return
    if action == "back":
        if d.stage == "price_budget" and d.sub == "amount":
            d.sub = "currency"
            await ask(update, ctx, d)
            return
        if d.stage == "customer_base" and d.sub == "manual":
            d.sub = ""
            await ask(update, ctx, d)
            return
        if d.stage == "quantity" and d.sub in _QTY_SUBS:
            hops = {
                "main_manual": "main",
                "main_sets": "main",
                "main_sets_manual": "main_sets",
                # need_app sits after sets for seasoning, after main for oil
                "need_app": "main" if _is_oil_product(d) else "main_sets",
                "app_amount": "need_app",
                "app_sets": "app_amount",
                "app_sets_manual": "app_sets",
                "app_base": "app_sets",
                "app_base_manual": "app_base",
            }
            if d.sub in hops:
                d.sub = hops[d.sub]
                await ask(update, ctx, d)
                return
            # d.sub == "main" — fall through to prev stage
        new_stage = prev_stage(d.stage)
        # Skip stages going backwards too when auto-linked.
        while True:
            if new_stage == "address" and d.data.get("_address_linked") == "1":
                new_stage = prev_stage("address")
                continue
            if new_stage in ("receiver_number", "receiving_person") and d.data.get("_contact_linked") == "1":
                new_stage = prev_stage(new_stage)
                continue
            break
        d.stage = new_stage
        d.sub = ""
        await ask(update, ctx, d)


async def _handle_seasoning_pick(update, ctx, d: state.Draft, payload: str):
    if payload == "retry":
        # User rejected the code match — prompt them to type a name instead.
        ctx.user_data.pop("seasoning_candidates", None)
        ctx.user_data.pop("seasoning_queries", None)
        await send(
            update,
            "OK — type the <b>product name</b> (or a hint like "
            "<i>cheese below 4.5 usd</i>) and I'll search the catalog.",
            kb([nav_row(include_back=False)]),
        )
        return
    if payload == "reset":
        # Clear the running query history and re-prompt the seasoning question.
        ctx.user_data.pop("seasoning_candidates", None)
        ctx.user_data.pop("seasoning_queries", None)
        ctx.user_data.pop("seasoning_query", None)
        await send(
            update,
            "🔄 Search reset. Type what you're looking for and I'll start over.",
            kb([nav_row(include_back=False)]),
        )
        return
    if payload == "raw":
        d.data["seasoning"] = ctx.user_data.get("seasoning_query", "")
        d.matched_code = ""
        d.matched_price = ""
        d.matched_category = ""
    else:
        try:
            idx = int(payload)
        except ValueError:
            return
        cands = ctx.user_data.get("seasoning_candidates") or []
        if 0 <= idx < len(cands):
            c = cands[idx]
            d.data["seasoning"] = c["name"]
            d.matched_code = c.get("code", "")
            d.matched_price = c.get("price", "")
            d.matched_category = c.get("category", "")
            # Prefill the comment with the picked product + code so R&D sees
            # exactly what sales chose. User can still edit later.
            if c.get("code"):
                d.data["comment"] = f"Use code {c['code']} — {c['name']}"
            else:
                d.data["comment"] = f"Use {c['name']}"
    # Search resolved — drop the running query history so the next draft
    # (or next edit pass) starts clean.
    ctx.user_data.pop("seasoning_queries", None)
    await _advance(update, ctx, d)


async def _handle_company_pick(update, ctx, d: state.Draft, payload: str):
    if payload == "new":
        d.sub = "new_name"
        await send(
            update,
            "Got it — new customer. What's the <b>correct full customer name</b>?",
            kb([nav_row()]),
        )
        return
    try:
        idx = int(payload)
    except ValueError:
        return
    cands = ctx.user_data.get("company_candidates") or []
    if not (0 <= idx < len(cands)):
        return
    c = cands[idx]
    d.data["company_name"] = c.get("name", "")
    # Reset any previously linked address/contact when switching customer.
    d.data.pop("_address_linked", None)
    d.data.pop("_contact_linked", None)

    # Contact details come from the merged entry (master + OPS overlay).
    # If both receiver fields are known, skip 13 and 15.
    recv_num = (c.get("receiver_number") or "").strip()
    recv_person = (c.get("receiving_person") or "").strip()
    if recv_num and recv_person:
        d.data["receiver_number"] = recv_num
        d.data["receiving_person"] = recv_person
        d.data["_contact_linked"] = "1"

    master_addr = (c.get("address") or "").strip()
    if master_addr:
        ctx.user_data["linked_address"] = master_addr
        d.sub = "confirm_address"
        await send(
            update,
            f"Linked address for <b>{h(d.data['company_name'])}</b>:\n\n"
            f"📍 <i>{h(master_addr)}</i>\n\n"
            "Use this address for shipping?",
            kb([
                [("✅ Yes, link this address", "ca:yes")],
                [("❌ No, I'll enter a different one", "ca:no")],
                nav_row(),
            ]),
        )
        return

    # No address on the master row — proceed to 13/15 to collect contacts.
    await _advance(update, ctx, d)


async def _handle_review(update, ctx, d: state.Draft, action: str):
    if action == "edit":
        # Show a picker with one button per field.
        buttons = []
        for key, label in FIELDS:
            buttons.append([(f"✏️ {label}", f"edit:{key}")])
        buttons.append([("◀ Back to draft", "rev:back")])
        await send(update, "<b>Which field do you want to edit?</b>", kb(buttons))
        return
    if action == "back":
        await q_review(update, ctx, d)
        return
    if action == "confirm":
        await _submit(update, ctx, d)


# --------------------------- advance & submit ---------------------------

async def _advance(update, ctx, d: state.Draft):
    # If we were editing from review, save new customer details if relevant and return.
    if d.return_to_review:
        # V1.0.1: bulk cross-fill. If this is a bulk-session edit that just
        # filled a previously-empty eligible field, and other pending bulk
        # items also have that field empty, offer to apply the new value to
        # those items before returning to review.
        bulk_idx_raw = d.data.get("_bulk_idx", "")
        just_edited = d.stage
        new_val = str(d.data.get(just_edited, "")).strip()
        if bulk_idx_raw != "" and new_val and just_edited in _BULK_CROSSFILL_FIELDS:
            try:
                cur_idx = int(bulk_idx_raw)
            except ValueError:
                cur_idx = -1
            if cur_idx >= 0:
                targets = _bulk_crossfill_targets(ctx, cur_idx, just_edited)
                if targets:
                    ctx.user_data["bulk_crossfill"] = {
                        "field": just_edited,
                        "value": new_val,
                        "targets": targets,
                        "current_idx": cur_idx,
                    }
                    # Update the current item's parsed value too so the state
                    # stays consistent if user re-opens.
                    parsed = ctx.user_data.get("bulk_parsed") or {}
                    items = parsed.get("items") or []
                    if 0 <= cur_idx < len(items):
                        items[cur_idx][just_edited] = new_val
                    await _show_bulk_crossfill_prompt(update, ctx)
                    return

        d.return_to_review = False
        d.stage = "review"
        d.sub = ""
        await ask(update, ctx, d)
        return

    d.stage = next_stage(d.stage)
    # Auto-skip stages whose values are already linked from master / OPS cache.
    while True:
        if d.stage == "address" and d.data.get("_address_linked") == "1":
            d.stage = next_stage("address")
            continue
        if d.stage in ("receiver_number", "receiving_person") and d.data.get("_contact_linked") == "1":
            d.stage = next_stage(d.stage)
            continue
        break
    d.sub = ""
    await ask(update, ctx, d)


async def _submit(update, ctx, d: state.Draft):
    user = update.effective_user
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    row = {
        "Timestamp": now,
        "Sales Person (Telegram)": f"@{user.username}" if user.username else user.first_name or "",
        "Telegram User ID": str(user.id),
        "Seasoning Requested": d.data.get("seasoning", ""),
        "Matched Code": d.matched_code,
        "Matched Price": d.matched_price,
        "Comment": _effective_comment(d),
        "Quantity": d.data.get("quantity", ""),
        "Selling Price Budget": d.data.get("price_budget", ""),
        "Application Method": d.data.get("app_method", ""),
        "Dosage": d.data.get("dosage", ""),
        "Requirement": d.data.get("requirement", ""),
        "Market": d.data.get("market", ""),
        "Deadline": d.data.get("deadline", ""),
        "Need to Check Taste": d.data.get("taste_check", ""),
        "Customer Base": d.data.get("customer_base", ""),
        "Preferred Courier": d.data.get("courier", ""),
        "Customer Company Name": d.data.get("company_name", ""),
        "Receiver Number": d.data.get("receiver_number", ""),
        "Address": d.data.get("address", ""),
        "Receiving Person": d.data.get("receiving_person", ""),
    }
    try:
        await update.effective_chat.send_action("typing")
    except Exception:  # noqa: BLE001
        pass
    try:
        await asyncio.to_thread(sheets.append_sample_request, row)
    except Exception as e:  # noqa: BLE001
        log.exception("append_sample_request failed: %s", e)
        await send(update, f"❌ Failed to save to Google Sheet: {e}\n\nTry again or contact admin.")
        return

    # Upsert customer record for future autofill.
    try:
        await asyncio.to_thread(
            sheets.upsert_customer,
            {
                "Company Name": d.data.get("company_name", ""),
                "Address": d.data.get("address", ""),
                "Receiver Number": d.data.get("receiver_number", ""),
                "Receiving Person": d.data.get("receiving_person", ""),
                "Preferred Courier": d.data.get("courier", ""),
            },
        )
    except Exception as e:  # noqa: BLE001
        log.warning("upsert_customer failed: %s", e)

    # Bulk-session submit: mark the item done and return to the bulk list
    # instead of the normal post-submit screen.
    bulk_idx_raw = d.data.get("_bulk_idx", "")
    if bulk_idx_raw != "":
        try:
            bi = int(bulk_idx_raw)
        except ValueError:
            bi = -1
        parsed = ctx.user_data.get("bulk_parsed") or {}
        items = parsed.get("items") or []
        if 0 <= bi < len(items):
            items[bi]["_done"] = True
        # Stash the 4 customer fields so subsequent bulk items auto-carry them
        # and can jump straight to review (the user already confirmed them).
        if "bulk_customer_carry" not in ctx.user_data:
            ctx.user_data["bulk_customer_carry"] = {
                "company_name": d.data.get("company_name", ""),
                "address": d.data.get("address", ""),
                "receiver_number": d.data.get("receiver_number", ""),
                "receiving_person": d.data.get("receiving_person", ""),
                "_address_linked": d.data.get("_address_linked", ""),
                "_contact_linked": d.data.get("_contact_linked", ""),
            }
        state.clear(user.id)
        await send(update, f"✅ Item {bi + 1} saved.")
        await _show_bulk_list(update, ctx)
        return

    # Stash the submitted draft data so user can add another seasoning for
    # the same customer without re-keying. Cleared by start/cancel.
    ctx.user_data["last_submission"] = dict(d.data)
    state.clear(user.id)
    company = d.data.get("company_name", "")
    company_line = f"\nCustomer: <b>{h(company)}</b>" if company else ""
    buttons = [
        [("➕ Same customer — add another seasoning", "again:same")],
        [("🆕 Start a fresh request", "again:fresh")],
        [("📋 My sample requests", "again:samples")],
    ]
    await send(
        update,
        f"✅ <b>Saved.</b>{company_line}\n\nWhat next?",
        kb(buttons),
    )


# --------------------------- samples view (V0.3.0) ---------------------------

async def show_samples_menu(update, ctx):
    await send(
        update,
        "📋 <b>My sample requests</b>\n\nPick a period:",
        kb([
            [("🗓 Today", "samp:today")],
            [("📆 This month", "samp:month")],
            [("✖ Close", "samp:close")],
        ]),
    )


async def _load_my_samples(update) -> list[dict[str, Any]] | None:
    user = update.effective_user
    try:
        await update.effective_chat.send_action("typing")
    except Exception:  # noqa: BLE001
        pass
    try:
        rows = await asyncio.to_thread(sheets.load_sample_log)
    except Exception as e:  # noqa: BLE001
        log.exception("load_sample_log failed: %s", e)
        await send(
            update,
            "⚠️ Couldn't read the sales log. Try again in a moment.",
            kb([[("◀ Back", "samp:menu"), ("✖ Close", "samp:close")]]),
        )
        return None
    return _mine_only(rows, user.id)


async def show_today(update, ctx, page: int = 0):
    mine = await _load_my_samples(update)
    if mine is None:
        return
    today = _sort_by_ts_desc(_filter_today_sgt(mine))
    ctx.user_data["samp_today_rows"] = today
    today_label = _sgt_now().strftime("%a, %b %d")
    if not today:
        await send(
            update,
            f"🗓 <b>Today — {h(today_label)}</b>\n\n<i>No samples raised today.</i>",
            kb([[("◀ Back", "samp:menu"), ("✖ Close", "samp:close")]]),
        )
        return

    # One sample per page in full draft-summary format.
    page_items, page, total = _page_slice(today, page, size=1)
    r = page_items[0]
    sgt = _log_ts_to_sgt(r.get("Timestamp", ""))
    ts = sgt.strftime("%H:%M SGT") if sgt else "—"
    header = (
        f"🗓 <b>Today — {h(today_label)}</b>\n"
        f"<b>Sample {page + 1} of {total}</b> · {h(ts)}\n\n"
        "📝 <b>Draft summary</b>\n"
    )
    rows_btns = [_page_nav_row(page, total, "samp:today")]
    rows_btns.append([("◀ Back", "samp:menu"), ("✖ Close", "samp:close")])
    await send(update, header + "\n" + _fmt_sample_summary(r), kb(rows_btns))


async def show_month_customers(update, ctx, page: int = 0):
    mine = await _load_my_samples(update)
    if mine is None:
        return
    month = _filter_month_sgt(mine)
    grouped = _group_by_customer(month)
    ctx.user_data["samp_month_customers"] = grouped
    ctx.user_data.pop("samp_current_cust_idx", None)

    month_label = _sgt_now().strftime("%B %Y")
    if not grouped:
        await send(
            update,
            f"📆 <b>{h(month_label)}</b>\n\n<i>No samples raised this month.</i>",
            kb([[("◀ Back", "samp:menu"), ("✖ Close", "samp:close")]]),
        )
        return

    page_items, page, total = _page_slice(grouped, page)
    start = page * SAMPLES_PAGE_SIZE
    total_samples = sum(len(g) for _, g in grouped)
    lines = [
        f"📆 <b>{h(month_label)}</b> · {total_samples} sample{'s' if total_samples != 1 else ''} across {len(grouped)} customer{'s' if len(grouped) != 1 else ''}\n",
        "Tap a customer to see their samples:",
    ]
    buttons: list[list[tuple[str, str]]] = []
    for i, (name, samples) in enumerate(page_items):
        absolute_idx = start + i
        line_n = absolute_idx + 1
        lines.append(f"<b>{line_n}.</b> {h(name)} — {len(samples)} sample{'s' if len(samples) != 1 else ''}")
        label = f"{line_n}. {name}"
        if len(label) > 40:
            label = label[:38] + "…"
        buttons.append([(label, f"samp:cust:{absolute_idx}")])
    buttons.append(_page_nav_row(page, total, "samp:month"))
    buttons.append([("◀ Back", "samp:menu"), ("✖ Close", "samp:close")])
    await send(update, "\n".join(lines), kb(buttons))


async def show_customer_samples(update, ctx, cust_idx: int, page: int = 0):
    grouped = ctx.user_data.get("samp_month_customers") or []
    if not (0 <= cust_idx < len(grouped)):
        # Stale index — refresh the customer list.
        await show_month_customers(update, ctx)
        return
    name, samples = grouped[cust_idx]
    samples = _sort_by_ts_desc(samples)
    ctx.user_data["samp_current_cust_idx"] = cust_idx

    # One sample per page in full draft-summary format.
    page_items, page, total = _page_slice(samples, page, size=1)
    r = page_items[0]
    sgt = _log_ts_to_sgt(r.get("Timestamp", ""))
    ts = sgt.strftime("%b %d · %H:%M SGT") if sgt else "—"
    month_label = _sgt_now().strftime("%B %Y")
    header = (
        f"🏢 <b>{h(name)}</b> · {h(month_label)}\n"
        f"<b>Sample {page + 1} of {total}</b> · {h(ts)}\n\n"
        "📝 <b>Draft summary</b>\n"
    )
    rows_btns = [_page_nav_row(page, total, "samp:custpage")]
    rows_btns.append([("◀ Back to customers", "samp:month"), ("✖ Close", "samp:close")])
    await send(update, header + "\n" + _fmt_sample_summary(r), kb(rows_btns))


async def _handle_samples_callback(update, ctx, action: str):
    if action == "noop":
        return
    if action == "close":
        q = update.callback_query
        try:
            await q.edit_message_text(
                "Closed.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🏠 Main menu", callback_data="menu:home")]]
                ),
            )
        except Exception:  # noqa: BLE001
            pass
        return
    if action == "menu":
        await show_samples_menu(update, ctx)
        return
    if action == "today":
        await show_today(update, ctx, page=0)
        return
    if action == "month":
        await show_month_customers(update, ctx, page=0)
        return
    if action.startswith("today:page:"):
        try:
            p = int(action.split(":")[-1])
        except ValueError:
            return
        await show_today(update, ctx, page=p)
        return
    if action.startswith("month:page:"):
        try:
            p = int(action.split(":")[-1])
        except ValueError:
            return
        await show_month_customers(update, ctx, page=p)
        return
    if action.startswith("cust:"):
        try:
            idx = int(action.split(":", 1)[1])
        except ValueError:
            return
        await show_customer_samples(update, ctx, idx, page=0)
        return
    if action.startswith("custpage:page:"):
        try:
            p = int(action.split(":")[-1])
        except ValueError:
            return
        idx = ctx.user_data.get("samp_current_cust_idx", 0)
        await show_customer_samples(update, ctx, idx, page=p)
        return


# --------------------------- bulk paste (V0.4.0) ---------------------------
#
# State machine in ctx.user_data:
#   bulk_stage  — one of:
#       "await_paste", "ask_taste", "ask_base", "ask_courier",
#       "ask_budget_cur", "ask_budget_amt", "parsing", "list", "review"
#   bulk_raw    — the pasted text
#   bulk_shared — {'taste_check', 'customer_base', 'courier',
#                  'price_budget', '_currency'}
#   bulk_parsed — dict from ai.parse_bulk_sample_request (customer/market/
#                  deadline/items) plus per-item markers {_done, _matched_*}
#   bulk_current_item — idx of item currently being reviewed as a Draft


async def _start_bulk(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    state.clear(user.id)
    # Reset any prior bulk state
    for k in ("bulk_stage", "bulk_raw", "bulk_shared", "bulk_parsed",
              "bulk_current_item", "bulk_customer_carry",
              "bulk_tokens_in", "bulk_tokens_out"):
        ctx.user_data.pop(k, None)
    ctx.user_data["bulk_stage"] = "await_paste"
    ctx.user_data["bulk_shared"] = {}
    await send(
        update,
        "📄 <b>Bulk paste — multi-seasoning request</b>\n\n"
        "Paste the full email or message from your customer. I'll split it "
        "into one sample request per seasoning, pre-fill everything I can, "
        "and let you review each one before submitting.\n\n"
        "Works best when the text includes:\n"
        "• customer name + shipping address + receiver name\n"
        "• each seasoning with its code (S-XXXX) and quantity\n"
        "• deadline / market / application notes (if any)\n\n"
        "Go ahead — paste it now.",
        kb([[("✖ Cancel", "bulk:cancel")]]),
    )


def _bulk_shared_summary(shared: dict[str, str]) -> str:
    rows = [
        ("Need to Check Taste", shared.get("taste_check", "")),
        ("Customer Base", shared.get("customer_base", "")),
        ("Preferred Courier", shared.get("courier", "")),
        ("Selling Price Budget", shared.get("price_budget", "")),
    ]
    lines = []
    for label, val in rows:
        val_str = h(val) if val else "<i>(pending)</i>"
        lines.append(f"• <b>{label}:</b> {val_str}")
    return "\n".join(lines)


async def _ask_bulk_taste(update, ctx):
    ctx.user_data["bulk_stage"] = "ask_taste"
    shared = ctx.user_data.get("bulk_shared", {})
    await send(
        update,
        "🤝 <b>Shared values — apply to ALL items</b>\n\n"
        "I'll ask once and use the same answer for every seasoning in your paste.\n\n"
        f"{_bulk_shared_summary(shared)}\n\n"
        "👅 <b>Need to Check Taste?</b>",
        kb([
            [("✅ Yes", "bsh:taste:Y"), ("❌ No", "bsh:taste:N")],
            [("✖ Cancel", "bulk:cancel")],
        ]),
    )


async def _ask_bulk_base(update, ctx):
    ctx.user_data["bulk_stage"] = "ask_base"
    shared = ctx.user_data.get("bulk_shared", {})
    buttons: list[list[tuple[str, str]]] = []
    row: list[tuple[str, str]] = []
    for i, b in enumerate(CUSTOMER_BASES):
        row.append((b, f"bsh:base:{i}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([("⌨️ Type it manually", "bsh:base:manual")])
    buttons.append([("✖ Cancel", "bulk:cancel")])
    await send(
        update,
        "🍿 <b>Customer Base (shared)</b>\n\n"
        f"{_bulk_shared_summary(shared)}\n\n"
        "Pick one to apply to every item, or tap Enter manually:",
        kb(buttons),
    )


async def _ask_bulk_base_manual(update, ctx):
    ctx.user_data["bulk_stage"] = "ask_base_manual"
    shared = ctx.user_data.get("bulk_shared", {})
    await send(
        update,
        "⌨️ <b>Customer Base (shared)</b>\n\n"
        f"{_bulk_shared_summary(shared)}\n\n"
        "Type the customer base to apply to every item:",
        kb([[("✖ Cancel", "bulk:cancel")]]),
    )


async def _ask_bulk_courier(update, ctx):
    ctx.user_data["bulk_stage"] = "ask_courier"
    shared = ctx.user_data.get("bulk_shared", {})
    buttons = [[(c, f"bsh:cou:{c}")] for c in COURIERS]
    buttons.append([("✖ Cancel", "bulk:cancel")])
    await send(
        update,
        "🚚 <b>Preferred Courier (shared)</b>\n\n"
        f"{_bulk_shared_summary(shared)}\n\n"
        "Pick one to apply to every item:",
        kb(buttons),
    )


async def _ask_bulk_budget_currency(update, ctx):
    ctx.user_data["bulk_stage"] = "ask_budget_cur"
    shared = ctx.user_data.get("bulk_shared", {})
    await send(
        update,
        "💰 <b>Selling Price Budget (shared) — Currency</b>\n\n"
        f"{_bulk_shared_summary(shared)}\n\n"
        "Pick a currency:",
        kb([
            [("USD", "bsh:cur:USD"), ("SGD", "bsh:cur:SGD")],
            [("✖ Cancel", "bulk:cancel")],
        ]),
    )


async def _ask_bulk_budget_amount(update, ctx):
    ctx.user_data["bulk_stage"] = "ask_budget_amt"
    shared = ctx.user_data.get("bulk_shared", {})
    cur = shared.get("_currency", "USD")
    await send(
        update,
        f"💰 <b>Selling Price Budget — Amount ({cur})</b>\n\n"
        f"{_bulk_shared_summary(shared)}\n\n"
        "Type the max budget. Example: <i>3.00</i>",
        kb([[("✖ Cancel", "bulk:cancel")]]),
    )


def _match_seasoning_by_code(code: str, catalog: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Look up a seasoning by code, trying the full code first, then shorter
    suffix-trimmed variants (e.g. S-6AUH2-12-Y1 → S-6AUH2-12 → S-6AUH2)."""
    code = (code or "").strip()
    if not code:
        return None
    by_code = {str(s.get("code", "")).strip(): s for s in catalog if s.get("code")}
    # Exact
    if code in by_code:
        return by_code[code]
    # Progressively trim trailing "-XXX" segments.
    parts = code.split("-")
    for n in range(len(parts) - 1, 0, -1):
        candidate = "-".join(parts[:n])
        if candidate in by_code:
            return by_code[candidate]
    return None


async def _run_bulk_parse(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Call Claude to parse the paste, then show the item list."""
    user = update.effective_user
    ctx.user_data["bulk_stage"] = "parsing"
    try:
        await update.effective_chat.send_action("typing")
    except Exception:  # noqa: BLE001
        pass
    await send(
        update,
        "🧠 <b>Reading your paste…</b>\n\n"
        "This usually takes a few seconds. Please wait.",
    )

    raw = ctx.user_data.get("bulk_raw", "")
    shared = ctx.user_data.get("bulk_shared", {})

    try:
        catalog = await asyncio.to_thread(sheets.load_seasonings)
    except Exception as e:  # noqa: BLE001
        log.warning("load_seasonings failed during bulk: %s", e)
        catalog = []
    try:
        customers = await asyncio.to_thread(sheets.load_merged_customers)
    except Exception as e:  # noqa: BLE001
        log.warning("load_merged_customers failed during bulk: %s", e)
        customers = []

    seasoning_codes = [str(s.get("code", "")) for s in catalog if s.get("code")]
    customer_names = [c.get("name", "") for c in customers if c.get("name")]

    try:
        result, tin, tout = await ai.parse_bulk_sample_request(
            raw, shared, seasoning_codes=seasoning_codes, customer_names=customer_names,
        )
    except Exception as e:  # noqa: BLE001
        log.exception("parse_bulk_sample_request failed: %s", e)
        await send(
            update,
            f"❌ Parse failed: {h(str(e))}\n\nTry again or use /start to enter manually.",
            kb([[("🔄 Retry", "bulk:retry"), ("✖ Cancel", "bulk:cancel")]]),
        )
        return

    # Stash tokens so the footer can show them on any later reply. There's no
    # draft at this point, so attach to the next draft we open per item.
    ctx.user_data["bulk_tokens_in"] = int(tin or 0)
    ctx.user_data["bulk_tokens_out"] = int(tout or 0)

    items = result.get("items") or []
    if not items:
        await send(
            update,
            "⚠️ I couldn't find any seasoning items in that paste.\n\n"
            "You can retry or cancel and enter manually.",
            kb([[("🔄 Retry", "bulk:retry"), ("✖ Cancel", "bulk:cancel")]]),
        )
        return

    # Enrich each item with a catalog match (code + name + price + category)
    # so the individual Draft looks like a normal matched request.
    for it in items:
        hit = _match_seasoning_by_code(str(it.get("code", "")), catalog)
        if hit:
            it["_matched_code"] = hit.get("code", "")
            it["_matched_price"] = hit.get("price", "")
            it["_matched_category"] = hit.get("category", "")
            # Prefer the catalog's canonical name when we have a hit.
            if not it.get("seasoning"):
                it["seasoning"] = hit.get("name", "")
        else:
            it["_matched_code"] = ""
            it["_matched_price"] = ""
            it["_matched_category"] = ""
        it["_done"] = False

    # Fuzzy-match the customer name to the master — store the best hit so the
    # user can confirm (or override) when opening an item.
    cust_parsed = result.get("customer") or {}
    cust_name = str(cust_parsed.get("name", "")).strip()
    cust_hit: dict[str, str] | None = None
    if cust_name and customers:
        top = matcher.top_customer_master(cust_name, customers, limit=1)
        if top:
            cust_hit = top[0]
    result["_customer_match"] = cust_hit or {}
    result["items"] = items

    ctx.user_data["bulk_parsed"] = result
    await _show_bulk_list(update, ctx)


async def _show_bulk_list(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["bulk_stage"] = "list"
    result = ctx.user_data.get("bulk_parsed") or {}
    items = result.get("items") or []
    customer = result.get("customer") or {}
    market = result.get("market", "")
    deadline = result.get("deadline", "")
    cust_hit = result.get("_customer_match") or {}

    done_n = sum(1 for it in items if it.get("_done"))
    header_bits = [f"📄 <b>Bulk request — {len(items)} seasonings</b>"]
    if customer.get("name"):
        matched = f" (master: <code>{h(cust_hit.get('code',''))}</code>)" if cust_hit.get("code") else ""
        header_bits.append(f"🏢 <b>{h(customer.get('name'))}</b>{matched}")
    if market:
        header_bits.append(f"🌏 {h(market)}")
    if deadline:
        header_bits.append(f"⏰ Deadline: <b>{h(deadline)}</b>")
    if customer.get("receiving_person"):
        header_bits.append(f"🙋 {h(customer.get('receiving_person'))}")
    header_bits.append(f"\nSubmitted: <b>{done_n}/{len(items)}</b>")
    header_bits.append("Tap an item to review and submit it.")

    buttons: list[list[tuple[str, str]]] = []
    for i, it in enumerate(items):
        mark = "✅" if it.get("_done") else "⬜"
        name = it.get("seasoning", "") or "(unnamed)"
        code = it.get("_matched_code") or it.get("code", "") or "—"
        qty = it.get("quantity", "") or ""
        label = f"{mark} {i+1}. {name} · {code}"
        if qty:
            label += f" · {qty}"
        if len(label) > 60:
            label = label[:58] + "…"
        buttons.append([(label, f"bitem:{i}")])

    if done_n == len(items) and items:
        buttons.append([("🎉 Finish bulk session", "bulk:finish")])
    buttons.append([("✖ Cancel remaining", "bulk:cancel")])

    await send(update, "\n".join(header_bits), kb(buttons))


# Fields eligible for bulk cross-fill (V1.0.1). These are fields that are
# PER-ITEM (so different items can legitimately differ) but often repeat
# across a customer's request list. When the user fills one of these on an
# item during review — and other pending items have the same field empty —
# we offer to apply the same value across.
#
# Deliberately NOT in this set:
#   - seasoning / comment / quantity / code: inherently per-item
#   - taste_check / customer_base / courier / price_budget: already shared-
#     by-design at bulk session start
#   - company_name / address / receiver_number / receiving_person: already
#     carry via bulk_customer_carry
_BULK_CROSSFILL_FIELDS = {"app_method", "dosage", "requirement"}


def _bulk_crossfill_targets(
    ctx: ContextTypes.DEFAULT_TYPE, current_idx: int, field_key: str
) -> list[int]:
    """Indexes of OTHER pending bulk items with ``field_key`` still empty.

    Returns ``[]`` if:
      - the field isn't eligible for cross-fill
      - we're not in a bulk session
      - the current item's parsed value for this field was NOT empty
        (i.e. user is correcting an existing value, not filling a blank)
    """
    if field_key not in _BULK_CROSSFILL_FIELDS:
        return []
    parsed = ctx.user_data.get("bulk_parsed") or {}
    items = parsed.get("items") or []
    if not items or not (0 <= current_idx < len(items)):
        return []
    # Only prompt when the current item started out empty for this field.
    cur_orig = str(items[current_idx].get(field_key, "")).strip()
    if cur_orig:
        return []
    target_idxs: list[int] = []
    for i, it in enumerate(items):
        if i == current_idx:
            continue
        if it.get("_done"):
            continue
        if not str(it.get(field_key, "")).strip():
            target_idxs.append(i)
    return target_idxs


async def _show_bulk_crossfill_prompt(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE
) -> None:
    pending = ctx.user_data.get("bulk_crossfill") or {}
    field = pending.get("field", "")
    value = pending.get("value", "")
    targets = pending.get("targets", [])
    label = FIELD_LABELS.get(field, field)
    other_word = "item" if len(targets) == 1 else "items"
    await send(
        update,
        f"🔁 <b>Apply to other bulk items?</b>\n\n"
        f"You just filled <b>{h(label)}</b> with <b>{h(value)}</b>.\n"
        f"<b>{len(targets)}</b> other pending {other_word} "
        f"{'has' if len(targets) == 1 else 'have'} <b>{h(label)}</b> still empty.\n\n"
        f"Apply <b>{h(value)}</b> to all of them?",
        kb([
            [("✅ Yes — apply to all", "bxf:yes")],
            [("❌ No — only this item", "bxf:no")],
        ]),
    )


async def _handle_bulk_crossfill_callback(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE, action: str
) -> None:
    pending = ctx.user_data.pop("bulk_crossfill", None)
    user = update.effective_user
    d = state.get(user.id)
    if pending and d and action == "yes":
        field = pending.get("field", "")
        value = pending.get("value", "")
        targets = pending.get("targets", [])
        parsed = ctx.user_data.get("bulk_parsed") or {}
        items = parsed.get("items") or []
        applied = 0
        for i in targets:
            if 0 <= i < len(items) and not items[i].get("_done"):
                items[i][field] = value
                applied += 1
        label = FIELD_LABELS.get(field, field)
        await send(
            update,
            f"✅ Applied <b>{h(value)}</b> to <b>{applied}</b> other "
            f"item{'s' if applied != 1 else ''} for <b>{h(label)}</b>.",
        )
    # In both cases, snap back to this item's review screen.
    if d:
        d.return_to_review = False
        d.stage = "review"
        d.sub = ""
        await ask(update, ctx, d)


async def _open_bulk_item(update: Update, ctx: ContextTypes.DEFAULT_TYPE, idx: int):
    """Build a Draft from a parsed item + shared values.

    First bulk item (no carry yet) → jumps to 13/16 · Customer Company Name so
    the user can look up the master record, confirm the address, and enter the
    receiver details accurately. After that item is submitted, the 4 customer
    fields are stashed in ``bulk_customer_carry`` so subsequent items go
    straight to review.
    """
    user = update.effective_user
    result = ctx.user_data.get("bulk_parsed") or {}
    items = result.get("items") or []
    if not (0 <= idx < len(items)):
        await _show_bulk_list(update, ctx)
        return
    it = items[idx]
    shared = ctx.user_data.get("bulk_shared", {})
    customer = result.get("customer") or {}
    market = result.get("market", "")
    deadline = result.get("deadline", "")
    carry = ctx.user_data.get("bulk_customer_carry") or {}

    state.clear(user.id)
    d = state.start(user.id, user.username or user.first_name or "")
    # Seed tokens with the cost of the parse call (only on the first open — after
    # that the draft is cleared on submit, so a fresh draft for the next item
    # starts clean, which is fine; cost is attached to this one).
    if idx == 0 or not any(x.get("_done") for x in items):
        d.tokens_in = int(ctx.user_data.get("bulk_tokens_in", 0) or 0)
        d.tokens_out = int(ctx.user_data.get("bulk_tokens_out", 0) or 0)

    # Seasoning match
    d.matched_code = it.get("_matched_code", "") or ""
    d.matched_price = it.get("_matched_price", "") or ""
    d.matched_category = it.get("_matched_category", "") or ""
    d.data["seasoning"] = it.get("seasoning", "") or ""
    # Comment: seed with parsed comment OR a stock "Use code X — Name" line.
    it_comment = str(it.get("comment", "") or "").strip()
    if it_comment:
        d.data["comment"] = it_comment
    elif d.matched_code:
        d.data["comment"] = f"Use code {d.matched_code} — {d.data['seasoning']}"
    elif d.data["seasoning"]:
        d.data["comment"] = f"Use {d.data['seasoning']}"

    # Per-item fields from parse
    d.data["quantity"] = str(it.get("quantity", "") or "").strip()
    d.data["dosage"] = str(it.get("dosage", "") or "").strip()
    d.data["requirement"] = str(it.get("requirement", "") or "").strip()
    d.data["app_method"] = str(it.get("app_method", "") or "").strip()

    # Shared values (asked once for the whole bulk session)
    d.data["taste_check"] = shared.get("taste_check", "")
    d.data["customer_base"] = shared.get("customer_base", "")
    d.data["courier"] = shared.get("courier", "")
    d.data["price_budget"] = shared.get("price_budget", "")

    # Customer-level fields at the session level
    d.data["market"] = market or str(it.get("market", "") or "").strip()
    d.data["deadline"] = deadline or str(it.get("deadline", "") or "").strip()

    # Mark this draft as belonging to a bulk session so _submit knows to
    # return to the list instead of the post-submit screen.
    d.data["_bulk_idx"] = str(idx)
    ctx.user_data["bulk_current_item"] = idx

    if carry:
        # Subsequent items — carry over the 4 customer fields the user just
        # confirmed on the first item and skip straight to review.
        d.data["company_name"] = carry.get("company_name", "")
        d.data["address"] = carry.get("address", "")
        d.data["receiver_number"] = carry.get("receiver_number", "")
        d.data["receiving_person"] = carry.get("receiving_person", "")
        # Preserve the link flags so Back / edit behaves sanely if the user
        # tweaks the customer on this item.
        if carry.get("_address_linked"):
            d.data["_address_linked"] = carry["_address_linked"]
        if carry.get("_contact_linked"):
            d.data["_contact_linked"] = carry["_contact_linked"]
        d.stage = "review"
        d.sub = ""
        await send(
            update,
            f"📝 Item <b>{idx + 1}/{len(items)}</b> — using the customer "
            f"details you entered earlier: <b>{h(d.data['company_name'])}</b>. "
            "Review the seasoning, then Confirm &amp; submit.",
        )
        await ask(update, ctx, d)
        return

    # First item — walk the user through 13/16 · Customer Company Name so the
    # master-list fuzzy match + address confirmation runs normally. After the
    # customer fields are filled, ``return_to_review`` snaps back to review.
    d.data["company_name"] = ""
    d.data["address"] = ""
    d.data["receiver_number"] = ""
    d.data["receiving_person"] = ""
    d.data.pop("_address_linked", None)
    d.data.pop("_contact_linked", None)

    d.stage = "company_name"
    d.sub = ""
    d.return_to_review = True

    # Show a hint with what Claude parsed so the user can copy the company
    # name / receiver into the upcoming inputs if it looks right.
    hint_lines = ["📝 <b>Bulk session — customer details</b>", ""]
    hint_lines.append(
        f"Item <b>{idx + 1}/{len(items)}</b>. I'll ask you the customer "
        "company, address, phone and receiver next so they're accurate — "
        "you only need to do this once; the rest of the items will reuse "
        "what you enter here."
    )
    parsed_bits = []
    if customer.get("name"):
        parsed_bits.append(f"• Company: <i>{h(customer['name'])}</i>")
    if customer.get("address"):
        parsed_bits.append(f"• Address: <i>{h(customer['address'])}</i>")
    if customer.get("receiver_number"):
        parsed_bits.append(f"• Phone: <i>{h(customer['receiver_number'])}</i>")
    if customer.get("receiving_person"):
        parsed_bits.append(f"• Receiver: <i>{h(customer['receiving_person'])}</i>")
    if parsed_bits:
        hint_lines.append("")
        hint_lines.append("<b>Parsed from your paste (for reference):</b>")
        hint_lines.extend(parsed_bits)
    await send(update, "\n".join(hint_lines))
    await ask(update, ctx, d)


async def _handle_bulk_shared_callback(update, ctx, action: str):
    """Route bsh:* callbacks for shared-value collection."""
    shared = ctx.user_data.setdefault("bulk_shared", {})
    if action.startswith("taste:"):
        shared["taste_check"] = "Yes" if action.split(":", 1)[1] == "Y" else "No"
        await _ask_bulk_base(update, ctx)
        return
    if action.startswith("base:"):
        payload = action.split(":", 1)[1]
        if payload == "manual":
            await _ask_bulk_base_manual(update, ctx)
            return
        try:
            i = int(payload)
        except ValueError:
            return
        if 0 <= i < len(CUSTOMER_BASES):
            shared["customer_base"] = CUSTOMER_BASES[i]
            await _ask_bulk_courier(update, ctx)
        return
    if action.startswith("cou:"):
        shared["courier"] = action.split(":", 1)[1]
        await _ask_bulk_budget_currency(update, ctx)
        return
    if action.startswith("cur:"):
        shared["_currency"] = action.split(":", 1)[1]
        await _ask_bulk_budget_amount(update, ctx)
        return


async def _handle_bulk_callback(update, ctx, action: str):
    """Route bulk:* callbacks for session control."""
    user = update.effective_user
    if action == "cancel":
        for k in ("bulk_stage", "bulk_raw", "bulk_shared", "bulk_parsed",
                  "bulk_current_item", "bulk_tokens_in", "bulk_tokens_out",
                  "bulk_customer_carry"):
            ctx.user_data.pop(k, None)
        state.clear(user.id)
        await send(
            update,
            "✖ Bulk session cancelled.",
            kb([[("🏠 Main menu", "menu:home")]]),
        )
        return
    if action == "retry":
        await _run_bulk_parse(update, ctx)
        return
    if action == "finish":
        for k in ("bulk_stage", "bulk_raw", "bulk_shared", "bulk_parsed",
                  "bulk_current_item", "bulk_tokens_in", "bulk_tokens_out",
                  "bulk_customer_carry"):
            ctx.user_data.pop(k, None)
        await send(
            update,
            "🎉 <b>All bulk items submitted.</b>",
            kb([
                [("📋 My sample requests", "again:samples")],
                [("🏠 Main menu", "menu:home")],
            ]),
        )
        return
    if action == "list":
        await _show_bulk_list(update, ctx)
        return


async def _handle_bulk_text(update, ctx, text: str) -> bool:
    """If we're in a bulk-flow text-input stage, consume the text and return
    True. Otherwise return False so the normal handler runs."""
    stage = ctx.user_data.get("bulk_stage")
    if stage == "await_paste":
        if len(text) < 20:
            await send(
                update,
                "That looks too short to be a full request. Paste the whole "
                "email / message (at least a few lines), or tap Cancel.",
                kb([[("✖ Cancel", "bulk:cancel")]]),
            )
            return True
        ctx.user_data["bulk_raw"] = text
        await _ask_bulk_taste(update, ctx)
        return True
    if stage == "ask_base_manual":
        shared = ctx.user_data.setdefault("bulk_shared", {})
        shared["customer_base"] = text
        await _ask_bulk_courier(update, ctx)
        return True
    if stage == "ask_budget_amt":
        shared = ctx.user_data.setdefault("bulk_shared", {})
        cur = shared.get("_currency", "USD")
        shared["price_budget"] = f"{text} {cur}"
        shared.pop("_currency", None)
        await _run_bulk_parse(update, ctx)
        return True
    return False


# --------------------------- error handler ---------------------------

async def on_error(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    log.exception("Unhandled error: %s", ctx.error)
    if isinstance(update, Update) and update.effective_chat:
        try:
            await send(update, "⚠️ Something went wrong. Please try again or /cancel.")
        except Exception:  # noqa: BLE001
            pass


# --------------------------- startup ---------------------------

def _preflight() -> list[str]:
    errs = []
    if not config.TELEGRAM_BOT_TOKEN:
        errs.append("TELEGRAM_BOT_TOKEN is missing in .env")
    if not config.SEASONING_SHEET_ID:
        errs.append("SEASONING_SHEET_ID is missing in .env")
    if not config.OPS_SHEET_ID:
        errs.append("OPS_SHEET_ID is missing in .env")
    return errs


def main():
    errs = _preflight()
    if errs:
        for e in errs:
            log.error(e)
        raise SystemExit("Fix your .env and re-run.")

    # Startup diagnostics — shows in Railway logs so we can verify config.
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    log.info(
        "DIAG: GOOGLE_SERVICE_ACCOUNT_JSON=%s (len=%d)",
        "SET" if sa_json else "MISSING",
        len(sa_json),
    )
    log.info(
        "DIAG: OPS_SHEET_ID=%s",
        (config.OPS_SHEET_ID[:12] + "…") if config.OPS_SHEET_ID else "MISSING",
    )
    log.info(
        "DIAG: SEASONING_SHEET_ID=%s",
        (config.SEASONING_SHEET_ID[:12] + "…") if config.SEASONING_SHEET_ID else "MISSING",
    )

    log.info("Ensuring ops tabs exist…")
    try:
        sheets.ensure_ops_tabs()
    except Exception as e:  # noqa: BLE001
        log.exception("ensure_ops_tabs failed: %s", e)
        raise SystemExit(
            "Could not access the OPS sheet. Check that the service account "
            "email has Editor access to OPS_SHEET_ID."
        )

    app = Application.builder().token(config.TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("edit", cmd_edit))
    app.add_handler(CommandHandler("reload", cmd_reload))
    app.add_handler(CommandHandler("samples", cmd_samples))
    app.add_handler(CommandHandler("bulk", cmd_bulk))
    app.add_handler(CommandHandler("whoami", cmd_whoami))
    app.add_handler(CommandHandler("diag", cmd_diag))
    app.add_handler(CommandHandler("pp", cmd_pp))
    app.add_handler(CommandHandler("scan", cmd_scan))
    app.add_handler(CommandHandler("updatesamplelist", cmd_update_sample_list))

    # Register Telegram's native blue slash-command menu so users see every
    # function the bot offers when they tap '/'. /updatesamplelist is NOT in
    # the default list (it's restricted), but we'll add it for @ragonic below
    # via post_init once the bot is running.
    async def _install_commands(application: Application) -> None:
        default_cmds = [
            BotCommand("start", "Main menu — new request / bulk / samples"),
            BotCommand("bulk", "Paste a multi-seasoning email, I split it"),
            BotCommand("samples", "List samples you've raised"),
            BotCommand("edit", "Jump to the draft review to change a field"),
            BotCommand("cancel", "Discard the current draft"),
            BotCommand("reload", "Refresh seasoning / customer lists"),
            BotCommand("whoami", "Show your Telegram ID & username"),
            BotCommand("pp", "💲 Product price — e.g. /pp S-62RG3-19"),
            BotCommand("scan", "📷 Scan a photo for product code(s)"),
            BotCommand("diag", "Diagnostics"),
            BotCommand("help", "Show all commands"),
        ]
        try:
            await application.bot.set_my_commands(
                default_cmds, scope=BotCommandScopeDefault()
            )
        except Exception as e:  # noqa: BLE001
            log.warning("set_my_commands (default) failed: %s", e)

    app.post_init = _install_commands
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_handler(MessageHandler(filters.PHOTO, on_photo))
    app.add_error_handler(on_error)

    # Warm the caches so the first user of the day doesn't wait on cold
    # Google Sheets reads. Failures here are non-fatal — the runtime caches
    # will refill on demand.
    log.info("Pre-warming caches…")
    try:
        users = sheets.load_users()
        log.info("DIAG: Authorized Users tab loaded — %d row(s)", len(users))
        sheets.load_seasonings()
        sheets.load_customer_master()
        sheets.load_customers()
    except Exception as e:  # noqa: BLE001
        log.warning("cache warmup failed (will lazy-load): %s", e)

    log.info("Bot starting…")
    app.run_polling()


if __name__ == "__main__":
    main()
