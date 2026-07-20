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
import io
import logging
import os
import re
from decimal import ROUND_CEILING, Decimal
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from telegram import (
    BotCommand,
    BotCommandScopeDefault,
    ForceReply,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ChatType, ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

import ai
import awb_sync
import config
import groq_voice
import matcher
import mms_product
import sheets
import state
import vision_scan
from state import FIELDS, FIELD_LABELS

# Sales names recognised by the Vercel quotation builder (?sales= param).
# Kept here (not pulled from a module) so bot.py has zero import-time
# dependency on reportlab — the in-bot Python PDF generator was removed
# in V1.15.0 when the form moved to the web.
_QUOTE_SALES_NAMES = ["Alex", "Adrian", "Eric", "Jay", "Rich", "Melissa"]

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
    markup=None,
    *,
    with_footer: bool = True,
    force_new: bool = False,
):
    """Send/edit a message to the user.

    The version + token footer is ON by default. Pass ``with_footer=False``
    only when stacking multiple bot replies for one logical action and you
    don't want the footer to repeat on each fragment.

    ``force_new=True`` bypasses the callback-edit fallback and always
    sends a fresh message. Required when ``markup`` is a ForceReply or
    ReplyKeyboardMarkup — Telegram's edit_message_text only accepts
    InlineKeyboardMarkup, so without this flag the special markup is
    silently dropped. Used by the /lastsample / /alllastsample welcome
    prompts so ForceReply actually surfaces in group chats (V1.10.6 fix).

    If the message has inline buttons, the originating user is recorded so
    on_callback can refuse cross-user clicks in group chats.
    """
    full = f"{text}\n\n{_footer(update)}" if with_footer else text
    user = update.effective_user
    stuck = bool(user and user.id in _stuck_reminder_users)
    sent_msg = None
    if update.callback_query and not stuck and not force_new:
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
        # Audit V1.13.14 — fail OPEN on Sheets API errors. Previously
        # returned False, which silently locked out the whole team
        # whenever Google Sheets had a transient hiccup (quota, 503,
        # network blip). Caller is told the bot is degraded so they
        # don't think they were de-authorized, but we still let them
        # through. Real "not on the list" denials go through the
        # explicit `not ok` branch below with the normal message.
        log.exception("auth check failed for uid=%s uname=%s: %s", user.id, user.username, e)
        await send(
            update,
            "⚠️ <b>Auth check is degraded right now.</b>\n\n"
            "Google Sheets isn't responding cleanly. You're being let "
            "through this time — please retry the command in a minute "
            "if anything else fails. (Admin: check Sheets quota / "
            "service-account permissions.)",
        )
        return True
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
    # V1.12.0:
    #   • removed the "Paste a multi-seasoning email" button (the /bulk
    #     command still works if a user types it directly — just decluttered
    #     the menu for new hires)
    #   • added "🔎 Search seasonings" — region-aware browse-only search
    #     that lets reps explore the SG / ID / TH sample history without
    #     raising a sample request
    # V1.17.x — typing-first menu. The smart router auto-detects typed
    # codes / customer / rep / product words, so the old '💲 Look up
    # product code' button (which only armed a typing flag) is gone;
    # 📷 Scan takes its slot because photos are the one input that still
    # needs a button (group-chat privacy mode requires the reply gate).
    # menu:lookup and menu:code handlers stay registered so buttons on
    # old messages keep working.
    menu = [
        [("🔎 Search seasonings", "menu:search")],
        [("📷 Scan a product photo", "menu:scan")],
        [("📄 Build a quotation", "menu:quote")],
        [("👤 My samples", "menu:lastsample"),
         ("🌐 All reps'", "menu:alllastsample")],
    ]
    # MMS → Full Sample Listing sync is now automated weekly via the
    # JobQueue (see main()). No manual Telegram trigger.
    await send(
        update,
        "👋 <b>Just type — I auto-detect what you mean:</b>\n"
        "  • code <code>S-668U1</code> → 💲 price\n"
        "  • customer name → 📦 samples sent to them\n"
        "  • rep name (<i>Alex</i>) → 👔 samples they sent\n"
        "  • product words (<i>cheese bbq</i>) → 🔎 matches\n\n"
        "<i>Buttons for photos &amp; browsing · /help for all commands.</i>",
        kb(menu),
    )


async def cmd_bulk(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await _authorized(update):
        return
    await _start_bulk(update, ctx)


def _resolve_quote_sales_name(user) -> str:
    """Best-effort map of the Telegram rep to a known sales-person name.

    Used to pre-fill the ?sales=<name> query param when handing out the
    Vercel quote-builder link. Strategy:
      1. Authorized-Users tab → MMS Name column → match against the six
         recognised sales names (Alex, Adrian, Eric, Jay, Rich, Melissa).
      2. Fall back to the Telegram first name if it matches.
      3. Return "" if nothing matches → web form just shows an empty
         dropdown, no harm done.
    """
    try:
        mms_name = sheets.get_user_mms_name(getattr(user, "id", None),
                                            getattr(user, "username", None))
    except Exception:
        mms_name = ""
    candidates = [mms_name or "", getattr(user, "first_name", "") or ""]
    for cand in candidates:
        cand = (cand or "").strip()
        if not cand:
            continue
        for name in _QUOTE_SALES_NAMES:
            if name.lower() == cand.lower():
                return name
            # Some MMS names are full names ("Jay Wong") — match on first token.
            first = cand.split()[0]
            if name.lower() == first.lower():
                return name
    return ""


async def cmd_quote(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Hand the rep a clickable link to the Vercel quotation web app.

    The form + PDF generation live on Vercel (see quote_web/). The bot's
    only job here is to deep-link with the rep's sales name pre-filled so
    the dropdown lands on the right person on page load.
    """
    if not await _authorized(update):
        return
    if not config.QUOTE_WEB_URL:
        await send(
            update,
            "⚠️ The quotation builder URL isn't configured yet.\n\n"
            "<b>Admin:</b> deploy the <code>quote_web/</code> folder to "
            "Vercel (see <code>quote_web/README.md</code>), then set the "
            "<code>QUOTE_WEB_URL</code> env var on Railway and restart the bot.",
            kb([[("🏠 Main menu", "menu:home")]]),
        )
        return
    user = update.effective_user
    sales = _resolve_quote_sales_name(user)
    sep = "&" if "?" in config.QUOTE_WEB_URL else "?"
    url = f"{config.QUOTE_WEB_URL}{sep}sales={sales}" if sales else config.QUOTE_WEB_URL
    body = (
        "📄 <b>Build a quotation</b>\n\n"
        "Tap the button below to open the quotation builder. Fill in the "
        "customer details and product lines, then tap <b>Generate &amp; "
        "download PDF</b> to save a print-ready A4 file."
    )
    if sales:
        body += f"\n\n<i>Signed-by dropdown pre-filled as: <b>{h(sales)}</b></i>"
    else:
        body += (
            "\n\n<i>I couldn't auto-detect your sales name — pick it from "
            "the dropdown on the page.</i>"
        )
    btns = InlineKeyboardMarkup([
        [InlineKeyboardButton("📄 Open quotation builder", url=url)],
        [InlineKeyboardButton("🏠 Main menu", callback_data="menu:home")],
    ])
    await send(update, body, btns)


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
        "💡 <b>Just type — no menu needed.</b> I auto-detect what you mean:",
        "  • a code (<code>S-668U1</code>) → price",
        "  • a customer name → samples sent to them",
        "  • a rep's name (e.g. <i>Alex</i>) → samples they sent",
        "  • product words (<i>cheese bbq</i>) → matching products",
        "",
        "/start — main menu",
        "/bulk — paste a multi-seasoning email, I split it for you",
        "/samples — review the requests you've raised",
        "",
        "<b>Quotations</b>",
        "/quote — open the web quotation builder (Vercel) in your browser",
        "",
        "<b>While drafting</b>",
        "/edit — jump back to the review to change any field",
        "/cancel — discard the current draft",
        "",
        "<b>Product lookup</b>",
        # NB: the placeholder is shown as &lt;code&gt; (HTML entities) NOT a literal
        # <code> tag, because Telegram's HTML parser would otherwise try to
        # interpret it as an unclosed <code> monospace span and reject the
        # whole message (which dropped /help into the generic error fallback).
        "/pp &lt;code&gt; — fetch price (Code · Name · R&amp;D Price · Raw Material Cost)",
        "/scan — send a photo, I OCR codes and run /pp on each",
        "🎤 Voice — just send a voice message saying the code (<i>'S dash 668 U 1'</i>)",
        "/lastsample [keyword] — most recent sample <b>you</b> sent (e.g. <code>/lastsample asian thai</code>)",
        "/alllastsample [keyword] — most recent sample <b>any rep</b> sent — shared visibility across the team",
        "",
        "<b>Account</b>",
        "/whoami — your Telegram ID and username",
        "/help — this message",
    ]
    if is_admin:
        # These commands stay registered as handlers but are hidden from
        # the / autocomplete menu (housekeeping audit) so regular reps
        # don't see admin tools cluttering their picker. /help is the
        # single place admins can rediscover them.
        lines += [
            "",
            "<b>🔧 Admin (hidden from / autocomplete; still work if typed)</b>",
            "/reload — refresh seasoning &amp; customer lists from Sheets",
            "/syncawb [dry] — run the AWB sync manually (DHL + FedEx → FSL col K)",
            "/diag — diagnostics (auth / sheet visibility)",
            "/whichchat — show this chat's ID (for DAILY_DIGEST_CHAT_ID setup)",
            "/sampleupdate — preview &amp; post today's 6pm digest now",
            "<i>(MMS → Full Sample Listing sync runs automatically weekday "
            "evenings — see Railway logs for run history.)</i>",
        ]
    await send(update, "\n".join(lines))


# /updatesamplelist Telegram command and its _run_update_sample_list helper
# were retired in V1.7.1. The MMS → Full Sample Listing sync is now
# automated via the JobQueue scheduled task in main(); see sync_engine.py
# for the actual fetch+enrich+append logic.


async def cmd_syncawb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Manual AWB sync — ragonic-only. Runs once, posts the result.

    /syncawb           → full run (last 14 days), write + push to group
    /syncawb dry       → preview run, no writes, no group post
    /syncawb quiet     → full run, write but DON'T post to group (useful
                         when re-running just to backfill without spam)
    /syncawb 365       → deep backfill — scrape the last 365 days of DHL/
                         FedEx history. Slower (~5-30min depending on
                         carrier history). Quiet by default. Max 730.
    """
    if not await _authorized(update):
        return
    if not _is_update_sample_owner(update.effective_user):
        await send(update, "🛑 This command is admin-only.")
        return
    args = [a.lower() for a in (ctx.args or [])]
    dry_run = any(a in ("dry", "preview", "test") for a in args)
    quiet = any(a in ("quiet", "nopost", "silent") for a in args)
    # V1.17.x — accept a numeric arg as days_back so reps can trigger a
    # deep backfill (e.g. `/syncawb 365` to scrape the last year of DHL
    # ship-to addresses into FSL col L). Capped at 730 days to avoid
    # accidental scrapes back to the dawn of MyDHL+. Backfills default
    # to quiet — a 30-minute scrape shouldn't push a noisy group update.
    days_back = 14
    custom_days = False
    for a in args:
        try:
            n = int(a)
            if n > 0:
                days_back = min(n, 730)
                custom_days = True
                break
        except ValueError:
            continue
    if custom_days and days_back > 30:
        quiet = True  # backfills don't push to chat by default
    mode_parts = [f"{days_back}d window"]
    mode_parts.append("preview" if dry_run else ("live, quiet" if quiet else "live + push"))
    mode = " · ".join(mode_parts)
    await send(
        update,
        f"📦 <b>Running AWB sync</b> ({mode})…\n\n"
        "<i>Fetching from DHL + FedEx, matching against the 3 FSL tabs, "
        f"then writing AWB to col K + address to col L. May take "
        f"{'~30s' if days_back <= 30 else '5-30min on deep backfills'}.</i>",
        with_footer=False,
    )
    try:
        result = await awb_sync.run_awb_sync(days_back=days_back, dry_run=dry_run)
    except Exception as e:  # noqa: BLE001
        await send(update, f"⚠️ AWB sync crashed: <code>{h(e)}</code>")
        log.exception("/syncawb crashed")
        return
    # Reply to the admin with the result summary first.
    await send(update, awb_sync.format_result_for_telegram(result))

    # Also push the 'AWB Update' message to the daily digest chat —
    # same behaviour as the scheduled job — unless this was a dry run
    # or the rep asked for 'quiet'. Skip silently if nothing was
    # written or DAILY_DIGEST_CHAT_ID isn't configured.
    if dry_run or quiet:
        return
    msg = awb_sync.format_update_message(result)
    if not msg:
        return  # no carrier matches were written → nothing to announce
    if not config.DAILY_DIGEST_CHAT_ID:
        await send(
            update,
            "<i>ℹ️ Skipped group post — DAILY_DIGEST_CHAT_ID env var is not "
            "set on Railway.</i>",
        )
        return
    try:
        chat_id = int(config.DAILY_DIGEST_CHAT_ID)
        await ctx.bot.send_message(
            chat_id=chat_id, text=msg, parse_mode=ParseMode.HTML,
        )
        await send(
            update,
            f"✅ AWB Update posted to chat <code>{h(config.DAILY_DIGEST_CHAT_ID)}</code>.",
        )
    except Exception as e:  # noqa: BLE001
        log.exception("/syncawb: group post failed")
        await send(update, f"⚠️ Group post failed: <code>{h(e)}</code>")


async def cmd_whoami(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await send(
        update,
        f"Username: <code>@{h(u.username or '(none)')}</code>\nID: <code>{u.id}</code>",
        with_footer=True,
    )


# Match seasoning codes. History of one-segment-at-a-time fixes:
#   V1.6.x: one optional '-XX' suffix → broke on S-TXF06-00-03
#   V1.9.7: up to 3 suffixes        → would break on a 5-segment code
#   V1.9.8: up to 6 suffixes        → covers anything we've seen and
#           leaves headroom (e.g. S-T4C83-35-07-11 has 3, but the user
#           wants margin so we don't chase regex bumps every time R&D
#           introduces a new naming layer)
#   V1.12.0: extended to also match J- (Indonesia, Jakarta factory) and
#           T- (Thailand, Bangkok factory). find_fsl_product_by_code in
#           sheets.py auto-routes by prefix to the matching tab, so /pp
#           and the new search just need to extract the code.
#   V1.12.1: added B- (legacy / older codes that share the Singapore
#           tab — 205 of them survived in FSL after the Jakarta cleanup,
#           and reps still look them up). Same factory routing as S-.
# Anchored on [SJTB]- + 3+ alphanumerics so we don't grab unrelated tokens.
_PP_CODE_RE = re.compile(
    r"\b[SJTB]-[A-Za-z0-9]{3,}(?:-[A-Za-z0-9]{1,6}){0,6}\b",
    re.IGNORECASE,
)


# V1.13.11 — per-rep currency display preference.
#
# Sales reps based in Indonesia (William, Leo, Freddy, Heidy) quote
# customers in IDR. Reps in Thailand (Nu, Jang, Ying) quote in THB.
# Everyone else sees raw prices as stored on the sheet (USD / SGD /
# whatever the FSL row holds).
#
# Match is by MMS Name (lowercase, first-word fallback) so we don't
# care about Telegram username casing. Reps not yet added to the
# Authorized Users tab get the override applied automatically once
# you add them — no code change needed.
_USER_CURRENCY_OVERRIDE: dict[str, str] = {
    # Indonesia reps → IDR
    "william": "IDR",
    "leo":     "IDR",
    "freddy":  "IDR",
    "heidy":   "IDR",
    # Thailand reps → THB
    "nu":      "THB",
    "jang":    "THB",
    "ying":    "THB",
}

# V1.17.x — hardcoded currency rates removed. ALL conversions now go
# through MMS3's live exchange-rate table via mms_product.get_client().
# get_rate_to_usd / get_rate_from_usd. When MMS3 is unreachable or
# doesn't list a currency, the bot SHOWS THE SOURCE VALUE VERBATIM
# rather than inventing a converted number — discrepancies between
# the bot and MMS3 are now impossible by design.
#
# Earlier iterations kept a fallback `_CURRENCY_USD_RATE` dict here
# (IDR 0.000063, THB 0.029, …). The values drifted from MMS3's actual
# rates (MMS3 has 1 USD = ~17,883 IDR vs the dict's 15,873) and any
# conversion path that touched both rate sources compounded the
# mismatch into visibly wrong figures. The dict has been deleted.

_CURRENCY_PRICE_PARSE_RE = re.compile(
    r"^([A-Z]{3,4}|RM|S\$|\$)\s*([\d,]+(?:\.\d+)?)$",
    re.IGNORECASE,
)


def _mms_rate_to_usd(currency: str) -> float | None:
    """1 unit of `currency` = X USD per MMS3's live rates. None on fail.

    Caller MUST NOT substitute a hardcoded rate when this returns None —
    that's how price discrepancies between the bot and MMS3 happen.
    Show the source value in its original currency instead.
    """
    cur = (currency or "").upper()
    if cur == "USD":
        return 1.0
    try:
        return mms_product.get_client().get_rate_to_usd(cur)
    except Exception as e:  # noqa: BLE001 — never block display on MMS3 error
        log.debug("MMS3 rate-to-USD lookup failed for %s: %s", cur, e)
        return None


def _mms_rate_from_usd(currency: str) -> float | None:
    """1 USD = X units of `currency` per MMS3's live rates. None on fail.

    Same no-fallback contract as `_mms_rate_to_usd` — when None, the
    caller shows source verbatim rather than inventing a number.
    """
    cur = (currency or "").upper()
    if cur == "USD":
        return 1.0
    try:
        return mms_product.get_client().get_rate_from_usd(cur)
    except Exception as e:  # noqa: BLE001
        log.debug("MMS3 rate-from-USD lookup failed for %s: %s", cur, e)
        return None


def _user_currency_for_mms(mms_name: str | None) -> str | None:
    """Return user's preferred currency code (IDR / THB) or None."""
    if not mms_name:
        return None
    norm = mms_name.strip().lower()
    if norm in _USER_CURRENCY_OVERRIDE:
        return _USER_CURRENCY_OVERRIDE[norm]
    # Fallback: first token, so 'William Lee' still resolves.
    parts = norm.split()
    if parts and parts[0] in _USER_CURRENCY_OVERRIDE:
        return _USER_CURRENCY_OVERRIDE[parts[0]]
    return None


async def _user_pref_currency(update: "Update") -> str | None:
    """Look up the rep's preferred currency override for this request.

    Resolves Telegram identity → MMS Name (via get_user_mms_name's
    5-min cache) → currency override. Returns None when there's no
    override configured for this rep, which means callers fall back
    to showing raw prices as stored on the sheet.
    """
    user = update.effective_user if update else None
    if not user:
        return None
    try:
        mms = await asyncio.to_thread(
            sheets.get_user_mms_name, user.id, user.username,
        )
    except Exception:  # noqa: BLE001 — never break price display
        return None
    return _user_currency_for_mms(mms or "")


def _parse_price_to_usd(raw: str) -> tuple[float | None, str]:
    """Parse a raw price cell to USD-equivalent + the cleaned original.

    Handles three input shapes the FSL stores:
      • bare number ('5.44')         → assumes USD
      • currency prefix ('USD 5.44') → straightforward
      • exotic currency ('IDR 59,322', 'THB 162.9', 'SGD 6.60', 'RM 12')
        → converted via MMS3's live exchange-rate table
    Returns (None, original) when we can't parse OR MMS3 doesn't list
    the source currency — the caller shows the original string
    untouched rather than inventing a fake conversion.
    """
    s = (raw or "").strip()
    if not s:
        return None, s
    # Bare numeric → USD by convention (matches existing code paths).
    try:
        v = float(s.replace(",", ""))
        return (v, s) if v > 0 else (None, s)
    except ValueError:
        pass
    m = _CURRENCY_PRICE_PARSE_RE.match(s)
    if not m:
        return None, s
    cur_raw, num_raw = m.group(1).upper(), m.group(2).replace(",", "")
    cur_norm = {"S$": "SGD", "$": "USD", "RM": "MYR"}.get(cur_raw, cur_raw)
    try:
        v = float(num_raw)
    except ValueError:
        return None, s
    if v <= 0:
        return None, s
    # V1.17.x — MMS3 rates only. If MMS3 doesn't list this currency,
    # signal "untranslatable" to the caller so it can show the source
    # verbatim instead of compounding a wrong number through USD.
    rate = _mms_rate_to_usd(cur_norm)
    if rate is None:
        return None, s
    return v * rate, s


def _format_price_for_currency(
    raw: str,
    target: str | None,
    *,
    show_original: bool = False,
) -> str:
    """Convert a raw price cell to the target currency for display.

    target=None (no override) → return the raw string unchanged (with
    a 'USD ' prefix added if the cell was bare numeric, matching the
    existing _fmt_price behaviour).

    target='IDR' / 'THB' → if input ALREADY in target currency, pass
    through verbatim. Otherwise convert via MMS3's live exchange-rate
    table; emit formatted with thousands separators and
    currency-appropriate decimal places (IDR: 0 dp, others: 2 dp).
    When MMS3 doesn't list the source or target currency, the raw
    input is returned unchanged — never a fake hardcoded conversion.
    """
    s = (raw or "").strip()
    if not s:
        return ""
    if not target:
        # No override — preserve existing behaviour (bare → 'USD X').
        try:
            float(s.replace(",", ""))
            return f"USD {s}"
        except ValueError:
            return s
    # V1.17.x — native passthrough. When the input string's currency
    # already matches the rep's target currency, return it verbatim
    # (with a clean normalised prefix). No conversion, no round-trip
    # through USD that compounds two different rates. Fixes the case
    # where FSL stores 'IDR 44,575' and the bot was previously
    # displaying that as 'IDR 50,147 (from IDR 44,575)' to an IDR rep.
    m = _CURRENCY_PRICE_PARSE_RE.match(s)
    if m:
        cur_raw = m.group(1).upper()
        cur_norm = {"S$": "SGD", "$": "USD", "RM": "MYR"}.get(cur_raw, cur_raw)
        if cur_norm == target.upper():
            return f"{cur_norm} {m.group(2)}"
    usd, original = _parse_price_to_usd(s)
    if usd is None:
        # Source-currency rate unavailable from MMS3 (or input unparseable).
        # Show the input verbatim rather than fake a number.
        return original
    # V1.17.x — MMS3 rates only. If MMS3 doesn't list the target
    # currency, show the source verbatim. NEVER fall back to a
    # hardcoded rate — see the comment where _CURRENCY_USD_RATE was
    # removed for why doing so introduced visible discrepancies.
    target_rate = _mms_rate_from_usd(target)
    if not target_rate:
        return original
    local = usd * target_rate
    if target == "IDR":
        body = f"{local:,.0f}"
    else:
        body = f"{local:,.2f}"
    formatted = f"{target} {body}"
    if show_original and original.strip().upper() != formatted.upper():
        # Only annotate when the original was non-empty AND different
        # from our converted output (avoids redundancy like
        # 'IDR 59,322 (was IDR 59,322)').
        formatted += f" <i>(from {h(original)})</i>"
    return formatted


# V1.12.6 — smarter text match used by /lastsample, /alllastsample, and the
# 🔎 Search seasonings flow. Three-pass: original substring → alphanumeric-
# stripped substring (handles "Datong"/"Da tong") → token-level WRatio for
# typos. Designed to fix the known V1.8.8 false-positive case (peri →
# pepper) while still catching common typos and spacing variations.
_ALNUM_ONLY_RE = re.compile(r"[^a-z0-9]+")
_TOKEN_RE = re.compile(r"[a-z0-9]+")
# Stopwords used by token-aware matching — short connectors that would
# otherwise pollute multi-word AND checks (e.g. 'fish AND chip' must
# still match 'fish & chips' after the connector is dropped).
_LASTSAMPLE_STOPWORDS = frozenset({"and", "or", "the", "of", "with", "in", "for", "to"})


def _lastsample_query_tokens(s: str) -> list[str]:
    """Lowercase + strip stopwords + drop 1-char tokens. Used by both
    /lastsample's matcher and the pagination callback so behaviour is
    identical across them."""
    return [
        t for t in _TOKEN_RE.findall((s or "").lower())
        if len(t) >= 2 and t not in _LASTSAMPLE_STOPWORDS
    ]


def _match_lastsample_product(row: dict, query: str) -> bool:
    """Match a single FSL row against `query` for the /lastsample +
    /alllastsample product search.

    V1.12.8: when the query is itself a product code (matches
    _PP_CODE_RE), we match ONLY against the code column — not against
    name or taste. Reps typing a code want THAT code; running fuzzy
    name match on a code-shaped query produced bizarre cross-prefix
    false positives (e.g. 'J-54Df1-04' → 'TEXTURE & FLAVOUR IMPROVER
    #2-04' because '04' is a target token).

    Otherwise, three layers:
      1. Code substring (strict — fuzzy on a code is too risky).
      2. Product name smart match (V1.12.6 — handles spacing & typos).
      3. Multi-word AND check across name tokens (precision filter for
         queries like 'spicy chicken below 4 usd').
    """
    name = (row.get("Product Name") or "").lower()
    code = (row.get("Product Code") or "").lower()
    q = (query or "").lower().strip()

    # Code-shaped query: search codes only.
    if _PP_CODE_RE.fullmatch(q.upper()) or _PP_CODE_RE.search(q.upper()):
        # The query looks like a code (or contains one). Match strictly
        # against the row's code — substring is fine so a base 'S-668'
        # surfaces all '-XX' variants.
        return bool(q) and q in code

    if q and q in code:
        return True
    if _smart_text_match(query, name):
        return True
    q_tokens = _lastsample_query_tokens(query)
    if q_tokens and len(q_tokens) > 1:
        name_tokens = _lastsample_query_tokens(name)
        if name_tokens:
            for qt in q_tokens:
                if not any(qt in nt for nt in name_tokens):
                    return False
            return True
    return False


def _filter_lastsample_products(rows: list[dict], query: str) -> list[dict]:
    """Filter `rows` to product matches and return them sorted date-desc."""
    matches = [r for r in rows if _match_lastsample_product(r, query)]
    from datetime import date as _date
    SENTINEL = _date(1900, 1, 1)
    matches.sort(key=lambda r: r.get("_date") or SENTINEL, reverse=True)
    return matches


async def _load_lastsample_rows(scope: str, mms_name: str = "") -> list[dict]:
    """Load FSL rows for /lastsample + /alllastsample from BOTH the
    Singapore tab (S- and B-codes) AND the Jakarta tab (J-codes), then
    return the merged list.

    V1.12.9 fix: was loading only FSL_TAB by default, so any J-code
    query (e.g. 'J-X4Ua5-01') silently returned 'no match' even though
    the J-code existed in the Jakarta tab. V1.13.0 added Thailand /
    BANGKOK_FSL_TAB to the union — same fix for B-codes which now
    have their own region tab instead of being lumped into Singapore.

    V1.16.0 — wraps the per-tab read in the 90s TTL cache (see
    sheets._FSL_ROWS_CACHE) so search bursts don't blow through the
    Sheets API's 60-reads-per-minute quota.

    scope='all'  → all reps' samples across all three tabs
    scope='self' → mms_name's samples across all three tabs

    Raises RuntimeError if every tab failed to read (typically
    Sheets API 429). Caller surfaces a clear retry message to the
    user instead of silently returning [] which used to look like
    "no match".
    """
    region_tabs = (sheets.FSL_TAB, sheets.JAKARTA_FSL_TAB, sheets.BANGKOK_FSL_TAB)

    # The three tabs load CONCURRENTLY (independent network reads; the
    # sheets-layer cache makes repeats instant, concurrency cuts the cold
    # path to the slowest single tab instead of the sum). Per-tab failures
    # are tracked; a single tab failing degrades gracefully.
    failures: list[str] = []

    async def _one_tab(tab: str) -> list[dict]:
        try:
            if scope == "all":
                return await asyncio.to_thread(sheets.load_fsl_rows_all, tab)
            return await asyncio.to_thread(
                sheets.load_fsl_rows_for_sales, mms_name, tab,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("_load_lastsample_rows: tab %r read failed: %s", tab, e)
            failures.append(f"{tab}: {type(e).__name__}")
            return []

    parts = await asyncio.gather(*(_one_tab(t) for t in region_tabs))
    rows: list[dict] = []
    for part in parts:
        rows.extend(part)
    # If ALL tabs failed we deliberately raise so the search handler can
    # tell the user 'Sheets is rate-limited, try again in a minute'
    # instead of showing 'no match'.
    if not rows and failures and len(failures) == len(region_tabs):
        raise RuntimeError(
            "All FSL tab reads failed (likely Sheets API rate limit). "
            "Failures: " + " · ".join(failures)
        )
    return rows


def _smart_text_match(query: str, target: str, fuzzy_threshold: int = 80) -> bool:
    """Match `query` against `target` with progressive tolerance.

    Pass 1: exact lowercase substring of the original target.
    Pass 2: alphanumeric-stripped substring (handles spacing & punctuation
            variations — "Datong" → "Da tong", "S.G. Foods" → "sg foods").
    Pass 3: rapidfuzz WRatio against each whitespace-tokenised target term
            AND the squished-target string. Threshold 80 is high enough to
            reject the V1.8.8 false-positive (peri → pepper, score 77) but
            low enough to catch common typos:
                rendnag → rendang seasoning  → 86
                rendng  → rendang seasoning  → 92
                cheez   → cheese seasoning   → 80
                Datng   → Da tong            → 91
            Pass 3 only runs for queries with ≥ 4 alphanumeric characters,
            so 'peri' (4 chars but score 77) and shorter queries are
            handled by Pass 1/2 only and fuzzy can't kick in.

    Code matching should stay STRICT and is handled separately by callers
    — fuzzy on product codes would be too risky (a 1-char distance could
    match the wrong SKU).
    """
    if not query or not target:
        return False
    q_low = query.lower().strip()
    t_low = target.lower().strip()
    # Pass 1
    if q_low and q_low in t_low:
        return True
    # Pass 2
    qn = _ALNUM_ONLY_RE.sub("", q_low)
    if not qn:
        return False
    tn = _ALNUM_ONLY_RE.sub("", t_low)
    if qn in tn:
        return True
    # Pass 3 — fuzzy fallback (only for non-trivial queries)
    if len(qn) < 4:
        return False
    try:
        from rapidfuzz import fuzz
    except ImportError:
        return False
    # Compare against every whitespace token AND the full squished string,
    # so 'rendnag' matches 'rendang' (token) even though the full target
    # 'rendang seasoning' would dilute the WRatio.
    #
    # Skip very short target tokens (< 4 chars) — V1.12.8 fix. Short
    # numeric tokens like '04' or '12' inside a target like 'TEXTURE
    # FLAVOUR IMPROVER #2-04' get partial_ratio = 100 against any query
    # whose squished form contains those digits (e.g. 'J-54Df1-04' →
    # 'j54df104' contains '04'). That produced the bizarre false match
    # where typing a J-code returned an unrelated S-code product.
    target_tokens = [t for t in _TOKEN_RE.findall(t_low) if len(t) >= 4]
    target_tokens.append(tn)
    # V1.13.8: target token must be at least 80% as long as the query
    # (alphanumeric form). V1.13.3 used 60%, but for 6-char queries the
    # ratio collapsed back to the 4-char floor (ceil(0.6*6)=4), letting
    # 'masala' (6) match the unrelated 4-char token 'mala'. Bumping to
    # 80% makes the guard kick in at qn=6 (threshold 5) while still
    # allowing legitimate typo matches at the same length (e.g.
    # 'rendnag'→'rendang', both 7 chars, threshold 6).
    # Ceil(0.8 * len(qn)) via integer math.
    min_tok_len = max(4, (len(qn) * 8 + 9) // 10)
    for tok in target_tokens:
        if tok is tn:
            # Squished full target — length comparison doesn't apply,
            # the partial_ratio already handles long-vs-short here.
            pass
        elif len(tok) < min_tok_len:
            continue
        if tok and fuzz.WRatio(qn, tok) >= fuzzy_threshold:
            return True
    return False


# Max product codes per /pp or /scan invocation. Each code triggers an
# MMS round-trip (product detail + R&D price scrape), so this caps both
# server load and the time the user waits before seeing results.
# Admin (UPDATE_SAMPLE_OWNER) gets a higher cap for bulk price audits;
# regular reps get the conservative 10 to keep MMS load predictable.
PP_BATCH_CAP_ADMIN = 30
PP_BATCH_CAP_USER = 10


def _pp_cap_for(user) -> int:
    """Per-user batch cap for /pp and /scan."""
    return PP_BATCH_CAP_ADMIN if _is_update_sample_owner(user) else PP_BATCH_CAP_USER


def _dedupe_codes(codes: list[str], cap: int = PP_BATCH_CAP_USER) -> list[str]:
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


async def _await_with_status(
    aw,
    on_status,
    *,
    slow_after: float = 10.0,
    busy_after: float = 30.0,
    hard_timeout: float = 90.0,
    slow_text: str = "⏳ Taking longer than usual — still working…",
    busy_text: str = "😮‍💨 The server seems slow or busy — still trying…",
):
    """Await `aw` but keep the user informed when it drags (V1.17.x).

    The rule this enforces: the user must never stare at a silent loader
    for more than ~30 seconds. Timeline:
        slow_after   → on_status(slow_text)    (~10s: "taking longer…")
        busy_after   → on_status(busy_text)    (~30s: "server busy…")
        hard_timeout → TimeoutError            (caller sends the give-up
                                                message + retry button)

    `on_status` is an async callable receiving the status text — callers
    pass something that edits their loader message (or sends a new one).
    Status failures are swallowed; the work itself is never interrupted
    by a messaging error. On hard timeout the task is cancelled — a
    threaded requests call keeps running in the background until its own
    socket timeout, but its result is discarded.
    """
    task = asyncio.ensure_future(aw)
    elapsed = 0.0
    for at, text in ((slow_after, slow_text), (busy_after, busy_text)):
        remaining = at - elapsed
        if remaining > 0:
            done, _ = await asyncio.wait({task}, timeout=remaining)
            if done:
                return task.result()
            elapsed = at
        try:
            await on_status(text)
        except Exception:  # noqa: BLE001 — status is best-effort
            pass
    done, _ = await asyncio.wait({task}, timeout=max(0.0, hard_timeout - elapsed))
    if done:
        return task.result()
    task.cancel()
    raise TimeoutError(f"no response after {int(hard_timeout)} seconds")


def _friendly_fetch_error(e: Exception) -> str:
    """Translate connection-ish exceptions into plain rep language.

    Reps see 'ConnectionError(MaxRetryError…)' as gibberish; tell them
    what actually matters — server unreachable / busy / timed out — and
    that retrying shortly is the fix.
    """
    blob = f"{type(e).__name__} {e}".lower()
    if "timeout" in blob or "timed out" in blob:
        return "the server took too long to respond (timeout) — it may be busy. Try again in a minute."
    if "connection" in blob or "getaddrinfo" in blob or "name resolution" in blob or "unreachable" in blob:
        return "I can't reach the server right now (connection problem) — it may be down or the network is flaky. Try again shortly."
    if "502" in blob or "503" in blob or "504" in blob or "bad gateway" in blob:
        return "the server is temporarily overloaded — try again in a minute."
    return h(str(e))


async def _close_catalog_codes(code: str, limit: int = 3) -> list[str]:
    """Near-miss catalog codes for a code that wasn't found anywhere.

    Backed by matcher.close_code_matches (edit distance ≤2, same prefix).
    Sheet errors just mean no suggestions — never let this helper break
    the not-found reply it decorates.
    """
    try:
        seasonings = await asyncio.to_thread(sheets.load_seasonings)
        catalog = {
            str(s.get("code", "")).strip().upper()
            for s in seasonings
            if s.get("code")
        }
        return [c for c, _d in matcher.close_code_matches(code, catalog, limit=limit)]
    except Exception as e:  # noqa: BLE001
        log.debug("close-code suggestions failed for %s: %s", code, e)
        return []


async def _run_pp_for_codes(update: Update, codes: list[str]) -> None:
    """Fetch /pp for each code, edit-in-place loader, audit-log every result.

    Used by `/pp <code>`, the ✏️ Enter a code menu flow, and the photo-
    scan flow. Caller passes already deduplicated, capped, uppercase
    codes.

    Always ends with a 'what next' footer (V1.12.5) so the conversation
    doesn't dead-end — same pattern /lastsample and 🔎 Search use.
    """
    if not codes:
        return
    client = mms_product.get_client()
    chat = update.effective_chat
    user = update.effective_user
    uname = (user.username or user.full_name or "") if user else ""
    uid = user.id if user else ""

    # V1.13.11 — resolve the rep's preferred currency override once per
    # /pp call (cached at the sheets layer so this is cheap). All price
    # lines below render through _format_price_for_currency so reps in
    # IDR-quoting or THB-quoting regions see prices in their currency.
    pref_currency = await _user_pref_currency(update)

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
            f"☕ Grab a tea. Loading <code>{h(code)}</code>…",
            parse_mode=ParseMode.HTML,
        )
        try:
            await chat.send_action("typing")
        except Exception:  # noqa: BLE001
            pass

        async def _replace(text: str, markup=None) -> None:
            try:
                await placeholder.edit_text(
                    text, parse_mode=ParseMode.HTML, reply_markup=markup
                )
            except Exception:  # noqa: BLE001 — message may be too old to edit
                await chat.send_message(
                    text, parse_mode=ParseMode.HTML, reply_markup=markup
                )

        # V1.17.x — single-code lookups get two bonus buttons under the
        # price: jump straight to the last sample of THIS code (own /
        # any rep). Batch pastes stay button-free — they're price sweeps.
        def _price_kb():
            if len(codes) != 1:
                return None
            return kb([
                [("📦 My last sample of this code", f"lsx:s:{code}")],
                [("🌐 Any rep's last sample", f"lsx:a:{code}")],
            ])

        # Helper: render an FSL-only reply (3 lines, no RMC). Used both
        # when MMS doesn't have the variant and when MMS returned a
        # parent code instead of the exact variant the user asked for.
        async def _reply_from_fsl(asked_code: str, fsl_row: dict) -> None:
            fsl_name = (fsl_row.get("Product Name") or "—").strip() or "—"
            fsl_price_raw = (fsl_row.get("R&D Price") or "").strip()
            # V1.13.11 — price rendered in the rep's preferred currency
            # when override is configured. _format_price_for_currency
            # handles bare-numeric and currency-prefixed inputs.
            fsl_price_display = _format_price_for_currency(
                fsl_price_raw, pref_currency, show_original=bool(pref_currency),
            ) if fsl_price_raw else "—"
            try:
                fsl_price_for_audit = float(fsl_price_raw)
            except ValueError:
                fsl_price_for_audit = None
            body = (
                f"<b>Code:</b> <code>{h(asked_code)}</code>\n"
                f"<b>Name:</b> {h(fsl_name)}\n"
                f"<b>R&amp;D Price:</b> {fsl_price_display}"
            )
            await _replace(body, markup=_price_kb())
            _audit(
                query=asked_code,
                result="Found (FSL)",
                matched_code=asked_code,
                name=fsl_name,
                rd_price_usd=fsl_price_for_audit,
                raw_material_cost_usd=None,
            )

        try:
            # V1.17.x — watchdog keeps the rep informed: status update at
            # ~10s and ~30s, hard give-up at 90s. No more silent stares at
            # "Grab a tea" while MMS hangs.
            product = await _await_with_status(
                asyncio.to_thread(client.fetch_product, code),
                on_status=_replace,
                slow_text=(
                    f"⏳ Still loading <code>{h(code)}</code>… "
                    "MMS is a bit slow right now."
                ),
                busy_text=(
                    f"😮‍💨 The MMS server seems <b>busy or slow</b> — still "
                    f"trying <code>{h(code)}</code>. I'll give up at 90 seconds…"
                ),
            )
        except TimeoutError:
            await _replace(
                f"🔌 MMS didn't answer after <b>90 seconds</b> for "
                f"<code>{h(code)}</code> — the server may be down or busy.\n"
                "Please try again in a minute.",
                markup=kb([[(f"🔄 Retry {code}", f"pp:{code}")],
                           [("🏠 Main menu", "menu:home")]]),
            )
            _audit(query=code, result="Timeout", error="MMS no response in 90s")
            continue
        except mms_product.ProductNotFound:
            # MMS has no record at all. Try FSL by exact code before giving up.
            fsl_row = await asyncio.to_thread(sheets.find_fsl_product_by_code, code)
            if fsl_row:
                await _reply_from_fsl(code, fsl_row)
                continue
            # V1.17.x — smarter not-found: suggest near-miss catalog codes
            # (one mistyped / misread character away) as tap-to-retry buttons.
            close = await _close_catalog_codes(code)
            if close:
                await _replace(
                    f"😕 No product found for <code>{h(code)}</code>.\n\n"
                    "🤔 <b>Did you mean one of these?</b> Closest codes in "
                    "the catalog — tap to try:",
                    markup=kb(
                        [[(f"🔁 {c}", f"pp:{c}")] for c in close]
                        + [[("🔎 Search seasonings", "menu:search"),
                            ("🏠 Main menu", "menu:home")]]
                    ),
                )
                _audit(query=code, result="Not Found")
                continue
            await _replace(f"😕 No product found for <code>{h(code)}</code>.")
            _audit(query=code, result="Not Found")
            continue
        except mms_product.MMSError as e:
            log.warning("MMS error for %s: %s", code, e)
            await _replace(
                f"😬 MMS error for <code>{h(code)}</code>: {h(str(e))}\n"
                "<i>Usually temporary — try again in a minute.</i>",
                markup=kb([[(f"🔄 Retry {code}", f"pp:{code}")]]),
            )
            _audit(query=code, result="MMS Error", error=str(e))
            continue
        except Exception as e:  # noqa: BLE001
            log.exception("Unexpected /pp error for %s", code)
            await _replace(
                f"😵 Couldn't fetch <code>{h(code)}</code>: "
                f"{_friendly_fetch_error(e)}",
                markup=kb([[(f"🔄 Retry {code}", f"pp:{code}")]]),
            )
            _audit(query=code, result="Error", error=str(e))
            continue

        # MMS returned a product. Did it return the EXACT code the user
        # asked for? MMS does prefix matching, so requesting
        # 'S-TXF06-00-03' can come back as 'S-TXF06-00' (the parent), and
        # the parent's R&D price is wrong for the variant. When that
        # happens, prefer FSL — it has the variant-specific price.
        asked = code.strip().upper()
        got = (product.code or "").strip().upper()
        if got != asked:
            fsl_row = await asyncio.to_thread(sheets.find_fsl_product_by_code, asked)
            if fsl_row:
                await _reply_from_fsl(asked, fsl_row)
                continue
            # else: fall through to the MMS reply; it'll show the parent
            # code, which the user can still cross-check.

        # Customer-facing Raw Material Cost rule:
        #   1) round UP to the next 0.10  (3.47622 → 3.50)
        #   2) add the standing markup    (+ config.RMC_MARKUP_USD = 0.30)
        # Result: a stable, .2f USD figure we quote to sales. Same value goes
        # to the user-visible reply AND the Query-tab audit row, so display
        # and audit log can never disagree.
        # Decimal (not math.ceil on a float) avoids cases where 3.40 stored as
        # 3.40000000001 would round up to 3.50 by mistake.
        raw_dec = Decimal(str(product.raw_material_cost_usd))
        rounded_up = float(raw_dec.quantize(Decimal("0.1"), rounding=ROUND_CEILING))
        adj_rmc = rounded_up + config.RMC_MARKUP_USD

        # R&D price — V1.12.4: fall back to the FSL/Jakarta tab when MMS
        # has no R&D price set. Common on J-codes (Indonesia factory's
        # MMS doesn't always carry R&D pricing for older formulations)
        # and on legacy B-codes. The sample tab has whatever price was
        # recorded when the sample was last raised, which is usually
        # close enough to the current quote — better than showing "—".
        rd_from_fsl_raw = ""
        if product.rd_price_usd is None:
            try:
                fsl_row = await asyncio.to_thread(
                    sheets.find_fsl_product_by_code, asked,
                )
                rd_from_fsl_raw = (
                    (fsl_row.get("R&D Price") or "").strip() if fsl_row else ""
                )
            except Exception as e:  # noqa: BLE001
                log.warning("/pp FSL fallback for %s failed: %s", asked, e)

        # V1.13.11 — both prices honour the rep's currency override.
        # rd_line: MMS price (when set) is canonical USD; FSL fallback
        # can be in any currency the sheet stores. Both go through the
        # same formatter so the conversion behaviour is consistent.
        #
        # V1.17.x — when the MMS3 SR page is already in the rep's
        # preferred currency (J- codes are usually IDR, B- codes THB),
        # bypass the USD round-trip entirely so the display is EXACTLY
        # the figure MMS3 shows. Otherwise fall back to the formatter,
        # which now uses MMS3's exchange rate for conversion.
        native_amount = product.rd_price_native_amount
        native_cur = (product.rd_price_native_currency or "").upper()
        if (
            native_amount is not None
            and native_cur
            and pref_currency
            and native_cur == pref_currency.upper()
        ):
            dp = 0 if native_cur == "IDR" else 2
            rd_line = f"{native_cur} {native_amount:,.{dp}f}"
        elif product.rd_price_usd is not None:
            rd_line = _format_price_for_currency(
                f"USD {product.rd_price_usd:.2f}", pref_currency,
                show_original=bool(pref_currency),
            )
        elif rd_from_fsl_raw:
            rd_line = _format_price_for_currency(
                rd_from_fsl_raw, pref_currency,
                show_original=bool(pref_currency),
            ) + " <i>(last sampled)</i>"
        else:
            rd_line = "—"

        # Raw Material Cost stays in USD when no override; otherwise
        # gets converted same as R&D Price. RMC is what reps quote to
        # customers — keeping it in the customer's currency matters.
        rmc_line = _format_price_for_currency(
            f"USD {adj_rmc:.2f}", pref_currency,
            show_original=bool(pref_currency),
        )

        body = (
            f"<b>Code:</b> <code>{h(product.code)}</code>\n"
            f"<b>Name:</b> {h(product.name)}\n"
            f"<b>R&amp;D Price:</b> {rd_line}\n"
            f"<b>Raw Material Cost:</b> {rmc_line}"
        )
        await _replace(body, markup=_price_kb())
        # For audit, log the FSL fallback as a numeric only if it parses
        # cleanly to a float — non-USD currency text isn't comparable, so
        # leave it None there.
        rd_for_audit = product.rd_price_usd
        if rd_for_audit is None and rd_from_fsl_raw:
            try:
                rd_for_audit = float(rd_from_fsl_raw)
            except ValueError:
                rd_for_audit = None
        _audit(
            query=code,
            result="Found" if product.rd_price_usd is not None else "Found (FSL fallback)",
            matched_code=product.code,
            name=product.name,
            rd_price_usd=rd_for_audit,
            raw_material_cost_usd=adj_rmc,
        )

    # V1.12.5: 'what next' footer. The price reply for each code is an
    # edited placeholder, not a fresh message, so without this footer the
    # rep is left staring at a price with no obvious action.
    # V1.17.x — typing-first: the old '✏️ Look up another code' button
    # only armed a typing flag; the smart router made it redundant, so
    # the footer now TEACHES typing and keeps just the two taps that do
    # something typing can't.
    try:
        await update.effective_chat.send_message(
            "✅ Done — <i>type another code, a customer, or a rep name "
            "anytime.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb([
                [("📷 Scan a photo", "menu:scan"),
                 ("🏠 Main menu", "menu:home")],
            ]),
        )
    except Exception as e:  # noqa: BLE001 — chat may have been closed; not worth retrying
        log.debug("/pp footer send failed: %s", e)


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
    cap = _pp_cap_for(update.effective_user)
    unique = _dedupe_codes(codes, cap=cap)
    # Audit fix #11 — only flash the "max N codes" warning when the cap
    # actually trimmed the run. Before: typing /pp S-1 S-1 S-1 S-1 S-1 S-1
    # (six duplicates of the same code) triggered the warning even though
    # _dedupe_codes already collapsed it to 1 unique code, producing the
    # misleading "running first 5: S-1". Now we compare the deduped count
    # to the cap, not the raw token count.
    if len(unique) >= cap and len(codes) > cap:
        await send(
            update,
            f"🙏 Max {cap} codes per /pp — running first {cap}: "
            f"{', '.join(unique)}",
        )
    await _run_pp_for_codes(update, unique)


async def cmd_scan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """`/scan` — prompt the user to send a photo of product code(s)."""
    if not await _authorized(update):
        return
    ctx.user_data["awaiting_scan_photo"] = True
    await send(
        update,
        "📷 <b>Scan a product photo</b>\n\n"
        "<b>📎 Reply to this message</b> with a photo of one or more "
        "product code labels (<code>S-XXXXX-XX</code>). I'll read them and "
        "pull the price for each.\n\n"
        "💡 <b>Codes tiny or far away? Send it as a FILE</b> "
        "(📎 → <i>File</i>) — Telegram shrinks normal photos to ~1280px and "
        "small print stops being readable. As a file I get your "
        "full-resolution original.",
    )


async def on_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle a photo upload — OCR for product codes, then auto-/pp each one.

    Accepts BOTH:
      • a normal photo  — Telegram re-encodes these and caps the long edge
        at ~1280px. A crisp 4000px gallery original arrives downsampled, and
        on a wide shot of several sachets each printed code lands ~20px tall
        — far too small to read. This is THE usual cause of "couldn't spot a
        product code" on a photo that looks perfectly sharp in the gallery.
      • an image sent as a FILE/document (V1.17.3) — Telegram passes those
        through UNCOMPRESSED, so we get the rep's full-resolution original
        and small codes stay legible.

    GATED: only fires if the user explicitly opted into a scan, via either:
      a) ctx.user_data["awaiting_scan_photo"] flag (set by /scan or the
         main-menu Scan button on THIS process), or
      b) the photo is a reply to one of our "📷 Scan a product photo"
         prompts (works across processes — useful when Railway runs more
         than one replica and the click + photo land on different workers).

    Without this gate, every photo any user posts in a group chat would
    trigger OCR (token spam, scan-result spam).
    """
    if not await _authorized(update):
        return
    msg = update.effective_message
    if not msg:
        return
    # An image sent as a file arrives as a document, not a photo.
    doc = getattr(msg, "document", None)
    doc_is_image = bool(
        doc and (doc.mime_type or "").lower().startswith("image/")
    )
    if not msg.photo and not doc_is_image:
        return
    has_flag = bool(ctx.user_data.pop("awaiting_scan_photo", None))
    replied = msg.reply_to_message
    is_scan_reply = bool(
        replied
        and getattr(replied, "from_user", None)
        and getattr(replied.from_user, "is_bot", False)
        and "Scan a product photo" in (replied.text or "")
    )
    # V1.17.5 — in a DM, just scan it. Sending the bot a photo of a label has
    # exactly one plausible meaning, so demanding a button tap or a reply
    # first was pure friction (the same reasoning that made bare typed text
    # auto-route). The gate still applies in GROUPS, where every holiday snap
    # would otherwise trigger an OCR run and a wall of replies.
    is_private = (update.effective_chat.type == ChatType.PRIVATE)
    if not (has_flag or is_scan_reply or is_private):
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
        if doc_is_image:
            # Sent as a FILE — Telegram stores it byte-for-byte, so this is
            # the rep's untouched original. Best possible input for OCR.
            src_id, was_compressed = doc.file_id, False
        else:
            # Sent as a PHOTO — [-1] is the largest of Telegram's re-encoded
            # sizes, still capped near 1280px on the long edge.
            src_id, was_compressed = msg.photo[-1].file_id, True
        tg_file = await ctx.bot.get_file(src_id)
        buf = await tg_file.download_as_bytearray()
    except Exception as e:  # noqa: BLE001
        log.exception("Photo download failed")
        await _cleanup()
        await send(update, f"😕 Couldn't read that photo: {_friendly_fetch_error(e)}")
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

    async def _notice_status(text: str) -> None:
        try:
            await notice.edit_text(text, parse_mode=ParseMode.HTML)
        except Exception:  # noqa: BLE001
            pass

    try:
        # V1.17.x — watchdog: status update at ~12s and ~30s, give up at
        # 75s, so a hung OCR/vision call never leaves the rep in silence.
        result = await _await_with_status(
            vision_scan.scan_image(bytes(buf), catalog_codes),
            on_status=_notice_status,
            slow_after=12,
            busy_after=30,
            hard_timeout=75,
            slow_text=(
                "🔍 Still reading your photo… double-checking the "
                "characters with AI vision."
            ),
            busy_text=(
                "😮‍💨 The vision service is slow right now — still "
                "working on your photo (giving up at 75s)…"
            ),
        )
    except TimeoutError:
        await _cleanup()
        await send(
            update,
            "🔌 Photo reading timed out after <b>75 seconds</b> — the "
            "vision service may be busy. Please send the photo again in "
            "a minute.",
        )
        return
    except Exception as e:  # noqa: BLE001
        log.exception("OCR failed")
        await _cleanup()
        await send(update, f"😵 OCR failed: {_friendly_fetch_error(e)}")
        return

    # OCR done — drop the loading notice AND the user's original photo so
    # the group chat doesn't accumulate clutter.
    await _cleanup()

    if not result.codes:
        # V1.17.3 — explain WHY, using the resolution we actually received.
        # The usual culprit isn't lighting: Telegram re-encodes photos down
        # to ~1280px, so a wide shot of several sachets leaves each code a
        # handful of pixels tall. Sending the same image as a FILE bypasses
        # that entirely, so lead with the fix that actually works.
        dims = ""
        try:
            import io as _io

            from PIL import Image as _Image

            with _Image.open(_io.BytesIO(bytes(buf))) as _im:
                w, hgt = _im.size
            dims = f" (I received {w}×{hgt}px)"
        except Exception:  # noqa: BLE001 — diagnostics only
            pass

        bits = [f"🙈 Couldn't spot a product code in that photo{dims}."]
        if was_compressed:
            bits += [
                "",
                "📉 <b>Telegram shrank it.</b> Photos get re-encoded to about "
                "1280px wide, so small printed codes turn to mush — even "
                "though the original looks sharp in your gallery.",
                "",
                "✅ <b>Best fix — send it as a file:</b>",
                "   <i>Attach (📎) → File → pick the photo</i>",
                "   (on iPhone: 📎 → File → Browse → Photos)",
                "That sends your <b>full-resolution original</b>, which I can "
                "actually read.",
                "",
                "📷 <b>Or:</b> photograph <b>fewer sachets up close</b> — one "
                "or two per shot beats seven in one frame.",
            ]
        else:
            bits += [
                "",
                "I got your full-resolution file, so the codes may be too "
                "small or blurry in frame. Try a <b>closer shot of fewer "
                "sachets</b>, or type the code instead.",
            ]
        bits += ["", "<i>Codes look like <code>S-XXXXX-XX</code> or "
                     "<code>S-XXXXXX</code>. You can also just type one.</i>"]
        # V1.17.5 — show which engines ran and what each returned. Railway's
        # logs aren't reachable from the dev box, so without this a scan that
        # found nothing because an OCR engine failed to load looks exactly
        # like a scan that found nothing because the photo was poor.
        if result.attempts:
            bits += [
                "",
                "<i>🔧 OCR engines: " + h(" · ".join(result.attempts)) + "</i>",
            ]
        await send(update, "\n".join(bits))
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
    # V1.17.x — near-miss suggestions for codes we couldn't confidently
    # heal (ties / implausible edits). Shown as tap-to-lookup buttons so
    # a misread never dead-ends the rep.
    sugg_buttons: list[list[tuple[str, str]]] = []
    for raw_code, cands in result.suggestions.items():
        if not cands:
            continue
        lines.append(
            f"🤔 <code>{h(raw_code)}</code> is close to: "
            + ", ".join(f"<code>{h(c)}</code>" for c in cands[:3])
            + " — tap below to check one."
        )
        for c in cands[:3]:
            sugg_buttons.append([(f"🔁 {c}", f"pp:{c}")])
    lines.append("\n☕ Grab a tea. Loading…")
    await send(update, "\n".join(lines), kb(sugg_buttons) if sugg_buttons else None)

    # Cap at 5 to match the /pp ceiling and avoid spamming MMS.
    unique = _dedupe_codes(result.codes, cap=_pp_cap_for(update.effective_user))
    await _run_pp_for_codes(update, unique)


# ---------- voice -> /pp (V1.13.8) -----------------------------------------
#
# Handsfree code lookup. Rep records a Telegram voice message saying a
# product code like "S-668U1" or "B 74 CH7 dash 02". The bot:
#   1) downloads the OGG/Opus voice file from Telegram CDN
#   2) sends it to Groq Whisper (whisper-large-v3-turbo) for transcription
#   3) normalises common Whisper artefacts (spelled-out "dash", spaces
#      between letters/digits, lowercase)
#   4) regex-matches product codes
#   5) routes any code hits straight to /pp
#
# Why voice → /pp only (not search): per user spec, voice is for fast on-
# the-fly price lookups when reps are walking the factory floor or driving.
# Free-text search by voice is a more open UX problem (region picker,
# refinement) and lives behind the typed keyboard for now.

def _normalise_voice_for_codes(text: str) -> str:
    """Rewrite Whisper output so existing _PP_CODE_RE catches codes.

    Whisper output for product codes is messy because the model treats
    them as natural language. Common artefacts we patch:
      • filler words ('find', 'search', 'look up', 'show me', etc.)
        stripped so they don't fragment the code pattern
      • spelled-out 'dash' instead of the literal '-'
      • spaces between letters and digits ('S 668 U 1' → 'S-668U1')
      • lowercase prefix letter
      • trailing punctuation like '.' or ','

    Doesn't replace _PP_CODE_RE — the regex still runs against the
    original transcription too, so a clean read still matches.
    """
    if not text:
        return ""
    t = text
    # V1.13.10 — strip filler words so "Find x-d2t43" → "x-d2t43" and
    # the loose extractor can phonetically coerce the prefix afterwards.
    # Kept conservative: only verbs/phrases that wouldn't appear inside
    # a product name.
    filler_patterns = [
        r"\bfind\b", r"\bsearch(?:ing|es)?\b", r"\blook(?:ing)?\s+up\b",
        r"\blook(?:ing)?\s+for\b", r"\bshow\s+(?:me|us)\b",
        r"\btell\s+(?:me|us)\b",
        r"\bwhat'?s?\b", r"\bwhat\s+(?:is|are|was|were)\b",
        r"\bcheck\b", r"\bpull\b", r"\bfetch\b", r"\bget\s+me\b",
        r"\bfor\s+me\b", r"\bplease\b", r"\bcan\s+you\b",
        r"\bthe\s+code\b", r"\bproduct\s+code\b",
        r"\bis\b", r"\bare\b",  # leftover linking verbs after 'what'
    ]
    for f in filler_patterns:
        t = re.sub(f, " ", t, flags=re.IGNORECASE)
    # Replace literal 'dash' / 'minus' / 'hyphen' tokens with '-'.
    t = re.sub(r"\b(dash|minus|hyphen)\b", "-", t, flags=re.IGNORECASE)
    # Collapse spaces around a hyphen so 'S - 668' becomes 'S-668'.
    t = re.sub(r"\s*-\s*", "-", t)
    # Squish single spaces between alphanumeric chars so 'S 668 U 1'
    # becomes 'S668U1'. Then a follow-up pass inserts a '-' after the
    # prefix letter if the model dropped the hyphen.
    t = re.sub(r"(?<=[A-Za-z0-9])\s+(?=[A-Za-z0-9])", "", t)
    t = re.sub(r"\b([SJTB])(?=[A-Za-z0-9])", r"\1-", t, flags=re.IGNORECASE)
    # Remove duplicate hyphens that the rewrite can introduce
    # ('S--668U1' → 'S-668U1').
    t = re.sub(r"-{2,}", "-", t)
    return t


# V1.13.10 — phonetic mapping from misheard first letters back to S/B/J.
# Codes always start with one of S/B/J/T (T is rare/legacy). Whisper
# routinely mishears the prefix because it's a single short vowel-y
# sound, so we coerce the first letter when the loose extractor caught
# a code-shape with an off-prefix.
#
# Grouped by phonetic similarity:
#   • S group: ess / ex / zee — all sibilant or vowel + 's' sounds
#   • B group: plosives (B/D/P/V/T/G) which sound alike in noisy audio
#   • J group: 'jay' / 'kay' / 'gay' / 'yay' — soft consonants
# Letters not in any group get ALL three prefixes tried.
_VOICE_PREFIX_MAP: dict[str, tuple[str, ...]] = {
    # Already-correct prefixes pass through unchanged.
    "S": ("S",), "J": ("J",), "B": ("B",), "T": ("T",),
    # S-likely
    "X": ("S",), "Z": ("S",), "C": ("S",), "F": ("S",),
    "E": ("S",),  # 'ess' / 'ex'
    # B-likely
    "D": ("B",), "P": ("B",), "V": ("B",), "G": ("B",),
    # J-likely
    "K": ("J",), "Q": ("J",), "Y": ("J",),
    # Ambiguous fallback: try all 3
}
_VOICE_PREFIXES_FALLBACK = ("S", "B", "J")
# Loose code-shape regex: any single letter, optional hyphen, then 3+
# alphanumerics, optionally extended by 0–6 hyphen-separated 1–6 char
# groups. Mirrors _PP_CODE_RE's structure but lets the first letter be
# anything (we'll coerce it).
_VOICE_LOOSE_CODE_RE = re.compile(
    r"\b([A-Z])-?([A-Z0-9]{3,}(?:-[A-Z0-9]{1,6}){0,6})\b",
    re.IGNORECASE,
)


def _voice_extract_candidates(text: str) -> list[str]:
    """Return a list of candidate product codes from voice transcription.

    Strategy:
      1. Run _PP_CODE_RE against the raw text and the normalised text.
         If anything matches strictly, use those (high-confidence).
      2. Otherwise run the loose pattern over the normalised text and
         phonetically coerce the first letter to S/B/J/T. Each match
         can yield multiple candidates if the original letter is
         ambiguous (e.g. unmapped letter → try all three).
      3. Cap at 5 candidates total, dedupe.

    The on_voice handler will validate candidates against the FSL
    before showing /pp results, so the false-positive bar here is OK
    to be loose — non-existent codes get filtered out before MMS calls.
    """
    if not text:
        return []
    normalised = _normalise_voice_for_codes(text)

    strict = _PP_CODE_RE.findall(text) + _PP_CODE_RE.findall(normalised)
    if strict:
        seen: set[str] = set()
        out: list[str] = []
        for c in strict:
            u = c.upper()
            if u not in seen:
                seen.add(u)
                out.append(u)
            if len(out) >= 5:
                break
        return out

    candidates: list[str] = []
    seen = set()
    for m in _VOICE_LOOSE_CODE_RE.finditer(normalised):
        first = m.group(1).upper()
        body = m.group(2).upper()
        # Real product codes always contain at least one digit in the
        # body (e.g. S-43EH1, B-74CH7-02). Without this guard, random
        # alphabetic words ('chatter') become phonetic candidates.
        if not any(ch.isdigit() for ch in body):
            continue
        prefixes = _VOICE_PREFIX_MAP.get(first, _VOICE_PREFIXES_FALLBACK)
        for p in prefixes:
            cand = f"{p}-{body}"
            if cand not in seen:
                seen.add(cand)
                candidates.append(cand)
            if len(candidates) >= 5:
                return candidates
    return candidates


async def on_voice(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Telegram voice message → Groq STT → product-code regex → /pp."""
    if not await _authorized(update):
        return
    msg = update.effective_message
    if not msg or not msg.voice:
        return

    if not config.GROQ_API_KEY:
        await send(
            update,
            "🎤 Voice messages aren't enabled yet — admin needs to set "
            "<code>GROQ_API_KEY</code> in Railway. For now, please type "
            "the code.",
        )
        return

    # Friendly "I heard you" so the rep knows the bot is working on it.
    # Whisper turnaround is usually <2s, but Telegram CDN download +
    # network can add a beat. Match the existing /scan loading vibe.
    try:
        await update.effective_chat.send_action("typing")
    except Exception:  # noqa: BLE001
        pass
    placeholder = await update.effective_chat.send_message(
        "🎤 <i>Listening… ☕ Grab a tea.</i>",
        parse_mode=ParseMode.HTML,
    )

    # Download the OGG voice file straight into memory — no temp file.
    try:
        tg_file = await msg.voice.get_file()
        audio_buf = io.BytesIO()
        await tg_file.download_to_memory(out=audio_buf)
        audio_bytes = audio_buf.getvalue()
    except Exception as e:  # noqa: BLE001
        log.exception("voice: download failed")
        await placeholder.edit_text(
            f"😬 Couldn't download the voice clip: {h(str(e))}",
            parse_mode=ParseMode.HTML,
        )
        return

    if not audio_bytes:
        await placeholder.edit_text(
            "🤔 Voice clip looked empty. Try again?",
            parse_mode=ParseMode.HTML,
        )
        return

    # Send to Groq Whisper.
    try:
        text = await groq_voice.transcribe_ogg(audio_bytes)
    except groq_voice.GroqError as e:
        log.warning("voice: Groq error: %s", e)
        await placeholder.edit_text(
            f"😬 Speech-to-text failed: {h(str(e))}",
            parse_mode=ParseMode.HTML,
        )
        return
    except Exception as e:  # noqa: BLE001
        log.exception("voice: unexpected STT error")
        await placeholder.edit_text(
            f"😵 Unexpected error during transcription: {h(str(e))}",
            parse_mode=ParseMode.HTML,
        )
        return

    if not text:
        await placeholder.edit_text(
            "🤔 I didn't catch anything in the voice clip — try again with "
            "the code spoken clearly, e.g. 'S dash six six eight U one'.",
            parse_mode=ParseMode.HTML,
        )
        return

    # V1.13.10 — smart candidate extraction. Fillers stripped, loose
    # pattern + phonetic prefix coercion. If the extractor returns
    # multiple candidates (e.g. ambiguous first letter), we validate
    # each against the FSL and only run /pp on the ones that actually
    # exist — keeps the reply clean instead of three 'not found' lines.
    candidates = _voice_extract_candidates(text)
    if not candidates:
        await placeholder.edit_text(
            f"🎤 I heard: <b>{h(text)}</b>\n\n"
            "But I couldn't pull out anything code-shaped. Try again "
            "with the code spoken clearly — e.g. <code>S 668 U 1</code> "
            "or <code>B dash 74 CH7 dash 02</code>.",
            parse_mode=ParseMode.HTML,
        )
        return

    # Filter candidates: keep only those that exist in the FSL.
    valid: list[str] = []
    for cand in candidates:
        try:
            row = await asyncio.to_thread(sheets.find_fsl_product_by_code, cand)
            if row:
                valid.append(cand)
        except Exception as e:  # noqa: BLE001 — FSL read shouldn't kill voice
            log.warning("voice: FSL lookup failed for %s: %s", cand, e)

    if not valid:
        # No candidate found in FSL. Show what we tried so the rep can
        # eyeball whether Whisper just butchered the code.
        tried = ", ".join(f"<code>{h(c)}</code>" for c in candidates[:5])
        await placeholder.edit_text(
            f"🎤 I heard: <b>{h(text)}</b>\n"
            f"→ Tried: {tried}\n\n"
            "None of these match a known product. Try again with the "
            "code spoken clearly?",
            parse_mode=ParseMode.HTML,
        )
        return

    unique = _dedupe_codes(valid, cap=_pp_cap_for(update.effective_user))
    await placeholder.edit_text(
        f"🎤 Heard: <b>{h(text)}</b>\n"
        f"→ Looking up {', '.join(f'<code>{h(c)}</code>' for c in unique)}…",
        parse_mode=ParseMode.HTML,
    )
    await _run_pp_for_codes(update, unique)


# ---------- /lastsample ----------------------------------------------------
#
# Sales rep workflow: "what was the last sample I sent matching X?"
#
# The Authorized Users tab carries an "MMS Name" column that maps the user's
# Telegram identity → the name MMS records against the sample (e.g. "Alex",
# "Joycelyn"). We use that to filter Full Sample Listing to just THIS rep's
# rows, then fuzzy-match the user's keywords against Product Name + Code +
# Taste describe, and return the row with the latest Sample Date Out.
#
# Same one-shot prompt → reply pattern as /scan and the manual code-entry
# button: set a flag in ctx.user_data, the user's next text message is
# treated as the search query.


async def cmd_lastsample(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """`/lastsample [keyword]` — find the most recent sample this rep sent.

    Two ways to use it:
      - ``/lastsample`` alone → bot prompts; user replies with a keyword.
      - ``/lastsample asian thai`` → search runs immediately. This bypasses
        the reply step entirely, which matters when Railway runs more than
        one worker (the in-memory awaiting flag wouldn't survive a worker
        switch, but a single-shot command always lands on the worker that
        also processes its body).
    """
    if not await _authorized(update):
        return
    user = update.effective_user
    mms_name = await asyncio.to_thread(sheets.get_user_mms_name, user.id, user.username)
    if not mms_name:
        await send(
            update,
            "🛑 <b>Your MMS name isn't set yet.</b>\n\n"
            "Ask the admin to fill in the <b>MMS Name</b> column for your row "
            "in the <i>Authorized Users</i> tab — that's the name MMS records "
            "against your samples (e.g. <code>Alex</code>, <code>Joycelyn</code>). "
            "Once it's set, /lastsample can find your past samples.\n\n"
            "<i>(I just refreshed the user list and still don't see an MMS Name "
            "for you. Double-check that your row's <b>Active</b> column is "
            "<code>Y</code> and that <b>MMS Name</b> isn't blank.)</i>",
        )
        return

    # Inline-args shortcut: '/lastsample asian thai' goes straight to search.
    # Falls back to the prompt-and-reply flow when no arg is given.
    inline = " ".join(ctx.args).strip() if ctx.args else ""
    if inline:
        ctx.user_data["lastsample_mms_name"] = mms_name
        ctx.user_data["lastsample_active_query"] = ""
        await _run_lastsample_search(update, ctx, mms_name, inline, prev="")
        return

    ctx.user_data["awaiting_lastsample_query"] = True
    ctx.user_data["lastsample_mms_name"] = mms_name
    # Reset accumulated query — every fresh /lastsample (or 🔎 Find another)
    # starts the refinement chain over from blank. Scope locked to 'self'
    # so any subsequent text/refinement is rep-scoped.
    ctx.user_data["lastsample_active_query"] = ""
    ctx.user_data["lastsample_scope"] = "self"
    sync_footer = await _last_sync_footer()
    sync_tail = f"\n\n<i>{sync_footer}</i>" if sync_footer else ""
    # ForceReply pops Telegram's reply-input UI so the user's next message
    # auto-attaches as a Reply to this prompt. Critical for group chats:
    # without it, a plain text reply gets eaten by privacy mode or lost
    # across worker switches.
    #
    # force_new=True is mandatory: when this command is reached via the
    # 'What I send ah?' button (callback_query), send() would otherwise
    # try to edit_message_text(...), which silently drops ForceReply.
    # selective=False so the reply UI works regardless of whether the
    # caller is @mentioned in the prompt — Telegram's targeting rules
    # are inconsistent for callback-driven messages.
    await send(
        update,
        "🔎 <b>Find your last sample</b>\n\n"
        f"You're set up as <b>{h(mms_name)}</b> in MMS.\n\n"
        "<b>📎 Reply to this message</b> with a product name or keyword "
        "(e.g. <i>BBQ</i>, <i>tom yum</i>, <i>S-668</i>). I'll show your "
        "<b>10 most recent matches</b> from <i>Full Sample Listing</i>, "
        "with Next-page buttons if you have more.\n\n"
        "<i>Tips:</i>\n"
        "<i>  • Type more words to narrow further (e.g. add a region or flavour).</i>\n"
        "<i>  • Spelling flexible — I handle small typos and missing spaces.</i>\n"
        "<i>  • Or skip this prompt: </i><code>/lastsample asian thai</code><i> in one go.</i>"
        f"{sync_tail}",
        ForceReply(
            selective=False,
            input_field_placeholder="e.g. tom yum, S-668, or customer name",
        ),
        force_new=True,
    )


async def cmd_alllastsample(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """`/alllastsample [keyword]` — same flow as /lastsample but searches
    across ALL reps' samples, not just the caller's.

    V1.10.2: opened up to every authorized user (was admin-only in V1.10.x).
    Sales reps can now look up what colleagues sent to a shared customer or
    which variants of a product have been quoted before.
    """
    if not await _authorized(update):
        return

    inline = " ".join(ctx.args).strip() if ctx.args else ""
    if inline:
        # No prompt step; run directly. mms_name is irrelevant in 'all' scope
        # — _run_lastsample_search ignores it when scope='all'.
        ctx.user_data["lastsample_scope"] = "all"
        ctx.user_data["lastsample_active_query"] = ""
        await _run_lastsample_search(
            update, ctx, mms_name="", query=inline, prev="", scope="all",
        )
        return

    ctx.user_data["awaiting_lastsample_query"] = True
    ctx.user_data["lastsample_scope"] = "all"
    ctx.user_data["lastsample_active_query"] = ""
    sync_footer = await _last_sync_footer()
    sync_tail = f"\n\n<i>{sync_footer}</i>" if sync_footer else ""
    # ForceReply + force_new — same fix as cmd_lastsample. Without
    # force_new, a callback-tap entry edits the menu message instead of
    # sending fresh, and ForceReply gets silently dropped (Telegram's
    # edit_message_text only honours InlineKeyboardMarkup).
    await send(
        update,
        "🌐 <b>Find ANY rep's last sample</b>\n\n"
        "This searches <b>all reps' samples</b> in Full Sample Listing — "
        "not just yours.\n\n"
        "<b>📎 Reply to this message</b> with a product name or keyword "
        "(e.g. <i>BBQ</i>, <i>tom yum</i>, <i>S-668</i>) or a customer name. "
        "I'll show the <b>10 most recent matches</b>, who sent each, and "
        "Next-page buttons for older results.\n\n"
        "<i>Tips:</i>\n"
        "<i>  • Type more words to narrow further.</i>\n"
        "<i>  • Spelling flexible — I handle small typos and missing spaces.</i>\n"
        "<i>  • Or skip this prompt: </i><code>/alllastsample q land</code><i> in one go.</i>"
        f"{sync_tail}",
        ForceReply(
            selective=False,
            input_field_placeholder="e.g. q land, tom yum, S-668",
        ),
        force_new=True,
    )


# =====================================================================
# V1.12.0 — Browse-only seasoning search (Search seasonings menu button)
# =====================================================================
#
# Decoupled from the existing 'Find a seasoning & raise request' flow:
# this search is purely exploratory (no draft, no sample request raised).
# Asks the rep which factory's catalog to browse — Singapore (S-codes),
# Indonesia (J-codes), or Thailand (T-codes, when ready) — then accepts
# free text or a product code. Code-prefix auto-detection bypasses the
# region picker entirely (a J-49JS1-03 query routes straight to /pp).
#
# Data source per region:
#   SG → 'Full Sample Listing' (FSL_TAB), filtered to last 36 months
#   ID → 'Full Sample Listing Jakarta' (JAKARTA_FSL_TAB), last 36 months
#   TH → 'Full Sample Listing Thailand' (BANGKOK_FSL_TAB), last 36 months
#
# Why sample history (not Seasoning Master): user prefers showing
# 'what's actually been sampled recently' over 'what's in the curated
# catalog', because Indonesia/Thailand don't have curated catalogs yet
# and this gives a uniform UX across regions.
_SEARCH_TOP_N = 10
_SEARCH_RECENT_MONTHS = 36


_REGION_TAB = {
    "sg": ("🇸🇬 Singapore", lambda: sheets.FSL_TAB),
    "id": ("🇮🇩 Indonesia", lambda: sheets.JAKARTA_FSL_TAB),
    "th": ("🇹🇭 Thailand",  lambda: sheets.BANGKOK_FSL_TAB),
}


# V1.13.3 — country / cuisine awareness in seasoning search. When the rep
# types a country name (e.g. 'malaysia'), surface samples whose Country
# column matches AND samples whose Product Name contains that country's
# signature cuisine keywords. Fixes the long-standing complaint that
# 'malaysia' returned 'mala chicken' (false fuzzy match) instead of
# nasi lemak / satay / kaya / super ring etc.
#
# Curated keyword lists — kept short and high-signal. Multi-word entries
# match as a substring of the lowercased product name (so 'nasi lemak'
# matches 'NASI LEMAK SEASONING' but not 'NASI'-only products).
# Tags use word-boundary matching so 'thai' doesn't match 'thaitex' or
# similar incidental substrings.
_COUNTRY_CUISINE: dict[str, dict] = {
    "malaysia": {
        "tags": ("malaysia", "malaysian"),
        "country_match": ("malaysia",),
        "cuisine": (
            "nasi lemak", "satay", "laksa", "rendang", "kaya",
            "sambal", "kway teow", "char kway", "ondeh", "chendol",
            "super ring", "mamak", "teh tarik", "ayam masak",
            "asam pedas", "curry puff", "mee goreng", "mee rebus",
            "bak kut teh", "rojak", "hainanese",
        ),
    },
    "indonesia": {
        "tags": ("indonesia", "indonesian"),
        "country_match": ("indonesia",),
        "cuisine": (
            "rendang", "soto", "gado", "sambal", "mie goreng",
            "kerupuk", "krupuk", "ayam goreng", "bakmi", "nasi goreng",
            "bakso", "rawon", "gulai", "tempe", "balado",
        ),
    },
    "thailand": {
        "tags": ("thailand", "thai"),
        "country_match": ("thailand",),
        "cuisine": (
            "tom yum", "pad thai", "tom kha", "som tum",
            "larb", "kaeng", "massaman", "panang",
            "khao soi", "phad", "thai basil", "tom yam",
        ),
    },
    "vietnam": {
        "tags": ("vietnam", "vietnamese"),
        "country_match": ("vietnam",),
        "cuisine": (
            "pho", "banh", "bun bo", "goi cuon", "nem", "vermicelli",
        ),
    },
    "philippines": {
        "tags": ("philippines", "filipino", "pilipino"),
        "country_match": ("philippines",),
        "cuisine": (
            "adobo", "sinigang", "lechon", "kare kare", "lumpia",
            "tapa", "longganisa",
        ),
    },
    "singapore": {
        "tags": ("singapore", "singaporean"),
        "country_match": ("singapore",),
        "cuisine": (
            "chilli crab", "hainanese", "bak kut teh", "kway teow",
            "char kway", "laksa", "rojak", "kaya",
        ),
    },
}


def _detect_country_query(q_lower: str) -> tuple[str, dict] | None:
    """Return (country_key, info) if `q_lower` mentions a recognised
    country tag with word-boundary match. Word-boundary is critical so
    'malaysia' doesn't trigger 'mala' and vice versa."""
    if not q_lower:
        return None
    for country, info in _COUNTRY_CUISINE.items():
        for tag in info["tags"]:
            if re.search(rf"\b{re.escape(tag)}\b", q_lower):
                return (country, info)
    return None


async def _start_seasoning_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Entry point for the 🔎 Search seasonings flow. Shows region picker."""
    # Wipe any prior search context — picker should always start clean.
    # V1.13.7: also clears the refinement chain so 'Search again' truly
    # starts fresh, not on top of the previous query.
    ctx.user_data.pop("awaiting_search_query", None)
    ctx.user_data.pop("search_region", None)
    ctx.user_data.pop("last_search_query", None)
    ctx.user_data.pop("last_search_region", None)
    btns = kb([
        [("🇸🇬 Singapore (S-codes)", "srch:reg:sg")],
        [("🇮🇩 Indonesia (J-codes)", "srch:reg:id")],
        [("🇹🇭 Thailand (B-codes)", "srch:reg:th")],
        [("🏠 Main menu", "menu:home")],
    ])
    await send(
        update,
        "🔎 <b>Search seasonings</b>\n\n"
        "Which factory's sample list to search?\n\n"
        "<i>Tip: if you already know the product code (<code>S-668U1</code>, "
        "<code>J-49JS1-03</code>, etc.), you can also tap "
        "<i>✏️ Enter a code</i> on the main menu — I auto-route by prefix.</i>",
        btns,
    )


async def _handle_search_callback(update, ctx, action: str) -> None:
    """Dispatch srch:* callbacks. Currently only 'reg:<sg|id|th>'."""
    if not action.startswith("reg:"):
        return
    region = action.split(":", 1)[1].lower()
    if region not in _REGION_TAB:
        return
    label, _tab_fn = _REGION_TAB[region]
    ctx.user_data["awaiting_search_query"] = True
    ctx.user_data["search_region"] = region
    # ForceReply so group-chat replies attach correctly even across multi-
    # replica deploys (same pattern as /lastsample).
    await send(
        update,
        f"🔎 <b>Search {label} seasonings</b>\n\n"
        "<b>📎 Reply to this message</b> with what you're looking for. You can mix:\n"
        "  • Keywords / taste — <code>spicy chicken</code>, <code>rendang</code>\n"
        "  • Price filter — <code>cheese below $4</code>\n"
        "  • Product code — <code>S-668U1</code> or <code>J-49JS1-03</code> "
        "(auto-routes by prefix, no region needed)\n\n"
        f"<i>I'll show up to {_SEARCH_TOP_N} most-recent matches from the "
        f"last {_SEARCH_RECENT_MONTHS} months.</i>",
        ForceReply(
            selective=False,
            input_field_placeholder="e.g. spicy chicken below $4",
        ),
        force_new=True,
    )


async def _run_seasoning_search(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    region: str,
    query: str,
    page: int = 0,
) -> None:
    """Search the region's sample tab for free-text matches, reply top-N.

    V1.13.4 — added pagination via `page` arg. Page 0 is the initial
    search; subsequent pages come in through the srpg: callback.
    """
    if region not in _REGION_TAB:
        await send(update, "🤔 Unknown region — please tap 🔎 Search seasonings to retry.")
        return
    label, tab_fn = _REGION_TAB[region]
    tab = tab_fn()

    # Code-prefix shortcut: if the user typed a recognisable code anywhere
    # in their query, hand off to /pp directly (auto-routes by prefix
    # via sheets.find_fsl_product_by_code). Saves them retyping it as
    # an "Enter a code" lookup. Code paste does NOT chain with the
    # previous query — it's an explicit price lookup.
    code_hits = _PP_CODE_RE.findall(query)
    if code_hits:
        unique = _dedupe_codes(code_hits, cap=_pp_cap_for(update.effective_user))
        await _run_pp_for_codes(update, unique)
        return

    # V1.13.7 — refinement chain. If the rep already ran a search in
    # this region and the new text doesn't already contain the previous
    # query, prepend the previous query so 'spicy chicken' followed by
    # 'below 4 usd' becomes 'spicy chicken below 4 usd'. Pagination
    # (page > 0) and code lookups don't chain. The chain is cleared
    # the moment the rep taps any menu:* button or re-enters the
    # search via the region picker.
    refine_note: str = ""
    if page == 0:
        prev_query = (ctx.user_data.get("last_search_query") or "").strip()
        prev_region = ctx.user_data.get("last_search_region") or ""
        new_clean = query.strip()
        if (
            prev_query
            and prev_region == region
            and prev_query.lower() not in new_clean.lower()
            and new_clean.lower() not in prev_query.lower()
        ):
            combined = f"{prev_query} {new_clean}".strip()
            refine_note = (
                f"🔗 <i>Refining: <b>{h(prev_query)}</b> + <b>{h(new_clean)}</b> · "
                "tap 🔎 Search again to start fresh.</i>"
            )
            query = combined

    # Strip out price filter (matcher already handles 'under $X', 'below
    # 4 usd' etc) before keyword matching against the catalog.
    cleaned, max_price = matcher.parse_seasoning_query(query)
    cleaned = (cleaned or "").strip()

    # V1.13.3 — country / cuisine awareness. Detect the country tag (if
    # any) BEFORE the short-query check, so a bare 'malaysia' query is
    # accepted (the country tag itself counts as the search signal).
    country_match = _detect_country_query(cleaned.lower())
    country_label = ""
    if country_match:
        _ck, _cinfo = country_match
        country_label = _ck.title()
        # Strip the country tag from the keyword tokens — we'll use the
        # leftover text (if any) for the normal name-token scoring, and
        # the country/cuisine bonus handles the rest. So 'malaysia
        # spicy' becomes leftover 'spicy' for token scoring + country
        # boost for Malaysian-Country rows + cuisine boost for sambal /
        # nasi lemak / etc names.
        stripped = cleaned.lower()
        for tag in _cinfo["tags"]:
            stripped = re.sub(rf"\b{re.escape(tag)}\b", " ", stripped)
        cleaned = " ".join(stripped.split())

    if len(cleaned) < 2 and max_price is None and not country_match:
        await send(
            update,
            "🤏 That's too short — try a keyword (e.g. <code>BBQ</code>), "
            "a code (<code>S-668</code>), or a price filter "
            "(<code>cheese below $4</code>).",
            kb([[("🔎 Search again", "menu:search"),
                 ("🏠 Main menu", "menu:home")]]),
        )
        return

    # V1.17.x — watchdog: transient status notes at ~10s / ~30s, hard
    # give-up at 90s. Notes are deleted once the data arrives.
    _status_msgs: list = []

    async def _search_status(text: str) -> None:
        try:
            _status_msgs.append(
                await update.effective_chat.send_message(
                    text, parse_mode=ParseMode.HTML
                )
            )
        except Exception:  # noqa: BLE001
            pass

    try:
        rows = await _await_with_status(
            asyncio.to_thread(sheets.load_fsl_rows_all, tab),
            on_status=_search_status,
            slow_text=(
                f"⏳ Fetching the {label} sample list… Google Sheets is "
                "a bit slow right now."
            ),
            busy_text=(
                "😮‍💨 Google Sheets seems <b>busy</b> — still trying "
                "(I'll give up at 90 seconds)…"
            ),
        )
    except TimeoutError:
        await send(
            update,
            f"🔌 Couldn't fetch the {label} sample list within "
            "<b>90 seconds</b> — Google Sheets may be down or busy. "
            "Please try again in a minute.",
            kb([[("🔎 Search again", "menu:search"),
                 ("🏠 Main menu", "menu:home")]]),
        )
        return
    except Exception as e:  # noqa: BLE001
        log.exception("seasoning_search: read failed for %s", tab)
        await send(
            update,
            f"😕 Couldn't read {label} sample list: {_friendly_fetch_error(e)}",
            kb([[("🔎 Search again", "menu:search"),
                 ("🏠 Main menu", "menu:home")]]),
        )
        return
    finally:
        for _m in _status_msgs:
            try:
                await _m.delete()
            except Exception:  # noqa: BLE001
                pass

    if not rows:
        await send(
            update,
            f"📭 {label} sample list is empty.",
            kb([[("🔎 Search again", "menu:search"),
                 ("🏠 Main menu", "menu:home")]]),
        )
        return

    # 36-month recency filter. Rows whose date didn't parse get a sentinel
    # of (1900-01-01) and naturally fall outside the cutoff.
    from datetime import date as _date, timedelta as _td
    cutoff = _date.today() - _td(days=int(_SEARCH_RECENT_MONTHS * 30.5))
    SENTINEL = _date(1900, 1, 1)
    rows = [r for r in rows if (r.get("_date") or SENTINEL) >= cutoff]
    if not rows:
        await send(
            update,
            f"📭 No samples in the last {_SEARCH_RECENT_MONTHS} months for {label}.",
            kb([[("🔎 Search again", "menu:search"),
                 ("🏠 Main menu", "menu:home")]]),
        )
        return

    # V1.13.5 — dynamic country detection from col C of the FSL. Augments
    # the static cuisine-aware detection above. If the static pass didn't
    # find a curated country (malaysia, indonesia, thailand, vietnam,
    # philippines, singapore) but the rep typed a country name that
    # actually appears in the Country column (e.g. Taiwan, India,
    # Bangladesh, China, Korea, Japan), match it here. The col-C bonus
    # applies but no cuisine keyword bonus, since we don't have curated
    # keyword lists for every country yet.
    if not country_match:
        data_countries = {
            (r.get("Country") or "").strip().lower()
            for r in rows
            if (r.get("Country") or "").strip()
        }
        # Skip very short / numeric / single-letter Country values that
        # might create false matches (e.g. a country code 'sg' inside an
        # unrelated word).
        data_countries = {c for c in data_countries if len(c) >= 4}
        # Match longest first so 'south korea' beats 'korea' when both
        # appear in the data.
        sorted_data_countries = sorted(data_countries, key=len, reverse=True)
        for c in sorted_data_countries:
            if re.search(rf"\b{re.escape(c)}\b", cleaned.lower()):
                country_match = (c, {
                    "tags": (c,),
                    "country_match": (c,),
                    "cuisine": (),  # no curated cuisine for dynamic countries
                })
                country_label = c.title()
                # Strip the country tag from the keyword tokens — same
                # logic as the static path.
                stripped_dyn = re.sub(rf"\b{re.escape(c)}\b", " ", cleaned.lower())
                cleaned = " ".join(stripped_dyn.split())
                break

    # V1.12.12 — price-cap filter rewritten to handle currency-prefixed
    # values (the actual data has 'USD 5.44', 'SGD 6.60', 'THB 223.5',
    # 'IDR 59,322'). Previous version did naive float() which failed on
    # ALL rows and silently returned zero matches for any price-filtered
    # query.
    #
    # Conversion rates are approximate but stable enough for filtering;
    # the rep is asking "≤ $X USD" as a rough budget filter, not a
    # quotation. If they need exact pricing they tap /pp.
    _USD_RATE = {
        "USD": 1.0,
        "SGD": 0.74,
        "THB": 0.029,
        "IDR": 0.000063,
        "MYR": 0.21,
        "IDR.": 0.000063,  # tolerate stray punctuation
    }
    _PRICE_PARSE_RE = re.compile(
        r"^([A-Z]{3,4}|RM|S\$|\$)\s*([\d,]+(?:\.\d+)?)$",
        re.IGNORECASE,
    )

    def _row_price_usd(r: dict) -> float | None:
        """Extract a USD-equivalent price from the R&D Price column.
        Returns None when the value is empty, malformed, or in a
        currency we don't recognise."""
        raw = (r.get("R&D Price") or "").strip()
        if not raw:
            return None
        # Bare number (legacy / Singapore master)
        try:
            v = float(raw.replace(",", ""))
            return v if v > 0 else None
        except ValueError:
            pass
        m = _PRICE_PARSE_RE.match(raw)
        if not m:
            return None
        cur_raw, num_raw = m.group(1).upper(), m.group(2).replace(",", "")
        cur_norm = {"S$": "SGD", "$": "USD", "RM": "MYR"}.get(cur_raw, cur_raw)
        try:
            v = float(num_raw)
        except ValueError:
            return None
        if v <= 0:
            return None
        rate = _USD_RATE.get(cur_norm)
        return v * rate if rate is not None else None

    # V1.12.13 — price cap is now a SOFT ranking signal, not a hard
    # filter. Why: a search like 'hot squid below 4.5' was returning 10
    # products that match 'hot' but NONE related to squid, because all
    # 4 squid products were over the cap. The rep wanted to see the
    # squid options even at $4.63 — they can decide whether to push for
    # a cheaper variant. Hiding the relevant matches entirely was
    # actively misleading.
    #
    # New behaviour:
    #   • All rows go through scoring + sorting.
    #   • In-budget items rank higher when scores tie (so a strictly
    #     in-budget exact-match wins).
    #   • Over-budget items still appear, with a '⚠️ over $X' marker
    #     in the meta line.
    #   • Header reflects whether the result mix is mostly in-budget or
    #     mostly above (so the rep knows what they're looking at).
    #
    # We compute the over-budget set up front so the renderer can flag
    # individual rows without re-parsing prices.
    over_budget_codes: set[str] = set()
    n_over_budget_total = 0
    if max_price is not None:
        for r in rows:
            usd = _row_price_usd(r)
            code = (r.get("Product Code") or "").strip().upper()
            if usd is None:
                # Unknown currency / missing price — treat as over budget
                # so it doesn't displace known in-budget rows. Reps still
                # see it via the marker.
                if code:
                    over_budget_codes.add(code)
                n_over_budget_total += 1
            elif usd > max_price:
                if code:
                    over_budget_codes.add(code)
                n_over_budget_total += 1

    # V1.12.11 — name-priority matching with taste fallback.
    #
    # Reps want results where the QUERY appears in the Product Name
    # first. Taste describe is a useful fallback only when nothing
    # matches by name. Previously the matcher treated name and taste
    # equally, which produced confusing results like 'KOPI SEASONING'
    # showing up for a query of 'BBQ' just because the taste describe
    # mentioned BBQ-style smoke notes.
    #
    # Scoring rules per row:
    #   • Code substring match  → score = max + 1 (highest priority)
    #   • Smart name match      → score = max (handles typos/spacing)
    #   • # of query tokens that appear in the name → graded score
    #   • No name hit + taste hit → score = 0, flagged 'taste_only'
    # Rows with score > 0 OR taste_only are kept; sorted by
    # (score desc, date desc); deduped by code.
    matches: list[dict] = []
    matches_taste_only: set[str] = set()  # codes flagged as taste-only

    if cleaned or country_match:
        _word_re = re.compile(r"[a-z0-9]+")
        _STOPWORDS = {"and", "or", "the", "of", "with", "in", "for", "to"}

        def _tokens(s: str) -> list[str]:
            return [t for t in _word_re.findall((s or "").lower())
                    if len(t) >= 2 and t not in _STOPWORDS]

        q_lower = cleaned.lower()
        q_tokens = _tokens(cleaned)

        # V1.12.11 — TF-weighted name scoring. For a multi-token query,
        # rarer tokens contribute more to the score. This matches user
        # intent: typing 'peri hot spicy' should surface 'PERI PERI
        # SEASONING' (rare token 'peri' is the specific keyword) ahead
        # of generic 'HOT & SPICY SEASONING' (common adjectives).
        # Token frequency is computed per-call across the in-window
        # rows we have — close enough to global IDF for our 36-month
        # dataset and avoids stale precomputed indexes.
        from collections import Counter as _Counter
        token_freq: _Counter = _Counter()
        for _r in rows:
            token_freq.update(_tokens(_r.get("Product Name") or ""))

        # Per-token weight: 1000 / sqrt(freq), so a token in 1 product
        # gets ~1000, in 100 products ~100, in 2000 products ~22. The
        # square root softens the curve so common-but-still-meaningful
        # terms (chicken, BBQ) still contribute meaningfully.
        import math
        def _token_weight(t: str) -> float:
            f = max(1, token_freq.get(t, 1))
            return 1000.0 / math.sqrt(f)

        # Sentinel scores: code substring match wins everything; full-
        # query substring is a clean exact name match.
        CODE_BONUS = 1_000_000
        EXACT_NAME_BONUS = 100_000
        # V1.13.3 — country/cuisine bonuses. Country match is a stronger
        # signal than cuisine-keyword match. Both are sized below the
        # exact-name bonus so a literal name hit (e.g. user types
        # 'rendang') still outranks a generic country tag.
        COUNTRY_BONUS = 50_000
        CUISINE_BONUS = 20_000

        def _name_score(row: dict) -> int:
            """Higher = stronger name match. 0 = no name match."""
            name = (row.get("Product Name") or "").lower()
            code = (row.get("Product Code") or "").lower()
            if q_lower and q_lower in code:
                return CODE_BONUS
            if q_lower and q_lower in name:
                return EXACT_NAME_BONUS
            if not q_tokens:
                # No leftover tokens (e.g. bare-country query like
                # 'malaysia'). Country/cuisine scoring handles ranking;
                # don't add a smart-match base hit here or every row
                # with any vague resemblance would surface.
                if not country_match and _smart_text_match(cleaned, name):
                    return 1000  # base hit
                return 0
            name_tokens = _tokens(name)
            if not name_tokens:
                # Smart match handles spacing/typos when tokens fail.
                return 1000 if _smart_text_match(cleaned, name) else 0
            score = 0.0
            matched_any = False
            for qt in q_tokens:
                if any(qt in nt for nt in name_tokens):
                    matched_any = True
                    score += _token_weight(qt)
            if not matched_any and _smart_text_match(cleaned, name):
                # Spacing/typo fallback when no token literally matched.
                return 1000
            return int(score)

        def _country_cuisine_score(row: dict) -> int:
            """Bonus when the row matches the detected country (Country
            column) or the country's signature cuisine keywords (in the
            Product Name). Returns 0 if no country was detected."""
            if not country_match:
                return 0
            _ck, info = country_match
            bonus = 0
            row_country = (row.get("Country") or "").lower()
            for c in info["country_match"]:
                if c in row_country:
                    bonus += COUNTRY_BONUS
                    break
            name = (row.get("Product Name") or "").lower()
            for kw in info["cuisine"]:
                if kw in name:
                    bonus += CUISINE_BONUS
                    break
            return bonus

        def _taste_match(row: dict) -> bool:
            taste = (row.get("Taste describe") or "").lower()
            if not taste:
                return False
            if q_lower and q_lower in taste:
                return True
            if q_tokens:
                taste_tokens = _tokens(taste)
                return any(
                    any(qt in tt for tt in taste_tokens)
                    for qt in q_tokens
                )
            return False

        scored: list[tuple[int, bool, dict]] = []  # (total_score, taste_only, row)
        for r in rows:
            ns = _name_score(r)
            cb = _country_cuisine_score(r)
            total = ns + cb
            if total > 0:
                scored.append((total, False, r))
            elif _taste_match(r):
                scored.append((0, True, r))

        # Sort: name score desc → in-budget first when scores tie → date
        # desc. The in-budget tiebreaker means a strictly-in-budget
        # 100,000-score row beats an over-budget 100,000-score row, but
        # an over-budget 340-score row (e.g. HOT & SPICY SQUID for the
        # 'hot squid' query) still beats an in-budget 51-score row
        # (HOT CHICKEN matching only the common 'hot' token).
        def _sort_key(x):
            ns, _t_only, r = x
            code = (r.get("Product Code") or "").strip().upper()
            in_budget = 0 if code not in over_budget_codes else 1
            d = r.get("_date") or SENTINEL
            return (-ns, in_budget, -d.toordinal())

        scored.sort(key=_sort_key)
        matches = [r for (_, _, r) in scored]
        matches_taste_only = {
            (r.get("Product Code") or "").strip().upper()
            for (ns, t_only, r) in scored if t_only
        }
    else:
        # Pure price-filter query — show recent rows matching the cap.
        # V1.13.7: sort in-budget rows first (so a strictly-in-budget
        # row beats an over-budget one) then by date desc — without
        # this, the dedupe step kept the OLDEST row for each code
        # because that was the insertion order from the sheet.
        matches = sorted(
            rows,
            key=lambda r: (
                0 if (r.get("Product Code") or "").strip().upper()
                not in over_budget_codes else 1,
                -((r.get("_date") or SENTINEL).toordinal()),
            ),
        )

    if not matches:
        bits = [f"🤷 No matches for <b>{h(query)}</b> in {label} "
                f"sample list (last {_SEARCH_RECENT_MONTHS} months)."]
        if max_price is not None:
            bits.append(f"<i>Price filter: ≤ ${max_price:g} USD</i>")
        bits.append("")
        bits.append("<i>Tips:</i>")
        bits.append("<i>  • Try shorter / different keywords</i>")
        bits.append("<i>  • Drop the price filter if you set one</i>")
        bits.append("<i>  • If you have a code, paste it directly — "
                    "auto-routes regardless of region</i>")
        # V1.17.x — cross-domain suggestion: maybe the rep typed a
        # CUSTOMER, not a product. Probe the customer master and offer
        # tap-through to that customer's recent samples. Best-effort.
        cust_buttons: list[list[tuple[str, str]]] = []
        try:
            master = await asyncio.to_thread(sheets.load_merged_customers)
            cust_hits = matcher.top_customer_master(
                cleaned or query, master, limit=3
            )
        except Exception as e:  # noqa: BLE001
            log.debug("search no-match customer probe failed: %s", e)
            cust_hits = []
        if cust_hits:
            bits.append("")
            bits.append(
                "👤 <i>Or did you mean a customer? Tap to see their "
                "recent samples:</i>"
            )
            for c in cust_hits:
                cname = (c.get("name") or "").strip()
                if not cname:
                    continue
                btn_label = f"👤 {cname}"
                if len(btn_label) > 40:
                    btn_label = btn_label[:38] + "…"
                payload = cname.encode("utf-8")[:55].decode(
                    "utf-8", errors="ignore"
                )
                cust_buttons.append([(btn_label, f"lsd:c:{payload}")])
        await send(
            update,
            "\n".join(bits),
            kb(cust_buttons
               + [[("🔎 Search again", "menu:search"),
                   ("🏠 Main menu", "menu:home")]]),
        )
        return

    # Dedupe by product code, keeping the FIRST occurrence (which is the
    # highest-scored or most-recent depending on the sort applied above).
    # Sample frequency is shown as a tag so reps can tell which products
    # have been requested often.
    by_code: dict[str, dict] = {}
    code_counts: dict[str, int] = {}
    for r in matches:
        code = (r.get("Product Code") or "").strip().upper()
        if not code:
            continue
        code_counts[code] = code_counts.get(code, 0) + 1
        if code not in by_code:
            by_code[code] = r
    unique_matches = list(by_code.values())
    total_unique = len(unique_matches)
    total_events = len(matches)

    # V1.13.4 — paginate. Page 0 shows 1-10, page 1 shows 11-20, etc.
    # Pages are naturally non-overlapping (slice of an already-deduped
    # list). Clamp to valid range so a stale callback can't blow up.
    total_pages = max(1, (total_unique + _SEARCH_TOP_N - 1) // _SEARCH_TOP_N)
    page = max(0, min(int(page or 0), total_pages - 1))
    page_start = page * _SEARCH_TOP_N
    page_end = min(page_start + _SEARCH_TOP_N, total_unique)
    top = unique_matches[page_start:page_end]

    # Count name vs taste-only buckets in the displayed top-N.
    n_name = sum(
        1 for r in top
        if (r.get("Product Code") or "").strip().upper() not in matches_taste_only
    )
    n_taste = len(top) - n_name

    # Count over-budget items in the displayed top-N (for header callout).
    n_over_budget_shown = sum(
        1 for r in top
        if (r.get("Product Code") or "").strip().upper() in over_budget_codes
    )

    # Header. Plural-handling for the count + callouts for taste-only
    # fallback and over-budget items in the visible page.
    if total_unique > _SEARCH_TOP_N:
        header_bits = [
            f"🔎 <b>{label} — {page_start + 1}–{page_end} of "
            f"{total_unique} products</b>"
        ]
    else:
        header_bits = [
            f"🔎 <b>{label} — {len(top)} of {total_unique} products</b>"
        ]
    # V1.13.7 — refinement-chain breadcrumb sits right under the title
    # so the rep can see immediately that the bot combined their input
    # with the previous query and how to opt out.
    if refine_note:
        header_bits.append(f"   {refine_note}")
    if max_price is not None:
        if n_over_budget_shown == 0:
            header_bits.append(f"   <i>≤ ${max_price:g} USD</i>")
        elif n_over_budget_shown == len(top):
            header_bits.append(
                f"   ⚠️ <i>No products under <b>${max_price:g} USD</b> — "
                "showing closest above-budget options.</i>"
            )
        else:
            header_bits.append(
                f"   <i>${max_price:g} USD budget · {n_over_budget_shown} "
                f"of {len(top)} are over budget (relevance-ranked).</i>"
            )
    if cleaned:
        header_bits.append(f"   <i>matching “{h(cleaned)}”</i>")
    if country_match:
        # Curated countries (with cuisine keyword lists) get the richer
        # header line. Dynamic-only countries (matched via col C of the
        # FSL but no curated cuisine) get a simpler line.
        _ck_info = country_match[1]
        if _ck_info.get("cuisine"):
            if country_label == "Malaysia":
                header_bits.append(
                    f"   🌏 <i>{country_label} samples + signature cuisine "
                    "(nasi lemak, satay, sambal…)</i>"
                )
            else:
                header_bits.append(
                    f"   🌏 <i>{country_label} samples + signature cuisine matches</i>"
                )
        else:
            header_bits.append(
                f"   🌏 <i>{country_label} samples (Country column match)</i>"
            )
    if n_name == 0 and n_taste > 0:
        header_bits.append(
            "   <i>No matches by product name — showing taste-similar "
            "recommendations instead.</i>"
        )
    elif n_taste > 0:
        header_bits.append(
            f"   <i>{n_name} by name · {n_taste} taste-similar fallback"
            f"{'s' if n_taste != 1 else ''}.</i>"
        )
    lines: list[str] = header_bits + [""]

    # V1.13.11 — per-rep currency override. Resolved once for this page
    # of results; the inline _fmt_price closure captures it and applies
    # to every row's price line.
    search_pref_currency = await _user_pref_currency(update)

    def _fmt_price(raw: str) -> str:
        """USD-prefix bare numeric prices; pass through anything that
        already has currency text (e.g. 'IDR 76,891', 'THB 162.9') —
        unless the rep has a currency override, in which case every
        price is converted to that currency. Matches the /lastsample
        formatter so search and lastsample look consistent."""
        return _format_price_for_currency(raw, search_pref_currency)

    def _truncate_at_word(s: str, n: int) -> str:
        """Truncate to <= n chars at the last word boundary (so we don't
        chop mid-word like 'savoury he…'). Falls back to a hard cut if no
        space is found in the head."""
        if len(s) <= n:
            return s
        head = s[: n - 1]
        cut = head.rfind(" ")
        if cut > n // 2:  # only word-cut if it doesn't lose too much
            head = head[:cut]
        return head.rstrip(",;: ") + "…"

    for i, r in enumerate(top, page_start + 1):
        d = r.get("_date")
        date_str = d.strftime("%d %b %Y") if d else (r.get("Sample Date Out") or "—")
        name = (r.get("Product Name") or "—").strip()
        code = (r.get("Product Code") or "—").strip().upper()
        price_str = _fmt_price(r.get("R&D Price") or "")
        taste = (r.get("Taste describe") or "").strip()
        category = (r.get("Category") or "").strip()
        n_samples = code_counts.get(code, 1)
        is_taste_only = code in matches_taste_only
        is_over_budget = code in over_budget_codes

        # Line 1: number + product name (bold) + small flags for taste-
        # similar fallback (didn't match by name) and over-budget (price
        # exceeds the rep's filter — but matched their keywords so we
        # surface it anyway with a clear marker).
        name_line = f"<b>{i}. {h(name)}</b>"
        if is_taste_only:
            name_line += "  <i>· taste-similar</i>"
        if is_over_budget and max_price is not None:
            name_line += f"  ⚠️ <i>over ${max_price:g}</i>"
        lines.append(name_line)
        # Line 2: code · price · date · category · sample count — each piece
        # HTML-escaped at construction so we don't accidentally emit broken
        # markup if a sheet cell contains < > & chars.
        meta_parts: list[str] = [f"<code>{h(code)}</code>"]
        if price_str:
            meta_parts.append(h(price_str))
        meta_parts.append(h(date_str))
        if category:
            meta_parts.append(h(category))
        if n_samples > 1:
            meta_parts.append(f"{n_samples}× sampled")
        lines.append("   " + " · ".join(meta_parts))
        # Line 3: italic taste, single short line truncated at a word
        # boundary so results don't wrap awkwardly on phone screens.
        if taste:
            lines.append(f"   <i>{h(_truncate_at_word(taste, 80))}</i>")
        # Blank line between results for visual breathing room.
        lines.append("")

    # Trailing footer with sample-event total (always — gives the rep a
    # sense of how many sample events are behind the unique product list).
    if total_events > total_unique:
        lines.append(
            f"<i>{total_events} sample events across {total_unique} unique "
            "products — refine the query to narrow down.</i>"
        )

    # V1.13.4 — pagination buttons (mirrors /lastsample's lspr: pattern).
    # Encode region + page + 10-char query hash in callback data; cache
    # the query against the hash in user_data so the page-flip callback
    # can recover it without storing the (potentially long) query in the
    # callback bytes (Telegram caps callback_data at 64 bytes).
    #
    # Audit fix #6 — also embed the query in the callback when it fits.
    # Telegram callback_data is 64 bytes, "srpg:sg:99:abcdef0123:" is
    # ~22 bytes, leaving ~42 bytes for the query. Most queries fit, so
    # the per-worker user_data cache is no longer the only path back —
    # which fixes the "context expired" spuriously firing on every
    # multi-replica Railway load-balancer hop.
    qhash = _query_hash(query)
    pager_state = ctx.user_data.setdefault("srpg_query_cache", {})
    pager_state[qhash] = {"query": query, "region": region}
    if len(pager_state) > 8:
        for k in list(pager_state.keys())[:-8]:
            pager_state.pop(k, None)

    # V1.13.7 — remember the (possibly combined) query so the next
    # text-input refinement can chain on top of it. Cleared by region-
    # picker entry and any menu:* navigation in _handle_menu_callback.
    ctx.user_data["last_search_query"] = query
    ctx.user_data["last_search_region"] = region

    def _srpg_cb(p: int) -> str:
        return _build_pager_cb(["srpg", region, str(p)], qhash, query)

    nav_row: list[tuple[str, str]] = []
    if page > 0:
        if page > 1:
            nav_row.append(("⏮ First", _srpg_cb(0)))
        nav_row.append(("◀ Prev", _srpg_cb(page - 1)))
    if page_end < total_unique:
        next_count = min(_SEARCH_TOP_N, total_unique - page_end)
        nav_row.append((f"Next {next_count} ▶", _srpg_cb(page + 1)))

    btn_rows: list[list[tuple[str, str]]] = []
    if nav_row:
        btn_rows.append(nav_row)
    btn_rows.append([("🔎 Search again", "menu:search"),
                     ("🏠 Main menu", "menu:home")])

    await send(
        update,
        "\n".join(lines).rstrip(),
        kb(btn_rows),
    )


# Reusable footer keyboard for every /lastsample reply — keeps the user in
# the loop without making them retype the slash command. Two flavours so
# the 'Find another' button reprompts in the right scope (self vs all-reps).
_LASTSAMPLE_KB = kb([[("🔎 Find another", "lastsample:again"), ("🏠 Main menu", "menu:home")]])
_ALL_LASTSAMPLE_KB = kb([[("🔎 Find another", "lastsample:again_all"), ("🏠 Main menu", "menu:home")]])


def _last_kb(scope: str):
    """Pick the right footer keyboard for the active scope."""
    return _ALL_LASTSAMPLE_KB if scope == "all" else _LASTSAMPLE_KB


# Cache the last-sync timestamp for 5 minutes so we don't read the
# _sync_meta tab on every /lastsample reply. Staleness here only affects
# the displayed timestamp, not the data itself — the FSL rows are
# whatever load_fsl_rows_*() reads at the moment of search.
_LAST_SYNC_CACHE: tuple[float, "object"] | None = None
_LAST_SYNC_TTL = 5 * 60  # seconds


async def _last_sync_footer() -> str:
    """Render a subtle one-liner showing when FSL was last synced from MMS.

    Used on /lastsample and /alllastsample replies so reps know how fresh
    the data they're seeing is. Returns '' if we've never recorded a sync
    (e.g. very fresh deploy with no run yet). Flags a warning when the
    last sync is over a week old — the auto-sync runs weekly, so anything
    older means something has broken.
    """
    global _LAST_SYNC_CACHE
    import time as _t
    now = _t.time()
    if _LAST_SYNC_CACHE and now - _LAST_SYNC_CACHE[0] < _LAST_SYNC_TTL:
        last = _LAST_SYNC_CACHE[1]
    else:
        last = await asyncio.to_thread(sheets.get_last_sample_sync)
        _LAST_SYNC_CACHE = (now, last)
    if last is None:
        return ""
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    if last.tzinfo is None:
        last = last.replace(tzinfo=_tz.utc)
    age = _dt.now(_tz.utc) - last
    pretty = last.strftime("%d %b %Y, %H:%M UTC")
    if age > _td(days=8):  # 1-day grace beyond the weekly cycle
        return f"⚠️ Sample list last refreshed: {pretty} — over a week old."
    return f"📅 Sample list last refreshed: {pretty}"


def _cust_hash(name: str) -> str:
    """Stable short hash for customer-name callback data.

    10 hex chars = 40 bits — collision-safe well past any plausible
    customer count for one rep. Stays under Telegram's 64-byte
    callback_data limit even with the 'lsc:' prefix.
    """
    import hashlib
    return hashlib.md5((name or "").lower().strip().encode("utf-8")).hexdigest()[:10]


async def _run_lastsample_search(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    mms_name: str,
    query: str,
    prev: str = "",
    mode: str = "auto",
    scope: str = "self",
) -> None:
    """Search FSL rows owned by `mms_name` for `query`, reply with the latest match.

    Refinement protocol (V1.8.6+): the caller passes the accumulated query
    string. If the search succeeds, we set ``lastsample_active_query`` to
    that query and re-arm ``awaiting_lastsample_query`` so the next text
    refines further. If the search fails, we clear ``lastsample_active_query``
    so the next text starts a fresh search rather than appending to a query
    we know returns nothing.

    ``prev`` is just the query before the user's latest word — used to give
    the user a clear "I tried <prev + new>, found nothing, search reset"
    explanation when a refinement fails.

    ``mode`` controls product-vs-customer routing (V1.9.5):
      - ``"auto"`` — default. Search both. If query matches a product AND a
        customer, ask the user which they meant via a disambiguation prompt.
      - ``"product"`` — search Product Name only (skip the customer step).
        Used by the disambiguation 'Product' button so the user's choice
        sticks.
      - ``"customer"`` — search Customer Name only. Used by the
        disambiguation 'Customer' button.
    """

    def _re_arm(active: str) -> None:
        """Persist the new search context so the next text continues the chain."""
        ctx.user_data["awaiting_lastsample_query"] = True
        ctx.user_data["lastsample_active_query"] = active

    query = (query or "").strip()
    if len(query) < 2:
        await send(
            update,
            "🤏 That's too short. Try at least 2 characters — a product name, "
            "code prefix, or flavour keyword.",
            _last_kb(scope),
        )
        _re_arm(prev)  # leave whatever was there alone
        return

    # Scope-aware FSL load — V1.12.9: now reads BOTH Singapore and
    # Jakarta tabs so J-code queries are visible to /alllastsample +
    # /lastsample. _load_lastsample_rows handles the per-tab read errors
    # internally (a single tab failing degrades gracefully).
    # V1.17.x — watchdog: transient status notes at ~10s / ~30s, hard
    # give-up at 90s. Notes are deleted once the data arrives.
    _ls_status_msgs: list = []

    async def _ls_status(text: str) -> None:
        try:
            _ls_status_msgs.append(
                await update.effective_chat.send_message(
                    text, parse_mode=ParseMode.HTML
                )
            )
        except Exception:  # noqa: BLE001
            pass

    try:
        rows = await _await_with_status(
            _load_lastsample_rows(scope, mms_name),
            on_status=_ls_status,
            slow_text=(
                "⏳ Fetching the sample listing… Google Sheets is a bit "
                "slow right now."
            ),
            busy_text=(
                "😮‍💨 Google Sheets seems <b>busy</b> — still trying "
                "(I'll give up at 90 seconds)…"
            ),
        )
    except TimeoutError:
        await send(
            update,
            "🔌 Couldn't fetch the sample listing within <b>90 seconds</b> "
            "— Google Sheets may be down or busy. Please try again in a "
            "minute.",
            _last_kb(scope),
        )
        _re_arm(prev)
        return
    except Exception as e:  # noqa: BLE001
        log.exception("lastsample: FSL read failed")
        # Detect the common rate-limit case and show a friendlier message
        # — reps used to interpret silent 'no match' as 'the sample isn't
        # in the system' and waste time double-checking. Now we tell
        # them it's a transient quota issue, retry in a moment.
        err_text = str(e)
        if "429" in err_text or "Quota exceeded" in err_text or "rate limit" in err_text.lower():
            msg = (
                "⏳ <b>Google Sheets rate limit hit</b> — try the same "
                "search again in 60-90 seconds.\n\n"
                "<i>This bot reads the Full Sample Listing tabs and "
                "Google caps reads at 60/minute per user. Multiple "
                "rapid searches can trip the limit; the next attempt "
                "after a short wait will succeed.</i>"
            )
        else:
            msg = f"😕 Couldn't read Full Sample Listing: {_friendly_fetch_error(e)}"
        await send(update, msg, _last_kb(scope))
        _re_arm(prev)
        return
    finally:
        for _m in _ls_status_msgs:
            try:
                await _m.delete()
            except Exception:  # noqa: BLE001
                pass

    if not rows:
        whose = "any rep" if scope == "all" else f"<b>{h(mms_name)}</b>"
        await send(
            update,
            f"📭 I don't see any samples logged under {whose} "
            "in Full Sample Listing yet.",
            _last_kb(scope),
        )
        _re_arm("")  # nothing to refine on
        return

    # Strict containment scoring (V1.8.8). The Product Name MUST literally
    # contain the user's input term — we no longer fuzzy-match. Rules:
    #
    #   - Whole query as substring of code or name → match.
    #   - Otherwise, every non-stopword token from the query must appear
    #     inside SOME name token (substring either direction, so 'chip'
    #     finds 'chips' and vice versa).
    #   - Anything else → no match. Bot says "no product found in your
    #     sample request list", no fuzzy 'did you mean' guesses.
    #
    # Why this is the right trade-off here: false positives (peri → PEPPER,
    # honey → BBQ via taste keywords) misled users into quoting the wrong
    # product. Strict containment is predictable: if the user's word isn't
    # in the name, the bot honestly says so.
    _word_re = re.compile(r"[a-z0-9]+")
    # Common connectors that pollute substring matching when present in
    # either the query or the product name (e.g. 'fish and chip' vs 'FISH
    # & CHIPS' — '&' becomes nothing after tokenizing, 'and' is a noise
    # word). Keep this short — only words that are truly never meaningful.
    _STOPWORDS = {"and", "or", "the", "of", "with", "in", "for", "to"}

    def _tokens(s: str) -> list[str]:
        return [
            t for t in _word_re.findall((s or "").lower())
            if len(t) >= 2 and t not in _STOPWORDS
        ]

    q = query.lower().strip()
    q_tokens = _tokens(query)

    # === Compute both match sets up front (when mode allows). ===
    # Product matches — use the top-level helper so the matching rules
    # (code-shape detection, smart text match, AND-tokens fallback) are
    # identical to what the lspr: pagination callback uses.
    product_candidates = (
        [r for r in rows if _match_lastsample_product(r, query)]
        if mode in ("auto", "product") else []
    )

    # Customer matches. V1.12.6: routed through _smart_text_match so
    # 'Datong' finds 'Da tong group' (alphanumeric-stripped substring),
    # and typos like 'Datng' fuzzy-match 'Da tong' (WRatio ≥ 80). The
    # existing token-AND check is also kept so multi-word queries like
    # 'q land' still narrow correctly to 'queensland trading' even when
    # the squished form ('qland') happens to substring-match.
    def _customer_matches(cust_name: str) -> bool:
        cn = (cust_name or "").lower()
        if not cn:
            return False
        if _smart_text_match(query, cn):
            return True
        if q_tokens and len(q_tokens) > 1 and all(qt in cn for qt in q_tokens):
            return True
        return False

    from datetime import date as _date
    SENTINEL = _date(1900, 1, 1)
    sorted_customers: list[str] = []
    if mode in ("auto", "customer"):
        cust_latest: dict[str, _date | None] = {}
        for r in rows:
            cust = (r.get("Customer Name") or "").strip()
            if not cust or not _customer_matches(cust):
                continue
            d = r.get("_date")
            prev_d = cust_latest.get(cust)
            if cust not in cust_latest or (d and (prev_d is None or d > prev_d)):
                cust_latest[cust] = d
        sorted_customers = sorted(
            cust_latest.keys(),
            key=lambda c: cust_latest.get(c) or SENTINEL,
            reverse=True,
        )

    # === Disambiguation: query matched both a product AND a customer. ===
    # Only fires in 'auto' mode — the disambiguation buttons themselves
    # call back with mode='product' or mode='customer' so we never recurse.
    if mode == "auto" and product_candidates and sorted_customers:
        # Encode the query into the callback so the choice survives worker
        # switches (no reliance on ctx.user_data). Telegram callback_data
        # limit is 64 bytes UTF-8; with 'lsdall:p:' (9 bytes) we leave 55
        # for the query. Truncate at byte boundary if longer.
        # Scope is encoded in the prefix:
        #   lsd:p:<q>     → self-scope, product
        #   lsd:c:<q>     → self-scope, customer
        #   lsdall:p:<q>  → all-scope (admin), product
        #   lsdall:c:<q>  → all-scope (admin), customer
        prefix = "lsdall" if scope == "all" else "lsd"
        q_bytes = query.encode("utf-8")[:55]
        q_safe = q_bytes.decode("utf-8", errors="ignore")
        again_cb = "lastsample:again_all" if scope == "all" else "lastsample:again"
        disambig_kb = kb([
            [
                ("🛍 Product Name", f"{prefix}:p:{q_safe}"),
                ("👤 Customer Name", f"{prefix}:c:{q_safe}"),
            ],
            [
                ("🔎 Find another", again_cb),
                ("🏠 Main menu", "menu:home"),
            ],
        ])
        n_prod = len(product_candidates)
        n_cust = len(sorted_customers)
        intro = [
            f"🤔 <b>{h(query)}</b> matches both a product and a customer in your samples.",
            "",
            "<b>Which one are you looking for?</b>",
            "",
            f"  🛍  <b>Product Name</b> — {n_prod} sample"
            f"{'s' if n_prod != 1 else ''} with <b>{h(query)}</b> in the product name",
            f"  👤  <b>Customer Name</b> — {n_cust} customer"
            f"{'s' if n_cust != 1 else ''} with <b>{h(query)}</b> in their name",
        ]
        await send(update, "\n".join(intro), disambig_kb)
        # Reset the refinement chain — the next move is one of these buttons.
        _re_arm("")
        return

    # Re-bind for downstream code (the rest of this function still uses
    # the old name `candidates`).
    candidates = product_candidates

    if not candidates:
        if sorted_customers:
            # Found at least one customer matching the query. Per the user's
            # spec ('1 b'): list the customers as buttons even when there's
            # only one — keeps the UX consistent and lets them confirm intent.
            options = sorted_customers[:9]  # cap at 9 buttons; rest hidden
            ctx.user_data["lastsample_mms_name"] = mms_name  # kept warm for callback

            # Use a short hash of the customer name as callback data instead
            # of a list index. Why: ctx.user_data is per-worker, so when
            # Railway switches workers between sending the buttons and the
            # user tapping one, an index lookup in user_data fails ('That
            # selection has expired'). A hash lets us re-derive the customer
            # name from the FSL on whichever worker handles the click —
            # state-free, multi-replica safe.
            # Scope-aware callback prefix (lsc=self, lscall=all-reps).
            cust_prefix = "lscall" if scope == "all" else "lsc"
            again_cb = "lastsample:again_all" if scope == "all" else "lastsample:again"
            btn_rows = [[(c, f"{cust_prefix}:{_cust_hash(c)}")] for c in options]
            btn_rows.append([
                ("🔎 Find another", again_cb),
                ("🏠 Main menu", "menu:home"),
            ])
            intro = [
                f"🤔 <b>No products matched {h(query)}.</b>",
                "",
                f"But I found <b>{len(sorted_customers)}</b> customer"
                f"{'s' if len(sorted_customers) != 1 else ''} "
                + (
                    "in Full Sample Listing"
                    if scope == "all"
                    else "you've sent samples to"
                )
                + f" with <b>{h(query)}</b> in the name.",
                "",
                "<b>Tap one to see their last 10 samples:</b>",
            ]
            if len(sorted_customers) > 9:
                intro.append("")
                intro.append(
                    f"<i>(Showing top {len(options)} by most-recent activity. "
                    "Refine with a longer keyword to narrow further.)</i>"
                )
            sync_footer = await _last_sync_footer()
            if sync_footer:
                intro.append("")
                intro.append(f"<i>{sync_footer}</i>")
            await send(update, "\n".join(intro), kb(btn_rows))
            # Reset the refinement chain — the customer button is the next step.
            _re_arm("")
            return

        # No products AND no customers matched.
        # V1.13.12 — when self-scope misses, silently peek at all-reps
        # to see if a colleague has the sample. Most common cause of the
        # 'no product found' confusion: rep typed a code that another
        # rep sent (e.g. William typed J-45GR1-06 which Heidy sent in
        # 2021). The bot is technically right, but reps interpret
        # 'sample request list' as the whole list, not their own. So
        # we surface the all-reps count + a one-tap switch button.
        reset_note = (
            "\n\n<i>🔄 Your search has been reset — send a fresh keyword "
            "to start over, or tap 🏠 Main menu.</i>" if prev else ""
        )
        whose = (
            "in Full Sample Listing"
            if scope == "all"
            else f"under your name (<b>{h(mms_name)}</b>)"
        )
        sync_footer = await _last_sync_footer()
        sync_tail = f"\n\n<i>{sync_footer}</i>" if sync_footer else ""

        all_scope_hits: list[dict] = []
        code_in_query = _PP_CODE_RE.findall(query) if scope == "self" else []
        if scope == "self":
            try:
                all_rows = await _load_lastsample_rows(scope="all")
                all_scope_hits = _filter_lastsample_products(all_rows, query)
            except Exception as e:  # noqa: BLE001
                log.warning("lastsample no-match all-scope peek failed: %s", e)

        body_lines = [
            "🙈 <b>No product found in your sample request list.</b>",
            "",
            f"Nothing {whose} has <b>{h(query)}</b> in the Product Name "
            "or Customer Name. Double-check the spelling, or try a "
            "different keyword.",
        ]
        extra_btn_rows: list[list[tuple[str, str]]] = []

        if scope == "self" and all_scope_hits:
            senders = sorted({
                (r.get("Sales") or "").strip()
                for r in all_scope_hits
                if (r.get("Sales") or "").strip()
            })
            senders_blurb = ""
            if senders:
                shown = ", ".join(h(s) for s in senders[:3])
                more = (
                    f" and {len(senders) - 3} more"
                    if len(senders) > 3
                    else ""
                )
                senders_blurb = f" sent by <b>{shown}</b>{more}"
            body_lines.append("")
            body_lines.append(
                f"💡 But <b>{len(all_scope_hits)}</b> sample"
                f"{'s' if len(all_scope_hits) != 1 else ''} match"
                f"{'es' if len(all_scope_hits) == 1 else ''} across "
                f"<b>all reps' samples</b>{senders_blurb}. Tap below to view."
            )
            # Encode query in callback. Telegram caps callback_data at
            # 64 bytes UTF-8; 'lsall:' prefix is 6 bytes → 58 byte budget.
            q_bytes = query.encode("utf-8")[:58]
            q_safe = q_bytes.decode("utf-8", errors="ignore")
            extra_btn_rows.append([
                ("🌐 Show all-reps result", f"lsall:{q_safe}"),
            ])

        if scope == "self" and code_in_query:
            first_code = code_in_query[0].upper()
            extra_btn_rows.append([
                (f"💲 Look up price for {first_code}", f"lspp:{first_code}"),
            ])

        # Append reset note + sync footer as before
        body = "\n".join(body_lines) + f"{reset_note}{sync_tail}"

        # Default fallback row: Find another + Main menu
        again_cb = "lastsample:again_all" if scope == "all" else "lastsample:again"
        extra_btn_rows.append([
            ("🔎 Find another", again_cb),
            ("🏠 Main menu", "menu:home"),
        ])

        await send(update, body, kb(extra_btn_rows))
        _re_arm("")
        return

    # V1.12.7 — paginated top-N display instead of single best match.
    # Sort by date desc, then defer to _show_lastsample_results to render
    # one page of 10 with Prev/Next buttons. Refinement chain is still
    # re-armed so typing more text narrows further.
    candidates.sort(key=lambda r: r.get("_date") or SENTINEL, reverse=True)
    await _show_lastsample_results(
        update, ctx, candidates, query=query, scope=scope, page=0,
        with_prev=bool(prev),
    )
    _re_arm(query)


_LASTSAMPLE_PAGE_SIZE = 10


def _query_hash(q: str) -> str:
    """10-char md5 of the query — used in pagination callbacks so the page
    state survives across multi-replica deploys without depending on
    user_data. We re-derive matches by re-running the search with this
    query on whichever worker handles the click."""
    import hashlib
    return hashlib.md5((q or "").encode("utf-8")).hexdigest()[:10]


def _build_pager_cb(
    prefix_parts: list[str], qhash: str, query: str, max_total: int = 64
) -> str:
    """Build a pagination callback_data string, embedding the query when
    it fits within Telegram's 64-byte callback limit.

    Format: '<prefix_parts joined by ":">:<qhash>[:<query>]'.
    When the query doesn't fit (long refined queries, multi-byte UTF-8),
    we drop it and the handler will fall back to its user_data cache.

    Audit fix #6: previously the renderers always built '<head>:<qhash>'
    only and relied 100% on the per-worker ctx.user_data cache to map
    qhash → query. On Railway's multi-replica setup the click could
    land on a worker that never saw the original search, so paging
    bailed with "context expired" — even though the qhash is
    deterministic and the search could be re-run if we just had the
    query. This helper preserves the cache-based fast-path AND adds
    the in-callback fallback for cross-replica recovery."""
    head = ":".join(prefix_parts + [qhash])
    remaining = max_total - len(head) - 1  # -1 for the ':' separator
    if remaining <= 0:
        return head
    q_bytes = (query or "").encode("utf-8")[:remaining]
    # safe-decode in case we sliced mid-multibyte
    q_safe = q_bytes.decode("utf-8", errors="ignore").strip()
    if not q_safe:
        return head
    return f"{head}:{q_safe}"


async def _show_lastsample_results(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    candidates: list[dict],
    query: str,
    scope: str,
    page: int,
    with_prev: bool = False,
) -> None:
    """Render one page of /lastsample (or /alllastsample) results.

    candidates : already date-sorted (most-recent first), already
                 filtered against the query.
    query      : the active search string — shown in the header and
                 used as the pagination key.
    scope      : 'self' or 'all'.
    page       : 0-indexed page number.
    with_prev  : True when this is a refinement turn (the rep typed
                 more words) — adds a small 'searched:' note.
    """
    total = len(candidates)
    total_pages = max(1, (total + _LASTSAMPLE_PAGE_SIZE - 1) // _LASTSAMPLE_PAGE_SIZE)
    page = max(0, min(int(page or 0), total_pages - 1))
    start = page * _LASTSAMPLE_PAGE_SIZE
    end = min(start + _LASTSAMPLE_PAGE_SIZE, total)
    page_rows = candidates[start:end]

    label = "Everyone's samples" if scope == "all" else "Your samples"
    header_bits = [
        f"🎯 <b>{label} — {start + 1}–{end} of {total} matches</b>"
    ]
    if query:
        header_bits.append(f"   <i>matching “{h(query)}”</i>")
    if with_prev:
        header_bits.append(f"   <i>(refined query)</i>")
    lines: list[str] = header_bits + [""]

    # V1.13.11 — per-rep currency override for /lastsample rows.
    lastsample_pref_currency = await _user_pref_currency(update)

    def _fmt_price(raw: str) -> str:
        return _format_price_for_currency(raw, lastsample_pref_currency)

    for i, r in enumerate(page_rows, start + 1):
        d = r.get("_date")
        date_str = d.strftime("%d %b %Y") if d else (r.get("Sample Date Out") or "—")
        name = (r.get("Product Name") or "—").strip()
        code = (r.get("Product Code") or "—").strip().upper()
        price_str = _fmt_price(r.get("R&D Price") or "")
        customer = (r.get("Customer Name") or "—").strip()
        sales = (r.get("Sales") or "").strip()
        awb_raw = (r.get("AWB") or "").strip()

        # Line 1: number + product name (bold).
        lines.append(f"<b>{i}. {h(name)}</b>")
        # Line 2: code · price · date — meta line, bullet-separated.
        meta_parts: list[str] = [f"<code>{h(code)}</code>"]
        if price_str:
            meta_parts.append(h(price_str))
        meta_parts.append(h(date_str))
        lines.append("   " + " · ".join(meta_parts))
        # Line 3: customer (always) + sales rep (only in all-scope).
        cust_line = f"   👤 {h(customer)}"
        if scope == "all" and sales:
            cust_line += f"  <i>· sent by {h(sales)}</i>"
        lines.append(cust_line)
        # Line 4: AWB. Same three-state rendering as the digest +
        # customer view — real tracking number, 🚗 hand-carry marker,
        # or '—' when the row is still unmapped.
        if not awb_raw:
            lines.append("   📦 AWB —")
        elif _is_hand_carry(awb_raw):
            lines.append("   🚗 Hand carry")
        else:
            lines.append(f"   📦 AWB <code>{h(awb_raw)}</code>")
        # Spacer between results.
        lines.append("")

    sync_footer = await _last_sync_footer()
    if sync_footer:
        lines.append(f"<i>{sync_footer}</i>")

    # Pagination buttons. Encoded as lspr:<s|a>:<page>:<query_hash>[:<query>].
    # The trailing query is included whenever it fits in the 64-byte
    # callback_data budget — that way pagination keeps working even
    # when the click lands on a Railway replica whose ctx.user_data
    # cache is empty (audit fix #6). user_data cache is still
    # populated as a fast-path for the common same-worker case.
    qhash = _query_hash(query)
    s_letter = "a" if scope == "all" else "s"
    pager_state = ctx.user_data.setdefault("lspr_query_cache", {})
    pager_state[qhash] = {"query": query, "scope": scope}
    # Keep the cache from growing forever — only the 8 most recent.
    if len(pager_state) > 8:
        for k in list(pager_state.keys())[:-8]:
            pager_state.pop(k, None)

    def _lspr_cb(p: int) -> str:
        return _build_pager_cb(["lspr", s_letter, str(p)], qhash, query)

    nav_row: list[tuple[str, str]] = []
    if page > 0:
        if page > 1:
            nav_row.append(("⏮ First", _lspr_cb(0)))
        nav_row.append(("◀ Prev", _lspr_cb(page - 1)))
    if end < total:
        next_count = min(_LASTSAMPLE_PAGE_SIZE, total - end)
        nav_row.append((f"Next {next_count} ▶", _lspr_cb(page + 1)))

    again_cb = "lastsample:again_all" if scope == "all" else "lastsample:again"
    btn_rows: list[list[tuple[str, str]]] = []
    if nav_row:
        btn_rows.append(nav_row)
    btn_rows.append([("🔎 Find another", again_cb), ("🏠 Main menu", "menu:home")])

    await send(update, "\n".join(lines).rstrip(), kb(btn_rows))


_CUST_PAGE_SIZE = 10


async def _show_customer_samples(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    mms_name: str,
    customer_name: str,
    scope: str = "self",
    page: int = 0,
) -> None:
    """Render a 10-sample page of samples to ``customer_name`` in ``scope``.

    scope='self'  — only samples this rep (mms_name) sent to that customer.
    scope='all'   — every rep's samples to that customer, with the rep's
                    name shown on each line.
    page          — 0-indexed page number (V1.10.4+). The view paginates
                    in groups of _CUST_PAGE_SIZE so customers with many
                    samples don't blow up the message.

    Pagination state is fully encoded in the callback button (scope, page,
    customer hash) so it survives worker switches. No reliance on
    ctx.user_data for the active page.
    """
    try:
        rows = await _load_lastsample_rows(scope, mms_name)
    except Exception as e:  # noqa: BLE001
        log.exception("lastsample: FSL read failed for customer view")
        await send(update, f"😕 Couldn't read Full Sample Listing: {h(str(e))}", _last_kb(scope))
        return

    target = " ".join(customer_name.lower().split())
    matches = [
        r for r in rows
        if " ".join((r.get("Customer Name") or "").lower().split()) == target
    ]
    if not matches:
        whose = "to" if scope == "all" else "you sent to"
        await send(
            update,
            f"📭 I don't see any samples {whose} <b>{h(customer_name)}</b>.",
            _last_kb(scope),
        )
        return

    from datetime import date as _date
    SENTINEL = _date(1900, 1, 1)
    matches.sort(key=lambda r: r.get("_date") or SENTINEL, reverse=True)

    total = len(matches)
    total_pages = max(1, (total + _CUST_PAGE_SIZE - 1) // _CUST_PAGE_SIZE)
    # Clamp the requested page so a stale callback (e.g. samples got
    # archived between the click and the read) doesn't 404.
    page = max(0, min(int(page or 0), total_pages - 1))
    start = page * _CUST_PAGE_SIZE
    end = min(start + _CUST_PAGE_SIZE, total)
    page_rows = matches[start:end]

    # V1.13.11 — per-rep currency override on customer-view list.
    cust_pref_currency = await _user_pref_currency(update)

    def _fmt_price(raw: str) -> str:
        s = (raw or "").strip()
        if not s:
            return "—"
        return _format_price_for_currency(raw, cust_pref_currency)

    title_suffix = " <i>(all reps)</i>" if scope == "all" else ""
    page_marker = (
        f"  <i>(page {page + 1} of {total_pages}, "
        f"showing {start + 1}–{end} of {total})</i>"
        if total_pages > 1
        else ""
    )
    lines = [
        f"📋 <b>Samples to {h(customer_name)}:</b>"
        f"{title_suffix}{page_marker}",
        "",
    ]
    # Continuous numbering: row 11 on page 2 displays as '11.'
    for i, r in enumerate(page_rows, start + 1):
        d = r.get("_date")
        date_str = d.strftime("%d %b %Y") if d else (r.get("Sample Date Out") or "—")
        name = r.get("Product Name") or "—"
        code = r.get("Product Code") or "—"
        price = _fmt_price(r.get("R&D Price") or "")
        # AWB suffix. The same value the digest shows — real tracking
        # number, '🚗 Hand carry' marker, or '—' when still unmatched.
        awb_raw = (r.get("AWB") or "").strip()
        if not awb_raw:
            awb_suffix = " · AWB —"
        elif _is_hand_carry(awb_raw):
            awb_suffix = " · 🚗 Hand carry"
        else:
            awb_suffix = f" · AWB <code>{h(awb_raw)}</code>"
        if scope == "all":
            sales = (r.get("Sales") or "").strip() or "—"
            lines.append(
                f" {i}. {h(date_str)} · <b>{h(sales)}</b> · {h(name)} · "
                f"<code>{h(code)}</code> · {h(price)}{awb_suffix}"
            )
        else:
            lines.append(
                f" {i}. {h(date_str)} · {h(name)} · <code>{h(code)}</code> · "
                f"{h(price)}{awb_suffix}"
            )

    sync_footer = await _last_sync_footer()
    if sync_footer:
        lines.append("")
        lines.append(f"<i>{sync_footer}</i>")

    # Pagination buttons. Callback format: lspg:<scope>:<page>:<cust_hash>
    # where scope is 's' (self) or 'a' (all). Customer hash is the same
    # md5[:10] used by the lsc / lscall callbacks, so we can re-derive
    # the customer name on whichever worker handles the click.
    cust_hash = _cust_hash(customer_name)
    s_letter = "a" if scope == "all" else "s"
    nav_row: list[tuple[str, str]] = []
    if page > 0:
        # 'Prev' steps back one page — ALWAYS shown when there's a page
        # behind us. Earlier versions hid Prev on page 1 (because First
        # already jumps to 0), but users read the missing ◀ button as
        # 'the bot died' and didn't realise First was the back nav.
        # 'First' is the extra skip-to-start shortcut, only useful from
        # page 2+. Mirrors _show_lastsample_results' nav row.
        if page > 1:
            nav_row.append(("⏮ First", f"lspg:{s_letter}:0:{cust_hash}"))
        nav_row.append(("◀ Prev", f"lspg:{s_letter}:{page - 1}:{cust_hash}"))
    if end < total:
        next_count = min(_CUST_PAGE_SIZE, total - end)
        nav_row.append(
            (f"Next {next_count} ▶", f"lspg:{s_letter}:{page + 1}:{cust_hash}")
        )

    again_cb = "lastsample:again_all" if scope == "all" else "lastsample:again"
    btn_rows = []
    if nav_row:
        btn_rows.append(nav_row)
    btn_rows.append([("🔎 Find another", again_cb), ("🏠 Main menu", "menu:home")])

    await send(update, "\n".join(lines), kb(btn_rows))


# --------------- V1.17.x: universal smart text router ---------------
#
# "Type anything" UX: a plain message with no active flow is identified
# and routed instead of dead-ending at 'no active draft':
#   • product code            → /pp price lookup
#   • sales-rep name (Alex…)  → that rep's recent samples
#   • customer/company name   → samples sent to that company (all reps)
#   • product keywords        → catalog matches with tap-to-price buttons
# Ambiguous input shows one compact message with all plausible readings
# as buttons — one tap to resolve, no menu digging.

# Filler words reps naturally type around a name ("samples for alex",
# "show me datong"). Stripped before identification probes.
_ROUTE_FILLER_RE = re.compile(
    r"\b(?:sample|samples|for|show|me|sent|send|by|to|of|the|please|list|"
    r"from|what|did|out|last|latest|recent|find|search|lookup|look|up)\b",
    re.IGNORECASE,
)


def _route_strip_fillers(text: str) -> str:
    stripped = _ROUTE_FILLER_RE.sub(" ", text)
    stripped = re.sub(r"\s+", " ", stripped).strip()
    return stripped or text.strip()


async def _active_rep_names() -> list[str]:
    """Distinct MMS Names of active rows on the Authorized Users tab."""
    try:
        users = await asyncio.to_thread(sheets.load_users)
    except Exception as e:  # noqa: BLE001
        log.debug("rep-name probe: load_users failed: %s", e)
        return []
    names: list[str] = []
    seen: set[str] = set()
    for row in users:
        active = str(sheets._row_get_loose(row, "Active")).strip().lower()
        if active not in {"y", "yes", "true", "1"}:
            continue
        name = str(sheets._row_get_loose(row, "MMS Name")).strip()
        key = " ".join(name.lower().split())
        if name and key not in seen:
            seen.add(key)
            names.append(name)
    return names


def _match_rep_names(text: str, rep_names: list[str]) -> tuple[list[str], bool]:
    """Match typed text against rep MMS names.

    Returns (matches, strong). Strong = exact full-name or exact
    first-name match ("alex" → "Alex Tan") — safe to route directly.
    Fuzzy (WRatio ≥ 85) hits are weak: offered as buttons only.
    """
    from rapidfuzz import fuzz

    q = " ".join((text or "").lower().split())
    if len(q) < 3:
        return [], False
    strong: list[str] = []
    weak: list[str] = []
    for name in rep_names:
        n = " ".join(name.lower().split())
        if q == n or q == n.split(" ")[0]:
            strong.append(name)
            continue
        if fuzz.WRatio(q, n) >= 85:
            weak.append(name)
    if strong:
        return strong, True
    return weak, False


async def _show_rep_samples(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    rep_name: str,
    page: int = 0,
) -> None:
    """Render a 10-sample page of ``rep_name``'s sent samples, newest first.

    Mirrors _show_customer_samples: pagination state fully encoded in the
    callback (rppg:<page>:<rep_hash>) so it survives worker switches.
    """
    try:
        rows = await _load_lastsample_rows("self", rep_name)
    except Exception as e:  # noqa: BLE001
        log.exception("rep view: FSL read failed")
        await send(
            update,
            f"😕 Couldn't read Full Sample Listing: {_friendly_fetch_error(e)}",
        )
        return

    if not rows:
        await send(
            update,
            f"📭 I don't see any samples sent by <b>{h(rep_name)}</b> in "
            "Full Sample Listing yet.",
            kb([[("🏠 Main menu", "menu:home")]]),
        )
        return

    from datetime import date as _date
    SENTINEL = _date(1900, 1, 1)
    rows.sort(key=lambda r: r.get("_date") or SENTINEL, reverse=True)

    total = len(rows)
    total_pages = max(1, (total + _CUST_PAGE_SIZE - 1) // _CUST_PAGE_SIZE)
    page = max(0, min(int(page or 0), total_pages - 1))
    start = page * _CUST_PAGE_SIZE
    end = min(start + _CUST_PAGE_SIZE, total)
    page_rows = rows[start:end]

    rep_pref_currency = await _user_pref_currency(update)

    page_marker = (
        f"  <i>(page {page + 1} of {total_pages}, "
        f"showing {start + 1}–{end} of {total})</i>"
        if total_pages > 1
        else f"  <i>({total} total)</i>"
    )
    lines = [f"👔 <b>Samples sent by {h(rep_name)}:</b>{page_marker}", ""]
    for i, r in enumerate(page_rows, start + 1):
        d = r.get("_date")
        date_str = d.strftime("%d %b %Y") if d else (r.get("Sample Date Out") or "—")
        cust = (r.get("Customer Name") or "—").strip() or "—"
        name = r.get("Product Name") or "—"
        code = r.get("Product Code") or "—"
        price_raw = (r.get("R&D Price") or "").strip()
        price = _format_price_for_currency(price_raw, rep_pref_currency) if price_raw else "—"
        lines.append(
            f" {i}. {h(date_str)} · <b>{h(cust)}</b> · {h(name)} · "
            f"<code>{h(code)}</code> · {h(price)}"
        )

    sync_footer = await _last_sync_footer()
    if sync_footer:
        lines.append("")
        lines.append(f"<i>{sync_footer}</i>")

    rep_hash = _cust_hash(rep_name)
    pnav: list[tuple[str, str]] = []
    if page > 0:
        if page > 1:
            pnav.append(("⏮ First", f"rppg:0:{rep_hash}"))
        pnav.append(("◀ Prev", f"rppg:{page - 1}:{rep_hash}"))
    if end < total:
        next_count = min(_CUST_PAGE_SIZE, total - end)
        pnav.append((f"Next {next_count} ▶", f"rppg:{page + 1}:{rep_hash}"))

    btn_rows = []
    if pnav:
        btn_rows.append(pnav)
    btn_rows.append([("🏠 Main menu", "menu:home")])
    await send(update, "\n".join(lines), kb(btn_rows))


async def _rep_name_from_hash(target_hash: str) -> str | None:
    """Resolve a rppg:/rep: callback hash back to an MMS name — stateless."""
    for name in await _active_rep_names():
        if _cust_hash(name) == target_hash:
            return name
    return None


async def _smart_route_text(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    text: str,
    header: str | None = None,
) -> None:
    """Identify what the rep typed and route it — no menu taps needed.

    Order of identification:
      1. Product code(s) anywhere in the text → /pp each.
      2. Exact rep name (and no equally-strong customer reading) → that
         rep's recent samples immediately.
      3. Strong customer-master match (score ≥ 90, no rep conflict) →
         all-scope customer search (shows samples to that company).
      4. Anything else that got probe hits → ONE compact message with
         every plausible reading as buttons (rep / customer / product).
      5. Nothing hit at all → all-scope FSL search (product+customer,
         existing disambiguation) as the final net.
    """
    # 1) Embedded codes win outright ("price for S-668U1 please").
    codes = _PP_CODE_RE.findall(text)
    if codes:
        unique = _dedupe_codes(codes, cap=_pp_cap_for(update.effective_user))
        await _run_pp_for_codes(update, unique)
        return

    try:
        await update.effective_chat.send_action("typing")
    except Exception:  # noqa: BLE001
        pass

    probe = _route_strip_fillers(text)

    # --- probes: three independent cached sheets, loaded CONCURRENTLY so
    # the cold path costs one round-trip, not three (V1.17.x perf pass).
    async def _cust_probe() -> list[dict]:
        try:
            master = await asyncio.to_thread(sheets.load_merged_customers)
            return matcher.top_customer_master(probe, master, limit=3)
        except Exception as e:  # noqa: BLE001
            log.warning("smart route: customer probe failed: %s", e)
            return []

    async def _prod_probe() -> list[dict]:
        try:
            seasonings = await asyncio.to_thread(sheets.load_seasonings)
            return matcher.top_seasonings(probe, seasonings, limit=3)
        except Exception as e:  # noqa: BLE001
            log.warning("smart route: product probe failed: %s", e)
            return []

    rep_names, cust_hits, prod_hits = await asyncio.gather(
        _active_rep_names(), _cust_probe(), _prod_probe()
    )
    rep_hits, rep_strong = _match_rep_names(probe, rep_names)
    cust_strong = bool(cust_hits) and cust_hits[0].get("score", 0) >= 90

    # 2) Unambiguous rep → straight to their samples.
    if rep_strong and len(rep_hits) == 1 and not cust_strong:
        await _show_rep_samples(update, ctx, rep_hits[0])
        return

    # 3) Unambiguous customer → straight to their sample history.
    if cust_strong and not rep_hits and not prod_hits:
        await _run_lastsample_search(
            update, ctx, mms_name="", query=cust_hits[0]["name"],
            prev="", mode="customer", scope="all",
        )
        # The lastsample engine re-arms its sticky "next text refines this
        # search" flag. For ROUTER-initiated searches that would hijack the
        # rep's next message (typing 'alex' after a customer lookup must
        # show Alex's samples, not refine the old search) — disarm it so
        # every bare text goes back through identification.
        ctx.user_data.pop("awaiting_lastsample_query", None)
        return

    # 4) Mixed / weak signals → one compact disambiguation message.
    if rep_hits or cust_hits or prod_hits:
        lines = [header or f"🤔 <b>{h(text)}</b> — here's what I found:"]
        buttons: list[list[tuple[str, str]]] = []

        if rep_hits:
            lines.append("")
            lines.append("👔 <b>Sales rep</b> — tap for their sent samples:")
            for name in rep_hits[:3]:
                label = f"👔 {name}"
                if len(label) > 40:
                    label = label[:38] + "…"
                buttons.append([(label, f"rep:{_cust_hash(name)}")])

        if cust_hits:
            lines.append("")
            lines.append("🏢 <b>Customer</b> — tap to see samples sent to them:")
            for c in cust_hits:
                cname = (c.get("name") or "").strip()
                if not cname:
                    continue
                label = f"🏢 {cname}"
                if len(label) > 40:
                    label = label[:38] + "…"
                # lsdall:c: = existing all-scope customer search callback.
                payload = cname.encode("utf-8")[:52].decode("utf-8", errors="ignore")
                buttons.append([(label, f"lsdall:c:{payload}")])

        if prod_hits:
            lines.append("")
            lines.append("🥫 <b>Product</b> — tap for the price:")
            for i, s in enumerate(prod_hits, 1):
                p_code = str(s.get("code") or "").strip().upper()
                price = s.get("price") or "—"
                lines.append(
                    f"  {i}. <b>{h(s['name'])}</b>\n"
                    f"      <code>{h(p_code or '—')}</code> · {h(str(price))}"
                )
                if p_code:
                    label = f"{i}. {p_code} · {s['name']}"
                    if len(label) > 40:
                        label = label[:38] + "…"
                    buttons.append([(label, f"pp:{p_code}")])

        buttons.append([("🔎 Region search", "menu:search"),
                        ("🏠 Main menu", "menu:home")])
        await send(update, "\n".join(lines), kb(buttons))
        return

    # 5) Final net: all-scope FSL search (its auto mode has product vs
    # customer disambiguation and honest no-match messaging built in).
    ctx.user_data["lastsample_scope"] = "all"
    ctx.user_data["lastsample_active_query"] = ""
    await _run_lastsample_search(
        update, ctx, mms_name="", query=probe, prev="", mode="auto", scope="all",
    )
    # Same disarm as step 3 — router-initiated searches must not capture
    # the rep's next message; identification runs fresh every time.
    ctx.user_data.pop("awaiting_lastsample_query", None)


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

    # V1.17.5 — OCR engine health. Answers "why did /scan find nothing?"
    # without needing Railway log access: if RapidOCR failed to load, the
    # bot silently drops to Tesseract, which is poor on photos.
    try:
        lines.append("")
        lines.append("<b>📷 OCR engines</b>")
        for name, status in vision_scan.engine_status().items():
            icon = "✅" if status == "ready" else "❌"
            lines.append(f"{icon} {name}: <code>{h(status)}</code>")
    except Exception as e:  # noqa: BLE001
        lines.append(f"❌ engine check failed: <code>{h(str(e)[:200])}</code>")

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
            # V1.17.x — FSL rows cache is cleared by invalidate_caches
            # too; re-warm all three region tabs concurrently.
            *(asyncio.to_thread(sheets.load_fsl_rows_all, t)
              for t in (sheets.FSL_TAB, sheets.JAKARTA_FSL_TAB,
                        sheets.BANGKOK_FSL_TAB)),
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
    # V1.12.3 — this stage is now CODE-ONLY. Browsing/search has moved to
    # the standalone 🔎 Search seasonings button. If the rep doesn't have
    # a code yet they should pop back to the main menu and search there.
    current = d.data.get("seasoning", "")
    hint = f"\n\nCurrent: <i>{h(current)}</i>" if current else ""
    await send(
        update,
        "🌶 <b>Seasoning Requested</b>\n\n"
        "Paste the <b>product code</b> you want to send a sample of.\n\n"
        "<i>Examples:</i> <code>S-668U1</code>, <code>J-49JS1-03</code>, "
        "<code>B-39HA1-23</code>\n\n"
        "<i>Don't have a code yet? Tap 🏠 Main menu → 🔎 Search seasonings "
        "to find one first.</i>" + hint,
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

async def _code_entry_text_fallback(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str
) -> None:
    """V1.17.x — smarter search when code-entry text isn't a code.

    Delegates to the universal smart router with a context-aware header,
    so the Look-up flow understands rep names, customer names, and product
    keywords exactly like bare chat text does.
    """
    await _smart_route_text(
        update, ctx, text,
        header=(
            f"🤔 <b>{h(text)}</b> isn't a product code — so I searched it "
            "as text instead:"
        ),
    )


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await _authorized(update):
        return
    user = update.effective_user
    text = (update.message.text or "").strip()
    if not text:
        return

    # Defensive: when a user types /sampleupdate (or any slash command)
    # as a REPLY to a previous ForceReply prompt — e.g. the 'Find your
    # last sample' prompt opened by clicking "My samples (me only)" —
    # some Telegram clients (web especially) deliver the message
    # WITHOUT a bot_command entity. PTB's filters.COMMAND then returns
    # False, the message falls through to on_text, and the flag-based
    # lastsample-reply handler treats the literal '/sampleupdate' as
    # a search keyword → "No product found... has /sampleupdate in..."
    #
    # We side-step this by short-circuiting anything that looks like a
    # slash-command at column 0. If CommandHandler didn't catch it,
    # the right behavior is "do nothing" — never search for /xxx.
    # Also clear the awaiting_lastsample_query flag so the user's
    # NEXT real text input starts a fresh search, not a stale one.
    if text.startswith("/"):
        ctx.user_data.pop("awaiting_lastsample_query", None)
        log.info(
            "on_text: ignoring slash-prefixed text %r (looks like a command "
            "that PTB didn't tag — most often a reply to a ForceReply prompt)",
            text[:40],
        )
        return

    # Bulk-paste flow text states (await_paste, ask_budget_amt) run with no
    # active Draft — check those FIRST before the "no draft" guard.
    if await _handle_bulk_text(update, ctx, text):
        return

    # V1.13.14 — fast path for pure code-shape input. If the message is
    # nothing but product codes (one or many, separated by whitespace /
    # commas), route straight to /pp regardless of any flag or draft
    # state. Kills the V1.13.x 'no active draft' dead-end for reps who
    # just paste a code into the chat without tapping a menu button.
    #
    # 'Pure' check: every non-code character must be whitespace or one
    # of a small set of separators. So 'S-668U1' → /pp. So is
    # 'S-668U1, J-49JS1-03' and 'S-668U1 B-74CH7-02 J-49JS1-03'.
    # But 'find spicy chicken S-668U1' keeps falling through to the
    # existing search / draft handlers — there are meaningful words
    # around the code, so user intent isn't purely a price lookup.
    code_hits_fast = _PP_CODE_RE.findall(text)
    if code_hits_fast:
        remainder = _PP_CODE_RE.sub(" ", text)
        remainder_clean = re.sub(r"[\s,;./\\|\-]+", "", remainder)
        if not remainder_clean:
            unique_fast = _dedupe_codes(code_hits_fast, cap=5)
            await _run_pp_for_codes(update, unique_fast)
            return

    # /lastsample reply flow: same per-process flag + reply-detection pattern
    # as the manual code entry below. Whichever signal fires first wins, and
    # we read+clear the cached MMS name so a stale flag from a prior session
    # doesn't leak into the next text the user sends.
    msg = update.effective_message
    replied = getattr(msg, "reply_to_message", None) if msg else None
    has_lastsample_flag = bool(ctx.user_data.pop("awaiting_lastsample_query", None))
    # Match either the self-scope prompt ('Find your last sample') or the
    # admin all-scope prompt ('Find ANY rep's last sample').
    _replied_text = (replied.text or "") if replied else ""
    is_lastsample_reply = bool(
        replied
        and getattr(replied, "from_user", None)
        and getattr(replied.from_user, "is_bot", False)
        and (
            "Find your last sample" in _replied_text
            or "Find ANY rep's last sample" in _replied_text
        )
    )
    if has_lastsample_flag or is_lastsample_reply:
        # Don't pop these — _run_lastsample_search rewrites them after the
        # search so the next text message can refine. cmd_lastsample /
        # cmd_alllastsample (or the 🔎 Find another button) is what clears
        # them on a fresh start.
        #
        # Scope detection (V1.10.7 fix). Prefer the prompt text we're
        # replying to: it's on the actual message, so multi-worker Railway
        # deploys can determine scope without a shared state store. Only
        # fall back to user_data when this isn't a reply path (i.e. flag
        # set on this same worker).
        if is_lastsample_reply and "Find ANY rep's last sample" in _replied_text:
            scope = "all"
        elif is_lastsample_reply and "Find your last sample" in _replied_text:
            scope = "self"
        else:
            scope = ctx.user_data.get("lastsample_scope", "self")
        # In all-scope, mms_name doesn't matter (sheets.load_fsl_rows_all
        # ignores it). In self-scope, we need it.
        mms_name = ctx.user_data.get("lastsample_mms_name", "")
        if scope == "self" and not mms_name:
            # Stateless fallback (multi-replica deploys): re-resolve from sheet.
            mms_name = await asyncio.to_thread(
                sheets.get_user_mms_name, user.id, user.username
            )
            ctx.user_data["lastsample_mms_name"] = mms_name
        if scope == "self" and not mms_name:
            await send(
                update,
                "🛑 I can't see your <b>MMS Name</b> — ask the admin to set "
                "it on the <i>Authorized Users</i> tab, then re-run /lastsample.",
            )
            return
        # Refinement: append the new text to whatever query was active
        # before. A "🔎 Find another" tap wipes the active query, so the
        # next message starts a fresh search.
        #
        # V1.12.10 — two corrections to the chain logic:
        #   (a) Code-shape queries are ALWAYS fresh — never refinements.
        #       A rep typing 'J-49JS1-03' wants that exact code, not
        #       'previous_keyword J-49JS1-03'. Reset active before
        #       combining.
        #   (b) If the new text is identical to the active query (user
        #       accidentally double-sent, or tapped Send twice), don't
        #       stack 'X X' which won't match anything — just re-run
        #       the same search.
        active = (ctx.user_data.get("lastsample_active_query") or "").strip()
        text_stripped = text.strip()
        if _PP_CODE_RE.search(text_stripped.upper()):
            active = ""  # (a)
        if text_stripped == active:
            combined = active  # (b) — same query, no refinement
        else:
            combined = (active + " " + text_stripped).strip() if active else text_stripped
        await _run_lastsample_search(
            update, ctx, mms_name, combined, prev=active, scope=scope,
        )
        return

    # V1.12.0 — Browse-only seasoning search reply flow. Same per-process
    # flag + reply-detection pattern as /lastsample so multi-replica
    # deploys don't lose the context across workers.
    # V1.13.2: flag is sticky (get, not pop) so back-to-back searches
    # work without re-tapping the menu. Cleared by any menu:* nav.
    has_search_flag = bool(ctx.user_data.get("awaiting_search_query"))
    is_search_reply = bool(
        replied
        and getattr(replied, "from_user", None)
        and getattr(replied.from_user, "is_bot", False)
        and "Search" in (replied.text or "")
        and "seasonings" in (replied.text or "")
    )
    if has_search_flag or is_search_reply:
        # Determine region: prefer user_data (most reliable on same worker),
        # fall back to parsing the prompt text we're replying to.
        region = (ctx.user_data.get("search_region") or "").lower()
        if not region and is_search_reply:
            ptext = replied.text or ""
            if "Singapore" in ptext:
                region = "sg"
            elif "Indonesia" in ptext:
                region = "id"
            elif "Thailand" in ptext:
                region = "th"
        if region not in _REGION_TAB:
            await send(
                update,
                "🤔 I lost track of which region you picked — please tap "
                "🔎 <i>Search seasonings</i> again from the main menu.",
                kb([[("🔎 Search seasonings", "menu:search"),
                     ("🏠 Main menu", "menu:home")]]),
            )
            return
        await _run_seasoning_search(update, ctx, region, text)
        return

    # Manual code-entry flow ("✏️ Enter a code" on the main menu): accept
    # text either when the per-process flag is set OR when the message is
    # a reply to one of our "Enter a product code" prompts. Reply-detection
    # makes the flow robust to multi-replica deployments where the click
    # and the typed reply may land on different workers.
    # V1.13.1: flag is sticky (get, not pop) so reps can paste code after
    # code without re-tapping the menu button. Cleared by any menu:* nav
    # in _handle_menu_callback.
    has_code_flag = bool(ctx.user_data.get("awaiting_code_text"))
    is_code_reply = bool(
        replied
        and getattr(replied, "from_user", None)
        and getattr(replied.from_user, "is_bot", False)
        and "Enter a product code" in (replied.text or "")
    )
    if has_code_flag or is_code_reply:
        codes = _PP_CODE_RE.findall(text)
        if not codes:
            # V1.17.x — smarter search: don't dead-end. Treat the text as
            # keywords and probe products + customers, then suggest.
            await _code_entry_text_fallback(update, ctx, text)
            return
        unique = _dedupe_codes(codes, cap=_pp_cap_for(update.effective_user))
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
            return
        # V1.17.x — universal smart router. No flow claimed this text, so
        # instead of the old 'no active draft' dead-end, identify what the
        # rep typed (code / rep name / customer / product keywords) and
        # route it. Typing IS the interface now; menus are the fallback.
        await _smart_route_text(update, ctx, text)
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
    """Code-only entry handler (V1.12.3+).

    The 'find a seasoning' search step has moved to the standalone 🔎
    Search seasonings menu button. This stage now strictly accepts a
    product code — anything else gets a friendly redirect to the search
    flow. Lookup cascade:
      1. Seasoning Master sheet (S-codes — full catalog with category
         and curated price). Best UX when present because category info
         drives downstream stages (e.g. quantity question for oils).
      2. FSL / Jakarta tab via sheets.find_fsl_product_by_code, which
         already auto-routes by [SJTB]- prefix. Picks up J-codes (no
         Master entry yet), legacy B-codes, and any S-code that's in
         the sample tab but not yet in Master.
      3. Otherwise: tell the rep we couldn't find it.
    """
    chat = update.effective_chat
    text = (text or "").strip()

    # Step 0: must be a recognisable code. _PP_CODE_RE matches [SJTB]-XXX
    # with up to 6 suffix segments (same shape /pp accepts).
    code_hits = _PP_CODE_RE.findall(text)
    if not code_hits:
        await send(
            update,
            "🤔 That doesn't look like a product code.\n\n"
            "Please paste a code like <code>S-668U1</code>, "
            "<code>J-49JS1-03</code>, or <code>B-39HA1-23</code>.\n\n"
            "<i>Don't have a code yet? Tap 🏠 Main menu → 🔎 Search "
            "seasonings to find one first.</i>",
            kb([
                [("🏠 Main menu", "menu:home")],
                [("🔎 Search seasonings", "menu:search")],
                nav_row(include_back=False),
            ]),
        )
        return
    code = code_hits[0].upper()

    try:
        await chat.send_action("typing")
    except Exception:  # noqa: BLE001
        pass

    # Step 1: Try the Seasoning Master sheet first (S-codes get richer
    # category data here). find_codes_matching also handles base→variant
    # expansion, so a base "S-668U1" surfaces all "-XX" SKUs for the user
    # to pick from.
    try:
        seasonings = await asyncio.to_thread(sheets.load_seasonings)
    except Exception as e:  # noqa: BLE001
        log.warning("load_seasonings failed (continuing with FSL fallback): %s", e)
        seasonings = []

    code_matches = matcher.find_codes_matching(code, seasonings) if seasonings else []
    if code_matches:
        ctx.user_data["seasoning_candidates"] = code_matches[:5]
        ctx.user_data["seasoning_query"] = code
        if len(code_matches) == 1:
            c = code_matches[0]
            cat = c.get("category") or ""
            cat_str = f" · <i>{h(cat)}</i>" if cat else ""
            price = c.get("price") or "—"
            msg = (
                f"🎯 <b>Code match</b> for <code>{h(code)}</code>:\n\n"
                f"<b>{h(c['name'])}</b>{cat_str}\n"
                f"    code <code>{h(c.get('code') or '—')}</code> · {h(price)}\n\n"
                "Use this product?"
            )
            buttons = [
                [("✅ Yes, use it", "ssn:0")],
                [("🔄 Different code", "ssn:reset")],
                nav_row(include_back=False),
            ]
        else:
            lines = [
                f"🎯 <b>{len(code_matches)} variants</b> for <code>{h(code)}</code> — pick the right one:",
                "",
            ]
            buttons = []
            for i, c in enumerate(code_matches[:5]):
                cat = c.get("category") or ""
                cat_str = f" · <i>{h(cat)}</i>" if cat else ""
                price = c.get("price") or "—"
                lines.append(
                    f"<b>{i+1}. {h(c['name'])}</b>{cat_str}\n"
                    f"    code <code>{h(c.get('code') or '—')}</code> · {h(price)}"
                )
                label = f"{i+1}. {c.get('code', '')} · {c['name']}"
                if len(label) > 40:
                    label = label[:38] + "…"
                buttons.append([(label, f"ssn:{i}")])
            buttons.append([("🔄 Different code", "ssn:reset")])
            buttons.append(nav_row(include_back=False))
            msg = "\n".join(lines)
        await send(update, msg, kb(buttons))
        return

    # Step 2: FSL / Jakarta tab fallback (handles J-, B-, and S-codes that
    # exist in sample history but not in Seasoning Master). find_fsl_
    # product_by_code auto-routes by prefix — S/B → FSL_TAB, J → Jakarta.
    try:
        fsl_row = await asyncio.to_thread(sheets.find_fsl_product_by_code, code)
    except Exception as e:  # noqa: BLE001
        log.warning("find_fsl_product_by_code failed for %s: %s", code, e)
        fsl_row = None

    if fsl_row:
        cand = {
            "code": (fsl_row.get("Product Code") or code).strip().upper(),
            "name": (fsl_row.get("Product Name") or "").strip(),
            "category": (fsl_row.get("Category") or "").strip(),
            "price": (fsl_row.get("R&D Price") or "").strip(),
        }
        ctx.user_data["seasoning_candidates"] = [cand]
        ctx.user_data["seasoning_query"] = code
        cat_str = f" · <i>{h(cand['category'])}</i>" if cand["category"] else ""
        price_str = cand["price"] or "—"
        await send(
            update,
            f"🎯 <b>Code match</b> for <code>{h(code)}</code> "
            "<i>(from sample list)</i>:\n\n"
            f"<b>{h(cand['name'])}</b>{cat_str}\n"
            f"    code <code>{h(cand['code'])}</code> · {h(price_str)}\n\n"
            "Use this product?",
            kb([
                [("✅ Yes, use it", "ssn:0")],
                [("🔄 Different code", "ssn:reset")],
                nav_row(include_back=False),
            ]),
        )
        return

    # Step 3: not found anywhere. V1.17.x — before giving up, offer
    # near-miss catalog codes (one mistyped character away) as candidates
    # the rep can pick directly into the draft.
    close_entries: list[dict] = []
    if seasonings:
        catalog = {
            str(s.get("code", "")).strip().upper()
            for s in seasonings
            if s.get("code")
        }
        for c, _d in matcher.close_code_matches(code, catalog, limit=3):
            m = matcher.find_by_code(c, seasonings)
            if m:
                close_entries.append(m)
    if close_entries:
        ctx.user_data["seasoning_candidates"] = close_entries[:5]
        ctx.user_data["seasoning_query"] = code
        lines = [
            f"🤷 <code>{h(code)}</code> isn't in any catalog — "
            "<b>did you mean one of these?</b>",
            "",
        ]
        buttons = []
        for i, c in enumerate(close_entries[:5]):
            cat = c.get("category") or ""
            cat_str = f" · <i>{h(cat)}</i>" if cat else ""
            price = c.get("price") or "—"
            lines.append(
                f"<b>{i+1}. {h(c['name'])}</b>{cat_str}\n"
                f"    code <code>{h(c.get('code') or '—')}</code> · {h(str(price))}"
            )
            label = f"{i+1}. {c.get('code', '')} · {c['name']}"
            if len(label) > 40:
                label = label[:38] + "…"
            buttons.append([(label, f"ssn:{i}")])
        buttons.append([("🔄 Different code", "ssn:reset")])
        buttons.append(nav_row(include_back=False))
        await send(update, "\n".join(lines), kb(buttons))
        return

    await send(
        update,
        f"🤷 Couldn't find <code>{h(code)}</code> in any catalog.\n\n"
        "Double-check the spelling, or use 🔎 Search seasonings to look it up first.",
        kb([
            [("🔎 Search seasonings", "menu:search")],
            [("🔄 Different code", "ssn:reset")],
            nav_row(include_back=False),
        ]),
    )


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

    # V1.17.x — tap-to-lookup a suggested code ("did you mean" buttons on
    # not-found replies, photo-scan suggestions, retry buttons on errors).
    # Stateless — the code rides in the callback payload, so it works
    # across workers and long after the original message. Lives BEFORE the
    # no-draft guard since these buttons appear outside draft flows.
    if data.startswith("pp:"):
        sugg_code = data.split(":", 1)[1].strip().upper()
        if sugg_code and _PP_CODE_RE.fullmatch(sugg_code):
            await _run_pp_for_codes(update, [sugg_code])
        return

    # /lastsample "Find another" button. Two flavours:
    #   lastsample:again      → reprompt for the rep-scoped search
    #   lastsample:again_all  → reprompt for the admin all-reps search
    # Live BEFORE the no-draft guard since /lastsample doesn't open a draft.
    if data == "lastsample:again":
        await cmd_lastsample(update, ctx)
        return
    if data == "lastsample:again_all":
        await cmd_alllastsample(update, ctx)
        return

    # /lastsample disambiguation — user tapped 'Product Name' or
    # 'Customer Name' on the disambig prompt. Two prefix flavours:
    #   lsd:p:<q> / lsd:c:<q>        → self-scope (rep-only)
    #   lsdall:p:<q> / lsdall:c:<q>  → all-scope (admin)
    # Query is encoded in the callback so the choice survives worker
    # switches without ctx.user_data.
    if data.startswith("lsd:") or data.startswith("lsdall:"):
        ds_scope = "all" if data.startswith("lsdall:") else "self"
        body = data.split(":", 1)[1]  # 'p:<query>' or 'c:<query>'
        parts = body.split(":", 1)
        if len(parts) < 2:
            return
        choice, qtext = parts[0], parts[1]
        if choice not in ("p", "c") or not qtext:
            return
        # Resolve mms only when needed for self-scope.
        mms = ""
        if ds_scope == "self":
            mms = ctx.user_data.get("lastsample_mms_name") or await asyncio.to_thread(
                sheets.get_user_mms_name,
                update.effective_user.id, update.effective_user.username,
            )
            if not mms:
                await send(
                    update,
                    "🛑 I can't see your <b>MMS Name</b> — ask the admin to set it "
                    "on the <i>Authorized Users</i> tab, then re-run /lastsample.",
                )
                return
            ctx.user_data["lastsample_mms_name"] = mms
        ctx.user_data["lastsample_scope"] = ds_scope
        forced = "product" if choice == "p" else "customer"
        await _run_lastsample_search(
            update, ctx, mms, qtext, prev="", mode=forced, scope=ds_scope,
        )
        return

    # /lastsample customer pick — user tapped a customer suggestion. Two
    # prefix flavours:
    #   lsc:<hash>     → self-scope; load this rep's FSL rows
    #   lscall:<hash>  → all-scope (admin); load the entire FSL
    # Hash is first 10 hex chars of the customer name's md5; we re-derive
    # the candidate set on whichever worker handles the click. State-free.
    if data.startswith("lsc:") or data.startswith("lscall:"):
        cs_scope = "all" if data.startswith("lscall:") else "self"
        target_hash = data.split(":", 1)[1].strip()
        if not target_hash:
            return
        mms = ""
        if cs_scope == "self":
            mms = ctx.user_data.get("lastsample_mms_name") or await asyncio.to_thread(
                sheets.get_user_mms_name,
                update.effective_user.id, update.effective_user.username,
            )
            if not mms:
                await send(
                    update,
                    "🛑 I can't see your <b>MMS Name</b> — ask the admin to set it "
                    "on the <i>Authorized Users</i> tab, then re-run /lastsample.",
                )
                return
            ctx.user_data["lastsample_mms_name"] = mms
        ctx.user_data["lastsample_scope"] = cs_scope
        # Re-derive the unique customer set in the right scope (both regions).
        try:
            rows = await _load_lastsample_rows(cs_scope, mms)
        except Exception as e:  # noqa: BLE001
            log.exception("lastsample: FSL read failed during customer-pick callback")
            await send(
                update,
                f"😕 Couldn't read Full Sample Listing: {h(str(e))}",
                _last_kb(cs_scope),
            )
            return
        seen_customers: set[str] = set()
        for r in rows:
            cust = (r.get("Customer Name") or "").strip()
            if cust:
                seen_customers.add(cust)
        chosen: str | None = next(
            (c for c in seen_customers if _cust_hash(c) == target_hash),
            None,
        )
        if chosen is None:
            log.warning(
                "lsc callback: no customer matches hash %s for scope=%s mms=%r "
                "(saw %d customers)",
                target_hash, cs_scope, mms, len(seen_customers),
            )
            await send(
                update,
                "🤔 Couldn't match that customer in the latest data — please "
                "tap 🔎 Find another and search again.",
                _last_kb(cs_scope),
            )
            return
        await _show_customer_samples(update, ctx, mms, chosen, scope=cs_scope)
        return

    # V1.17.x — "last sample of this code" buttons under a /pp price
    # reply. Callback format: lsx:<s|a>:<code>
    #   s → the tapping rep's own samples (resolves their MMS Name)
    #   a → any rep's samples (all-scope)
    # Stateless: the code rides in the payload. Runs the lastsample
    # search in product mode so the code is matched against Product Code.
    if data.startswith("lsx:"):
        lsx_parts = data.split(":", 2)
        if len(lsx_parts) < 3:
            return
        lsx_scope = "all" if lsx_parts[1] == "a" else "self"
        lsx_code = lsx_parts[2].strip().upper()
        if not lsx_code:
            return
        lsx_mms = ""
        if lsx_scope == "self":
            lsx_mms = ctx.user_data.get("lastsample_mms_name") or await asyncio.to_thread(
                sheets.get_user_mms_name,
                update.effective_user.id, update.effective_user.username,
            )
            if not lsx_mms:
                await send(
                    update,
                    "🛑 I can't see your <b>MMS Name</b> — ask the admin to set it "
                    "on the <i>Authorized Users</i> tab, then try again.",
                )
                return
            ctx.user_data["lastsample_mms_name"] = lsx_mms
        ctx.user_data["lastsample_scope"] = lsx_scope
        await _run_lastsample_search(
            update, ctx, lsx_mms, lsx_code, prev="", mode="product", scope=lsx_scope,
        )
        # Same disarm as the smart router: a button-initiated search must
        # not capture the rep's next typed message.
        ctx.user_data.pop("awaiting_lastsample_query", None)
        return

    # V1.17.x — smart-router rep pick + pagination. Stateless: the rep is
    # identified by md5[:10] hash of their MMS Name, re-derived from the
    # Authorized Users tab on whichever worker handles the click.
    #   rep:<hash>          → first page of that rep's sent samples
    #   rppg:<page>:<hash>  → pagination within the rep view
    if data.startswith("rep:") or data.startswith("rppg:"):
        if data.startswith("rep:"):
            rep_hash, rep_page = data.split(":", 1)[1].strip(), 0
        else:
            parts = data.split(":", 2)
            if len(parts) < 3:
                return
            try:
                rep_page = int(parts[1])
            except ValueError:
                return
            rep_hash = parts[2].strip()
        rep_name = await _rep_name_from_hash(rep_hash)
        if not rep_name:
            await send(
                update,
                "🤔 Couldn't match that rep in the latest Authorized Users "
                "list — they may have been renamed. Please type the name again.",
                kb([[("🏠 Main menu", "menu:home")]]),
            )
            return
        await _show_rep_samples(update, ctx, rep_name, page=rep_page)
        return

    # /lastsample customer pagination — user tapped First / Prev / Next on
    # the customer 10-sample view. Callback format:
    #   lspg:<s|a>:<page>:<cust_hash>
    # Everything needed to render the page is encoded here, so the
    # callback works across worker switches and process restarts.
    if data.startswith("lspg:"):
        parts = data.split(":", 3)
        if len(parts) < 4:
            return
        _, scope_letter, page_str, cust_hash = parts
        cs_scope = "all" if scope_letter == "a" else "self"
        try:
            page = int(page_str)
        except ValueError:
            return
        mms = ""
        if cs_scope == "self":
            mms = ctx.user_data.get("lastsample_mms_name") or await asyncio.to_thread(
                sheets.get_user_mms_name,
                update.effective_user.id, update.effective_user.username,
            )
            if not mms:
                await send(
                    update,
                    "🛑 I can't see your <b>MMS Name</b> — ask the admin to set it "
                    "on the <i>Authorized Users</i> tab, then re-run /lastsample.",
                )
                return
            ctx.user_data["lastsample_mms_name"] = mms
        ctx.user_data["lastsample_scope"] = cs_scope
        # Re-derive the customer name from its hash by scanning both regions
        # in the right scope.
        try:
            rows = await _load_lastsample_rows(cs_scope, mms)
        except Exception as e:  # noqa: BLE001
            log.exception("lastsample: FSL read failed during paginate callback")
            await send(
                update,
                f"😕 Couldn't read Full Sample Listing: {h(str(e))}",
                _last_kb(cs_scope),
            )
            return
        seen_customers: set[str] = set()
        for r in rows:
            cust = (r.get("Customer Name") or "").strip()
            if cust:
                seen_customers.add(cust)
        chosen: str | None = next(
            (c for c in seen_customers if _cust_hash(c) == cust_hash),
            None,
        )
        if chosen is None:
            await send(
                update,
                "🤔 Couldn't match that customer in the latest data — please "
                "tap 🔎 Find another and search again.",
                _last_kb(cs_scope),
            )
            return
        await _show_customer_samples(update, ctx, mms, chosen, scope=cs_scope, page=page)
        return

    # V1.12.7 — /lastsample + /alllastsample product-results pagination.
    # Format: lspr:<s|a>:<page>:<query_hash>. We recover the query from
    # ctx.user_data['lspr_query_cache']; on a cache miss (e.g. multi-
    # replica deploy where the click landed on a different worker), we
    # ask the rep to re-search. Could be made fully stateless by
    # encoding the query in the callback like lsd: does, but query can
    # be long (refined multi-word) and Telegram caps callback_data at
    # 64 bytes — the cache approach is simpler.
    if data.startswith("lspr:"):
        # Format: lspr:<s|a>:<page>:<qhash>[:<query>] — the trailing
        # query is present when it fit in 64 bytes (audit fix #6).
        # Use maxsplit=4 so a query that itself contains ':' stays
        # intact in the final group.
        parts = data.split(":", 4)
        if len(parts) < 4:
            return
        _, scope_letter, page_str, qhash = parts[:4]
        embedded_query = parts[4] if len(parts) > 4 else None
        cs_scope = "all" if scope_letter == "a" else "self"
        try:
            page = int(page_str)
        except ValueError:
            return
        # Prefer the embedded query (works across workers); fall back
        # to the per-worker user_data cache.
        query: str | None = embedded_query
        if not query:
            pager_state = ctx.user_data.get("lspr_query_cache", {}) or {}
            cached = pager_state.get(qhash)
            if cached:
                query = cached.get("query")
        if not query:
            await send(
                update,
                "🤔 The search context expired (probably a redeploy). "
                "Please tap 🔎 Find another to start a new search.",
                _last_kb(cs_scope),
            )
            return
        # Resolve mms_name for self-scope (cache → sheet fallback).
        mms = ""
        if cs_scope == "self":
            mms = ctx.user_data.get("lastsample_mms_name") or await asyncio.to_thread(
                sheets.get_user_mms_name,
                update.effective_user.id, update.effective_user.username,
            )
            if not mms:
                await send(
                    update,
                    "🛑 I can't see your <b>MMS Name</b> — ask the admin to "
                    "set it on the <i>Authorized Users</i> tab.",
                )
                return
            ctx.user_data["lastsample_mms_name"] = mms
        # Reload (both regions) + filter using the same matcher.
        try:
            rows = await _load_lastsample_rows(cs_scope, mms)
        except Exception as e:  # noqa: BLE001
            log.exception("lastsample pagination read failed")
            await send(
                update,
                f"😕 Couldn't read Full Sample Listing: {h(str(e))}",
                _last_kb(cs_scope),
            )
            return
        candidates = _filter_lastsample_products(rows, query)
        if not candidates:
            await send(
                update,
                f"🤷 No matches for <b>{h(query)}</b> in the current sample "
                "list (data may have changed since your search).",
                _last_kb(cs_scope),
            )
            return
        await _show_lastsample_results(
            update, ctx, candidates, query=query, scope=cs_scope, page=page,
        )
        return

    # V1.13.4 — 🔎 Search seasonings pagination. Format:
    #   srpg:<region>:<page>:<query_hash>
    # We re-run _run_seasoning_search with the saved query + region; the
    # function already computes everything from scratch and slices to the
    # requested page. Multi-replica safe via the user_data cache; cache
    # miss falls back to a friendly retry prompt.
    if data.startswith("srpg:"):
        # Format: srpg:<region>:<page>:<qhash>[:<query>] — see lspr
        # handler for the embedded-query rationale (audit fix #6).
        parts = data.split(":", 4)
        if len(parts) < 4:
            return
        _, sr_region, sr_page_str, sr_qhash = parts[:4]
        sr_embedded_query = parts[4] if len(parts) > 4 else None
        try:
            sr_page = int(sr_page_str)
        except ValueError:
            return
        # Prefer embedded query (cross-worker safe); fall back to cache.
        sr_query: str | None = sr_embedded_query
        sr_resolved_region = sr_region
        if not sr_query:
            sr_state = ctx.user_data.get("srpg_query_cache", {}) or {}
            sr_cached = sr_state.get(sr_qhash)
            if sr_cached:
                sr_query = sr_cached.get("query")
                sr_resolved_region = sr_cached.get("region") or sr_region
        if not sr_query:
            await send(
                update,
                "🤔 The search context expired (probably a redeploy). "
                "Please tap 🔎 Search again to start a new search.",
                kb([[("🔎 Search again", "menu:search"),
                     ("🏠 Main menu", "menu:home")]]),
            )
            return
        await _run_seasoning_search(
            update, ctx,
            region=sr_resolved_region,
            query=sr_query,
            page=sr_page,
        )
        return

    # V1.13.12 — escape hatches shown when self-scope /lastsample
    # returns no match. Two callbacks, both encode the query/code
    # directly in callback_data (no user_data dependency — multi-
    # replica safe).
    if data.startswith("lsall:"):
        # Switch to all-reps scope and re-run the search.
        ls_query = data[len("lsall:"):]
        ctx.user_data["lastsample_scope"] = "all"
        ctx.user_data["lastsample_active_query"] = ""
        await _run_lastsample_search(
            update, ctx, mms_name="", query=ls_query, prev="", scope="all",
        )
        return
    if data.startswith("lspp:"):
        # Direct /pp lookup on a code the rep already typed.
        ls_code = data[len("lspp:"):].strip().upper()
        if ls_code:
            await _run_pp_for_codes(update, [ls_code])
        return

    # Main menu and /samples browsing work with or without a draft.
    if data.startswith("menu:"):
        await _handle_menu_callback(update, ctx, data.split(":", 1)[1])
        return
    if data.startswith("samp:"):
        await _handle_samples_callback(update, ctx, data.split(":", 1)[1])
        return
    # Browse-only seasoning search (V1.12.0). Region picker callbacks land
    # here. Independent of any draft — search is exploratory, doesn't raise
    # a sample request.
    if data.startswith("srch:"):
        await _handle_search_callback(update, ctx, data.split(":", 1)[1])
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
    # V1.12.10 — clear ALL awaiting-text flags when the rep navigates the
    # menu, so a stale one from an abandoned earlier flow (e.g. tapped
    # ✏️ Enter a code, never replied, then tapped 🌐 What everyone Send
    # ah and typed) can't cross-route the next text to /pp instead of
    # /alllastsample. The action-specific branch below will re-set the
    # right flag for whichever flow the rep actually picked. Without
    # this, on multi-replica deploys the worker that still holds the
    # stale flag wins the routing race.
    ctx.user_data.pop("awaiting_code_text", None)
    ctx.user_data.pop("awaiting_lastsample_query", None)
    ctx.user_data.pop("awaiting_search_query", None)
    # V1.13.7: clear the search refinement chain on every menu nav so
    # going home or tapping anything resets to a clean search state.
    ctx.user_data.pop("last_search_query", None)
    ctx.user_data.pop("last_search_region", None)
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
    if action == "quote":
        await cmd_quote(update, ctx)
        return
    if action == "search":
        # V1.12.0 — browse-only seasoning search (does NOT raise a sample
        # request). Asks region first, then takes free text or a code.
        await _start_seasoning_search(update, ctx)
        return
    if action == "scan":
        ctx.user_data["awaiting_scan_photo"] = True
        await send(
            update,
            "📷 <b>Scan a product photo</b>\n\n"
            "<b>📎 Reply to this message</b> with a photo of one or more "
            "product code labels (<code>S-XXXXX-XX</code>). I'll read them "
            "and pull the price for each.\n\n"
            "💡 <b>Codes small or far away? Send it as a FILE</b> "
            "(📎 → <i>File</i>) instead of a photo — Telegram shrinks normal "
            "photos to ~1280px and small print becomes unreadable. As a file "
            "I get your full-resolution original.\n\n"
            "<i>Why reply? In group chats, Telegram's privacy mode hides "
            "non-reply messages from bots. In a DM you can just send the photo.</i>",
        )
        return
    if action == "code":
        ctx.user_data["awaiting_code_text"] = True
        await send(
            update,
            "✏️ <b>Enter a product code</b>\n\n"
            "<b>📎 Reply to this message</b> with one or more product codes "
            "and I'll pull the price for each. You can paste a base code "
            "like <code>S-668U1</code> and I'll list all its variants — or "
            "paste up to 5 full codes separated by spaces.",
        )
        return
    if action == "lookup":
        # V1.13.15 — combined code-entry + photo-scan. Arm BOTH flags so
        # the rep can send either a typed code, a photo of a label, or
        # a voice message (voice always works). The existing handlers
        # for text / photo / voice each pick up the right flag.
        ctx.user_data["awaiting_code_text"] = True
        ctx.user_data["awaiting_scan_photo"] = True
        await send(
            update,
            "💲 <b>Look up product code</b>\n\n"
            "<b>📎 Reply to this message</b> with any of:\n"
            "  • <b>Typed code(s)</b> — e.g. <code>S-668U1</code>, or up "
            "to 5 codes separated by spaces\n"
            "  • <b>Photo</b> of a product label (I'll OCR the codes)\n"
            "  • <b>Voice message</b> saying the code (e.g. "
            "<i>'S dash 668 U 1'</i>)\n\n"
            "<i>Codes are auto-routed by prefix — S- for Singapore, "
            "B- for Thailand, J- for Indonesia.</i>",
        )
        return
    if action == "lastsample":
        # Same flow as the /lastsample command — reuse it so the prompt and
        # MMS-name lookup logic stay in one place.
        await cmd_lastsample(update, ctx)
        return
    if action == "alllastsample":
        # Admin-only entry — cmd_alllastsample re-checks the gate, so even
        # if a non-admin somehow gets this callback (forwarded button etc.)
        # they're refused.
        await cmd_alllastsample(update, ctx)
        return
    # menu:updsample retired in V1.7.1 — sync is now automated weekly. If
    # a stale menu message gets tapped, route to the home menu so the user
    # isn't left staring at a broken button.
    if action == "updsample":
        await cmd_start(update, ctx)
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
        # Audit fix #13 — also clear any in-flight bulk session state.
        # Without this, a rep mid-bulk who cancels the per-item draft
        # leaves bulk_parsed / bulk_stage / bulk_customer_carry in
        # user_data; a later /start or unrelated message can trip the
        # bulk-state branch in on_message with stale data. The /bulk
        # entry point does re-clear, but reps don't know that's the
        # only escape hatch.
        was_in_bulk = "_bulk_idx" in d.data
        state.clear(d.user_id)
        # Mirrors the keys cmd_bulk re-initialises on entry — verified
        # against `user_data["bulk_*"]` call sites in this file.
        for key in (
            "bulk_parsed", "bulk_stage", "bulk_raw", "bulk_shared",
            "bulk_customer_carry", "bulk_crossfill",
            "bulk_tokens_in", "bulk_tokens_out",
        ):
            ctx.user_data.pop(key, None)
        msg = (
            "✖ Bulk session and draft both discarded."
            if was_in_bulk
            else "✖ Draft discarded."
        )
        await send(
            update,
            msg,
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
    # V1.12.3: 'retry' (search-by-name) and 'raw' (use-text-as-is) paths
    # are gone. The seasoning stage is now strictly code-only. We treat
    # legacy 'retry' callbacks the same as 'reset' so old menu messages
    # don't break for users who tap them after a deploy.
    if payload in ("retry", "reset"):
        ctx.user_data.pop("seasoning_candidates", None)
        ctx.user_data.pop("seasoning_queries", None)
        ctx.user_data.pop("seasoning_query", None)
        await q_seasoning(update, ctx, d)
        return
    # 'raw' — kept as a no-op redirect so any stray callback from a
    # pre-V1.12.3 message also routes back to the prompt cleanly.
    if payload == "raw":
        ctx.user_data.pop("seasoning_candidates", None)
        ctx.user_data.pop("seasoning_queries", None)
        ctx.user_data.pop("seasoning_query", None)
        await q_seasoning(update, ctx, d)
        return
    else:
        # Audit fix #14 — surface stale legacy payloads instead of
        # silently returning. Old menu messages still tagged with
        # callback formats from before V1.12.3 would otherwise look
        # like dead buttons.
        try:
            idx = int(payload)
        except ValueError:
            await send(
                update,
                "🤔 That button is from an older version of the menu — "
                "it doesn't apply to your current draft. Type the "
                "seasoning code again to refresh the options.",
            )
            return
        cands = ctx.user_data.get("seasoning_candidates") or []
        # Audit fix #5 — if the candidates cache is empty (worker
        # switch / redeploy between rendering the menu and the click),
        # we MUST NOT silently advance the draft with the seasoning
        # field blank. That used to produce sheet rows with empty
        # Matched Code / Matched Price after a deploy. Tell the user
        # to retype the code so we can rebuild the candidate list.
        if not cands:
            await send(
                update,
                "🤔 I lost the seasoning options list (probably a bot "
                "redeploy between you seeing the buttons and tapping). "
                "Please <b>type the seasoning code again</b> so I can "
                "rebuild the picker.",
            )
            return
        if not (0 <= idx < len(cands)):
            await send(
                update,
                "🤔 That option isn't in the current list anymore — "
                "type the seasoning code again to refresh the options.",
            )
            return
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
    # Audit fix #3 — refuse the click when the draft has already moved
    # past the company_name stage. Otherwise a stale `co:N` button (from
    # scrolling up to an earlier suggestion list) silently overwrites
    # the customer mid-draft and jumps d.sub to "confirm_address" while
    # d.stage still points at e.g. "deadline" — the next ca:yes/no then
    # runs _advance from a corrupted state.
    if d.stage != "company_name":
        await send(
            update,
            "🤔 That customer button is from an earlier point in this "
            "draft — you've moved past picking the customer. If you "
            "want to change the customer, go to <b>Review</b> and tap "
            "✏️ Edit on the Customer Company Name field.",
        )
        return
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
    # Audit fix #4 — encode customer idx in the callback prefix so the
    # next-page click can rebuild the same view even if it lands on a
    # different Railway worker (whose ctx.user_data is empty). Before:
    # the click would default to idx=0 and silently show customer #1's
    # samples labelled as if they were customer #N's.
    rows_btns = [_page_nav_row(page, total, f"samp:custpage:{cust_idx}")]
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
    if action.startswith("custpage:"):
        # Two formats accepted:
        #   custpage:<idx>:page:<p>  — new, worker-safe (audit fix #4)
        #   custpage:page:<p>        — legacy, falls back to user_data
        parts = action.split(":")
        try:
            if len(parts) >= 4 and parts[2] == "page":
                # custpage : <idx> : page : <p>
                idx = int(parts[1])
                p = int(parts[3])
            elif len(parts) >= 3 and parts[1] == "page":
                # legacy custpage:page:<p>
                p = int(parts[2])
                idx = ctx.user_data.get("samp_current_cust_idx", 0)
            else:
                return
        except ValueError:
            return
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
        # V1.17.x — say WHAT went wrong in rep language, not just
        # "something went wrong". Network / rate-limit problems get their
        # own wording so reps know retrying is the fix (not a bug report).
        err = ctx.error
        from telegram.error import NetworkError, RetryAfter, TimedOut
        if isinstance(err, RetryAfter):
            wait_s = int(getattr(err, "retry_after", 5) or 5)
            msg = (
                f"🐢 Telegram is rate-limiting me — please wait "
                f"~{wait_s} seconds and try again."
            )
        elif isinstance(err, (NetworkError, TimedOut)):
            msg = (
                "📶 Network hiccup — I lost the connection for a moment. "
                "Your last action may not have gone through; please try "
                "it again."
            )
        else:
            msg = "⚠️ Something went wrong. Please try again or /cancel."
        try:
            await send(update, msg)
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


async def _weekly_mms_sync_job(ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """JobQueue callback — runs MMS → Full Sample Listing sync.

    The actual fetch + enrich + append lives in `sync_engine.run_mms_to_fsl_sync`,
    a synchronous function we offload to a thread so the bot's event loop
    isn't blocked by gspread / requests.

    force=True so the scheduled run never gets blocked by the 24h
    cooldown in sync_engine. The cooldown is meant to stop accidental
    manual overlap, not block the scheduler itself. Without force,
    running daily (Mon-Fri 17:40 SGT) hits the cooldown 4 out of 5
    weekdays and silently no-ops.
    """
    import sync_engine
    log.info("weekly_mms_sync_job: starting")

    async def _alert(text: str) -> None:
        """Shout about a broken sync instead of burying it in a log line.

        V1.17.11 — an expired MMS password stopped ALL scraping for 3 days
        and nobody noticed: run_mms_to_fsl_sync returned
        {"status": "error", "error": "MMS login failed (check creds)"} and
        that went straight into a log file no human reads. Silent data-loss
        is the worst failure mode this bot has, so a failed sync now posts to
        the digest chat where the team already looks every evening.
        """
        chat_id = config.DAILY_DIGEST_CHAT_ID
        if not chat_id:
            log.warning("sync alert not sent — DAILY_DIGEST_CHAT_ID unset: %s", text)
            return
        try:
            await ctx.bot.send_message(
                chat_id=chat_id, text=text, parse_mode=ParseMode.HTML,
            )
        except Exception as e:  # noqa: BLE001 — alerting must never crash the job
            log.warning("sync alert send failed: %s", e)

    try:
        result = await asyncio.to_thread(sync_engine.run_mms_to_fsl_sync, True)
        log.info("weekly_mms_sync_job: result %s", result)
        if isinstance(result, dict) and result.get("status") == "error":
            err = str(result.get("error", "unknown"))
            hint = ""
            if "login" in err.lower():
                hint = (
                    "\n\n<i>MMS is rejecting the bot's credentials. Someone "
                    "needs to update <code>MMS_PASSWORD</code> in Railway → "
                    "NPSampleBot → Variables. Until then NO new samples are "
                    "being recorded.</i>"
                )
            await _alert(
                "🚨 <b>MMS sync FAILED</b> — no new samples were imported.\n"
                f"<code>{h(err)}</code>{hint}"
            )
    except Exception as e:  # noqa: BLE001
        log.exception("weekly_mms_sync_job failed: %s", e)
        await _alert(
            "🚨 <b>MMS sync CRASHED</b> — no new samples were imported.\n"
            f"<code>{h(str(e)[:300])}</code>"
        )


# --------------------------- V1.13.13: daily sample digest ---------------------------
#
# Every weekday at 18:00 Asia/Singapore, post a list of every sample
# logged that calendar day (SGT date) to the configured group chat.
# Across all 3 regions (Singapore + Indonesia + Thailand). Zero-sample
# days still post 'No samples today' so the group knows the bot is
# alive and the schedule isn't broken.

def _today_sgt() -> "object":
    """Return today's date in Asia/Singapore."""
    from datetime import datetime as _dt
    return _dt.now(ZoneInfo("Asia/Singapore")).date()


def _fmt_digest_price(raw: str) -> str:
    """Compact price formatter for the digest line.

    Bare numeric → 'USD X.XX'. Currency-prefixed pass-through. Empty
    cell → '—' so the line still parses cleanly even when MMS didn't
    record an R&D price for that variant.
    """
    s = (raw or "").strip()
    if not s:
        return "—"
    try:
        v = float(s.replace(",", ""))
        return f"USD {v:.2f}"
    except ValueError:
        return s


def _is_hand_carry(awb_value: str) -> bool:
    """Detect manually-entered hand-carry markers in the AWB cell.

    Reps mark a row as hand-delivered by typing one of these in the AWB
    column (case + whitespace agnostic). The sync's skip-if-non-empty
    rule preserves the value across re-runs.
    """
    s = (awb_value or "").strip().upper()
    if not s:
        return False
    return s in {"HAND CARRY", "HAND-CARRY", "HANDCARRY", "HC"} or \
        s.startswith("HAND ")


async def _build_daily_digest_body() -> tuple[str, int]:
    """Read all 3 FSL tabs, filter to today's SGT date, build the
    formatted digest body. Returns (html_body, total_sample_count).

    Pure data → string. No Telegram I/O. Extracted from
    _daily_sample_digest_job so cmd_sampleupdate can echo a preview
    of the exact same body back to the admin who triggered it
    (audit V1.13.14, UX D).
    """
    today = _today_sgt()
    log.info("daily_digest: building for SGT date %s", today)

    # Load alias map once and reverse it (FSL Customer Name → carrier
    # label name). Used to show 'SARL HYGIENIX MANUFACTURE COMPANY' in
    # parens alongside the FSL-side 'Daiya Food'. Multiple aliases per
    # FSL name are joined with ' / ' on display. Lookup key is lower-
    # cased to match the carrier name regardless of capitalisation
    # variations between the alias sheet and the FSL row.
    try:
        raw_aliases = await asyncio.to_thread(sheets.load_customer_aliases)
    except Exception as e:  # noqa: BLE001
        log.warning("daily_digest: alias load failed (continuing): %s", e)
        raw_aliases = {}
    reverse_aliases: dict[str, list[str]] = {}
    for carrier_name, fsl_name in raw_aliases.items():
        key = (fsl_name or "").strip().lower()
        if not key or not carrier_name:
            continue
        reverse_aliases.setdefault(key, []).append(carrier_name.strip())

    # Region order fixed (SG → ID → TH) for predictable scanning.
    region_specs = (
        ("Singapore / Intl", "🇸🇬", sheets.FSL_TAB, True),   # show country
        ("Indonesia",        "🇮🇩", sheets.JAKARTA_FSL_TAB, False),
        ("Thailand",         "🇹🇭", sheets.BANGKOK_FSL_TAB, False),
    )
    by_region: dict[str, list[dict]] = {}
    total = 0
    for label, _flag, tab, _show_country in region_specs:
        bucket: list[dict] = []
        try:
            rows = await asyncio.to_thread(sheets.load_fsl_rows_all, tab)
        except Exception as e:  # noqa: BLE001
            log.warning("daily_digest: failed to read tab %r: %s", tab, e)
            by_region[label] = bucket
            continue
        for r in rows:
            if r.get("_date") == today:
                bucket.append(r)
        # Sort so customer rows cluster together under each salesperson —
        # this is what lets the "▸ Customer — N samples" collapse work
        # without us having to re-bucket after the fact.
        bucket.sort(
            key=lambda r: (
                (r.get("Sales") or "").lower(),
                (r.get("Customer Name") or "").lower(),
                (r.get("Product Name") or "").lower(),
            )
        )
        by_region[label] = bucket
        total += len(bucket)

    pretty_date = today.strftime("%a, %d %b %Y")
    if total == 0:
        body = (
            f"📋 <b>This is all the sample send today ah! — {pretty_date}</b>\n\n"
            "🪴 <i>No samples logged today across all 3 factories.</i>"
        )
        return body, 0

    # Footer aggregates — tallied as we walk the regions below.
    sales_totals: dict[str, int] = {}
    customer_keys: set[str] = set()

    lines = [
        f"📋 <b>This is all the sample send today ah! — {pretty_date}</b>",
        f"<i>{total} sample{'s' if total != 1 else ''} across all "
        "3 factories.</i>",
    ]
    for label, flag, _tab, show_country in region_specs:
        bucket = by_region.get(label, [])
        n = len(bucket)
        lines.append("")
        lines.append(
            f"{flag} <b>{h(label)}</b> — "
            f"{n} sample{'s' if n != 1 else ''}"
        )
        if not bucket:
            lines.append("<i>No samples today.</i>")
            continue

        # Group: salesperson → customer (with country if shown) → samples.
        # Dict insertion order preserved from the sorted bucket above.
        by_sales: dict[str, dict[str, list[dict]]] = {}
        for r in bucket:
            sales = (r.get("Sales") or "—").strip() or "—"
            customer = (r.get("Customer Name") or "—").strip() or "—"
            country = (r.get("Country") or "").strip()
            cust_key = (
                f"{customer} ({country})"
                if show_country and country
                else customer
            )
            by_sales.setdefault(sales, {}).setdefault(cust_key, []).append(r)

        # Region-level chip: "👥 Alex (10) · Eric (2) · Jay (1) · Melissa (2)"
        chips = []
        for s in sorted(by_sales, key=str.lower):
            cnt = sum(len(v) for v in by_sales[s].values())
            chips.append(f"<b>{h(s)}</b> ({cnt})")
            sales_totals[s] = sales_totals.get(s, 0) + cnt
        lines.append("👥 " + " · ".join(chips))

        for sales in sorted(by_sales, key=str.lower):
            customers = by_sales[sales]
            lines.append("")
            lines.append(f"👤 <b>{h(sales)}</b>")
            for cust_label, samples in customers.items():
                customer_keys.add(f"{label}::{cust_label}")
                cnt = len(samples)
                suffix = f" — {cnt} samples" if cnt > 1 else ""

                # Build the parenthesised header parts:
                #   (FSL Name) (Carrier Name) (Country)
                # The carrier name appears only when there's a known
                # alias from the OPS sheet's 'AWB Customer Aliases'
                # tab (reverse lookup). Country only on regions where
                # show_country is True (Singapore / Intl).
                bare_customer = (
                    samples[0].get("Customer Name") or ""
                ).strip() or "—"
                country = (samples[0].get("Country") or "").strip()
                aliases_for_this = reverse_aliases.get(
                    bare_customer.lower(), []
                )
                header_parts = [f"({h(bare_customer)})"]
                if aliases_for_this:
                    carrier_display = " / ".join(aliases_for_this)
                    header_parts.append(f"({h(carrier_display)})")
                if show_country and country:
                    header_parts.append(f"({h(country)})")
                lines.append(
                    f"   ▸ {' '.join(header_parts)}{suffix}"
                )

                # Sample bullets
                for s in samples:
                    name = (s.get("Product Name") or "—").strip() or "—"
                    code = (s.get("Product Code") or "—").strip() or "—"
                    qty_raw = (s.get("Quantity (g)") or "").strip()
                    qty = f"{qty_raw}g" if qty_raw else "—"
                    price = _fmt_digest_price(s.get("R&D Price") or "")
                    lines.append(
                        f"       • {h(name)} · <code>{h(code)}</code> · "
                        f"{h(qty)} · R&amp;D {h(price)}"
                    )

                # AWB Number on its own line AFTER the samples. The
                # matcher fills every row in the same customer/date
                # block with the same AWB (one DHL box → many sample
                # bags), so usually one distinct value. Multiple
                # distinct AWBs only happen if a customer got two
                # separate shipments on the same day — joined with
                # ' / ' so the digest doesn't silently hide one.
                # Missing AWBs show as '—' so the reader knows it's
                # unmapped, not that we forgot to fetch. Hand-carry
                # samples (rep delivers in person, no DHL/FedEx
                # record — see _is_hand_carry) render as 🚗 Hand
                # carry instead of an AWB code.
                awbs = sorted({
                    (s.get("AWB") or "").strip()
                    for s in samples
                    if (s.get("AWB") or "").strip()
                })
                if any(_is_hand_carry(a) for a in awbs):
                    lines.append("       🚗 Hand carry")
                elif awbs:
                    awb_str = " / ".join(awbs)
                    lines.append(
                        f"       AWB Number: <code>{h(awb_str)}</code>"
                    )
                else:
                    lines.append("       AWB Number: —")

    # Unknown-receiver block — carrier records (DHL/FedEx) the bot
    # couldn't match to any FSL customer. Sales can self-identify these
    # at a glance and either add the customer to FSL or set an alias.
    # Appears BEFORE the top-sender divider so the actionable block
    # sits closer to the regional sections it relates to.
    #
    # V1.17.x — read from the OPS "Unmatched AWBs" tab (persisted by
    # awb_sync). Falls back to in-memory cache for the cold-boot case.
    unmatched_rows: list[dict] = []
    try:
        unmatched_rows = await asyncio.to_thread(sheets.load_unmatched_awbs)
    except Exception as e:  # noqa: BLE001
        log.warning("Could not read persisted unmatched AWBs: %s", e)
    if not unmatched_rows:
        for s in awb_sync.get_last_unmatched_shipments():
            unmatched_rows.append({
                "awb": s.awb, "carrier": s.carrier,
                "recipient_name": s.recipient_name,
                "ship_date": s.ship_date.strftime("%Y-%m-%d") if s.ship_date else "",
                "last_updated_utc": "",
            })

    # V1.17.x — TODAY-only filter for the digest. The OPS tab keeps a
    # rolling backlog of every unresolved unmatched AWB (so reps can
    # triage anytime), but the 6pm digest is about "samples sent
    # today" — so only today's carrier AWBs without an FSL match
    # belong in the UNKNOWN RECEIVER section. Sales explicitly
    # flagged that older entries pollute the daily message.
    today_iso = today.strftime("%Y-%m-%d")
    unmatched_rows = [
        r for r in unmatched_rows
        if str(r.get("ship_date") or "").strip() == today_iso
    ]

    # Always render the section, even when there's nothing to surface.
    # User wants the header visible so the absence is explicit ("None")
    # rather than ambiguous (header missing → maybe the bot forgot, or
    # maybe scrape failed). With this layout the message is consistent
    # day-to-day and reps know zero-state by reading "None" once.
    lines.append("")
    lines.append(
        "<b>UNKNOWN RECEIVER</b> "
        "<i>(Data from DHL &amp; FEDEX that i can't identify)</i>"
    )
    if not unmatched_rows:
        lines.append("None")
    else:
        # Sort by ship date desc — newest first, easiest to triage.
        unmatched_sorted = sorted(
            unmatched_rows,
            key=lambda r: str(r.get("ship_date") or ""),
            reverse=True,
        )
        for r in unmatched_sorted:
            lines.append("")
            lines.append(f"{h(r.get('recipient_name', ''))}")
            lines.append(f"AWB: <code>{h(r.get('awb', ''))}</code>")

    # V1.17.x — User flagged that the 'Top sender / Customers' summary
    # was admin-internal stats noise on the group digest. Removed.
    # Version footer added inline so it appears on the group post too
    # (the cron-fired _send_digest_to_chat path doesn't go through
    # send(), which is where /sampleupdate's preview gets it for free).
    lines.append("")
    lines.append(f"<i>{config.BOT_VERSION}</i>")

    return "\n".join(lines), total


def _split_digest_body(body: str, max_chars: int = 3900) -> list[str]:
    """Split a long digest body into Telegram-safe chunks.

    Telegram's API caps `send_message` at 4096 chars; we use 3900 to
    leave a bit of headroom for HTML entities counting as multiple
    bytes server-side. Splits on double-newlines first (preserves
    the region/salesperson group boundaries the digest is built
    around), and only falls back to mid-line splits if a single
    paragraph is itself too long (shouldn't happen with the current
    one-line-per-sample format, but defends against pathological
    customer names).
    """
    if len(body) <= max_chars:
        return [body]
    blocks = body.split("\n\n")
    chunks: list[str] = []
    current = ""
    for block in blocks:
        candidate = (current + "\n\n" + block) if current else block
        if len(candidate) <= max_chars:
            current = candidate
            continue
        if current:
            chunks.append(current)
        # Block itself fits — start a new chunk with it.
        if len(block) <= max_chars:
            current = block
            continue
        # Pathological case: a single paragraph > max_chars. Slice it
        # at the last newline that fits so we don't break inside a
        # sample line and corrupt HTML tags.
        remaining = block
        while len(remaining) > max_chars:
            cut = remaining.rfind("\n", 0, max_chars)
            if cut <= 0:
                cut = max_chars
            chunks.append(remaining[:cut])
            remaining = remaining[cut:].lstrip("\n")
        current = remaining
    if current:
        chunks.append(current)
    return chunks


async def _send_digest_to_chat(
    ctx: ContextTypes.DEFAULT_TYPE, chat_id: int, body: str
) -> None:
    """Send the digest body to a chat, splitting into multiple messages
    if it would exceed Telegram's 4096-char limit. Each chunk gets the
    same HTML parse mode so formatting stays consistent."""
    for chunk in _split_digest_body(body):
        await ctx.bot.send_message(
            chat_id=chat_id,
            text=chunk,
            parse_mode=ParseMode.HTML,
        )


async def _daily_sample_digest_job(ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """JobQueue callback — posts today's sample list to the group chat.

    Thin wrapper around _build_daily_digest_body — see that for the
    formatting and aggregation logic. Uses _send_digest_to_chat so
    busy-day digests that exceed Telegram's 4096-char cap get split
    into multiple messages instead of failing silently.

    V1.17.x — runs an inline MMS → FSL sync FIRST so the digest body
    is built against the freshest possible state. The 17:40 cron sync
    is still the primary path, but inlining means a failure of that
    cron (DHL rate limit, MMS3 outage, etc.) doesn't silently leave
    the 18:00 digest missing today's sample submissions.
    """
    if not config.DAILY_DIGEST_CHAT_ID:
        log.info("daily_digest: DAILY_DIGEST_CHAT_ID not set — skipping")
        return

    try:
        chat_id = int(config.DAILY_DIGEST_CHAT_ID)
    except ValueError:
        log.warning("daily_digest: DAILY_DIGEST_CHAT_ID is not a valid int: %r",
                    config.DAILY_DIGEST_CHAT_ID)
        return

    # Inline MMS→FSL sync so the digest never builds from stale FSL.
    try:
        import sync_engine
        sync_result = await asyncio.to_thread(
            sync_engine.run_mms_to_fsl_sync, True,
        )
        log.info("daily_digest: inline MMS sync result: %s", sync_result)
    except Exception as e:  # noqa: BLE001 — never block the digest on sync
        log.warning("daily_digest: inline MMS sync failed (continuing): %s", e)

    body, total = await _build_daily_digest_body()

    try:
        await _send_digest_to_chat(ctx, chat_id, body)
        log.info("daily_digest: sent (%d samples, %d chars) to chat %s",
                 total, len(body), chat_id)
    except Exception as e:  # noqa: BLE001
        log.exception("daily_digest: send failed: %s", e)


async def _schedule_daily_digest(application: Application) -> None:
    """Schedule the daily digest at 18:00 Asia/Singapore, weekdays only."""
    from datetime import time as _time
    job_queue = application.job_queue
    if job_queue is None:
        log.warning("JobQueue not available — daily digest NOT scheduled.")
        return
    if not config.DAILY_DIGEST_CHAT_ID:
        log.info("daily_digest: DAILY_DIGEST_CHAT_ID not set — schedule skipped")
        return

    sgt = ZoneInfo("Asia/Singapore")
    # days: 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri. Skips Saturday + Sunday.
    job_queue.run_daily(
        _daily_sample_digest_job,
        time=_time(hour=18, minute=0, tzinfo=sgt),
        days=(0, 1, 2, 3, 4),
        name="daily_sample_digest",
    )
    log.info("daily_digest scheduled: 18:00 SGT, Mon-Fri, chat %s",
             config.DAILY_DIGEST_CHAT_ID)


async def cmd_whichchat(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """`/whichchat` — print the current chat's ID. Useful for setting
    DAILY_DIGEST_CHAT_ID. Authorized users only; non-admins still get
    the ID but only for the chat they ran it in."""
    if not await _authorized(update):
        return
    chat = update.effective_chat
    if not chat:
        return
    title = getattr(chat, "title", "") or chat.type or "?"
    await send(
        update,
        f"💬 <b>Chat info</b>\n"
        f"Chat ID: <code>{chat.id}</code>\n"
        f"Title:   {h(title)}\n"
        f"Type:    {chat.type}\n\n"
        "<i>Paste the chat ID into Railway as "
        "<code>DAILY_DIGEST_CHAT_ID</code> to enable the 18:00 SGT "
        "weekday sample digest in this chat.</i>",
    )


async def cmd_sampleupdate(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """`/sampleupdate` — admin-only: trigger the daily sample digest right
    now AND echo the exact body back to the caller in DM so the admin
    can review without alt-tabbing to the group chat.

    Audit V1.13.14 (UX D) — previously only confirmed "posted to
    chat_id"; admin had to switch to the group to see the actual
    content. Now the same HTML body lands here too.
    """
    user = update.effective_user
    if not _is_update_sample_owner(user):
        await send(update, "🔒 Admin-only command.")
        return
    await send(update, "📋 Building today's digest… <i>(running MMS→FSL sync first for fresh data — takes ~30-60s)</i>")
    # V1.17.x — inline MMS sync before building the digest body so the
    # preview/post is always against fresh FSL. Mirrors the cron path
    # in _daily_sample_digest_job.
    try:
        import sync_engine
        sync_result = await asyncio.to_thread(
            sync_engine.run_mms_to_fsl_sync, True,
        )
        log.info("cmd_sampleupdate: inline MMS sync result: %s", sync_result)
    except Exception as e:  # noqa: BLE001
        log.warning("cmd_sampleupdate: inline MMS sync failed (continuing): %s", e)
        await send(update, f"⚠️ MMS sync had an issue — digest will reflect last-known FSL state: <code>{h(str(e))}</code>")
    try:
        body, total = await _build_daily_digest_body()
    except Exception as e:  # noqa: BLE001
        log.exception("cmd_sampleupdate: build failed")
        await send(update, f"❌ Build failed: <code>{h(str(e))}</code>")
        return

    # Always show the preview to the caller, even if the group isn't
    # configured. That way the admin can sanity-check formatting
    # before wiring DAILY_DIGEST_CHAT_ID. Split on the 4096-char limit
    # so busy days don't crash the preview either.
    await send(update, "👇 <b>Preview (what the group will see):</b>")
    for chunk in _split_digest_body(body):
        await send(update, chunk)

    if not config.DAILY_DIGEST_CHAT_ID:
        await send(
            update,
            "⚠️ <code>DAILY_DIGEST_CHAT_ID</code> is not set in Railway, "
            "so this preview was NOT posted to any group. "
            "Use /whichchat in the target group, paste the ID into "
            "Railway → Variables, redeploy, then re-run /sampleupdate.",
        )
        return

    try:
        chat_id = int(config.DAILY_DIGEST_CHAT_ID)
        await _send_digest_to_chat(ctx, chat_id, body)
        log.info("cmd_sampleupdate: sent (%d samples, %d chars) to chat %s",
                 total, len(body), chat_id)
        await send(
            update,
            f"✅ Posted to <code>{h(config.DAILY_DIGEST_CHAT_ID)}</code> "
            f"({total} sample{'s' if total != 1 else ''}).",
        )
    except Exception as e:  # noqa: BLE001
        log.exception("cmd_sampleupdate: send failed")
        await send(update, f"❌ Post to group failed: <code>{h(str(e))}</code>")


async def _awb_sync_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """JobQueue callback for the twice-daily AWB sync.

    Runs awb_sync.run_awb_sync(), logs the result, and posts an 'AWB
    Update' message to the digest chat if any new AWBs were actually
    written this run. Skip-post when nothing was written so the chat
    doesn't get 'Update: 0 new AWBs' noise. Failures are caught and
    logged — a broken DHL scrape must not take down the rest of the
    bot.
    """
    log.info("awb_sync_job: starting")
    try:
        result = await awb_sync.run_awb_sync(days_back=14, dry_run=False)
    except Exception as e:  # noqa: BLE001
        log.exception("awb_sync_job crashed: %s", e)
        return
    log.info(
        "awb_sync_job: done · fetched %d (DHL %d + FedEx %d) · "
        "matched %d · written %d · errors %d",
        result.dhl_count + result.fedex_count,
        result.dhl_count, result.fedex_count,
        result.total_matched, result.total_written, len(result.errors),
    )
    for err in result.errors:
        log.warning("awb_sync_job error: %s", err)

    # Push a follow-up "AWB Update" message to the same chat as the
    # daily digest, when (a) we wrote at least one new AWB AND (b)
    # DAILY_DIGEST_CHAT_ID is configured. format_update_message
    # returns None on no-writes so we skip the post then.
    msg = awb_sync.format_update_message(result)
    if not msg:
        return
    if not config.DAILY_DIGEST_CHAT_ID:
        log.info(
            "awb_sync_job: %d update(s) written but DAILY_DIGEST_CHAT_ID "
            "not set — skipping chat post",
            result.total_written,
        )
        return
    try:
        chat_id = int(config.DAILY_DIGEST_CHAT_ID)
        await context.bot.send_message(
            chat_id=chat_id, text=msg, parse_mode=ParseMode.HTML,
        )
        log.info(
            "awb_sync_job: posted AWB update (%d shipment(s)) to chat %s",
            len(result.applied_updates), chat_id,
        )
    except Exception as e:  # noqa: BLE001
        log.warning("awb_sync_job: chat post failed: %s", e)


async def _schedule_awb_sync(application: Application) -> None:
    """Set up the twice-daily AWB sync.

    Schedule (user-specified):
      • 17:30 SGT daily — first pass, aligned with the 5:30 PM FSL
        update window. Catches AWBs that became available during the
        day for FSL rows already on the sheet.
      • 00:00 SGT daily — overnight catch-up. Picks up AWBs for FSL
        rows that arrived after the 17:30 pass (e.g. samples added to
        MMS in the late afternoon, then MMS-synced into the FSL at
        17:40 — after our 17:30 AWB pass missed them).

    The matcher's skip-if-non-empty rule means re-runs only fill empty
    AWB cells, so neither time can clobber a previously-written value
    (real AWB, HAND CARRY marker, anything else manually entered).
    The 14-day overlap window in the fetchers means a missed run isn't
    fatal either — the next pass re-checks the same period.
    """
    from datetime import time as _time
    job_queue = application.job_queue
    if job_queue is None:
        log.warning("JobQueue not available — AWB sync NOT scheduled.")
        return
    sgt = ZoneInfo("Asia/Singapore")
    # Both runs fire every day (0=Mon … 6=Sun) — DHL/FedEx still
    # record AWBs on weekends if a rep ships then, and we want the
    # FSL to be current even when the Mon-Fri digest is off.
    job_queue.run_daily(
        _awb_sync_job,
        time=_time(hour=17, minute=30, tzinfo=sgt),
        days=(0, 1, 2, 3, 4, 5, 6),
        name="awb_sync_evening",
    )
    job_queue.run_daily(
        _awb_sync_job,
        time=_time(hour=0, minute=0, tzinfo=sgt),
        days=(0, 1, 2, 3, 4, 5, 6),
        name="awb_sync_overnight",
    )
    log.info("awb_sync scheduled: 17:30 + 00:00 SGT, daily")


async def _schedule_weekly_mms_sync(application: Application) -> None:
    """Set up the recurring weekday sync job + catch-up if overdue.

    Schedule:
      - Monday through Friday at 17:40 Asia/Singapore (= 09:40 UTC).
        Fires 20 minutes before the 18:00 SGT daily digest so the FSL
        tabs are guaranteed fresh when the digest reads them. Saturday
        and Sunday skipped.
      - On startup, if last successful sync was >24h ago (or never),
        kick off a one-shot run after a 60s delay so the bot has time
        to finish initialising. Fri 17:40 → Mon 17:40 is 72h, so any
        Mon morning startup will catch-up. force=True on the sync
        bypasses the 24h cooldown that would otherwise block daily
        runs (see _weekly_mms_sync_job).

    Each run pulls Singapore (S- codes → FSL_TAB), Indonesia (J- codes
    → JAKARTA_FSL_TAB), and Thailand (B- codes → BANGKOK_FSL_TAB) in a
    single MMS round-trip.
    """
    from datetime import time as _time
    job_queue = application.job_queue
    if job_queue is None:
        log.warning("JobQueue not available — sync NOT scheduled. "
                    "Install python-telegram-bot[job-queue] to enable.")
        return

    # Recurring weekday run: Mon-Fri 17:40 SGT.
    # PTB day numbering: 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri, 5=Sat, 6=Sun.
    # PTB interprets `days` in the timezone of the `time` object, so
    # all 5 fire at 17:40 local Singapore wall time.
    sgt = ZoneInfo("Asia/Singapore")
    job_queue.run_daily(
        _weekly_mms_sync_job,
        time=_time(hour=17, minute=40, tzinfo=sgt),
        days=(0, 1, 2, 3, 4),
        name="weekday_mms_sync",
    )
    log.info("mms_sync scheduled: Mon-Fri 17:40 SGT (20 min before digest)")

    # Catch-up: if last sync was >24h ago, trigger once 60s after
    # startup. Tighter than the prior 4-day threshold so a Railway
    # redeploy refreshes the list whenever it's stale, while the 24h
    # cooldown in sync_engine prevents double-syncs from rapid
    # redeploys.
    try:
        last = await asyncio.to_thread(sheets.get_last_sample_sync)
    except Exception as e:  # noqa: BLE001
        log.warning("Could not read last sync timestamp: %s", e)
        last = None
    if last is None:
        overdue = True
        last_str = "never"
    else:
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        overdue = (datetime.now(timezone.utc) - last) > timedelta(hours=24)
        last_str = last.isoformat(timespec="seconds")
    if overdue:
        log.info(
            "mms_sync: last run was %s — scheduling catch-up in 60s",
            last_str,
        )
        job_queue.run_once(_weekly_mms_sync_job, when=60, name="catchup_mms_sync")
    else:
        log.info("mms_sync: last run %s — no catch-up needed", last_str)


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

    log.info("Ensuring Indonesia FSL tab exists…")
    try:
        sheets.ensure_jakarta_tab()
    except Exception as e:  # noqa: BLE001
        # Non-fatal — the bot can still serve Singapore queries even if the
        # Jakarta tab bootstrap fails. Log loudly and carry on.
        log.exception("ensure_jakarta_tab failed: %s", e)

    log.info("Ensuring Thailand FSL tab exists…")
    try:
        sheets.ensure_bangkok_tab()
    except Exception as e:  # noqa: BLE001
        # Non-fatal — same pattern as the Jakarta bootstrap.
        log.exception("ensure_bangkok_tab failed: %s", e)

    app = Application.builder().token(config.TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("edit", cmd_edit))
    app.add_handler(CommandHandler("reload", cmd_reload))
    app.add_handler(CommandHandler("samples", cmd_samples))
    app.add_handler(CommandHandler("bulk", cmd_bulk))
    app.add_handler(CommandHandler("quote", cmd_quote))
    app.add_handler(CommandHandler("whoami", cmd_whoami))
    app.add_handler(CommandHandler("whichchat", cmd_whichchat))
    app.add_handler(CommandHandler("sampleupdate", cmd_sampleupdate))
    app.add_handler(CommandHandler("syncawb", cmd_syncawb))
    app.add_handler(CommandHandler("diag", cmd_diag))
    app.add_handler(CommandHandler("pp", cmd_pp))
    app.add_handler(CommandHandler("scan", cmd_scan))
    app.add_handler(CommandHandler("lastsample", cmd_lastsample))
    app.add_handler(CommandHandler("alllastsample", cmd_alllastsample))
    # /updatesamplelist command retired in V1.7.1. The MMS → FSL sync is
    # now scheduled by the JobQueue setup at the bottom of main().

    # Register Telegram's native blue slash-command menu so users see every
    # function the bot offers when they tap '/'. /updatesamplelist is NOT in
    # the default list (it's restricted), but we'll add it for @ragonic below
    # via post_init once the bot is running.
    async def _install_commands(application: Application) -> None:
        # Housekeeping audit — the / autocomplete menu now lists only the
        # commands a regular sales rep actually uses day-to-day.
        # /whoami, /whichchat, /reload, /diag, /sampleupdate are still
        # registered as CommandHandlers (above) and respond when typed,
        # but no longer clutter the autocomplete list for new hires who
        # would never need them. Admins type them from memory or look
        # them up in /help, which keeps the full inventory.
        default_cmds = [
            BotCommand("start", "Main menu — new request / bulk / samples"),
            BotCommand("bulk", "Paste a multi-seasoning email, I split it"),
            BotCommand("quote", "📄 Open the web quotation builder"),
            BotCommand("samples", "List samples you've raised"),
            BotCommand("edit", "Jump to the draft review to change a field"),
            BotCommand("cancel", "Discard the current draft"),
            BotCommand("pp", "💲 Product price — e.g. /pp S-62RG3-19"),
            BotCommand("scan", "📷 Scan a photo for product code(s)"),
            BotCommand("lastsample", "🔎 Find your last sample — /lastsample <keyword>"),
            BotCommand("alllastsample", "🌐 Search any rep's samples — /alllastsample <keyword>"),
            BotCommand("help", "Show all commands"),
        ]
        try:
            await application.bot.set_my_commands(
                default_cmds, scope=BotCommandScopeDefault()
            )
        except Exception as e:  # noqa: BLE001
            log.warning("set_my_commands (default) failed: %s", e)

    # Compose post_init: install Telegram command menu AND schedule the
    # weekly MMS → Full Sample Listing sync via JobQueue.
    async def _warm_fsl_tabs() -> None:
        # V1.17.x — background warm of the three FSL region tabs so the
        # first search / lastsample / smart-route after a deploy hits the
        # rows cache instead of three cold Google API reads. Runs AFTER
        # the bot is live (never delays startup); failures are harmless —
        # caches refill lazily on first use.
        try:
            await asyncio.gather(
                *(asyncio.to_thread(sheets.load_fsl_rows_all, t)
                  for t in (sheets.FSL_TAB, sheets.JAKARTA_FSL_TAB,
                            sheets.BANGKOK_FSL_TAB))
            )
            log.info("FSL tabs pre-warmed")
        except Exception as e:  # noqa: BLE001
            log.warning("FSL warmup failed (will lazy-load): %s", e)

    async def _post_init(application: Application) -> None:
        await _install_commands(application)
        await _schedule_weekly_mms_sync(application)
        await _schedule_daily_digest(application)
        await _schedule_awb_sync(application)
        asyncio.create_task(_warm_fsl_tabs())
    app.post_init = _post_init
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_handler(MessageHandler(filters.PHOTO, on_photo))
    # V1.17.3 — images sent as a FILE arrive as documents, not photos, and
    # Telegram does NOT re-encode them. That's the only way to get the rep's
    # full-resolution original, which is what makes small printed codes
    # readable. Same handler; on_photo detects which one it got.
    app.add_handler(MessageHandler(filters.Document.IMAGE, on_photo))
    # V1.13.8 — voice messages route through Groq Whisper → /pp.
    app.add_handler(MessageHandler(filters.VOICE, on_voice))
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
