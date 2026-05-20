"""FedEx scraper — fetch recent shipments + their tracking numbers (AWBs).

Login flow (user-confirmed, May 2026):
  1. GET LOGIN_URL → fill User ID + Password → click LOG IN
  2. Page lands on fedex.com/en-us/logged-in-home.html and a
     "Choose your location" modal pops up. Click "Singapore ENGLISH".
  3. Redirect to fedex.com/en-sg/home.html. Click the username
     dropdown in the top-right (e.g. "Li Ting"), pick "View all my
     Shipments".
  4. Tracking page at fedex.com/fedextracking/ loads asynchronously
     (5-10s before the table populates).
  5. Parse each row → tracking number (AWB), recipient company, ship
     date.

Returns Shipment dataclasses with carrier='FedEx'. Empty list on any
failure — never raises (awb_sync's wrapper expects this).

FEDEX_DEBUG=1 saves a screenshot + page HTML after each step to
/tmp/fedex_debug/ for first-run diagnostics.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

from awb_sync import Shipment, cutoff_date


log = logging.getLogger("npsamplebot.awb_fedex")


# FedEx secure-login URL. The "#/credentials" hash route is the literal
# username+password screen — landing on /secure-login/ alone drops the
# user on the FedEx homepage. User-confirmed value.
LOGIN_URL = "https://www.fedex.com/secure-login/en-us/#/credentials"

# Direct URL to the "All my shipments" table. Reachable via the user
# menu dropdown, but direct-nav is faster + survives layout drift in
# the home page.
SHIPMENTS_URL = "https://www.fedex.com/fedextracking/"


_TEST_MODE = os.getenv("AWB_TEST_MODE", "0").strip() == "1"
_DEBUG = os.getenv("FEDEX_DEBUG", "0").strip() == "1"
# FEDEX_HEADED=1 → launch chromium with a visible window so you can
# watch the automation drive the FedEx site. Off by default for Railway.
_HEADED = os.getenv("FEDEX_HEADED", "0").strip() == "1"


# --------------------------- public entrypoint ---------------------------

async def fetch_recent_shipments(*, days_back: int = 14) -> list[Shipment]:
    """Return FedEx outbound shipments since (today - days_back).

    Returns [] (with a logged warning) on any failure — never raises.
    """
    if _TEST_MODE:
        log.info("AWB_TEST_MODE=1 — returning hardcoded FedEx test shipments")
        today = date.today()
        return [
            Shipment(
                carrier="FedEx",
                awb="872017312410",
                recipient_name="TAKATA KORYO CO LTD",
                recipient_country="Japan",
                ship_date=today - timedelta(days=2),
            ),
        ]

    user = os.getenv("FEDEX_USER", "").strip()
    pwd = os.getenv("FEDEX_PASS", "").strip()
    if not user or not pwd:
        log.info("FEDEX_USER / FEDEX_PASS not set — skipping FedEx fetch")
        return []

    try:
        from playwright.async_api import async_playwright  # noqa: WPS433
    except ImportError:
        log.warning(
            "playwright is not installed — skipping FedEx scrape. Add "
            "`playwright` to requirements.txt and run "
            "`python -m playwright install chromium` to enable."
        )
        return []

    cutoff = cutoff_date(days_back)
    log.info("FedEx scrape starting · cutoff=%s · debug=%s", cutoff, _DEBUG)

    async with async_playwright() as p:
        log.info("FedEx: launching chromium (headless=%s)", not _HEADED)
        browser = await p.chromium.launch(headless=not _HEADED)
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/127.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 900},
            locale="en-SG",
            timezone_id="Asia/Singapore",
            permissions=[],
            geolocation=None,
        )
        page = await ctx.new_page()
        try:
            return await _scrape(page, user, pwd, cutoff)
        except Exception as e:  # noqa: BLE001
            log.exception("FedEx scrape failed: %s", e)
            if _DEBUG:
                try:
                    await _dump(page, "failure")
                except Exception:  # noqa: BLE001
                    pass
            return []
        finally:
            await ctx.close()
            await browser.close()


# --------------------------- scraper internals ---------------------------

async def _scrape(page, user: str, pwd: str, cutoff: date) -> list[Shipment]:
    """Walk through the login → location → user menu → tracking flow."""
    # ---- 1. Login page ----
    log.info("FedEx step 1/7: loading login page…")
    await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30_000)
    await _dump(page, "01_login_page")

    # ---- 2. Fill credentials and click LOG IN ----
    log.info("FedEx step 2/7: filling credentials…")
    user_field = (
        page.get_by_label(re.compile(r"^user\s*id$", re.I))
        .or_(page.locator('input[id*="user" i]'))
        .or_(page.locator('input[name*="user" i]'))
        .or_(page.locator('input[type="text"]'))
    ).first
    pw_field = (
        page.get_by_label(re.compile(r"^password$", re.I))
        .or_(page.locator('input[type="password"]'))
    ).first

    # Same React-controlled-component fix as DHL: real keyboard events
    # via press_sequentially, otherwise React's onChange doesn't fire
    # and the form validator rejects with "Required".
    await user_field.click(timeout=8_000)
    await user_field.press_sequentially(user, delay=25)
    await pw_field.click(timeout=8_000)
    await pw_field.press_sequentially(pwd, delay=25)
    await asyncio.sleep(0.3)

    log.info("FedEx step 3/7: submitting login…")
    login_btn = page.get_by_role("button", name=re.compile(r"^\s*log in\s*$", re.I)).first
    await login_btn.click(timeout=8_000)

    # Snapshot what's on screen right after the click — diagnostic for
    # "wrong password" / captcha / MFA states.
    await asyncio.sleep(1.5)
    log.info("FedEx: post-click URL = %s", page.url)
    await _dump(page, "02_just_after_login_click")

    # ---- 3. Wait for the post-login landing page ----
    log.info("FedEx step 4/7: waiting for post-login redirect…")
    try:
        await page.wait_for_url(
            re.compile(r"fedex\.com/.*(logged-in-home|en-sg/home)", re.I),
            timeout=45_000,
        )
    except Exception:  # noqa: BLE001
        log.error("FedEx: redirect timed out. Current URL: %s", page.url)
        try:
            snippet = await page.evaluate(
                "() => document.body.innerText.slice(0, 600)"
            )
            log.error("FedEx: visible page text snippet:\n%s", snippet)
        except Exception:  # noqa: BLE001
            pass
        await _dump(page, "03_redirect_timeout")
        raise

    # ---- 4. "Choose your location" modal — click Singapore English ----
    log.info("FedEx step 5/7: choosing location (Singapore)…")
    try:
        # The modal renders TWO "ENGLISH" buttons (SG + US). We want
        # the one in the "Singapore" column. Strategy:
        #  a) Match a button whose nearby text reads "Singapore"
        #  b) Fall back: first "ENGLISH" button on the page is the
        #     orange Singapore one in the layout shown to the user
        sg_btn = (
            page.get_by_role("button", name=re.compile(r"english", re.I))
            .or_(page.get_by_role("link", name=re.compile(r"english", re.I)))
        ).first
        await sg_btn.click(timeout=8_000)
        await asyncio.sleep(1.0)
    except Exception:  # noqa: BLE001
        # If the modal didn't appear (user has already chosen before),
        # FedEx remembers the location — that's fine, just proceed.
        log.info("FedEx: no location modal (already chosen previously)")

    await _dump(page, "04_after_location")

    # ---- 5. Open the user-menu → "View all my Shipments" ----
    # Direct-nav is faster than driving the menu, and works regardless
    # of which name the user account displays at the top right.
    log.info("FedEx step 6/7: opening shipments page…")
    await page.goto(SHIPMENTS_URL, wait_until="domcontentloaded", timeout=30_000)
    await _wait_for_shipments_table(page)
    await _dump(page, "05_shipments_table")

    # ---- 6. Extract rows ----
    log.info("FedEx step 7/7: extracting rows…")
    rows = await page.evaluate(_EXTRACT_JS)
    log.info("FedEx: extracted %d raw row(s)", len(rows))

    shipments: list[Shipment] = []
    for r in rows:
        s = _row_to_shipment(r)
        if s is None:
            continue
        if s.ship_date < cutoff:
            continue
        shipments.append(s)
    log.info(
        "FedEx: %d shipment(s) within %d-day window (cutoff=%s)",
        len(shipments), (date.today() - cutoff).days, cutoff,
    )
    return shipments


async def _wait_for_shipments_table(page) -> None:
    """User said the table needs 5-10s to populate. Wait for any row,
    or the empty-state, whichever appears first. Generous 30s ceiling."""
    populated = page.locator("table tbody tr").first
    empty = page.get_by_text(re.compile(r"no shipments", re.I)).first
    try:
        await populated.or_(empty).wait_for(state="visible", timeout=30_000)
        # Even after the first row appears, FedEx often adds more rows
        # in subsequent XHRs. Brief settling pause so we catch them.
        await asyncio.sleep(2.0)
    except Exception:  # noqa: BLE001
        log.warning("FedEx: shipments table didn't populate in 30s")


# --------------------------- DOM extraction JS ---------------------------

# Header-driven extractor. Reads the table's <th> cells, maps header
# names to indices, then for each <tr> emits a dict keyed by header
# name. This way we don't hard-code column positions — if FedEx
# rearranges or the user adds/removes columns via EDIT COLUMNS, the
# scraper still finds the right cell by name.
#
# Cell text uses element.innerText (preserves line breaks for multi-
# line cells like an address block).
_EXTRACT_JS = r"""
() => {
  const tables = Array.from(document.querySelectorAll("table"));
  // Pick the table whose header row contains a 'tracking' label —
  // FedEx may render auxiliary tables on the same page (filters,
  // stats etc.), and we want the shipments grid specifically.
  const table = tables.find(t => {
    const ths = Array.from(t.querySelectorAll("thead th"));
    return ths.some(th => /tracking/i.test(th.innerText || ""));
  });
  if (!table) return [];

  const ths = Array.from(table.querySelectorAll("thead th"));
  const headers = ths.map(th => (th.innerText || "").trim());

  const rows = [];
  const trs = Array.from(table.querySelectorAll("tbody tr"));
  for (const tr of trs) {
    const cells = Array.from(tr.querySelectorAll("td"));
    if (cells.length === 0) continue;
    const obj = {};
    for (let i = 0; i < headers.length && i < cells.length; i++) {
      const key = headers[i] || `col_${i}`;
      obj[key] = (cells[i].innerText || "").trim();
    }
    rows.push(obj);
  }
  return rows;
}
"""


# --------------------------- row → Shipment ---------------------------

_MONTH_LOOKUP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _find_field(row: dict, *patterns: str) -> str:
    """Look up a field by case-insensitive substring match on header name.

    Lets us be tolerant of small wording differences — e.g. "Recipient
    Company Name" vs "Recipient Company" vs "Recipient Company / Org"
    all match `_find_field(row, "recipient", "company")`.
    """
    for k, v in row.items():
        kl = k.lower()
        if all(p.lower() in kl for p in patterns):
            return (v or "").strip()
    return ""


def _parse_fedex_date(text: str) -> date | None:
    """Parse FedEx's many date formats: '6/3/26', '06/03/2026',
    'Jun 3, 2026', '3 Jun 2026', etc."""
    if not text:
        return None
    t = text.strip()
    # Drop non-date prefixes like 'Estimated' / 'On' that FedEx
    # sometimes prepends.
    t = re.sub(r"^(estimated|on|by|delivered)\s*", "", t, flags=re.I)
    for fmt in (
        "%m/%d/%Y", "%m/%d/%y",
        "%d/%m/%Y", "%d/%m/%y",
        "%B %d, %Y", "%b %d, %Y",
        "%d %B %Y", "%d %b %Y",
    ):
        try:
            return datetime.strptime(t, fmt).date()
        except ValueError:
            continue
    return None


def _row_to_shipment(raw: dict) -> Shipment | None:
    """Convert one parsed table row → Shipment. None on bad data."""
    awb = _find_field(raw, "tracking")
    awb = re.sub(r"\D", "", awb)  # strip status icons / whitespace
    if not re.fullmatch(r"\d{10,14}", awb):
        return None

    # Prefer the recipient COMPANY column if present; fall back to
    # recipient contact name as the last resort.
    customer = (
        _find_field(raw, "recipient", "company")
        or _find_field(raw, "recipient", "contact")
        or _find_field(raw, "recipient")
    )
    if not customer:
        log.info("FedEx: skipping row with no recipient (awb=%s)", awb)
        return None

    # Prefer an actual ship/created/label date column. If none exists,
    # back-derive from the scheduled delivery date (typical FedEx
    # international transit is ~3 days SG → most markets).
    ship_date = None
    for patterns, offset in [
        (("ship", "date"), 0),
        (("label", "date"), 0),
        (("created",), 0),
        (("pickup",), 0),
        (("scheduled", "delivery"), -3),  # back-derive
        (("delivered",), -3),             # back-derive from delivery
    ]:
        raw_date = _find_field(raw, *patterns)
        parsed = _parse_fedex_date(raw_date) if raw_date else None
        if parsed:
            ship_date = parsed + timedelta(days=offset)
            break
    if ship_date is None:
        log.info(
            "FedEx: no parseable date on row (awb=%s) — falling back to today",
            awb,
        )
        ship_date = date.today()

    # Best-effort country: not in the visible table, so leave blank.
    return Shipment(
        carrier="FedEx",
        awb=awb,
        recipient_name=customer,
        recipient_country="",
        ship_date=ship_date,
    )


# --------------------------- debug ---------------------------

async def _dump(page, label: str) -> None:
    if not _DEBUG:
        return
    try:
        outdir = Path(tempfile.gettempdir()) / "fedex_debug"
        outdir.mkdir(exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        png = outdir / f"{stamp}_{label}.png"
        html = outdir / f"{stamp}_{label}.html"
        await page.screenshot(path=str(png), full_page=True)
        content = await page.content()
        html.write_text(content, encoding="utf-8")
        log.info("FedEx debug: wrote %s and %s", png, html)
    except Exception as e:  # noqa: BLE001
        log.warning("FedEx debug dump failed: %s", e)
