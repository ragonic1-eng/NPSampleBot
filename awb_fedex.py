"""FedEx scraper — fetch recent shipments + their tracking numbers (AWBs).

Login flow (user-confirmed, May 2026):
  1. GET LOGIN_URL → fill User ID + Password → click LOG IN
  2. Page lands on fedex.com/en-sg/home.html (or /en-us/logged-in-home
     if account hasn't set a location yet). If a "Choose your location"
     modal pops up, click "Singapore ENGLISH" — but most live accounts
     have this remembered and the modal never appears.
  3. Direct-nav to SHIPMENTS_URL (the FedEx Ship Manager all-shipments
     view). This bypasses the Shipping → Ship Now → Shipments nav
     dance entirely.
  4. Read the data grid. Columns: SHIP DATE / RECIPIENT (formatted as
     "COMPANY, CONTACT" — split on the first comma to get the
     company) / STATUS / TRACKING ID (the AWB) / SHIPMENT TYPE /
     REFERENCE.

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

# Direct URL to the FedEx Ship Manager "all shipments" view. The user-
# friendly path is Shipping menu → Ship Now → Shipments tab in the
# left rail, but this URL drops us on the same page in one navigation
# (works on every NPFoods account session — user-confirmed).
SHIPMENTS_URL = (
    "https://www.fedex.com/shippingplus/en-sg/shipments-overview/all-shipments"
)


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

    # Block: wait for the Angular spinner overlay (wlgn-spinner) to
    # disappear, otherwise it intercepts every click. Generous 15s —
    # the FedEx login bundle is heavy on first paint.
    log.info("FedEx: waiting for spinner overlay to clear…")
    try:
        await page.locator(".loading-overlay, wlgn-spinner").first.wait_for(
            state="hidden", timeout=15_000
        )
    except Exception:  # noqa: BLE001
        log.info("FedEx: no spinner overlay (or already gone)")

    # Block: dismiss the Usercentrics cookie consent banner — it
    # renders as <aside id="usercentrics-cmp-ui"> on top of the form
    # and absorbs every click into a void. See _dismiss_usercentrics.
    await _dismiss_usercentrics(page)

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

    # Even after the cookie banner is dismissed, FedEx's Angular app
    # takes a beat to re-enable the login form. Wait until the
    # username input is properly interactive before trying to click —
    # otherwise we click 'into' a still-animating overlay and lose
    # the keystrokes.
    await user_field.wait_for(state="visible", timeout=15_000)
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


async def _dismiss_usercentrics(page) -> None:
    """Click whichever 'Accept All Cookies' button is visible.

    FedEx serves two different consent banners depending on tenant /
    AB-test bucket:
      (a) a custom FedEx banner pinned to the bottom of the page with
          two buttons: "REJECT OPTIONAL COOKIES" and "ACCEPT ALL
          COOKIES"
      (b) a Usercentrics CMP overlay (<aside id='usercentrics-cmp-ui'>)
          with the controls inside a Shadow DOM
    We DO NOT gate on the Usercentrics aside being present — earlier
    versions returned early when FedEx was actually showing banner (a),
    which left the form blocked. Instead just try the visible-button
    click first; that works for both providers since the text reads
    'Accept All Cookies' / 'Accept All' in both.

    Strategies, in priority order:
      1. Playwright role-based click on any 'Accept ...' button
         (auto-pierces open shadow DOMs, also picks up FedEx's
         native bottom banner)
      2. Usercentrics' official JS API: window.UC_UI.acceptAllConsents()
      3. Manual shadow-DOM walk + button.click()
      4. Remove the <aside> so it stops intercepting clicks (so the
         login can still proceed even if accept itself failed)
    """
    # --- Strategy 1: Playwright role-based click ---
    # 'accept all cookies' is the most specific match (FedEx's bottom
    # banner button), so we try it first. Each subsequent pattern is
    # a looser fallback. 3s per attempt — fast even when all six fail.
    for name_pattern in (
        re.compile(r"accept all cookies", re.I),
        re.compile(r"^accept all$", re.I),
        re.compile(r"^allow all$", re.I),
        re.compile(r"accept all", re.I),
        re.compile(r"^accept$", re.I),
        re.compile(r"i agree", re.I),
    ):
        try:
            btn = page.get_by_role("button", name=name_pattern).first
            await btn.click(timeout=3_000)
            log.info(
                "FedEx: cookie consent dismissed via Accept button (%r)",
                name_pattern.pattern,
            )
            # FedEx's bottom banner has a slide-out animation and the
            # Angular app re-renders the form region after consent is
            # recorded — both take ~1-2s. Wait it out so the username
            # field is actually interactive before we try to fill it.
            await asyncio.sleep(2.5)
            return
        except Exception:  # noqa: BLE001
            continue

    # --- Strategy 2: Usercentrics' official JS API ---
    try:
        accepted = await page.evaluate(
            """
            async () => {
                if (typeof window.UC_UI === 'undefined') return false;
                if (typeof window.UC_UI.acceptAllConsents === 'function') {
                    await window.UC_UI.acceptAllConsents();
                    if (typeof window.UC_UI.closeCMP === 'function') {
                        window.UC_UI.closeCMP();
                    }
                    return true;
                }
                return false;
            }
            """
        )
        if accepted:
            log.info("FedEx: cookie consent accepted via UC_UI JS API")
            # FedEx's bottom banner has a slide-out animation and the
            # Angular app re-renders the form region after consent is
            # recorded — both take ~1-2s. Wait it out so the username
            # field is actually interactive before we try to fill it.
            await asyncio.sleep(2.5)
            return
    except Exception:  # noqa: BLE001
        pass

    # --- Strategy 3: walk the shadow DOM and click manually ---
    try:
        clicked = await page.evaluate(
            r"""
            () => {
                const accepts = /^(accept all cookies|accept all|allow all|accept|i agree)$/i;
                const walk = (root) => {
                    if (!root) return false;
                    const buttons = root.querySelectorAll
                        ? root.querySelectorAll("button, [role='button']")
                        : [];
                    for (const b of buttons) {
                        const t = (b.textContent || "").trim();
                        if (accepts.test(t)) { b.click(); return true; }
                    }
                    const all = root.querySelectorAll
                        ? root.querySelectorAll("*")
                        : [];
                    for (const el of all) {
                        if (el.shadowRoot && walk(el.shadowRoot)) return true;
                    }
                    return false;
                };
                // First try inside the Usercentrics aside, then the
                // whole document (FedEx's native banner lives there).
                const aside = document.getElementById("usercentrics-cmp-ui")
                              || document.getElementById("usercentrics-root");
                if (aside && walk(aside.shadowRoot || aside)) return true;
                return walk(document.body);
            }
            """
        )
        if clicked:
            log.info("FedEx: cookie consent — clicked Accept via shadow-DOM walk")
            # FedEx's bottom banner has a slide-out animation and the
            # Angular app re-renders the form region after consent is
            # recorded — both take ~1-2s. Wait it out so the username
            # field is actually interactive before we try to fill it.
            await asyncio.sleep(2.5)
            return
    except Exception:  # noqa: BLE001
        pass

    # --- Strategy 4: last resort — remove banner elements ---
    log.warning(
        "FedEx: couldn't trigger Accept All — removing banner from DOM "
        "as fallback so the login form is unblocked"
    )
    try:
        await page.evaluate(
            """
            () => {
                ["usercentrics-cmp-ui", "usercentrics-root"].forEach(id => {
                    const el = document.getElementById(id);
                    if (el) el.remove();
                });
            }
            """
        )
    except Exception:  # noqa: BLE001
        pass


async def _wait_for_shipments_table(page) -> None:
    """The FedEx Ship Manager grid is XHR-loaded. Wait for either a
    tracking-id-shaped text node to appear (populated) or the empty-
    state text. Generous 30s ceiling; 2s settling pause once the
    first row appears so subsequent XHRs can fill the rest."""
    populated = page.locator("text=/^\\d{10,14}$/").first
    empty = page.get_by_text(re.compile(r"no shipments", re.I)).first
    try:
        await populated.or_(empty).wait_for(state="visible", timeout=30_000)
        await asyncio.sleep(2.0)
    except Exception:  # noqa: BLE001
        log.warning("FedEx: shipments table didn't populate in 30s")


# --------------------------- DOM extraction JS ---------------------------

# Multi-strategy extractor. The FedEx Ship Manager grid isn't a plain
# <table> — it's a React data grid that may use <table>, role="grid",
# or pure <div>s with class names. We try the three patterns in order
# and return the first non-empty result.
#
# In all strategies we build a {header_name → value} dict per row so
# downstream code (Python _find_field) can look fields up by name
# substring, surviving column reorders or renames.
_EXTRACT_JS = r"""
() => {
  const TRACKING_RE = /^\s*\d{10,14}\s*$/;

  // -- Strategy 1: standard HTML <table> --
  const tables = Array.from(document.querySelectorAll("table"));
  for (const t of tables) {
    const headers = Array.from(t.querySelectorAll("thead th"))
      .map(th => (th.innerText || "").trim());
    if (!headers.some(h => /tracking/i.test(h) || /recipient/i.test(h))) continue;
    const out = [];
    const trs = Array.from(t.querySelectorAll("tbody tr"));
    for (const tr of trs) {
      const cells = Array.from(tr.querySelectorAll("td"));
      if (!cells.length) continue;
      const obj = {};
      for (let i = 0; i < headers.length && i < cells.length; i++) {
        obj[headers[i] || ("col_" + i)] = (cells[i].innerText || "").trim();
      }
      out.push(obj);
    }
    if (out.length) return out;
  }

  // -- Strategy 2: ARIA grid --
  const grids = Array.from(document.querySelectorAll(
    '[role="grid"], [role="table"]'
  ));
  for (const g of grids) {
    const headers = Array.from(g.querySelectorAll('[role="columnheader"]'))
      .map(h => (h.innerText || "").trim());
    if (!headers.some(h => /tracking/i.test(h) || /recipient/i.test(h))) continue;
    const out = [];
    const rows = Array.from(g.querySelectorAll('[role="row"]'));
    for (const r of rows) {
      const cells = Array.from(
        r.querySelectorAll('[role="cell"], [role="gridcell"]')
      );
      if (!cells.length) continue;
      const obj = {};
      for (let i = 0; i < headers.length && i < cells.length; i++) {
        obj[headers[i] || ("col_" + i)] = (cells[i].innerText || "").trim();
      }
      out.push(obj);
    }
    if (out.length) return out;
  }

  // -- Strategy 3: heuristic walk-up from each tracking-id text node --
  // Find every element whose direct text matches the AWB pattern, then
  // climb up looking for the row container (heuristic: nearest ancestor
  // whose innerText also contains a date pattern). Returns raw text
  // chunks for Python-side parsing.
  const out3 = [];
  const all = document.querySelectorAll("*");
  const seenRows = new Set();
  for (const el of all) {
    if (el.children.length) continue;
    const txt = (el.textContent || "").trim();
    if (!TRACKING_RE.test(txt)) continue;
    const awb = txt;

    let row = el;
    let rowText = "";
    for (let i = 0; i < 12; i++) {
      row = row.parentElement;
      if (!row) break;
      const rt = row.innerText || "";
      // Found a container with this row's tracking AND a recipient label
      // OR a date pattern → that's the row.
      if (rt.includes(awb) && /\d{4}-\d{2}-\d{2}/.test(rt) && rt.length > 30) {
        rowText = rt;
        break;
      }
    }
    if (!rowText) continue;
    if (seenRows.has(rowText.slice(0, 200))) continue;
    seenRows.add(rowText.slice(0, 200));

    // Pull ship date — first ISO date in the row text.
    const dateMatch = rowText.match(/\d{4}-\d{2}-\d{2}/);
    const ship_date_text = dateMatch ? dateMatch[0] : "";

    // Pull recipient — the FedEx grid renders "COMPANY, CONTACT" as a
    // single line. Find the first non-date, non-tracking, non-status
    // line that contains a comma.
    const lines = rowText.split("\n").map(l => l.trim()).filter(Boolean);
    let recipient = "";
    for (const ln of lines) {
      if (/^\d{4}-\d{2}-\d{2}$/.test(ln)) continue;
      if (TRACKING_RE.test(ln)) continue;
      if (/^(outbound|inbound|shipment|status)/i.test(ln)) continue;
      if (ln.length < 5 || ln.length > 200) continue;
      if (ln.includes(",")) { recipient = ln; break; }
      if (!recipient && /[A-Z]{3,}/.test(ln)) recipient = ln;
    }

    out3.push({
      "Tracking ID": awb,
      "Recipient": recipient,
      "Ship Date": ship_date_text,
    });
  }
  return out3;
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
    """Parse FedEx's many date formats.

    The Ship Manager grid uses ISO (e.g. '2026-05-20'); the older
    tracking views use US/UK locale formats ('6/3/26', 'Jun 3 2026'
    etc.). Try ISO first, then a fan of locale variants.
    """
    if not text:
        return None
    t = text.strip()
    t = re.sub(r"^(estimated|on|by|delivered|shipped)\s*", "", t, flags=re.I)
    # ISO comes first — it's an unambiguous match and the cheap case.
    for fmt in (
        "%Y-%m-%d",
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
    awb = re.sub(r"\D", "", awb)  # strip status icons / whitespace / copy icon
    if not re.fullmatch(r"\d{10,14}", awb):
        return None

    # The Ship Manager grid renders recipient as a single cell with the
    # company name BEFORE the comma and the contact person AFTER, e.g.
    # "TAKATA KORYO CO LTD, MR NAKANISHI". We split on the first comma
    # and take the left side as the customer (which is what FSL stores
    # under "Customer Name"). Fall through to dedicated company /
    # contact columns if the grid layout changes back to multi-column.
    raw_recipient = (
        _find_field(raw, "recipient")
        or _find_field(raw, "ship", "to")
        or _find_field(raw, "consignee")
    )
    customer = ""
    if raw_recipient:
        # Strip trailing whitespace / commas; take the part before the
        # first comma. Handles "COMPANY, CONTACT" and just "COMPANY".
        customer = raw_recipient.split(",", 1)[0].strip()
    if not customer:
        # Last resort: explicit company / contact columns.
        customer = (
            _find_field(raw, "recipient", "company")
            or _find_field(raw, "recipient", "contact")
        )
    if not customer:
        log.info("FedEx: skipping row with no recipient (awb=%s)", awb)
        return None

    # Prefer an actual ship-date column (this view has it). If absent,
    # fall back to label/created/pickup, or back-derive from scheduled
    # delivery or delivered date (typical SG → destination FedEx
    # international is ~3 days).
    ship_date = None
    for patterns, offset in [
        (("ship", "date"), 0),
        (("label", "date"), 0),
        (("created",), 0),
        (("pickup",), 0),
        (("scheduled", "delivery"), -3),
        (("delivered",), -3),
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

    return Shipment(
        carrier="FedEx",
        awb=awb,
        recipient_name=customer,
        recipient_country="",  # not in the Ship Manager grid
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
