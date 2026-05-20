"""DHL Express MyDHL+ scraper — fetch recent shipments + their AWBs.

Login flow (user-confirmed, May 2026):
  1. GET LOGIN_URL
  2. Fill 'Email Address or Phone Number' + 'Password' → click Login
  3. Page redirects to mydhl.express.dhl/sg/en/home.html?login=successful
  4. (One-off) accept the cookie consent banner if it appears
  5. Open https://mydhl.express.dhl/sg/en/manage-shipments.html
  6. Parse each shipment row:
       - AWB (10-digit number, top-left of each row, blue link)
       - Shipment Date (e.g. "May 19, 2026")
       - Ship To block: line 1 = customer company name
                       line 2 = contact person
                       line 3 = "City, Country"

Returns Shipment dataclasses with carrier='DHL'. Anything older than
(today - days_back) is filtered out client-side. Empty list is returned
gracefully on any failure (timeouts, login rejected, layout drift) —
awb_sync logs the error and the rest of the pipeline continues.

DHL_DEBUG=1 (env var) saves a screenshot + page HTML after each step
to /tmp/dhl_step_* so we can inspect what the headless browser
actually saw. Use it for the first live run.
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


log = logging.getLogger("npsamplebot.awb_dhl")


# Full DHL Express MyDHL+ login URL — Singapore tenant, OIDC flow. The
# exact URL matters because mydhl.express.dhl rejects the cookie unless
# the redirect_uri matches the OIDC client registration. User-confirmed
# value, do not abbreviate or query-string-mangle.
LOGIN_URL = (
    "https://dhlpass.dhl.com/en-sg/login/"
    "?CountryCode=sg"
    "&LangCode=en"
    "&client_id=mydhlplus"
    "&redirect_uri=https://mydhl.express.dhl/index/en/login-redirect.html"
    "&response_type=code"
    "&rt=1"
    "&oidc-auth=true"
)

# Landing page after login. Direct-nav here once authenticated, skipping
# any "what would you like to do" interstitial on the home page.
SHIPMENTS_URL = "https://mydhl.express.dhl/sg/en/manage-shipments.html"

# When ship_date can't be parsed from the row text, we fall back to
# today — better to have a row matched within ±2 days than to drop it
# entirely. Tracked so we can warn in logs if this happens often.
_TODAY_FALLBACK_WARN = "DHL shipment date unparseable, falling back to today"


_TEST_MODE = os.getenv("AWB_TEST_MODE", "0").strip() == "1"
_DEBUG = os.getenv("DHL_DEBUG", "0").strip() == "1"
# DHL_HEADED=1 → launch chromium with a visible window so you can watch
# the automation drive the page. Useful for diagnosing 'why didn't it
# redirect?' issues. Default is headless (Railway can't show a UI).
_HEADED = os.getenv("DHL_HEADED", "0").strip() == "1"


# --------------------------- public entrypoint ---------------------------

async def fetch_recent_shipments(*, days_back: int = 14) -> list[Shipment]:
    """Return DHL outbound shipments since (today - days_back).

    Returns [] (with a logged warning) on any failure — never raises.
    """
    if _TEST_MODE:
        log.info("AWB_TEST_MODE=1 — returning hardcoded DHL test shipments")
        today = date.today()
        return [
            Shipment(
                carrier="DHL",
                awb="7765013981",
                recipient_name="M.D. FOOD INDUSTRIES",
                recipient_country="Pakistan",
                ship_date=today - timedelta(days=1),
            ),
        ]

    user = os.getenv("DHL_USER", "").strip()
    pwd = os.getenv("DHL_PASS", "").strip()
    if not user or not pwd:
        log.info("DHL_USER / DHL_PASS not set — skipping DHL fetch")
        return []

    try:
        from playwright.async_api import async_playwright  # noqa: WPS433
    except ImportError:
        log.warning(
            "playwright is not installed — skipping DHL scrape. Add "
            "`playwright` to requirements.txt and run "
            "`python -m playwright install chromium` to enable."
        )
        return []

    cutoff = cutoff_date(days_back)
    log.info("DHL scrape starting · cutoff=%s · debug=%s", cutoff, _DEBUG)

    async with async_playwright() as p:
        log.info("DHL: launching chromium (headless=%s)", not _HEADED)
        browser = await p.chromium.launch(
            headless=not _HEADED,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--no-sandbox",
            ],
        )
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
        # Patch automation-detection signals (navigator.webdriver,
        # plugins list, window.chrome, languages). Matches awb_fedex.py
        # — DHL doesn't currently demand this, but adding it now keeps
        # the two scrapers consistent and pre-empts the next time a
        # carrier turns up the bot-detection dial.
        await ctx.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            Object.defineProperty(navigator, 'plugins', { get: () => [
                { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
                { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                { name: 'Native Client', filename: 'internal-nacl-plugin' },
            ] });
            window.chrome = window.chrome || { runtime: {} };
            """
        )
        page = await ctx.new_page()
        try:
            return await _scrape(page, user, pwd, cutoff)
        except Exception as e:  # noqa: BLE001
            log.exception("DHL scrape failed: %s", e)
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
    """Walk through the login → manage shipments flow and parse the list."""
    # ---- 1. Login page ----
    log.info("DHL step 1/6: loading login page…")
    await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30_000)
    await _dump(page, "01_login_page")

    log.info("DHL step 2/6: checking for cookie banner…")
    await _maybe_accept_cookies(page)

    log.info("DHL step 3/6: filling credentials…")
    # Locator.or_() composes selector strategies into a single race —
    # Playwright resolves whichever matches first, no per-strategy
    # timeout stacking. Cuts the worst-case wait from ~30s to ~3s.
    email_field = (
        page.get_by_label(re.compile(r"email address or phone number", re.I))
        .or_(page.locator('input[type="email"]'))
        .or_(page.locator('input[name*="email" i]'))
    ).first
    pw_field = (
        page.get_by_label(re.compile(r"^password$", re.I))
        .or_(page.locator('input[type="password"]'))
    ).first

    # IMPORTANT: use click() + type() instead of fill() — DHL's login
    # form is a React/MUI controlled component. fill() writes to the
    # DOM but doesn't trigger React's onChange the way real typing
    # does, so the form's internal state stays empty and validation
    # rejects on submit ("Required"). type() with a small delay
    # dispatches real keydown/keyup events; React picks them up the
    # same way it picks up a user typing.
    await email_field.click(timeout=8_000)
    await email_field.press_sequentially(user, delay=25)
    await pw_field.click(timeout=8_000)
    await pw_field.press_sequentially(pwd, delay=25)
    # Brief pause so DHL's client-side validator clears the "Required"
    # markers before we click Login — without it, the button click
    # sometimes lands a frame before React commits the state.
    await asyncio.sleep(0.3)

    log.info("DHL step 4/6: submitting login…")
    login_btn = page.get_by_role("button", name=re.compile(r"^\s*login\s*$", re.I)).first
    await login_btn.click(timeout=8_000)

    # Snapshot what's on screen the moment after the click — invaluable
    # if step 5 then times out, because we can SEE whether DHL bounced
    # us to an error page, a captcha, an MFA prompt, or the redirect
    # silently ate the click.
    await asyncio.sleep(1.5)  # let DHL's JS show whatever happens next
    log.info("DHL: post-click URL = %s", page.url)
    await _dump(page, "02_just_after_login_click")

    # Wait for the OIDC redirect chain to land on the MyDHL+ home page.
    log.info("DHL step 5/6: waiting for post-login redirect…")
    try:
        await page.wait_for_url(
            re.compile(r"mydhl\.express\.dhl/.*login=successful", re.I),
            timeout=45_000,
        )
    except Exception as e:  # noqa: BLE001
        log.error("DHL: redirect timed out. Current URL: %s", page.url)
        # Read any visible error/banner text so the user sees it in the log.
        try:
            visible_text = await page.evaluate("() => document.body.innerText.slice(0, 600)")
            log.error("DHL: visible page text snippet:\n%s", visible_text)
        except Exception:  # noqa: BLE001
            pass
        await _dump(page, "02b_redirect_timeout")
        raise
    await _dump(page, "02c_post_login_home")

    # Cookie consent banner sometimes appears AFTER login too.
    await _maybe_accept_cookies(page)

    # ---- 2. Manage Shipments → All Shipments ----
    log.info("DHL step 6/6: opening Manage Shipments…")
    await page.goto(SHIPMENTS_URL, wait_until="domcontentloaded", timeout=30_000)
    # The list itself is XHR-loaded — wait for at least one shipment row
    # to be visible or 'no shipments' text, whichever appears first.
    await _wait_for_shipments_loaded(page)
    await _dump(page, "03_manage_shipments")

    # ---- 3. Extract structured shipment data ----
    # Pagination was attempted in V1.16.0 (see _extract_all_pages) but
    # produced ghost rows on later pages — the JS extractor found
    # 10-digit AWB anchors that didn't belong to real shipment cards,
    # so every paginated AWB got skipped with "no customer name".
    # Reverted to first-page-only until we have proper diagnostics
    # from the live DOM. The 14-day rolling window + twice-daily sync
    # still catches everything because each run re-checks the same
    # period; older AWBs get backfilled gradually.
    rows = await page.evaluate(_EXTRACT_JS)
    log.info("DHL: extracted %d raw row(s) (page 1 only)", len(rows))

    shipments: list[Shipment] = []
    for r in rows:
        s = _row_to_shipment(r)
        if s is None:
            continue
        if s.ship_date < cutoff:
            continue
        shipments.append(s)
    log.info(
        "DHL: %d shipment(s) within %d-day window (cutoff=%s)",
        len(shipments), (date.today() - cutoff).days, cutoff,
    )
    return shipments


# --------------------------- locator helpers ---------------------------

async def _maybe_accept_cookies(page) -> None:
    """One-shot click of 'Accept all' if visible, otherwise no-op.

    Uses Playwright's Locator.or_() to wait for whichever consent
    button appears first (the OneTrust id is standard across DHL
    tenants; the role-based fallback covers the custom banner). Single
    2-second wait — if nothing's there we move on, no fallback chain.
    """
    accept = (
        page.locator("#onetrust-accept-btn-handler").or_(
            page.get_by_role("button", name=re.compile(r"accept all", re.I))
        )
    ).first
    try:
        await accept.click(timeout=2_000)
        log.info("DHL: cookie consent dismissed")
    except Exception:  # noqa: BLE001
        log.info("DHL: no cookie banner visible")


async def _fill_first_match(page, strategies: list[tuple], value: str) -> None:
    """Try each (kind, target) selector strategy; fill the first one
    that resolves to a visible element. Raises if none match."""
    last_err: Exception | None = None
    for kind, target in strategies:
        try:
            loc = _build_locator(page, kind, target)
            await loc.wait_for(state="visible", timeout=8_000)
            await loc.fill(value)
            return
        except Exception as e:  # noqa: BLE001
            last_err = e
            continue
    raise RuntimeError(
        f"DHL fill: no strategy matched for value (last error: {last_err})"
    )


async def _click_first_match(page, strategies: list[tuple],
                             timeout: int = 8_000) -> None:
    last_err: Exception | None = None
    for kind, target in strategies:
        try:
            loc = _build_locator(page, kind, target)
            await loc.wait_for(state="visible", timeout=timeout)
            await loc.click()
            return
        except Exception as e:  # noqa: BLE001
            last_err = e
            continue
    raise RuntimeError(f"DHL click: no strategy matched (last error: {last_err})")


def _build_locator(page, kind: str, target):
    """Translate (kind, target) into a Playwright Locator."""
    if kind == "label":
        return page.get_by_label(target)
    if kind == "placeholder":
        return page.get_by_placeholder(target)
    if kind == "text":
        return page.get_by_text(target).first
    if kind == "role":
        role, name = target
        return page.get_by_role(role, name=name).first
    if kind == "css":
        return page.locator(target).first
    raise ValueError(f"unknown locator kind: {kind}")


async def _extract_all_pages(page, max_pages: int = 20) -> list[dict]:
    """Extract shipments from every page of the Manage Shipments list.

    DHL paginates the list (typically 100 rows per page). We extract
    page 1, then look for the next-page control — either a numbered
    button ("2", "3", …) or an arrow (">"). Click it, wait for the
    table to re-render, extract again, repeat until exhausted or we
    hit max_pages (defensive cap so a misbehaving DOM doesn't loop
    forever). All rows from all pages get accumulated into one list.
    """
    all_rows: list[dict] = []
    page_num = 1
    while page_num <= max_pages:
        rows = await page.evaluate(_EXTRACT_JS)
        log.info("DHL: page %d → %d row(s) (running total: %d)",
                 page_num, len(rows), len(all_rows) + len(rows))
        all_rows.extend(rows)
        # Look for the link/button that goes to the NEXT page.
        # Try numbered first ("2", "3" …), then a generic arrow.
        next_num = page_num + 1
        candidates = [
            f'button[aria-label="Page {next_num}"]',
            f'a[aria-label="Page {next_num}"]',
            f'button:has-text("{next_num}")',
            f'a:has-text("{next_num}")',
            'button[aria-label="Next"]',
            'a[aria-label="Next"]',
            'button:has-text("›")',
            'button:has-text(">")',
        ]
        clicked = False
        for css in candidates:
            try:
                btn = page.locator(css).first
                # Visible-and-enabled check via a brief wait — the
                # disabled-state on the "Next" arrow at the last page
                # will fail this and we'll break out cleanly.
                await btn.wait_for(state="visible", timeout=1_500)
                is_enabled = await btn.is_enabled()
                if not is_enabled:
                    continue
                await btn.click(timeout=3_000)
                clicked = True
                break
            except Exception:  # noqa: BLE001
                continue
        if not clicked:
            log.info("DHL: no more pages after %d (no next button visible)",
                     page_num)
            break
        # Wait for the table to refresh. DHL's pagination is an XHR;
        # the existing row nodes get reused so we can't simply wait
        # for a 'detached' state. Sleep + re-wait for shipments-loaded
        # is the most reliable path.
        await asyncio.sleep(2.5)
        try:
            await _wait_for_shipments_loaded(page)
        except Exception:  # noqa: BLE001
            pass
        page_num += 1
    return all_rows


async def _wait_for_shipments_loaded(page) -> None:
    """Wait until the shipments list is either populated or empty.

    Races a 10-digit AWB anchor (the populated case) against the
    "no shipments" empty-state text. Single 25-second timeout instead
    of three sequential 20-second ones.
    """
    populated = page.locator("a").filter(has_text=re.compile(r"^\s*\d{10}\s*$")).first
    empty = page.get_by_text(re.compile(r"no shipments", re.I)).first
    try:
        await populated.or_(empty).wait_for(state="visible", timeout=25_000)
    except Exception:  # noqa: BLE001
        log.warning("DHL: shipments page didn't show a recognised state in 25s")


# --------------------------- DOM extraction JS ---------------------------

# Runs inside the page. Returns a list of {awb, ship_date_text, ship_to_text}.
# Strategy: every shipment row contains an anchor whose text matches
# a 10-digit number (the AWB). For each such anchor, climb up to the
# nearest "shipment card" ancestor — heuristically, the closest <li>,
# <article>, or <div> that contains at least 2 paragraph-like children.
# Then read all the text inside that card; the AWB sits at the top,
# ship date follows the literal "Shipment Date" label, ship-to block
# follows the "Ship To" label.
_EXTRACT_JS = r"""
() => {
  const rows = [];

  // 1. Find every potential AWB link (anchor with 10-digit text).
  const awbAnchors = Array.from(document.querySelectorAll("a"))
    .filter(a => /^\s*\d{10}\s*$/.test(a.textContent || ""));

  for (const anchor of awbAnchors) {
    // 2. Climb to the nearest card-like ancestor. We stop at the first
    //    element that contains both a 'Ship To' label and a date-ish
    //    block, capped at 8 levels to avoid grabbing the whole page.
    let card = anchor;
    for (let i = 0; i < 8; i++) {
      card = card.parentElement;
      if (!card) break;
      const txt = card.innerText || "";
      if (/Ship To/i.test(txt) && /Shipment Date/i.test(txt)) break;
    }
    if (!card) continue;

    const text = card.innerText || "";

    // 3. Parse AWB out of the anchor itself.
    const awb = (anchor.textContent || "").trim();

    // 4. Pull the "Shipment Date" — the line right after that label.
    //    DHL renders it on the line below the label.
    const dateMatch = text.match(
      /Shipment Date\s*\n?\s*([A-Z][a-z]+ \d{1,2},\s*\d{4})/i
    );
    const ship_date_text = dateMatch ? dateMatch[1] : "";

    // 5. The Ship To block — three lines (company, contact, city/country)
    //    following the literal "Ship To" label. Lenient stop pattern
    //    so we don't drop the whole capture on headless renders where
    //    DHL's action-row labels (Quick View / Print Labels) aren't
    //    visible. We accept ANY of: double-newline paragraph break,
    //    explicit DHL action labels, a "Title Case Title Case" line
    //    (likely the next labelled section), or end-of-string.
    const shipToMatch = text.match(
      /Ship To\s*\n([\s\S]*?)(?:\n\s*\n|\n\s*(?:Quick View|Print Labels|Create Return|Copy|Track|Edit)\b|\n[A-Z][a-z]+ [A-Z][a-z]|$)/i
    );
    const ship_to_text = shipToMatch ? shipToMatch[1].trim() : "";

    rows.push({ awb, ship_date_text, ship_to_text });
  }

  return rows;
}
"""


# --------------------------- row → Shipment ---------------------------

_MONTH_LOOKUP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _parse_dhl_date(text: str) -> date | None:
    """Parse 'May 19, 2026' (and a couple of common variants)."""
    if not text:
        return None
    t = text.strip()
    # Try the canonical MyDHL+ format first.
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(t, fmt).date()
        except ValueError:
            continue
    # Loose regex fallback (different month abbrevs / spacing).
    m = re.match(r"([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})", t)
    if m:
        mo = _MONTH_LOOKUP.get(m.group(1).lower()[:3])
        if mo:
            try:
                return date(int(m.group(3)), mo, int(m.group(2)))
            except ValueError:
                return None
    return None


def _row_to_shipment(raw: dict) -> Shipment | None:
    """Turn one extracted row dict into a Shipment. Returns None on bad data."""
    awb = (raw.get("awb") or "").strip()
    if not re.fullmatch(r"\d{10}", awb):
        return None

    ship_date = _parse_dhl_date(raw.get("ship_date_text") or "")
    if ship_date is None:
        log.warning(_TODAY_FALLBACK_WARN + " (awb=%s)", awb)
        ship_date = date.today()

    # Ship To block: line 1 is the customer company, last line is "City, Country".
    raw_block = (raw.get("ship_to_text") or "").strip()
    lines = [ln.strip() for ln in raw_block.splitlines() if ln.strip()]
    customer = lines[0] if lines else ""
    country = ""
    if lines:
        last = lines[-1]
        # "KARACHI, Pakistan" → country = "Pakistan"
        if "," in last:
            country = last.rsplit(",", 1)[1].strip()
        else:
            country = last

    if not customer:
        log.info("DHL: skipping row with no customer name (awb=%s)", awb)
        return None

    return Shipment(
        carrier="DHL",
        awb=awb,
        recipient_name=customer,
        recipient_country=country,
        ship_date=ship_date,
    )


# --------------------------- debug ---------------------------

async def _dump(page, label: str) -> None:
    """When DHL_DEBUG=1, save a screenshot + HTML for the current page.

    Helpful for the first live run — if anything misbehaves we get the
    actual rendered DOM and pixels, not just a stack trace. Files land
    in the OS temp dir so they don't pollute the project.
    """
    if not _DEBUG:
        return
    try:
        outdir = Path(tempfile.gettempdir()) / "dhl_debug"
        outdir.mkdir(exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        png = outdir / f"{stamp}_{label}.png"
        html = outdir / f"{stamp}_{label}.html"
        await page.screenshot(path=str(png), full_page=True)
        content = await page.content()
        html.write_text(content, encoding="utf-8")
        log.info("DHL debug: wrote %s and %s", png, html)
    except Exception as e:  # noqa: BLE001
        log.warning("DHL debug dump failed: %s", e)
