"""MMS3 product-price client — minimal port from NPProductBot for /pp.

Pricing only. Fetches enough to print:
    Code: ...
    Name: ...
    R&D Price: USD x.xx
    Raw Material Cost: USD x.xxxx

Does NOT parse or enrich ingredients (that's NPProductBot's /pi job).
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from threading import Lock
from typing import Optional

import requests
from bs4 import BeautifulSoup

import config

log = logging.getLogger(__name__)

BASE_URL = "http://www.npsin.com/mms3"


class MMSError(Exception):
    pass


class ProductNotFound(MMSError):
    pass


@dataclass
class Product:
    sid: str
    code: str
    name: str
    raw_material_cost_usd: float
    rd_price_usd: Optional[float] = None
    # V1.17.x — preserve the SR page's native price + currency so /pp
    # can display the exact figure when the rep's preferred currency
    # matches the source (no round-trip via USD). For J- codes the SR
    # row is usually in IDR, for B- codes in THB, for S- codes in USD.
    rd_price_native_amount: Optional[float] = None
    rd_price_native_currency: Optional[str] = None


class MMSProductClient:
    """Thread-safe MMS3 product info fetcher. Auto re-logs in on session expiry."""

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update(
            {"User-Agent": "Mozilla/5.0 NPSampleBot/pp"}
        )
        self._lock = Lock()
        self._logged_in = False
        self._rates_to_usd: Optional[dict[str, float]] = None

    # ---------- auth ----------
    def _login_locked(self) -> None:
        if not config.MMS_PASSWORD:
            raise MMSError("MMS_PASSWORD missing in .env")
        r = self._session.get(f"{BASE_URL}/login.do", timeout=15)
        m = re.search(r'action="([^"]*login\.do[^"]*)"', r.text)
        if not m:
            raise MMSError("Login form action not found")
        action = m.group(1)
        if action.startswith("/"):
            action = "http://www.npsin.com" + action
        r = self._session.post(
            action,
            data={
                "name": config.MMS_USER,
                "password": config.MMS_PASSWORD,
                "login": "Login",
                "faildCount": "1",
            },
            headers={"Referer": f"{BASE_URL}/login.do"},
            timeout=15,
            allow_redirects=True,
        )
        if "Logout" not in r.text:
            raise MMSError("MMS login failed — check MMS_USER / MMS_PASSWORD")
        self._logged_in = True
        log.info("MMS product client login successful")

    def _ensure_logged_in(self) -> None:
        if not self._logged_in:
            self._login_locked()

    @staticmethod
    def _looks_like_login(text: str) -> bool:
        return 'name="loginForm"' in text or "User ID is required" in text

    def _get(self, url: str, **kw) -> requests.Response:
        with self._lock:
            self._ensure_logged_in()
            r = self._session.get(url, timeout=20, **kw)
            # Audit V1.13.14 — MMS responses are UTF-8 but the server
            # sometimes omits charset, in which case requests guesses
            # ISO-8859-1 and Chinese product names come out mojibake.
            r.encoding = "utf-8"
            if self._looks_like_login(r.text):
                self._logged_in = False
                self._login_locked()
                r = self._session.get(url, timeout=20, **kw)
                r.encoding = "utf-8"
            return r

    def _post(self, url: str, data: dict, **kw) -> requests.Response:
        with self._lock:
            self._ensure_logged_in()
            r = self._session.post(url, data=data, timeout=20, **kw)
            r.encoding = "utf-8"  # see _get comment
            if self._looks_like_login(r.text):
                self._logged_in = False
                self._login_locked()
                r = self._session.post(url, data=data, timeout=20, **kw)
                r.encoding = "utf-8"
            return r

    # ---------- lookup ----------
    def find_sid(self, code: str) -> str:
        """doFind → doList, return prod_id (SID) for the given code.

        Retries the whole sequence once if the doFind response is missing
        the "Found <b>N</b>" marker entirely — that signals the server's
        search state was wiped between requests (typically session
        expiry mid-flow) which would otherwise make a perfectly-valid
        code look "not found." The retry forces a clean re-login so
        doFind and doList run against the same fresh session.
        """
        payload_base = {"code": code, "codeOptions": ["d-code", "p-code"]}

        def _attempt() -> tuple[Optional[str], bool]:
            """Returns (sid_or_None, search_page_reached)."""
            r = self._post(
                f"{BASE_URL}/master/productSearch.do",
                data={"command": "doFind", **payload_base},
            )
            m = re.search(r"Found\s*<b>(\d+)</b>", r.text)
            search_page_reached = m is not None
            if not search_page_reached:
                # doFind didn't land on the search-results page at all —
                # likely session-state mismatch. Signal the caller to retry.
                return None, False
            if int(m.group(1)) == 0:
                # Real "no such product" — the page rendered cleanly with
                # a zero count. Don't waste a retry.
                return None, True
            r = self._post(
                f"{BASE_URL}/master/productSearch.do",
                data={"command": "doList", **payload_base},
            )
            ids = re.findall(r"sampleRequestCreate\.do\?prod_id=(\d+)", r.text)
            if not ids:
                # doList came back empty even though doFind reported hits.
                # Strong signal that the server-side search session was
                # invalidated between the two POSTs. Retry with a fresh
                # login so both POSTs share a session.
                return None, False
            return ids[0], True

        sid, search_page_reached = _attempt()
        if sid:
            return sid
        if not search_page_reached:
            log.info(
                "find_sid: search state lost mid-flow for %s — forcing re-login and retrying",
                code,
            )
            with self._lock:
                self._logged_in = False
            sid, _ = _attempt()
            if sid:
                return sid
        raise ProductNotFound(f"No product found for code {code!r}")

    def fetch_detail(self, sid: str) -> Product:
        """Pull the productDetail page; only extract code, name, priceTotal."""
        r = self._get(f"{BASE_URL}/master/productDetail.do?sid={sid}")
        html = r.text

        def hidden(name: str) -> Optional[str]:
            m = re.search(
                rf'<input[^>]*type="hidden"[^>]*name="{re.escape(name)}"[^>]*value="([^"]*)"',
                html,
            )
            return m.group(1) if m else None

        code = hidden("code") or ""
        name = hidden("name") or ""
        price_total = hidden("priceTotal")
        if not code:
            raise MMSError(f"Could not parse productDetail for sid={sid}")
        return Product(
            sid=sid,
            code=code,
            name=name,
            raw_material_cost_usd=float(price_total) if price_total else 0.0,
        )

    def fetch_rd_price(
        self, code: str
    ) -> Optional[tuple[float, float, str]]:
        """Scrape the latest sample request page for the R&D Price cell.

        Returns (usd_value, native_amount, native_currency) so callers can
        choose to display the native figure exactly (without round-tripping
        through USD and losing precision). Returns None when no SR page
        holds a price for this code, or the SR price is in a currency MMS3
        won't convert (preserves previous behaviour of "no R&D price").
        """
        payload = {"code": code, "codeOptions": ["d-code", "p-code"]}
        # doFind first to seed search session, then doList to get sreq codes.
        self._post(
            f"{BASE_URL}/master/productSearch.do",
            data={"command": "doFind", **payload},
        )
        r = self._post(
            f"{BASE_URL}/master/productSearch.do",
            data={"command": "doList", **payload},
        )
        sreq_codes = re.findall(
            r'sampleRequestUpdate\.do\?code=([A-Za-z0-9\-]+)', r.text
        )
        seen: set[str] = set()
        ordered: list[str] = []
        for c in sreq_codes:
            if c not in seen:
                seen.add(c)
                ordered.append(c)
        if not ordered:
            log.info(
                "RD price: productSearch for %s returned no sample-request "
                "codes. Product has no sampling history in MMS3.", code,
            )
            return None
        log.info(
            "RD price: trying %d SR page(s) for code %s: %s",
            len(ordered), code, ordered[:10],
        )
        for sreq_code in ordered:
            r = self._get(
                f"{BASE_URL}/master/sampleRequestUpdate.do?code={sreq_code}"
            )
            extracted = _extract_rd_price_from_sample_request(
                r.text, code, debug_label=sreq_code,
            )
            if extracted is None:
                continue
            amount, cur = extracted
            usd = amount if cur == "USD" else self._to_usd(amount, cur)
            if usd is None:
                log.warning(
                    "R&D price for %s in %s found but currency %s not converted",
                    code, sreq_code, cur,
                )
                continue
            return usd, amount, cur
        log.warning(
            "RD price: code %s not parseable on any of %d SR page(s). "
            "Falling back to FSL.", code, len(ordered),
        )
        return None

    def _fetch_rd_via_probe_sr(
        self, code: str
    ) -> Optional[tuple[float, float, str]]:
        """Fallback R&D price lookup via the Add-then-Delete trick.

        Used when `fetch_rd_price` finds no existing sample-request page
        listing this code (i.e. the product has never been sampled). MMS3
        only renders R&D price as part of the `addproduct` response on a
        sampleRequestUpdate.do page — there is no standalone lookup
        endpoint. So the bot opens a stable "probe" SR, runs the
        Find → Add dance to make MMS3 emit the price, scrapes the row,
        then ALWAYS deletes the row to leave the SR untouched.

        Returns (usd, native_amount, native_currency) on success, None on
        any failure. Cleanup of the probed row is best-effort but logged
        loudly when it fails so the SR can be inspected manually.
        """
        probe_sr = config.MMS_PROBE_SR_CODE
        if not probe_sr:
            log.info(
                "RD probe: no MMS_PROBE_SR_CODE configured — skipping fallback. "
                "Set it to an SR code (e.g. 'J-123J43-001') to enable.",
            )
            return None
        url = f"{BASE_URL}/master/sampleRequestUpdate.do?code={probe_sr}"

        # GET the probe SR to load form state.
        r = self._get(url)
        soup = BeautifulSoup(r.text, "html.parser")
        form = soup.find("form")
        if not form:
            log.warning("RD probe: SR %s has no form", probe_sr)
            return None

        # Pick the section with the fewest samples — smallest payload and
        # least chance of touching whatever a rep is concurrently editing.
        section_n = _pick_smallest_section(r.text)
        if section_n is None:
            log.warning("RD probe: SR %s has no usable sections", probe_sr)
            return None
        baseline = _count_section_samples(r.text, section_n)
        log.info(
            "RD probe: using SR=%s section=%d (baseline samples=%d) to look up %s",
            probe_sr, section_n, baseline, code,
        )

        added_idx: Optional[int] = None
        result: Optional[tuple[float, float, str]] = None
        try:
            # Find: server populates the candidate dropdown.
            payload = _form_payload(form)
            find_payload = _override(payload, {
                "command": f"find{section_n}",
                f"productSearchCode[{section_n}]": code,
            })
            r = self._post(url, data=find_payload)
            # Check that exactly one candidate matched. If none → product
            # doesn't exist; if multiple → ambiguous, abort to be safe.
            soup = BeautifulSoup(r.text, "html.parser")
            opts = []
            for sel in soup.find_all("select"):
                if sel.get("name") == f"productSearchCode[{section_n}]":
                    opts = [
                        o.get("value", "") for o in sel.find_all("option")
                        if o.get("value", "")
                    ]
                    break
            real_matches = [o for o in opts if o.upper() == code.upper()]
            if not real_matches:
                log.info("RD probe: Find returned no match for %s", code)
                return None

            # Add: MMS3 renders a new row with the R&D price.
            form_after_find = soup.find("form")
            if form_after_find is None:
                log.warning("RD probe: Find response has no form")
                return None
            payload = _form_payload(form_after_find)
            add_payload = _override(payload, {"command": f"addproduct{section_n}"})
            r = self._post(url, data=add_payload)
            after_count = _count_section_samples(r.text, section_n)
            if after_count != baseline + 1:
                log.warning(
                    "RD probe: addproduct didn't add exactly 1 sample "
                    "(baseline=%d after=%d). Bailing.",
                    baseline, after_count,
                )
                # added_idx stays None — no row to delete
                return None
            added_idx = after_count - 1

            # Scrape the price from the new row.
            extracted = _extract_rd_price_from_sample_request(
                r.text, code, debug_label=f"{probe_sr}#probe",
            )
            if extracted is None:
                log.warning("RD probe: row added but no price cell parsed")
            else:
                amount, cur = extracted
                usd = amount if cur == "USD" else self._to_usd(amount, cur)
                if usd is None:
                    log.warning(
                        "RD probe: extracted %s %s but MMS3 has no rate to USD",
                        cur, amount,
                    )
                else:
                    log.info(
                        "RD probe: success for %s → %s %s (USD %.4f)",
                        code, cur, f"{amount:,.2f}", usd,
                    )
                    result = (usd, amount, cur)
        finally:
            # ALWAYS attempt cleanup if we added a row. Multiple retries
            # on Delete because leaving a stray row in production SRs is
            # the worst possible outcome — pollutes data the team relies on.
            if added_idx is not None:
                self._cleanup_probe_row(url, probe_sr, section_n, added_idx, baseline)
        return result

    def _cleanup_probe_row(
        self, url: str, probe_sr: str, section_n: int,
        added_idx: int, baseline: int,
    ) -> None:
        """Delete a probe row from the SR, with retries. Logs loudly on
        failure so the team can manually inspect the SR."""
        for attempt in range(3):
            try:
                r = self._get(url)
                soup = BeautifulSoup(r.text, "html.parser")
                form = soup.find("form")
                if form is None:
                    log.error("RD probe cleanup: form not found on attempt %d", attempt)
                    continue
                # If the row at the expected index already isn't there,
                # check if any sample in this section has the wrong count
                # and stop trying — we don't want to delete the wrong row.
                current = _count_section_samples(r.text, section_n)
                if current == baseline:
                    log.info("RD probe cleanup: SR %s already at baseline", probe_sr)
                    return
                payload = _form_payload(form)
                del_payload = _override(payload, {
                    "command": f"delete{section_n}_{added_idx}",
                })
                self._post(url, data=del_payload)
                r = self._get(url)
                final = _count_section_samples(r.text, section_n)
                if final == baseline:
                    log.info(
                        "RD probe cleanup: SR %s restored to baseline=%d",
                        probe_sr, baseline,
                    )
                    return
                log.warning(
                    "RD probe cleanup attempt %d: count=%d, expected=%d",
                    attempt, final, baseline,
                )
            except Exception as e:  # noqa: BLE001
                log.warning("RD probe cleanup attempt %d failed: %s", attempt, e)
        log.error(
            "RD PROBE CLEANUP FAILED on SR %s section %d. Sample row %d "
            "may still be present — inspect manually.",
            probe_sr, section_n, added_idx,
        )

    def get_rate_to_usd(self, currency: str) -> Optional[float]:
        """Return how many USD equal 1 unit of `currency` per MMS3.

        e.g. for IDR this is ~0.0000559 (1 IDR ≈ 0.0000559 USD). Returns
        None when MMS3 doesn't list this currency or the rates page
        couldn't be parsed. Callers should NOT substitute a hardcoded
        rate — they should show the source value verbatim instead.
        """
        cur = (currency or "").upper()
        if cur == "USD":
            return 1.0
        rates_to_usd = self._get_rates_to_usd()
        return rates_to_usd.get(cur)

    def get_rate_from_usd(self, currency: str) -> Optional[float]:
        """Return how many `currency` units make up 1 USD per MMS3.

        Inverse of `get_rate_to_usd`. Used by /pp + /lastsample display
        so IDR / THB reps see figures consistent with MMS3's quotes,
        not derived from stale hardcoded constants. Returns None when
        MMS3 doesn't list this currency or the rates page couldn't be
        parsed — caller must NOT fall back to a hardcoded rate.
        """
        rate_to_usd = self.get_rate_to_usd(currency)
        if not rate_to_usd:
            return None
        return 1.0 / rate_to_usd

    def _get_rates_to_usd(self) -> dict[str, float]:
        if self._rates_to_usd is not None:
            return self._rates_to_usd
        r = self._get(f"{BASE_URL}/master/exchangeRates.do")
        rates = _parse_rates_to_usd(r.text)
        # If parsing only produced the {"USD": 1.0} fallback, the MMS
        # exchange-rates page layout has changed and every non-USD R&D
        # price will silently come back as "n/a" until someone notices.
        # Log loudly so this shows up in Railway error logs, AND don't
        # cache the broken result — retry parsing on the next /pp call
        # so a transient bad response doesn't poison the rest of the
        # bot's uptime.
        if len(rates) <= 1:
            log.error(
                "MMS exchange-rate page parse FAILED — only %d rate(s) extracted: %s. "
                "Non-USD R&D prices will show as 'n/a'. Response length: %d chars. "
                "Check whether the exchangeRates.do page layout has changed.",
                len(rates), rates, len(r.text),
            )
            # Don't cache — next call retries.
            return rates
        self._rates_to_usd = rates
        log.info("Loaded MMS exchange rates → USD: %s", rates)
        return rates

    def _to_usd(self, value: float, currency: str) -> Optional[float]:
        cur = (currency or "").upper()
        if cur == "USD":
            return value
        rate = self._get_rates_to_usd().get(cur)
        if rate is None:
            return None
        return round(value * rate, 4)

    def fetch_product(self, code: str) -> Product:
        sid = self.find_sid(code)
        product = self.fetch_detail(sid)
        try:
            rd = self.fetch_rd_price(code)
        except Exception as e:  # noqa: BLE001
            log.warning("R&D price lookup failed for %s: %s", code, e)
            rd = None
        # V1.17.x — when the normal SR-search path returns nothing (code
        # has never been added to any sample request), fall back to the
        # Add-then-Delete probe trick. Disabled when MMS_PROBE_SR_CODE
        # isn't configured. Audit data suggests this only fires for a
        # small fraction of /pp lookups so the per-call overhead is fine.
        if rd is None:
            try:
                rd = self._fetch_rd_via_probe_sr(code)
            except Exception as e:  # noqa: BLE001
                log.warning("RD probe SR fallback errored for %s: %s", code, e)
        if rd is not None:
            usd, native_amount, native_cur = rd
            product.rd_price_usd = usd
            product.rd_price_native_amount = native_amount
            product.rd_price_native_currency = native_cur
        return product


# ---------- form-payload helpers (for the RD probe Add-Delete dance) ----------

def _form_payload(form) -> list[tuple[str, str]]:
    """Build the (name, value) list that mirrors what the browser would
    submit for this form. Preserves field order so the server-side state
    machine (it's a stateful Struts app) sees identical input to the
    user's browser. Skips button inputs."""
    out: list[tuple[str, str]] = []
    for inp in form.find_all(["input", "select", "textarea"]):
        name = inp.get("name")
        if not name:
            continue
        if inp.name == "input":
            typ = (inp.get("type") or "text").lower()
            if typ in ("button", "submit", "reset"):
                continue
            if typ == "checkbox":
                if inp.has_attr("checked"):
                    out.append((name, inp.get("value", "on")))
                continue
            if typ == "radio":
                if inp.has_attr("checked"):
                    out.append((name, inp.get("value", "")))
                continue
            out.append((name, inp.get("value", "")))
        elif inp.name == "select":
            sel = inp.find("option", selected=True)
            out.append((name, sel.get("value", "") if sel else ""))
        elif inp.name == "textarea":
            out.append((name, inp.get_text()))
    return out


def _override(payload: list[tuple[str, str]], kvs: dict[str, str]) -> list[tuple[str, str]]:
    """Override payload entries by name; append any not previously present."""
    used: set[str] = set()
    out: list[tuple[str, str]] = []
    for k, v in payload:
        if k in kvs:
            out.append((k, kvs[k]))
            used.add(k)
        else:
            out.append((k, v))
    for k, v in kvs.items():
        if k not in used:
            out.append((k, v))
    return out


def _count_section_samples(html: str, section_n: int) -> int:
    """Count how many sample rows exist in sreq1[N] on this page."""
    return len(set(re.findall(
        rf"sreq1\[{section_n}\]\.sample\[(\d+)\]\.callId", html,
    )))


def _pick_smallest_section(html: str) -> Optional[int]:
    """Pick the SR section with the fewest sample rows. Smallest payload,
    least risk of touching whatever a rep is actively editing."""
    # Find all section indices present on the page.
    sections = set(int(s) for s in re.findall(r"sreq1\[(\d+)\]\.", html))
    if not sections:
        return None
    by_count = sorted((_count_section_samples(html, n), n) for n in sections)
    return by_count[0][1]


# ---------- parser helpers ----------

# Loosened in V1.17.x: was anchored at end with `$`, which broke when
# MMS3 renders the cell with trailing units like 'IDR 44,707 / Kg' or
# stray whitespace. Now matches at the START of the cell and allows
# anything after the number — safe because each row only has one
# "CUR amount" cell (the price), so the first-match wins is correct.
_RD_PRICE_CELL = re.compile(r"^\s*([A-Z]{3})\s*([\d.,]+(?:\.\d+)?)")


def _extract_rd_price_from_sample_request(
    html: str, product_code: str, *, debug_label: str = ""
) -> Optional[tuple[float, str]]:
    """On a sampleRequestUpdate page, find the product's row and read its R&D Price cell.

    `debug_label` is a free-form string (typically the SR code) included
    in log lines so we can correlate a failure to the specific MMS3
    page that didn't yield a price. Helps diagnose Jakarta-specific
    parsing issues without re-running with verbose tracing.
    """
    soup = BeautifulSoup(html, "html.parser")
    # Case-insensitive code match — MMS3 stores codes in canonical
    # upper-case (e.g. 'J-X33A1-06') but reps often type the casing
    # they see on labels ('J-X33a1-06'). Without this, the R&D price
    # lookup silently failed for any mixed-case code and the bot
    # fell back to the FSL "last sampled" value.
    target_code = (product_code or "").strip().upper()
    # Collect ALL <small><b>X</b></small> codes on this page so a failed
    # match can log what was actually there — invaluable for diagnosing
    # codes the bot consistently can't find (the Jakarta J- code
    # cluster, in particular, fails for reasons we haven't pinned down).
    all_smallb_codes: list[str] = []
    for tag in soup.find_all("small"):
        b = tag.find("b")
        if not b:
            continue
        b_txt = b.get_text(strip=True).strip()
        if b_txt:
            all_smallb_codes.append(b_txt.upper())
        if b_txt.upper() != target_code:
            continue
        row = tag.find_parent("tr")
        if not row:
            continue
        cell_texts: list[str] = []
        for td in row.find_all("td"):
            text = td.get_text(" ", strip=True).replace("\xa0", " ").strip()
            cell_texts.append(text)
            m = _RD_PRICE_CELL.match(text)
            if not m:
                continue
            cur = m.group(1)
            try:
                amount = float(m.group(2).replace(",", ""))
            except ValueError:
                continue
            if amount <= 0:
                continue
            return amount, cur
        # Code matched but no parseable price cell in the row — log the
        # row contents so we can teach the regex about whatever format
        # MMS3 used here. Common Jakarta-side culprits: '/Kg' suffix,
        # newlines, or the price living on the next row down.
        log.warning(
            "RD price match failed: code=%s found in SR=%r but no price "
            "cell parsed. Row cells: %s",
            target_code, debug_label, cell_texts,
        )
    # No <small><b>CODE</b></small> matched on this page. Log what codes
    # we DID find so we can spot Jakarta-side oddities (different
    # casing, parent-vs-variant codes, alternate HTML structure).
    if all_smallb_codes:
        log.info(
            "RD price: code %s not on SR=%r. Page had %d code tag(s): %s",
            target_code, debug_label,
            len(all_smallb_codes), all_smallb_codes[:20],
        )
    return None


def _parse_rates_to_usd(html: str) -> dict[str, float]:
    """Parse exchangeRates.do matrix → {CUR: how many USD per 1 unit of CUR}."""
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for table in soup.find_all("table"):
        trs = table.find_all("tr", recursive=False)
        if not trs:
            continue
        header_cells = [c.get_text(strip=True) for c in trs[0].find_all("td", recursive=False)]
        if "USD" in header_cells and "SGD" in header_cells and "JPY" in header_cells:
            rows = trs
            break
    if not rows:
        return {"USD": 1.0}
    headers = [c.get_text(strip=True) for c in rows[0].find_all("td", recursive=False)]
    try:
        usd_col = headers.index("USD")
    except ValueError:
        return {"USD": 1.0}
    rates: dict[str, float] = {"USD": 1.0}
    for tr in rows[1:]:
        cells = [c.get_text(" ", strip=True) for c in tr.find_all("td", recursive=False)]
        if len(cells) <= usd_col:
            continue
        row_cur = cells[0].strip().upper()
        if not row_cur or row_cur == "USD":
            continue
        raw = cells[usd_col].replace(",", "").strip()
        try:
            rates[row_cur] = float(raw)
        except ValueError:
            continue
    return rates


# ---------- formatting ----------

def format_pp(p: Product) -> str:
    rd = f"USD {p.rd_price_usd:.2f}" if p.rd_price_usd is not None else "n/a"
    return (
        f"Code: {p.code}\n"
        f"Name: {p.name}\n"
        f"R&D Price: {rd}\n"
        f"Raw Material Cost: USD {p.raw_material_cost_usd:.4f}"
    )


# Module-level singleton — created lazily so importing the module is cheap.
_singleton: Optional[MMSProductClient] = None
_singleton_lock = Lock()


def get_client() -> MMSProductClient:
    global _singleton
    with _singleton_lock:
        if _singleton is None:
            _singleton = MMSProductClient()
    return _singleton
