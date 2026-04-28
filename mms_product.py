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
            if self._looks_like_login(r.text):
                self._logged_in = False
                self._login_locked()
                r = self._session.get(url, timeout=20, **kw)
            return r

    def _post(self, url: str, data: dict, **kw) -> requests.Response:
        with self._lock:
            self._ensure_logged_in()
            r = self._session.post(url, data=data, timeout=20, **kw)
            if self._looks_like_login(r.text):
                self._logged_in = False
                self._login_locked()
                r = self._session.post(url, data=data, timeout=20, **kw)
            return r

    # ---------- lookup ----------
    def find_sid(self, code: str) -> str:
        """doFind → doList, return prod_id (SID) for the given code."""
        payload_base = {"code": code, "codeOptions": ["d-code", "p-code"]}
        r = self._post(
            f"{BASE_URL}/master/productSearch.do",
            data={"command": "doFind", **payload_base},
        )
        m = re.search(r"Found\s*<b>(\d+)</b>", r.text)
        found_n = int(m.group(1)) if m else 0
        if found_n == 0:
            raise ProductNotFound(f"No product found for code {code!r}")
        r = self._post(
            f"{BASE_URL}/master/productSearch.do",
            data={"command": "doList", **payload_base},
        )
        ids = re.findall(r"sampleRequestCreate\.do\?prod_id=(\d+)", r.text)
        if not ids:
            raise ProductNotFound(f"Product list had no prod_id link for {code!r}")
        return ids[0]

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

    def fetch_rd_price(self, code: str) -> Optional[float]:
        """Scrape the latest sample request page for the R&D Price cell."""
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
        for sreq_code in ordered:
            r = self._get(
                f"{BASE_URL}/master/sampleRequestUpdate.do?code={sreq_code}"
            )
            extracted = _extract_rd_price_from_sample_request(r.text, code)
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
            return usd
        return None

    def _get_rates_to_usd(self) -> dict[str, float]:
        if self._rates_to_usd is not None:
            return self._rates_to_usd
        r = self._get(f"{BASE_URL}/master/exchangeRates.do")
        self._rates_to_usd = _parse_rates_to_usd(r.text)
        log.info("Loaded MMS exchange rates → USD: %s", self._rates_to_usd)
        return self._rates_to_usd

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
            product.rd_price_usd = self.fetch_rd_price(code)
        except Exception as e:  # noqa: BLE001
            log.warning("R&D price lookup failed for %s: %s", code, e)
        return product


# ---------- parser helpers ----------

_RD_PRICE_CELL = re.compile(r"^([A-Z]{3})\s*([\d.,]+)$")


def _extract_rd_price_from_sample_request(
    html: str, product_code: str
) -> Optional[tuple[float, str]]:
    """On a sampleRequestUpdate page, find the product's row and read its R&D Price cell."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("small"):
        b = tag.find("b")
        if not (b and b.get_text(strip=True) == product_code):
            continue
        row = tag.find_parent("tr")
        if not row:
            continue
        for td in row.find_all("td"):
            text = td.get_text(" ", strip=True).replace("\xa0", " ").strip()
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
