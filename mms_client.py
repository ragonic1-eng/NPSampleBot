"""MMS3 HTTP client — login + fetch sample submissions via date range.

The MMS site (http://www.npsin.com/mms3/) is a plain JSP app with:
  - POST /login.do   (form: faildCount=0, name, password, login=Login)
  - GET  /master/sampleSubmissionSearch.do  (HTML table result; 1000-row cap)

We use a requests.Session so the JSESSIONID cookie persists across calls.

For our use case the search scope is always Sample Date Out between two dates.
We keep chunks ≤ ~6 months to stay well under the 1000-row cap.
"""
from __future__ import annotations

import datetime as dt
import logging
import random
import re
import string
from dataclasses import dataclass
from typing import Iterator

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

BASE_URL = "http://www.npsin.com/mms3"
LOGIN_URL = f"{BASE_URL}/login.do"
SEARCH_URL = f"{BASE_URL}/master/sampleSubmissionSearch.do"
DWR_URL = f"{BASE_URL}/dwr/call/plaincall/sampleDwr.loadItemList.dwr"

HEADERS_BASE = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) NPSampleBot/1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-SG,en;q=0.9",
}

# Columns we expect in the MMS HTML result table, in order.
MMS_COLS = [
    "sample_request_date",
    "sample_request_code",
    "sales",
    "country",
    "customer_code",
    "customer_name",
    "product_code",
    "product_name",
    "rd_price",
    "confirmed",
    "rd_name_dc",
    "rd_sent",
    "sample_date_out",
    "quantity_g",
    "pack",
    "mode_of_delivery",
    "awb",
    "rd_remarks",
    "shipping_remarks",
    "feedback",
    "star",
]


@dataclass
class SampleRow:
    """One MMS sample-submission row, in the shape our sheet cares about."""
    sample_request_date: str
    sample_request_code: str
    sales: str
    country: str
    customer_code: str
    customer_name: str
    product_code: str
    product_name: str
    rd_price: str
    sample_date_out: str
    feedback: str

    def sample_date_out_as_date(self) -> dt.date | None:
        s = (self.sample_date_out or "").strip()
        try:
            return dt.datetime.strptime(s, "%d/%b/%Y").date()
        except ValueError:
            return None


# ---------- Login ----------

def login(session: requests.Session, user: str, password: str) -> bool:
    """Return True on successful login.

    MMS returns HTTP 200 with HTML regardless of success; we detect failure by
    looking for the login form still being present in the response body.
    """
    # First GET to seed JSESSIONID.
    session.get(f"{BASE_URL}/", headers=HEADERS_BASE, timeout=30)
    resp = session.post(
        LOGIN_URL,
        data={
            "faildCount": "0",
            "name": user,
            "password": password,
            "login": "Login",
        },
        headers={
            **HEADERS_BASE,
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": f"{BASE_URL}/",
        },
        timeout=30,
        allow_redirects=True,
    )
    body = resp.text.lower()
    ok = "logout" in body or "sample submission" in body
    if not ok:
        log.warning("MMS login appears to have failed (status=%s)", resp.status_code)
    return ok


# ---------- Search ----------

def _parse_html_rows(html: str) -> list[SampleRow]:
    """Pull every data row out of the MMS sample-submission result table.

    The page has several tables (nav, filters, result, footer). We find the
    one whose first row starts with 'Sample Request Date' / 'Sample Request
    Code' and treat subsequent rows as data.
    """
    soup = BeautifulSoup(html, "html.parser")
    out: list[SampleRow] = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [
            (c.get_text(" ", strip=True) or "") for c in rows[0].find_all(["th", "td"])
        ]
        joined = " ".join(h.lower() for h in headers)
        if "sample" not in joined or "request" not in joined or "product" not in joined:
            continue
        # This is our result table.
        for tr in rows[1:]:
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
            if len(cells) < len(MMS_COLS):
                continue
            rec = dict(zip(MMS_COLS, cells[: len(MMS_COLS)]))
            # Skip rows where the first cell isn't a real date.
            if not re.match(r"\d{1,2}/[A-Za-z]{3}/\d{4}", rec["sample_request_date"]):
                continue
            out.append(
                SampleRow(
                    sample_request_date=rec["sample_request_date"],
                    sample_request_code=rec["sample_request_code"],
                    sales=rec["sales"],
                    country=rec["country"],
                    customer_code=rec["customer_code"],
                    customer_name=rec["customer_name"],
                    product_code=rec["product_code"],
                    product_name=rec["product_name"],
                    rd_price=rec["rd_price"],
                    sample_date_out=rec["sample_date_out"],
                    feedback=rec["feedback"],
                )
            )
    return out


def _fmt_date(d: dt.date) -> str:
    return d.strftime("%d/%b/%Y")


_MONTH_ABBR = [
    "jan", "feb", "mar", "apr", "may", "jun",
    "jul", "aug", "sep", "oct", "nov", "dec",
]


def _month_code(d: dt.date) -> str:
    return _MONTH_ABBR[d.month - 1]


def _script_session_id() -> str:
    first = "".join(random.choices(string.ascii_letters + string.digits, k=16))
    suffix = str(random.randint(1, 9999))
    return f"{first}/{suffix}"


def _extract_sample_dtos(dwr_reply: str) -> list[dict]:
    """Pull every SampleDto({...}) out of a DWR reply.

    The DWR format looks like:
        dwr.engine.remote.newObject("SampleDto",{key:type:value,key:value,...})
    Keys are unquoted JS identifiers; values may be strings (double-quoted),
    numbers, booleans, or null. We parse one object at a time by brace-matching
    so quoted braces / commas inside string values don't confuse us.
    """
    out: list[dict] = []
    i = 0
    marker = 'newObject("SampleDto",{'
    while True:
        start = dwr_reply.find(marker, i)
        if start == -1:
            break
        # Find matching '}' accounting for string literals and escapes.
        p = start + len(marker)
        depth = 1
        in_str = False
        esc = False
        while p < len(dwr_reply) and depth > 0:
            ch = dwr_reply[p]
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif in_str:
                if ch == '"':
                    in_str = False
            else:
                if ch == '"':
                    in_str = True
                elif ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
            p += 1
        if depth != 0:
            break
        body = dwr_reply[start + len(marker) : p - 1]
        out.append(_parse_js_object_body(body))
        i = p
    return out


def _parse_js_object_body(body: str) -> dict:
    """Parse `key1:val1,key2:val2,...` into a dict. Strings are double-quoted."""
    result: dict[str, object] = {}
    j = 0
    n = len(body)
    while j < n:
        # Skip whitespace / commas.
        while j < n and body[j] in ", \t\r\n":
            j += 1
        if j >= n:
            break
        # Read key (unquoted identifier up to ':').
        key_start = j
        while j < n and body[j] != ":":
            j += 1
        key = body[key_start:j].strip()
        if j >= n:
            break
        j += 1  # skip ':'
        # Read value.
        while j < n and body[j] in " \t":
            j += 1
        if j >= n:
            break
        if body[j] == '"':
            # Quoted string.
            j += 1
            buf: list[str] = []
            while j < n:
                ch = body[j]
                if ch == "\\" and j + 1 < n:
                    nxt = body[j + 1]
                    if nxt == "n":
                        buf.append("\n")
                    elif nxt == "t":
                        buf.append("\t")
                    elif nxt == "r":
                        buf.append("\r")
                    elif nxt == '"':
                        buf.append('"')
                    elif nxt == "\\":
                        buf.append("\\")
                    elif nxt == "/":
                        buf.append("/")
                    else:
                        buf.append(nxt)
                    j += 2
                elif ch == '"':
                    j += 1
                    break
                else:
                    buf.append(ch)
                    j += 1
            result[key] = "".join(buf)
        else:
            # Unquoted literal: null / true / false / number.
            v_start = j
            while j < n and body[j] != ",":
                j += 1
            raw = body[v_start:j].strip()
            if raw == "null":
                result[key] = None
            elif raw == "true":
                result[key] = True
            elif raw == "false":
                result[key] = False
            else:
                try:
                    result[key] = int(raw)
                except ValueError:
                    try:
                        result[key] = float(raw)
                    except ValueError:
                        result[key] = raw
    return result


def _dto_to_sample_row(d: dict) -> SampleRow:
    price = (d.get("samplePrice") or "")
    ccy = (d.get("currencyToUpcase") or "")
    rd_price = f"{ccy} {price}".strip() if ccy or price else ""
    return SampleRow(
        sample_request_date=str(d.get("sreq1ReqdateString") or ""),
        sample_request_code=str(d.get("sreqCode") or d.get("sreq1Code") or ""),
        sales=str(d.get("sreq1ReqUserName") or ""),
        country=str(d.get("sreqCustomerCountry") or ""),
        customer_code=str(d.get("sreqCustomerCode") or ""),
        customer_name=str(d.get("sreqCustomerName") or ""),
        product_code=str(d.get("productCode") or ""),
        product_name=str(d.get("productName") or ""),
        rd_price=rd_price,
        sample_date_out=str(d.get("shipdateString") or ""),
        feedback=str(d.get("feedback") or ""),
    )


def search_samples(
    session: requests.Session, date_from: dt.date, date_to: dt.date
) -> list[SampleRow]:
    """Fetch sample submissions where Sample Date Out ∈ [date_from, date_to].

    MMS filters by month+year granularity. We submit the Find form to lock in
    the search on the server side, then call the DWR RPC that the HTML list
    view uses to stream back every matching SampleDto. Result is finally
    filtered client-side to the exact day range.
    """
    common = {
        "country": "", "code": "", "requestFrom": "", "nextActionBy": "",
        "fromMonth": "", "fromYear": "", "toMonth": "", "toYear": "",
        "outFromMonth": _month_code(date_from),
        "outFromYear": str(date_from.year),
        "outToMonth": _month_code(date_to),
        "outToYear": str(date_to.year),
        "customer_id": "", "customer_name": "",
    }

    # Step 1: POST command=find to run the search (server stashes result state).
    session.post(
        SEARCH_URL,
        data={**common, "command": "find"},
        headers={**HEADERS_BASE, "Referer": SEARCH_URL,
                 "Content-Type": "application/x-www-form-urlencoded"},
        timeout=60,
    )

    # Step 2: DWR RPC loadItemList — returns every matching SampleDto inline.
    ssid = _script_session_id()
    dwr_body = "\n".join([
        "callCount=1",
        "windowName=",
        "page=/mms3/master/sampleSubmissionSearch.do",
        "httpSessionId=",
        f"scriptSessionId={ssid}",
        "c0-scriptName=sampleDwr",
        "c0-methodName=loadItemList",
        "c0-id=0",
        "c0-param0=Object_SampleSubmissionSearchDto:{"
        "country:string:,"
        "requestFrom:string:,"
        "nextActionBy:string:,"
        "fromMonth:string:,"
        "fromYear:string:,"
        "toMonth:string:,"
        "toYear:string:,"
        f"outFromMonth:string:{_month_code(date_from)},"
        f"outFromYear:string:{date_from.year},"
        f"outToMonth:string:{_month_code(date_to)},"
        f"outToYear:string:{date_to.year},"
        "customerId:string:,"
        "prodCode:string:,"
        "code:string:,"
        "tmpCustomerName:string:,"
        "prodName:string:,"
        "noship:string:false"
        "}",
        "batchId=0",
    ]) + "\n"
    resp = session.post(
        DWR_URL,
        data=dwr_body,
        headers={
            **HEADERS_BASE,
            "Content-Type": "text/plain",
            "Referer": SEARCH_URL,
        },
        timeout=180,
    )
    resp.raise_for_status()

    dtos = _extract_sample_dtos(resp.text)
    rows = [_dto_to_sample_row(d) for d in dtos]

    # Client-side day-precision filter.
    filtered: list[SampleRow] = []
    for r in rows:
        d = r.sample_date_out_as_date()
        if d is None:
            filtered.append(r)
            continue
        if date_from <= d <= date_to:
            filtered.append(r)

    log.info(
        "MMS: fetched %d DTOs (%d after date filter) for %s..%s",
        len(rows), len(filtered),
        _fmt_date(date_from), _fmt_date(date_to),
    )
    return filtered


def monthly_chunks(
    start: dt.date, end: dt.date, months_per_chunk: int = 3
) -> Iterator[tuple[dt.date, dt.date]]:
    """Yield (chunk_from, chunk_to) inclusive windows covering [start, end]."""
    cur = start
    while cur <= end:
        # month-arithmetic: go months_per_chunk months forward minus one day.
        y, m = cur.year, cur.month + months_per_chunk
        while m > 12:
            y += 1
            m -= 12
        try:
            chunk_end = dt.date(y, m, 1) - dt.timedelta(days=1)
        except ValueError:
            chunk_end = end
        if chunk_end > end:
            chunk_end = end
        yield cur, chunk_end
        cur = chunk_end + dt.timedelta(days=1)


def fetch_all_samples(
    session: requests.Session, date_from: dt.date, date_to: dt.date
) -> list[SampleRow]:
    """Chunked fetch to stay under the 1000-row MMS cap."""
    out: list[SampleRow] = []
    seen: set[tuple[str, str]] = set()  # (sample_request_code, product_code)
    for a, b in monthly_chunks(date_from, date_to, months_per_chunk=3):
        chunk = search_samples(session, a, b)
        if len(chunk) >= 995:
            log.warning(
                "Chunk %s..%s returned %d rows (near 1000 cap) — consider "
                "reducing months_per_chunk.",
                _fmt_date(a), _fmt_date(b), len(chunk),
            )
        for r in chunk:
            key = (r.sample_request_code, r.product_code)
            if key in seen:
                continue
            seen.add(key)
            out.append(r)
    return out
