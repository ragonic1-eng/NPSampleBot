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
    quantity_g: str = ""

    def sample_date_out_as_date(self) -> dt.date | None:
        s = (self.sample_date_out or "").strip()
        try:
            return dt.datetime.strptime(s, "%d/%b/%Y").date()
        except ValueError:
            return None

    def sample_request_date_as_date(self) -> dt.date | None:
        """v0.28.35 (2026-05-20) — Parse the REQUEST date (when the
        sample was submitted in MMS, vs sample_date_out which is when
        it was physically dispatched). Used by the new request-date
        sync path so the Sample Master Google Sheet can also surface
        REQUESTED but not yet dispatched samples."""
        s = (self.sample_request_date or "").strip()
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
    seed = session.get(f"{BASE_URL}/", headers=HEADERS_BASE, timeout=30)

    # V1.17.13 — post to the form's OWN action, not a hardcoded URL.
    #
    # MMS (a Java app) does URL-rewritten session tracking: the login form's
    # action carries the session inline, e.g.
    #     /mms3/login.do;jsessionid=5ADA1157BBD079D387C1D22F14E28D18
    # Posting to a bare /mms3/login.do submits credentials with no session
    # attached, so the server answers "Login Failed!!" even when the password
    # is perfectly correct — which is exactly what happened from 17 Jul 2026:
    # a human could log in fine (the browser follows the form action) while
    # the bot could not, and the stored credential matched the working one
    # byte-for-byte. Scraping the action makes us follow whatever session
    # scheme the server is using today, cookie- or URL-based.
    post_url = LOGIN_URL
    try:
        seed.encoding = "utf-8"
        m = re.search(
            r"<form[^>]+action\s*=\s*[\"']([^\"']*login\.do[^\"']*)", seed.text, re.I
        )
        if m:
            from urllib.parse import urljoin
            post_url = urljoin(f"{BASE_URL}/", m.group(1))
    except Exception as e:  # noqa: BLE001 — fall back to the static URL
        log.debug("MMS login: could not read form action (%s); using %s", e, LOGIN_URL)

    resp = session.post(
        post_url,
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
    # Audit V1.13.14 — MMS responses are UTF-8 but server occasionally
    # omits charset, in which case requests guesses ISO-8859-1 and we
    # get mojibake on Chinese product names downstream. Force UTF-8 on
    # every MMS response before any .text/HTML parsing.
    resp.encoding = "utf-8"
    body = resp.text.lower()
    ok = "logout" in body or "sample submission" in body
    if not ok:
        # V1.17.14 — MMS's user id is CASE-SENSITIVE. "Alex" vs "alex" was the
        # entire cause of the 17-21 Jul 2026 blackout: a valid password, a
        # correctly deployed config, and four days of zero scraping. One
        # lowercase retry costs a single request and removes that whole class
        # of outage. Only retried when the id isn't already lowercase, so we
        # never double-count a genuine bad-credential failure (MMS increments
        # a server-side faildCount that can lock the account).
        if user != user.lower():
            log.warning(
                "MMS login failed as %r — retrying lowercase (MMS is "
                "case-sensitive)", user,
            )
            return login(session, user.lower(), password)
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
                        j += 2
                    elif nxt == "t":
                        buf.append("\t")
                        j += 2
                    elif nxt == "r":
                        buf.append("\r")
                        j += 2
                    elif nxt == '"':
                        buf.append('"')
                        j += 2
                    elif nxt == "\\":
                        buf.append("\\")
                        j += 2
                    elif nxt == "/":
                        buf.append("/")
                        j += 2
                    elif nxt == "u" and j + 5 < n:
                        # Audit V1.13.14 — \uXXXX (4-hex-digit Unicode
                        # escape). DWR uses these for non-ASCII chars
                        # (Chinese product names). Previously fell into
                        # the catch-all else branch and we kept just
                        # "u" + the 4 hex chars verbatim, producing the
                        # "u9C9C" mojibake all over the sheet.
                        hex_str = body[j + 2:j + 6]
                        try:
                            buf.append(chr(int(hex_str, 16)))
                            j += 6
                        except ValueError:
                            # Not a valid hex sequence — fall back to
                            # the literal char so we don't lose data.
                            buf.append(nxt)
                            j += 2
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
        quantity_g=str(d.get("quantity") or ""),
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
    resp.encoding = "utf-8"  # see comment in `login()` above

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


def search_samples_by_request_date(
    session: requests.Session, date_from: dt.date, date_to: dt.date
) -> list[SampleRow]:
    """v0.28.35 (2026-05-20) — Variant of search_samples() that filters
    by REQUEST DATE instead of Sample Date Out, AND includes unshipped
    samples (noship=true). This catches every MMS submission regardless
    of dispatch status — pending, in-queue, cancelled, dispatched, the
    lot.

    USER 2026-05-20 reported: 'I checked mms Sample Submission List
    from Feb to May Ying has requested 82 request' but the bot's sheet
    showed only 8. Cause: the original search_samples() filters by
    Sample Date Out + noship=false, which only catches DISPATCHED
    samples. This new function captures the full REQUEST list.

    Writes to a SEPARATE set of tabs in the Google Sheet so it never
    pollutes the existing dispatched-only data that the analyst / YoY
    chart / weekly action list rely on. See sync_engine.py's
    run_mms_to_submissions_sync() entry point.

    Same DWR-RPC shape as search_samples(); only the date-field names
    and the noship flag differ.
    """
    common = {
        "country": "", "code": "", "requestFrom": "", "nextActionBy": "",
        # Filter by REQUEST date (was empty in dispatched-only path)
        "fromMonth": _month_code(date_from),
        "fromYear": str(date_from.year),
        "toMonth": _month_code(date_to),
        "toYear": str(date_to.year),
        # Leave OUT date params empty so MMS doesn't double-filter
        "outFromMonth": "", "outFromYear": "",
        "outToMonth": "", "outToYear": "",
        "customer_id": "", "customer_name": "",
    }

    # Step 1: POST command=find to run the search.
    session.post(
        SEARCH_URL,
        data={**common, "command": "find"},
        headers={**HEADERS_BASE, "Referer": SEARCH_URL,
                 "Content-Type": "application/x-www-form-urlencoded"},
        timeout=60,
    )

    # Step 2: DWR RPC loadItemList. MMS ver 6.06.151 (2026-07-06): noship=true
    # now returns ONLY-unshipped (it used to mean include-unshipped). false
    # returns everything -- shipped AND pending -- which is what we want.
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
        f"fromMonth:string:{_month_code(date_from)},"
        f"fromYear:string:{date_from.year},"
        f"toMonth:string:{_month_code(date_to)},"
        f"toYear:string:{date_to.year},"
        "outFromMonth:string:,"
        "outFromYear:string:,"
        "outToMonth:string:,"
        "outToYear:string:,"
        "customerId:string:,"
        "prodCode:string:,"
        "code:string:,"
        "tmpCustomerName:string:,"
        "prodName:string:,"
        "noship:string:false"  # ver 6.06.151: false = everything (true = only unshipped)
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
    resp.encoding = "utf-8"

    dtos = _extract_sample_dtos(resp.text)
    rows = [_dto_to_sample_row(d) for d in dtos]

    # Client-side day-precision filter, by REQUEST date this time.
    # Rows with no request date are kept (better to over-include than
    # silently drop — calling sync writes them with empty date and
    # the analyst can decide).
    filtered: list[SampleRow] = []
    for r in rows:
        d = r.sample_request_date_as_date()
        if d is None:
            filtered.append(r)
            continue
        if date_from <= d <= date_to:
            filtered.append(r)

    log.info(
        "MMS (request-date): fetched %d DTOs (%d after date filter) for %s..%s",
        len(rows), len(filtered),
        _fmt_date(date_from), _fmt_date(date_to),
    )
    return filtered


def fetch_all_samples_by_request_date(
    session: requests.Session, date_from: dt.date, date_to: dt.date
) -> list[SampleRow]:
    """Chunked variant of fetch_all_samples() that uses request-date
    filtering. Same per-chunk retry + dedup + per-chunk-fatal-soft
    error handling as the dispatched-date path."""
    out: list[SampleRow] = []
    seen: set[tuple[str, str]] = set()
    failed_chunks: list[tuple[dt.date, dt.date, str]] = []
    for a, b in monthly_chunks(date_from, date_to, months_per_chunk=3):
        chunk: list[SampleRow] | None = None
        last_err: Exception | None = None
        for attempt in range(2):
            try:
                chunk = search_samples_by_request_date(session, a, b)
                break
            except Exception as e:  # noqa: BLE001
                last_err = e
                if attempt == 0:
                    log.warning(
                        "Request-date chunk %s..%s attempt 1 failed: %s — retrying once",
                        _fmt_date(a), _fmt_date(b), e,
                    )
                    continue
        if chunk is None:
            log.error(
                "Request-date chunk %s..%s permanently failed: %s",
                _fmt_date(a), _fmt_date(b), last_err,
            )
            failed_chunks.append((a, b, str(last_err)))
            continue
        if len(chunk) >= 995:
            log.warning(
                "Request-date chunk %s..%s returned %d rows (near 1000 cap) — "
                "reduce months_per_chunk if this keeps happening.",
                _fmt_date(a), _fmt_date(b), len(chunk),
            )
        for r in chunk:
            key = (r.sample_request_code, r.product_code)
            if key in seen:
                continue
            seen.add(key)
            out.append(r)
    if failed_chunks:
        log.warning(
            "fetch_all_samples_by_request_date: %d chunks failed — recovered %d rows",
            len(failed_chunks), len(out),
        )
    return out


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
    """Chunked fetch to stay under the 1000-row MMS cap.

    Audit fix #9 — chunk failures are now per-chunk fatal-soft: a single
    bad chunk (502, timeout, malformed reply) is logged but does NOT
    poison the whole run. Earlier behaviour propagated the exception
    out of search_samples and threw away every row from prior chunks
    that had already succeeded. With a per-chunk try/except, we
    preserve good chunks and report the failure count so the caller
    can decide whether to retry (most callers run again the next day
    anyway, and the dedupe step covers the chunk that gets re-fetched).

    A chunk is retried ONCE before giving up — handles the case where
    the MMS session expired mid-loop. We re-acquire any state by
    re-issuing the search (search_samples is stateless from the
    caller's perspective).
    """
    out: list[SampleRow] = []
    seen: set[tuple[str, str]] = set()  # (sample_request_code, product_code)
    failed_chunks: list[tuple[dt.date, dt.date, str]] = []
    for a, b in monthly_chunks(date_from, date_to, months_per_chunk=3):
        chunk: list[SampleRow] | None = None
        last_err: Exception | None = None
        for attempt in range(2):
            try:
                chunk = search_samples(session, a, b)
                break
            except Exception as e:  # noqa: BLE001
                last_err = e
                if attempt == 0:
                    log.warning(
                        "Chunk %s..%s attempt 1 failed: %s — retrying once",
                        _fmt_date(a), _fmt_date(b), e,
                    )
                    continue
        if chunk is None:
            log.error(
                "Chunk %s..%s permanently failed after retry: %s — "
                "preserving %d rows from earlier chunks and continuing",
                _fmt_date(a), _fmt_date(b), last_err, len(out),
            )
            failed_chunks.append((a, b, str(last_err)))
            continue
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
    if failed_chunks:
        log.warning(
            "fetch_all_samples: %d/%d chunks failed — recovered %d rows "
            "from surviving chunks. Failed windows: %s",
            len(failed_chunks),
            sum(1 for _ in monthly_chunks(date_from, date_to, months_per_chunk=3)),
            len(out),
            [(f"{_fmt_date(a)}..{_fmt_date(b)}", err[:80]) for a, b, err in failed_chunks],
        )
    return out
