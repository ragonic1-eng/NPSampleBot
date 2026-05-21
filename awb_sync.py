"""AWB sync — fill the AWB column on each FSL tab from DHL + FedEx shipments.

Pipeline (called twice daily by the bot's JobQueue at 7am and 8pm SGT, plus
on-demand via /syncawb):

  1. Fetch the last N days of shipments from DHL Express (awb_dhl.py) and
     FedEx (awb_fedex.py) in parallel.
  2. For each FSL tab (Singapore / Jakarta / Thailand), read all rows.
  3. Match shipments to FSL rows by (customer name fuzzy-match, sample
     date within ±2 days). When several FSL rows match one shipment —
     typical case, one DHL box contains multiple sample bags — every
     matching row gets the same AWB.
  4. Skip rows that already have an AWB (re-runs don't overwrite).
  5. Write the matched AWBs into col K of each tab.

The DHL + FedEx fetchers are intentionally pluggable so we can later swap
the Playwright-based scraping for the official APIs without changing this
file. Each fetcher returns a list[Shipment]; this module owns the matcher
and the sheet writes.
"""
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

from rapidfuzz import fuzz

import sheets


log = logging.getLogger("npsamplebot.awb_sync")


# Customer-name fuzzy match threshold. Higher = stricter. 85 picks up
# things like "HURNG FUR FOODS FACTORY CO LTD" vs "HURNG FUR FOODS
# FACTORY CO., LTD" but rejects unrelated names. Tuned empirically.
NAME_FUZZ_THRESHOLD = 85

# How many days of slack between courier ship date and FSL "Sample Date
# Out" we'll still consider a match. Reps usually put the sample in the
# courier the same day, but a courier picking up an hour after midnight
# would still be the same shipment in everyone's mental model.
DATE_SLACK_DAYS = 2


# --------------------------- data types ---------------------------

@dataclass(frozen=True)
class Shipment:
    """One outbound shipment as we get it from a carrier portal/API."""
    carrier: str          # "DHL" or "FedEx"
    awb: str              # the airwaybill / tracking number
    recipient_name: str   # customer/company name as printed on the label
    recipient_country: str  # ISO country name (best-effort, may be "")
    ship_date: date       # the day the carrier picked the box up
    # Multi-line mailing address from the carrier label. DHL extracts
    # this from the Ship-To block (contact + street + city). FedEx
    # leaves it empty — the FedEx grid view doesn't expose a full
    # address and drilling into each shipment's detail page would 10x
    # the scrape time without much payoff for FSL coverage.
    ship_to_address: str = ""


@dataclass
class MatchUpdate:
    """One AWB destined for one FSL row."""
    tab: str
    row_number: int  # 1-indexed sheet row
    awb: str
    carrier: str
    customer: str
    fsl_date_iso: str
    sales: str = ""   # filled from FSL row — used by the chat update post
    country: str = ""  # filled from FSL row — used by the chat update post
    address: str = ""  # carrier ship-to address, written to FSL col L


@dataclass
class SyncResult:
    """Returned to callers (the scheduled job + /syncawb)."""
    dhl_count: int = 0
    fedex_count: int = 0
    by_tab: dict[str, dict[str, int]] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    dry_run: bool = False
    # Carrier-matched updates that were written this run. Used to build
    # the post-sync chat message. Stays empty on dry_run.
    applied_updates: list[MatchUpdate] = field(default_factory=list)
    # SG-local rows we defaulted to HAND CARRY this run. Tracked
    # separately so the chat message isn't drowned in default writes —
    # they're useful as a tally line, not as individual blocks.
    hand_carry_defaults: int = 0
    # Carrier shipments that didn't semantically match ANY FSL row on
    # ANY tab (customer + date). These are the AWBs that need human
    # attention — either the sample was never logged in MMS/FSL or
    # the customer name needs an alias. The 18:00 digest reads this
    # and lists them in an 'AWBs not in FSL' footer section.
    unmatched_shipments: list[Shipment] = field(default_factory=list)

    @property
    def total_matched(self) -> int:
        return sum(t["matched"] for t in self.by_tab.values())

    @property
    def total_written(self) -> int:
        return sum(t["written"] for t in self.by_tab.values())


# --------------------------- normalisation helpers ---------------------------

_COMPANY_NOISE_RE = re.compile(
    # Common company-form suffixes that vary between carrier labels and
    # the FSL customer master. Stripped before fuzzy-matching. Kept
    # conservative — words like 'trading', 'foods', 'industries' are
    # NOT stripped because they're often part of the brand name proper
    # ('PT NP Foods' vs 'PT Foods' would otherwise wrongly collapse).
    r"\b(co\.?,?\s*ltd\.?|pte\.?\s*ltd\.?|sdn\.?\s*bhd\.?|inc\.?|llc|"
    r"corp(?:oration)?|limited|company|factory|gmbh|pvt|private)\b",
    re.IGNORECASE,
)

def _normalize_name(name: str) -> str:
    """Aggressive normalisation so the fuzzy matcher can compare apples
    to apples between carrier labels and the FSL customer master.

    Pipeline:
      1. Strip parens CHARACTERS but keep what's inside
         ('Acme (HK)' → 'Acme HK', '(Food Excellence )' → 'Food Excellence ')
      2. Lowercase
      3. Replace '&' with ' and '              (PG&E ↔ PG and E)
      4. Replace dots, commas, dashes          ('CO., LTD' → 'CO LTD')
      5. Strip company-form suffixes           ('Sdn Bhd', 'Pte Ltd', …)
      6. Collapse all non-alphanum to spaces, collapse whitespace
    """
    if not name:
        return ""
    # Strip parens characters only — keep inner content. Earlier
    # iteration deleted everything inside parens, which broke FSL
    # names that are ENTIRELY parenthesised like '(Food Excellence )'.
    s = name.replace("(", " ").replace(")", " ")
    s = s.lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[.,\-]+", " ", s)
    s = _COMPANY_NOISE_RE.sub(" ", s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = " ".join(s.split())
    return s


def _customer_matches(carrier_name: str, fsl_name: str) -> bool:
    """Multi-strategy fuzzy compare.

    Runs both token_set_ratio AND token_sort_ratio and takes the max:
      • token_set_ratio: tolerant of extra/missing tokens — wins when
        the carrier label has 'SPECIALIST' / 'GENERAL TRADING' tokens
        that aren't in the FSL name
      • token_sort_ratio: tolerant of reordering — wins when 'AL
        WAZZAN JASSIM SONS' label = 'JASSIM AL WAZZAN SONS' in FSL

    Deliberately NOT using partial_ratio: it returns 100 whenever the
    shorter string is a substring of the longer, which produces too
    many false positives.

    Limitation accepted: when the FSL legitimately contains two
    very-similar customer names (e.g. 'PT FOODS' and 'PT NP FOODS'),
    a carrier shipment to one may also match the other by fuzzy
    score alone. The disambiguation is the date-window filter (same-
    customer-same-day is the realistic ambiguity surface) plus the
    'AWB Customer Aliases' override sheet for hard cases.
    """
    a = _normalize_name(carrier_name)
    b = _normalize_name(fsl_name)
    if not a or not b:
        return False
    return max(
        fuzz.token_set_ratio(a, b),
        fuzz.token_sort_ratio(a, b),
    ) >= NAME_FUZZ_THRESHOLD


def _apply_alias(s: Shipment, aliases_normalized: dict[str, str]) -> Shipment:
    """Replace s.recipient_name with the FSL-side alias if one exists.

    Lookup is by normalised carrier name (lowercase, no company-form
    suffixes) so the rep doesn't have to worry about capitalisation or
    'CO LTD' / 'CO., LTD' variants in the alias sheet.
    """
    if not aliases_normalized:
        return s
    key = _normalize_name(s.recipient_name)
    if key in aliases_normalized:
        replacement = aliases_normalized[key]
        log.info(
            "alias: '%s' → '%s' (awb=%s)",
            s.recipient_name, replacement, s.awb,
        )
        return Shipment(
            carrier=s.carrier,
            awb=s.awb,
            recipient_name=replacement,
            recipient_country=s.recipient_country,
            ship_date=s.ship_date,
            ship_to_address=s.ship_to_address,
        )
    return s


def _date_matches(carrier_date: date, fsl_date: date | None) -> bool:
    if fsl_date is None:
        return False
    return abs((carrier_date - fsl_date).days) <= DATE_SLACK_DAYS


# --------------------------- matcher ---------------------------

def build_updates_for_tab(
    tab: str,
    fsl_rows: list[dict],
    shipments: Iterable[Shipment],
    semantic_match_awbs: set | None = None,
) -> list[MatchUpdate]:
    """Match each shipment against every FSL row on `tab`.

    Returns one MatchUpdate per FSL row that should receive an AWB.
    Rows whose AWB cell is already filled are SKIPPED for the WRITE
    (so a re-run doesn't overwrite manually-typed values). When
    multiple FSL rows match one shipment, each row gets the same AWB.

    If `semantic_match_awbs` is provided, it's populated with the AWBs
    of shipments that had ≥1 semantic (date+customer) match on this
    tab, REGARDLESS of whether the matching row's AWB cell was already
    filled. The caller uses this to identify 'truly unmatched'
    shipments — AWBs we have but couldn't link anywhere — for the
    digest's 'AWBs not in FSL' section.
    """
    out: list[MatchUpdate] = []
    seen_rows: set[int] = set()  # avoid double-updating the same FSL row

    for s in shipments:
        for fsl in fsl_rows:
            row_num = fsl.get("_row")
            if not row_num or row_num in seen_rows:
                continue
            # Semantic check BEFORE the skip-if-non-empty filter so we
            # can track 'this shipment matched somewhere' independently
            # of whether the cell write was blocked.
            if not _date_matches(s.ship_date, fsl.get("_date")):
                continue
            if not _customer_matches(s.recipient_name, fsl.get("Customer Name", "")):
                continue
            if semantic_match_awbs is not None:
                semantic_match_awbs.add(s.awb)
            # Decide what's writable on this row. AWB is one-shot (we
            # never overwrite an existing AWB — could be manually typed
            # or set by an earlier run). Customer Address is opportunistic:
            # we fill it if empty even when AWB is already present, so
            # historical rows that pre-date the address feature still
            # benefit from later DHL ship-to scrapes.
            awb_empty = not (fsl.get("AWB") or "").strip()
            addr_empty = not (fsl.get("Customer Address") or "").strip()
            if not awb_empty and not addr_empty:
                # Both cells already populated — nothing for us to do.
                continue
            if not awb_empty and addr_empty and not s.ship_to_address:
                # AWB already set, no new address to contribute — skip.
                continue
            out.append(MatchUpdate(
                tab=tab,
                row_number=row_num,
                # Empty AWB on the update tells write_awb_updates to leave
                # the K cell alone. Address still gets written.
                awb=s.awb if awb_empty else "",
                carrier=s.carrier,
                customer=fsl.get("Customer Name", ""),
                fsl_date_iso=fsl.get("Sample Date Out", ""),
                sales=fsl.get("Sales", ""),
                country=fsl.get("Country", ""),
                address=s.ship_to_address if addr_empty else "",
            ))
            seen_rows.add(row_num)
    return out


# --------------------------- orchestrator ---------------------------

def build_hand_carry_sg_updates(
    tab: str, fsl_rows: list[dict]
) -> list[MatchUpdate]:
    """Default 'HAND CARRY' for SG-country rows on the main FSL tab.

    Rationale: NP Foods Singapore's main FSL ('Full Sample Listing')
    contains SG-produced samples shipped to customers. Rows whose
    Country == 'Singapore' are local Singapore deliveries — couriered
    by hand or by local pickup, never via DHL/FedEx — so they should
    default to 'HAND CARRY' without waiting for a courier match that
    will never come.

    Only applied to FSL_TAB (the Singapore production tab). On
    Jakarta / Thailand tabs, Country='Singapore' means 'shipped TO
    Singapore from that factory' — which IS couriered, so we leave
    those alone for the carrier scrape to fill.

    Skip rows whose AWB cell already has SOMETHING (a real AWB, a
    manually-typed override, etc.) — the same skip-if-non-empty
    rule the matcher uses.
    """
    if tab != sheets.FSL_TAB:
        return []
    out: list[MatchUpdate] = []
    for fsl in fsl_rows:
        row_num = fsl.get("_row")
        if not row_num:
            continue
        if (fsl.get("AWB") or "").strip():
            continue
        country = (fsl.get("Country") or "").strip().lower()
        if country != "singapore":
            continue
        out.append(MatchUpdate(
            tab=tab,
            row_number=row_num,
            awb="HAND CARRY",
            # Marker that excludes the row from the chat update post.
            carrier="(local SG default)",
            customer=fsl.get("Customer Name", ""),
            fsl_date_iso=fsl.get("Sample Date Out", ""),
            sales=fsl.get("Sales", ""),
            country="Singapore",
        ))
    return out


async def run_awb_sync(
    *,
    days_back: int = 14,
    dry_run: bool = False,
) -> SyncResult:
    """Twice-daily entrypoint. Fetches shipments, matches, writes.

    `dry_run=True` skips the sheet writes — useful for /syncawb's
    preview mode and for tests.
    """
    # Lazy imports so the bot can import awb_sync even when Playwright /
    # the carrier modules are missing (e.g. local dev without Chromium).
    # In that case the fetchers will raise ImportError and we record it
    # in the result without crashing the scheduled job.
    result = SyncResult(dry_run=dry_run)
    shipments: list[Shipment] = []

    async def _safe_fetch(name: str, coro):
        try:
            got = await coro
        except Exception as e:  # noqa: BLE001
            log.warning("%s fetch failed: %s", name, e)
            result.errors.append(f"{name}: {type(e).__name__}: {e}")
            return []
        log.info("%s returned %d shipment(s)", name, len(got))
        return got

    # Run both carriers in parallel — they're independent network calls.
    try:
        import awb_dhl  # noqa: WPS433
        dhl_coro = awb_dhl.fetch_recent_shipments(days_back=days_back)
    except ImportError as e:
        dhl_coro = _empty_coro()
        result.errors.append(f"awb_dhl unavailable: {e}")
    try:
        import awb_fedex  # noqa: WPS433
        fedex_coro = awb_fedex.fetch_recent_shipments(days_back=days_back)
    except ImportError as e:
        fedex_coro = _empty_coro()
        result.errors.append(f"awb_fedex unavailable: {e}")

    dhl_list, fedex_list = await asyncio.gather(
        _safe_fetch("DHL", dhl_coro),
        _safe_fetch("FedEx", fedex_coro),
    )
    result.dhl_count = len(dhl_list)
    result.fedex_count = len(fedex_list)
    shipments = list(dhl_list) + list(fedex_list)

    # Apply customer-name aliases BEFORE matching. Carriers often print
    # the distributor / importer's name on the label while the FSL
    # records the end-customer brand (e.g. 'SARL HYGIENIX MANUFACTURE
    # COMPANY' on DHL → 'Daiya Food' in the FSL). Reps maintain the
    # mapping in the OPS sheet's 'AWB Customer Aliases' tab; we apply
    # it here so the matcher sees the FSL-side name and the fuzzy
    # compare actually has a chance to land. Lookup is normalized so
    # capitalisation differences don't matter.
    try:
        raw_aliases = await asyncio.to_thread(sheets.load_customer_aliases)
    except Exception as e:  # noqa: BLE001
        log.warning("Could not load customer aliases: %s", e)
        raw_aliases = {}
    aliases_normalized = {
        _normalize_name(k): v for k, v in raw_aliases.items() if k and v
    }
    if aliases_normalized:
        log.info("AWB sync: applying %d customer alias(es)", len(aliases_normalized))
    shipments = [_apply_alias(s, aliases_normalized) for s in shipments]

    # Track shipments that had a semantic (customer+date) match on
    # ANY tab, even if the write was blocked by an existing AWB cell.
    # Used to compute result.unmatched_shipments at the end — the list
    # the 18:00 digest surfaces so no AWB stays silently orphaned.
    semantic_matched_awbs: set[str] = set()

    # For each region tab, load → match → write.
    tabs = [sheets.FSL_TAB, sheets.JAKARTA_FSL_TAB, sheets.BANGKOK_FSL_TAB]
    for tab in tabs:
        try:
            await asyncio.to_thread(sheets.ensure_awb_column, tab)
            fsl_rows = await asyncio.to_thread(
                sheets.load_fsl_rows_with_row_numbers, tab,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("read %s failed: %s", tab, e)
            result.errors.append(f"{tab} read: {e}")
            result.by_tab[tab] = {"matched": 0, "written": 0}
            continue

        carrier_updates = build_updates_for_tab(
            tab, fsl_rows, shipments,
            semantic_match_awbs=semantic_matched_awbs,
        )
        hc_updates = build_hand_carry_sg_updates(tab, fsl_rows)
        # De-dupe: carrier match wins if a row is queued by both paths
        # (this shouldn't happen — SG rows almost never appear on
        # DHL/FedEx — but be defensive).
        used_rows = {u.row_number for u in carrier_updates}
        hc_updates = [u for u in hc_updates if u.row_number not in used_rows]
        all_updates = carrier_updates + hc_updates
        log.info(
            "%s: %d FSL row(s), %d carrier match(es), %d HAND CARRY default(s)",
            tab, len(fsl_rows), len(carrier_updates), len(hc_updates),
        )

        written = 0
        if all_updates and not dry_run:
            try:
                written = await asyncio.to_thread(
                    sheets.write_awb_updates,
                    tab,
                    [(u.row_number, u.awb, u.address) for u in all_updates],
                )
                # write_awb_updates is batch + atomic — either every
                # cell wrote or none did. When written > 0 the whole
                # all_updates list got persisted. Split into the two
                # buckets so the chat message only shows carrier hits.
                if written:
                    result.applied_updates.extend(carrier_updates)
                    result.hand_carry_defaults += len(hc_updates)
            except Exception as e:  # noqa: BLE001
                log.warning("write %s failed: %s", tab, e)
                result.errors.append(f"{tab} write: {e}")
        result.by_tab[tab] = {
            "matched": len(carrier_updates),
            "hand_carry": len(hc_updates),
            "written": written,
        }

    # Compute truly-unmatched shipments — carrier records whose
    # customer+date didn't link to ANY FSL row (filled or not). Saved
    # on the result so the 18:00 digest's footer can surface them and
    # also cached at module level so the digest in another async
    # context (build_daily_digest_body) can read the latest run.
    result.unmatched_shipments = [
        s for s in shipments if s.awb not in semantic_matched_awbs
    ]
    if result.unmatched_shipments:
        log.info(
            "AWB sync: %d shipment(s) had no FSL match — surfacing in digest:",
            len(result.unmatched_shipments),
        )
        for s in result.unmatched_shipments:
            log.info(
                "  unmatched · %s · %s · ship_date=%s · recipient=%r",
                s.carrier, s.awb, s.ship_date, s.recipient_name,
            )
    global _LAST_RESULT
    _LAST_RESULT = result

    return result


# Module-level cache of the most recent SyncResult. Read by the
# 18:00 digest job so it can append unmatched-AWB info without
# having to re-fetch from DHL+FedEx itself. Survives between async
# contexts in the same Python process; resets on bot restart.
_LAST_RESULT: SyncResult | None = None


def get_last_unmatched_shipments() -> list[Shipment]:
    """Return shipments from the most recent sync that didn't land on
    any FSL row (i.e. no customer+date semantic match anywhere).
    Empty list if no sync has run yet or all shipments matched."""
    if _LAST_RESULT is None:
        return []
    return list(_LAST_RESULT.unmatched_shipments)


async def _empty_coro() -> list[Shipment]:
    return []


def format_update_message(result: SyncResult) -> str | None:
    """Build the 'AWB Update' message we post to the group chat.

    Used by the scheduled AWB sync job to announce newly-filled AWBs
    after the run completes. Returns None when there's nothing to
    announce (no writes happened) — the caller skips the post in
    that case so we don't spam the chat with empty updates.

    Format (one block per customer-shipment, grouped by rep):

      📦 <b>AWB Update</b> — 2026-05-20 22:00 SGT
      <i>5 new AWB(s) just filled in from DHL + FedEx</i>

      👤 <b>Adrian</b>
         • Daiya Food (Middle East) — 8 samples · 19 May
           AWB Number: <code>871963570184</code>

      👤 <b>Jay</b>
         • Acme Brand (Vietnam) — 3 samples · 18 May
           AWB Number: <code>872017312410</code>
    """
    if not result.applied_updates:
        return None

    # Group by (sales, customer, fsl_date_iso, awb). One shipment block
    # collapses N FSL rows that share all four fields — typical when a
    # DHL box contained multiple sample bags.
    groups: dict[tuple[str, str, str, str], dict] = {}
    for u in result.applied_updates:
        key = (
            u.sales or "—",
            u.customer or "—",
            u.fsl_date_iso or "",
            u.awb,
        )
        g = groups.setdefault(key, {
            "sales": u.sales or "—",
            "customer": u.customer or "—",
            "country": u.country or "",
            "fsl_date_iso": u.fsl_date_iso or "",
            "awb": u.awb,
            "count": 0,
        })
        g["count"] += 1

    # Order: sales A→Z, then most-recent date first within each rep.
    grouped = list(groups.values())
    grouped.sort(key=lambda g: (g["sales"].lower(), -_date_sort_key(g["fsl_date_iso"])))

    sgt_now = (datetime.now(timezone.utc) + timedelta(hours=8)).strftime(
        "%Y-%m-%d %H:%M SGT"
    )
    lines = [
        f"📦 <b>AWB Update</b> — {sgt_now}",
        f"<i>{len(grouped)} new AWB(s) just filled in from DHL + FedEx</i>",
    ]
    current_sales = None
    for g in grouped:
        if g["sales"] != current_sales:
            current_sales = g["sales"]
            lines.append("")
            lines.append(f"👤 <b>{_h(current_sales)}</b>")
        cust = g["customer"]
        country_tail = f" ({_h(g['country'])})" if g["country"] else ""
        count = g["count"]
        date_tail = ""
        if g["fsl_date_iso"]:
            try:
                d = datetime.strptime(g["fsl_date_iso"], "%Y-%m-%d").date()
                date_tail = " · " + d.strftime("%d %b")
            except ValueError:
                date_tail = " · " + g["fsl_date_iso"]
        sample_word = "samples" if count != 1 else "sample"
        lines.append(
            f"   • {_h(cust)}{country_tail} — {count} {sample_word}{date_tail}"
        )
        lines.append(f"     AWB Number: <code>{_h(g['awb'])}</code>")
    return "\n".join(lines)


def _date_sort_key(iso: str) -> int:
    """Best-effort numeric key for descending date sort. Bad dates → 0."""
    try:
        return int(iso.replace("-", ""))
    except ValueError:
        return 0


def _h(s) -> str:
    """Minimal HTML escape — used by format_update_message."""
    import html as _html_lib
    return _html_lib.escape(str(s or ""), quote=False)


def format_result_for_telegram(result: SyncResult) -> str:
    """Human-readable summary for /syncawb output + admin notifications."""
    lines = ["<b>📦 AWB sync result</b>"]
    if result.dry_run:
        lines.append("<i>(dry run — no sheets were written)</i>")
    lines.append("")
    lines.append(
        f"Shipments fetched: <b>{result.dhl_count + result.fedex_count}</b> "
        f"(DHL {result.dhl_count} · FedEx {result.fedex_count})"
    )
    lines.append("")
    for tab, stats in result.by_tab.items():
        hc = stats.get("hand_carry", 0)
        hc_text = f" · 🚗 {hc} hand-carry" if hc else ""
        lines.append(
            f"• <i>{tab}</i>: {stats['matched']} matched · "
            f"{stats['written']} written{hc_text}"
        )
    if result.errors:
        lines.append("")
        lines.append("<b>⚠️ Errors</b>")
        for e in result.errors:
            lines.append(f"• {e}")
    return "\n".join(lines)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def cutoff_date(days_back: int) -> date:
    """Helper carrier fetchers use to compute their start date."""
    return (utc_now() + timedelta(hours=8) - timedelta(days=days_back)).date()
