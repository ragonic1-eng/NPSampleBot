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


@dataclass
class MatchUpdate:
    """One AWB destined for one FSL row."""
    tab: str
    row_number: int  # 1-indexed sheet row
    awb: str
    carrier: str
    customer: str  # for the log line only
    fsl_date_iso: str  # for the log line only


@dataclass
class SyncResult:
    """Returned to callers (the scheduled job + /syncawb)."""
    dhl_count: int = 0
    fedex_count: int = 0
    by_tab: dict[str, dict[str, int]] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    dry_run: bool = False

    @property
    def total_matched(self) -> int:
        return sum(t["matched"] for t in self.by_tab.values())

    @property
    def total_written(self) -> int:
        return sum(t["written"] for t in self.by_tab.values())


# --------------------------- normalisation helpers ---------------------------

_COMPANY_NOISE_RE = re.compile(
    # Common company-form suffixes that vary between carrier labels and
    # the FSL customer master. Stripped before fuzzy-matching.
    r"\b(co\.?,?\s*ltd\.?|pte\.?\s*ltd\.?|sdn\.?\s*bhd\.?|inc\.?|llc|"
    r"corp(?:oration)?|limited|company|factory|gmbh|pvt|private)\b",
    re.IGNORECASE,
)


def _normalize_name(name: str) -> str:
    """Lowercase + strip punctuation + drop common company-form suffixes.

    Both the carrier label and the FSL "Customer Name" cell run through
    this before fuzz comparison. Avoids the WRatio noise where one side
    has "CO., LTD" and the other doesn't.
    """
    if not name:
        return ""
    s = name.lower()
    s = _COMPANY_NOISE_RE.sub(" ", s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = " ".join(s.split())
    return s


def _customer_matches(carrier_name: str, fsl_name: str) -> bool:
    """Fuzzy compare two customer names. Tolerant of company-form suffixes."""
    a = _normalize_name(carrier_name)
    b = _normalize_name(fsl_name)
    if not a or not b:
        return False
    # token_set_ratio handles "HURNG FUR FOODS" vs "FOODS HURNG FUR" and
    # is the most lenient of rapidfuzz's WRatio family — appropriate here
    # because carrier labels often abbreviate words.
    return fuzz.token_set_ratio(a, b) >= NAME_FUZZ_THRESHOLD


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
) -> list[MatchUpdate]:
    """Match each shipment against every FSL row on `tab`.

    Returns one MatchUpdate per FSL row that should receive an AWB.
    Rows whose AWB cell is already filled are SKIPPED (so a re-run
    doesn't overwrite the existing value). When multiple FSL rows match
    one shipment, each row gets the same AWB — matches how a single DHL
    box contains multiple sample bags in practice.
    """
    out: list[MatchUpdate] = []
    seen_rows: set[int] = set()  # avoid double-updating the same FSL row

    # Iterate shipments first → for each, find all matching FSL rows.
    for s in shipments:
        for fsl in fsl_rows:
            row_num = fsl.get("_row")
            if not row_num or row_num in seen_rows:
                continue
            # Re-runs shouldn't trample manually-filled AWBs.
            if (fsl.get("AWB") or "").strip():
                continue
            if not _date_matches(s.ship_date, fsl.get("_date")):
                continue
            if not _customer_matches(s.recipient_name, fsl.get("Customer Name", "")):
                continue
            out.append(MatchUpdate(
                tab=tab,
                row_number=row_num,
                awb=s.awb,
                carrier=s.carrier,
                customer=fsl.get("Customer Name", ""),
                fsl_date_iso=fsl.get("Sample Date Out", ""),
            ))
            seen_rows.add(row_num)
    return out


# --------------------------- orchestrator ---------------------------

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

        updates = build_updates_for_tab(tab, fsl_rows, shipments)
        log.info("%s: %d FSL row(s), %d match(es)", tab, len(fsl_rows), len(updates))

        written = 0
        if updates and not dry_run:
            try:
                written = await asyncio.to_thread(
                    sheets.write_awb_updates,
                    tab,
                    [(u.row_number, u.awb) for u in updates],
                )
            except Exception as e:  # noqa: BLE001
                log.warning("write %s failed: %s", tab, e)
                result.errors.append(f"{tab} write: {e}")
        result.by_tab[tab] = {"matched": len(updates), "written": written}

    return result


async def _empty_coro() -> list[Shipment]:
    return []


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
        lines.append(
            f"• <i>{tab}</i>: {stats['matched']} matched · "
            f"{stats['written']} written"
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
