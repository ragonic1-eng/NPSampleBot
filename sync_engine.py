"""MMS → Full Sample Listing sync, runnable without a Telegram update.

V1.11.0: now handles BOTH Singapore (FSL_TAB) and Indonesia (JAKARTA_FSL_TAB).
A single MMS pull is split by code prefix:
  S-XXX → Full Sample Listing       (existing behaviour, country derived per customer)
  J-XXX → Full Sample Listing Jakarta (V1.11.0+, country hardcoded to 'Indonesia')
Anything else (e.g. legacy B- codes) defaults to FSL_TAB so we don't lose data.

Singapore data is **never** modified — the sync only ever appends to its
own region's tab. Re-sort runs per-tab. Singapore behaviour pre-V1.11.0
is preserved bit-for-bit.

Used by:
  - bot.py's twice-weekly auto-sync job (Mon + Thu 02:00 UTC)
  - One-off scripts (e.g. `_jakarta_initial_backfill.py`)

Returns a result dict callers can log / report:
  {
    "status": "ok" | "cooldown" | "no_credentials" | "error",
    "mms_pulled": int,
    "rows_added": int,                # total across both regions
    "elapsed_secs": float,
    "window": (start_iso, end_iso),
    "regions": [
      {"region": "S", "tab": "Full Sample Listing", "fetched": int, "added": int},
      {"region": "J", "tab": "Full Sample Listing Jakarta", "fetched": int, "added": int},
    ],
    "haiku_calls": int,
    "free_lookups": int,
  }
"""
from __future__ import annotations

import datetime as dt
import logging

import requests

import config
import enrich
import mms_client
import sheets

log = logging.getLogger(__name__)

# How often the sync may run before we refuse (force=False). The scheduler
# now runs twice a week, so this guard mostly stops overlapping manual +
# automated runs.
SAMPLE_SYNC_COOLDOWN_HOURS = 24

# Region routing. Code prefix → (target tab, country mode).
#   country='derive' — use the existing per-customer resolution
#   country='Indonesia' — hardcode every row's Country to 'Indonesia'
#   country='Thailand'  — hardcode every row's Country to 'Thailand'
# V1.13.0: B- prefix added (Thailand factory). User clarified that B-
# is Thailand, not legacy Singapore as previously assumed; the 205
# stranded B-rows that lived in FSL_TAB before this change were
# migrated to BANGKOK_FSL_TAB by _thailand_migrate_stranded.py.
_REGIONS = {
    "S": (sheets.FSL_TAB, "derive"),
    "J": (sheets.JAKARTA_FSL_TAB, "Indonesia"),
    "B": (sheets.BANGKOK_FSL_TAB, "Thailand"),
}


def _now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _elapsed(t0: dt.datetime) -> float:
    return (_now_utc() - t0).total_seconds()


def _row_region(row: "mms_client.SampleRow") -> str:
    """Return 'S', 'J', or 'S' (default) for a sample row's product code."""
    code = (row.product_code or "").strip().upper()
    if not code:
        return "S"  # treat empty-code rows as legacy/default
    return code.split("-", 1)[0] if code.split("-", 1)[0] in _REGIONS else "S"


def _process_region(
    region: str,
    region_rows: list["mms_client.SampleRow"],
    haiku_client,
    metrics: dict,
    skip_enrichment: bool = False,
) -> dict:
    """Dedupe → enrich → append → sort for one region. Returns a stats dict.

    Reads the region's own FSL state, so the dedupe / customer-country /
    self-cache logic is fully isolated per region. Singapore's existing
    behaviour is unchanged because it goes through the same code path with
    country='derive'.

    skip_enrichment=True writes blank taste/category and still hardcodes
    'Indonesia' for J- country. Used by the V1.11.0 backfill so we don't
    burn API credits — Sonnet does the enrichment in chat afterwards
    against the populated rows. Singapore mode + skip is still safe (would
    leave country blank for unmapped customers, but the backfill never
    targets Singapore with skip_enrichment=True).
    """
    tab, country_mode = _REGIONS[region]

    # Region-scoped FSL state (single sheet read).
    try:
        state = sheets.load_fsl_state(tab=tab)
    except Exception as e:  # noqa: BLE001
        log.exception("sync_engine[%s]: FSL state read failed", tab)
        return {"region": region, "tab": tab, "fetched": len(region_rows),
                "added": 0, "error": f"fsl_read: {e}"}

    existing_keys = state["dedupe_keys"]
    customer_map = state["customer_country"]
    fsl_taste_map = state["code_taste"]
    fsl_category_map = state["code_category"]
    # Category-tab lookup (the 6 manual tabs Snack / Noodle ... / Beverage)
    # is only meaningful for Singapore S- codes — Indonesia products aren't
    # listed there. Skip the read entirely for J-.
    if region == "S":
        try:
            tab_map = sheets.load_fsl_category_tab_map()
        except Exception as e:  # noqa: BLE001
            log.warning("sync_engine[%s]: category tab map read failed: %s", tab, e)
            tab_map = {}
    else:
        tab_map = {}

    log.info(
        "sync_engine[%s]: %d MMS rows · state: %d dedupe, %d customers, "
        "%d code-taste, %d code-category",
        tab, len(region_rows), len(existing_keys), len(customer_map),
        len(fsl_taste_map), len(fsl_category_map),
    )

    # Dedupe.
    new_rows: list[mms_client.SampleRow] = []
    for r in region_rows:
        code = (r.product_code or "").strip().upper()
        date = (r.sample_date_out or "").strip()
        cust = (r.customer_name or "").strip()
        if not (code and date and cust):
            continue
        key = (date, code, " ".join(cust.lower().split()))
        if key in existing_keys:
            continue
        new_rows.append(r)
    log.info("sync_engine[%s]: %d new rows after dedupe", tab, len(new_rows))

    # Bump the region's last-sync timestamp regardless — even on zero-new
    # we want the cooldown clock to start ticking.
    if not new_rows:
        try:
            sheets.set_last_sample_sync(_now_utc(), tab=tab)
        except Exception as e:  # noqa: BLE001
            log.warning("sync_engine[%s]: set_last_sample_sync failed: %s", tab, e)
        return {"region": region, "tab": tab, "fetched": len(region_rows), "added": 0}

    # Chronological sort before enrichment so the appended block is in date
    # order (the per-tab sort_fsl_by_date at the end re-sorts the whole tab
    # anyway, but this keeps the appended block coherent if the sort step
    # fails for some reason).
    new_rows.sort(
        key=lambda r: r.sample_date_out_as_date() or dt.date(1900, 1, 1)
    )

    country_cache, taste_cache, category_cache = enrich.load_all_caches()

    def _resolve_country_tracked(r):
        """Country resolution with metrics tracking. Indonesia/Thailand
        modes skip all the lookup work and hardcode the value, which
        means zero Haiku spend per row for non-Singapore syncs."""
        if country_mode == "Indonesia":
            return "Indonesia", "free"
        if country_mode == "Thailand":
            return "Thailand", "free"
        # Singapore mode = original V1.7.4 cascade.
        if (r.country or "").strip():
            return enrich.normalize_country(r.country), "free"
        if not r.customer_name:
            return "", "free"
        cust_norm = " ".join(r.customer_name.lower().split())
        if customer_map.get(cust_norm):
            return enrich.normalize_country(customer_map[cust_norm]), "free"
        if enrich._country_from_tokens(r.customer_name):
            return enrich._country_from_tokens(r.customer_name), "free"
        if enrich._country_from_suffix(r.customer_name):
            return enrich._country_from_suffix(r.customer_name), "free"
        out = enrich.resolve_country(
            raw_country=r.country, customer_name=r.customer_name,
            customer_map=customer_map, country_cache=country_cache,
            haiku_client=haiku_client,
        )
        was_cached = (
            r.customer_name in country_cache
            and country_cache[r.customer_name] == out
        )
        return out, ("free" if was_cached else "haiku")

    enriched: list[list[str]] = []
    for r in new_rows:
        if skip_enrichment:
            # Country still hardcoded for Indonesia/Thailand modes; for
            # derive mode we only use the FREE lookups (raw country /
            # customer map / tokens / suffix) — skip the Haiku fallback.
            # Empty if nothing cheap matches.
            if country_mode == "Indonesia":
                country = "Indonesia"
            elif country_mode == "Thailand":
                country = "Thailand"
            else:
                country = ""
                if (r.country or "").strip():
                    country = enrich.normalize_country(r.country)
                elif r.customer_name:
                    cust_norm = " ".join(r.customer_name.lower().split())
                    if customer_map.get(cust_norm):
                        country = enrich.normalize_country(customer_map[cust_norm])
                    elif enrich._country_from_tokens(r.customer_name):
                        country = enrich._country_from_tokens(r.customer_name)
                    elif enrich._country_from_suffix(r.customer_name):
                        country = enrich._country_from_suffix(r.customer_name)
            metrics["country_free"] += 1
            taste = ""
            category = ""
            metrics["taste_free"] += 1
            metrics["category_free"] += 1
        else:
            country, country_src = _resolve_country_tracked(r)
            metrics[f"country_{country_src}"] += 1

            code_upper = (r.product_code or "").strip().upper()
            # Taste source classification (for cost reporting).
            taste_src = "haiku"
            if not code_upper:
                taste_src = "free"
            elif fsl_taste_map.get(code_upper):
                taste_src = "free"
            elif code_upper in taste_cache and taste_cache[code_upper]:
                taste_src = "free"
            taste = enrich.resolve_taste(
                code=r.product_code, name=r.product_name,
                taste_cache=taste_cache, haiku_client=haiku_client,
                fsl_map=fsl_taste_map,
            )
            metrics[f"taste_{taste_src}"] += 1

            # Category source classification.
            cat_src = "haiku"
            if not code_upper:
                cat_src = "free"
            elif code_upper in tab_map:
                cat_src = "free"
            elif fsl_category_map.get(code_upper) in enrich.CATEGORIES:
                cat_src = "free"
            elif code_upper in category_cache and category_cache[code_upper] in enrich.CATEGORIES:
                cat_src = "free"
            category = enrich.resolve_category(
                code=r.product_code, name=r.product_name,
                tab_map=tab_map, category_cache=category_cache,
                haiku_client=haiku_client, fsl_map=fsl_category_map,
            )
            metrics[f"category_{cat_src}"] += 1

        enriched.append([
            r.sales,
            r.customer_name,
            country,
            r.product_code,
            r.product_name,
            r.quantity_g,
            r.sample_date_out,
            taste,
            category,
            r.rd_price,
        ])

    # Append + sort + record sync time. All three are best-effort: a failure
    # downstream of append shouldn't lose the rows we already wrote.
    try:
        appended = sheets.append_fsl_rows(enriched, tab=tab)
    except Exception as e:  # noqa: BLE001
        log.exception("sync_engine[%s]: append failed", tab)
        return {"region": region, "tab": tab, "fetched": len(region_rows),
                "added": 0, "error": f"append: {e}"}

    try:
        n_sorted = sheets.sort_fsl_by_date(tab=tab)
        log.info("sync_engine[%s]: re-sorted %d rows by date", tab, n_sorted)
    except Exception as e:  # noqa: BLE001
        log.warning("sync_engine[%s]: sort failed: %s", tab, e)

    try:
        sheets.set_last_sample_sync(_now_utc(), tab=tab)
    except Exception as e:  # noqa: BLE001
        log.warning("sync_engine[%s]: set_last_sample_sync failed: %s", tab, e)

    return {"region": region, "tab": tab, "fetched": len(region_rows),
            "added": appended}


def run_mms_to_fsl_sync(
    force: bool = False,
    start_override: dt.date | None = None,
    regions: tuple[str, ...] = ("S", "J", "B"),
    skip_enrichment: bool = False,
) -> dict:
    """Pull fresh sample submissions from MMS and append new ones to the
    matching FSL tab. All 3 factories — Singapore (S-), Indonesia (J-),
    and Thailand/Bangkok (B-) — are processed by default; pass an
    explicit subset like ``regions=('S',)`` to limit.

    Audit V1.13.14 — Thailand was missing from the default tuple, so
    the scheduled daily sync (which calls this with the default) had
    never refreshed Bangkok samples since the B- region was added.

    ``start_override`` lets callers (e.g. the Indonesia initial backfill
    script) request a much earlier window than the default
    config.SAMPLE_UPDATE_START. The MMS round-trip is the same whether we
    fetch 2 months or 10 years of history — the dedupe step ensures we
    only enrich+append rows we haven't seen.

    Cooldown: 24h between successful runs unless ``force=True``. Gated on
    Singapore's last-sync (kept simple — both regions are usually in lock-
    step on the twice-weekly schedule).
    """
    t0 = _now_utc()

    if not config.MMS_PASSWORD:
        log.warning("sync_engine: MMS_PASSWORD not set — skipping run")
        return {
            "status": "no_credentials",
            "mms_pulled": 0,
            "rows_added": 0,
            "elapsed_secs": 0.0,
        }

    # 24h cooldown — checked per-region rather than only on Singapore.
    # Audit fix #7: previously this read only sheets.FSL_TAB's
    # timestamp, which meant a region-only call (e.g. regions=("J",))
    # could be blocked by a stale SG cooldown it never updated, and a
    # successful Jakarta-only run wouldn't bump the SG timestamp the
    # next gate reads. Now we proceed if ANY requested region is past
    # cooldown — the single fetch+dedupe pass below catches them all
    # up regardless of which one tripped the gate.
    if not force:
        region_ages: list[tuple[str, dt.timedelta | None]] = []
        for region in regions:
            spec = _REGIONS.get(region)
            if not spec:
                continue
            tab = spec[0]
            last = sheets.get_last_sample_sync(tab=tab)
            if last is None:
                region_ages.append((region, None))
                continue
            if last.tzinfo is None:
                last = last.replace(tzinfo=dt.timezone.utc)
            region_ages.append((region, t0 - last))
        cooldown_td = dt.timedelta(hours=SAMPLE_SYNC_COOLDOWN_HOURS)
        stale_regions = [
            r for r, age in region_ages
            if age is None or age >= cooldown_td
        ]
        if region_ages and not stale_regions:
            ages_str = ", ".join(
                f"{r}={age}" for r, age in region_ages
            )
            log.info(
                "sync_engine: skipped (cooldown). All requested regions fresh: %s",
                ages_str,
            )
            return {
                "status": "cooldown",
                "mms_pulled": 0,
                "rows_added": 0,
                "elapsed_secs": 0.0,
                "region_ages": {r: (str(a) if a else None) for r, a in region_ages},
            }
        if stale_regions:
            log.info(
                "sync_engine: cooldown allows run — stale regions: %s",
                stale_regions,
            )

    # Date window.
    if start_override is not None:
        start_date = start_override
    else:
        try:
            start_date = dt.datetime.strptime(config.SAMPLE_UPDATE_START, "%Y-%m-%d").date()
        except ValueError:
            start_date = dt.date(2026, 3, 1)
    end_date = dt.date.today()
    log.info("sync_engine: starting MMS pull window %s → %s (regions=%s)",
             start_date, end_date, regions)

    # Step 1: MMS login.
    session = requests.Session()
    try:
        ok = mms_client.login(session, config.MMS_USER, config.MMS_PASSWORD)
    except Exception as e:  # noqa: BLE001
        log.exception("sync_engine: MMS login error")
        return {
            "status": "error", "error": f"login: {e}",
            "mms_pulled": 0, "rows_added": 0, "elapsed_secs": _elapsed(t0),
        }
    if not ok:
        return {
            "status": "error", "error": "MMS login failed (check creds)",
            "mms_pulled": 0, "rows_added": 0, "elapsed_secs": _elapsed(t0),
        }

    # Step 2: Single MMS fetch — both regions share the same pull.
    try:
        mms_rows = mms_client.fetch_all_samples(session, start_date, end_date)
    except Exception as e:  # noqa: BLE001
        log.exception("sync_engine: MMS fetch error")
        return {
            "status": "error", "error": f"fetch: {e}",
            "mms_pulled": 0, "rows_added": 0, "elapsed_secs": _elapsed(t0),
        }
    log.info("sync_engine: pulled %d MMS rows total", len(mms_rows))

    # Step 3: Split rows by region prefix, then process each.
    rows_by_region: dict[str, list[mms_client.SampleRow]] = {r: [] for r in regions}
    other_count = 0
    for r in mms_rows:
        reg = _row_region(r)
        if reg in rows_by_region:
            rows_by_region[reg].append(r)
        else:
            other_count += 1
    if other_count:
        log.info("sync_engine: %d MMS rows had unhandled prefix (skipped this run)", other_count)

    haiku_client = enrich.haiku_client()

    # Shared metrics accumulator across regions for the final cost summary.
    metrics = {
        "country_free": 0, "country_haiku": 0,
        "taste_free": 0, "taste_haiku": 0,
        "category_free": 0, "category_haiku": 0,
    }

    region_results: list[dict] = []
    for region in regions:
        region_rows = rows_by_region.get(region, [])
        try:
            res = _process_region(
                region, region_rows, haiku_client, metrics,
                skip_enrichment=skip_enrichment,
            )
        except Exception as e:  # noqa: BLE001
            log.exception("sync_engine: region %s failed", region)
            res = {
                "region": region, "tab": _REGIONS[region][0],
                "fetched": len(region_rows), "added": 0, "error": str(e),
            }
        region_results.append(res)

    haiku_total = metrics["country_haiku"] + metrics["taste_haiku"] + metrics["category_haiku"]
    free_total = metrics["country_free"] + metrics["taste_free"] + metrics["category_free"]
    total_added = sum(r.get("added", 0) for r in region_results)

    log.info(
        "sync_engine: enrichment cost — %d Haiku calls, %d free lookups "
        "(country: %d free / %d haiku · taste: %d free / %d haiku · "
        "category: %d free / %d haiku)",
        haiku_total, free_total,
        metrics["country_free"], metrics["country_haiku"],
        metrics["taste_free"], metrics["taste_haiku"],
        metrics["category_free"], metrics["category_haiku"],
    )
    log.info(
        "sync_engine: total appended %d rows in %.1fs (window %s → %s)",
        total_added, _elapsed(t0), start_date, end_date,
    )
    return {
        "status": "ok",
        "mms_pulled": len(mms_rows),
        "rows_added": total_added,
        "elapsed_secs": _elapsed(t0),
        "window": (start_date.isoformat(), end_date.isoformat()),
        "regions": region_results,
        "haiku_calls": haiku_total,
        "free_lookups": free_total,
        "enrichment_metrics": metrics,
    }


if __name__ == "__main__":
    # Allow running standalone for one-off / debugging.
    import json
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    force = "--force" in sys.argv
    skip_enrichment = "--skip-enrichment" in sys.argv
    # Optional region filter: --region=S or --region=J
    regions = ("S", "J")
    for arg in sys.argv:
        if arg.startswith("--region="):
            regions = tuple(arg.split("=", 1)[1].upper().split(","))
    # Optional start-date override: --start=2010-01-01
    start_override = None
    for arg in sys.argv:
        if arg.startswith("--start="):
            try:
                start_override = dt.datetime.strptime(
                    arg.split("=", 1)[1], "%Y-%m-%d"
                ).date()
            except ValueError:
                pass
    result = run_mms_to_fsl_sync(
        force=force, regions=regions, start_override=start_override,
        skip_enrichment=skip_enrichment,
    )
    print(json.dumps(result, indent=2, default=str))
