"""MMS → Full Sample Listing sync, runnable without a Telegram update.

Used by:
  - bot.py's weekly auto-sync job (no UI, no admin trigger)
  - One-off scripts if we ever need a manual rerun outside Telegram

The function is synchronous (intended to be called via asyncio.to_thread
from the JobQueue callback) so it can use the blocking gspread / requests
clients without contaminating the bot's event loop.

Returns a result dict callers can log / report:
  {
    "status": "ok" | "cooldown" | "no_credentials" | "error",
    "mms_pulled": int,
    "rows_added": int,
    "elapsed_secs": float,
    "window": (start_iso, end_iso),     # only on status="ok"
    "last_sync": iso,                    # only on status="cooldown"
    "error": str,                        # only on status="error"
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

# How often the sync may run before we refuse (force=False). The weekly
# scheduler runs every 7 days so this is mostly a guard against overlapping
# manual + automated runs. Same value as the legacy /updatesamplelist gate.
SAMPLE_SYNC_COOLDOWN_HOURS = 24


def _now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _elapsed(t0: dt.datetime) -> float:
    return (_now_utc() - t0).total_seconds()


def run_mms_to_fsl_sync(force: bool = False) -> dict:
    """Pull fresh sample submissions from MMS and append new ones to FSL.

    Cooldown: 24h between successful runs unless `force=True`.
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

    # 24h cooldown.
    if not force:
        last = sheets.get_last_sample_sync()
        if last is not None:
            if last.tzinfo is None:
                last = last.replace(tzinfo=dt.timezone.utc)
            age = t0 - last
            if age < dt.timedelta(hours=SAMPLE_SYNC_COOLDOWN_HOURS):
                log.info(
                    "sync_engine: skipped (cooldown). Last sync %s, age %s",
                    last.isoformat(timespec="seconds"), age,
                )
                return {
                    "status": "cooldown",
                    "mms_pulled": 0,
                    "rows_added": 0,
                    "elapsed_secs": 0.0,
                    "last_sync": last.isoformat(timespec="seconds"),
                }

    # Date window.
    try:
        start_date = dt.datetime.strptime(config.SAMPLE_UPDATE_START, "%Y-%m-%d").date()
    except ValueError:
        start_date = dt.date(2026, 3, 1)
    end_date = dt.date.today()
    log.info("sync_engine: starting MMS pull window %s → %s", start_date, end_date)

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

    # Step 2: Fetch.
    try:
        mms_rows = mms_client.fetch_all_samples(session, start_date, end_date)
    except Exception as e:  # noqa: BLE001
        log.exception("sync_engine: MMS fetch error")
        return {
            "status": "error", "error": f"fetch: {e}",
            "mms_pulled": 0, "rows_added": 0, "elapsed_secs": _elapsed(t0),
        }
    log.info("sync_engine: pulled %d MMS rows", len(mms_rows))

    # Step 3: Read FSL state for dedupe + lookup maps.
    try:
        existing_keys = sheets.load_fsl_dedupe_keys()
        customer_map = sheets.load_fsl_customer_country_map()
        tab_map = sheets.load_fsl_category_tab_map()
    except Exception as e:  # noqa: BLE001
        log.exception("sync_engine: FSL read failed")
        return {
            "status": "error", "error": f"fsl_read: {e}",
            "mms_pulled": len(mms_rows), "rows_added": 0, "elapsed_secs": _elapsed(t0),
        }

    # Filter to genuinely new rows.
    new_rows: list[mms_client.SampleRow] = []
    for r in mms_rows:
        code = (r.product_code or "").strip().upper()
        date = (r.sample_date_out or "").strip()
        cust = (r.customer_name or "").strip()
        if not (code and date and cust):
            continue
        key = (date, code, " ".join(cust.lower().split()))
        if key in existing_keys:
            continue
        new_rows.append(r)
    log.info("sync_engine: %d new rows after dedupe", len(new_rows))

    # If MMS gave us nothing new, still bump the last-sync timestamp so the
    # cooldown clock starts ticking and Railway logs clearly say "checked".
    if not new_rows:
        try:
            sheets.set_last_sample_sync(_now_utc())
        except Exception as e:  # noqa: BLE001
            log.warning("sync_engine: set_last_sample_sync failed: %s", e)
        return {
            "status": "ok",
            "mms_pulled": len(mms_rows),
            "rows_added": 0,
            "elapsed_secs": _elapsed(t0),
            "window": (start_date.isoformat(), end_date.isoformat()),
        }

    # Step 4: Sort chronologically and enrich each new row.
    def _date_key(r: mms_client.SampleRow):
        return r.sample_date_out_as_date() or dt.date(1900, 1, 1)
    new_rows.sort(key=_date_key)

    country_cache, taste_cache, category_cache = enrich.load_all_caches()
    haiku = enrich.haiku_client()

    enriched: list[list[str]] = []
    for r in new_rows:
        country = enrich.resolve_country(
            raw_country=r.country, customer_name=r.customer_name,
            customer_map=customer_map, country_cache=country_cache,
            haiku_client=haiku,
        )
        taste = enrich.resolve_taste(
            code=r.product_code, name=r.product_name,
            taste_cache=taste_cache, haiku_client=haiku,
        )
        category = enrich.resolve_category(
            code=r.product_code, name=r.product_name,
            tab_map=tab_map, category_cache=category_cache,
            haiku_client=haiku,
        )
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

    # Step 5: Append to FSL.
    try:
        appended = sheets.append_fsl_rows(enriched)
    except Exception as e:  # noqa: BLE001
        log.exception("sync_engine: FSL append failed")
        return {
            "status": "error", "error": f"fsl_append: {e}",
            "mms_pulled": len(mms_rows), "rows_added": 0, "elapsed_secs": _elapsed(t0),
        }

    sync_time = _now_utc()
    try:
        sheets.set_last_sample_sync(sync_time)
    except Exception as e:  # noqa: BLE001
        log.warning("sync_engine: set_last_sample_sync failed: %s", e)

    log.info(
        "sync_engine: appended %d new rows in %.1fs (window %s → %s)",
        appended, _elapsed(t0), start_date, end_date,
    )
    return {
        "status": "ok",
        "mms_pulled": len(mms_rows),
        "rows_added": appended,
        "elapsed_secs": _elapsed(t0),
        "window": (start_date.isoformat(), end_date.isoformat()),
    }


if __name__ == "__main__":
    # Allow running standalone for one-off / debugging.
    import json
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    force = "--force" in sys.argv
    result = run_mms_to_fsl_sync(force=force)
    print(json.dumps(result, indent=2, default=str))
