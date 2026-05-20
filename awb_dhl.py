"""DHL Express MyDHL+ scraper — fetch recent shipments + their AWBs.

Phase 1 STUB. Implements the function signature awb_sync.py expects but
returns no shipments (or a tiny hardcoded test set if AWB_TEST_MODE=1).
Phase 2 will replace the body with the actual Playwright login + shipment-
list scrape — that part needs your portal open so I can read the right
CSS selectors / network responses.

When implementing the real version, fill in:
  - log in via DHL_USER / DHL_PASS env vars at
    https://dhlpass.dhl.com/en-sg/login/...
  - navigate to "Shipment History" (or equivalent)
  - read each row's AWB + recipient + ship date
  - return them as Shipment dataclasses

Failure modes the orchestrator already handles:
  - ImportError on `import playwright` → recorded as error, sync continues
  - Any exception during fetch → caught in awb_sync._safe_fetch, recorded
"""
from __future__ import annotations

import logging
import os
from datetime import date, timedelta

from awb_sync import Shipment, cutoff_date


log = logging.getLogger("npsamplebot.awb_dhl")


# Env-var driven test mode. When AWB_TEST_MODE=1, returns a hardcoded
# shipment so we can verify the matcher end-to-end without DHL access.
# Useful while the real Playwright code is still being written.
_TEST_MODE = os.getenv("AWB_TEST_MODE", "0").strip() == "1"


async def fetch_recent_shipments(*, days_back: int = 14) -> list[Shipment]:
    """Return DHL outbound shipments since (today - days_back).

    Phase 1: returns [] unless AWB_TEST_MODE=1.
    Phase 2: scrapes DHL Express MyDHL+ via Playwright.
    """
    if _TEST_MODE:
        log.info("AWB_TEST_MODE=1 — returning hardcoded DHL test shipments")
        today = date.today()
        return [
            Shipment(
                carrier="DHL",
                awb="1234567890",
                recipient_name="HURNG FUR FOODS FACTORY CO., LTD",
                recipient_country="Taiwan",
                ship_date=today - timedelta(days=1),
            ),
        ]

    user = os.getenv("DHL_USER", "").strip()
    pwd = os.getenv("DHL_PASS", "").strip()
    if not user or not pwd:
        log.info("DHL_USER / DHL_PASS not set — skipping DHL fetch")
        return []

    # Phase 2 placeholder. Until selectors are written, return [] so the
    # scheduled job doesn't blow up. The matcher and sheet-writer were
    # verified end-to-end with AWB_TEST_MODE in dev.
    _ = cutoff_date(days_back)  # will be used in Phase 2 for the date filter
    log.info("DHL scraper not yet implemented — returning 0 shipments")
    return []
