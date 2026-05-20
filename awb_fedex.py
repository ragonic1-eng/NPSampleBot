"""FedEx scraper — fetch recent shipments + their AWBs.

Phase 1 STUB. Same shape as awb_dhl.py — Phase 2 fills in the Playwright
login + shipment-list scrape.

When implementing the real version, fill in:
  - log in via FEDEX_USER / FEDEX_PASS env vars at LOGIN_URL (below)
  - navigate to the recent-shipments view
  - read each row's tracking number + recipient + ship date
  - return them as Shipment dataclasses
"""
from __future__ import annotations

import logging
import os
from datetime import date, timedelta

from awb_sync import Shipment, cutoff_date


log = logging.getLogger("npsamplebot.awb_fedex")


# FedEx secure-login URL. The "#/credentials" hash route is the literal
# username+password screen — landing on /secure-login/ alone drops the
# user on the FedEx homepage. User-confirmed value.
LOGIN_URL = "https://www.fedex.com/secure-login/en-us/#/credentials"


_TEST_MODE = os.getenv("AWB_TEST_MODE", "0").strip() == "1"


async def fetch_recent_shipments(*, days_back: int = 14) -> list[Shipment]:
    """Return FedEx outbound shipments since (today - days_back).

    Phase 1: returns [] unless AWB_TEST_MODE=1.
    Phase 2: scrapes FedEx via Playwright.
    """
    if _TEST_MODE:
        log.info("AWB_TEST_MODE=1 — returning hardcoded FedEx test shipments")
        today = date.today()
        return [
            Shipment(
                carrier="FedEx",
                awb="987654321098",
                recipient_name="HURNG FUR FOODS FACTORY",
                recipient_country="Taiwan",
                ship_date=today - timedelta(days=2),
            ),
        ]

    user = os.getenv("FEDEX_USER", "").strip()
    pwd = os.getenv("FEDEX_PASS", "").strip()
    if not user or not pwd:
        log.info("FEDEX_USER / FEDEX_PASS not set — skipping FedEx fetch")
        return []

    _ = cutoff_date(days_back)
    log.info("FedEx scraper not yet implemented — returning 0 shipments")
    return []
