import os
from dotenv import load_dotenv

load_dotenv(override=True)

# Bot version — only bump when the user explicitly asks.
BOT_VERSION = "V1.17.19"

# DHL Express + FedEx login credentials used by awb_sync to scrape recent
# shipments. NEVER commit values to .env — set them on Railway's
# Variables tab. Empty values disable the corresponding carrier (the
# AWB sync logs a "skipping" line and continues with whatever's
# available). See awb_dhl.py / awb_fedex.py.
DHL_USER = os.getenv("DHL_USER", "").strip()
DHL_PASS = os.getenv("DHL_PASS", "").strip()
FEDEX_USER = os.getenv("FEDEX_USER", "").strip()
FEDEX_PASS = os.getenv("FEDEX_PASS", "").strip()

# Public URL of the Vercel-hosted quotation builder web app (no trailing
# slash). /quote and the 📄 menu button hand the rep a clickable button that
# opens this URL with their sales name pre-filled via ?sales= — quote_web/
# app/page.tsx reads that param and pre-selects the signatory.
#
# V1.17.4 — this is now the SINGLE SOURCE OF TRUTH and the QUOTE_WEB_URL env
# var is deliberately NOT read any more. Why: Railway still carries a
# QUOTE_WEB_URL pointing at the retired np-quote-web.vercel.app deployment
# (now a hard 404), and because an env var beats a code default, that stale
# value silently overrode V1.17.2 and kept handing reps a dead link. The URL
# is public, not a secret, and only ever changes when the app is redeployed
# — i.e. alongside a code change anyway — so config is the right home for it
# and there's no dashboard step to forget.
#
# Deleting the stale Railway variable is harmless but no longer necessary;
# it is simply ignored. To move to a new deployment, edit this line.
QUOTE_WEB_URL = "https://quoteweb-blue.vercel.app"

# Margin added to MMS raw_material_cost before showing it to the user (and
# before logging to the Query audit tab). Covers handling / overhead so
# /pp and /scan output the customer-facing cost, not the bare MMS figure.
# Adjust by editing this constant; price discipline lives in one place.
RMC_MARKUP_USD = 0.30

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json")
SEASONING_SHEET_ID = os.getenv("SEASONING_SHEET_ID", "").strip()
SEASONING_WORKSHEET_NAME = os.getenv("SEASONING_WORKSHEET_NAME", "Sheet1")
OPS_SHEET_ID = os.getenv("OPS_SHEET_ID", "").strip()

# Customer master list (col A = Customer Code, col B = Customer Name).
# Authoritative source for customer name matching at 12/15.
CUSTOMER_MASTER_SHEET_ID = os.getenv(
    "CUSTOMER_MASTER_SHEET_ID",
    "1ZfmAfkcybZ9Gi-UA-uD33QoLDSz0fBrFiYsDc2EGJjA",
).strip()
CUSTOMER_MASTER_WORKSHEET_NAME = os.getenv("CUSTOMER_MASTER_WORKSHEET_NAME", "Sheet1")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()

# V1.17.x — emergency kill-switch for the RapidOCR engine. Set to 1 on
# Railway if the OCR models ever cause memory pressure; the bot then uses
# the Tesseract → Haiku path exactly as before. No redeploy needed beyond
# the env-var change.
DISABLE_RAPIDOCR = os.getenv("DISABLE_RAPIDOCR", "").strip().lower() in ("1", "true", "yes")

# Groq Whisper — handsfree voice → /pp code lookup. Voice messages get
# downloaded, transcribed by Groq's whisper-large-v3-turbo (free tier
# covers ~7,500 sec/day, paid is ~$0.04/hr), then any product code
# in the transcription is routed to /pp. No code = friendly "what
# did you say?" response. Set GROQ_API_KEY in Railway env vars.
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
GROQ_WHISPER_MODEL = os.getenv("GROQ_WHISPER_MODEL", "whisper-large-v3-turbo")

# Daily sample-digest reminder (V1.13.13). Posts a list of every sample
# logged that day to a specific group chat at 18:00 SGT, weekdays only.
# Empty value = feature disabled. Get the chat ID by running /whichchat
# from inside the group, then set this env var on Railway.
DAILY_DIGEST_CHAT_ID = os.getenv("DAILY_DIGEST_CHAT_ID", "").strip()
# Haiku is the default — used by ai.rerank_seasonings() for semantic
# catalog search ranking. Plenty good for picking among 3–10 fuzzy
# candidates and ~3x cheaper than Sonnet ($1/$5 input/output per Mtok
# vs Sonnet's $3/$15). Override via env if a future task needs Sonnet.
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-haiku-4-5")

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434").rstrip("/")
# V1.17.1 — local_llm.py runs the taste/category enrichment on the PC's CPU
# for $0 instead of paying Haiku. qwen3.5:4b is the current best speed/quality
# pick for CPU-only inference (~3.4 GB, no GPU needed). Was "llama3.1", which
# was never pulled on this machine — the old default would have 404'd.
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3.5:4b")

DRAFT_TIMEOUT_MINUTES = int(os.getenv("DRAFT_TIMEOUT_MINUTES", "30"))

# MMS3 — credentials for /updatesamplelist (sync Sample Master List from MMS).
# Only visible to the ragonic-gated command, never exposed to other users.
# V1.17.14 — lowercase. MMS's login is CASE-SENSITIVE on the user id, and the
# capitalised "Alex" silently stopped authenticating on 17 Jul 2026, killing
# all scraping for four days while the password was perfectly valid.
MMS_USER = os.getenv("MMS_USER", "alex").strip()
MMS_PASSWORD = os.getenv("MMS_PASSWORD", "").strip()
# V1.17.x — Sample Request code the /pp lookup borrows for the Add-Delete
# probe trick (mms_product._fetch_rd_via_probe_sr). Used only when a code
# has never been added to any SR and the normal lookup returns nothing.
# The bot adds the product, scrapes the R&D price, then ALWAYS deletes
# the row to leave the SR untouched. Pick a stable, rarely-edited SR.
# Empty string disables the probe fallback entirely.
MMS_PROBE_SR_CODE = os.getenv("MMS_PROBE_SR_CODE", "J-123J43-001").strip()

# Telegram username (without @) allowed to use /updatesamplelist.
UPDATE_SAMPLE_OWNER = os.getenv("UPDATE_SAMPLE_OWNER", "ragonic").lstrip("@").lower()

# Start date for /updatesamplelist — Mar 2026 overlap (catches late-arriving
# rows from March that weren't in the historical PDF backfill).
SAMPLE_UPDATE_START = os.getenv("SAMPLE_UPDATE_START", "2026-03-01").strip()

# Tab names inside OPS_SHEET_ID
TAB_CUSTOMERS = "Customers"
TAB_SALES_LOG = "Sample request list from sales"
TAB_USERS = "Authorized Users"

# Columns
SEASONING_COL_NAME = "Seasoning Name"
SEASONING_COL_PRICE = "R&D Price (USD)"
SEASONING_COL_CODE = "Code"

CUSTOMER_COLS = [
    "Company Name",
    "Address",
    "Receiver Number",
    "Receiving Person",
    "Preferred Courier",
]

SALES_LOG_COLS = [
    "Timestamp",
    "Sales Person (Telegram)",
    "Telegram User ID",
    "Seasoning Requested",
    "Matched Code",
    "Matched Price",
    "Comment",
    "Quantity",
    "Selling Price Budget",
    "Application Method",
    "Dosage",
    "Requirement",
    "Market",
    "Deadline",
    "Need to Check Taste",
    "Customer Base",
    "Preferred Courier",
    "Customer Company Name",
    "Receiver Number",
    "Address",
    "Receiving Person",
]

USER_COLS = ["Telegram Username", "Telegram User ID", "Name", "Active", "MMS Name"]
