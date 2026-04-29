import os
from dotenv import load_dotenv

load_dotenv(override=True)

# Bot version — only bump when the user explicitly asks.
BOT_VERSION = "V1.9.7"

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
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1")

DRAFT_TIMEOUT_MINUTES = int(os.getenv("DRAFT_TIMEOUT_MINUTES", "30"))

# MMS3 — credentials for /updatesamplelist (sync Sample Master List from MMS).
# Only visible to the ragonic-gated command, never exposed to other users.
MMS_USER = os.getenv("MMS_USER", "Alex").strip()
MMS_PASSWORD = os.getenv("MMS_PASSWORD", "").strip()

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
