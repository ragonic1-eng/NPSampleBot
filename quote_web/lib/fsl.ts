// FSL data loader for the quote_web autocomplete API.
//
// Reads the three Full Sample Listing tabs from the same Google Sheet
// the bot uses (SEASONING_SHEET_ID), de-dups customers and products,
// and serves them via /api/customers and /api/products endpoints.
//
// Caching: module-level, 10-minute TTL. Vercel serverless functions
// keep the cache warm across requests on the same instance. A cold
// start re-reads the sheet (~3s). Worth it to avoid blowing through
// the Sheets API quota when 5 reps type at once.
//
// Auth: GOOGLE_SERVICE_ACCOUNT_JSON env var on Vercel — same service
// account the bot uses (npsamplebot-robot@nprecobot.iam.gserviceaccount.com).
// Treat that env var as a secret; never log it.

import { GoogleSpreadsheet } from "google-spreadsheet";
import { JWT } from "google-auth-library";

const SHEET_ID = process.env.SEASONING_SHEET_ID || "1ISo8GyI_btbnHcPwr0q6owotoT1mO4ZykYPwfNmwCNg";
const FSL_TABS = [
  "Full Sample Listing",
  "Full Sample Listing Jakarta",
  "Full Sample Listing Thailand",
];

// FSL column order (must match sheets.py FSL_HEADER).
const COL_SALES = 0;
const COL_CUSTOMER = 1;
const COL_COUNTRY = 2;
const COL_CODE = 3;
const COL_NAME = 4;
const COL_QTY = 5;
const COL_DATE = 6;
const COL_RD_PRICE = 9;

export type CustomerSummary = {
  name: string;
  country: string;
  sales: string;        // most-frequent sales rep
  productCount: number; // unique product codes ever sent
  lastDate: string;     // YYYY-MM-DD of most-recent shipment
};

export type ProductSummary = {
  code: string;
  name: string;
  rdPrice: string;      // raw value from the sheet — caller decides currency
  lastDate: string;
  count: number;        // times this code has been sent to this customer
};

type CacheEntry = {
  ts: number;
  customers: CustomerSummary[];
  byCustomerKey: Map<string, ProductSummary[]>;
};

const CACHE_TTL_MS = 10 * 60 * 1000; // 10 min
let _cache: CacheEntry | null = null;
let _loading: Promise<CacheEntry> | null = null;


function jwtClient(): JWT {
  const raw = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!raw) {
    throw new Error(
      "GOOGLE_SERVICE_ACCOUNT_JSON env var is not set on Vercel. " +
      "Set it to the contents of service_account.json."
    );
  }
  const creds = JSON.parse(raw);
  return new JWT({
    email: creds.client_email,
    key: creds.private_key,
    // Drive scope needed in addition to spreadsheets — google-spreadsheet
    // calls the Drive API for sheet metadata (loadInfo) and was 403ing
    // with just the spreadsheets scope. Both are readonly so this only
    // grants the service account read-only access to whatever it can see.
    scopes: [
      "https://www.googleapis.com/auth/spreadsheets.readonly",
      "https://www.googleapis.com/auth/drive.readonly",
    ],
  });
}


function parseIsoDate(s: string): string {
  // Accept the FSL's many date formats (ISO, DD/MMM/YYYY, etc.) and
  // normalize to YYYY-MM-DD for comparison. Bad parses return "" so
  // they sort to the end.
  if (!s) return "";
  const t = s.trim();
  // Already ISO?
  if (/^\d{4}-\d{2}-\d{2}$/.test(t)) return t;
  // DD/MMM/YYYY or DD-MMM-YYYY
  const m = t.match(/^(\d{1,2})[\/\-]([A-Za-z]+)[\/\-](\d{2,4})$/);
  if (m) {
    const day = m[1].padStart(2, "0");
    const months: Record<string, string> = {
      jan: "01", feb: "02", mar: "03", apr: "04", may: "05", jun: "06",
      jul: "07", aug: "08", sep: "09", oct: "10", nov: "11", dec: "12",
    };
    const mo = months[m[2].slice(0, 3).toLowerCase()];
    if (mo) {
      let yr = m[3];
      if (yr.length === 2) yr = "20" + yr;
      return `${yr}-${mo}-${day}`;
    }
  }
  // DD/MM/YYYY
  const m2 = t.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (m2) {
    let yr = m2[3];
    if (yr.length === 2) yr = "20" + yr;
    return `${yr}-${m2[2].padStart(2, "0")}-${m2[1].padStart(2, "0")}`;
  }
  return "";
}


function customerKey(name: string): string {
  // Used as the lookup key in byCustomerKey and as the comparison
  // key on the API. Normalizes whitespace + case so 'Daiya Food'
  // and ' daiya  food ' collapse to the same bucket.
  return name.toLowerCase().replace(/\s+/g, " ").trim();
}


async function loadFromSheet(): Promise<CacheEntry> {
  console.log("[fsl] loading three FSL tabs from Sheets…");
  const auth = jwtClient();
  const doc = new GoogleSpreadsheet(SHEET_ID, auth);
  await doc.loadInfo();

  // Aggregations:
  //   customers: per-customer summary (best country, top sales, etc.)
  //   byCustomerKey: detailed products per customer
  const custMap = new Map<string, {
    name: string;
    countries: Map<string, number>;
    salesCounts: Map<string, number>;
    codeSet: Set<string>;
    lastDate: string;
  }>();
  const byCustomerKey = new Map<string, Map<string, ProductSummary>>();

  for (const tabName of FSL_TABS) {
    const sheet = doc.sheetsByTitle[tabName];
    if (!sheet) {
      console.warn(`[fsl] tab not found: ${tabName}`);
      continue;
    }
    await sheet.loadCells({
      startRowIndex: 0,
      endRowIndex: sheet.rowCount,
      startColumnIndex: 0,
      endColumnIndex: 11,
    });
    const rowCount = sheet.rowCount;
    for (let r = 1; r < rowCount; r++) {  // skip header row
      const get = (c: number) =>
        String(sheet.getCell(r, c).value ?? "").trim();
      const customerName = get(COL_CUSTOMER);
      if (!customerName) continue;
      const key = customerKey(customerName);
      const country = get(COL_COUNTRY);
      const sales = get(COL_SALES);
      const code = get(COL_CODE).toUpperCase();
      const productName = get(COL_NAME);
      const rdPrice = get(COL_RD_PRICE);
      const isoDate = parseIsoDate(get(COL_DATE));

      // ----- customer aggregation -----
      let cust = custMap.get(key);
      if (!cust) {
        cust = {
          name: customerName,
          countries: new Map(),
          salesCounts: new Map(),
          codeSet: new Set(),
          lastDate: "",
        };
        custMap.set(key, cust);
      }
      if (country) {
        cust.countries.set(country, (cust.countries.get(country) || 0) + 1);
      }
      if (sales) {
        cust.salesCounts.set(sales, (cust.salesCounts.get(sales) || 0) + 1);
      }
      if (code) cust.codeSet.add(code);
      if (isoDate && isoDate > cust.lastDate) cust.lastDate = isoDate;

      // ----- per-customer product aggregation -----
      if (code) {
        let prodMap = byCustomerKey.get(key);
        if (!prodMap) {
          prodMap = new Map();
          byCustomerKey.set(key, prodMap);
        }
        const existing = prodMap.get(code);
        if (existing) {
          existing.count++;
          if (isoDate && isoDate > existing.lastDate) {
            existing.lastDate = isoDate;
            existing.rdPrice = rdPrice;  // refresh to latest
            if (productName) existing.name = productName;
          }
        } else {
          prodMap.set(code, {
            code,
            name: productName,
            rdPrice,
            lastDate: isoDate,
            count: 1,
          });
        }
      }
    }
  }

  // Materialise customer summaries (pick most-frequent country + sales).
  const customers: CustomerSummary[] = [];
  for (const cust of custMap.values()) {
    const topCountry = [...cust.countries.entries()]
      .sort((a, b) => b[1] - a[1])[0]?.[0] || "";
    const topSales = [...cust.salesCounts.entries()]
      .sort((a, b) => b[1] - a[1])[0]?.[0] || "";
    customers.push({
      name: cust.name,
      country: topCountry,
      sales: topSales,
      productCount: cust.codeSet.size,
      lastDate: cust.lastDate,
    });
  }
  // Sort by most-recent activity for the autocomplete.
  customers.sort((a, b) => (a.lastDate < b.lastDate ? 1 : -1));

  // Materialise the per-customer product arrays.
  const finalByCustomer = new Map<string, ProductSummary[]>();
  for (const [key, prodMap] of byCustomerKey.entries()) {
    const arr = [...prodMap.values()].sort((a, b) =>
      a.lastDate < b.lastDate ? 1 : -1,
    );
    finalByCustomer.set(key, arr);
  }

  console.log(
    `[fsl] loaded ${customers.length} customers, ` +
    `${finalByCustomer.size} customers have products`,
  );
  return {
    ts: Date.now(),
    customers,
    byCustomerKey: finalByCustomer,
  };
}


async function getCache(): Promise<CacheEntry> {
  const now = Date.now();
  if (_cache && now - _cache.ts < CACHE_TTL_MS) return _cache;
  if (_loading) return _loading;  // de-dupe concurrent first-time loads
  _loading = loadFromSheet()
    .then((entry) => {
      _cache = entry;
      _loading = null;
      return entry;
    })
    .catch((err) => {
      _loading = null;
      throw err;
    });
  return _loading;
}


// ---- public API used by the route handlers ----

export async function searchCustomers(
  query: string,
  limit = 10,
): Promise<CustomerSummary[]> {
  const cache = await getCache();
  const q = query.trim().toLowerCase();
  if (!q) {
    // Empty query → return most-recently-active customers.
    return cache.customers.slice(0, limit);
  }
  // Substring match on customer name; case-insensitive.
  const out: CustomerSummary[] = [];
  for (const cust of cache.customers) {
    if (cust.name.toLowerCase().includes(q)) {
      out.push(cust);
      if (out.length >= limit) break;
    }
  }
  return out;
}


export async function productsForCustomer(
  customerName: string,
  limit = 25,
): Promise<ProductSummary[]> {
  const cache = await getCache();
  const arr = cache.byCustomerKey.get(customerKey(customerName));
  return arr ? arr.slice(0, limit) : [];
}
