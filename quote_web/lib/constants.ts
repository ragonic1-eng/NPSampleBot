// Centralised option lists and reference data for the quotation form.
// Mirrors the constants in the Python bot (quotation.py) so the two stay
// in sync. Edit here if a new sales person is added or packaging size
// changes — the form rebuilds automatically.

export const PACKAGING_OPTIONS = [
  "20 KG / BOX",
  "25 KG / BOX",
  "15 KG / BOTTLE",
  "50 KG / BARREL",
  "1 KG / PACK",
  "500 GRAM / PACK",
] as const;

export const PAYMENT_OPTIONS = [
  "LC AT SIGHT",
  "TT 30 DAYS",
  "TT 60 DAYS",
] as const;

// Shipping incoterms. Order from least → most carriage-responsibility,
// then alphabetical within tier. User-curated set; matches what reps
// actually quote on.
export const INCOTERM_OPTIONS = [
  "EXW",
  "FOB",
  "CFR",
  "CIF",
  "DAP",
  "DDP",
] as const;

export const CURRENCY_OPTIONS = ["USD", "SGD"] as const;
export type Currency = (typeof CURRENCY_OPTIONS)[number];

export const SALES_PEOPLE: Record<string, { fullName: string; hp: string }> = {
  Alex:    { fullName: "Alex",     hp: "+65 8163 1804" },
  Adrian:  { fullName: "Adrian",   hp: "+65 9838 3682" },
  Eric:    { fullName: "Eric",     hp: "+65 9488 2397" },
  Jay:     { fullName: "Jay Wong", hp: "+65 8484 5618" },
  Rich:    { fullName: "Rich",     hp: "+65 9786 9062" },
  Melissa: { fullName: "Melissa",  hp: "+65 8288 3421" },
};

export const SALES_NAMES = Object.keys(SALES_PEOPLE);

// Company address block — fixed in the letterhead, never editable.
export const COMPANY_ADDRESS_LINES = [
  "N. P. Foods (Singapore) Pte Ltd",
  "No. 1 Woodlands Link, Singapore Postal: 738719",
  "TEL: +65-6756-2777 / FAX: +65-6756-2555",
  "URL: www.npsin.com",
  "R.O.C. No.: 198701412M",
];

// Standard greeting baked into every quote, before the product table.
export const DEFAULT_LEAD_COMMENT =
  "Thank you for your faith and your expression of interest in our products. We are pleased to quote you the following price:";

export type Product = {
  name: string;
  code: string;
  currency: Currency;
  price: string;
};

export type QuoteData = {
  companyName: string;
  customerAddress: string;
  customerName: string;
  quotationTitle: string;
  extraComment: string;
  products: Product[];
  // MOQ applies to the whole order, not per-product — a single value
  // entered once on the form, rendered as a merged cell on the PDF.
  moq: string;
  packagingSize: string;
  paymentTerm: string;
  incoterm: string;
  port: string;
  validityDate: string;
  salesPerson: string;
};

export function blankProduct(): Product {
  return { name: "", code: "", currency: "USD", price: "" };
}

export function blankQuote(salesPerson = ""): QuoteData {
  return {
    companyName: "",
    customerAddress: "",
    customerName: "",
    quotationTitle: "",
    extraComment: "",
    products: [blankProduct()],
    moq: "",
    packagingSize: PACKAGING_OPTIONS[0],
    paymentTerm: PAYMENT_OPTIONS[1], // TT 30 DAYS — most common default
    incoterm: "CFR",
    port: "",
    validityDate: "",
    salesPerson,
  };
}

// Render the price cell label as "USD 5.50" unless the rep already wrote a
// currency prefix into the price field manually. Matches the Python
// Product.price_label() so both renderers produce identical strings.
export function priceLabel(p: Product): string {
  const v = (p.price || "").trim();
  if (!v) return "";
  if (/^(usd|sgd|\$)/i.test(v)) return v;
  return `${p.currency} ${v}`;
}

// Today's date in Singapore time, formatted "DD Month YYYY".
export function todayLabel(): string {
  const sgt = new Date(Date.now() + 8 * 60 * 60 * 1000);
  // Use a Date directly w/ Intl in UTC so we don't double-shift.
  const utcCopy = new Date(Date.UTC(
    sgt.getUTCFullYear(),
    sgt.getUTCMonth(),
    sgt.getUTCDate(),
  ));
  return utcCopy.toLocaleDateString("en-GB", {
    day: "numeric",
    month: "long",
    year: "numeric",
    timeZone: "UTC",
  });
}

export function suggestedFilename(d: QuoteData): string {
  const safe = (d.companyName || "Customer")
    .replace(/[^a-zA-Z0-9 _-]/g, "_")
    .trim()
    .split(/\s+/)
    .join("_")
    .slice(0, 40) || "Customer";
  const sgt = new Date(Date.now() + 8 * 60 * 60 * 1000);
  const ymd = `${sgt.getUTCFullYear()}${String(sgt.getUTCMonth() + 1).padStart(2, "0")}${String(sgt.getUTCDate()).padStart(2, "0")}`;
  return `Quotation_${safe}_${ymd}.pdf`;
}
