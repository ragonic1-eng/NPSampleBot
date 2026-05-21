"use client";

import { Suspense, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "next/navigation";

import {
  blankProduct,
  blankQuote,
  CURRENCY_OPTIONS,
  INCOTERM_OPTIONS,
  PACKAGING_OPTIONS,
  PAYMENT_OPTIONS,
  Product,
  QuoteData,
  SALES_NAMES,
  SALES_PEOPLE,
  suggestedFilename,
} from "@/lib/constants";


// ---- types matching the /api/customers + /api/customer-products responses ----
type CustomerSuggestion = {
  name: string;
  country: string;
  sales: string;
  productCount: number;
  lastDate: string;
  // V1.17.0 — auto-populated by AWB sync from the carrier's ship-to
  // label. Empty for customers with no DHL shipment yet (FedEx-only
  // customers, hand-carry samples). The form pre-fills the address
  // textarea on customer pick; rep can edit before generating the PDF.
  address: string;
};
type ProductSuggestion = {
  code: string;
  name: string;
  rdPrice: string;
  lastDate: string;
  count: number;
};

// IMPORTANT: do NOT use PDFDownloadLink here. It re-renders the entire
// PDF document on every prop change — typing one letter in a field
// would trigger a full @react-pdf/renderer pass, and any transient bad
// input state (mid-edit empty price, half-typed code, etc.) could
// throw and blank the page with no error boundary to catch it.
//
// Instead we keep all PDF work inside an on-click handler:
//   - imports @react-pdf/renderer ONLY when the user clicks Generate
//   - renders the document to a Blob, triggers a download anchor click
//   - any error surfaces in setError() state instead of crashing render


/** Wrapper so a non-button placeholder shares the action-button styles. */
function ButtonShell({ children }: { children: React.ReactNode }) {
  return (
    <span className="inline-block px-5 py-2.5 rounded-md bg-gray-300 text-gray-700 text-sm font-semibold">
      {children}
    </span>
  );
}


/** Reusable labelled input field row. */
function Field({
  label,
  hint,
  children,
}: {
  label: string;
  hint?: string;
  children: React.ReactNode;
}) {
  return (
    <label className="flex flex-col gap-1 mb-3">
      <span className="text-sm font-medium text-gray-700">{label}</span>
      {children}
      {hint ? <span className="text-xs text-gray-500">{hint}</span> : null}
    </label>
  );
}


/** Render an ISO YYYY-MM-DD as 'DD Month YYYY' for the PDF + the
 * 'Will print as:' hint under the date input. Returns the original
 * string verbatim if it's not parseable (e.g. legacy free-text values
 * carried over from a saved draft). */
function formatValidityForDisplay(iso: string): string {
  if (!iso) return "";
  const m = iso.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return iso;  // unrecognised format → leave as-is
  const d = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]));
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString("en-GB", {
    day: "numeric",
    month: "long",
    year: "numeric",
    timeZone: "UTC",
  });
}


/** Debounce hook — returns a value that lags `value` by `delay` ms. Used
 * by the autocomplete inputs so we don't fire a fetch on every keystroke. */
function useDebounced<T>(value: T, delay = 250): T {
  const [v, setV] = useState(value);
  useEffect(() => {
    const t = setTimeout(() => setV(value), delay);
    return () => clearTimeout(t);
  }, [value, delay]);
  return v;
}


/** Free-text input with a dropdown of suggestions fetched from `fetcher`.
 *
 *  - The user can type ANY value (we don't gate on suggestion match).
 *  - When they click a suggestion, `onPick(item)` fires so the parent
 *    can also fill OTHER fields from the picked record (country, sales
 *    rep, etc. for customer; code, price, currency for product).
 *  - Dropdown auto-hides on outside click + blur.
 */
function Autocomplete<T extends Record<string, unknown>>({
  value,
  onChange,
  onPick,
  placeholder,
  fetcher,
  renderItem,
  className = "w-full border rounded-md px-3 py-2 text-sm",
  minChars = 1,
  emptyMessage = "No matches",
  disabledMessage,
}: {
  value: string;
  onChange: (s: string) => void;
  onPick: (item: T) => void;
  placeholder?: string;
  fetcher: (q: string) => Promise<T[]>;
  renderItem: (item: T) => React.ReactNode;
  className?: string;
  minChars?: number;
  // Shown when items.length === 0 and !loading — replaces the
  // default "No matches" so we can explain WHY (e.g. "Pick a
  // customer first" for the product autocomplete).
  emptyMessage?: string;
  // When set, the autocomplete is fully visible but disabled, and
  // shows this message instead of opening a dropdown. Used when a
  // prerequisite (like 'customer must be picked first') is unmet.
  disabledMessage?: string;
}) {
  const [open, setOpen] = useState(false);
  const [items, setItems] = useState<T[]>([]);
  const [loading, setLoading] = useState(false);
  const debounced = useDebounced(value, 250);
  const wrapperRef = useRef<HTMLDivElement>(null);

  // Fetch suggestions whenever the debounced value changes AND the box
  // is open. Don't fetch when closed — saves bandwidth and keeps the
  // user from triggering reqs while just typing in a closed dropdown.
  useEffect(() => {
    if (!open) return;
    if (debounced.length < minChars) {
      setItems([]);
      return;
    }
    let cancelled = false;
    setLoading(true);
    fetcher(debounced)
      .then((rows) => {
        if (!cancelled) setItems(rows);
      })
      .catch(() => {
        if (!cancelled) setItems([]);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [debounced, open, fetcher, minChars]);

  // Close on click outside.
  useEffect(() => {
    function onDocClick(e: MouseEvent) {
      if (!wrapperRef.current) return;
      if (!wrapperRef.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", onDocClick);
    return () => document.removeEventListener("mousedown", onDocClick);
  }, []);

  return (
    <div className="relative" ref={wrapperRef}>
      <input
        className={className}
        value={value}
        onChange={(e) => {
          onChange(e.target.value);
          if (!disabledMessage) setOpen(true);
        }}
        onFocus={() => setOpen(true)}
        placeholder={placeholder}
        autoComplete="off"
      />
      {open && disabledMessage && (
        // Prerequisite-not-met state. Shown when the autocomplete is
        // gated on a previous field (e.g. product needs customer).
        // Italic + grey so it reads as advice, not an error.
        <div className="absolute z-10 left-0 right-0 mt-1 bg-yellow-50 border border-yellow-200 rounded-md px-3 py-2 text-xs text-yellow-800 italic">
          {disabledMessage}
        </div>
      )}
      {open && !disabledMessage && (
        <div className="absolute z-10 left-0 right-0 mt-1 bg-white border border-gray-200 rounded-md shadow-lg max-h-72 overflow-auto">
          {loading && (
            <div className="px-3 py-2 text-xs text-gray-500">Searching…</div>
          )}
          {!loading && items.length === 0 && (
            <div className="px-3 py-2 text-xs text-gray-500">{emptyMessage}</div>
          )}
          {items.map((it, i) => (
            <button
              type="button"
              key={i}
              onMouseDown={(e) => {
                // mouseDown fires before the input's blur, so the
                // selection lands BEFORE the dropdown auto-closes
                // — avoiding the classic "click vanishes" flicker.
                e.preventDefault();
                onPick(it);
                setOpen(false);
              }}
              className="block w-full text-left px-3 py-2 text-sm hover:bg-gray-100 border-b border-gray-100 last:border-0"
            >
              {renderItem(it)}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}


export default function Page() {
  return (
    <Suspense fallback={<div className="p-8">Loading…</div>}>
      <QuoteBuilder />
    </Suspense>
  );
}


function QuoteBuilder() {
  // Bot deep-link can pre-select the sales rep via ?sales=Jay etc.
  const params = useSearchParams();
  const initialSales = useMemo(() => {
    const raw = (params.get("sales") || "").trim();
    if (!raw) return "";
    // Case-insensitive match against known names.
    const match = SALES_NAMES.find((n) => n.toLowerCase() === raw.toLowerCase());
    return match || "";
  }, [params]);

  // Initial state: try localStorage first (refresh recovery), fall
  // back to a blank form pre-filled with the rep's sales name from
  // the ?sales=... URL param.
  const DRAFT_KEY = "npfoods-quote-draft-v1";
  const [data, setData] = useState<QuoteData>(() => blankQuote(initialSales));
  const [draftRestored, setDraftRestored] = useState(false);

  // Restore draft on mount (effect runs only on client, so window is safe).
  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(DRAFT_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw) as QuoteData;
      // Sanity check — only restore if it looks like a quote.
      if (parsed && typeof parsed === "object" && Array.isArray(parsed.products)) {
        setData(parsed);
        setDraftRestored(true);
      }
    } catch {
      // Malformed draft — ignore.
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Persist every change to localStorage. Debounced via the React
  // commit boundary — runs after every state-driven re-render, but
  // that's cheap (JSON.stringify of a few KB).
  useEffect(() => {
    try {
      window.localStorage.setItem(DRAFT_KEY, JSON.stringify(data));
    } catch {
      // Quota exceeded or storage disabled — silently ignore. Worst
      // case the rep loses their draft on refresh, same as before.
    }
  }, [data]);

  // If the param resolves after the initial render and no draft was
  // restored, sync the sales pre-fill from the URL.
  useEffect(() => {
    if (initialSales && !data.salesPerson && !draftRestored) {
      setData((d) => ({ ...d, salesPerson: initialSales }));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialSales]);

  // ---- helpers for nested products array ----
  function patch<K extends keyof QuoteData>(key: K, value: QuoteData[K]) {
    setData((d) => ({ ...d, [key]: value }));
  }

  function patchProduct(i: number, p: Partial<Product>) {
    setData((d) => ({
      ...d,
      products: d.products.map((row, idx) =>
        idx === i ? { ...row, ...p } : row,
      ),
    }));
  }

  function addProduct() {
    setData((d) => ({ ...d, products: [...d.products, blankProduct()] }));
  }

  function removeProduct(i: number) {
    setData((d) => ({
      ...d,
      products: d.products.length === 1
        ? d.products  // never remove the last row — keep at least one
        : d.products.filter((_, idx) => idx !== i),
    }));
  }

  // Auto-prefix "RE: Quotation for " when rep types a bare topic.
  function normaliseTitleOnBlur() {
    const t = (data.quotationTitle || "").trim();
    if (!t) return;
    const low = t.toLowerCase();
    if (low.startsWith("re:")) return;
    if (low.startsWith("quotation for")) {
      patch("quotationTitle", "RE: " + t);
      return;
    }
    patch("quotationTitle", `RE: Quotation for ${t}`);
  }

  // Quick presets for the validity date. Stored as ISO (YYYY-MM-DD)
  // so the native <input type="date"> can read/write the same field
  // round-trip; PDF rendering formats it as 'DD Month YYYY'.
  function presetValidity(daysAhead: number) {
    const sgt = new Date(Date.now() + (8 + 24 * daysAhead) * 60 * 60 * 1000);
    const iso = sgt.toISOString().slice(0, 10); // YYYY-MM-DD
    patch("validityDate", iso);
  }

  const filename = suggestedFilename(data);
  // Stricter readiness check: don't let reps download a PDF that's
  // missing customer-facing essentials. Each missing field surfaces
  // as a one-line nudge under the Generate button.
  const missingFields: string[] = [];
  if (!data.companyName.trim()) missingFields.push("Company name");
  if (!data.salesPerson) missingFields.push("Sales person");
  const hasValidProduct = data.products.some(
    (p) => p.name.trim() && p.price.trim(),
  );
  if (!hasValidProduct) missingFields.push("a product with name + price");
  const canGenerate = missingFields.length === 0;

  // Reset confirm-then-clear. Two-step UX so a stray click doesn't
  // discard a half-written quote.
  const [confirmReset, setConfirmReset] = useState(false);
  function startNewQuote() {
    setData(blankQuote(initialSales));
    productCacheRef.current.clear();
    setConfirmReset(false);
    setError(null);
    setDraftRestored(false);
    try { window.localStorage.removeItem(DRAFT_KEY); } catch {}
    // Scroll back to top so the rep starts cleanly on the customer block.
    if (typeof window !== "undefined") window.scrollTo({ top: 0, behavior: "smooth" });
  }

  // ---- Autocomplete fetchers ----
  // Each fetcher hits one of the /api/* routes. JSON shape matches
  // CustomerSuggestion / ProductSuggestion above. Errors swallow to
  // an empty list — the UI just shows 'No matches' instead of crashing.
  const fetchCustomers = useCallback(
    async (q: string): Promise<CustomerSuggestion[]> => {
      try {
        const r = await fetch(
          `/api/customers?q=${encodeURIComponent(q)}`,
          { cache: "no-store" },
        );
        if (!r.ok) return [];
        const j = (await r.json()) as { customers?: CustomerSuggestion[] };
        return j.customers || [];
      } catch {
        return [];
      }
    },
    [],
  );

  // Cache the products-for-customer call by customer name. Same
  // customer typically shows up on multiple product rows — we don't
  // want to re-fetch for each row. 30s TTL is plenty since the
  // server-side cache also dedupes.
  const productCacheRef = useRef<Map<string, { ts: number; items: ProductSuggestion[] }>>(new Map());
  const fetchProductsForCustomer = useCallback(
    async (q: string): Promise<ProductSuggestion[]> => {
      const customerName = data.companyName.trim();
      if (!customerName) return [];
      const cached = productCacheRef.current.get(customerName);
      const now = Date.now();
      let items: ProductSuggestion[];
      if (cached && now - cached.ts < 30_000) {
        items = cached.items;
      } else {
        try {
          const r = await fetch(
            `/api/customer-products?name=${encodeURIComponent(customerName)}`,
            { cache: "no-store" },
          );
          if (!r.ok) return [];
          const j = (await r.json()) as { products?: ProductSuggestion[] };
          items = j.products || [];
          productCacheRef.current.set(customerName, { ts: now, items });
        } catch {
          return [];
        }
      }
      // Client-side filter by the typed-in product name (server returns
      // up to 25 candidates regardless of the user's query).
      const ql = q.trim().toLowerCase();
      if (!ql) return items.slice(0, 12);
      return items
        .filter(
          (p) =>
            p.name.toLowerCase().includes(ql) ||
            p.code.toLowerCase().includes(ql),
        )
        .slice(0, 12);
    },
    [data.companyName],
  );

  function pickCustomer(c: CustomerSuggestion) {
    // Fill company name and pre-fill the address textarea from the
    // latest DHL ship-to capture (if we have one — see fsl.ts). Address
    // stays editable so the rep can tweak before generating the PDF.
    // We DON'T overwrite an address the rep has already typed — they
    // might have hand-customised it for this specific quote.
    productCacheRef.current.delete(data.companyName);
    setData((d) => ({
      ...d,
      companyName: c.name,
      customerAddress:
        d.customerAddress.trim() === "" && c.address
          ? c.address
          : d.customerAddress,
    }));
  }

  function pickProductForRow(i: number, p: ProductSuggestion) {
    // Extract price digits + currency from the FSL R&D Price string.
    // Examples seen in the wild: '4.53', 'USD 4.53', 'SGD 6.20',
    // '$4.50' — strip everything non-numeric/dot.
    const m = p.rdPrice.match(/[0-9]+(?:\.[0-9]+)?/);
    const price = m ? m[0] : "";
    const currencyMatch = p.rdPrice.toUpperCase().match(/USD|SGD/);
    const currency: Product["currency"] =
      currencyMatch?.[0] === "SGD" ? "SGD" : "USD";
    patchProduct(i, {
      name: p.name || p.code,
      code: p.code,
      price,
      currency,
    });
  }

  // On-click PDF generation. Imports the heavy modules lazily so the
  // initial page load stays small. Any failure (broken input, OOM,
  // network issue fetching logos) lands in `error` state and shows
  // an inline message — the page itself can't crash from this path.
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function handleGenerate() {
    if (!canGenerate || busy) return;
    setError(null);
    setBusy(true);
    try {
      const [{ pdf }, { QuotePDF }] = await Promise.all([
        import("@react-pdf/renderer"),
        import("@/lib/pdf"),
      ]);
      const origin =
        typeof window !== "undefined" ? window.location.origin : "";
      // Bridge: form stores validityDate as ISO (YYYY-MM-DD) so the
      // native <input type="date"> can round-trip it. The PDF wants
      // human-readable 'DD Month YYYY'. Convert at render time only;
      // don't mutate the stored form state.
      const dataForPdf: QuoteData = {
        ...data,
        validityDate: formatValidityForDisplay(data.validityDate),
      };
      const blob = await pdf(
        <QuotePDF data={dataForPdf} origin={origin} />
      ).toBlob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      // Revoke a tick later so Safari has time to actually trigger
      // the download before the URL goes invalid.
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Failed to generate PDF"
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="max-w-3xl mx-auto p-4 sm:p-8">
      <header className="mb-6">
        <div className="flex items-start justify-between gap-3">
          <div>
            <h1 className="text-2xl font-bold text-gray-900">
              📄 NP Foods — Quotation Builder
            </h1>
            <p className="text-sm text-gray-600 mt-1">
              Fill in the customer details and product lines, then tap{" "}
              <span className="font-semibold">Generate PDF</span> to download
              a print-ready A4 quotation.
            </p>
          </div>
          {/* Reset / start-new. Two-tap confirmation so a stray click
              doesn't wipe a half-finished quote. */}
          {confirmReset ? (
            <div className="flex items-center gap-2 shrink-0">
              <button
                type="button"
                onClick={startNewQuote}
                className="text-xs px-3 py-1.5 rounded-md bg-red-600 text-white font-semibold hover:opacity-90"
              >
                Confirm — discard
              </button>
              <button
                type="button"
                onClick={() => setConfirmReset(false)}
                className="text-xs px-3 py-1.5 rounded-md border bg-white hover:bg-gray-100"
              >
                Cancel
              </button>
            </div>
          ) : (
            <button
              type="button"
              onClick={() => setConfirmReset(true)}
              className="shrink-0 text-xs px-3 py-1.5 rounded-md border bg-white hover:bg-gray-100"
              title="Clear the form and start over"
            >
              🆕 New quotation
            </button>
          )}
        </div>
        {/* 'Draft restored' banner — shown for one session after a
            refresh restored saved state. Lets the rep know that the
            form ISN'T blank because of a bug, it's their own work. */}
        {draftRestored && (
          <div className="mt-3 p-2 rounded-md bg-blue-50 border border-blue-200 text-xs text-blue-800 flex items-center justify-between gap-2">
            <span>
              💾 Restored your draft from the last session. Tap{" "}
              <b>New quotation</b> above to start fresh.
            </span>
            <button
              type="button"
              onClick={() => setDraftRestored(false)}
              className="text-blue-700 hover:underline shrink-0"
            >
              Dismiss
            </button>
          </div>
        )}
      </header>

      {/* ---- Customer block ---- */}
      <section className="bg-white rounded-lg border border-gray-200 p-4 sm:p-5 mb-4">
        <h2 className="text-base font-semibold text-gray-800 mb-3">Customer details</h2>

        <Field
          label="Company name"
          hint="Start typing — I'll suggest customers from your past samples."
        >
          <Autocomplete<CustomerSuggestion>
            value={data.companyName}
            onChange={(s) => patch("companyName", s)}
            onPick={pickCustomer}
            placeholder="HURNG FUR FOODS FACTORY CO., LTD"
            fetcher={fetchCustomers}
            renderItem={(c) => (
              <div className="flex justify-between gap-3 items-baseline">
                <span className="font-medium">{c.name}</span>
                <span className="text-xs text-gray-500 whitespace-nowrap">
                  {c.country ? `${c.country} · ` : ""}
                  {c.productCount} product{c.productCount === 1 ? "" : "s"}
                </span>
              </div>
            )}
          />
        </Field>

        <Field label="Customer address" hint="Multi-line supported; line breaks are preserved.">
          <textarea
            rows={3}
            className="w-full border rounded-md px-3 py-2 text-sm"
            value={data.customerAddress}
            onChange={(e) => patch("customerAddress", e.target.value)}
            placeholder={"No. 268, Ln. 190, Dianyan Rd.,\nYangmei Dist., Taoyuan City 326, Taiwan"}
          />
        </Field>

        <Field label="Customer contact name" hint="The person addressed in the letter, e.g. Mr Tony Cheng">
          <input
            className="w-full border rounded-md px-3 py-2 text-sm"
            value={data.customerName}
            onChange={(e) => patch("customerName", e.target.value)}
            placeholder="Mr Tony Cheng"
          />
        </Field>

        <Field
          label="Quotation title"
          hint="Type the topic — I'll auto-prefix 'RE: Quotation for' when you click out of the box."
        >
          <input
            className="w-full border rounded-md px-3 py-2 text-sm"
            value={data.quotationTitle}
            onChange={(e) => patch("quotationTitle", e.target.value)}
            onBlur={normaliseTitleOnBlur}
            placeholder="Seasonings"
          />
        </Field>

        <Field
          label="Extra comment (optional)"
          hint="Appears under the standard greeting. Leave blank if not needed."
        >
          <textarea
            rows={2}
            className="w-full border rounded-md px-3 py-2 text-sm"
            value={data.extraComment}
            onChange={(e) => patch("extraComment", e.target.value)}
          />
        </Field>
      </section>

      {/* ---- Product rows ---- */}
      <section className="bg-white rounded-lg border border-gray-200 p-4 sm:p-5 mb-4">
        <h2 className="text-base font-semibold text-gray-800 mb-3">Products</h2>

        {data.products.map((p, i) => (
          <div
            key={i}
            className="grid grid-cols-12 gap-2 items-end mb-3 pb-3 border-b border-gray-100 last:border-0 last:pb-0 last:mb-0"
          >
            <div className="col-span-12 sm:col-span-5">
              <label className="text-xs font-medium text-gray-600">
                Product name
                {data.companyName ? (
                  <span className="ml-1 text-[10px] text-gray-400">
                    (suggesting from {data.companyName})
                  </span>
                ) : null}
              </label>
              <Autocomplete<ProductSuggestion>
                value={p.name}
                onChange={(s) => patchProduct(i, { name: s })}
                onPick={(pr) => pickProductForRow(i, pr)}
                placeholder="SOUR CHILLI SEASONING"
                fetcher={fetchProductsForCustomer}
                className="border rounded-md px-2 py-1.5 text-sm w-full"
                minChars={0}
                emptyMessage={
                  data.companyName.trim()
                    ? `No products in the FSL for ${data.companyName.trim()} yet — type the product name manually.`
                    : "Type a product name to search."
                }
                disabledMessage={
                  data.companyName.trim()
                    ? undefined
                    : "👆 Pick a customer in Company name first — suggestions are based on what you've previously sent them."
                }
                renderItem={(pr) => (
                  <div className="flex justify-between gap-3 items-baseline">
                    <span className="flex-1 truncate">
                      <span className="font-medium">{pr.name || pr.code}</span>
                      <span className="ml-2 text-xs text-gray-500">
                        {pr.code}
                      </span>
                    </span>
                    <span className="text-xs text-gray-500 whitespace-nowrap">
                      {pr.rdPrice ? `${pr.rdPrice} · ` : ""}
                      {pr.count}×
                    </span>
                  </div>
                )}
              />
            </div>
            <div className="col-span-6 sm:col-span-2">
              <label className="text-xs font-medium text-gray-600">Code</label>
              <input
                className="border rounded-md px-2 py-1.5 text-sm w-full"
                value={p.code}
                onChange={(e) => patchProduct(i, { code: e.target.value })}
                placeholder="S-Y3LL1"
              />
            </div>
            <div className="col-span-3 sm:col-span-2">
              <label className="text-xs font-medium text-gray-600">Currency</label>
              <select
                className="border rounded-md px-2 py-1.5 text-sm w-full bg-white"
                value={p.currency}
                onChange={(e) =>
                  patchProduct(i, { currency: e.target.value as Product["currency"] })
                }
              >
                {CURRENCY_OPTIONS.map((c) => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
            </div>
            <div className="col-span-3 sm:col-span-3">
              <label className="text-xs font-medium text-gray-600">Price / Kg</label>
              <input
                className="border rounded-md px-2 py-1.5 text-sm w-full"
                value={p.price}
                onChange={(e) => patchProduct(i, { price: e.target.value })}
                placeholder="5.50"
                inputMode="decimal"
              />
            </div>
            <div className="col-span-12 flex justify-end">
              <button
                type="button"
                onClick={() => removeProduct(i)}
                disabled={data.products.length === 1}
                className="text-xs text-red-600 hover:underline disabled:text-gray-300 disabled:cursor-not-allowed py-1"
                title={data.products.length === 1 ? "Keep at least one row" : "Remove this row"}
              >
                ✕ Remove row
              </button>
            </div>
          </div>
        ))}

        <button
          type="button"
          onClick={addProduct}
          className="mt-2 inline-flex items-center gap-1 px-3 py-1.5 rounded-md bg-npOrange text-white text-sm font-semibold hover:opacity-90"
        >
          ➕ Add product row
        </button>
      </section>

      {/* ---- Terms & remarks ---- */}
      <section className="bg-white rounded-lg border border-gray-200 p-4 sm:p-5 mb-4">
        <h2 className="text-base font-semibold text-gray-800 mb-3">Terms</h2>

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-4">
          <Field label="Packaging size">
            <select
              className="w-full border rounded-md px-3 py-2 text-sm bg-white"
              value={data.packagingSize}
              onChange={(e) => patch("packagingSize", e.target.value)}
            >
              {PACKAGING_OPTIONS.map((o) => (
                <option key={o} value={o}>{o}</option>
              ))}
            </select>
          </Field>

          <Field label="Payment term">
            <select
              className="w-full border rounded-md px-3 py-2 text-sm bg-white"
              value={data.paymentTerm}
              onChange={(e) => patch("paymentTerm", e.target.value)}
            >
              {PAYMENT_OPTIONS.map((o) => (
                <option key={o} value={o}>{o}</option>
              ))}
            </select>
          </Field>

          <Field label="Shipping Incoterms">
            <select
              className="w-full border rounded-md px-3 py-2 text-sm bg-white"
              value={
                // Migrate legacy 'EXWORK' (older drafts in localStorage)
                // to the new short 'EXW' so the select shows a value.
                data.incoterm === "EXWORK" ? "EXW" : data.incoterm
              }
              onChange={(e) => patch("incoterm", e.target.value)}
            >
              {INCOTERM_OPTIONS.map((o) => (
                <option key={o} value={o}>{o}</option>
              ))}
            </select>
          </Field>

          <Field label="Destination port / city" hint="e.g. Keelung Port">
            <input
              className="w-full border rounded-md px-3 py-2 text-sm"
              value={data.port}
              onChange={(e) => patch("port", e.target.value)}
              placeholder="Keelung Port"
            />
          </Field>

          <Field
            label="MOQ"
            hint="Applies to the whole order — prints as one merged cell on the right of the product table."
          >
            <input
              className="w-full border rounded-md px-3 py-2 text-sm"
              value={data.moq}
              onChange={(e) => patch("moq", e.target.value)}
              placeholder="1000 Kgs"
            />
          </Field>
        </div>

        <Field
          label="Quotation validity"
          hint={
            data.validityDate
              ? `Will print as: ${formatValidityForDisplay(data.validityDate)}`
              : "Pick a date from the calendar or tap a preset below."
          }
        >
          <input
            type="date"
            className="w-full border rounded-md px-3 py-2 text-sm"
            value={data.validityDate}
            onChange={(e) => patch("validityDate", e.target.value)}
          />
          <div className="flex flex-wrap items-center gap-2 mt-2">
            <button type="button" onClick={() => presetValidity(7)}
              className="text-xs px-2 py-1 rounded-md border bg-gray-50 hover:bg-gray-100">
              + 1 week
            </button>
            <button type="button" onClick={() => presetValidity(14)}
              className="text-xs px-2 py-1 rounded-md border bg-gray-50 hover:bg-gray-100">
              + 2 weeks
            </button>
            <button type="button" onClick={() => presetValidity(30)}
              className="text-xs px-2 py-1 rounded-md border bg-gray-50 hover:bg-gray-100">
              + 1 month
            </button>
            <button type="button" onClick={() => presetValidity(60)}
              className="text-xs px-2 py-1 rounded-md border bg-gray-50 hover:bg-gray-100">
              + 2 months
            </button>
          </div>
        </Field>
      </section>

      {/* ---- Sales person ---- */}
      <section className="bg-white rounded-lg border border-gray-200 p-4 sm:p-5 mb-6">
        <h2 className="text-base font-semibold text-gray-800 mb-3">Signature</h2>
        <Field
          label="Sales person"
          hint={
            data.salesPerson
              ? `HP on letter: ${SALES_PEOPLE[data.salesPerson]?.hp ?? ""}`
              : "Pick the name that signs this quote."
          }
        >
          <select
            className="w-full border rounded-md px-3 py-2 text-sm bg-white"
            value={data.salesPerson}
            onChange={(e) => patch("salesPerson", e.target.value)}
          >
            <option value="">— pick a name —</option>
            {SALES_NAMES.map((n) => {
              const full = SALES_PEOPLE[n]?.fullName ?? n;
              // Show 'Jay (Jay Wong)' so reps see what their signature
              // line will actually read on the PDF.
              const label = full === n ? n : `${n} — ${full}`;
              return (
                <option key={n} value={n}>{label}</option>
              );
            })}
          </select>
        </Field>
      </section>

      {/* ---- Action buttons ---- */}
      <div className="flex flex-wrap items-center gap-3 mb-3">
        <button
          type="button"
          onClick={handleGenerate}
          disabled={!canGenerate || busy}
          className="inline-block px-5 py-2.5 rounded-md bg-npOrange text-white text-sm font-semibold hover:opacity-90 disabled:bg-gray-300 disabled:cursor-not-allowed"
        >
          {busy
            ? "Building PDF…"
            : "⬇ Generate & download PDF"}
        </button>
        <span className="text-xs text-gray-500">{filename}</span>
      </div>
      {/* Missing-fields list — explicit so reps know exactly what
          stops the Generate button from working. Hidden when ready. */}
      {!canGenerate && missingFields.length > 0 && !busy && (
        <div className="mb-3 p-3 rounded-md bg-amber-50 border border-amber-200 text-sm text-amber-900">
          <b>Still needed before you can generate:</b>
          <ul className="list-disc pl-5 mt-1 space-y-0.5">
            {missingFields.map((m) => (
              <li key={m}>{m}</li>
            ))}
          </ul>
        </div>
      )}
      {error ? (
        <div className="mb-10 p-3 rounded-md bg-red-50 border border-red-200 text-sm text-red-800">
          <b>Couldn&apos;t generate PDF:</b> {error}
        </div>
      ) : (
        <div className="mb-10" />
      )}
    </main>
  );
}
