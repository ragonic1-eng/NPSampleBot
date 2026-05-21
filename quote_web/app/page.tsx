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
  className = "border rounded-md px-3 py-2 text-sm",
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

  const [data, setData] = useState<QuoteData>(() => blankQuote(initialSales));

  // If the param resolves after the initial render, sync it once.
  useEffect(() => {
    if (initialSales && !data.salesPerson) {
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

  // Quick presets for the validity date.
  function presetValidity(daysAhead: number) {
    const dt = new Date(Date.now() + (8 + 24 * daysAhead) * 60 * 60 * 1000);
    const human = dt.toLocaleDateString("en-GB", {
      day: "numeric",
      month: "long",
      year: "numeric",
      timeZone: "UTC",
    });
    patch("validityDate", human);
  }

  const filename = suggestedFilename(data);
  const canGenerate = data.products.some((p) => p.name && p.price);

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
    // Fill company name + we leave the customer address blank (FSL
    // doesn't have addresses — rep types it). Bump 'no products yet'
    // out of productCacheRef so the next product autocomplete fetches
    // the fresh customer's products.
    productCacheRef.current.delete(data.companyName);
    setData((d) => ({
      ...d,
      companyName: c.name,
      // If the rep hasn't typed a quotation title yet, suggest one
      // based on the customer's typical sales rep activity.
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
      const blob = await pdf(
        <QuotePDF data={data} origin={origin} />
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
        <h1 className="text-2xl font-bold text-gray-900">
          📄 NP Foods — Quotation Builder
        </h1>
        <p className="text-sm text-gray-600 mt-1">
          Fill in the customer details and product lines, then tap{" "}
          <span className="font-semibold">Generate PDF</span> to download
          a print-ready A4 quotation.
        </p>
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
            className="border rounded-md px-3 py-2 text-sm"
            value={data.customerAddress}
            onChange={(e) => patch("customerAddress", e.target.value)}
            placeholder={"No. 268, Ln. 190, Dianyan Rd.,\nYangmei Dist., Taoyuan City 326, Taiwan"}
          />
        </Field>

        <Field label="Customer contact name" hint="The person addressed in the letter, e.g. Mr Tony Cheng">
          <input
            className="border rounded-md px-3 py-2 text-sm"
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
            className="border rounded-md px-3 py-2 text-sm"
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
            className="border rounded-md px-3 py-2 text-sm"
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
            <div className="col-span-12 sm:col-span-4">
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
            <div className="col-span-3 sm:col-span-2">
              <label className="text-xs font-medium text-gray-600">Price / Kg</label>
              <input
                className="border rounded-md px-2 py-1.5 text-sm w-full"
                value={p.price}
                onChange={(e) => patchProduct(i, { price: e.target.value })}
                placeholder="5.50"
                inputMode="decimal"
              />
            </div>
            <div className="col-span-9 sm:col-span-2">
              <label className="text-xs font-medium text-gray-600">MOQ</label>
              <input
                className="border rounded-md px-2 py-1.5 text-sm w-full"
                value={p.moq}
                onChange={(e) => patchProduct(i, { moq: e.target.value })}
                placeholder="1000 Kgs"
              />
            </div>
            <div className="col-span-3 sm:col-span-12 flex justify-end">
              <button
                type="button"
                onClick={() => removeProduct(i)}
                disabled={data.products.length === 1}
                className="text-xs text-red-600 hover:underline disabled:text-gray-300 disabled:cursor-not-allowed"
                title={data.products.length === 1 ? "Keep at least one row" : "Remove this row"}
              >
                ✕ Remove
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
              className="border rounded-md px-3 py-2 text-sm bg-white"
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
              className="border rounded-md px-3 py-2 text-sm bg-white"
              value={data.paymentTerm}
              onChange={(e) => patch("paymentTerm", e.target.value)}
            >
              {PAYMENT_OPTIONS.map((o) => (
                <option key={o} value={o}>{o}</option>
              ))}
            </select>
          </Field>

          <Field label="Price basis (incoterm)">
            <select
              className="border rounded-md px-3 py-2 text-sm bg-white"
              value={data.incoterm}
              onChange={(e) => patch("incoterm", e.target.value)}
            >
              {INCOTERM_OPTIONS.map((o) => (
                <option key={o} value={o}>{o}</option>
              ))}
            </select>
          </Field>

          <Field label="Destination port / city" hint="e.g. Keelung Port">
            <input
              className="border rounded-md px-3 py-2 text-sm"
              value={data.port}
              onChange={(e) => patch("port", e.target.value)}
              placeholder="Keelung Port"
            />
          </Field>
        </div>

        <Field label="Quotation validity" hint="Pick a preset or type your own date.">
          <div className="flex flex-wrap items-center gap-2">
            <input
              className="border rounded-md px-3 py-2 text-sm flex-1 min-w-[200px]"
              value={data.validityDate}
              onChange={(e) => patch("validityDate", e.target.value)}
              placeholder="30 June 2026"
            />
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
            className="border rounded-md px-3 py-2 text-sm bg-white"
            value={data.salesPerson}
            onChange={(e) => patch("salesPerson", e.target.value)}
          >
            <option value="">— pick a name —</option>
            {SALES_NAMES.map((n) => (
              <option key={n} value={n}>{n}</option>
            ))}
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
            : canGenerate
              ? "⬇ Generate & download PDF"
              : "Add a product with a name and price to enable"}
        </button>
        <span className="text-xs text-gray-500">{filename}</span>
      </div>
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
