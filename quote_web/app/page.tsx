"use client";

import { Suspense, useEffect, useMemo, useState } from "react";
import dynamic from "next/dynamic";
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

// @react-pdf/renderer touches the DOM at import time. Keep it off the
// server bundle to avoid SSR errors during Vercel build.
const PDFDownloadLink = dynamic(
  () => import("@react-pdf/renderer").then((m) => m.PDFDownloadLink),
  { ssr: false, loading: () => <ButtonShell>Loading PDF…</ButtonShell> },
);

// Lazy-loaded too — keeps the heavy PDF renderer out of the first paint.
const QuotePDFLazy = dynamic(
  () => import("@/lib/pdf").then((m) => ({ default: m.QuotePDF })),
  { ssr: false },
);


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

        <Field label="Company name" hint="e.g. HURNG FUR FOODS FACTORY CO., LTD">
          <input
            className="border rounded-md px-3 py-2 text-sm"
            value={data.companyName}
            onChange={(e) => patch("companyName", e.target.value)}
            placeholder="HURNG FUR FOODS FACTORY CO., LTD"
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
              <label className="text-xs font-medium text-gray-600">Product name</label>
              <input
                className="border rounded-md px-2 py-1.5 text-sm w-full"
                value={p.name}
                onChange={(e) => patchProduct(i, { name: e.target.value })}
                placeholder="SOUR CHILLI SEASONING"
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
      <div className="flex flex-wrap items-center gap-3 mb-10">
        {canGenerate ? (
          <PDFDownloadLink
            document={
              <QuotePDFLazy
                data={data}
                origin={typeof window !== "undefined" ? window.location.origin : ""}
              />
            }
            fileName={filename}
            className="inline-block px-5 py-2.5 rounded-md bg-npOrange text-white text-sm font-semibold hover:opacity-90"
          >
            {({ loading }) => (loading ? "Building PDF…" : "⬇ Generate & download PDF")}
          </PDFDownloadLink>
        ) : (
          <ButtonShell>Add a product with a name and price to enable</ButtonShell>
        )}

        <span className="text-xs text-gray-500">{filename}</span>
      </div>
    </main>
  );
}
