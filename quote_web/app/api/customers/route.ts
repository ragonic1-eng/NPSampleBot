// GET /api/customers?q=<text>
// Returns up to 10 customer matches for the autocomplete dropdown.
// Empty query returns the most-recently-active customers.

import { NextRequest, NextResponse } from "next/server";
import { searchCustomers } from "@/lib/fsl";

// Vercel: this route runs on the Node runtime (not Edge) because we
// need google-spreadsheet which uses Node built-ins. Cache stays warm
// across requests on the same instance.
export const runtime = "nodejs";

// Skip Next.js's static optimization — this route reads from Sheets
// at runtime, never at build time.
export const dynamic = "force-dynamic";

export async function GET(req: NextRequest) {
  const q = req.nextUrl.searchParams.get("q") || "";
  try {
    const results = await searchCustomers(q, 10);
    return NextResponse.json({ customers: results });
  } catch (err) {
    console.error("[/api/customers] error:", err);
    return NextResponse.json(
      { error: err instanceof Error ? err.message : String(err) },
      { status: 500 },
    );
  }
}
