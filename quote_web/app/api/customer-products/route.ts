// GET /api/customer-products?name=<customer name>
// Returns up to 25 products sent to that customer, most-recent first.

import { NextRequest, NextResponse } from "next/server";
import { productsForCustomer } from "@/lib/fsl";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(req: NextRequest) {
  const name = req.nextUrl.searchParams.get("name") || "";
  if (!name.trim()) {
    return NextResponse.json({ products: [] });
  }
  try {
    const results = await productsForCustomer(name, 25);
    return NextResponse.json({ products: results });
  } catch (err) {
    console.error("[/api/customer-products] error:", err);
    return NextResponse.json(
      { error: err instanceof Error ? err.message : String(err) },
      { status: 500 },
    );
  }
}
