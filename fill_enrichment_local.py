"""Fill blank 'Taste describe' / 'Category' cells in Full Sample Listing — FREE, local CPU.

Replaces the paid Haiku backfill. Runs the local Ollama model (see
local_llm.py) on this PC; nothing is sent to Anthropic.

WHY THIS SCRIPT EXISTS
----------------------
The bot on Railway can't reach this PC's Ollama, so when the weekday MMS sync
meets a brand-new product code it now leaves taste/category BLANK instead of
paying for a Haiku call. This script is the other half: run it whenever you
like, it fills those blanks in the sheet. Once a value is in the sheet, the
bot reads it from there forever (enrich.resolve_* checks the sheet first), so
each product is only ever generated once.

USAGE
-----
    python fill_enrichment_local.py --dry-run           # preview, writes nothing
    python fill_enrichment_local.py --limit 50          # do 50, then stop
    python fill_enrichment_local.py                     # fill every blank
    python fill_enrichment_local.py --tab "Jakarta Full Sample Listing"

Safe to interrupt (Ctrl-C) and re-run — it only ever touches blank cells, and
writes in batches as it goes, so completed work is never lost.
"""
from __future__ import annotations

import argparse
import sys
import time

import gspread

import config
import local_llm
import sheets

# Column letters in Full Sample Listing (see sheets.FSL_HEADER).
COL_TASTE_LETTER = "H"
COL_CAT_LETTER = "I"
WRITE_EVERY = 25  # flush to the sheet this often, so Ctrl-C loses ≤25 rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tab", default=sheets.FSL_TAB, help="worksheet name")
    ap.add_argument("--limit", type=int, default=0, help="max rows to fill (0 = all)")
    ap.add_argument("--dry-run", action="store_true", help="preview only, no writes")
    args = ap.parse_args()

    if not local_llm.available():
        print(f"ERROR: no Ollama at {config.OLLAMA_URL}.\n"
              f"Start it, then re-run. (Model: {config.OLLAMA_MODEL})")
        return 1
    print(f"Local model: {config.OLLAMA_MODEL}  (free, CPU-only, no Anthropic)")

    sh = sheets._open_seasoning_master()
    try:
        ws = sh.worksheet(args.tab)
    except gspread.WorksheetNotFound:
        print(f"ERROR: tab {args.tab!r} not found")
        return 1

    values = ws.get_all_values()
    if len(values) < 2:
        print("Nothing to do — tab is empty.")
        return 0
    hdr = sheets.FSL_HEADER
    i_code, i_name = hdr.index("Product Code"), hdr.index("Product Name")
    i_taste, i_cat = hdr.index("Taste describe"), hdr.index("Category")

    # Reuse answers within this run: the same code often appears on many rows.
    known_taste: dict[str, str] = {}
    known_cat: dict[str, str] = {}
    for r in values[1:]:
        def cell(i): return (r[i] if len(r) > i else "").strip()
        code = cell(i_code).upper()
        if not code:
            continue
        if cell(i_taste):
            known_taste.setdefault(code, cell(i_taste))
        if cell(i_cat):
            known_cat.setdefault(code, cell(i_cat))

    todo = []
    for row_n, r in enumerate(values[1:], start=2):
        def cell(i): return (r[i] if len(r) > i else "").strip()
        code, name = cell(i_code).upper(), cell(i_name)
        if not code or not name:
            continue
        if not cell(i_taste) or not cell(i_cat):
            todo.append((row_n, code, name, cell(i_taste), cell(i_cat)))
    if args.limit:
        todo = todo[: args.limit]

    print(f"{args.tab}: {len(values)-1} rows, {len(todo)} need filling"
          f"{' (limited)' if args.limit else ''}")
    if not todo:
        return 0

    updates: list[dict] = []
    filled = generated = 0
    t0 = time.time()

    def flush():
        nonlocal updates
        if updates and not args.dry_run:
            ws.batch_update(updates, value_input_option="RAW")
            sheets._invalidate_fsl_cache()
        updates = []

    try:
        for n, (row_n, code, name, cur_taste, cur_cat) in enumerate(todo, 1):
            # Another row of the same product already has the answer → free.
            taste = cur_taste or known_taste.get(code, "")
            cat = cur_cat or known_cat.get(code, "")
            if not taste or not cat:
                g_taste, g_cat = local_llm.describe_and_classify(name)
                generated += 1
                taste = taste or g_taste
                cat = cat or g_cat
                if taste:
                    known_taste.setdefault(code, taste)
                if cat:
                    known_cat.setdefault(code, cat)
            if not taste and not cat:
                continue
            if taste and not cur_taste:
                updates.append({"range": f"{COL_TASTE_LETTER}{row_n}", "values": [[taste]]})
            if cat and not cur_cat:
                updates.append({"range": f"{COL_CAT_LETTER}{row_n}", "values": [[cat]]})
            filled += 1
            rate = n / max(1e-9, time.time() - t0)
            eta = (len(todo) - n) / rate / 60
            print(f"[{n}/{len(todo)}] {code:14} {name[:34]:36} | {cat:20} | {taste[:34]:36}"
                  f" ETA {eta:.0f}m")
            if len(updates) >= WRITE_EVERY:
                flush()
    except KeyboardInterrupt:
        print("\nInterrupted — flushing what's done…")
    finally:
        flush()

    mins = (time.time() - t0) / 60
    print(f"\n{'DRY RUN — nothing written' if args.dry_run else 'Done'}: "
          f"{filled} rows filled ({generated} model calls, {filled-generated} reused) "
          f"in {mins:.1f} min. Cost: $0.00")
    return 0


if __name__ == "__main__":
    sys.exit(main())
