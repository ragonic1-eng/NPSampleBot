"""Fill blank Taste describe / Category cells using the LOCAL AI — $0, no Anthropic.

Why this script exists
----------------------
Enrichment used to happen inside the Railway sync via Claude Haiku (paid).
Ollama runs on the user's PC at localhost, and Railway has no route to a home
machine, so the cloud bot can NEVER call it. Local enrichment therefore has to
be a batch job run HERE, which is exactly what this is.

What it does
------------
 1. Reads the three Full Sample Listing tabs (SG / Jakarta / Thailand).
 2. Collects every product code with a blank Taste describe or Category.
 3. Asks the local model once per unique CODE (not per row) — the same product
    appears in many rows, so this is typically ~2x fewer calls than rows.
 4. Writes the answers back to every row sharing that code, in batched
    Sheets updates.
 5. Appends a run record to the Obsidian vault so the second brain has the
    provenance of every value the AI wrote.

Usage
-----
    python enrich_local_batch.py --dry-run     # audit only, writes nothing
    python enrich_local_batch.py               # enrich + write Sheets + Obsidian
    python enrich_local_batch.py --limit 10    # try a small batch first

Safe to re-run: it only ever fills BLANK cells, never overwrites a value a
human (or an earlier run) already put there.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time

import gspread

import local_llm
import sheets

# Column positions inside FSL_HEADER (0-indexed).
COL_CODE = 3
COL_NAME = 4
COL_TASTE = 7
COL_CATEGORY = 8

# Obsidian second brain. Notes here are read by NPMarketingBot's specialists;
# `_`-prefixed files are skipped by that retrieval, which is what we want for
# an operational log — it's provenance for humans, not marketing knowledge.
VAULT = r"C:\Users\admin\Desktop\Claude\NPMarketingBot\knowledge"
VAULT_SUBDIR = "data"
NOTE_NAME = "np-sample-enrichment-log.md"

REGIONS = [
    ("Singapore", sheets.FSL_TAB),
    ("Jakarta", sheets.JAKARTA_FSL_TAB),
    ("Thailand", sheets.BANGKOK_FSL_TAB),
]


def _a1(row_idx: int, col_idx: int) -> str:
    """0-indexed (row, col) -> A1, accounting for the header row."""
    return f"{chr(ord('A') + col_idx)}{row_idx + 2}"


def collect_gaps(limit: int | None = None) -> tuple[dict, dict, dict]:
    """Return (work, stats, known).

    work:  {CODE: {"name": str, "cells": [(tab, a1, field), ...]}}
           Only rows whose Taste/Category cell is actually blank.
    known: {PRODUCT_NAME_UPPER: (taste, category)} built from rows that ALREADY
           have both values — the proven, human-reviewed house vocabulary.
    """
    work: dict[str, dict] = {}
    known: dict[str, tuple[str, str]] = {}
    stats = {"rows": 0, "blank_taste": 0, "blank_cat": 0}

    for label, tab in REGIONS:
        rows = sheets.load_fsl_rows_all(tab)
        stats["rows"] += len(rows)
        for i, r in enumerate(rows):
            code = str(r.get("Product Code", "")).strip().upper()
            name = str(r.get("Product Name", "")).strip()
            if not code or not name:
                continue
            taste = str(r.get("Taste describe", "")).strip()
            cat = str(r.get("Category", "")).strip()
            if taste and cat:
                known.setdefault(name.upper(), (taste, cat))
                continue
            entry = work.setdefault(code, {"name": name, "cells": []})
            if not taste:
                stats["blank_taste"] += 1
                entry["cells"].append((tab, _a1(i, COL_TASTE), "taste"))
            if not cat:
                stats["blank_cat"] += 1
                entry["cells"].append((tab, _a1(i, COL_CATEGORY), "category"))

    if limit is not None:
        work = dict(list(work.items())[:limit])
    return work, stats, known


def resolve(name: str, known: dict, choices: list) -> tuple[str, str, str]:
    """Best taste/category for `name`. Returns (taste, category, source).

    Proven data beats a small model, every time. Measured on this catalogue:
    the 4B model rendered "TOM YUM SEASONING" as "tomato — sweet-tangy"
    (it read Tom Yum as tomato) and filed "SINGAPORE LAKSA" under Snack, while
    the sheet already held the correct "Thai tom yum — sour-spicy lemongrass-
    lime-galangal" and "Noodle & Instant Soup". Reuse also keeps the column a
    consistent controlled vocabulary: identical product names MUST get
    identical descriptors, which only a lookup can guarantee.

    Order: exact name hit -> very-close fuzzy hit (>=95) -> local model.
    """
    from rapidfuzz import fuzz, process

    key = name.upper()
    if key in known:
        t, c = known[key]
        return t, c, "reuse-exact"

    m = process.extractOne(key, choices, scorer=fuzz.WRatio)
    if m and m[1] >= 95:
        t, c = known[m[0]]
        return t, c, f"reuse-fuzzy{m[1]:.0f}"

    t, c = local_llm.describe_and_classify(name)
    return t, c, "local-ai"


def write_obsidian(entries: list[dict], stats: dict, dry: bool) -> str | None:
    """Append this run to the vault log note. Returns the note path."""
    folder = os.path.join(VAULT, VAULT_SUBDIR)
    if not os.path.isdir(folder):
        print(f"  ! vault folder missing, skipping Obsidian: {folder}")
        return None
    path = os.path.join(folder, NOTE_NAME)
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    today = dt.date.today().isoformat()

    if not os.path.exists(path):
        header = (
            "---\n"
            "type: log\n"
            "description: Provenance for every Taste describe / Category value "
            "written by the local AI into the NP sample listing.\n"
            f"updated: {today}\n"
            "tags: [data, provenance, npsamplebot, local-ai]\n"
            "---\n\n"
            "# 🧪 NP Sample Listing — local-AI enrichment log\n\n"
            "Every row below was resolved **locally on the PC** "
            f"(Ollama · `{sheets.config.OLLAMA_MODEL}`) "
            "at $0 API cost — no Anthropic credit used.\n\n"
            "Source of truth is the *Sample Master List 2024-Present* Google Sheet; "
            "this note is the audit trail of what the AI filled in and when.\n\n"
            "Related: [[_BOT_BRAIN]]\n\n"
        )
    else:
        header = ""

    body = [f"\n## Run {now}\n"]
    body.append(
        f"- Scanned **{stats['rows']:,}** rows across SG / Jakarta / Thailand\n"
        f"- Filled **{stats['blank_taste']}** blank *Taste describe* cells and "
        f"**{stats['blank_cat']}** blank *Category* cells\n"
        f"- Unique products enriched: **{len(entries)}**\n"
        f"- Cost: **$0** (local CPU model, Anthropic not called)\n"
    )
    if dry:
        body.append("\n> ⚠️ DRY RUN — nothing was written to the sheet.\n")
    if entries:
        n_reuse = sum(1 for e in entries if str(e.get("source", "")).startswith("reuse"))
        n_ai = len(entries) - n_reuse
        body.append(
            f"- Provenance: **{n_reuse} reused** from existing reviewed rows, "
            f"**{n_ai} newly generated** by the local model\n"
        )
        body.append(
            "\n> ♻️ = copied from an existing row with the same product name "
            "(keeps the column a consistent controlled vocabulary, and is more "
            "accurate than a 4B model on culinary terms — it read *Tom Yum* as "
            "*tomato*).  🤖 = genuinely new product, generated locally.\n"
        )
        body.append("\n| Src | Product code | Product name | Taste describe | Category |\n")
        body.append("|---|---|---|---|---|\n")
        for e in entries:
            nm = e["name"].replace("|", "/")[:60]
            ts = (e.get("taste") or "—").replace("|", "/")[:70]
            ct = e.get("category") or "—"
            src = "♻️" if str(e.get("source", "")).startswith("reuse") else "🤖"
            body.append(f"| {src} | `{e['code']}` | {nm} | {ts} | {ct} |\n")

    with open(path, "a", encoding="utf-8") as f:
        f.write(header + "".join(body))
    return path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="audit only, write nothing")
    ap.add_argument("--limit", type=int, default=None, help="only process N codes")
    args = ap.parse_args()

    print("=== NP sample listing — LOCAL enrichment (no Claude credit) ===\n")
    if not local_llm.available():
        print("!! Ollama is not reachable. Start it, then re-run:")
        print("   ollama serve      (and: ollama pull <model>)")
        return 1
    print(f"local model OK  ({sheets.config.OLLAMA_MODEL} @ {sheets.config.OLLAMA_URL})\n")

    print("scanning sheets for blank Taste / Category cells…")
    work, stats, known = collect_gaps(args.limit)
    choices = list(known.keys())
    print(
        f"  rows scanned         : {stats['rows']:,}\n"
        f"  blank taste cells    : {stats['blank_taste']}\n"
        f"  blank cat cells      : {stats['blank_cat']}\n"
        f"  unique products      : {len(work)}\n"
        f"  proven vocab entries : {len(known):,}  (reused before asking the model)\n"
    )
    if not work:
        print("Nothing to do — every row already has taste + category. ✅")
        write_obsidian([], stats, args.dry_run)
        return 0

    # --- resolve: proven data first, local model only for genuinely new products ---
    entries: list[dict] = []
    updates_by_tab: dict[str, list[dict]] = {}
    src_counts: dict[str, int] = {}
    t0 = time.time()
    for n, (code, info) in enumerate(work.items(), 1):
        taste, category, source = resolve(info["name"], known, choices)
        src_counts[source.split("-")[0] + "-" + source.split("-")[1][:5]] = (
            src_counts.get(source.split("-")[0] + "-" + source.split("-")[1][:5], 0) + 1
        )
        entries.append({
            "code": code, "name": info["name"],
            "taste": taste, "category": category, "source": source,
        })
        tag = "♻" if source.startswith("reuse") else "🤖"
        print(f"  [{n}/{len(work)}] {tag} {code:14} {info['name'][:34]:36} -> {taste[:32]:34} | {category}")
        for tab, a1, field in info["cells"]:
            val = taste if field == "taste" else category
            if not val:
                continue
            updates_by_tab.setdefault(tab, []).append({"range": a1, "values": [[val]]})
    print(f"\nresolved in {time.time()-t0:.0f}s  (cost: $0 — no Anthropic call)")
    print("  sources:", ", ".join(f"{k}={v}" for k, v in sorted(src_counts.items())))

    # --- write back ---
    if args.dry_run:
        cells = sum(len(v) for v in updates_by_tab.values())
        print(f"\nDRY RUN — would update {cells} cells across {len(updates_by_tab)} tabs.")
    else:
        sh = sheets._open_seasoning_master()
        for tab, ups in updates_by_tab.items():
            ws = sh.worksheet(tab)
            for i in range(0, len(ups), 500):          # stay under API limits
                ws.batch_update(ups[i : i + 500], value_input_option="RAW")
            print(f"  wrote {len(ups):5} cells -> {tab}")
        sheets.invalidate_caches()

    note = write_obsidian(entries, stats, args.dry_run)
    if note:
        print(f"\n2nd brain updated: {note}")
    print("\nDone. ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
