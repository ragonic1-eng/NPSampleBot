"""OCR for product-code photos — RapidOCR/Tesseract first, Haiku vision fallback.

Cost-aware ladder (cheapest path that solves the problem):

  1) **RapidOCR (free, preferred)** — ONNX port of PaddleOCR's PP-OCR
     models. Unlike Tesseract it does scene-text DETECTION (finds the code
     region in a cluttered, angled phone photo) before recognition, which
     is where Tesseract loses most of its accuracy. Falls back silently to
     Tesseract when the package isn't installed.

  2) **Tesseract (free, fallback engine)** — local OCR with image
     preprocessing. Used when RapidOCR isn't available or found nothing.

  3) **Catalog-aware self-healing (free)** — after OCR, every detected code
     that isn't in the seasoning master gets repaired in two passes:
       a. variant search: swap each ambiguous char (B↔8, O↔0, S↔5, I↔1,
          Z↔2, G↔6, D↔0, A↔4) and look up the swapped code.
       b. edit-distance snap: find catalog codes within Levenshtein
          distance ≤2 (same prefix, similar length). Distance-1 with a
          unique winner auto-corrects; distance-2 additionally requires
          every edit to be visually plausible (confusable char pair, or
          a doubled-char insert/delete like 844→B4). Ties never auto-fix —
          they surface as tap-to-pick suggestions instead.
     The catalog IS the dictionary — every code added to the master sheet
     makes the healer smarter, no retraining needed.

  4) **Claude Haiku 4.5 (paid, escalation)** — used when local OCR returns
     NOTHING, and also when local OCR returned codes that still fail
     catalog validation after healing (a suspect read). The two reads are
     merged, preferring catalog-validated codes. Skipped entirely when the
     free path fully validates, so most scans stay $0.

Public API:
    scan_image(img_bytes: bytes, catalog_codes: set[str]) -> ScanResult
"""
from __future__ import annotations

import asyncio
import base64
import logging
import re
from dataclasses import dataclass, field
from itertools import product
from typing import Iterable

from rapidfuzz.distance import Levenshtein

import config

log = logging.getLogger(__name__)

# Match seasoning codes from OCR output. V1.12.1: aligned with bot.py's
# _PP_CODE_RE — [SJTB]- + 3+ alnum + up to 6 suffix segments of up to 6
# chars each. Was previously S-only with one short suffix; that truncated
# multi-segment codes like B-39HA1-23-02 → B-39HA1-23 and silently
# returned the wrong product. Same prefix set as bot.py so OCR and code-
# entry behave identically across S- (Singapore), B- (legacy Singapore),
# J- (Indonesia), T- (Thailand, when ready).
_CODE_RE = re.compile(
    r"\b[SJTB]-[A-Za-z0-9]{3,}(?:-[A-Za-z0-9]{1,6}){0,6}\b",
    re.IGNORECASE,
)

# Per-character ambiguity table. Each set is "chars Sonnet might confuse with
# this one." We try every swap when a code fails catalog validation.
# Keep these CONSERVATIVE — adding too many pairs explodes the variant space.
_AMBIGUOUS = {
    "B": ["8"], "8": ["B"],
    "O": ["0", "Q", "D"], "0": ["O", "D", "Q"], "D": ["0", "O"],
    "Q": ["0", "O"],
    "I": ["1", "L"], "1": ["I", "L"], "L": ["1", "I"],
    "S": ["5"], "5": ["S"],
    "Z": ["2"], "2": ["Z"],
    "G": ["6"], "6": ["G"],
    # V1.17.x — real-world misread from a rep's photo: S-44XG1 for
    # S-4AXG1. Open-top 4s look like A on thermal/small print.
    "A": ["4"], "4": ["A"],
}

# Substitution pairs accepted as "visually plausible" when validating a
# distance-2 snap (see _distance_snap). Broader than _AMBIGUOUS — these
# don't drive variant generation (no explosion risk), they only gate
# whether an already-unique 2-edit candidate is believable as an OCR slip.
_PLAUSIBLE_SUBS: set[tuple[str, str]] = set()
for _k, _vals in _AMBIGUOUS.items():
    for _v in _vals:
        _PLAUSIBLE_SUBS.add((_k, _v))
        _PLAUSIBLE_SUBS.add((_v, _k))
_PLAUSIBLE_SUBS |= {
    ("3", "4"), ("4", "3"),     # this photo: S-643G1 read for S-633G1
    ("1", "7"), ("7", "1"),
    ("3", "8"), ("8", "3"),
    ("5", "6"), ("6", "5"),
    ("C", "G"), ("G", "C"),
    ("E", "F"), ("F", "E"),
    ("M", "N"), ("N", "M"),
    ("U", "V"), ("V", "U"),
    ("K", "X"), ("X", "K"),
    ("P", "R"), ("R", "P"),
    ("T", "Y"), ("Y", "T"),
    ("H", "N"), ("N", "H"),
    ("3", "9"), ("9", "3"),
}
# Stop variant explosion: at most this many ambiguous chars per code
# (2^N expansion otherwise). Most MMS codes have ≤ 8 chars after `S-`.
_MAX_VARIANT_SLOTS = 6
_MAX_VARIANTS = 64

_HAIKU_MODEL = "claude-haiku-4-5"  # cheapest model that supports vision

# Prompt used for the Haiku fallback. Emphasises the character pairs we know
# trip OCR up so Haiku slows down on those.
_VISION_PROMPT = """\
This image contains one or more **product codes** from an internal manufacturing
system. Codes ALWAYS look like one of:

    S-XXXXX        Singapore / International (e.g. S-62RG3, S-Y9KY2)
    S-XXXXX-NN     Singapore variant         (e.g. S-62RG3-19, S-S7CG5-61)
    J-XXXXX        Indonesia / Jakarta       (e.g. J-61TS2, J-B3681)
    J-XXXXX-NN     Indonesia variant         (e.g. J-61TS2-22-01, J-B3681-04)
    B-XXXXX        Thailand / Bangkok        (e.g. B-A2K91, B-1UL1)
    B-XXXXX-NN     Thailand variant          (e.g. B-A2K91-03)
    T-XXXXX        Thailand (legacy)         (rare)

Every code starts with one literal prefix: `S-`, `J-`, `B-`, or `T-`. The body
after the dash is 3-10 alphanumeric characters and may contain a SECOND dash
followed by a 1-4 char suffix (and rarely a third).

Do NOT silently rewrite a `J-` to `S-` or a `B-` to `S-` because Singapore codes
are more common — the prefix is part of the printed material; read what's there.

Read the image and list **every** product code you can see, one per line, in
UPPERCASE. Do not output anything else — no explanations, no quotes, no preamble.

⚠️ READ CAREFULLY — phone photos of small printed text confuse these character
pairs constantly. Look at each character at least twice and do NOT guess:

    • B vs 8        (B has two bumps; 8 has two closed loops)
    • O vs 0        (0 is narrower / has a slash on some printers)
    • S vs 5        (5 has a flat top; S has a curve top)
    • I vs 1 vs L   (1 has a serif foot on most printers)
    • Z vs 2        (2 has a curved bottom; Z is angular)
    • G vs 6        (G has an inner bar; 6 is a closed loop)
    • D vs 0        (D is angular on the left)

If a character is genuinely ambiguous between two options, prefer the one that
looks more likely on the printed material. Output each unique code on its own
line.

If you cannot read any code clearly, output the single word: NONE
"""


@dataclass
class ScanResult:
    codes: list[str]                    # final, possibly auto-corrected codes
    raw_codes: list[str]                # exactly what OCR returned (uppercased)
    corrections: dict[str, str]         # raw → corrected, only when changed
    unmatched: list[str]                # codes we couldn't validate against catalog
    source: str = "none"                # "rapidocr" | "tesseract" | "haiku" | "<local>+haiku" | "none"
    tokens_in: int = 0
    tokens_out: int = 0
    # For unmatched codes: near-miss catalog codes we found but weren't
    # confident enough to auto-apply (ties, implausible edits). The bot
    # shows these as tap-to-pick suggestions. raw code → [candidates].
    suggestions: dict[str, list[str]] = field(default_factory=dict)


def _client():
    if not config.ANTHROPIC_API_KEY:
        return None
    try:
        from anthropic import Anthropic
        return Anthropic(api_key=config.ANTHROPIC_API_KEY)
    except Exception as e:  # noqa: BLE001
        log.warning("Anthropic client init failed: %s", e)
        return None


def _detect_media_type(img_bytes: bytes) -> str:
    if img_bytes[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if img_bytes[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if img_bytes[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if img_bytes[:4] == b"RIFF" and img_bytes[8:12] == b"WEBP":
        return "image/webp"
    return "image/jpeg"  # Telegram serves jpegs by default


def _extract_codes(raw_text: str) -> list[str]:
    """Parse Sonnet's free-text reply into canonical uppercase codes."""
    if raw_text.strip().upper() == "NONE":
        return []
    found = list(_CODE_RE.findall(raw_text))
    seen: set[str] = set()
    out: list[str] = []
    for c in found:
        cu = c.upper()
        if cu not in seen:
            seen.add(cu)
            out.append(cu)
    return out


def _generate_variants(code: str) -> Iterable[str]:
    """Yield each code variant produced by swapping ambiguous characters.

    Only chars in `_AMBIGUOUS` are flipped — and we cap the slot count to keep
    the explosion bounded. Yields the original first; callers should ignore it
    if they don't want to re-test what they already tried.
    """
    chars = list(code)
    # Find ambiguous slot indexes.
    slots = [i for i, c in enumerate(chars) if c in _AMBIGUOUS]
    if not slots:
        yield code
        return
    if len(slots) > _MAX_VARIANT_SLOTS:
        # Keep the first N slots; rest stay fixed. Avoids 2^N explosion.
        slots = slots[:_MAX_VARIANT_SLOTS]
    # For each slot, the candidate set is {original_char, *ambiguous_alternates}.
    options = [[chars[i]] + _AMBIGUOUS.get(chars[i], []) for i in slots]
    count = 0
    for combo in product(*options):
        # Skip pure original (caller already tried it); but only if everything matched.
        if list(combo) == [chars[i] for i in slots]:
            yield code
            continue
        new_chars = chars.copy()
        for slot_idx, ch in zip(slots, combo):
            new_chars[slot_idx] = ch
        count += 1
        if count > _MAX_VARIANTS:
            return
        yield "".join(new_chars)


def _plausible_edit_ops(raw: str, cand: str) -> bool:
    """True when every edit turning `raw` into `cand` is a believable OCR slip.

    Believable = substitution of a visually-confusable pair, an
    insert/delete of a char that duplicates its neighbour (double-strike
    artifact, e.g. reading '844' where 'B4' is printed), or a dropped /
    phantom hyphen.
    """
    for op, spos, dpos in Levenshtein.editops(raw, cand):
        if op == "replace":
            if (raw[spos], cand[dpos]) not in _PLAUSIBLE_SUBS:
                return False
        elif op == "delete":
            ch = raw[spos]
            neighbours = raw[max(0, spos - 1):spos] + raw[spos + 1:spos + 2]
            if ch != "-" and ch not in neighbours:
                return False
        elif op == "insert":
            ch = cand[dpos]
            neighbours = cand[max(0, dpos - 1):dpos] + cand[dpos + 1:dpos + 2]
            if ch != "-" and ch not in neighbours:
                return False
    return True


def _distance_snap(
    raw: str, catalog_codes: set[str]
) -> tuple[str | None, list[str]]:
    """Second-chance healer: nearest catalog code by edit distance.

    Complements the ambiguity-table variant pass — that pass only handles
    known char swaps; this one handles ANY single-char slip plus plausible
    two-edit slips (e.g. S-844AJ1 → S-B4AJ1: 8→B swap + doubled-4 drop).

    Rules (conservative on purpose — a wrong snap shows the wrong price):
      • candidates share the prefix letter and differ in length by ≤1
      • distance 1 + a UNIQUE winner        → snap
      • distance 2 + unique + plausible ops → snap
      • ties or implausible edits           → no snap; return candidates
        so the bot can show "did you mean" buttons instead.

    Returns (snapped_code | None, close_candidates).
    """
    if "-" not in raw:
        return None, []
    prefix = raw.split("-", 1)[0]
    d1: list[str] = []
    d2: list[str] = []
    for c in catalog_codes:
        if not c or c.split("-", 1)[0] != prefix or abs(len(c) - len(raw)) > 1:
            continue
        d = Levenshtein.distance(raw, c, score_cutoff=2)
        if d == 1:
            d1.append(c)
        elif d == 2:
            d2.append(c)
    d1.sort()
    d2.sort()
    if len(d1) == 1:
        return d1[0], []
    if d1:  # 2+ codes equally close — never guess between twins
        return None, d1[:3]
    plausible_d2 = [c for c in d2 if _plausible_edit_ops(raw, c)]
    if len(plausible_d2) == 1:
        return plausible_d2[0], []
    candidates = plausible_d2 or d2
    return None, candidates[:3]


def _heal_against_catalog(
    raw_codes: list[str], catalog_codes: set[str]
) -> tuple[list[str], dict[str, str], list[str], dict[str, list[str]]]:
    """Snap each raw code to a real catalog code when possible.

    Returns:
        final_codes: what to actually use (original or corrected)
        corrections: raw → corrected mapping (only when changed)
        unmatched: codes we couldn't snap to anything in the catalog
        suggestions: unmatched raw → close-but-not-certain catalog codes
    """
    final: list[str] = []
    corrections: dict[str, str] = {}
    unmatched: list[str] = []
    suggestions: dict[str, list[str]] = {}

    if not catalog_codes:
        return list(raw_codes), {}, list(raw_codes), {}

    for raw in raw_codes:
        if raw in catalog_codes:
            final.append(raw)
            continue
        # Pass 1: ambiguity-table variants — the FIRST catalog hit wins
        # (variants iterated in a deterministic order from _generate_variants).
        snapped = None
        for v in _generate_variants(raw):
            if v == raw:
                continue
            if v in catalog_codes:
                snapped = v
                break
        # Pass 2: edit-distance snap (any 1-edit slip, plausible 2-edit slips).
        close: list[str] = []
        if snapped is None:
            snapped, close = _distance_snap(raw, catalog_codes)
        if snapped is not None:
            final.append(snapped)
            corrections[raw] = snapped
        else:
            # Keep the raw code so /pp can still try it (and log Not Found).
            final.append(raw)
            unmatched.append(raw)
            if close:
                suggestions[raw] = close
    return final, corrections, unmatched, suggestions


# RapidOCR engine is a lazy module-level singleton: first call loads the
# ONNX models (~1-2 s, ~20 MB), subsequent scans reuse the warm engine.
_rapidocr_engine = None
_rapidocr_unavailable = False


def _get_rapidocr():
    global _rapidocr_engine, _rapidocr_unavailable
    if _rapidocr_unavailable or config.DISABLE_RAPIDOCR:
        return None
    if _rapidocr_engine is None:
        try:
            from rapidocr import RapidOCR  # v2/v3 package name
            _rapidocr_engine = RapidOCR()
        except Exception as e:  # noqa: BLE001 — ImportError, model download failure, …
            log.info("RapidOCR unavailable (%s); using Tesseract path", e)
            _rapidocr_unavailable = True
            return None
    return _rapidocr_engine


def _rapidocr_extract(img_bytes: bytes) -> list[str]:
    """Scene-text OCR via RapidOCR (PP-OCR ONNX models). Free, local.

    Feeds the original colour photo — RapidOCR's DB detector handles
    angle / clutter / lighting itself, so no preprocessing needed.
    """
    eng = _get_rapidocr()
    if eng is None:
        return []
    try:
        import io as _io

        import numpy as np
        from PIL import Image

        img = Image.open(_io.BytesIO(img_bytes)).convert("RGB")
        arr = np.asarray(img)[:, :, ::-1]  # RGB → BGR (OpenCV convention)
        result = eng(arr)
        # v3.x returns a RapidOCROutput with .txts; v1.x returned
        # ([[box, text, score], ...], elapse). Handle both.
        txts = getattr(result, "txts", None)
        if txts is None and isinstance(result, tuple):
            rows = result[0] or []
            txts = [r[1] for r in rows if len(r) > 1]
        text = "\n".join(txts or [])
    except Exception as e:  # noqa: BLE001
        log.info("RapidOCR failed: %s", e)
        return []
    return _extract_codes(text)


def _tesseract_extract(img_bytes: bytes) -> list[str]:
    """Local Tesseract OCR — free. Returns codes or [] if unavailable / nothing useful."""
    try:
        import pytesseract
        from PIL import Image, ImageOps, ImageFilter
    except ImportError as e:
        log.info("Tesseract path unavailable (%s); will use Haiku fallback", e)
        return []
    try:
        import io as _io
        img = Image.open(_io.BytesIO(img_bytes))
        # Phone photos of small printed text need preprocessing — grayscale,
        # autocontrast, sharpen, then upscale if the image is small.
        img = img.convert("L")
        img = ImageOps.autocontrast(img, cutoff=2)
        img = img.filter(ImageFilter.SHARPEN)
        if min(img.size) < 800:
            scale = max(2, 800 // min(img.size))
            img = img.resize((img.size[0] * scale, img.size[1] * scale))
        # PSM 11 = sparse text; whitelist the characters MMS codes use.
        cfg = (
            "--psm 11 "
            "-c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz0123456789-"
        )
        raw = pytesseract.image_to_string(img, config=cfg)
    except Exception as e:  # noqa: BLE001 — pytesseract.TesseractNotFound etc.
        log.info("Tesseract OCR failed: %s", e)
        return []
    return _extract_codes(raw)


def _haiku_extract(img_bytes: bytes) -> tuple[list[str], int, int]:
    """Paid fallback — Claude Haiku 4.5 vision. Returns (codes, tokens_in, tokens_out)."""
    c = _client()
    if c is None:
        return [], 0, 0
    media_type = _detect_media_type(img_bytes)
    b64 = base64.standard_b64encode(img_bytes).decode("ascii")
    try:
        resp = c.messages.create(
            model=_HAIKU_MODEL,
            max_tokens=400,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": media_type,
                                "data": b64,
                            },
                        },
                        {"type": "text", "text": _VISION_PROMPT},
                    ],
                }
            ],
        )
        text = "".join(
            b.text for b in resp.content if getattr(b, "type", None) == "text"
        ).strip()
    except Exception as e:  # noqa: BLE001
        log.warning("Haiku vision OCR failed: %s", e)
        return [], 0, 0
    return _extract_codes(text), resp.usage.input_tokens, resp.usage.output_tokens


def _merge_haiku_read(
    l_raw: list[str],
    l_final: list[str],
    l_corr: dict[str, str],
    l_unmatched: list[str],
    l_sugg: dict[str, list[str]],
    h_raw: list[str],
    catalog_codes: set[str],
) -> tuple[list[str], list[str], dict[str, str], list[str], dict[str, list[str]]]:
    """Merge a Haiku re-read into a suspect local OCR read.

    Strategy: each unvalidated local code looks for its Haiku counterpart —
    a code within edit distance ≤2 (same printed line, read differently).
    A validated counterpart replaces the local code (recorded as a
    correction). Haiku codes with no local counterpart are appended (local
    OCR missed that line entirely). Validated local codes are never touched.
    """
    h_final, _h_corr, h_unmatched, h_sugg = _heal_against_catalog(h_raw, catalog_codes)

    used_h: set[int] = set()

    def _find_counterpart(raw: str, final: str) -> int | None:
        best_i: int | None = None
        best_d = 3
        for i, (hr, hf) in enumerate(zip(h_raw, h_final)):
            if i in used_h:
                continue
            d = min(
                Levenshtein.distance(final, hf, score_cutoff=2),
                Levenshtein.distance(final, hr, score_cutoff=2),
                Levenshtein.distance(raw, hf, score_cutoff=2),
                Levenshtein.distance(raw, hr, score_cutoff=2),
            )
            if d < best_d:
                best_d, best_i = d, i
        return best_i if best_d <= 2 else None

    out_raw: list[str] = []
    out_final: list[str] = []
    corrections = dict(l_corr)
    unmatched: list[str] = []
    suggestions: dict[str, list[str]] = {}

    for raw, final in zip(l_raw, l_final):
        if final not in l_unmatched:
            out_raw.append(raw)
            out_final.append(final)
            continue
        hi = _find_counterpart(raw, final)
        if hi is not None:
            used_h.add(hi)
            h_code = h_final[hi]
            if h_code not in h_unmatched:
                # Haiku's read validates against the catalog — trust it.
                out_raw.append(raw)
                out_final.append(h_code)
                if h_code != raw:
                    corrections[raw] = h_code
                suggestions.pop(raw, None)
                continue
        # No counterpart, or Haiku's read is unvalidated too — keep the
        # local read (MMS may know codes the catalog sheet doesn't yet).
        out_raw.append(raw)
        out_final.append(final)
        unmatched.append(final)
        if raw in l_sugg:
            suggestions[raw] = l_sugg[raw]

    # Codes Haiku saw that local OCR missed entirely.
    for i, (hr, hf) in enumerate(zip(h_raw, h_final)):
        if i in used_h or hf in out_final:
            continue
        out_raw.append(hr)
        out_final.append(hf)
        if hf != hr:
            corrections[hr] = hf
        if hf in h_unmatched:
            unmatched.append(hf)
            if hr in h_sugg:
                suggestions[hr] = h_sugg[hr]

    return out_raw, out_final, corrections, unmatched, suggestions


async def scan_image(
    img_bytes: bytes, catalog_codes: set[str] | None = None
) -> ScanResult:
    """OCR a photo: local engines first (free), Haiku when needed, heal against catalog.

    `catalog_codes` should be the set of canonical uppercase codes from the
    seasoning master sheet. Pass an empty set to skip self-healing.
    """
    catalog_codes = catalog_codes or set()

    # 1) Free path — RapidOCR (scene-text detection) then Tesseract.
    raw_codes = await asyncio.to_thread(_rapidocr_extract, img_bytes)
    source = "rapidocr" if raw_codes else "none"
    if not raw_codes:
        raw_codes = await asyncio.to_thread(_tesseract_extract, img_bytes)
        if raw_codes:
            source = "tesseract"
    tin = tout = 0

    # 2) Paid fallback — Haiku, only if local OCR returned nothing
    if not raw_codes:
        raw_codes, tin, tout = await asyncio.to_thread(_haiku_extract, img_bytes)
        if raw_codes:
            source = "haiku"

    final, corrections, unmatched, suggestions = _heal_against_catalog(
        raw_codes, catalog_codes
    )

    # 3) V1.17.x — validation-driven escalation. Local OCR read something
    # but ≥1 code still fails catalog validation after healing: the read is
    # suspect, so pay for ONE Haiku re-read and merge, preferring whichever
    # read validates. This is what rescues photos where the free engine
    # confidently reads the wrong twin (e.g. S-844AJ1 for S-B4AJ1).
    if unmatched and catalog_codes and source in ("rapidocr", "tesseract"):
        h_raw, tin, tout = await asyncio.to_thread(_haiku_extract, img_bytes)
        if h_raw:
            raw_codes, final, corrections, unmatched, suggestions = _merge_haiku_read(
                raw_codes, final, corrections, unmatched, suggestions,
                h_raw, catalog_codes,
            )
            source = f"{source}+haiku"

    log.info(
        "scan_image source=%s raw=%s corrections=%s unmatched=%s suggestions=%s",
        source, raw_codes, corrections, unmatched, suggestions,
    )
    return ScanResult(
        codes=final,
        raw_codes=raw_codes,
        corrections=corrections,
        unmatched=unmatched,
        source=source,
        tokens_in=tin,
        tokens_out=tout,
        suggestions=suggestions,
    )
