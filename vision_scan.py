"""OCR for product-code photos — Tesseract first, Haiku vision fallback.

Cost-aware ladder (cheapest path that solves the problem):

  1) **Tesseract (free)** — local OCR with image preprocessing. Handles most
     phone photos of clearly-printed codes with $0 per scan.

  2) **Catalog-aware self-healing (free)** — after OCR, every detected code
     that isn't in the seasoning master gets a variant search: swap each
     ambiguous char (B↔8, O↔0, S↔5, I↔1, Z↔2, G↔6, D↔0) and look up the
     swapped code. If a swap lands a real catalog hit, we silently fix it.
     Invisible most of the time, life-saving on photos where OCR picks the
     wrong twin. THIS is the main accuracy boost for B/8 confusion.

  3) **Claude Haiku 4.5 (paid, fallback)** — only used when Tesseract
     returns NOTHING. Roughly 1/6 the cost of Sonnet for OCR; combined with
     the self-healing layer, accuracy is fine. Skipped entirely when the
     Tesseract path succeeds, so most scans are free.

Public API:
    scan_image(img_bytes: bytes, catalog_codes: set[str]) -> ScanResult
"""
from __future__ import annotations

import asyncio
import base64
import logging
import re
from dataclasses import dataclass
from itertools import product
from typing import Iterable

import config

log = logging.getLogger(__name__)

# Same shape NPProductBot uses — must start with "S-" then 3+ alnum, optional dash + suffix.
_CODE_RE = re.compile(r"\bS-[A-Za-z0-9]{3,}(?:-[A-Za-z0-9]{1,4})?\b", re.IGNORECASE)

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
system. Codes ALWAYS look like:

    S-XXXXX        (e.g. S-62RG3, S-BACIT02, S-Y9KY2)
    S-XXXXX-NN     (e.g. S-62RG3-19, S-51ZB1-11, S-S7CG5-61)

Every code starts with literal `S-`. The body after the dash is 3-10 alphanumeric
characters, may contain a SECOND dash followed by a 1-4 char suffix.

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
    source: str = "none"                # "tesseract" | "haiku" | "none"
    tokens_in: int = 0
    tokens_out: int = 0


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


def _heal_against_catalog(
    raw_codes: list[str], catalog_codes: set[str]
) -> tuple[list[str], dict[str, str], list[str]]:
    """Snap each raw code to a real catalog code when possible.

    Returns:
        final_codes: what to actually use (original or corrected)
        corrections: raw → corrected mapping (only when changed)
        unmatched: codes we couldn't snap to anything in the catalog
    """
    final: list[str] = []
    corrections: dict[str, str] = {}
    unmatched: list[str] = []

    if not catalog_codes:
        return list(raw_codes), {}, list(raw_codes)

    for raw in raw_codes:
        if raw in catalog_codes:
            final.append(raw)
            continue
        # Try variants — the FIRST catalog hit wins (variants iterated in a
        # deterministic order from _generate_variants).
        snapped = None
        for v in _generate_variants(raw):
            if v == raw:
                continue
            if v in catalog_codes:
                snapped = v
                break
        if snapped is not None:
            final.append(snapped)
            corrections[raw] = snapped
        else:
            # Keep the raw code so /pp can still try it (and log Not Found).
            final.append(raw)
            unmatched.append(raw)
    return final, corrections, unmatched


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


async def scan_image(
    img_bytes: bytes, catalog_codes: set[str] | None = None
) -> ScanResult:
    """OCR a photo: Tesseract first (free), Haiku fallback (paid), then heal against catalog.

    `catalog_codes` should be the set of canonical uppercase codes from the
    seasoning master sheet. Pass an empty set to skip self-healing.
    """
    catalog_codes = catalog_codes or set()

    # 1) Free path — Tesseract
    raw_codes = await asyncio.to_thread(_tesseract_extract, img_bytes)
    source = "tesseract" if raw_codes else "none"
    tin = tout = 0

    # 2) Paid fallback — Haiku, only if Tesseract returned nothing
    if not raw_codes:
        raw_codes, tin, tout = await asyncio.to_thread(_haiku_extract, img_bytes)
        if raw_codes:
            source = "haiku"

    final, corrections, unmatched = _heal_against_catalog(raw_codes, catalog_codes)
    log.info(
        "scan_image source=%s raw=%s corrections=%s unmatched=%s",
        source, raw_codes, corrections, unmatched,
    )
    return ScanResult(
        codes=final,
        raw_codes=raw_codes,
        corrections=corrections,
        unmatched=unmatched,
        source=source,
        tokens_in=tin,
        tokens_out=tout,
    )
