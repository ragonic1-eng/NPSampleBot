"""Claude Sonnet 4.6 main brain with Ollama backup.

Currently used for optional semantic re-ranking of seasoning matches when
the fuzzy score is ambiguous. Kept lightweight and never blocks the flow
(always falls back to the fuzzy-only list on any error).
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import httpx

import config

log = logging.getLogger(__name__)

_anthropic_client = None


def _claude():
    global _anthropic_client
    if _anthropic_client is None and config.ANTHROPIC_API_KEY:
        from anthropic import Anthropic

        _anthropic_client = Anthropic(api_key=config.ANTHROPIC_API_KEY)
    return _anthropic_client


def _prompt_for_seasoning_rerank(query: str, candidates: list[dict[str, Any]]) -> str:
    lines = [f"{i+1}. {c['name']} (code {c.get('code','')})" for i, c in enumerate(candidates)]
    return (
        "You are helping a salesperson find a seasoning in our catalog.\n"
        f"User typed: {query!r}\n"
        "Candidates:\n" + "\n".join(lines) + "\n\n"
        "Return JSON only: {\"order\": [<1-based indexes best-first>]}. "
        "Base order on likely intent (flavor, application, keywords)."
    )


async def rerank_seasonings(
    query: str, candidates: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], int, int]:
    """Return (reordered candidates, input_tokens, output_tokens)."""
    if len(candidates) <= 1:
        return candidates, 0, 0
    prompt = _prompt_for_seasoning_rerank(query, candidates)
    text, tin, tout = await _ask(prompt)
    if not text:
        return candidates, tin, tout
    try:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1:
            return candidates, tin, tout
        data = json.loads(text[start : end + 1])
        order = data.get("order", [])
        seen = set()
        out = []
        for i in order:
            idx = int(i) - 1
            if 0 <= idx < len(candidates) and idx not in seen:
                out.append(candidates[idx])
                seen.add(idx)
        for i, c in enumerate(candidates):
            if i not in seen:
                out.append(c)
        return out, tin, tout
    except Exception as e:  # noqa: BLE001
        log.debug("Rerank parse failed: %s", e)
        return candidates, tin, tout


async def _ask(prompt: str, max_tokens: int = 200, http_timeout: float = 20) -> tuple[str, int, int]:
    """Try Claude, fall back to Ollama. Returns (text, input_tokens, output_tokens).

    Claude call is wrapped in asyncio.to_thread so we don't block the event loop.
    """
    client = _claude()
    if client is not None:
        try:
            msg = await asyncio.to_thread(
                lambda: client.messages.create(
                    model=config.CLAUDE_MODEL,
                    max_tokens=max_tokens,
                    messages=[{"role": "user", "content": prompt}],
                )
            )
            text = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text")
            tin = getattr(msg.usage, "input_tokens", 0) or 0
            tout = getattr(msg.usage, "output_tokens", 0) or 0
            return text, tin, tout
        except Exception as e:  # noqa: BLE001
            log.warning("Claude failed, falling back to Ollama: %s", e)

    try:
        async with httpx.AsyncClient(timeout=http_timeout) as http:
            r = await http.post(
                f"{config.OLLAMA_URL}/api/generate",
                json={
                    "model": config.OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                },
            )
            r.raise_for_status()
            data = r.json()
            return (
                data.get("response", ""),
                int(data.get("prompt_eval_count", 0) or 0),
                int(data.get("eval_count", 0) or 0),
            )
    except Exception as e:  # noqa: BLE001
        log.warning("Ollama failed: %s", e)
        return "", 0, 0


# ---------- Bulk-paste parser (V0.4.0) ----------

def _prompt_for_bulk_parse(
    raw_text: str,
    shared: dict[str, str],
    seasoning_codes: list[str],
    customer_names: list[str],
) -> str:
    codes_hint = ", ".join(seasoning_codes[:300]) if seasoning_codes else "(none)"
    custs_hint = ", ".join(customer_names[:100]) if customer_names else "(none)"
    return (
        "You are parsing a sales email / chat note into structured seasoning "
        "sample requests for R&D.\n\n"
        f"Raw user input:\n<<<\n{raw_text}\n>>>\n\n"
        f"Known seasoning codes in our catalog (prefer exact match): {codes_hint}\n"
        f"Known customers in our master list: {custs_hint}\n\n"
        "Shared values ALREADY supplied by the user (apply to every item; "
        "do not re-infer these):\n"
        f"- Need to Check Taste: {shared.get('taste_check', '')}\n"
        f"- Customer Base: {shared.get('customer_base', '')}\n"
        f"- Preferred Courier: {shared.get('courier', '')}\n"
        f"- Selling Price Budget: {shared.get('price_budget', '')}\n\n"
        "Return JSON only (no prose, no code fences), with this shape:\n"
        "{\n"
        '  "customer": {\n'
        '    "name": "<company name>",\n'
        '    "address": "<full shipping address or empty>",\n'
        '    "receiving_person": "<person receiving the parcel>",\n'
        '    "receiver_number": "<phone or empty>"\n'
        "  },\n"
        '  "market": "<country or region>",\n'
        '  "deadline": "<date string as written, e.g. \'30 April 2026\'>",\n'
        '  "items": [\n'
        "    {\n"
        '      "seasoning": "<seasoning name>",\n'
        '      "code": "<S-XXXX code exactly as written, else empty>",\n'
        '      "quantity": "<see format rules below>",\n'
        '      "dosage": "<e.g. \'7%\' or empty>",\n'
        '      "requirement": "<regulatory notes or empty>",\n'
        '      "app_method": "<Dusting | Slurry | 3DF | empty>",\n'
        '      "comment": "<any extra note for R&D>"\n'
        "    }\n"
        "  ]\n"
        "}\n\n"
        "Quantity format rules:\n"
        "- If the text says something like '100g concentrate + 100g normal' with "
        "NO application sample, output exactly \"100g concentrate + 100g normal\" "
        "(do NOT add \"1 x\" prefix).\n"
        "- If the text lists bottle counts for an oil, use e.g. \"2 bottles\".\n"
        "- If the text gives sets + weight like '2 packets of 100g', output "
        "\"2 x 100g seasoning\".\n"
        "- If an application sample is mentioned, append \" + N x Xg on <base>\" "
        "(e.g. \"2 x 100g seasoning + 3 x 20g on Potato chips\").\n"
        "- If unsure, output the verbatim quantity text as written by the user.\n\n"
        "Other rules:\n"
        "- One entry per seasoning SKU in the source text.\n"
        "- Preserve seasoning codes EXACTLY as written (case, dashes, suffixes).\n"
        "- If the source lists only one overall deadline, put it in the top-level "
        "\"deadline\" and leave it out of per-item fields.\n"
        "- Never invent addresses, phone numbers, or dosages — use empty string "
        "when not present.\n"
        "- Output ONLY the JSON object, nothing else."
    )


async def parse_bulk_sample_request(
    raw_text: str,
    shared: dict[str, str],
    seasoning_codes: list[str] | None = None,
    customer_names: list[str] | None = None,
) -> tuple[dict[str, Any], int, int]:
    """Parse a pasted multi-seasoning request into structured JSON.

    Returns (result, input_tokens, output_tokens) where result has keys
    'customer', 'market', 'deadline', 'items'. Falls back to empty structure
    if Claude and Ollama both fail.
    """
    empty: dict[str, Any] = {"customer": {}, "market": "", "deadline": "", "items": []}
    if not raw_text.strip():
        return empty, 0, 0
    prompt = _prompt_for_bulk_parse(
        raw_text, shared, seasoning_codes or [], customer_names or []
    )
    text, tin, tout = await _ask(prompt, max_tokens=4000, http_timeout=90)
    if not text:
        return empty, tin, tout
    try:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1:
            return empty, tin, tout
        data = json.loads(text[start : end + 1])
    except Exception as e:  # noqa: BLE001
        log.warning("Bulk parse JSON decode failed: %s", e)
        return empty, tin, tout
    result = {
        "customer": data.get("customer") or {},
        "market": str(data.get("market") or "").strip(),
        "deadline": str(data.get("deadline") or "").strip(),
        "items": data.get("items") or [],
    }
    if not isinstance(result["customer"], dict):
        result["customer"] = {}
    if not isinstance(result["items"], list):
        result["items"] = []
    return result, tin, tout
