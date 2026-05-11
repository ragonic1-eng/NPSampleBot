"""Groq Whisper STT for Telegram voice messages.

Telegram voice messages come as OGG/Opus. Groq's Whisper endpoint
accepts that directly, so we don't need ffmpeg or any local audio
processing — just forward the bytes and read back the transcription.

Why Groq (vs OpenAI Whisper or local Whisper):
  • Free tier covers ~7,500 audio seconds/day on whisper-large-v3-turbo
    — more than enough for the team's voice volume.
  • Paid pricing is ~$0.04/hour of audio (~10x cheaper than OpenAI).
  • Sub-second latency for short clips (sales reps don't wait).
  • Same transcription quality as OpenAI Whisper.

Auth: set GROQ_API_KEY in env (Railway → Variables).
"""
from __future__ import annotations

import logging

import httpx

import config

log = logging.getLogger(__name__)

_GROQ_URL = "https://api.groq.com/openai/v1/audio/transcriptions"


class GroqError(Exception):
    """Raised when the Groq API call fails (auth, rate limit, network)."""


async def transcribe_ogg(audio_bytes: bytes, *, language: str | None = None) -> str:
    """Transcribe a Telegram-style OGG/Opus voice message via Groq Whisper.

    Args:
        audio_bytes: raw OGG/Opus bytes (as Telegram serves them).
        language: optional ISO-639-1 hint (e.g. 'en'). Default None lets
            Whisper auto-detect — handles English + Bahasa + Mandarin etc.

    Returns:
        The transcription as a single-line string (whitespace stripped).

    Raises:
        GroqError if the key is missing or the API returns non-2xx.
    """
    if not config.GROQ_API_KEY:
        raise GroqError("GROQ_API_KEY is not configured — add it in Railway env vars.")

    headers = {"Authorization": f"Bearer {config.GROQ_API_KEY}"}
    files: list[tuple[str, tuple[str | None, bytes | str, str | None]]] = [
        ("file", ("voice.ogg", audio_bytes, "audio/ogg")),
        ("model", (None, config.GROQ_WHISPER_MODEL, None)),
        # response_format=text returns a bare string — simpler than JSON.
        ("response_format", (None, "text", None)),
    ]
    if language:
        files.append(("language", (None, language, None)))

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.post(_GROQ_URL, headers=headers, files=files)
    except httpx.HTTPError as e:
        raise GroqError(f"network: {e}") from e

    if r.status_code != 200:
        # Groq error bodies are short JSON; surface them so the rep
        # gets a useful hint (rate limit, bad key, etc.).
        body = (r.text or "")[:300]
        raise GroqError(f"HTTP {r.status_code}: {body}")

    return (r.text or "").strip()
