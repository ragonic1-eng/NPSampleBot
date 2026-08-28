"""Sync the local .env FROM Railway — Railway is the source of truth.

Workflow this supports: you change a variable on Railway (the deployed bot
picks it up automatically), then run this once so your local machine matches.
No more "works on Railway, fails locally" from a stale local copy — which is
exactly what caused the July MMS-login failures (local .env held an old
password and load_dotenv(override=True) let it win).

    python sync_env.py            # sync
    python sync_env.py --dry-run  # show what WOULD change, touch nothing

What it does:
  • pulls every variable from the linked Railway service
  • drops RAILWAY_* platform metadata (meaningless on a local machine)
  • PRESERVES local-only keys (Ollama endpoint, service-account file path)
  • backs up the existing .env before writing
  • never prints secret values — only key names and change types
"""
from __future__ import annotations

import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ENV_PATH = Path(__file__).with_name(".env")

# Keys that exist only on this machine and must survive a sync.
# Ollama runs on the PC (Railway can't reach localhost); the service-account
# FILE is the local fallback when GOOGLE_SERVICE_ACCOUNT_JSON isn't set.
LOCAL_ONLY_KEYS = (
    "GOOGLE_SERVICE_ACCOUNT_FILE",
    "OLLAMA_URL",
    "OLLAMA_MODEL",
)

KEY_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _unquote(v: str) -> str:
    """Inverse of _quote, so a value we wrote last run compares equal to the
    raw value Railway hands back this run (otherwise every sync would report
    the quoted GOOGLE_SERVICE_ACCOUNT_JSON blob as 'changed')."""
    if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
        inner = v[1:-1]
        if v[0] == '"':
            inner = inner.replace('\\"', '"').replace("\\\\", "\\")
        return inner
    return v


def _parse_env_text(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in text.splitlines():
        line = line.rstrip("\n")
        if not line.strip() or line.lstrip().startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        if KEY_RE.fullmatch(k):
            out[k] = _unquote(v)
    return out


def _fetch_railway() -> dict[str, str]:
    """Pull variables from the linked Railway service.

    A stale RAILWAY_API_TOKEN in the environment overrides the stored login
    and makes the CLI fail with 'Invalid RAILWAY_API_TOKEN', so drop both
    token vars for this subprocess and let the stored credentials work.
    """
    import os

    env = {k: v for k, v in os.environ.items()
           if k not in ("RAILWAY_API_TOKEN", "RAILWAY_TOKEN")}
    try:
        proc = subprocess.run(
            ["railway", "variables", "--kv"],
            capture_output=True, text=True, timeout=120, env=env,
            cwd=str(Path(__file__).parent), shell=(os.name == "nt"),
        )
    except FileNotFoundError:
        sys.exit("railway CLI not found. Install it, then run `railway link`.")
    except subprocess.TimeoutExpired:
        sys.exit("railway CLI timed out.")
    if proc.returncode != 0:
        sys.exit(f"railway variables failed:\n{proc.stderr.strip()[:500]}")
    data = _parse_env_text(proc.stdout)
    # RAILWAY_* are injected by the platform at runtime; copying them locally
    # would just be misleading.
    return {k: v for k, v in data.items() if not k.startswith("RAILWAY_")}


def _quote(v: str) -> str:
    """Quote when the value contains whitespace/quotes/backslashes so
    python-dotenv reads it back byte-for-byte (matters for the
    GOOGLE_SERVICE_ACCOUNT_JSON blob, which carries \\n escapes)."""
    if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
        return v  # already quoted
    if re.search(r"[\s\"'\\#]", v):
        return '"' + v.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return v


def main() -> None:
    dry_run = "--dry-run" in sys.argv

    railway = _fetch_railway()
    if not railway:
        sys.exit("Railway returned no variables — is the service linked?")

    current = _parse_env_text(ENV_PATH.read_text(encoding="utf-8")) \
        if ENV_PATH.exists() else {}

    preserved = {k: current[k] for k in LOCAL_ONLY_KEYS if k in current}

    added = sorted(set(railway) - set(current))
    changed = sorted(k for k in railway if k in current and current[k] != railway[k])
    removed = sorted(set(current) - set(railway) - set(LOCAL_ONLY_KEYS))
    unchanged = len(railway) - len(added) - len(changed)

    print(f"Railway variables: {len(railway)}")
    print(f"  + added    : {', '.join(added) or '(none)'}")
    print(f"  ~ changed  : {', '.join(changed) or '(none)'}")
    print(f"  - dropped  : {', '.join(removed) or '(none)'}")
    print(f"  = unchanged: {unchanged}")
    print(f"  keep local : {', '.join(preserved) or '(none)'}")

    if dry_run:
        print("\n--dry-run: nothing written.")
        return

    if ENV_PATH.exists():
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")
        backup = ENV_PATH.with_name(f".env.backup-{stamp}")
        shutil.copy2(ENV_PATH, backup)
        print(f"\nbackup: {backup.name}")

    synced_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"# Local .env — synced FROM Railway by sync_env.py on {synced_at}.",
        "# Railway is the source of truth: change it there, then re-run",
        "#     python sync_env.py",
        "#",
        "# config.py uses load_dotenv(override=False), so a real environment",
        "# variable still beats anything in this file.",
        "",
        "# ---- synced from Railway ----",
    ]
    lines += [f"{k}={_quote(railway[k])}" for k in sorted(railway)]
    lines += ["", "# ---- local-machine only (NOT on Railway) ----"]
    lines += [f"{k}={_quote(preserved[k])}" for k in LOCAL_ONLY_KEYS
              if k in preserved]

    ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    print(f"wrote {ENV_PATH.name}: {len(railway)} synced + {len(preserved)} preserved")


if __name__ == "__main__":
    main()
