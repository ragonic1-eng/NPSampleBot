# NPSampleBot container build.
#
# WHY A DOCKERFILE: Railway builds this project on Railpack, which ignores
# nixpacks.toml — so every aptPkgs / build fix there silently did nothing
# (the "libxcb.so.1" RapidOCR error persisted identically V1.17.6→V1.17.8). A
# Dockerfile is honoured regardless of builder; railway.json pins it.
#
# The OCR system-lib problem is dissolved, not patched: RapidOCR pulls the
# DESKTOP opencv-python (needs X11/GL libs a server lacks); we replace it with
# opencv-python-headless (identical cv2 API, zero GUI deps). Verified in the
# Railway build log: the headless swap installs cleanly and the libxcb error
# is gone.
#
# Base is pinned to BOOKWORM (Debian 12), NOT bare python:3.12-slim which now
# resolves to Debian 13/trixie. Two reasons: (1) trixie's t64 transition
# renamed many lib packages (libcups2 -> libcups2t64 …), breaking a manual
# apt list; (2) Playwright's dependency list targets these classic names. A
# V1.17.9 build failed on exactly this — Playwright's `--with-deps` tried to
# install Ubuntu-only font packages (ttf-unifont, ttf-ubuntu-font-family) that
# don't exist on modern Debian. We avoid `--with-deps` entirely and install
# Chromium's real runtime libs by hand below.

FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# System libraries:
#   tesseract-ocr        — OCR fallback engine (pytesseract shells out to it)
#   libgl1 / libglib2.0-0 — cheap insurance for opencv-headless
#   the rest             — Chromium's runtime deps for the DHL/FedEx scrape
#                          (classic bookworm names; all real packages)
RUN apt-get update && apt-get install -y --no-install-recommends \
        tesseract-ocr \
        libgl1 \
        libglib2.0-0 \
        libnss3 \
        libnspr4 \
        libatk1.0-0 \
        libatk-bridge2.0-0 \
        libcups2 \
        libdrm2 \
        libxcomposite1 \
        libxdamage1 \
        libxfixes3 \
        libxrandr2 \
        libgbm1 \
        libxkbcommon0 \
        libpango-1.0-0 \
        libcairo2 \
        libasound2 \
        fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps first so this layer caches across code-only changes.
COPY requirements.txt .
RUN pip install -r requirements.txt \
    # RapidOCR's transitive desktop opencv is the X11-linked one that fails on
    # a headless server. Purge every opencv variant, then install ONLY the
    # headless build so `import cv2` resolves to a GUI-free binary.
    && pip uninstall -y opencv-python opencv-contrib-python opencv-python-headless opencv-contrib-python-headless \
    && pip install opencv-python-headless

# Chromium BROWSER BINARY only — NOT `--with-deps` (its Ubuntu font list
# breaks on Debian; we installed the real libs above). Non-fatal on failure:
# a Chromium/Playwright hiccup must never block a deploy whose main job is OCR
# + price lookups. Worst case the twice-daily AWB scrape is skipped and logs a
# warning; everything else still runs.
RUN python -m playwright install chromium || \
    echo "WARN: playwright chromium install failed — AWB scrape will be skipped"

# App code.
COPY . .

# Bake the RapidOCR ONNX models into the image so the first real scan is
# instant and can't fail on a runtime network hiccup. Non-fatal — the runtime
# path still downloads lazily.
RUN python warm_ocr_models.py || true

# Matches the Procfile's `worker` process.
CMD ["python", "-u", "bot.py"]
