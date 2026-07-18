# NPSampleBot container build.
#
# WHY A DOCKERFILE: Railway stopped honouring nixpacks.toml (the project builds
# on Railpack now), so every aptPkgs / build-phase fix silently did nothing —
# the "libxcb.so.1 not found" RapidOCR error persisted identically across
# V1.17.6→V1.17.8 even after those libs were added to nixpacks.toml. A
# Dockerfile is honoured regardless of builder, so this takes full control of
# the build and ends the ambiguity.
#
# The OCR system-lib problem is dissolved rather than patched: RapidOCR pulls
# the DESKTOP opencv-python (needs X11/GL libs a server lacks); we replace it
# with opencv-python-headless (identical cv2 API, zero GUI deps). Verified
# locally: RapidOCR reads codes fine on headless opencv.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Tesseract is the OCR fallback engine (pytesseract shells out to it).
# libgl1 + libglib2.0-0 are cheap insurance for opencv-headless on older
# wheels; headless generally needs neither, but they cost ~nothing.
RUN apt-get update && apt-get install -y --no-install-recommends \
        tesseract-ocr \
        libgl1 \
        libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first so this layer caches across code-only changes.
COPY requirements.txt .
RUN pip install -r requirements.txt \
    # RapidOCR's transitive desktop opencv is the X11-linked one that fails on
    # a headless server. Purge every opencv variant, then install ONLY the
    # headless build so `import cv2` resolves to a GUI-free binary.
    && pip uninstall -y opencv-python opencv-contrib-python opencv-python-headless opencv-contrib-python-headless \
    && pip install opencv-python-headless

# Playwright's Chromium + its own system deps (twice-daily DHL/FedEx scrape).
# --with-deps installs the apt packages Chromium needs; far more reliable than
# hand-listing them.
RUN python -m playwright install --with-deps chromium

# App code.
COPY . .

# Bake the RapidOCR ONNX models into the image so the first real scan is
# instant and can't fail on a runtime network hiccup. Non-fatal on failure —
# the runtime path still downloads lazily.
RUN python warm_ocr_models.py || true

# Matches the Procfile's `worker` process.
CMD ["python", "-u", "bot.py"]
