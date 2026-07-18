"""Force RapidOCR to download its ONNX models at BUILD time, into the image.

Run once during the Railway build (see nixpacks.toml). RapidOCR otherwise
fetches ~20 MB of models on first use — and if that first use is a rep's scan
on a container whose outbound network is slow or firewalled, initialisation
fails or times out and the scan silently degrades to Tesseract.

Baking the models in at build time makes the first real scan instant and
immune to a runtime network hiccup. Exits 0 even on failure so a transient
model-registry outage can never break the whole deploy — the runtime path
still downloads lazily as a fallback.
"""
from __future__ import annotations

import sys


def main() -> int:
    try:
        from rapidocr import RapidOCR

        import numpy as np

        eng = RapidOCR()
        # A tiny blank frame forces the det/cls/rec models to actually load,
        # not just the package. Result is irrelevant.
        eng(np.zeros((32, 64, 3), dtype=np.uint8))
        print("warm_ocr_models: RapidOCR models downloaded and initialised OK")
        return 0
    except Exception as e:  # noqa: BLE001
        # Non-fatal by design — see module docstring.
        print(f"warm_ocr_models: WARNING — could not pre-warm RapidOCR: "
              f"{type(e).__name__}: {e}", file=sys.stderr)
        return 0


if __name__ == "__main__":
    sys.exit(main())
