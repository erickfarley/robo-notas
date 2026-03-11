# core/ocr_fallback.py
# OCR fallback usando Tesseract

import io
import re
from typing import Optional

from PIL import Image, ImageFilter, ImageOps
import pytesseract

from config import TESSERACT_CMD

# Configurar o caminho do Tesseract
pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD


def _extract_token(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return ""
    tokens = re.findall(r"[A-Za-z0-9]{4}", raw)
    if tokens:
        return tokens[0]
    cleaned = re.sub(r"[^A-Za-z0-9]", "", raw)
    return cleaned if len(cleaned) == 4 else ""


def ocr_tesseract_quick(b: bytes) -> Optional[str]:
    """
    OCR fallback para captcha alfanumerico de 4 caracteres.
    """
    try:
        base = Image.open(io.BytesIO(b)).convert("L")
        base = ImageOps.autocontrast(base)
        base = base.filter(ImageFilter.MedianFilter(size=3))
        base = base.filter(ImageFilter.MaxFilter(size=3))
        base = base.resize((420, 130), Image.LANCZOS)

        variants = []
        variants.append(base)
        for thr in (115, 130, 145, 160, 175):
            bw = base.point(lambda p, t=thr: 255 if p > t else 0, mode="1").convert("L")
            variants.append(bw)
            variants.append(ImageOps.invert(bw))
        variants.append(ImageOps.invert(base))

        configs = [
            r"--oem 3 --psm 8 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
            r"--oem 3 --psm 7 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
            r"--oem 3 --psm 13 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
        ]

        candidates = []
        for img in variants:
            for config in configs:
                txt = pytesseract.image_to_string(img, config=config)
                token = _extract_token(txt).upper()
                if len(token) == 4:
                    candidates.append(token)

        if not candidates:
            return None

        counts = {}
        for code in candidates:
            counts[code] = counts.get(code, 0) + 1
        ranked = sorted(candidates, key=lambda c: (-counts[c], candidates.index(c)))
        return ranked[0]
    except Exception:
        return None
