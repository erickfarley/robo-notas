# core/ocr_remote.py
# OCR remoto usando OpenRouter

import base64
import io
import re
import requests
from PIL import Image, ImageFilter, ImageOps
from typing import Optional, List, Tuple
import pathlib

from config import (
    MAX_CHAMADAS_OPENROUTER,
    OPENROUTER_API_KEY,
    OPENROUTER_ENDPOINT,
    OPENROUTER_MODEL,
    OPENROUTER_MODELS,
    OPENROUTER_TIMEOUT,
    PROMPTS,
)
from utils.paths import save
from utils.logger import log_warning, log_error

_OPENROUTER_KEY_WARNING_SHOWN = False


def _extract_captcha_token(raw_text: str) -> str:
    text = (raw_text or "").strip().upper()
    if not text:
        return ""

    # Tenta token exato de 4 chars alfanumericos.
    tokens = re.findall(r"[A-Z0-9]{4}", text)
    if tokens:
        return tokens[0]

    # Fallback: remove ruido e aceita somente se resultar exatamente em 4 chars.
    cleaned = re.sub(r"[^A-Z0-9]", "", text)
    return cleaned if len(cleaned) == 4 else ""


def _resize_keep(img: Image.Image, target_w: int) -> Image.Image:
    w, h = img.size
    if w <= 0:
        return img
    if w != target_w:
        scale = target_w / w
        img = img.resize((target_w, int(h * scale)), Image.LANCZOS)
    return img


def prepare_image(b: bytes, target_w: int = 512) -> bytes:
    """Retorna a variante processada da imagem (pre-processamento leve)."""
    img = Image.open(io.BytesIO(b)).convert("L")
    img = ImageOps.autocontrast(img)
    img = img.filter(ImageFilter.MedianFilter(size=3))
    img = img.filter(ImageFilter.UnsharpMask(radius=1.5, percent=160, threshold=3))
    img = _resize_keep(img, target_w)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def prepare_image_variants(b: bytes, target_w: int = 512) -> List[Tuple[str, bytes]]:
    """Gera variantes para OCR: raw e processada."""
    img = Image.open(io.BytesIO(b))
    variants: List[Tuple[str, bytes]] = []

    raw = _resize_keep(img.convert("RGB"), target_w)
    buf_raw = io.BytesIO()
    raw.save(buf_raw, format="PNG")
    variants.append(("raw", buf_raw.getvalue()))

    proc = img.convert("L")
    proc = ImageOps.autocontrast(proc)
    proc = proc.filter(ImageFilter.MedianFilter(size=3))
    proc = proc.filter(ImageFilter.UnsharpMask(radius=1.5, percent=160, threshold=3))
    proc = _resize_keep(proc, target_w)
    buf_proc = io.BytesIO()
    proc.save(buf_proc, format="PNG")
    variants.append(("proc", buf_proc.getvalue()))

    return variants


def ocr_openrouter_candidates(b: bytes, dbg_dir: pathlib.Path, tag: str) -> List[str]:
    """Retorna candidatos de OCR usando modelos/prompt/variantes."""
    global _OPENROUTER_KEY_WARNING_SHOWN
    if not OPENROUTER_API_KEY:
        if not _OPENROUTER_KEY_WARNING_SHOWN:
            log_warning(
                "OPENROUTER_API_KEY ausente. OCR remoto desativado; usando apenas fallback local (Tesseract)."
            )
            _OPENROUTER_KEY_WARNING_SHOWN = True
        return []

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }

    candidates: List[str] = []
    variant_items = prepare_image_variants(b)
    variant_map = {name: payload for name, payload in variant_items}
    ordered_variants: List[Tuple[str, bytes]] = []
    if "raw" in variant_map:
        ordered_variants.append(("raw", variant_map["raw"]))
    if "proc" in variant_map:
        ordered_variants.append(("proc", variant_map["proc"]))
    if not ordered_variants:
        ordered_variants = variant_items

    models = list(dict.fromkeys(OPENROUTER_MODELS or [OPENROUTER_MODEL]))
    model_priority = [
        "openai/gpt-4o-mini",
        "openai/gpt-4o",
        str(OPENROUTER_MODEL or "").strip(),
    ]
    ordered_models: List[str] = []
    for name in model_priority:
        if name and name in models and name not in ordered_models:
            ordered_models.append(name)
    for name in models:
        if name not in ordered_models:
            ordered_models.append(name)
    models = ordered_models
    prompt_list = PROMPTS if PROMPTS else [
        "Retorne somente os 4 caracteres do captcha (A-Z e 0-9), sem explicacao."
    ]
    prompt = prompt_list[0]
    try:
        max_calls = max(1, int(MAX_CHAMADAS_OPENROUTER))
    except Exception:
        max_calls = 3
    timeout_sec = max(5, min(int(OPENROUTER_TIMEOUT), 10))
    request_count = 0

    # Agenda de chamadas: primeiro "raw" para todos os modelos, depois "proc".
    tasks: List[Tuple[str, str, bytes]] = []
    for variant_name, variant_bytes in ordered_variants:
        for model in models:
            tasks.append((model, variant_name, variant_bytes))

    for call_idx, (model, variant_name, variant_bytes) in enumerate(tasks, 1):
        if request_count >= max_calls:
            break
        request_count += 1
        img64 = base64.b64encode(variant_bytes).decode()
        data_url = f"data:image/png;base64,{img64}"
        body = {
            "model": model,
            "temperature": 0,
            "max_tokens": 32,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image_url",
                            "image_url": {"url": data_url, "detail": "low"},
                        },
                    ],
                }
            ],
        }

        try:
            r = requests.post(
                OPENROUTER_ENDPOINT,
                json=body,
                headers=headers,
                timeout=timeout_sec,
            )
            r.raise_for_status()

            raw_text = str(r.json()["choices"][0]["message"]["content"] or "")
            txt = _extract_captcha_token(raw_text)

            save(
                dbg_dir / f"resp_{tag}_{model.replace('/', '-')}_{variant_name}_p1.txt",
                raw_text.encode(),
            )

            if txt:
                candidates.append(txt)
                if len(dict.fromkeys(candidates)) >= 3:
                    break
            else:
                log_warning("OCR vazio: {} {} call{}", model, variant_name, call_idx)

        except Exception as e:
            log_error("OCR call {} falhou ({} {}): {}", call_idx, model, variant_name, e)

    return list(dict.fromkeys(candidates))


def ocr_openrouter(b: bytes, dbg_dir: pathlib.Path, tag: str) -> Optional[str]:
    candidates = ocr_openrouter_candidates(b, dbg_dir, tag)
    for txt in candidates:
        if 3 <= len(txt) <= 8:
            return txt
    return None
