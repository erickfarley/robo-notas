# config.py
# Configuracoes gerais do projeto NotaManausRPA

import json
import os
import pathlib
import tempfile
from typing import Any, Dict, Optional

# Configuracoes do sistema
URL = "https://nfse-prd.manaus.am.gov.br"
PERIODO = ("2025-08-01", "2025-08-31")


def _find_config_json() -> Optional[pathlib.Path]:
    name = "config.json"
    here = pathlib.Path(__file__).resolve()
    candidates = [
        pathlib.Path.cwd() / name,
        here.parent / name,
        here.parent.parent / name,
    ]
    for parent in here.parents:
        candidates.append(parent / name)
    for path in candidates:
        if path.exists():
            return path
    return None


def _read_runtime_config() -> Dict[str, Any]:
    cfg_path = _find_config_json()
    if not cfg_path:
        return {}
    try:
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _cfg_value(cfg: Dict[str, Any], key: str, default: Any = "") -> Any:
    raw = cfg.get(key)
    if raw is None:
        return default
    if isinstance(raw, str):
        value = raw.strip()
        return value if value else default
    return raw


_RUNTIME_CFG = _read_runtime_config()
_OPENROUTER_CFG = _RUNTIME_CFG.get("openrouter") if isinstance(_RUNTIME_CFG.get("openrouter"), dict) else {}


def _get_openrouter_value(env_key: str, cfg_key: str, default: str = "") -> str:
    env_val = (os.getenv(env_key) or "").strip()
    if env_val:
        return env_val
    cfg_val = _cfg_value(_OPENROUTER_CFG, cfg_key, "")
    if cfg_val:
        return str(cfg_val).strip()
    # Compatibilidade com chave plana no config.json (ex.: "openrouter_api_key")
    flat_val = _cfg_value(_RUNTIME_CFG, f"openrouter_{cfg_key}", "")
    if flat_val:
        return str(flat_val).strip()
    return default


def _get_openrouter_timeout(default_seconds: int = 30) -> int:
    env_val = (os.getenv("OPENROUTER_TIMEOUT") or "").strip()
    if env_val:
        try:
            return max(5, int(float(env_val)))
        except Exception:
            return default_seconds

    cfg_val = _cfg_value(_OPENROUTER_CFG, "timeout", None)
    if cfg_val is None:
        cfg_val = _cfg_value(_RUNTIME_CFG, "openrouter_timeout", None)
    if cfg_val is None:
        return default_seconds
    try:
        return max(5, int(float(cfg_val)))
    except Exception:
        return default_seconds


def _get_openrouter_models(default_model: str) -> list[str]:
    env_models = (os.getenv("OPENROUTER_MODELS") or "").strip()
    if env_models:
        models = [m.strip() for m in env_models.split(",") if m.strip()]
    else:
        raw_models = _cfg_value(_OPENROUTER_CFG, "models", None)
        if raw_models is None:
            raw_models = _cfg_value(_RUNTIME_CFG, "openrouter_models", None)
        if isinstance(raw_models, str):
            models = [m.strip() for m in raw_models.split(",") if m.strip()]
        elif isinstance(raw_models, list):
            models = [str(m).strip() for m in raw_models if str(m).strip()]
        else:
            models = []

    if not models:
        return [default_model, "openai/gpt-4o", "openai/gpt-4o-mini"]
    if default_model not in models:
        models.insert(0, default_model)
    return models


# Arquivos de entrada e saida
ARQ_ENTRADA = r"C:\Users\USER78\Documents\Projetos\Robo\cnpjs.xlsx"
ARQ_SAIDA = r"C:\Users\USER78\Documents\Projetos\Robo\resultado_notas.xlsx"
ARQ_ENTRADA_EMPRESAS = r"C:\Users\USER78\Documents\Projetos\Robo\ClientesAProcessar.xlsx"

# Configuracoes de tentativas
MAX_TENTATIVAS_CAPTCHA = 5  # ciclos completos
MAX_CHAMADAS_OPENROUTER = 2  # por ciclo (captcha)

# Configuracoes do Tesseract
TESSERACT_CMD = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

# Configuracoes do OpenRouter
OPENROUTER_API_KEY = _get_openrouter_value("OPENROUTER_API_KEY", "api_key", "")
OPENROUTER_ENDPOINT = _get_openrouter_value(
    "OPENROUTER_ENDPOINT",
    "endpoint",
    "https://openrouter.ai/api/v1/chat/completions",
)
OPENROUTER_MODEL = _get_openrouter_value("OPENROUTER_MODEL", "model", "anthropic/claude-3.5-sonnet")
OPENROUTER_TIMEOUT = _get_openrouter_timeout(default_seconds=30)  # segundos
OPENROUTER_MODELS = _get_openrouter_models(OPENROUTER_MODEL)

# Prompts para OCR
PROMPTS = [
    "Retorne exatamente 4 caracteres do captcha, usando apenas A-Z e 0-9. Sem espacos, sem pontuacao.",
    "Captcha alfanumerico de 4 caracteres. Nao converta digitos em letras nem letras em digitos. Responda so os 4 caracteres.",
]

# Configuracoes de diretorios
FALLBACK_DIR = pathlib.Path(tempfile.gettempdir()) / "NotaManausRPA"
FALLBACK_DIR.mkdir(parents=True, exist_ok=True)
