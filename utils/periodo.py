# utils/periodo.py
# Armazena e valida o período escolhido no GUI (mês/ano inicial e final).

from __future__ import annotations
import datetime as _dt
import json
import threading
from pathlib import Path
from typing import Dict, Tuple

__all__ = ["set_periodo", "get_periodo", "get_periodo_tuple"]

_lock = threading.RLock()
_periodo: Dict[str, int] | None = None  # {"mes_de":..., "ano_de":..., "mes_ate":..., "ano_ate":...}

def _find_config_json() -> Path | None:
    name = "config.json"
    candidates = [
        Path.cwd() / name,
        Path(__file__).resolve().parent / name,
        Path(__file__).resolve().parent.parent / name,
    ]
    for parent in Path(__file__).resolve().parents:
        candidates.append(parent / name)
    return next((c for c in candidates if c.exists()), None)


def _load_period_from_config() -> Dict[str, int] | None:
    cfg_file = _find_config_json()
    if not cfg_file:
        return None
    try:
        with open(cfg_file, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        return None
    if not isinstance(cfg, dict):
        return None
    period = cfg.get("period") or cfg.get("periodo")
    if not isinstance(period, dict):
        return None
    try:
        return _normalize(period["mes_de"], period["ano_de"], period["mes_ate"], period["ano_ate"])
    except Exception:
        return None



def _now() -> Tuple[int, int]:
    hoje = _dt.date.today()
    return hoje.month, hoje.year

def _normalize(mes_de: int, ano_de: int, mes_ate: int, ano_ate: int) -> Dict[str, int]:
    # Inteiros e faixas válidas
    mes_de  = max(1, min(12, int(mes_de)))
    mes_ate = max(1, min(12, int(mes_ate)))
    ano_de  = int(ano_de)
    ano_ate = int(ano_ate)

    cm, cy = _now()  # mês/ano atuais

    # Não permitir datas futuras
    if ano_de > cy:  ano_de, mes_de  = cy, cm
    if ano_ate > cy: ano_ate, mes_ate = cy, cm
    if ano_de == cy and mes_de  > cm: mes_de  = cm
    if ano_ate == cy and mes_ate > cm: mes_ate = cm

    # Garantir ordem (início <= fim)
    if (ano_de, mes_de) > (ano_ate, mes_ate):
        ano_de, mes_de, ano_ate, mes_ate = ano_ate, mes_ate, ano_de, mes_de

    return {"mes_de": mes_de, "ano_de": ano_de, "mes_ate": mes_ate, "ano_ate": ano_ate}

def set_periodo(mes_de: int, ano_de: int, mes_ate: int, ano_ate: int) -> None:
    """Define/atualiza o período global usado pelo robô (valida e normaliza)."""
    global _periodo
    with _lock:
        _periodo = _normalize(mes_de, ano_de, mes_ate, ano_ate)

def get_periodo() -> Dict[str, int]:
    """
    Retorna um dicionário {"mes_de","ano_de","mes_ate","ano_ate"}.
    Se nada foi definido ainda, usa mês/ano atuais como início e fim.
    """
    global _periodo
    with _lock:
        if _periodo is None:
            loaded = _load_period_from_config()
            if loaded:
                _periodo = loaded
            else:
                cm, cy = _now()
                return _normalize(cm, cy, cm, cy)
        return dict(_periodo)  # copia defensiva
def get_periodo_tuple() -> Tuple[int, int, int, int]:
    """Retorna (mes_de, ano_de, mes_ate, ano_ate)."""
    p = get_periodo()
    return p["mes_de"], p["ano_de"], p["mes_ate"], p["ano_ate"]
