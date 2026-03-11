# utils/logger.py
# Utilitários de logging (usando prints por enquanto)

import datetime
import os
import sys
from typing import Any


def _ensure_utf8_stream(stream) -> None:
    if not stream:
        return
    if hasattr(stream, "reconfigure"):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


_ensure_utf8_stream(sys.stdout)
_ensure_utf8_stream(sys.stderr)


def _safe_text(message: str) -> str:
    enc = getattr(sys.stdout, "encoding", None) or "utf-8"
    try:
        message.encode(enc)
        return message
    except Exception:
        return message.encode(enc, errors="replace").decode(enc, errors="replace")


def log_info(message: str, *args: Any):
    """
    Log de informação.
    
    Args:
        message: Mensagem a ser logada
        *args: Argumentos adicionais para formatação
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_message = message.format(*args) if args else message
    worker = os.getenv("NM_WORKER_ID")
    prefix = f"[W{worker}] " if worker else ""
    formatted_message = _safe_text(f"{prefix}{formatted_message}")
    print(f"[{timestamp}] INFO: {formatted_message}")


def log_error(message: str, *args: Any):
    """
    Log de erro.
    
    Args:
        message: Mensagem de erro
        *args: Argumentos adicionais para formatação
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_message = message.format(*args) if args else message
    worker = os.getenv("NM_WORKER_ID")
    prefix = f"[W{worker}] " if worker else ""
    formatted_message = _safe_text(f"{prefix}{formatted_message}")
    print(f"[{timestamp}] ERROR: {formatted_message}")


def log_warning(message: str, *args: Any):
    """
    Log de aviso.
    
    Args:
        message: Mensagem de aviso
        *args: Argumentos adicionais para formatação
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_message = message.format(*args) if args else message
    worker = os.getenv("NM_WORKER_ID")
    prefix = f"[W{worker}] " if worker else ""
    formatted_message = _safe_text(f"{prefix}{formatted_message}")
    print(f"[{timestamp}] WARNING: {formatted_message}")


def log_debug(message: str, *args: Any):
    """
    Log de debug.
    
    Args:
        message: Mensagem de debug
        *args: Argumentos adicionais para formatação
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_message = message.format(*args) if args else message
    worker = os.getenv("NM_WORKER_ID")
    prefix = f"[W{worker}] " if worker else ""
    formatted_message = _safe_text(f"{prefix}{formatted_message}")
    print(f"[{timestamp}] DEBUG: {formatted_message}")
