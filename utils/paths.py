# utils/paths.py
# Utilitários para gerenciamento de diretórios e arquivos

import pathlib
from pathlib import Path
import os, sys
from config import FALLBACK_DIR

def resource_path(*parts):
    base = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
    return str(base.joinpath(*parts))

def ensure_dir(p: pathlib.Path) -> pathlib.Path:
    """
    Garante que um diretório existe, criando-o se necessário.
    Se falhar, retorna o diretório fallback.
    
    Args:
        p: Caminho do diretório
        
    Returns:
        Caminho do diretório (original ou fallback)
    """
    try:
        p.mkdir(parents=True, exist_ok=True)
        return p
    except Exception:
        return FALLBACK_DIR


def save(p: pathlib.Path, data: bytes):
    """
    Salva dados binários em um arquivo, garantindo que o diretório existe.
    
    Args:
        p: Caminho do arquivo
        data: Dados binários para salvar
    """
    (ensure_dir(p.parent) / p.name).write_bytes(data)