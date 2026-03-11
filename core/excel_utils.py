# core/excel_utils.py
# Utilitários para leitura e escrita de arquivos Excel

import re
import pathlib
import unicodedata
from typing import Tuple, List, Any, TYPE_CHECKING

from config import ARQ_ENTRADA, ARQ_SAIDA, ARQ_ENTRADA_EMPRESAS
from utils.paths import ensure_dir
from utils.logger import log_info, log_warning

if TYPE_CHECKING:
    import pandas as pd  # pragma: no cover


def clean_number(s: str) -> str:
    """
    Limpa e formata números (CPF/CNPJ), preservando zeros à esquerda.
    
    Args:
        s: String com o número
        
    Returns:
        Número formatado com zeros à esquerda
    """
    raw = re.sub(r"\D", "", s)
    return raw.zfill(11) if len(raw) == 11 else raw.zfill(14)

def _lazy_import_pandas():
    try:
        import pandas as pd  # type: ignore
        return pd
    except Exception as exc:
        raise RuntimeError("Pandas nao disponivel para leitura/escrita de Excel.") from exc


def ler_excel() -> Tuple[str, str, List[str]]:
    """
    Lê o arquivo Excel de entrada e extrai usuário, senha e lista de CNPJs.
    
    Returns:
        Tupla com (usuario, senha, lista_cnpjs)
        
    Raises:
        ValueError: Se dados obrigatórios estiverem ausentes
    """
    pd = _lazy_import_pandas()
    df = pd.read_excel(ARQ_ENTRADA, dtype=str).fillna("")

    # Extrai usuário e senha da primeira linha
    usuario = clean_number(df.iloc[0].get("USUARIO", "").strip())
    senha = df.iloc[0].get("SENHA", "").strip()

    # Verifica se a coluna CNPJ existe
    if "CNPJ" not in df.columns:
        raise ValueError("Planilha precisa da coluna 'CNPJ'")

    # Extrai e limpa a lista de CNPJs
    lista = [clean_number(c) for c in df["CNPJ"] if c.strip()]

    # Valida dados obrigatórios
    if not usuario or not senha:
        raise ValueError("Primeira linha deve conter USUARIO e SENHA válidos")

    log_info("Usuario login: {}", usuario)
    log_info("CNPJs carregados: {}", len(lista))
    
    return usuario, senha, lista


def salvar_excel(df_out: Any):
    """
    Salva o DataFrame de resultados em arquivo Excel.
    
    Args:
        df_out: DataFrame com os resultados
    """
    if not df_out.empty:
        _lazy_import_pandas()
        # Define as colunas padrão
        df_out.columns = ["CNPJ", "Série", "Número", "Data", "Valor", "Tipo"]
        
        # Garante que o diretório existe
        ensure_dir(pathlib.Path(ARQ_SAIDA).parent)
        
        # Salva o arquivo
        df_out.to_excel(ARQ_SAIDA, index=False)
        log_info("Arquivo salvo em {}", ARQ_SAIDA)
    else:
        log_warning("Nenhum dado para salvar")

def _normalize(text: str) -> str:
    """
    • tira acentos,
    • converte para minúsculas,
    • remove espaços duplicados.
    """
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.split()).lower()


def _normalize(text: str) -> str:
    """
    Remove acentos, põe tudo em minúsculas e compacta espaços.
    Facilita a comparação “razão social do Excel” × “nome exibido no site”.
    """
    txt = unicodedata.normalize("NFKD", text)
    txt = "".join(c for c in txt if not unicodedata.combining(c))
    return " ".join(txt.split()).lower()


def ler_excel_empresas(
    caminho: str = ARQ_ENTRADA_EMPRESAS,
    coluna_nome: str = "RAZAO SOCIAL"
) -> List[str]:
    """
    Lê o Excel de clientes a processar e devolve uma **lista normalizada**
    de razões sociais.

    Parameters
    ----------
    caminho      : str
        Caminho do arquivo Excel (padrão = ARQ_EMPRESAS).
    coluna_nome  : str
        Nome da coluna que contém a razão social (padrão = “RAZAO SOCIAL”).

    Returns
    -------
    List[str]
        Lista de razões sociais (já normalizadas) pronta para comparação.

    Raises
    ------
    ValueError
        • Se o arquivo não existir  
        • Se a coluna indicada não estiver presente  
        • Se não houver valores válidos na coluna
    """
    try:
        pd = _lazy_import_pandas()
        df = pd.read_excel(caminho, dtype=str).fillna("")
    except FileNotFoundError:
        raise ValueError(f"Arquivo Excel não encontrado: {caminho}")

    if coluna_nome not in df.columns:
        raise ValueError(f"Planilha precisa da coluna '{coluna_nome}'")

    # Extrai, limpa e normaliza
    empresas = [_normalize(nome) for nome in df[coluna_nome] if nome.strip()]

    if not empresas:
        raise ValueError(f"Nenhum valor válido encontrado na coluna '{coluna_nome}'")

    log_info("Empresas carregadas: {}", len(empresas))
    return empresas
