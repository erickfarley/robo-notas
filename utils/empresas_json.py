from __future__ import annotations
from pathlib import Path
import os
from typing import List, Dict, Any, Optional, Tuple, Union
import json
from datetime import datetime
import tempfile
import shutil
import re
from utils.logger import log_info


def clean_number(
    value: object,
    *,
    strip_leading_zeros: bool = False,
    max_digits: Optional[int] = None,
    kind: Optional[str] = None,           # "cpf" | "cnpj" | None
    validate: bool = False
) -> str:
    """
    Normaliza um identificador numérico:
      - Converte para str
      - Mantém apenas dígitos 0-9
      - (opcional) remove zeros à esquerda
      - (opcional) corta em max_digits
      - (opcional) valida CPF/CNPJ (kind="cpf"|"cnpj", validate=True)

    Retorna string de dígitos (ou "" se não houver).
    """
    if value is None:
        return ""

    s = str(value)
    # remove tudo que não for dígito
    digits = re.sub(r"\D+", "", s)

    if not digits:
        return ""

    if strip_leading_zeros:
        digits = digits.lstrip("0") or "0"

    if max_digits is not None and max_digits > 0:
        digits = digits[:max_digits]

    if validate and kind:
        k = kind.lower()
        if k == "cpf" and not _is_valid_cpf(digits):
            raise ValueError(f"CPF inválido: {digits}")
        if k == "cnpj" and not _is_valid_cnpj(digits):
            raise ValueError(f"CNPJ inválido: {digits}")

    return digits


# ---------- Validadores (opcionais) ----------

def _is_valid_cpf(cpf: str) -> bool:
    cpf = re.sub(r"\D", "", cpf or "")
    if len(cpf) != 11 or cpf == cpf[0] * 11:
        return False

    # 1º dígito verificador
    soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
    d1 = (soma * 10) % 11
    d1 = 0 if d1 == 10 else d1

    # 2º dígito verificador
    soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
    d2 = (soma * 10) % 11
    d2 = 0 if d2 == 10 else d2

    return cpf[-2:] == f"{d1}{d2}"


def _is_valid_cnpj(cnpj: str) -> bool:
    cnpj = re.sub(r"\D", "", cnpj or "")
    if len(cnpj) != 14 or cnpj == cnpj[0] * 14:
        return False

    pesos1 = [5,4,3,2,9,8,7,6,5,4,3,2]
    pesos2 = [6] + pesos1

    def dv(nums, pesos):
        soma = sum(int(n) * p for n, p in zip(nums, pesos))
        r = soma % 11
        return "0" if r < 2 else str(11 - r)

    d1 = dv(cnpj[:12], pesos1)
    d2 = dv(cnpj[:12] + d1, pesos2)

    return cnpj[-2:] == d1 + d2

def _data_file_path(caminho: Optional[str] = None) -> Path:
    # utils/ -> raiz do projeto -> data/empresas_liberadas.json
    env_path = os.getenv("NM_EMPRESAS_JSON")
    if env_path:
        return Path(env_path)
    if caminho:
        return Path(caminho)
    return Path(__file__).resolve().parents[1] / "data" / "empresas_liberadas.json"


def _read_json(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _atomic_write(path: Path, data: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_name = tempfile.mkstemp(prefix="emp_", suffix=".json", dir=str(path.parent))
    try:
        with open(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        shutil.move(tmp_name, str(path))
    finally:
        try:
            Path(tmp_name).unlink(missing_ok=True)
        except Exception:
            pass


def _as_bool(val: Any) -> bool:
    if isinstance(val, bool): return val
    if isinstance(val, (int, float)): return val != 0
    if isinstance(val, str): return val.strip().lower() in {"1","true","t","on","yes","sim","y","s"}
    return False


def _norm_name(s: str) -> str:
    return " ".join((s or "").split()).strip()


def _digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")


# --- APIs de leitura já existentes ---
def ler_empresas_json(caminho: Optional[str] = None) -> List[str]:
    p = _data_file_path(caminho)
    data = _read_json(p)
    nomes: List[str] = []
    for item in data:
        if isinstance(item, dict) and _as_bool(item.get("Sel", False)):
            nome = _norm_name(item.get("empresa") or item.get("Empresa") or "")
            if nome:
                nomes.append(nome)
    # únicos preservando ordem
    out, seen = [], set()
    for n in nomes:
        if n not in seen:
            seen.add(n); out.append(n)
    return out

# Assume que você já tem clean_number() no projeto

def ler_credencial(
    cfg_path: Union[str, Path] = "config.json",
    empresas_json: Union[str, Path, None] = None
) -> Tuple[str, str, List[str]]:
    """
    Lê usuário e senha do config.json e a lista de CNPJs de JSON:
      - Prioriza a lista 'cnpjs' dentro do config.json
      - Na ausência, usa data/empresas_liberadas.json (apenas itens com Sel=True)
    Retorna: (usuario, senha, lista_cnpjs)

    Raises:
        ValueError em caso de arquivo ausente, JSON inválido ou dados obrigatórios faltando.
    """
    base_dir = Path(__file__).resolve().parents[1]

    cfg_path = Path(cfg_path)
    if not cfg_path.is_absolute():
        cfg_path = base_dir / cfg_path

    if empresas_json is None:
        empresas_json = base_dir / "data" / "empresas_liberadas.json"
    else:
        empresas_json = Path(empresas_json)

    # --- 1) Credenciais no config.json ---
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except FileNotFoundError:
        raise ValueError(f"Arquivo de configuração não encontrado: {cfg_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"config.json inválido: {e}")

    creds = cfg.get("credentials") or {}
    usuario = (creds.get("username") or creds.get("usuario") or "").strip()
    senha   = (creds.get("password") or creds.get("senha") or "").strip()

    # Normaliza usuário caso seja numérico (CPF/CNPJ como login)
    usuario = clean_number(usuario)

    if not usuario or not senha:
        raise ValueError(
            "Credenciais ausentes no config.json. Informe em credentials.username e credentials.password."
        )

    # --- 2) Lista de CNPJs de JSON ---
    lista: List[str] = []

    # 2a) Prioriza 'cnpjs' direto no config.json
    cfg_cnpjs = cfg.get("cnpjs") or cfg.get("CNPJS")
    if isinstance(cfg_cnpjs, list):
        for c in cfg_cnpjs:
            cstr = clean_number(str(c))
            if cstr:
                lista.append(cstr)

    # 2b) Se não houver, tenta empresas_liberadas.json (somente Sel=True)
    if not lista and empresas_json.exists():
        try:
            with open(empresas_json, "r", encoding="utf-8") as f:
                empresas = json.load(f)
            if isinstance(empresas, list):
                for e in empresas:
                    cnpj = clean_number(str((e or {}).get("cnpj", "")))
                    if cnpj and (e.get("Sel") is True or e.get("Sel") == 1):
                        lista.append(cnpj)
        except Exception:
            # Ignora erros de leitura aqui; trataremos mais abaixo se lista ficar vazia
            pass

    # Remove duplicatas preservando ordem
    seen = set()
    lista = [c for c in lista if not (c in seen or seen.add(c))]

    if not lista:
        raise ValueError(
            "Nenhum CNPJ encontrado nos JSONs. "
            "Adicione uma lista 'cnpjs' no config.json ou marque empresas (Sel=true) em data/empresas_liberadas.json."
        )

    log_info("Usuario login (config.json): {}", usuario)
    log_info("CNPJs carregados (JSON): {}", len(lista))

    return usuario, senha, lista

def ler_empresas_json_completo(caminho: Optional[str] = None) -> List[Dict[str, Any]]:
    p = _data_file_path(caminho)
    data = _read_json(p)
    return [d for d in data if isinstance(d, dict) and _as_bool(d.get("Sel", False))]


# --- NOVO: fila/pendências e marcação de processadas ---
def empresas_selecionadas(caminho: Optional[str] = None) -> List[Dict[str, Any]]:
    """Retorna todos os registros com Sel=True (ordem do arquivo)."""
    return ler_empresas_json_completo(caminho)


def primeira_selecionada(caminho: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Retorna a primeira empresa com Sel=True, ou None se não houver."""
    pend = empresas_selecionadas(caminho)
    return pend[0] if pend else None


def marcar_empresa_processada_por_cnpj(
    cnpj: str,
    ok: bool,
    obs: Optional[str] = None,
    caminho: Optional[str] = None,
    deselect_on_error: bool = False,
    report: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Atualiza o JSON marcando a empresa pelo CNPJ.
      - Se ok=True: Sel=False, grava processado_em + ultimo_status='ok'
      - Se ok=False: mantem Sel como esta (ou desmarca se deselect_on_error=True),
        grava ultimo_status='erro' e ultimo_erro (obs)
      - Se report for informado, grava em ultimo_relatorio.
    Retorna True se achar e salvar, False caso não encontre.
    """
    p = _data_file_path(caminho)
    data = _read_json(p)
    alvo = _digits(cnpj)
    if not alvo:
        return False

    achou = False
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for item in data:
        if not isinstance(item, dict):
            continue
        if _digits(item.get("cnpj", "")) == alvo:
            achou = True
            if ok:
                item["Sel"] = False
                item["ultimo_status"] = "ok"
                item["processado_em"] = agora
                if "ultimo_erro" in item:
                    item.pop("ultimo_erro", None)
            else:
                item["ultimo_status"] = "erro"
                item["processado_em"] = agora
                item["ultimo_erro"] = _norm_name(obs or "")
                if deselect_on_error:
                    item["Sel"] = False
                # mantém Sel como está para possibilitar reprocesso (quando deselect_on_error=False)
            if isinstance(report, dict):
                item["ultimo_relatorio"] = report
            break

    if achou:
        _atomic_write(p, data)
    return achou
