# main.py 
# Entrada principal â€“ NotaManausRPA (navegaÃ§Ã£o atÃ© "Empresas Autorizadas")

import json
import math
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import time
import csv
from contextlib import suppress
from pathlib import Path
from typing import List, Dict, Optional, Callable, Tuple, Any, Set

from utils.empresas_json import (
    ler_empresas_json_completo,  # mantÃ©m para compatibilidade, se precisar
    ler_credencial,
    primeira_selecionada,
    marcar_empresa_processada_por_cnpj,
)
from playwright.sync_api import sync_playwright, Page
from core.captcha      import login, manual_login, is_on_login_screen
from utils.paths       import ensure_dir
from utils.logger      import log_info, log_error, log_warning
from core.browser      import (
    abrir_empresas_autorizadas, abrir_movimentacao_mensal_nfse,
    encerrar_mov_nfse_mensal, encerrar_mov_ret_mensal,
    encerrar_mov_mensal, selecionar_empresa_por_lista,
    abrir_movimentacao_mensal, abrir_movimentacao_ret_mensal,
    abrir_movimentacao_mensal_nacional, enncerrar_mov_mensal_nacioal,
    abrir_movimentacao_ret_mensal_nacional, encerrar_mov_ret_mensal_nacional,
    abrir_escrituracao_contabilidade, baixar_notas_emitidas,
    baixar_notas_recebidas, abrir_emissao_guias,
    baixar_relatorio_nota_nacional_recebidas,
    baixar_relatorio_nota_nacional_recebidas_intermediario,
    baixar_relatorio_nota_nacional_emetidas,
    baixar_extrato_issqn
)

# >>> mÃ³dulo correto para carregar empresas (sempre faz login + abre a tela)

# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ controle de execuÃ§Ã£o â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
_playwright: Optional[any] = None
_browser:    Optional[any] = None
_context:    Optional[any] = None
_page:       Optional[Page] = None
_stop_flag  = False

# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ contadores + hooks para a GUI â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
_processed: int = 0
_errors: int = 0
_gui_hooks: dict = {"on_processed": None, "on_error": None}
PARALLEL_LOGIN_WAIT_SEC = 180

FLOW_GROUPS = {
    "encerramentos": "Encerramentos",
    "downloads": "Downloads",
}

FLOW_STEPS = [
    ("encerrar_mov_mensal", "Encerrar Movimentacao Mensal (Servicos)", "encerramentos"),
    ("encerrar_mov_ret_mensal", "Encerrar Movimentacao Mensal (Retencao)", "encerramentos"),
    ("encerrar_mov_nfse_mensal", "Encerrar Movimentacao Mensal (NFSe)", "encerramentos"),
    ("enncerrar_mov_mensal_nacioal", "Encerrar Movimentacao Mensal (Nota Nacional)", "encerramentos"),
    ("encerrar_mov_ret_mensal_nacional", "Encerrar Movimentacao Mensal (Ret. Nota Nacional)", "encerramentos"),
    ("baixar_notas_emitidas", "Baixar Notas Emitidas (ZIP)", "downloads"),
    ("baixar_notas_recebidas", "Baixar Notas Recebidas (ZIP)", "downloads"),
    ("abrir_emissao_guias", "Emissao de Guias (PDF)", "downloads"),
    ("baixar_relatorio_nota_nacional_recebidas", "Baixar Relatorio Nota Nacional Recebidas", "downloads"),
    ("baixar_relatorio_nota_nacional_recebidas_intermediario", "Baixar Relatorio Nota Nacional Recebidas (Intermediario)", "downloads"),
    ("baixar_relatorio_nota_nacional_emetidas", "Baixar Relatorio Nota Nacional Emitidas", "downloads"),
    ("baixar_extrato_issqn", "Baixar Extrato Detalhado ISSQN (Tomado/Prestado)", "downloads"),
]

FLOW_LABELS = {key: label for key, label, _group in FLOW_STEPS}
FLOW_META = {key: {"label": label, "group": group} for key, label, group in FLOW_STEPS}
_FLOW_OVERRIDE: Optional[Dict[str, bool]] = None


def get_flow_steps() -> List[Dict[str, str]]:
    return [{"key": k, "label": label, "group": group} for k, label, group in FLOW_STEPS]


def get_flow_groups() -> Dict[str, str]:
    return dict(FLOW_GROUPS)


def _flow_defaults() -> Dict[str, bool]:
    return {k: True for k, _label, _group in FLOW_STEPS}


def _normalize_flow_selection(raw: Optional[object]) -> Dict[str, bool]:
    defaults = _flow_defaults()
    if isinstance(raw, dict):
        out = defaults.copy()
        for key in out:
            if key in raw:
                out[key] = bool(raw[key])
        return out
    if isinstance(raw, list):
        out = {k: False for k in defaults}
        for key in raw:
            if key in out:
                out[key] = True
        return out
    return defaults


def _parse_flow_env(value: str) -> Optional[object]:
    if not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        keys = [k.strip() for k in value.split(",") if k.strip()]
        return keys or None


def _set_flow_env(selection: Dict[str, bool]) -> None:
    try:
        os.environ["NM_FLOW_STEPS"] = json.dumps(selection)
    except Exception:
        pass


def get_flow_selection() -> Dict[str, bool]:
    env_raw = (os.getenv("NM_FLOW_STEPS") or "").strip()
    parsed = _parse_flow_env(env_raw)
    if parsed is not None:
        return _normalize_flow_selection(parsed)
    if _FLOW_OVERRIDE is not None:
        return _normalize_flow_selection(_FLOW_OVERRIDE)
    cfg = _read_config_json()
    raw = None
    if isinstance(cfg, dict):
        raw = cfg.get("flow_steps")
        if raw is None:
            raw = cfg.get("steps")
    return _normalize_flow_selection(raw)


def set_flow_selection(selection: Dict[str, bool]) -> None:
    global _FLOW_OVERRIDE
    normalized = _normalize_flow_selection(selection)
    _FLOW_OVERRIDE = normalized
    _set_flow_env(normalized)


def set_stop_flag()   -> None:  globals()['_stop_flag'] = True
def reset_stop_flag() -> None:  globals()['_stop_flag'] = False
def is_stop_requested() -> bool: return _stop_flag

def is_browser_open() -> bool:
    return _browser is not None

def set_gui_hooks(on_processed: Optional[Callable[[int], None]] = None,
                  on_error: Optional[Callable[[int], None]] = None) -> None:
    """GUI chama isto para receber atualizaÃ§Ãµes em tempo real."""
    _gui_hooks["on_processed"] = on_processed
    _gui_hooks["on_error"] = on_error

def mark_processed(n: int = 1) -> None:
    """Chame ao concluir o processamento de UMA empresa."""
    global _processed
    _processed += int(n)
    cb = _gui_hooks.get("on_processed")
    try:
        if cb:
            cb(int(n))
    except Exception:
        pass

def mark_error(n: int = 1) -> None:
    """Chame quando houver falha no processamento de UMA empresa."""
    global _errors
    _errors += int(n)
    cb = _gui_hooks.get("on_error")
    try:
        if cb:
            cb(int(n))
    except Exception:
        pass

def get_stats() -> Tuple[int, int]:
    """Retorna (processadas, erros) atÃ© o momento."""
    return _processed, _errors


def _ensure_page() -> Page:
    """
    Abre (ou reaproveita) Chrome e cria um Page.
    - Aceita downloads
    - Ignora erros de HTTPS (evita travar em pÃ¡ginas de login com TLS quebrado)
    - Define timeouts padrÃ£o para evitar â€œtravadasâ€ silenciosas
    """
    global _playwright, _browser, _context, _page
    if _page is not None:
        return _page

    cfg = _read_config_json()
    env_downloads = (os.getenv("NM_DOWNLOADS_DIR") or "").strip()
    raw_downloads = env_downloads or (cfg.get("downloads_dir") or "").strip()
    downloads_path = Path(raw_downloads) if raw_downloads else (Path(tempfile.gettempdir()) / "NotaManausRPA" / "downloads")
    downloads_dir = ensure_dir(downloads_path)
    headless = bool(cfg.get("headless", False))
    if _manual_login_enabled(cfg) and headless:
        log_warning("Manual login ativo: desabilitando headless.")
        headless = False

    log_info("Iniciando navegador (Google Chrome)â€¦")
    _playwright = sync_playwright().start()
    _browser = _playwright.chromium.launch(
        channel="chrome",
        headless=headless,
        args=[
            "--no-sandbox",
            "--disable-gpu",
            "--disable-software-rasterizer",
        ],
    )
    context_args = {
        "accept_downloads": True,
        "ignore_https_errors": True,  # â† IMPORTANTE p/ tela de login nÃ£o travar
    }
    try:
        _context = _browser.new_context(
            **context_args,
            downloads_path=str(downloads_dir),
        )
    except TypeError:
        log_info("Playwright sem suporte a downloads_path; usando diretorio padrao.")
        _context = _browser.new_context(**context_args)
    _page = _context.new_page()
    # Dialogos JS: aceitar sempre (confirm/alert)
    def _auto_accept_dialog(dialog) -> None:
        try:
            dialog.accept()
            log_info("Dialog JS aceito automaticamente.")
        except Exception as exc:
            log_warning(f"Falha ao aceitar dialogo: {exc}")

    try:
        _context.on("page", lambda p: p.on("dialog", _auto_accept_dialog))
    except Exception:
        pass
    try:
        _page.on("dialog", _auto_accept_dialog)
    except Exception:
        pass
    # Marca para evitar handlers duplicados em outras rotinas
    try:
        setattr(_page, "_nm_auto_dialog", True)
    except Exception:
        pass
    try:
        setattr(_context, "_nm_auto_dialog", True)
    except Exception:
        pass
# Timeouts padrÃ£o (mais previsÃ­vel)
    _page.set_default_timeout(30_000)               # 30s p/ aÃ§Ãµes/seletores
    _page.set_default_navigation_timeout(45_000)    # 45s p/ navegaÃ§Ãµes

    log_info("Navegador/Contexto prontos.")
    return _page


# ---------- Config.json helpers ----------
def _find_config_json() -> Optional[Path]:
    name = "config.json"
    candidates = [
        Path.cwd() / name,
        Path(__file__).resolve().parent / name,
        Path(__file__).resolve().parent.parent / name,
        Path(__file__).resolve().parent.parent.parent / name,
    ]
    for parent in Path(__file__).resolve().parents:
        candidates.append(parent / name)
    return next((c for c in candidates if c.exists()), None)


def _read_config_json() -> dict:
    cfg_file = _find_config_json()
    if not cfg_file:
        return {}
    try:
        with open(cfg_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _manual_login_enabled(cfg: Optional[dict] = None) -> bool:
    raw = (os.getenv("NM_MANUAL_LOGIN") or "").strip().lower()
    if raw in ("1", "true", "yes", "y", "sim"):
        return True
    if raw in ("0", "false", "no", "n", "nao"):
        return False
    if cfg is None:
        cfg = _read_config_json()
    try:
        return bool(cfg.get("manual_login"))
    except Exception:
        return False


def _manual_login_timeout_sec(cfg: Optional[dict] = None) -> int:
    raw = (os.getenv("NM_MANUAL_LOGIN_TIMEOUT_SEC") or "").strip()
    if raw:
        try:
            return max(30, int(raw))
        except Exception:
            pass
    if cfg is None:
        cfg = _read_config_json()
    try:
        return max(30, int(cfg.get("manual_login_timeout_sec", 900)))
    except Exception:
        return 900


def _login_with_mode(page: Page, dbg_dir: pathlib.Path, usuario: str, senha: str, *, navigate: bool = True) -> bool:
    cfg = _read_config_json()
    if _manual_login_enabled(cfg):
        return manual_login(page, usuario, senha, timeout_sec=_manual_login_timeout_sec(cfg), navigate=navigate)
    return bool(login(page, dbg_dir, usuario, senha))


def _relogin_automatic(page: Page, dbg_dir: pathlib.Path, usuario: str, senha: str, *, navigate: bool = False) -> bool:
    """Re-login sempre no modo automatico quando a sessao volta para a tela de login."""
    return bool(login(page, dbg_dir, usuario, senha))


def _resolve_parallel_workers(override: Optional[int] = None) -> int:
    if override is not None:
        try:
            return max(1, int(override))
        except Exception:
            return 1
    cfg = _read_config_json()
    if not isinstance(cfg, dict):
        return 1
    raw = cfg.get("parallel_workers", 1)
    try:
        return max(1, int(raw))
    except Exception:
        return 1


def _digits(value: object) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _read_empresas_file(path: Path) -> List[Dict[str, any]]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _write_empresas_file(path: Path, data: List[Dict[str, any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _split_chunks(items: List[Dict[str, any]], workers: int) -> List[List[Dict[str, any]]]:
    if workers <= 1:
        return [items]
    chunk_size = max(1, int(math.ceil(len(items) / workers)))
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def _resolve_downloads_root(cfg: dict) -> Path:
    env_dir = (os.getenv("NM_DOWNLOADS_DIR") or "").strip()
    if env_dir:
        return Path(env_dir)
    raw = (cfg.get("downloads_dir") or "").strip()
    if raw:
        return Path(raw)
    return Path(tempfile.gettempdir()) / "NotaManausRPA" / "downloads"


def _resolve_reports_root(cfg: Optional[dict] = None) -> Path:
    env_dir = (os.getenv("NM_REPORTS_DIR") or "").strip()
    if env_dir:
        return ensure_dir(Path(env_dir))
    if cfg is None:
        cfg = _read_config_json()
    cfg = cfg if isinstance(cfg, dict) else {}
    raw = str(cfg.get("reports_dir") or "").strip()
    if raw:
        return ensure_dir(Path(raw))
    return ensure_dir(Path(__file__).resolve().parent / "data" / "reports")


def _resolve_empresas_json_path() -> Path:
    env_path = (os.getenv("NM_EMPRESAS_JSON") or "").strip()
    if env_path:
        return Path(env_path)
    return Path(__file__).resolve().parent / "data" / "empresas_liberadas.json"


def _json_safe(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, set):
        return [_json_safe(v) for v in value]
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for k, v in value.items():
            out[str(k)] = _json_safe(v)
        return out
    return str(value)


def _collect_run_report_entries(data: List[Dict[str, Any]], selected_cnpjs: Optional[Set[str]] = None) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        cnpj = _digits(row.get("cnpj"))
        if selected_cnpjs is not None and cnpj not in selected_cnpjs:
            continue

        rel = row.get("ultimo_relatorio")
        if not isinstance(rel, dict):
            rel = {}
        step_results = rel.get("step_results")
        if not isinstance(step_results, dict):
            step_results = {}
        downloaded_files = rel.get("downloaded_files")
        if not isinstance(downloaded_files, list):
            downloaded_files = []
        closed_movements = rel.get("closed_movements")
        if not isinstance(closed_movements, list):
            closed_movements = []
        movement_without_action = rel.get("movement_without_action")
        if not isinstance(movement_without_action, list):
            movement_without_action = []
        alerts = rel.get("alerts")
        if not isinstance(alerts, list):
            alerts = []
        enabled_step_keys = rel.get("enabled_step_keys")
        if not isinstance(enabled_step_keys, list):
            enabled_step_keys = [k for k, _label, _group in FLOW_STEPS]
        executed_step_keys = rel.get("executed_step_keys")
        if not isinstance(executed_step_keys, list):
            executed_step_keys = []
        extrato_step = step_results.get("baixar_extrato_issqn", {}) if isinstance(step_results, dict) else {}
        extrato_files = extrato_step.get("files") if isinstance(extrato_step, dict) else []
        if not isinstance(extrato_files, list):
            extrato_files = []
        if not extrato_files:
            extrato_files = [
                str(x)
                for x in downloaded_files
                if isinstance(x, str)
                and ("extrato_detalhado" in x.lower() or "esxtrato_detalhado" in x.lower())
            ]

        entries.append(
            {
                "empresa": row.get("empresa") or row.get("Empresa") or "",
                "cnpj": cnpj,
                "status": row.get("ultimo_status") or "",
                "processado_em": row.get("processado_em") or "",
                "erro": row.get("ultimo_erro") or "",
                "worker_id": rel.get("worker_id") or "",
                "startup_ok": bool(rel.get("startup_ok", False)),
                "relogins_automaticos": int(rel.get("relogins_automaticos") or 0),
                "enabled_step_keys": enabled_step_keys,
                "executed_step_keys": executed_step_keys,
                "step_results": step_results,
                "downloaded_files": downloaded_files,
                "extrato_detalhado_files": extrato_files,
                "closed_movements": closed_movements,
                "movement_without_action": movement_without_action,
                "alerts": alerts,
            }
        )
    return entries


def _write_run_report_files(entries: List[Dict[str, Any]], cfg: Optional[dict] = None) -> Optional[Path]:
    if not entries:
        return None
    reports_dir = _resolve_reports_root(cfg)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    json_path = reports_dir / f"execucao_{stamp}.json"
    csv_path = reports_dir / f"execucao_{stamp}.csv"

    json_path.write_text(
        json.dumps([_json_safe(e) for e in entries], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    fieldnames = [
        "empresa",
        "cnpj",
        "status",
        "processado_em",
        "erro",
        "worker_id",
        "startup_ok",
        "relogins_automaticos",
        "enabled_steps_count",
        "executed_steps_count",
        "closed_movements_count",
        "closed_movements",
        "movement_without_action_count",
        "movement_without_action",
        "downloaded_files_count",
        "downloaded_files",
        "extrato_detalhado_files_count",
        "extrato_detalhado_files",
        "alerts_count",
        "alerts",
        "steps_summary",
    ]
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for entry in entries:
            step_results = entry.get("step_results") if isinstance(entry.get("step_results"), dict) else {}
            enabled_keys = entry.get("enabled_step_keys") if isinstance(entry.get("enabled_step_keys"), list) else []
            steps_summary_parts: List[str] = []
            for key in enabled_keys:
                meta = FLOW_META.get(str(key), {"label": str(key)})
                label = meta.get("label") or str(key)
                step_data = step_results.get(str(key), {}) if isinstance(step_results, dict) else {}
                status = str(step_data.get("status") or "nao_executada")
                steps_summary_parts.append(f"{label}={status}")

            downloaded_files = [str(x) for x in (entry.get("downloaded_files") or [])]
            extrato_detalhado_files = [str(x) for x in (entry.get("extrato_detalhado_files") or [])]
            closed_movements = [str(x) for x in (entry.get("closed_movements") or [])]
            movement_without_action = [str(x) for x in (entry.get("movement_without_action") or [])]
            alerts = [str(x) for x in (entry.get("alerts") or [])]

            writer.writerow(
                {
                    "empresa": entry.get("empresa") or "",
                    "cnpj": entry.get("cnpj") or "",
                    "status": entry.get("status") or "",
                    "processado_em": entry.get("processado_em") or "",
                    "erro": entry.get("erro") or "",
                    "worker_id": entry.get("worker_id") or "",
                    "startup_ok": bool(entry.get("startup_ok", False)),
                    "relogins_automaticos": int(entry.get("relogins_automaticos") or 0),
                    "enabled_steps_count": len(enabled_keys),
                    "executed_steps_count": len(entry.get("executed_step_keys") or []),
                    "closed_movements_count": len(closed_movements),
                    "closed_movements": " | ".join(closed_movements),
                    "movement_without_action_count": len(movement_without_action),
                    "movement_without_action": " | ".join(movement_without_action),
                    "downloaded_files_count": len(downloaded_files),
                    "downloaded_files": " | ".join(downloaded_files),
                    "extrato_detalhado_files_count": len(extrato_detalhado_files),
                    "extrato_detalhado_files": " | ".join(extrato_detalhado_files),
                    "alerts_count": len(alerts),
                    "alerts": " | ".join(alerts),
                    "steps_summary": " | ".join(steps_summary_parts),
                }
            )

    log_info("Relatorio de execucao gerado: {} e {}", json_path, csv_path)
    return json_path


def _count_status_from_files(worker_files: List[Path]) -> Tuple[int, int]:
    ok_count = 0
    err_count = 0
    for path in worker_files:
        data = _read_empresas_file(path)
        for row in data:
            if not isinstance(row, dict):
                continue
            status = str(row.get("ultimo_status") or "").lower()
            if status == "ok":
                ok_count += 1
            elif status == "erro":
                err_count += 1
    return ok_count, err_count


def _merge_worker_results(base_data: List[Dict[str, any]], worker_files: List[Path]) -> Tuple[int, int]:
    idx = {}
    for row in base_data:
        if isinstance(row, dict):
            cnpj = _digits(row.get("cnpj"))
            if cnpj:
                idx[cnpj] = row
    ok_count = 0
    err_count = 0
    for path in worker_files:
        data = _read_empresas_file(path)
        for row in data:
            if not isinstance(row, dict):
                continue
            cnpj = _digits(row.get("cnpj"))
            if not cnpj or cnpj not in idx:
                continue
            base_row = idx[cnpj]
            for key in (
                "Sel",
                "ultimo_status",
                "processado_em",
                "ultimo_erro",
                "ultimo_relatorio",
            ):
                if key in row:
                    base_row[key] = row[key]
            status = str(row.get("ultimo_status") or "").lower()
            if status == "ok":
                ok_count += 1
            elif status == "erro":
                err_count += 1
    return ok_count, err_count


def _pump_worker_output(
    proc: subprocess.Popen,
    login_event: Optional[threading.Event] = None,
    worker_id: Optional[int] = None,
) -> None:
    if proc.stdout is None:
        return
    for line in proc.stdout:
        text = line.rstrip()
        if text:
            print(text)
            if login_event and not login_event.is_set():
                lower = text.lower()
                if "login ok" in lower:
                    if worker_id is None:
                        login_event.set()
                    elif f"[w{worker_id}]" in lower or "[w" not in lower:
                        login_event.set()


def _terminate_process(proc: subprocess.Popen) -> None:
    try:
        proc.terminate()
        proc.wait(timeout=10)
    except Exception:
        try:
            proc.kill()
            proc.wait(timeout=5)
        except Exception:
            pass


def _worker_pending_count(worker_file: Path, *, force_select_pending: bool = False) -> int:
    """
    Conta empresas sem status final no arquivo de um worker.
    Se force_select_pending=True, garante Sel=True para as pendentes.
    """
    data = _read_empresas_file(worker_file)
    pending = 0
    changed = False
    for row in data:
        if not isinstance(row, dict):
            continue
        status = str(row.get("ultimo_status") or "").lower()
        if status in ("ok", "erro"):
            continue
        pending += 1
        if force_select_pending and not bool(row.get("Sel")):
            row["Sel"] = True
            changed = True
    if changed:
        _write_empresas_file(worker_file, data)
    return pending


def _run_parallel(workers: int) -> Tuple[bool, int, int]:
    global _processed, _errors
    _processed = 0
    _errors = 0
    reset_stop_flag()
    flow_selection = get_flow_selection()
    _set_flow_env(flow_selection)
    flow_env = os.getenv("NM_FLOW_STEPS", "")

    try:
        try:
            ler_credencial()
        except Exception:
            _ler_credenciais_config_fallback()
    except Exception as exc:
        log_error(f"Credenciais ausentes/invalidas: {exc}")
        return False, 0, 0

    base_path = Path(__file__).resolve().parent / "data" / "empresas_liberadas.json"
    base_data = _read_empresas_file(base_path)
    selected = [row for row in base_data if isinstance(row, dict) and bool(row.get("Sel"))]
    if not selected:
        log_info("Nenhuma empresa selecionada (Sel=True).")
        return True, 0, 0
    selected_cnpjs = {
        _digits(row.get("cnpj"))
        for row in selected
        if isinstance(row, dict) and _digits(row.get("cnpj"))
    }

    max_per_worker = 30
    cfg = _read_config_json()
    manual_login = _manual_login_enabled(cfg if isinstance(cfg, dict) else None)
    force_parallel = False
    raw_force = (os.getenv("NM_FORCE_PARALLEL") or "").strip().lower()
    if raw_force in ("1", "true", "yes", "y", "sim"):
        force_parallel = True
    if isinstance(cfg, dict) and bool(cfg.get("force_parallel")):
        force_parallel = True

    max_workers = max(1, int(workers))
    env_max = (os.getenv("NM_MAX_WORKERS") or "").strip()
    if env_max:
        try:
            max_workers = max(1, int(env_max))
        except Exception:
            pass

    required_workers = int(math.ceil(len(selected) / max_per_worker))
    if manual_login and not force_parallel:
        max_workers = 1
        log_warning("Login manual ativo: forÃ§ando 1 worker. Use NM_FORCE_PARALLEL=1 para permitir varios workers.")

    worker_count = min(max_workers, len(selected))
    if worker_count < required_workers:
        log_warning(
            f"Limite de workers ({worker_count}) menor que o requerido ({required_workers}) para manter <= {max_per_worker} empresas/worker. "
            "Alguns workers terao mais de 30 empresas."
        )
    manual_all_workers = bool(worker_count > 1 and manual_login)
    if manual_all_workers:
        log_warning(
            "Execucao paralela ativa com login manual em todos os workers. "
            "Cada worker vai aguardar captcha/login manual na propria sessao."
        )
    chunks = _split_chunks(selected, worker_count)
    run_dir = Path(__file__).resolve().parent / "data" / "parallel_runs" / time.strftime("%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    cfg = _read_config_json()
    downloads_root = ensure_dir(_resolve_downloads_root(cfg if isinstance(cfg, dict) else {}))
    script_path = Path(__file__).resolve().parent / "tools" / "parallel_worker.py"
    if not script_path.exists():
        log_error(f"Worker script nao encontrado: {script_path}")
        return False, 0, 0

    log_info(f"Iniciando execucao paralela com {worker_count} workers (max {max_per_worker} empresas/worker).")

    worker_files: List[Path] = []
    procs: List[subprocess.Popen] = []
    worker_tasks: List[Tuple[int, int, dict]] = []

    for idx, chunk in enumerate(chunks, start=1):
        if not chunk:
            continue
        for row in chunk:
            if isinstance(row, dict):
                row["Sel"] = True
        worker_file = run_dir / f"empresas_worker_{idx}.json"
        _write_empresas_file(worker_file, chunk)
        worker_files.append(worker_file)

        worker_downloads = ensure_dir(downloads_root / f"worker_{idx}")
        env = os.environ.copy()
        env["NM_EMPRESAS_JSON"] = str(worker_file)
        env["NM_DOWNLOADS_DIR"] = str(worker_downloads)
        env["NM_WORKER_ID"] = str(idx)
        env["NM_PARALLEL_WORKER"] = "1"
        if manual_login:
            env["NM_MANUAL_LOGIN"] = "1"
        else:
            env["NM_MANUAL_LOGIN"] = "0"
        env["PYTHONUNBUFFERED"] = "1"
        # Garantir stdout UTF-8 dos workers (evita UnicodeDecodeError no pump)
        env.setdefault("PYTHONIOENCODING", "utf-8")
        env.setdefault("PYTHONUTF8", "1")
        if flow_env:
            env["NM_FLOW_STEPS"] = flow_env

        worker_tasks.append((idx, len(chunk), env))

    if not worker_tasks:
        log_info("Nenhuma empresa selecionada (Sel=True).")
        return True, 0, 0

    restart_limit = 2
    raw_restart_limit = (os.getenv("NM_WORKER_RESTART_LIMIT") or "").strip()
    if raw_restart_limit:
        try:
            restart_limit = int(raw_restart_limit)
        except Exception:
            restart_limit = 2
    elif isinstance(cfg, dict):
        try:
            restart_limit = int(cfg.get("worker_restart_limit", 2))
        except Exception:
            restart_limit = 2
    restart_limit = max(0, min(5, restart_limit))

    def _spawn_worker(env: dict) -> subprocess.Popen:
        return subprocess.Popen(
            [sys.executable, str(script_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            encoding="utf-8",
            errors="replace",
            env=env,
        )

    def _recount_selected_status() -> Tuple[int, int, int]:
        ok_count_local = 0
        err_count_local = 0
        unfinished_local = 0
        if not selected_cnpjs:
            return 0, 0, 0
        for row in base_data:
            if not isinstance(row, dict):
                continue
            cnpj = _digits(row.get("cnpj"))
            if not cnpj or cnpj not in selected_cnpjs:
                continue
            status = str(row.get("ultimo_status") or "").lower()
            if status == "ok":
                ok_count_local += 1
            elif status == "erro":
                err_count_local += 1
            else:
                unfinished_local += 1
        return ok_count_local, err_count_local, unfinished_local

    def _run_final_retry_for_errors() -> None:
        if is_stop_requested():
            return
        retry_rows: List[Dict[str, any]] = []
        for row in base_data:
            if not isinstance(row, dict):
                continue
            cnpj = _digits(row.get("cnpj"))
            if not cnpj or cnpj not in selected_cnpjs:
                continue
            if str(row.get("ultimo_status") or "").lower() != "erro":
                continue
            item = dict(row)
            item["Sel"] = True
            item["ultimo_status"] = ""
            item.pop("processado_em", None)
            item.pop("ultimo_erro", None)
            item.pop("ultimo_relatorio", None)
            retry_rows.append(item)

        if not retry_rows:
            return

        retry_file = run_dir / "empresas_worker_retry_final.json"
        _write_empresas_file(retry_file, retry_rows)
        retry_downloads = ensure_dir(downloads_root / "worker_retry_final")
        retry_env = os.environ.copy()
        retry_env["NM_EMPRESAS_JSON"] = str(retry_file)
        retry_env["NM_DOWNLOADS_DIR"] = str(retry_downloads)
        retry_env["NM_WORKER_ID"] = "R"
        retry_env["NM_PARALLEL_WORKER"] = "1"
        retry_env["NM_MANUAL_LOGIN"] = "1" if manual_login else "0"
        retry_env["PYTHONUNBUFFERED"] = "1"
        retry_env.setdefault("PYTHONIOENCODING", "utf-8")
        retry_env.setdefault("PYTHONUTF8", "1")
        if flow_env:
            retry_env["NM_FLOW_STEPS"] = flow_env

        log_warning(
            "Retentativa final: {} empresa(s) com status erro serao reprocessadas em um novo worker.",
            len(retry_rows),
        )
        try:
            retry_proc = _spawn_worker(retry_env)
        except Exception as exc:
            log_error("Falha ao iniciar worker de retentativa final: {}", exc)
            return

        threading.Thread(target=_pump_worker_output, args=(retry_proc,), daemon=True).start()
        while retry_proc.poll() is None:
            if is_stop_requested():
                log_warning("Parada solicitada durante retentativa final. Encerrando worker de retentativa.")
                _terminate_process(retry_proc)
                break
            time.sleep(3)

        _merge_worker_results(base_data, [retry_file])
        _write_empresas_file(base_path, base_data)

        retry_data = _read_empresas_file(retry_file)
        still_error = 0
        fixed_ok = 0
        pending = 0
        for row in retry_data:
            if not isinstance(row, dict):
                continue
            status = str(row.get("ultimo_status") or "").lower()
            if status == "ok":
                fixed_ok += 1
            elif status == "erro":
                still_error += 1
            else:
                pending += 1

        log_info(
            "Retentativa final concluida: {} recuperada(s), {} ainda com erro, {} sem status final.",
            fixed_ok,
            still_error,
            pending,
        )

    first_idx, first_count, first_env = worker_tasks[0]
    log_info(f"Worker {first_idx}: {first_count} empresas.")
    first_proc = _spawn_worker(first_env)
    procs.append(first_proc)

    login_event = None
    if len(worker_tasks) > 1:
        login_wait_sec = PARALLEL_LOGIN_WAIT_SEC
        if manual_login:
            login_wait_sec = max(login_wait_sec, _manual_login_timeout_sec(cfg if isinstance(cfg, dict) else None))
        login_event = threading.Event()
        threading.Thread(
            target=_pump_worker_output,
            args=(first_proc, login_event, first_idx),
            daemon=True,
        ).start()
        if manual_login:
            log_info("Aguardando login manual do worker 1 antes de iniciar os demais.")
        else:
            log_info("Aguardando login do worker 1 antes de iniciar os demais.")
        if not login_event.wait(timeout=login_wait_sec):
            log_warning("Tempo limite de espera pelo login do worker 1. Iniciando demais workers.")
    else:
        threading.Thread(target=_pump_worker_output, args=(first_proc,), daemon=True).start()

    for idx, count, env in worker_tasks[1:]:
        log_info(f"Worker {idx}: {count} empresas.")
        proc = _spawn_worker(env)
        procs.append(proc)
        threading.Thread(target=_pump_worker_output, args=(proc,), daemon=True).start()

    prev_ok = 0
    prev_err = 0
    restart_counts: List[int] = [0 for _ in procs]
    finalized: List[bool] = [False for _ in procs]
    try:
        while True:
            # Detecta workers finalizados e tenta relancar os que ficaram com pendencias.
            for pos, proc in enumerate(procs):
                ret = proc.poll()
                if ret is None or finalized[pos]:
                    continue

                worker_idx, _worker_count, worker_env = worker_tasks[pos]
                worker_file = worker_files[pos]
                pending = _worker_pending_count(worker_file, force_select_pending=True)
                if pending <= 0:
                    finalized[pos] = True
                    continue

                if is_stop_requested():
                    finalized[pos] = True
                    continue

                if restart_counts[pos] < restart_limit:
                    restart_counts[pos] += 1
                    log_warning(
                        "Worker {} desconectado (exit={}). Reiniciando {}/{} com {} empresa(s) pendente(s).",
                        worker_idx,
                        ret,
                        restart_counts[pos],
                        restart_limit,
                        pending,
                    )
                    new_proc = _spawn_worker(worker_env)
                    procs[pos] = new_proc
                    threading.Thread(target=_pump_worker_output, args=(new_proc,), daemon=True).start()
                    continue

                finalized[pos] = True
                log_error(
                    "Worker {} desconectado (exit={}) e excedeu limite de reinicio ({}). "
                    "{} empresa(s) ficaram pendentes.",
                    worker_idx,
                    ret,
                    restart_limit,
                    pending,
                )

            alive = any(proc.poll() is None for proc in procs)
            ok_count, err_count = _count_status_from_files(worker_files)
            if ok_count > prev_ok:
                mark_processed(ok_count - prev_ok)
                prev_ok = ok_count
            if err_count > prev_err:
                mark_error(err_count - prev_err)
                prev_err = err_count

            if not alive:
                break

            if is_stop_requested():
                log_warning("Parada solicitada. Encerrando workers...")
                for proc in procs:
                    if proc.poll() is None:
                        _terminate_process(proc)
                break

            time.sleep(5)
    finally:
        for proc in procs:
            if proc.poll() is None:
                _terminate_process(proc)

    _merge_worker_results(base_data, worker_files)
    _run_final_retry_for_errors()
    ok_count, err_count, unfinished = _recount_selected_status()
    if unfinished:
        log_error(
            "Execucao paralela incompleta: {} empresa(s) ficaram sem status final nos workers.",
            unfinished,
        )
        err_count += unfinished
    _write_empresas_file(base_path, base_data)
    try:
        entries = _collect_run_report_entries(base_data, selected_cnpjs=selected_cnpjs)
        _write_run_report_files(entries, cfg=cfg if isinstance(cfg, dict) else None)
    except Exception as report_exc:
        log_warning(f"Falha ao gerar relatorio consolidado da execucao paralela: {report_exc}")
    _processed = ok_count
    _errors = err_count

    all_ok = all(proc.returncode == 0 for proc in procs if proc.returncode is not None)
    if unfinished:
        all_ok = False
    if is_stop_requested():
        return False, _processed, _errors
    return all_ok, _processed, _errors

def ensure_logged_in(page: Page, usuario: str, senha: str, dbg_dir: pathlib.Path) -> bool:
    """Se estiver na tela de login, refaz o login e retorna True em caso de sucesso."""
    try:
        if is_on_login_screen(page):
            log_info("Sessao expirada detectada. Refazendo login automatico...")
            return bool(_relogin_automatic(page, dbg_dir, usuario, senha, navigate=False))
    except Exception as exc:
        log_error(f"Falha ao validar/refazer login: {exc}")
        return False
    return True


# ---------- Fallback: credenciais direto do config.json raiz ----------
def _ler_credenciais_config_fallback() -> Tuple[str, str]:
    """
    LÃª credentials.username/password de um config.json buscado em:
    - diretÃ³rio atual
    - diretÃ³rio deste arquivo
    - pais (atÃ© a raiz do projeto)

    Levanta ValueError se nÃ£o encontrar/estiver invÃ¡lido.
    """
    name = "config.json"
    cfg_file = _find_config_json()
    if not cfg_file:
        raise ValueError(f"Arquivo de configuraÃ§Ã£o nÃ£o encontrado (procurado como '{name}' na raiz do projeto).")

    try:
        with open(cfg_file, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"config.json invÃ¡lido: {e}")

    creds = cfg.get("credentials") or {}
    usuario = (creds.get("username") or creds.get("usuario") or "").strip()
    senha   = (creds.get("password") or creds.get("senha") or "").strip()
    if not usuario or not senha:
        raise ValueError("Credenciais ausentes no config.json (credentials.username/password).")

    return usuario, senha


# ---------- Helper: voltar para a tela de empresas com seguranÃ§a ----------
def _safe_voltar_empresas(page: Page) -> None:
    """Tenta garantir que voltamos Ã  tela 'Empresas Autorizadas', sem quebrar o fluxo."""
    try:
        abrir_empresas_autorizadas(page)
        time.sleep(0.3)
    except Exception as e:
        log_error(f"(aviso) nÃ£o consegui reabrir Empresas Autorizadas agora: {e}")


# ---------- Rotina completa por EMPRESA ----------
def _rotina_para_empresa(
    page: Page,
    empresa: Dict[str, any],
    usuario: str,
    senha: str,
    dbg_dir: pathlib.Path,
    flow_selection: Optional[Dict[str, bool]] = None,
    execution_report: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Executa todo o pipeline para a empresa ja selecionada na grade.
    Retorna erro apenas para falha critica de inicializacao da empresa.
    """
    critical_errors: List[str] = []
    alerts: List[str] = []
    downloaded_files: List[str] = []
    closed_movements: List[str] = []
    movement_without_action: List[str] = []
    relogins_automaticos = 0
    worker_id = (os.getenv("NM_WORKER_ID") or "").strip()

    def _record_critical(context: str, exc: Exception) -> None:
        critical_errors.append(f"{context}: {exc}")

    def _record_alert(context: str, detail: str) -> None:
        msg = f"{context}: {detail}"[:500]
        alerts.append(msg)

    def _summarize_critical() -> Optional[str]:
        if not critical_errors:
            return None
        head = critical_errors[:3]
        out = "; ".join(head)
        if len(critical_errors) > 3:
            out += f" (+{len(critical_errors) - 3} ocorrencia(s))"
        return out[:500]

    def _check_login() -> None:
        nonlocal relogins_automaticos
        try:
            if is_on_login_screen(page):
                log_warning("Sessao voltou para login. Reautenticando automaticamente...")
                if not _relogin_automatic(page, dbg_dir, usuario, senha, navigate=False):
                    raise RuntimeError("Falha ao refazer login automatico.")
                relogins_automaticos += 1
                abrir_empresas_autorizadas(page)
                selecionar_empresa_por_lista(page, [empresa], col_nome=4)
        except Exception as exc:
            raise RuntimeError(f"Login expirado e re-login falhou: {exc}")

    if flow_selection is None:
        flow = get_flow_selection()
    else:
        flow = _normalize_flow_selection(flow_selection)

    def _enabled(key: str) -> bool:
        return bool(flow.get(key, True))

    enabled_step_keys = [key for key, _label, _group in FLOW_STEPS if _enabled(key)]
    executed_step_keys: Set[str] = set()
    step_results: Dict[str, Dict[str, Any]] = {}

    def _mark_step_executed(key: str) -> None:
        executed_step_keys.add(key)

    def _set_step_result(
        key: str,
        status: str,
        detail: str = "",
        files: Optional[List[str]] = None,
    ) -> None:
        meta = FLOW_META.get(key, {"label": key, "group": ""})
        step_results[key] = {
            "label": meta.get("label", key),
            "group": meta.get("group", ""),
            "status": status,
            "detail": detail,
            "files": list(files or []),
        }

    download_keys = [
        "baixar_notas_emitidas",
        "baixar_notas_recebidas",
        "baixar_relatorio_nota_nacional_recebidas",
        "baixar_relatorio_nota_nacional_recebidas_intermediario",
        "baixar_relatorio_nota_nacional_emetidas",
        "baixar_extrato_issqn",
        "abrir_emissao_guias",
    ]
    downloads_enabled = any(_enabled(k) for k in download_keys)
    downloads_dir: Optional[Path] = None
    cfg_runtime = _read_config_json()
    cfg_runtime = cfg_runtime if isinstance(cfg_runtime, dict) else {}

    download_retry_attempts = 3
    download_retry_wait_sec = 2.0
    try:
        raw_attempts = (os.getenv("NM_DOWNLOAD_RETRY_ATTEMPTS") or "").strip()
        if raw_attempts:
            download_retry_attempts = int(raw_attempts)
        else:
            download_retry_attempts = int(cfg_runtime.get("download_retry_attempts", 3))
    except Exception:
        download_retry_attempts = 3
    try:
        raw_wait = (os.getenv("NM_DOWNLOAD_RETRY_WAIT_SEC") or "").strip()
        if raw_wait:
            download_retry_wait_sec = float(raw_wait)
        else:
            download_retry_wait_sec = float(cfg_runtime.get("download_retry_wait_sec", 2.0))
    except Exception:
        download_retry_wait_sec = 2.0

    download_retry_attempts = max(1, min(10, int(download_retry_attempts)))
    download_retry_wait_sec = max(0.2, min(15.0, float(download_retry_wait_sec)))

    def _extract_download_paths(result: object) -> List[str]:
        candidates: List[Path] = []
        if isinstance(result, Path):
            candidates.append(result)
        elif isinstance(result, str) and result.strip():
            candidates.append(Path(result))
        elif isinstance(result, (list, tuple, set)):
            for item in result:
                if isinstance(item, Path):
                    candidates.append(item)
                elif isinstance(item, str) and item.strip():
                    candidates.append(Path(item))
        files: List[str] = []
        for path_obj in candidates:
            try:
                if path_obj.exists():
                    files.append(str(path_obj))
            except Exception:
                continue
        return files

    def _run_download_step(
        key: str,
        step_label: str,
        runner: Callable[[], object],
    ) -> Optional[object]:
        if not _enabled(key):
            return None
        _mark_step_executed(key)
        last_exc: Optional[Exception] = None
        had_runner_exception = False
        saw_no_file = False
        for attempt in range(1, download_retry_attempts + 1):
            try:
                _check_login()
                result = runner()
                files = _extract_download_paths(result)
                if files:
                    downloaded_files.extend(files)
                    _set_step_result(key, "arquivo_baixado", f"Arquivo confirmado na tentativa {attempt}.", files)
                    if attempt > 1:
                        log_info(f"{step_label}: download confirmado na tentativa {attempt}.")
                    return result
                if isinstance(result, bool) and result:
                    _set_step_result(key, "executada_sem_arquivo", f"Etapa executada sem arquivo (tentativa {attempt}).")
                    log_info(f"{step_label}: executada sem arquivo.")
                    return result
                saw_no_file = True
                last_exc = RuntimeError("sem arquivo disponivel para baixar")
            except Exception as e:
                last_exc = e
                had_runner_exception = True
                log_warning(f"Falha em {step_label} (tentativa {attempt}/{download_retry_attempts}): {e}")

            if attempt < download_retry_attempts:
                log_info(
                    f"{step_label}: tentando novamente em {download_retry_wait_sec:.1f}s "
                    f"({attempt + 1}/{download_retry_attempts})..."
                )
                with suppress(Exception):
                    page.wait_for_timeout(int(download_retry_wait_sec * 1000))

        if had_runner_exception and last_exc is not None:
            _record_alert(step_label, str(last_exc))
            _set_step_result(key, "falha_execucao", str(last_exc))
            log_warning(
                f"{step_label}: falhou apos {download_retry_attempts} tentativa(s). "
                "Seguindo para a proxima etapa."
            )
            return None

        if saw_no_file:
            _set_step_result(key, "sem_arquivo", "Nao havia arquivo para baixar.")
            log_info(
                f"{step_label}: nao houve arquivo nesta empresa "
                f"apos {download_retry_attempts} tentativa(s)."
            )
            return None

        _set_step_result(key, "executada", "Etapa concluida sem arquivo.")
        return None

    def _run_movement_step(
        key: str,
        open_fn: Callable[[Page], None],
        close_fn: Callable[[Page], bool],
        open_label: str,
    ) -> None:
        if not _enabled(key):
            return
        _mark_step_executed(key)
        try:
            _check_login()
            log_info(open_label)
            open_fn(page)
            log_info("Verificando tabela...")
            changed = bool(close_fn(page))
            label = FLOW_LABELS.get(key, key)
            if changed:
                closed_movements.append(label)
                _set_step_result(key, "encerrada", "Movimentacao encerrada.")
            else:
                movement_without_action.append(label)
                _set_step_result(key, "sem_acao", "Sem acao pendente para encerrar.")
                log_warning(f"{label} sem acao pendente. Seguindo.")
        except Exception as e:
            _record_alert(FLOW_LABELS.get(key, key), str(e))
            _set_step_result(key, "falha_execucao", str(e))
            log_warning(f"Falha em {FLOW_LABELS.get(key, key)}: {e}. Seguindo.")

    startup_ok = True
    try:
        _check_login()
    except Exception as exc:
        startup_ok = False
        _record_critical("Inicializacao da empresa", exc)
        log_error(f"Falha critica ao iniciar empresa: {exc}")

    if downloads_enabled:
        cfg_downloads = cfg_runtime
        downloads_dir = ensure_dir(_resolve_downloads_root(cfg_downloads))
        os.environ["NM_DOWNLOADS_DIR"] = str(downloads_dir)
        log_info(f"Diretorio de downloads ativo: {downloads_dir}")

    if startup_ok:
        _run_movement_step(
            "encerrar_mov_mensal",
            abrir_movimentacao_mensal,
            encerrar_mov_mensal,
            "Abrindo Movimentacoes...",
        )
        _run_movement_step(
            "encerrar_mov_ret_mensal",
            abrir_movimentacao_ret_mensal,
            encerrar_mov_ret_mensal,
            "Abrindo Movimentacoes de Retencao...",
        )
        _run_movement_step(
            "encerrar_mov_nfse_mensal",
            abrir_movimentacao_mensal_nfse,
            encerrar_mov_nfse_mensal,
            "Abrindo Movimentacoes de NFSE...",
        )
        _run_movement_step(
            "enncerrar_mov_mensal_nacioal",
            abrir_movimentacao_mensal_nacional,
            enncerrar_mov_mensal_nacioal,
            "Abrindo Movimentacoes - Nota Nacional...",
        )
        _run_movement_step(
            "encerrar_mov_ret_mensal_nacional",
            abrir_movimentacao_ret_mensal_nacional,
            encerrar_mov_ret_mensal_nacional,
            "Abrindo Movimentacoes - Ret. Nota Nacional...",
        )

    if startup_ok and (_enabled("baixar_notas_emitidas") or _enabled("baixar_notas_recebidas")):
        log_info("Abrindo Escrituracao...")
        try:
            _check_login()
            abrir_escrituracao_contabilidade(page)
        except Exception as e:
            _record_alert("Abrir Escrituracao", str(e))
            log_warning(f"Falha ao abrir Escrituracao: {e}. Seguindo com as demais etapas.")

    if startup_ok and _enabled("baixar_notas_emitidas"):
        log_info("Baixando Notas Emitidas...")
        arq_emitidas = _run_download_step(
            "baixar_notas_emitidas",
            "Baixar Notas Emitidas",
            lambda: baixar_notas_emitidas(page, saida_dir=downloads_dir),
        )
        if arq_emitidas:
            time.sleep(1)

    if startup_ok and _enabled("baixar_notas_recebidas"):
        log_info("Baixando Notas Recebidas...")
        _run_download_step(
            "baixar_notas_recebidas",
            "Baixar Notas Recebidas",
            lambda: baixar_notas_recebidas(page, saida_dir=downloads_dir),
        )

    if startup_ok and _enabled("baixar_relatorio_nota_nacional_recebidas"):
        log_info("Baixando Relatorio Nota Nacional Recebidas...")
        _run_download_step(
            "baixar_relatorio_nota_nacional_recebidas",
            "Relatorio Nota Nacional Recebidas",
            lambda: baixar_relatorio_nota_nacional_recebidas(page, saida_dir=downloads_dir),
        )

    if startup_ok and _enabled("baixar_relatorio_nota_nacional_recebidas_intermediario"):
        log_info("Baixando Relatorio Nota Nacional Recebidas (Intermediario)...")
        _run_download_step(
            "baixar_relatorio_nota_nacional_recebidas_intermediario",
            "Relatorio Nota Nacional Recebidas (Intermediario)",
            lambda: baixar_relatorio_nota_nacional_recebidas_intermediario(page, saida_dir=downloads_dir),
        )

    if startup_ok and _enabled("baixar_relatorio_nota_nacional_emetidas"):
        log_info("Baixando Relatorio Nota Nacional Emitidas...")
        _run_download_step(
            "baixar_relatorio_nota_nacional_emetidas",
            "Relatorio Nota Nacional Emitidas",
            lambda: baixar_relatorio_nota_nacional_emetidas(page, saida_dir=downloads_dir),
        )

    if startup_ok and _enabled("baixar_extrato_issqn"):
        log_info("Baixando Extrato Detalhado ISSQN (Tomado/Prestado)...")
        _run_download_step(
            "baixar_extrato_issqn",
            "Extrato Detalhado ISSQN (Tomado/Prestado)",
            lambda: baixar_extrato_issqn(page, saida_dir=downloads_dir),
        )

    if startup_ok and _enabled("abrir_emissao_guias"):
        log_info("Abrindo Emissao de Guias...")
        _run_download_step(
            "abrir_emissao_guias",
            "Emissao de Guias",
            lambda: abrir_emissao_guias(page),
        )

    missing_steps = [key for key in enabled_step_keys if key not in executed_step_keys]
    if missing_steps:
        missing_labels = [FLOW_LABELS.get(key, key) for key in missing_steps]
        missing_msg = "Etapas habilitadas nao executadas: " + ", ".join(missing_labels)
        _record_alert("Fluxo incompleto", missing_msg)
        log_warning(missing_msg)

    report_payload: Dict[str, Any] = {
        "worker_id": worker_id,
        "empresa": (empresa.get("empresa") or empresa.get("Empresa") or "").strip(),
        "cnpj": _digits(empresa.get("cnpj")),
        "startup_ok": startup_ok,
        "relogins_automaticos": relogins_automaticos,
        "enabled_step_keys": enabled_step_keys,
        "executed_step_keys": sorted(executed_step_keys),
        "step_results": step_results,
        "downloaded_files": sorted(set(downloaded_files)),
        "closed_movements": closed_movements,
        "movement_without_action": movement_without_action,
        "alerts": alerts,
        "critical_errors": critical_errors,
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    if execution_report is not None:
        execution_report.clear()
        execution_report.update(_json_safe(report_payload))

    return (len(critical_errors) == 0), _summarize_critical()
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def main(parallel_workers: Optional[int] = None) -> Tuple[bool, int, int]:
    """Fluxo completo (login ??' Empresas Autorizadas ??' rotina por fila Sel=True).
    Retorna: (ok, processadas, erros)
    """
    global _processed, _errors
    _processed = 0
    _errors = 0
    flow_selection = get_flow_selection()
    _set_flow_env(flow_selection)

    if not os.getenv("NM_PARALLEL_WORKER"):
        workers = _resolve_parallel_workers(parallel_workers)
        cfg_local = _read_config_json()
        force_parallel = False
        raw_force = (os.getenv("NM_FORCE_PARALLEL") or "").strip().lower()
        if raw_force in ("1", "true", "yes", "y", "sim"):
            force_parallel = True
        if isinstance(cfg_local, dict) and bool(cfg_local.get("force_parallel")):
            force_parallel = True
        if _manual_login_enabled(cfg_local if isinstance(cfg_local, dict) else None) and workers > 1 and not force_parallel:
            log_warning("Login manual ativo: executando em modo sequencial (1 worker). Use NM_FORCE_PARALLEL=1 para permitir varios workers.")
            workers = 1
        if workers > 1:
            return _run_parallel(workers)

    try:
        reset_stop_flag()

        # 1) dados de login
        try:
            usuario, senha, _extra = ler_credencial()
        except Exception as e:
            log_error(f"FALHA ao ler credenciais via ler_credencial(): {e} â€¢ tentando config.json na raizâ€¦")
            usuario, senha = _ler_credenciais_config_fallback()

        worker_id = (os.getenv("NM_WORKER_ID") or "").strip()
        dbg_root = pathlib.Path(tempfile.gettempdir()) / "NotaManausRPA" / "debug"
        if worker_id:
            dbg_root = dbg_root / f"worker_{worker_id}"
        dbg_dir = ensure_dir(dbg_root)
        log_info(f"Debug em: {dbg_dir}")

        # 2) navegador/pÃ¡gina unificados
        page = _ensure_page()

        # 3) login
        log_info("Abrindo tela de loginâ€¦")
        if not _login_with_mode(page, dbg_dir, usuario, senha, navigate=True):
            log_error("Falha no login inicial.")
            close_browser()
            return False, _processed, _errors
        log_info("Login realizado")
        time.sleep(0.5)
        enabled_steps = [label for key, label, _group in FLOW_STEPS if flow_selection.get(key)]
        if enabled_steps:
            log_info("Etapas habilitadas: " + ", ".join(enabled_steps))
        else:
            log_warning("Nenhuma etapa habilitada. Fluxo nao executara encerramentos/downloads.")
        if is_stop_requested():
            log_info("Processo interrompido pelo usuÃ¡rio.")
            close_browser()
            return False, _processed, _errors

        empresas_path = _resolve_empresas_json_path()
        selected_cnpjs_run = {
            _digits(row.get("cnpj"))
            for row in _read_empresas_file(empresas_path)
            if isinstance(row, dict) and bool(row.get("Sel")) and _digits(row.get("cnpj"))
        }

        # 4) loop: processa empresas com Sel=True atÃ© acabar (tolerante a falhas)
        while True:
            if is_stop_requested():
                log_info("Interrompido pelo usuÃ¡rio (antes de selecionar prÃ³xima empresa).")
                break

            # Recarrega a grade de empresas (estado sempre â€œfrescoâ€)
            try:
                log_info("Navegando para pÃ¡gina de empresasâ€¦")
                if not ensure_logged_in(page, usuario, senha, dbg_dir):
                    log_warning("Falha ao revalidar login; tentando novamente no prÃ³ximo ciclo.")
                    continue
                abrir_empresas_autorizadas(page)
                time.sleep(0.4)
            except Exception as e:
                log_warning(f"Falha ao abrir Empresas Autorizadas: {e}")
                continue

            empresa = primeira_selecionada()  # dict com 'empresa', 'cnpj', 'situacao', 'Sel'
            if not empresa:
                log_info("Nao ha mais empresas selecionadas (Sel=True). Fim.")
                break

            nome = (empresa.get("empresa") or empresa.get("Empresa") or "").strip()
            cnpj = (empresa.get("cnpj") or "").strip()
            log_info(f"âž¡ï¸ PrÃ³xima empresa: {nome} ({cnpj})")

            # Tudo por empresa fica isolado â€” qualquer erro cai no except e segue a prÃ³xima
            try:
                empresa_report: Dict[str, Any] = {}
                # 1) Selecionar na grade
                try:
                    selecionar_empresa_por_lista(
                        page,
                        [empresa],   # passa o dict; a funÃ§Ã£o jÃ¡ suporta
                        col_nome=4
                    )
                except Exception as e_sel:
                    log_error(f"âŒ Falha ao selecionar '{nome}' na grade: {e_sel}")
                    empresa_report = {
                        "worker_id": (os.getenv("NM_WORKER_ID") or "").strip(),
                        "empresa": nome,
                        "cnpj": _digits(cnpj),
                        "startup_ok": False,
                        "enabled_step_keys": [k for k, _l, _g in FLOW_STEPS if flow_selection.get(k, True)],
                        "executed_step_keys": [],
                        "step_results": {},
                        "downloaded_files": [],
                        "closed_movements": [],
                        "movement_without_action": [],
                        "alerts": [f"Selecao da empresa falhou: {e_sel}"],
                        "critical_errors": [f"Selecao da empresa falhou: {e_sel}"],
                        "relogins_automaticos": 0,
                        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    marcar_empresa_processada_por_cnpj(
                        cnpj,
                        ok=False,
                        obs=f"selecao: {e_sel}",
                        deselect_on_error=True,
                        report=empresa_report,
                    )
                    mark_error(1)
                    # volta Ã  grade e segue para a prÃ³xima
                    _safe_voltar_empresas(page)
                    continue

                # 2) Rodar a rotina para a empresa jÃ¡ selecionada
                try:
                    ok_exec, err_exec = _rotina_para_empresa(
                        page,
                        empresa,
                        usuario,
                        senha,
                        dbg_dir,
                        flow_selection,
                        execution_report=empresa_report,
                    )
                except Exception as e_run:
                    log_error(f"âŒ Erro na rotina da empresa '{nome}': {e_run}")
                    if not empresa_report:
                        empresa_report = {
                            "worker_id": (os.getenv("NM_WORKER_ID") or "").strip(),
                            "empresa": nome,
                            "cnpj": _digits(cnpj),
                            "startup_ok": False,
                            "enabled_step_keys": [k for k, _l, _g in FLOW_STEPS if flow_selection.get(k, True)],
                            "executed_step_keys": [],
                            "step_results": {},
                            "downloaded_files": [],
                            "closed_movements": [],
                            "movement_without_action": [],
                            "alerts": [f"Rotina da empresa falhou: {e_run}"],
                            "critical_errors": [f"Rotina da empresa falhou: {e_run}"],
                            "relogins_automaticos": 0,
                            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    marcar_empresa_processada_por_cnpj(
                        cnpj,
                        ok=False,
                        obs=f"rotina: {e_run}",
                        deselect_on_error=True,
                        report=empresa_report,
                    )
                    mark_error(1)
                    # mesmo com erro, volta Ã  grade e segue
                    _safe_voltar_empresas(page)
                    continue

                if not ok_exec:
                    obs = err_exec or "falha em etapa(s)"
                    log_warning(f"Empresa '{nome}' concluida com erros: {obs}")
                    marcar_empresa_processada_por_cnpj(
                        cnpj,
                        ok=False,
                        obs=obs,
                        deselect_on_error=True,
                        report=empresa_report,
                    )
                    mark_error(1)
                    _safe_voltar_empresas(page)
                    continue

                # 3) Sucesso â€” marcar como processada e seguir
                if marcar_empresa_processada_por_cnpj(cnpj, ok=True, report=empresa_report):
                    log_info("Empresa marcada como processada no JSON (Sel=False).")
                else:
                    log_error("âš ï¸ NÃ£o foi possÃ­vel marcar como processada (CNPJ nÃ£o encontrado no JSON).")
                mark_processed(1)

            except Exception as e_inesp:
                # Qualquer surpresa fora dos blocos acima
                log_error(f"âš ï¸ Erro inesperado no ciclo da empresa '{nome}': {e_inesp}")
                try:
                    unexpected_report = {
                        "worker_id": (os.getenv("NM_WORKER_ID") or "").strip(),
                        "empresa": nome,
                        "cnpj": _digits(cnpj),
                        "startup_ok": False,
                        "enabled_step_keys": [k for k, _l, _g in FLOW_STEPS if flow_selection.get(k, True)],
                        "executed_step_keys": [],
                        "step_results": {},
                        "downloaded_files": [],
                        "closed_movements": [],
                        "movement_without_action": [],
                        "alerts": [f"Erro inesperado: {e_inesp}"],
                        "critical_errors": [f"Erro inesperado: {e_inesp}"],
                        "relogins_automaticos": 0,
                        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    marcar_empresa_processada_por_cnpj(
                        cnpj,
                        ok=False,
                        obs=f"inesperado: {e_inesp}",
                        deselect_on_error=True,
                        report=unexpected_report,
                    )
                except Exception:
                    pass
                mark_error(1)

            finally:
                # SEMPRE tenta retornar Ã  grade para a prÃ³xima iteraÃ§Ã£o
                _safe_voltar_empresas(page)
                # pequeno respiro para estabilizar UI
                time.sleep(0.3)

        if not os.getenv("NM_PARALLEL_WORKER"):
            try:
                final_data = _read_empresas_file(empresas_path)
                entries = _collect_run_report_entries(final_data, selected_cnpjs=selected_cnpjs_run)
                _write_run_report_files(entries, cfg=_read_config_json())
            except Exception as report_exc:
                log_warning(f"Falha ao gerar relatorio consolidado da execucao: {report_exc}")

        log_info("Pronto! ExecuÃ§Ã£o de fila concluÃ­da. Navegador permanece aberto.")
        return True, _processed, _errors

    except Exception as exc:
        log_error(f"ERRO FATAL: {exc}")
        close_browser()
        return False, _processed, _errors


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ utilitÃ¡rios de navegador â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def close_browser() -> None:
    global _browser, _playwright, _context, _page
    try:
        if _context:
            _context.close()
            _context = None
        if _browser:
            _browser.close()
            _browser = None
            log_info("Navegador fechado.")
        if _playwright:
            _playwright.stop()
            _playwright = None
            log_info("Playwright finalizado.")
    except Exception as exc:
        log_error(f"Erro ao fechar navegador: {exc}")
    finally:
        _page = None


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ suporte ao botÃ£o "Carregar empresas" (GUI) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def get_empresas() -> List[Dict]:
    """
    Chamado pela GUI.
    - Garante/abre um Page
    - Usa o mÃ³dulo que SEMPRE faz login e abre 'Empresas Autorizadas'
    - Salva JSON/CSV em ./data
    - Retorna a lista filtrada (SituaÃ§Ã£o contÃ©m 'Liberado')
    """
    from core.empresas_autorizadas import carregar_empresas_liberadas
    page = _ensure_page()

    out_dir = Path(__file__).resolve().parent / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_json = str(out_dir / "empresas_liberadas.json")
    out_csv  = str(out_dir / "empresas_liberadas.csv")

    log_info("Iniciando fluxo 'Carregar empresas'â€¦")
    empresas = carregar_empresas_liberadas(page, out_json=out_json, out_csv=out_csv)
    log_info(f"Fluxo 'Carregar empresas' concluÃ­do. Total: {len(empresas)}")
    return empresas


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
if __name__ == "__main__":
    ok, proc, errs = main()
    log_info(f"Resumo execuÃ§Ã£o â€¢ OK={ok} â€¢ Processadas={proc} â€¢ Erros={errs}")

