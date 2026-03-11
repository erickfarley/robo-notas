from __future__ import annotations

import asyncio
import contextlib
import json
import os
import queue
import sys
import threading
import tempfile
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Callable

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask

import main
from utils.periodo import get_periodo, set_periodo


ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "web" / "static"
INDEX_FILE = STATIC_DIR / "index.html"
CONFIG_FILE = ROOT / "config.json"
EMPRESAS_FILE = ROOT / "data" / "empresas_liberadas.json"

LOG_LIMIT = 5000


app = FastAPI()
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


state_lock = threading.Lock()
log_lock = threading.Lock()

log_buffer: List[Dict[str, str]] = []

state: Dict[str, Any] = {
    "running": False,
    "started_at": None,
    "status_text": "parado",
    "load_empresas_running": False,
    "last_run": {
        "at": "",
        "ok": None,
        "processed": 0,
        "errors": 0,
        "duration": 0,
    },
}

stats: Dict[str, int] = {"processed": 0, "errors": 0}


def _now_ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _normalize_level(level: str) -> str:
    raw = (level or "INFO").strip().upper()
    synonyms = {
        "ERRO": "ERROR",
        "EXCEPTION": "ERROR",
        "FATAL": "ERROR",
        "WARNING": "WARN",
        "AVISO": "WARN",
        "SUCESSO": "SUCCESS",
        "OK": "SUCCESS",
        "INFORMACAO": "INFO",
    }
    lvl = synonyms.get(raw, raw)
    return lvl if lvl in ("INFO", "SUCCESS", "WARN", "ERROR") else "INFO"


def log_message(message: str, level: str = "INFO") -> None:
    entry = {"ts": _now_ts(), "level": _normalize_level(level), "message": message}
    with log_lock:
        log_buffer.append(entry)
        if len(log_buffer) > LOG_LIMIT:
            del log_buffer[: len(log_buffer) - LOG_LIMIT]


worker_queue: "queue.Queue[Callable[[], None]]" = queue.Queue()


def _enqueue_task(task: Callable[[], None]) -> None:
    worker_queue.put(task)


def _worker_loop() -> None:
    while True:
        task = worker_queue.get()
        try:
            task()
        except Exception as exc:
            log_message(f"[Worker] Falha ao executar tarefa: {exc}", "ERROR")
        finally:
            worker_queue.task_done()


class _Redirect:
    def __init__(self, level: str = "INFO") -> None:
        self.level = level

    def write(self, txt: str) -> None:
        if txt:
            for line in txt.splitlines(True):
                msg = line.rstrip("\n")
                if msg:
                    log_message(msg, self.level)

    def flush(self) -> None:
        return


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _resolve_downloads_dir(raw_downloads: str) -> Path:
    download_dir = Path(str(raw_downloads or "").strip()) if str(raw_downloads or "").strip() else (ROOT / "downloads")
    try:
        download_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        download_dir = ROOT / "downloads"
        download_dir.mkdir(parents=True, exist_ok=True)
    return download_dir


def _apply_runtime_login_mode(cfg: Any = None) -> None:
    cfg_obj = cfg if isinstance(cfg, dict) else _read_json(CONFIG_FILE, {})
    if not isinstance(cfg_obj, dict):
        cfg_obj = {}
    manual = bool(cfg_obj.get("manual_login", False))
    os.environ["NM_MANUAL_LOGIN"] = "1" if manual else "0"
    try:
        timeout_val = max(30, int(cfg_obj.get("manual_login_timeout_sec", 900)))
    except Exception:
        timeout_val = 900
    os.environ["NM_MANUAL_LOGIN_TIMEOUT_SEC"] = str(timeout_val)


def _digits(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())


def _read_empresas() -> List[Dict[str, Any]]:
    data = _read_json(EMPRESAS_FILE, [])
    return data if isinstance(data, list) else []


def _calc_columns(data: List[Dict[str, Any]]) -> List[str]:
    base = ["Sel", "empresa", "cnpj", "situacao"]
    extras: List[str] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        for k in row.keys():
            if k == "Sel":
                continue
            kl = k.lower()
            if kl == "empresa":
                continue
            if k not in base and k not in extras and kl not in (c.lower() for c in (base + extras)):
                extras.append(k)
    return base + extras


def _company_match(row: Dict[str, Any], upd: Dict[str, Any]) -> bool:
    cnpj_u = _digits(str(upd.get("cnpj", "")))
    if cnpj_u:
        return _digits(str(row.get("cnpj", ""))) == cnpj_u
    nome_u = (upd.get("empresa") or upd.get("Empresa") or "").strip().lower()
    nome_r = (row.get("empresa") or row.get("Empresa") or "").strip().lower()
    return bool(nome_u and nome_u == nome_r)


def _update_empresas(updates: List[Dict[str, Any]]) -> bool:
    data = _read_empresas()
    if not data:
        return False
    changed = False
    for upd in updates:
        for row in data:
            if isinstance(row, dict) and _company_match(row, upd):
                row["Sel"] = bool(upd.get("Sel", False))
                changed = True
                break
    if changed:
        _write_json(EMPRESAS_FILE, data)
    return changed


def _mark_all(value: bool) -> None:
    data = _read_empresas()
    for row in data:
        if isinstance(row, dict):
            row["Sel"] = bool(value)
    _write_json(EMPRESAS_FILE, data)


def _get_counts() -> Dict[str, int]:
    empresas = _read_empresas()
    total = len(empresas)
    selected = sum(1 for e in empresas if isinstance(e, dict) and bool(e.get("Sel")))
    processed = stats["processed"]
    errors = stats["errors"]
    waiting = max(total - (processed + errors), 0)
    return {
        "total": total,
        "selected": selected,
        "processed": processed,
        "errors": errors,
        "waiting": waiting,
    }


def _set_status(text: str) -> None:
    with state_lock:
        state["status_text"] = text


def _normalize_flow_selection(selection: Any, keys: List[str]) -> Dict[str, bool]:
    out: Dict[str, bool] = {k: True for k in keys}
    if isinstance(selection, dict):
        for key in keys:
            if key in selection:
                out[key] = bool(selection.get(key))
    elif isinstance(selection, list):
        picked = {str(k) for k in selection}
        out = {k: (k in picked) for k in keys}
    return out


def _on_processed(n: int = 1) -> None:
    with state_lock:
        stats["processed"] += int(n)


def _on_error(n: int = 1) -> None:
    with state_lock:
        stats["errors"] += int(n)


def _run_robot() -> None:
    stdout_bak, stderr_bak = sys.stdout, sys.stderr
    sys.stdout = _Redirect("INFO")
    sys.stderr = _Redirect("ERROR")
    ok = False
    try:
        _set_status("em execucao")
        cfg = _read_json(CONFIG_FILE, {})
        _apply_runtime_login_mode(cfg)
        log_message(
            f"Modo de login ativo: {'manual' if bool((cfg if isinstance(cfg, dict) else {}).get('manual_login')) else 'automatico'}.",
            "INFO",
        )
        res = main.main()
        ok = True
        proc = stats["processed"]
        errs = stats["errors"]
        if isinstance(res, tuple) and len(res) >= 3:
            ok, proc, errs = bool(res[0]), int(res[1]), int(res[2])
        stats["processed"] = proc
        stats["errors"] = errs
        if ok:
            log_message("[Resumo] Execucao concluida.", "SUCCESS")
        elif main.is_stop_requested():
            log_message("[Resumo] Execucao interrompida.", "WARN")
        else:
            log_message("[Resumo] Execucao finalizada com alertas.", "WARN")
        log_message(f"[Resumo] Processadas: {proc}", "SUCCESS")
        if errs:
            log_message(f"[Resumo] Erros: {errs}", "ERROR")
    except Exception as exc:
        log_message(f"[Erro] Excecao na execucao: {exc}", "ERROR")
    finally:
        finished_at = time.time()
        with state_lock:
            started_at = state.get("started_at") or finished_at
            state["running"] = False
            state["started_at"] = None
            state["status_text"] = "parado"
            state["last_run"] = {
                "at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "ok": bool(ok),
                "processed": int(stats["processed"]),
                "errors": int(stats["errors"]),
                "duration": int(finished_at - float(started_at)),
            }
        sys.stdout, sys.stderr = stdout_bak, stderr_bak


def _start_robot() -> bool:
    with state_lock:
        if state["running"]:
            return False
        state["running"] = True
        state["started_at"] = time.time()
        state["status_text"] = "iniciando"
    stats["processed"] = 0
    stats["errors"] = 0
    main.set_gui_hooks(on_processed=_on_processed, on_error=_on_error)
    main.reset_stop_flag()
    log_message("Execucao iniciada.", "INFO")
    _enqueue_task(_run_robot)
    return True


def _load_empresas_async() -> None:
    try:
        log_message("Carregando empresas (Liberado)...", "INFO")
        main.get_empresas()
        data = _read_empresas()
        log_message(f"Empresas carregadas: {len(data)}", "SUCCESS")
    except Exception as exc:
        log_message(f"Falha ao carregar empresas: {exc}", "ERROR")
    finally:
        with state_lock:
            state["load_empresas_running"] = False


def _scheduler_next_month(dt: datetime) -> datetime:
    y, m = dt.year, dt.month
    m = 1 if m == 12 else m + 1
    y = y + 1 if dt.month == 12 else y
    day = min(dt.day, [31, 29 if y % 4 == 0 and (y % 100 != 0 or y % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1])
    return datetime(y, m, day, dt.hour, dt.minute, 0)


def _scheduler_loop() -> None:
    while True:
        time.sleep(15)
        cfg = _read_json(CONFIG_FILE, {})
        sched = cfg.get("scheduler", {}) if isinstance(cfg, dict) else {}
        enabled = bool(sched.get("enabled", False))
        if not enabled:
            continue
        with state_lock:
            if state["running"]:
                continue
        next_run_str = sched.get("next_run") or ""
        next_run = None
        if next_run_str:
            try:
                next_run = datetime.strptime(next_run_str, "%Y-%m-%d %H:%M:%S")
            except Exception:
                next_run = None
        if not next_run:
            try:
                dt = datetime.strptime(
                    f"{sched.get('date', '')} {sched.get('time', '00:00')}", "%Y-%m-%d %H:%M"
                )
                next_run = dt
            except Exception:
                continue
        now = datetime.now()
        if now >= next_run:
            if _start_robot():
                log_message("[Agendador] Execucao iniciada.", "INFO")
            if bool(sched.get("recurring", False)):
                sched["next_run"] = _scheduler_next_month(next_run).strftime("%Y-%m-%d %H:%M:%S")
                cfg["scheduler"] = sched
            else:
                sched["enabled"] = False
                sched["next_run"] = ""
                cfg["scheduler"] = sched
            _write_json(CONFIG_FILE, cfg)


@app.on_event("startup")
def _start_background_workers() -> None:
    # Execucao manual via /api/start e automatica quando agendamento estiver ativo.
    _apply_runtime_login_mode()
    threading.Thread(target=_worker_loop, daemon=True).start()
    threading.Thread(target=_scheduler_loop, daemon=True).start()


@app.get("/")
def index() -> FileResponse:
    return FileResponse(str(INDEX_FILE))


@app.get("/api/status")
def api_status() -> Dict[str, Any]:
    counts = _get_counts()
    with state_lock:
        running = state["running"]
        started_at = state["started_at"]
        status_text = state["status_text"]
        loading = state["load_empresas_running"]
        last_run = dict(state.get("last_run") or {})
    elapsed = int(time.time() - started_at) if started_at else 0
    return {
        "running": running,
        "loading_empresas": loading,
        "status_text": status_text,
        "elapsed": elapsed,
        "browser_open": bool(getattr(main, "is_browser_open", lambda: False)()),
        "last_run": last_run,
        **counts,
    }


@app.post("/api/start")
def api_start() -> Dict[str, Any]:
    ok = _start_robot()
    return {"ok": ok, "error": "" if ok else "running"}


@app.post("/api/stop")
def api_stop() -> Dict[str, Any]:
    main.set_stop_flag()
    _set_status("solicitando parada")
    log_message("Parada solicitada.", "WARN")
    return {"ok": True}


@app.post("/api/close-browser")
def api_close_browser() -> Dict[str, Any]:
    try:
        main.set_stop_flag()
        _set_status("solicitando parada")
        log_message("Fechamento do navegador solicitado.", "WARN")
        _enqueue_task(main.close_browser)
        return {"ok": True, "queued": True}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@app.get("/api/config")
def api_get_config() -> Dict[str, Any]:
    cfg = _read_json(CONFIG_FILE, {})
    return cfg if isinstance(cfg, dict) else {}


@app.post("/api/config")
def api_set_config(payload: Dict[str, Any]) -> Dict[str, Any]:
    cfg = _read_json(CONFIG_FILE, {})
    if not isinstance(cfg, dict):
        cfg = {}
    raw_downloads = str(payload.get("downloads_dir", "") or "").strip()
    download_dir = _resolve_downloads_dir(raw_downloads)
    cfg["downloads_dir"] = str(download_dir)
    cfg["close_browser_after"] = bool(payload.get("close_browser_after", False))
    cfg["headless"] = bool(payload.get("headless", False))
    cfg["manual_login"] = bool(payload.get("manual_login", False))
    creds = payload.get("credentials") or {}
    cfg["credentials"] = {
        "username": str(creds.get("username", "") or ""),
        "password": str(creds.get("password", "") or ""),
    }
    _write_json(CONFIG_FILE, cfg)
    _apply_runtime_login_mode(cfg)
    return {"ok": True}


@app.get("/api/downloads/archive")
def api_downloads_archive() -> FileResponse:
    cfg = _read_json(CONFIG_FILE, {})
    cfg = cfg if isinstance(cfg, dict) else {}
    download_dir = _resolve_downloads_dir(str(cfg.get("downloads_dir", "") or ""))
    files = [p for p in download_dir.rglob("*") if p.is_file()]
    if not files:
        raise HTTPException(status_code=404, detail="Nenhum arquivo encontrado na pasta de downloads.")

    zip_path = Path(tempfile.gettempdir()) / f"nota_manaus_downloads_{time.time_ns()}.zip"
    try:
        with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for file_path in files:
                arcname = str(file_path.relative_to(download_dir))
                zf.write(file_path, arcname=arcname)
    except Exception as exc:
        if zip_path.exists():
            with contextlib.suppress(Exception):
                zip_path.unlink()
        raise HTTPException(status_code=500, detail=f"Falha ao compactar downloads: {exc}") from exc

    filename = f"downloads_nota_manaus_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    return FileResponse(
        str(zip_path),
        media_type="application/zip",
        filename=filename,
        background=BackgroundTask(lambda p=zip_path: p.unlink(missing_ok=True)),
    )


@app.get("/api/flow")
def api_get_flow() -> Dict[str, Any]:
    steps_raw = getattr(main, "get_flow_steps", lambda: [])()
    groups_raw = getattr(main, "get_flow_groups", lambda: {})()
    selection_raw = getattr(main, "get_flow_selection", lambda: {})()

    steps = steps_raw if isinstance(steps_raw, list) else []
    groups = groups_raw if isinstance(groups_raw, dict) else {}
    keys = [
        str(item.get("key"))
        for item in steps
        if isinstance(item, dict) and str(item.get("key") or "").strip()
    ]
    selection = _normalize_flow_selection(selection_raw, keys)
    return {"steps": steps, "groups": groups, "selection": selection}


@app.post("/api/flow")
def api_set_flow(payload: Dict[str, Any]) -> Dict[str, Any]:
    steps_raw = getattr(main, "get_flow_steps", lambda: [])()
    steps = steps_raw if isinstance(steps_raw, list) else []
    keys = [
        str(item.get("key"))
        for item in steps
        if isinstance(item, dict) and str(item.get("key") or "").strip()
    ]
    raw_selection = payload.get("selection") if isinstance(payload, dict) else {}
    selection = _normalize_flow_selection(raw_selection, keys)

    setter = getattr(main, "set_flow_selection", None)
    if callable(setter):
        setter(selection)

    cfg = _read_json(CONFIG_FILE, {})
    if not isinstance(cfg, dict):
        cfg = {}
    cfg["flow_steps"] = selection
    _write_json(CONFIG_FILE, cfg)
    return {"ok": True, "selection": selection}


@app.get("/api/period")
def api_get_period() -> Dict[str, Any]:
    return get_periodo()


@app.post("/api/period")
def api_set_period(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        mes_de = int(payload["mes_de"])
        ano_de = int(payload["ano_de"])
        mes_ate = int(payload["mes_ate"])
        ano_ate = int(payload["ano_ate"])
        set_periodo(mes_de, ano_de, mes_ate, ano_ate)
        cfg = _read_json(CONFIG_FILE, {})
        if not isinstance(cfg, dict):
            cfg = {}
        cfg["period"] = {
            "mes_de": mes_de,
            "ano_de": ano_de,
            "mes_ate": mes_ate,
            "ano_ate": ano_ate,
        }
        _write_json(CONFIG_FILE, cfg)
        return {"ok": True}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@app.get("/api/scheduler")
def api_get_scheduler() -> Dict[str, Any]:
    cfg = _read_json(CONFIG_FILE, {})
    sched = cfg.get("scheduler", {}) if isinstance(cfg, dict) else {}
    return sched if isinstance(sched, dict) else {}


@app.post("/api/scheduler")
def api_set_scheduler(payload: Dict[str, Any]) -> Dict[str, Any]:
    cfg = _read_json(CONFIG_FILE, {})
    if not isinstance(cfg, dict):
        cfg = {}
    sched = cfg.get("scheduler", {}) if isinstance(cfg.get("scheduler", {}), dict) else {}
    enabled = bool(payload.get("enabled", False))
    recurring = bool(payload.get("recurring", False))
    date_str = str(payload.get("date", "") or "")
    time_str = str(payload.get("time", "00:00") or "00:00")
    next_run = ""
    try:
        if enabled:
            dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
            if recurring and dt <= datetime.now():
                dt = _scheduler_next_month(dt)
            next_run = dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        enabled = False
    sched.update({
        "enabled": enabled,
        "recurring": recurring,
        "date": date_str,
        "time": time_str,
        "next_run": next_run,
    })
    cfg["scheduler"] = sched
    _write_json(CONFIG_FILE, cfg)
    return {"ok": True, "scheduler": sched}


@app.get("/api/empresas")
def api_get_empresas() -> Dict[str, Any]:
    data = _read_empresas()
    return {"columns": _calc_columns(data), "items": data}


@app.post("/api/empresas/mark")
def api_mark_empresas(payload: Dict[str, Any]) -> Dict[str, Any]:
    updates = payload.get("updates") or []
    if not isinstance(updates, list):
        return {"ok": False}
    return {"ok": _update_empresas(updates)}


@app.post("/api/empresas/mark-all")
def api_mark_all(payload: Dict[str, Any]) -> Dict[str, Any]:
    _mark_all(bool(payload.get("value", False)))
    return {"ok": True}


@app.post("/api/empresas/load")
def api_load_empresas() -> Dict[str, Any]:
    with state_lock:
        if state["running"] or state["load_empresas_running"]:
            return {"ok": False, "error": "busy"}
        state["load_empresas_running"] = True
    _enqueue_task(_load_empresas_async)
    return {"ok": True}


@app.get("/api/logs")
def api_logs(since: int = 0) -> Dict[str, Any]:
    with log_lock:
        size = len(log_buffer)
        start = max(0, int(since))
        if start > size:
            start = size
        items = list(log_buffer[start:])
    return {"items": items, "next": start + len(items)}


@app.websocket("/ws/logs")
async def ws_logs(ws: WebSocket) -> None:
    await ws.accept()
    idx = 0
    with log_lock:
        snapshot = list(log_buffer)
    for entry in snapshot:
        await ws.send_json(entry)
    idx = len(snapshot)
    try:
        while True:
            await asyncio.sleep(0.3)
            with log_lock:
                new = log_buffer[idx:]
                idx = len(log_buffer)
            for entry in new:
                await ws.send_json(entry)
    except WebSocketDisconnect:
        return


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("web_app:app", host="127.0.0.1", port=8000, reload=False)
