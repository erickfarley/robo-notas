"""
Rotinas de navegaÃ§Ã£o com Playwright para o portal Nota Manaus.

FunÃ§Ãµes exportadas
â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
abrir_empresas_autorizadas(page) â†’ None
    Leva o navegador atÃ© `/nfse/servlet/hwcminhasempresas`.

abrir_consulta(page) â†’ None
    Primeiro abre "Empresas Autorizadas"; em seguida
    "Notas Fiscais â†’ Emitidas/Recebidas".

OBS.: se um destino jÃ¡ estiver na tela, a chamada Ã© idempotente
(e simplesmente retorna).
"""
import calendar
import json
import os
from contextlib import suppress
from datetime import datetime
from pathlib import Path
import random
from contextlib import suppress
from typing import Callable, Dict, Iterable, Union

from tkinter import Frame
from unicodedata import normalize as _uninorm
from urllib.parse import unquote, urlparse
x = random.uniform(0.0, 1.0)
import pathlib
import re
from typing import Any, List,Mapping, Optional, Tuple
from playwright.sync_api import Page, TimeoutError as PWTimeout
import time

from core.build_url import (_build_empresas_url,
                          _build_movimentacoes_ret_url, _build_movimentacoes_url,
                          _build_movimentacoes_nfce_url, _build_movimentacoes_nacional_url,
                          _build_movimentacoes_ret_nacional_url,
                          _build_contabilidade_url, _build_guias_url, _build_extrato_issqn_url,
                          _build_relatorio_nota_nacional_recebidas_url, _build_relatorio_nota_nacional_emitidas_url)
from utils.logger import log_info, log_error, log_warning
from utils.paths import ensure_dir
from utils.browser_utils import (get_periodo_dict, get_periodo_de, normalize_text,
                              is_select_element, get_selected_text, select_month,
                              select_year, find_period_inputs_in_frame,
                              handle_dialog_and_modal, _MESES_PT)
from core.selectors import (
    X_MENU_PERFIL, X_SUBMENU_EMPRESAS_AUT,
    X_MENU_MOVIMENTACOES, X_SUBMENU_MOV_MENSAL, X_SUBMENU_MOV_MENSAL_NACIONAL,
    X_SUBMENU_MOV_MENSAL_RET_NACIONAL,
    X_BTN_MOV_PESQUISAR, X_BTN_MOV_RET_PESQUISAR,
    X_BTN_MOV_PESQUISAR_LIVRO,
    X_MENU_NOTAS, X_SUBMENU_CONSULTA, X_CNPJ_INPUT,
)

ENC_KEYWORDS = ("enc", "encer", "fech", "final", "encerrar")  # encerrar/fechar/finalizarâ€¦

def _safe_accept_dialog(dlg, context: str = "") -> None:
    try:
        if context:
            log_info(context)
        dlg.accept()
    except Exception as exc:
        log_info(f"(aviso) Dialog ja tratado: {exc}")

def _expect_dialog_event(page: Page, timeout: int):
    if hasattr(page, "expect_dialog"):
        return page.expect_dialog(timeout=timeout)
    return page.expect_event("dialog", timeout=timeout)

def _hook_dialog_once(page: Page, context: str = ""):
    return page.once("dialog", lambda d: _safe_accept_dialog(d, context))


EmpresaAlvo = Union[str, dict]

def _reset_empresa_context():
    """Limpa o contexto global de nome/CNPJ para evitar 'vazar' dados entre empresas."""
    global _CURRENT_EMPRESA_NAME, _CURRENT_EMPRESA_CNPJ
    _CURRENT_EMPRESA_NAME = None
    _CURRENT_EMPRESA_CNPJ = None

def _voltar_empresas(page: Page) -> None:
    """Garante que voltamos para a tela de Empresas Autorizadas antes da prÃ³xima iteraÃ§Ã£o."""
    with suppress(Exception):
        # tentar via URL direta
        target = _build_empresas_url(page)
        page.goto(target, wait_until="networkidle", timeout=15_000)
        if "hwcminhasempresas" in page.url.lower():
            return
    # fallback via menu
    with suppress(Exception):
        abrir_empresas_autorizadas(page)

def processar_empresas_continuo(
    page: Page,
    lista_empresas: Iterable[EmpresaAlvo],
    rotina_por_empresa: Callable[[Page], None],
    delay_entre_empresas: float = 0.8,
) -> Dict[str, Any]:
    """
    Percorre `lista_empresas` SEM interromper em caso de erro:
      1) volta/abre "Empresas Autorizadas"
      2) seleciona a empresa (match parcial jÃ¡ tratado por `selecionar_empresa_por_lista`)
      3) executa `rotina_por_empresa(page)` (vocÃª pluga sua rotina existente aqui)
      4) captura exceÃ§Ãµes, loga e segue para a prÃ³xima

    Retorna um dicionÃ¡rio-sumÃ¡rio: {'ok': [...], 'falhas': [...], 'totais': n}
    """
    ok: list[str] = []
    falhas: list[dict] = []

    empresas_norm = list(lista_empresas or [])
    log_info(f"â–¶ï¸ Iniciando processamento contÃ­nuo de {len(empresas_norm)} empresa(s).")

    for idx, alvo in enumerate(empresas_norm, start=1):
        # Normaliza nome para log
        nome_alvo = _extrair_nome_alvo(alvo) or str(alvo)
        log_info("â€”" * 60)
        log_info(f"ðŸ [{idx}/{len(empresas_norm)}] Empresa alvo: {nome_alvo}")

        try:
            # 1) volta para a grade de empresas
            _voltar_empresas(page)

            # 2) seleciona a empresa
            try:
                selecionar_empresa_por_lista(page, [alvo])
            except Exception as e_sel:
                msg = f"Falha ao selecionar empresa '{nome_alvo}': {e_sel}"
                log_error(f"âŒ {msg}")
                falhas.append({"empresa": nome_alvo, "etapa": "seleÃ§Ã£o", "erro": str(e_sel)})
                _reset_empresa_context()
                with suppress(Exception):
                    time.sleep(delay_entre_empresas)
                continue

            # 3) executa a rotina especÃ­fica dessa empresa
            try:
                rotina_por_empresa(page)
                ok.append(nome_alvo)
                log_info(f"Rotina concluida para: {nome_alvo}")
            except Exception as e_run:
                log_error(f"âŒ Erro na rotina da empresa '{nome_alvo}': {e_run}")
                falhas.append({"empresa": nome_alvo, "etapa": "rotina", "erro": str(e_run)})

        finally:
            # 4) sempre tenta voltar/limpar antes da prÃ³xima
            _reset_empresa_context()
            with suppress(Exception):
                _voltar_empresas(page)
            with suppress(Exception):
                time.sleep(delay_entre_empresas)

    resumo = {"ok": ok, "falhas": falhas, "totais": len(empresas_norm)}
    log_info("â€”" * 60)
    log_info(f"ðŸ“Š Resumo: {len(ok)} ok | {len(falhas)} falhas | {len(empresas_norm)} total")
    if falhas:
        for f in falhas:
            log_info(f" â€¢ FALHA: {f['empresa']} â€” etapa: {f['etapa']} â€” erro: {f['erro']}")
    return resumo









# ==== Helpers de pasta/arquivo por mÃªs + CNPJ + empresa ====
_CURRENT_EMPRESA_NAME: Optional[str] = None
_CURRENT_EMPRESA_CNPJ: Optional[str] = None

def _only_digits(s: Optional[str]) -> str:
    return re.sub(r"\D+", "", s or "")

def _slug_nome(s: str) -> str:
    # remove acentos -> ASCII, mantÃ©m letras, nÃºmeros, espaÃ§o, ._-; troca espaÃ§o por hÃ­fen
    s = _uninorm("NFKD", s or "").encode("ASCII", "ignore").decode("ASCII")
    s = re.sub(r"[^A-Za-z0-9\.\-_ ]+", "", s).strip()
    s = re.sub(r"\s+", "-", s)
    return s or "EMPRESA"

def _get_mm_from_gui() -> str:
    try:
        p = get_periodo_dict()  # jÃ¡ existe no seu projeto
        return f"{int(p['mes_de']):02d}"
    except Exception:
        return f"{datetime.now().month:02d}"

def _detect_cnpj_nome_from_dom(page: Page) -> Tuple[str, str]:
    """
    HeurÃ­stica: tenta ler input de CNPJ; senÃ£o, varre o body por um CNPJ.
    Para nome, tenta textos comuns (Empresa/RazÃ£o Social) ou, em Ãºltimo caso,
    usa um pedaÃ§o do tÃ­tulo/URL.
    """
    cnpj = ""
    nome = ""

    # 1) inputs com 'cnpj'/'cgccpf'
    try:
        loc = page.locator(
            "xpath=//input[contains(translate(@id,'CNPJCGCPF','cnpjcgcpf'),'cnpj') "
            " or contains(translate(@name,'CNPJCGCPF','cnpjcgcpf'),'cnpj') "
            " or contains(translate(@id,'CNPJCGCPF','cnpjcgcpf'),'cgccpf') "
            " or contains(translate(@name,'CNPJCGCPF','cnpjcgcpf'),'cgccpf')]"
        ).first
        if loc and loc.count():
            cnpj = _only_digits(loc.input_value(timeout=2000))
    except Exception:
        pass

    # 2) fallback: varre body em busca de CNPJ
    if not cnpj:
        try:
            body_txt = page.evaluate("() => document.body ? document.body.innerText : ''") or ""
            m = re.search(r"\b(\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2})\b", body_txt)
            if m:
                cnpj = _only_digits(m.group(1))
        except Exception:
            pass

    # 3) nome por labels comuns
    try:
        # algo do tipo "Empresa: ACME LTDA", "RazÃ£o Social: ..."
        body_txt = page.evaluate("() => document.body ? document.body.innerText : ''") or ""
        for chave in ("Empresa", "RazÃ£o Social", "Razao Social", "RazÃ£o", "Razao"):
            m = re.search(rf"{chave}\s*:\s*(.+)", body_txt, re.I)
            if m:
                cand = m.group(1).strip()
                # corta na quebra de linha
                cand = cand.splitlines()[0].strip()
                if len(cand) >= 3:
                    nome = cand
                    break
    except Exception:
        pass

    if not nome:
        try:
            nome = page.title() or ""
            nome = re.sub(r"[-|â€¢].*$", "", nome).strip()
        except Exception:
            pass

    return _only_digits(cnpj), (nome or "EMPRESA")

def _set_empresa_context(nome: Optional[str], cnpj: Optional[str]) -> None:
    global _CURRENT_EMPRESA_NAME, _CURRENT_EMPRESA_CNPJ
    if nome:
        _CURRENT_EMPRESA_NAME = nome
    if cnpj:
        _CURRENT_EMPRESA_CNPJ = _only_digits(cnpj)

def _empresa_context(page: Page) -> Tuple[str, str]:
    """
    Retorna (cnpj, nome) usando prioridade: contexto global -> DOM -> defaults.
    """
    cnpj = _only_digits(_CURRENT_EMPRESA_CNPJ or "")
    nome = (_CURRENT_EMPRESA_NAME or "").strip()
    if cnpj and nome:
        return cnpj, nome
    c2, n2 = _detect_cnpj_nome_from_dom(page)
    return (cnpj or c2 or "00000000000000"), (nome or n2 or "EMPRESA")

def _get_downloads_dir() -> Path:
    base_dir: Optional[Path] = None
    try:
        env_dir = os.getenv("NM_DOWNLOADS_DIR")
        if env_dir:
            base_dir = Path(env_dir)
        else:
            cfg_path = Path(__file__).resolve().parents[1] / "config.json"
            if cfg_path.exists():
                data = json.loads(cfg_path.read_text(encoding="utf-8"))
                raw = (data.get("downloads_dir") or "").strip()
                if raw:
                    base_dir = Path(raw)
    except Exception:
        pass
    if base_dir is None:
        base_dir = Path.cwd() / "downloads"
    return ensure_dir(base_dir)


def _get_download_wait_timeout_ms(default_ms: int = 120_000, minimum_ms: int = 30_000) -> int:
    """
    Resolve timeout de espera para downloads em cenarios de portal lento.
    Prioridade:
      1) ENV `NM_DOWNLOAD_WAIT_TIMEOUT_MS`
      2) config.json: `download_wait_timeout_ms`
      3) config.json: `download_wait_timeout_sec`
      4) `default_ms`
    """
    value = None
    try:
        raw_env = (os.getenv("NM_DOWNLOAD_WAIT_TIMEOUT_MS") or "").strip()
        if raw_env:
            value = int(raw_env)
    except Exception:
        value = None

    if value is None:
        try:
            cfg_path = Path(__file__).resolve().parents[1] / "config.json"
            if cfg_path.exists():
                data = json.loads(cfg_path.read_text(encoding="utf-8"))
                raw_ms = data.get("download_wait_timeout_ms")
                raw_sec = data.get("download_wait_timeout_sec")
                if raw_ms is not None:
                    value = int(raw_ms)
                elif raw_sec is not None:
                    value = int(raw_sec) * 1000
        except Exception:
            value = None

    if value is None:
        value = int(default_ms or 0)
    value = max(int(minimum_ms), int(value))
    # teto de seguranca para evitar espera infinita em cenarios ruins
    return min(value, 900_000)

def _resolve_target_filepath(saida_dir: Path, page: Page, original_filename: str,
                             mes_mm: Optional[str] = None,
                             nome_override: Optional[str] = None,
                             cnpj_override: Optional[str] = None) -> Path:
    """
    Cria subpasta 'mÃªs_empresa' e devolve caminho final
    'mÃªs_cnpj_nome_original.ext'
    """
    mm = (mes_mm or _get_mm_from_gui())
    cnpj, nome = _empresa_context(page)
    if cnpj_override:
        cnpj = _only_digits(cnpj_override)
    if nome_override:
        nome = nome_override

    slug = _slug_nome(nome)
    subdir = (saida_dir or _get_downloads_dir()) / f"{mm}_{slug}"
    subdir.mkdir(parents=True, exist_ok=True)

    stem = Path(original_filename).stem
    ext  = Path(original_filename).suffix
    new_name = f"{mm}_{cnpj}_{slug}_{stem}{ext}"
    # guarda comprimento razoÃ¡vel
    if len(new_name) > 180:
        new_name = f"{mm}_{cnpj}_{slug}_{stem[:120]}{ext}"
    return _ensure_unique_path(subdir / new_name)


def _empresa_download_subdir(
    saida_dir: Path,
    page: Page,
    mes_mm: Optional[str] = None,
    nome_override: Optional[str] = None,
    cnpj_override: Optional[str] = None,
) -> Tuple[Path, str, str, str]:
    mm = (mes_mm or _get_mm_from_gui())
    cnpj, nome = _empresa_context(page)
    if cnpj_override:
        cnpj = _only_digits(cnpj_override)
    if nome_override:
        nome = nome_override
    slug = _slug_nome(nome)
    subdir = (saida_dir or _get_downloads_dir()) / f"{mm}_{slug}"
    return subdir, mm, _only_digits(cnpj), slug


def _wait_for_download_file(
    saida_dir: Path,
    page: Page,
    fname_hint: Optional[str],
    timeout_ms: int = 60000,
    start_ts: Optional[float] = None,
) -> Optional[Path]:
    try:
        timeout_ms = int(timeout_ms or 0)
    except Exception:
        timeout_ms = 0
    if timeout_ms <= 0:
        return None

    start = start_ts or time.time()
    end = start + (timeout_ms / 1000.0)

    subdir, mm, cnpj, slug = _empresa_download_subdir(saida_dir, page)
    with suppress(Exception):
        subdir.mkdir(parents=True, exist_ok=True)

    stem = Path(fname_hint).stem if fname_hint else ""
    if mm and cnpj and slug and stem:
        pattern = f"{mm}_{cnpj}_{slug}_{stem}*.pdf"
    else:
        pattern = "*.pdf"

    while time.time() < end:
        try:
            matches = list(subdir.glob(pattern)) if subdir.exists() else []
        except Exception:
            matches = []
        if matches:
            matches.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            for p in matches:
                try:
                    st = p.stat()
                except Exception:
                    continue
                if st.st_size > 0 and st.st_mtime >= (start - 1.0):
                    log_info(f"Download confirmado por arquivo: {p}")
                    return p
        time.sleep(0.5)
    return None


def _ensure_unique_path(path: Path) -> Path:
    """Evita sobrescrita: se o arquivo existir, adiciona sufixo incremental."""
    if not path.exists():
        return path
    parent = path.parent
    stem = path.stem
    suf = path.suffix
    i = 1
    while True:
        candidate = parent / f"{stem}_{i}{suf}"
        if not candidate.exists():
            return candidate
        i += 1


def _download_pdf_from_click(
    page: Page,
    click_callable,
    saida_dir: Path,
    fname_hint: Optional[str] = None,
    download_timeout_ms: int = 60000,
) -> Optional[Path]:
    """
    Executa um clique que abre/gera PDF e tenta baixar o arquivo usando
    estratÃ©gias jÃ¡ usadas no projeto (fetch, download do viewer, CDP, etc.).
    """
    from urllib.parse import parse_qs, urlparse, unquote
    import base64

    saida_dir.mkdir(parents=True, exist_ok=True)

    dl_box = {"dl": None}
    download_timeout_ms = _get_download_wait_timeout_ms(download_timeout_ms or 60_000, minimum_ms=30_000)

    def _on_download(download):
        dl_box["dl"] = download

    def _final_fname(suggested: Optional[str]) -> str:
        base = suggested or "relatorio.pdf"
        if not fname_hint:
            return base
        hint_path = Path(fname_hint)
        ext = Path(base).suffix or hint_path.suffix
        if ext:
            return f"{hint_path.stem}{ext}"
        return hint_path.name

    def _save_download(dl) -> Optional[Path]:
        if not dl:
            return None
        try:
            fname = _final_fname(dl.suggested_filename)
            fp = _resolve_target_filepath(saida_dir, page, fname)
            dl.save_as(str(fp))
            log_info(f"Download salvo: {fp}")
            return fp
        except Exception:
            return None

    def _safe_filename_from_resp(resp, fallback: str = "relatorio.pdf") -> str:
        try:
            cd = (resp.headers or {}).get("content-disposition", "")
        except Exception:
            cd = ""
        m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd, re.I)
        if m:
            return unquote(m.group(1)).strip()
        try:
            name = Path(urlparse(resp.url).path).name
        except Exception:
            name = ""
        return name or fallback

    def _extract_pdf_from_viewer_url(viewer_url: str) -> Optional[str]:
        try:
            qp = parse_qs(urlparse(viewer_url).query)
            for key in ("src", "file", "url", "target", "openfile", "pdf"):
                if key in qp and qp[key]:
                    val = unquote(qp[key][0])
                    if val and not val.lower().startswith(("about:",)):
                        return val
        except Exception:
            pass
        return None

    def _extract_pdf_src(pg_or_fr) -> Optional[str]:
        try:
            srcs = pg_or_fr.evaluate(
                """
                () => {
                  const abs = (u) => { try { return new URL(u, location.href).href; } catch(e) { return u; } };
                  const get = (sel, attr) => Array.from(document.querySelectorAll(sel))
                        .map(e => e.getAttribute(attr)).filter(Boolean).map(abs);
                  const list = [];
                  list.push(...get("embed[type='application/pdf']", "src"));
                  list.push(...get("object[type='application/pdf']", "data"));
                  list.push(...get("iframe", "src"));
                  return list;
                }
                """
            )
            for u in srcs or []:
                if u and (".pdf" in u.lower() or "application/pdf" in u.lower()):
                    return u
            return srcs[0] if srcs else None
        except Exception:
            return None

    def _js_fetch_and_save(pg_or_fr, url: str) -> Optional[Path]:
        if not url or url.lower().startswith(("about:", "blob:")):
            return None
        try:
            result = pg_or_fr.evaluate(
                """
                async (u) => {
                    try {
                        const res = await fetch(u, {credentials:'include'});
                        const ct  = (res.headers.get('content-type') || '').toLowerCase();
                        const cd  = res.headers.get('content-disposition') || '';
                        if (!res.ok) return {ok:false, status: res.status, statusText: res.statusText, ct};
                        const buf = await res.arrayBuffer();
                        return {ok:true, bytes: Array.from(new Uint8Array(buf)), ct, cd};
                    } catch(e) {
                        return {ok:false, error: String(e)};
                    }
                }
                """,
                url,
            )
            if not result or not result.get("ok"):
                return None

            data = bytes(result["bytes"])
            ct = (result.get("ct") or "").lower()
            if "application/pdf" not in ct and not data.startswith(b"%PDF"):
                return None

            cd = result.get("cd") or ""
            fname = None
            m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd, re.I)
            if m:
                fname = unquote(m.group(1)).strip()
            elif url.lower().split("?")[0].endswith(".pdf"):
                fname = Path(urlparse(url).path).name or fname

            fname = _final_fname(fname)
            fp = _resolve_target_filepath(saida_dir, page, fname)
            fp.write_bytes(data)
            log_info(f"PDF salvo: {fp} ({len(data)} bytes)")
            return fp
        except Exception:
            return None

    def _download_via_context(url: str) -> Optional[Path]:
        if not url or url.lower().startswith(("about:", "blob:", "data:")):
            return None
        try:
            req_timeout = min(max(25_000, int(download_timeout_ms * 0.6)), download_timeout_ms)
            resp = page.context.request.get(url, timeout=req_timeout)
        except Exception:
            return None
        if not resp:
            return None
        try:
            ct = (resp.headers or {}).get("content-type", "").lower()
        except Exception:
            ct = ""
        try:
            data = resp.body()
        except Exception:
            data = b""
        if "application/pdf" not in ct and "octet-stream" not in ct and not (data and data[:4] == b"%PDF"):
            return None
        fname = _safe_filename_from_resp(resp, "relatorio.pdf")
        fname = _final_fname(fname)
        fp = _resolve_target_filepath(saida_dir, page, fname)
        fp.write_bytes(data)
        log_info(f"PDF salvo via contexto: {fp}")
        return fp

    def _find_pdf_urls(pg_or_fr) -> List[str]:
        urls = set()

        def _collect(tgt):
            try:
                lst = tgt.evaluate(
                    """
                    () => {
                      const abs = u => { try { return new URL(u, location.href).href; } catch(e) { return u; } };
                      const get = (sel, attr) => Array.from(document.querySelectorAll(sel))
                            .map(e => e.getAttribute(attr)).filter(Boolean).map(abs);
                      const cand = [
                        ...get("embed[type='application/pdf']", "src"),
                        ...get("object[type='application/pdf']", "data"),
                        ...get("iframe", "src"),
                        ...get("a[href]", "href"),
                      ];
                      return cand;
                    }
                    """
                ) or []
            except Exception:
                lst = []
            for u in lst or []:
                lu = (u or "").lower()
                if ".pdf" in lu or "application/pdf" in lu or "pdf=" in lu or lu.startswith("blob:") or lu.startswith("data:application/pdf"):
                    urls.add(u)

        _collect(pg_or_fr)
        with suppress(Exception):
            for fr in pg_or_fr.frames:
                _collect(fr)
        return list(urls)

    def _wait_for_download(pg, timeout_ms: int = 3000, label: str = "") -> Optional[Path]:
        try:
            if timeout_ms and label:
                log_info(f"Aguardando download{label} por ate {int(timeout_ms/1000)}s...")
            dl = pg.wait_for_event("download", timeout=timeout_ms)
            return _save_download(dl)
        except Exception:
            return None

    def _sniff_first_pdf_response(pg, timeout_ms: int = 8_000) -> Optional[str]:
        try:
            resp = pg.wait_for_response(
                lambda r: ("application/pdf" in (r.headers.get("content-type", "").lower()))
                or r.url.lower().endswith(".pdf"),
                timeout=timeout_ms,
            )
            if resp:
                return resp.url
        except Exception:
            pass
        return None

    def _find_pdf_download_btn(pg_or_fr):
        sels = [
            "css=#toolbarViewerRight #download",
            "css=button#download", "css=a#download",
            "css=button[title*='Download' i]",
            "css=button[aria-label*='Download' i]",
            "xpath=//*[contains(translate(@title,'BAIXAR','baixar'),'baixar') or "
            "            contains(translate(@aria-label,'BAIXAR','baixar'),'baixar') or "
            "            normalize-space(.)='Baixar' or normalize-space(.)='Download']",
        ]
        for s in sels:
            with suppress(Exception):
                loc = pg_or_fr.locator(s).first
                if loc and loc.count() and loc.is_visible():
                    return loc
        try:
            pdfv = pg_or_fr.locator("css=pdf-viewer").first
            if pdfv and pdfv.count():
                btn = pdfv.locator("css=viewer-toolbar").locator("css=#download").first
                if btn and btn.count():
                    return btn
                btn = pdfv.locator("css=viewer-download-controls").locator("css=#download").first
                if btn and btn.count():
                    return btn
                btn = pdfv.locator("css=cr-icon-button#download").first
                if btn and btn.count():
                    return btn
        except Exception:
            pass
        return None

    def _click_viewer_download(pg) -> Optional[Path]:
        btn = _find_pdf_download_btn(pg)
        if not btn:
            return None
        try:
            with pg.expect_download(timeout=min(download_timeout_ms, 30_000)) as dlinfo:
                btn.click(timeout=6_000)
            dl = dlinfo.value
            fname = _final_fname(dl.suggested_filename)
            fp = _resolve_target_filepath(saida_dir, page, fname)
            dl.save_as(str(fp))
            log_info(f"Download via viewer salvo: {fp}")
            return fp
        except Exception:
            return None

    def _print_to_pdf_via_cdp(pg) -> Optional[Path]:
        try:
            try:
                pg.emulate_media(media="print")
            except Exception:
                pass
            client = page.context.new_cdp_session(pg)
            res = client.send(
                "Page.printToPDF",
                {
                    "printBackground": True,
                    "preferCSSPageSize": True,
                    "paperWidth": 8.27,
                    "paperHeight": 11.69,
                    "marginTop": 0.25,
                    "marginBottom": 0.25,
                    "marginLeft": 0.25,
                    "marginRight": 0.25,
                    "scale": 1.0,
                    "landscape": False,
                },
            )
            data = base64.b64decode(res.get("data", ""))
            if data and data[:4] == b"%PDF":
                fp = _resolve_target_filepath(saida_dir, page, _final_fname("relatorio.pdf"))
                fp.write_bytes(data)
                log_info(f"PDF gerado via CDP: {fp}")
                return fp
        except Exception:
            pass
        return None

    def _wait_popup_navigation(pg, timeout_ms: int, require_pdf_hint: bool = False) -> None:
        """
        Aguarda popup sair de about:blank e, no caso de relatorio nacional,
        espera a URL de listagem carregar antes de tentar extrair/baixar PDF.
        """
        budget_ms = max(2_000, min(int(timeout_ms or 0), 20_000))
        end = time.time() + (budget_ms / 1000.0)
        last_url = ""
        while time.time() < end:
            cur = (pg.url or "").strip()
            cur_low = cur.lower()
            if cur and not cur_low.startswith("about:blank"):
                if cur != last_url:
                    last_url = cur
                    log_info(f"Popup URL detectada: {cur}")
                with suppress(Exception):
                    pg.wait_for_load_state("domcontentloaded", timeout=min(1_500, budget_ms))
                if (not require_pdf_hint) or ("arrelnfnaclistagem" not in cur_low):
                    return
                if _extract_pdf_src(pg) or _find_pdf_urls(pg):
                    return
            with suppress(Exception):
                pg.wait_for_timeout(150)

    def _handle_page(pg, wait_budget_ms: Optional[int] = None) -> Optional[Path]:
        budget_ms = max(10_000, int(wait_budget_ms or download_timeout_ms or 60_000))
        quick_nav_timeout = min(max(2_500, int(budget_ms * 0.10)), 12_000)
        dom_timeout = min(max(8_000, int(budget_ms * 0.20)), 35_000)
        net_timeout = min(max(4_000, int(budget_ms * 0.10)), 18_000)
        sniff_timeout = min(max(5_000, int(budget_ms * 0.12)), 16_000)

        if "arrelnfnaclistagem" in (pg.url or "").lower():
            _wait_popup_navigation(pg, timeout_ms=quick_nav_timeout, require_pdf_hint=False)
        else:
            _wait_popup_navigation(pg, timeout_ms=min(4_000, quick_nav_timeout), require_pdf_hint=False)

        # Caminho rapido: popup abriu e download/URL do PDF ja esta disponivel.
        dst = _wait_for_download(pg, timeout_ms=min(3_500, max(1_500, int(budget_ms * 0.06))))
        if dst:
            return dst

        dst = _download_via_context(pg.url) or _js_fetch_and_save(pg, pg.url)
        if dst:
            return dst

        real = _extract_pdf_from_viewer_url(pg.url)
        if real:
            dst = _download_via_context(real) or _js_fetch_and_save(pg, real)
            if dst:
                return dst

        sniff = _sniff_first_pdf_response(pg, timeout_ms=sniff_timeout)
        if sniff:
            dst = _download_via_context(sniff) or _js_fetch_and_save(pg, sniff)
            if dst:
                return dst

        # Fallback mais completo para portais lentos.
        with suppress(Exception):
            pg.wait_for_load_state("domcontentloaded", timeout=dom_timeout)
        with suppress(Exception):
            pg.wait_for_load_state("networkidle", timeout=net_timeout)

        dst = _wait_for_download(pg, timeout_ms=min(8_000, max(2_000, int(budget_ms * 0.10))))
        if dst:
            return dst

        dst = _click_viewer_download(pg)
        if dst:
            return dst

        pdf_src = _extract_pdf_src(pg)
        if not pdf_src:
            with suppress(Exception):
                for fr in pg.frames:
                    pdf_src = _extract_pdf_src(fr)
                    if pdf_src:
                        break
        if pdf_src and pdf_src.lower() != "about:blank":
            dst = _download_via_context(pdf_src) or _js_fetch_and_save(pg, pdf_src)
            if dst:
                return dst

        for u in _find_pdf_urls(pg):
            dst = _download_via_context(u) or _js_fetch_and_save(pg, u)
            if dst:
                return dst

        return _print_to_pdf_via_cdp(pg)

    clicked = False
    page.on("download", _on_download)
    try:
        try:
            popup_timeout = min(max(20_000, int(download_timeout_ms * 0.8)), download_timeout_ms)
            with page.expect_popup(timeout=popup_timeout) as popinfo:
                click_callable()
                clicked = True
            pdf_pg = popinfo.value
            dst = _wait_for_download(pdf_pg, timeout_ms=min(6_000, max(2_000, int(download_timeout_ms * 0.08))))
            if dst:
                with suppress(Exception):
                    pdf_pg.close()
                return dst
            dst = _handle_page(pdf_pg, wait_budget_ms=download_timeout_ms)
            if (not dst) and download_timeout_ms > 0:
                dst = _wait_for_download(pdf_pg, timeout_ms=download_timeout_ms, label=" (popup)")
            with suppress(Exception):
                pdf_pg.close()
            return dst
        except PWTimeout:
            pass
        except Exception:
            pass

        # aguarda download capturado apÃ³s o clique
        if dl_box["dl"] is None:
            end = time.time() + min(20.0, max(3.0, download_timeout_ms / 10_000.0))
            while time.time() < end and dl_box["dl"] is None:
                time.sleep(0.2)

        if dl_box["dl"] is not None:
            dst = _save_download(dl_box["dl"])
            if dst:
                return dst

        if clicked:
            dst = _handle_page(page, wait_budget_ms=download_timeout_ms)
            if dst:
                return dst
        if download_timeout_ms > 0:
            dst = _wait_for_download(page, timeout_ms=download_timeout_ms, label=" (pagina)")
            if dst:
                return dst

        try:
            timeout_dl = download_timeout_ms if download_timeout_ms > 0 else 12_000
            with page.expect_download(timeout=timeout_dl) as dlinfo:
                click_callable()
            return _save_download(dlinfo.value)
        except Exception:
            pass

        with suppress(Exception):
            click_callable()
        return _handle_page(page, wait_budget_ms=download_timeout_ms)
    finally:
        try:
            page.off("download", _on_download)
        except Exception:
            try:
                page.remove_listener("download", _on_download)
            except Exception:
                pass


def _print_page_to_pdf(
    page: Page,
    saida_dir: Path,
    fname_hint: str = "extrato_issqn.pdf",
) -> Optional[Path]:
    """Gera PDF da pÃ¡gina atual via CDP (equivalente ao Ctrl+P)."""
    saida_dir.mkdir(parents=True, exist_ok=True)
    try:
        import base64
        with suppress(Exception):
            page.emulate_media(media="print")
        client = page.context.new_cdp_session(page)
        params = {
            "printBackground": True,
            "preferCSSPageSize": True,
            "paperWidth": 8.27,
            "paperHeight": 11.69,
            "marginTop": 0.25,
            "marginBottom": 0.25,
            "marginLeft": 0.25,
            "marginRight": 0.25,
            "scale": 1.0,
            "landscape": False,
        }
        res = client.send("Page.printToPDF", params)
        data = base64.b64decode(res.get("data", ""))
        if data and data[:4] == b"%PDF":
            fp = _resolve_target_filepath(saida_dir, page, fname_hint)
            fp.write_bytes(data)
            log_info(f"PDF gerado via CDP: {fp}")
            return fp
    except Exception as e:
        log_error(f"printToPDF via CDP falhou: {e}")
    return None
# ==== FIM helpers ====


# â•­â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â•®
# â”‚  Abrir pagina de empresas autorizadas                       â”‚
# â•°â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â•¯
def abrir_empresas_autorizadas(page: Page) -> None:
    """
    Leva o navegador atÃ© /nfse/servlet/hwcminhasempresas

    1. Tenta navegaÃ§Ã£o direta via URL dinÃ¢mica;
    2. Se falhar, recorre ao menu â€œPerfil â†’ Empresas Autorizadasâ€.
    """
    if "hwcminhasempresas" in page.url.lower():           # jÃ¡ na tela
        return

    # â”€â”€ tentativa 1: navegaÃ§Ã£o direta â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    target = _build_empresas_url(page)
    try:
        page.goto(target, wait_until="networkidle", timeout=15_000)
        if "hwcminhasempresas" in page.url.lower():
            return
    except Exception:
        # mesmo com exceÃ§Ãµes fazemos o fallback a seguir
        pass

    # â”€â”€ tentativa 2: menu lateral Perfil â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    try:
        page.hover(X_MENU_PERFIL, timeout=8_000)
        page.click(X_SUBMENU_EMPRESAS_AUT, timeout=8_000)
        page.wait_for_url("**/hwcminhasempresas*", timeout=15_000)
    except Exception as e:
        raise RuntimeError(
            "Falha ao abrir a tela 'Empresas Autorizadas' "
            f"via URL '{target}' ou via menu. Erro: {e}"
        )
# â•­â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â•®
# â”‚  Selecionar Empresa                                   â”‚
# â•°â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â•¯

def _extrair_nome_alvo(item) -> str:
    """
    Aceita:
      - string com o nome/razÃ£o social
      - dict com possÃ­veis chaves: 'empresa', 'razao', 'razao_social', 'nome'
    Retorna string (ou "" se nada adequado).
    """
    if isinstance(item, str):
        return item.strip()
    if isinstance(item, dict):
        for k in ("empresa", "razao", "razao_social", "nome", "Nome", "Razao", "RazÃ£o Social", "Razao Social"):
            v = item.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        partes = []
        for v in item.values():
            if isinstance(v, (str, int, float)):
                partes.append(str(v))
        return " ".join(partes).strip()
    return ""


# ------------ helper: clique + trata confirm/modal (Ã  prova de duplicidade) ------------

def _wait_and_click_modal_ok(page: Page, timeout_ms: int = 8000) -> bool:
    """
    Varre pagina e iframes procurando botoes 'OK/Confirmar/Sim' (button/input/a).
    Clica no primeiro visivel/habilitado. Retorna True se conseguiu.
    """
    sel = (
        "xpath=//button[normalize-space()='OK' or normalize-space()='Ok' "
        "               or normalize-space()='Confirmar' or normalize-space()='Sim']"
        " | //input[( @type='button' or @type='submit') and "
        "           ( translate(@value,'OKCONFIRMARSIM','okconfirmarsim')='ok' "
        "          or translate(@value,'OKCONFIRMARSIM','okconfirmarsim')='confirmar' "
        "          or translate(@value,'OKCONFIRMARSIM','okconfirmarsim')='sim')]"
        " | //a[normalize-space()='OK' or normalize-space()='Ok' "
        "      or normalize-space()='Confirmar' or normalize-space()='Sim']"
        " | //div[contains(@class,'modal') or contains(@id,'dialog') or contains(@class,'dialog')]"
        "//button|//input|//a[normalize-space()='OK' or normalize-space()='Ok' "
        "                       or normalize-space()='Confirmar' or normalize-space()='Sim']"
    )
    end_time = time.time() + (timeout_ms / 1000.0)
    while time.time() < end_time:
        for ctx in [page] + list(page.frames):
            try:
                btn = ctx.locator(sel).first
                if btn and btn.count() and btn.is_visible() and btn.is_enabled():
                    try:
                        with suppress(Exception):
                            btn.scroll_into_view_if_needed(timeout=1000)
                        btn.click(timeout=1500)
                        return True
                    except Exception:
                        try:
                            btn.evaluate("el => el.click()")
                            return True
                        except Exception:
                            pass
            except Exception:
                continue
        time.sleep(0.2)
    return False

PT_ABBR = ["","jan","fev","mar","abr","mai","jun","jul","ago","set","out","nov","dez"]
IMG_XP = "xpath=.//img[not(contains(translate(@src,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'cinza')) and not(contains(translate(@src,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'gray'))]"

# ===== clique seguro (evita page.off e nÃ£o propaga exceÃ§Ãµes) =====
def _safe_click_with_fallback(page: Page, link, contexto_log: str = "") -> bool:
    try:
        handle_dialog_and_modal(page, link)  # sua lÃ³gica central
        return True
    except Exception as e:
        log_info(f"(aviso) handler falhou em {contexto_log}: {e!r} â€” fallbackâ€¦")

    # tenta detectar/aceitar dialogo rapidamente (se houver)
    try:
        with _expect_dialog_event(page, timeout=800) as dlg:
            with suppress(Exception):
                link.scroll_into_view_if_needed(timeout=600)
            link.click(timeout=1500)
        _safe_accept_dialog(dlg.value, "Dialog JS - OK.")
        return True
    except PWTimeout:
        pass
    except Exception as e:
        log_info(f"(aviso) dialog fallback falhou em {contexto_log}: {e!r}")

    # clique normal
    with suppress(Exception):
        link.click(timeout=1500)
        return True
    # force
    with suppress(Exception):
        link.click(timeout=1200, force=True)
        return True
    # JS
    with suppress(Exception):
        link.evaluate("el => el.click()")
        return True
    # delegaÃ§Ã£o na cÃ©lula
    with suppress(Exception):
        td = link.locator("xpath=ancestor::td[1]").first
        if td and td.count():
            td.click(timeout=1200, force=True)
            return True
    return False


def _wait_for_mov_table(page: Page, timeout_ms: int = 8000) -> bool:
    selectors = [
        "//div[@id='GridContainerDiv']//table",
        "//div[@id='Grid2ContainerDiv']//table",
        "//table[contains(@class,'Grid') or contains(@class,'DataGrid')]",
        "//table[@id='TABLE1']",
    ]
    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
    for fr in frames:
        for xp in selectors:
            try:
                fr.wait_for_selector(f"xpath={xp}", timeout=timeout_ms)
                return True
            except Exception:
                continue
    return False


def _click_pesquisar_movimentacao(page: Page, selector: str, label: str) -> None:
    log_info(f"Clicando em 'Pesquisar' na {label}...")
    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
    def _normalize_selector(sel: str) -> str:
        sel = (sel or "").strip()
        if not sel:
            return sel
        if sel.startswith(("xpath=", "css=")):
            return sel
        if sel.startswith(("//", ".//", "(")):
            return f"xpath={sel}"
        return sel

    selector = _normalize_selector(selector)
    generic = (
        "//input[(contains(translate(@value,'PESQUISAR','pesquisar'),'pesquisar')"
        "      or contains(translate(@title,'PESQUISAR','pesquisar'),'pesquisar')"
        "      or contains(translate(@id,'PESQUISAR','pesquisar'),'pesquisar')"
        "      or contains(translate(@name,'PESQUISAR','pesquisar'),'pesquisar'))"
        "      and not(contains(@style,'display:none'))]"
        " | //button[contains(translate(normalize-space(.),'PESQUISAR','pesquisar'),'pesquisar')"
        "      and not(contains(@style,'display:none'))]"
        " | //a[contains(translate(normalize-space(.),'PESQUISAR','pesquisar'),'pesquisar')]"
    )
    def _find_btn(fr):
        if selector:
            try:
                loc = fr.wait_for_selector(selector, state="visible", timeout=1500)
                if loc:
                    return loc
            except Exception:
                pass
            try:
                btn = fr.locator(selector).first
                if btn.count() and btn.is_visible():
                    return btn
            except Exception:
                pass
        try:
            loc = fr.wait_for_selector(f"xpath={generic}", state="visible", timeout=1500)
            if loc:
                return loc
        except Exception:
            pass
        try:
            btn = fr.locator(f"xpath={generic}").first
            return btn if btn.count() and btn.is_visible() else None
        except Exception:
            return None

    def _try_click() -> bool:
        for fr in frames:
            btn = _find_btn(fr)
            if not btn:
                continue
            try:
                with suppress(Exception): btn.scroll_into_view_if_needed(timeout=1000)
                btn.click(timeout=5000)
                return True
            except Exception:
                try:
                    btn.evaluate("el => el.click()")
                    return True
                except Exception:
                    continue
        return False

    if not _try_click():
        log_error("Nao foi possivel localizar o botao Pesquisar.")
        raise RuntimeError("Botao Pesquisar nao encontrado")
    with suppress(Exception):
        page.wait_for_load_state('domcontentloaded', timeout=8000)
    if not _wait_for_mov_table(page, timeout_ms=8000):
        with suppress(Exception):
            page.wait_for_timeout(300)
        _try_click()
        _wait_for_mov_table(page, timeout_ms=8000)
    log_info("Clique em Pesquisar executado.")
    time.sleep(1.0)

def _get_periodo_dict() -> dict:
    """Pega perÃ­odo escolhido no GUI; fallback = mÃªs/ano atual."""
    try:
        from utils.periodo import get_periodo
        p = get_periodo()  # esperado: {"mes_de","ano_de","mes_ate","ano_ate"}
        if isinstance(p, dict) and all(k in p for k in ("mes_de","ano_de","mes_ate","ano_ate")):
            # normaliza para strings: mm = 2 dÃ­gitos; ano = 4
            mm_de  = f"{int(p['mes_de']):02d}"
            mm_ate = f"{int(p['mes_ate']):02d}"
            yy_de  = f"{int(p['ano_de']):04d}"
            yy_ate = f"{int(p['ano_ate']):04d}"
            return {"mes_de": mm_de, "ano_de": yy_de, "mes_ate": mm_ate, "ano_ate": yy_ate}
    except Exception:
        pass
    now = datetime.now()
    mm = f"{now.month:02d}"; yy = f"{now.year:04d}"
    return {"mes_de": mm, "ano_de": yy, "mes_ate": mm, "ano_ate": yy}




def _apply_period_to_movimentacao(page):
    p = get_periodo_dict()
    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
    for fr in frames:
        mes_de = ano_de = mes_ate = ano_ate = None
        def _first_visible_xpath(xp: str):
            try:
                loc = fr.locator(f"xpath={xp}").first
                return loc if (loc.count() and loc.is_visible()) else None
            except Exception:
                return None
        def _wait_visible_xpath(xp: str, timeout_ms: int = 3000):
            try:
                return fr.wait_for_selector(f"xpath={xp}", state="visible", timeout=timeout_ms)
            except Exception:
                return None

        y1 = _first_visible_xpath("//select[@id='vANOSEMMOV1']")
        y2 = _first_visible_xpath("//select[@id='vANOSEMMOV2']")
        y_only = _first_visible_xpath("//select[@id='vANOSEMMOV']")
        for _ in range(2):
            mes_de, ano_de, mes_ate, ano_ate = find_period_inputs_in_frame(fr)
            if any((mes_de, ano_de, mes_ate, ano_ate)):
                break
            time.sleep(0.6)
        if not any((mes_de, ano_de, mes_ate, ano_ate)):
            # fallback: anos especificos das telas de movimentacao
            if not y1:
                y1 = _wait_visible_xpath("//select[@id='vANOSEMMOV1']")
            if not y2:
                y2 = _wait_visible_xpath("//select[@id='vANOSEMMOV2']")
            if not y_only:
                y_only = _wait_visible_xpath("//select[@id='vANOSEMMOV']")
            if not (y1 or y2 or y_only):
                # fallback: qualquer select com opcoes que contenham o ano
                try:
                    sels = fr.locator("select")
                    count = min(sels.count(), 12)
                    for i in range(count):
                        cand = sels.nth(i)
                        opts = cand.evaluate(
                            "el => Array.from(el.options||[]).map(o => o.textContent || o.label || o.value || '')"
                        )
                        if any(f"{int(p['ano_de']):04d}" in (o or "") for o in (opts or [])):
                            y_only = cand
                            break
                except Exception:
                    y_only = None
        if not any((mes_de, ano_de, mes_ate, ano_ate)) and not (y1 or y2 or y_only):
            continue
        only_year = bool(y1 or y2 or y_only)
        if y_only:
            ano_de = y_only
            ano_ate = None
            mes_de = None
            mes_ate = None
        else:
            if y1:
                ano_de = y1
                mes_de = None
            if y2:
                ano_ate = y2
                mes_ate = None
        def _year_matches(sel_text: str, yy: str) -> bool:
            if not sel_text:
                return False
            yy = str(int(yy))
            yy2 = yy[-2:]
            norm = normalize_text(sel_text)
            return yy in norm or yy2 in norm

        def _force_year(el, yy: str) -> bool:
            yy = str(int(yy))
            yy2 = yy[-2:]
            try:
                if is_select_element(el):
                    try:
                        el.select_option(value=yy, timeout=2000)
                    except Exception:
                        try:
                            el.select_option(label=yy, timeout=2000)
                        except Exception:
                            pass
                    sel = get_selected_text(el)
                    if _year_matches(sel, yy):
                        return True
            except Exception:
                pass
            try:
                if select_year(el, yy):
                    return True
            except Exception:
                pass
            try:
                ok = el.evaluate(
                    """
                    (el, yy, yy2) => {
                      const norm = (s)=> (s||'')
                        .normalize('NFKD').replace(/[\\u0300-\\u036f]/g,'')
                        .replace(/\\s+/g,' ').trim().toLowerCase();
                      if (!el) return false;
                      const tag = (el.tagName||'').toLowerCase();
                      if (tag === 'select') {
                        const opts = Array.from(el.options||[]);
                        let idx = opts.findIndex(o => (o.value||'') === yy);
                        if (idx < 0) {
                          idx = opts.findIndex(o => norm(o.textContent||o.label||'') === norm(yy));
                        }
                        if (idx < 0) {
                          idx = opts.findIndex(o => (o.value||'').includes(yy2));
                          if (idx < 0) {
                            idx = opts.findIndex(o => norm(o.textContent||o.label||'').includes(yy2));
                          }
                        }
                        if (idx < 0) return false;
                        for (let i = 0; i < opts.length; i++) {
                          opts[i].selected = (i === idx);
                        }
                        el.selectedIndex = idx;
                        const val = opts[idx].value || opts[idx].textContent || '';
                        el.value = String(val);
                        if (typeof el.onfocus === 'function') el.onfocus.call(el);
                        el.dispatchEvent(new Event('input',  {bubbles:true}));
                        el.dispatchEvent(new Event('change', {bubbles:true}));
                        if (typeof el.onchange === 'function') el.onchange.call(el);
                        if (typeof el.onblur === 'function') el.onblur.call(el);
                        if (window.gx && gx.evt && el.id === 'vANOSEMMOV' && gx.evt.execEvt) {
                          gx.evt.execEvt('', false, 'EVANOSEMMOV.CLICK.', el);
                        }
                        return true;
                      }
                      if ('value' in el) el.value = String(yy);
                      if (typeof el.onfocus === 'function') el.onfocus.call(el);
                      el.dispatchEvent(new Event('input',  {bubbles:true}));
                      el.dispatchEvent(new Event('change', {bubbles:true}));
                      if (typeof el.onchange === 'function') el.onchange.call(el);
                      if (typeof el.onblur === 'function') el.onblur.call(el);
                      if (window.gx && gx.evt && el.id === 'vANOSEMMOV' && gx.evt.execEvt) {
                        gx.evt.execEvt('', false, 'EVANOSEMMOV.CLICK.', el);
                      }
                      return true;
                    }
                    """,
                    yy,
                    yy2,
                )
                if ok:
                    sel = get_selected_text(el)
                    return _year_matches(sel, yy)
            except Exception:
                pass
            try:
                el.scroll_into_view_if_needed(timeout=1000)
                el.click(timeout=1200, force=True)
                with suppress(Exception):
                    el.press("Control+A")
                with suppress(Exception):
                    el.type(yy, delay=30)
                with suppress(Exception):
                    el.press("Enter")
                with suppress(Exception):
                    el.press("Tab")
                sel = get_selected_text(el)
                if _year_matches(sel, yy):
                    return True
            except Exception:
                pass
            try:
                el.click(timeout=1200, force=True)
                for xp in (
                    f"//li[normalize-space()='{yy}' or normalize-space()='{yy2}' or contains(.,'{yy}') or contains(.,'{yy2}')]",
                    f"//*[@role='option' and (normalize-space()='{yy}' or normalize-space()='{yy2}' or contains(.,'{yy}') or contains(.,'{yy2}'))]",
                    f"//div[contains(@class,'option') and (contains(.,'{yy}') or contains(.,'{yy2}'))]",
                    f"//span[normalize-space()='{yy}' or normalize-space()='{yy2}']",
                ):
                    opt = fr.locator(f"xpath={xp}").first
                    if opt.count() and opt.is_visible():
                        opt.click(timeout=1500)
                        sel = get_selected_text(el)
                        if _year_matches(sel, yy):
                            return True
                        break
            except Exception:
                pass
            return False
        def _force_year_any_select(frame, yy: str) -> bool:
            yy = str(int(yy))
            yy2 = yy[-2:]
            try:
                sels = frame.locator("select")
                count = min(sels.count(), 12)
            except Exception:
                return False
            for i in range(count):
                sel = sels.nth(i)
                try:
                    if not sel.is_visible():
                        continue
                except Exception:
                    continue
                try:
                    opts = sel.evaluate(
                        "el => Array.from(el.options||[]).map(o => (o.value||'') + '|' + (o.textContent||o.label||''))"
                    )
                except Exception:
                    continue
                if not any((yy in (o or "")) or (yy2 in (o or "")) for o in (opts or [])):
                    continue
                if _force_year(sel, yy):
                    return True
            return False
        def _force_year_all_fields(frame, yy: str) -> bool:
            yy = str(int(yy))
            yy2 = yy[-2:]
            try:
                ok = frame.evaluate(
                    """
                    (yy, yy2) => {
                      const norm = (s)=> (s||'')
                        .normalize('NFKD').replace(/[\\u0300-\\u036f]/g,'')
                        .replace(/\\s+/g,' ').trim().toLowerCase();
                      const cand = Array.from(document.querySelectorAll(
                        "select[id*='ANO' i], select[name*='ANO' i], " +
                        "input[id*='ANO' i], input[name*='ANO' i]"
                      ));
                      if (!cand.length) return false;
                      let changed = false;
                      for (const el of cand) {
                        const tag = (el.tagName||'').toLowerCase();
                        if (tag === 'select') {
                          const opts = Array.from(el.options||[]);
                          let idx = opts.findIndex(o => (o.value||'') === yy);
                          if (idx < 0) {
                            idx = opts.findIndex(o => norm(o.textContent||o.label||'').includes(yy));
                          }
                          if (idx < 0) {
                            idx = opts.findIndex(o => (o.value||'').includes(yy2));
                            if (idx < 0) {
                              idx = opts.findIndex(o => norm(o.textContent||o.label||'').includes(yy2));
                            }
                          }
                          if (idx < 0) continue;
                          for (let i = 0; i < opts.length; i++) {
                            opts[i].selected = (i === idx);
                          }
                          el.selectedIndex = idx;
                          el.value = String(opts[idx].value || opts[idx].textContent || '');
                          if (typeof el.onfocus === 'function') el.onfocus.call(el);
                          el.dispatchEvent(new Event('input',  {bubbles:true}));
                          el.dispatchEvent(new Event('change', {bubbles:true}));
                          if (typeof el.onchange === 'function') el.onchange.call(el);
                          if (typeof el.onblur === 'function') el.onblur.call(el);
                          if (window.gx && gx.evt && el.id === 'vANOSEMMOV' && gx.evt.execEvt) {
                            gx.evt.execEvt('', false, 'EVANOSEMMOV.CLICK.', el);
                          }
                          changed = true;
                        } else if ('value' in el) {
                          el.value = String(yy);
                          if (typeof el.onfocus === 'function') el.onfocus.call(el);
                          el.dispatchEvent(new Event('input',  {bubbles:true}));
                          el.dispatchEvent(new Event('change', {bubbles:true}));
                          if (typeof el.onchange === 'function') el.onchange.call(el);
                          if (typeof el.onblur === 'function') el.onblur.call(el);
                          if (window.gx && gx.evt && el.id === 'vANOSEMMOV' && gx.evt.execEvt) {
                            gx.evt.execEvt('', false, 'EVANOSEMMOV.CLICK.', el);
                          }
                          changed = true;
                        }
                      }
                      return changed;
                    }
                    """,
                    yy,
                    yy2,
                )
                return bool(ok)
            except Exception:
                return False

        def _post_year_change(el) -> None:
            try:
                el.evaluate(
                    """
                    (el) => {
                      try {
                        if (typeof el.onfocus === 'function') el.onfocus.call(el);
                        if (typeof el.onchange === 'function') el.onchange.call(el);
                        if (typeof el.onblur === 'function') el.onblur.call(el);
                        if (window.gx && gx.evt && el.id === 'vANOSEMMOV' && gx.evt.execEvt) {
                          gx.evt.execEvt('', false, 'EVANOSEMMOV.CLICK.', el);
                        }
                      } catch (e) {}
                    }
                    """
                )
            except Exception:
                pass

        def _force_year_by_id(frame, sel_id: str, yy: str) -> bool:
            yy = str(int(yy))
            try:
                ok = frame.evaluate(
                    """
                    (sel_id, yy) => {
                      const el = document.getElementById(sel_id);
                      if (!el || !el.options) return false;
                      const opts = Array.from(el.options || []);
                      let idx = opts.findIndex(o => (o.value || '') === yy);
                      if (idx < 0) {
                        idx = opts.findIndex(o => (o.textContent || '').trim() === yy);
                      }
                      if (idx < 0) return false;
                      for (let i = 0; i < opts.length; i++) {
                        opts[i].selected = (i === idx);
                      }
                      el.selectedIndex = idx;
                      el.value = String(opts[idx].value || opts[idx].textContent || '');
                      if (typeof el.onfocus === 'function') el.onfocus.call(el);
                      el.dispatchEvent(new Event('input',  {bubbles:true}));
                      el.dispatchEvent(new Event('change', {bubbles:true}));
                      if (typeof el.onchange === 'function') el.onchange.call(el);
                      if (typeof el.onblur === 'function') el.onblur.call(el);
                      if (window.gx && gx.evt && el.id === 'vANOSEMMOV' && gx.evt.execEvt) {
                        gx.evt.execEvt('', false, 'EVANOSEMMOV.CLICK.', el);
                      }
                      return true;
                    }
                    """,
                    sel_id,
                    yy,
                )
                if ok:
                    try:
                        loc = frame.locator(f"#{sel_id}").first
                        sel = get_selected_text(loc)
                        log_info(f"Ano selecionado id={sel_id}: {sel}")
                    except Exception:
                        log_info(f"Ano selecionado id={sel_id}: {yy}")
                return bool(ok)
            except Exception:
                return False

        with suppress(Exception):
            fr.wait_for_load_state("domcontentloaded", timeout=5000)
        for _ in range(3):
            ok = True
            if mes_de and not only_year:
                ok &= select_month(mes_de, p['mes_de'])
            if ano_de:
                if only_year and y_only:
                    ok_year = _force_year_by_id(fr, "vANOSEMMOV", p['ano_de'])
                    if not ok_year:
                        ok_year = _force_year(ano_de, p['ano_de']) or _force_year_any_select(fr, p['ano_de'])
                    if ok_year:
                        _post_year_change(ano_de)
                    ok &= ok_year
                else:
                    ok_year = _force_year(ano_de, p['ano_de'])
                    if not ok_year:
                        ok_year = _force_year_any_select(fr, p['ano_de'])
                    if ok_year:
                        _post_year_change(ano_de)
                    ok &= ok_year
                    with suppress(Exception):
                        sel = get_selected_text(ano_de)
                        if sel and not _year_matches(sel, p['ano_de']):
                            time.sleep(0.4)
                            ok &= _force_year(ano_de, p['ano_de']) or _force_year_any_select(fr, p['ano_de'])
                            _post_year_change(ano_de)
            else:
                ok &= _force_year_any_select(fr, p['ano_de'])
            if mes_ate and not only_year:
                ok &= select_month(mes_ate, p['mes_ate'])
            if ano_ate:
                if only_year and y2:
                    ok_year = _force_year_by_id(fr, "vANOSEMMOV2", p['ano_ate'])
                    if not ok_year:
                        ok_year = _force_year(ano_ate, p['ano_ate']) or _force_year_any_select(fr, p['ano_ate'])
                    if ok_year:
                        _post_year_change(ano_ate)
                    ok &= ok_year
                else:
                    ok_year = _force_year(ano_ate, p['ano_ate'])
                    if not ok_year:
                        ok_year = _force_year_any_select(fr, p['ano_ate'])
                    if ok_year:
                        _post_year_change(ano_ate)
                    ok &= ok_year
                    with suppress(Exception):
                        sel = get_selected_text(ano_ate)
                        if sel and not _year_matches(sel, p['ano_ate']):
                            time.sleep(0.4)
                            ok &= _force_year(ano_ate, p['ano_ate']) or _force_year_any_select(fr, p['ano_ate'])
                            _post_year_change(ano_ate)
            else:
                ok &= _force_year_any_select(fr, p['ano_ate'])
            if not ok and str(p.get('ano_de')) == str(p.get('ano_ate')):
                ok = _force_year_all_fields(fr, p['ano_de']) or ok
            if ok:
                log_info(f"Periodo aplicado (movimentacao): {p['mes_de']}/{p['ano_de']} ate {p['mes_ate']}/{p['ano_ate']} (ok={ok})")
                return
            time.sleep(0.5)
        log_info(f"Periodo aplicado (movimentacao): {p['mes_de']}/{p['ano_de']} ate {p['mes_ate']}/{p['ano_ate']} (ok={ok})")
        return
    log_info("Periodo de movimentacao: campos nao encontrados.")
#Download de livro




# === Restored functions (auto) ===

def _nm_click_pesquisar(page: Page, label: str, selector: Optional[str] = None) -> None:
    log_info(f"Clicando em 'Pesquisar' na {label}...")
    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
    def _normalize_selector(sel: str) -> str:
        sel = (sel or "").strip()
        if not sel:
            return sel
        if sel.startswith(("xpath=", "css=")):
            return sel
        if sel.startswith(("//", ".//", "(")):
            return f"xpath={sel}"
        return sel
    selectors = []
    if selector:
        selectors.append(_normalize_selector(selector))
    selectors.extend([
        "xpath=//input[(contains(translate(@value,'PESQUISAR','pesquisar'),'pesquisar')"
        "      or contains(translate(@title,'PESQUISAR','pesquisar'),'pesquisar')"
        "      or contains(translate(@id,'PESQUISAR','pesquisar'),'pesquisar')"
        "      or contains(translate(@name,'PESQUISAR','pesquisar'),'pesquisar'))"
        "      and not(contains(@style,'display:none'))]",
        "xpath=//button[contains(translate(normalize-space(.),'PESQUISAR','pesquisar'),'pesquisar')"
        "      and not(contains(@style,'display:none'))]",
        "xpath=//a[contains(translate(normalize-space(.),'PESQUISAR','pesquisar'),'pesquisar')]",
    ])
    for fr in frames:
        for sel in selectors:
            try:
                with suppress(Exception):
                    loc = fr.wait_for_selector(sel, state="visible", timeout=1500)
                    if loc:
                        loc.scroll_into_view_if_needed(timeout=1000)
                        loc.click(timeout=5000)
                        log_info("Clique em Pesquisar executado.")
                        time.sleep(1.0)
                        return
                loc = fr.locator(sel).first
                if not loc.count() or not loc.is_visible():
                    continue
                with suppress(Exception):
                    loc.scroll_into_view_if_needed(timeout=1000)
                try:
                    loc.click(timeout=5000)
                except Exception:
                    loc.evaluate("el => el.click()")
                log_info("Clique em Pesquisar executado.")
                time.sleep(1.0)
                return
            except Exception:
                continue
    log_error("Nao foi possivel localizar o botao Pesquisar.")
    raise RuntimeError("Botao Pesquisar nao encontrado")


def _nm_find_table(fr, candidates: Optional[List[str]] = None):
    cands = candidates or [
        "//div[@id='Grid2ContainerDiv']//table",
        "//div[@id='GridContainerDiv']//table",
        "//table[contains(@class,'Grid') or contains(@class,'DataGrid')]",
        "//table[@id='TABLE1']",
    ]
    for xp in cands:
        try:
            sel = xp if xp.startswith("xpath=") or xp.startswith("css=") else f"xpath={xp}"
            tbl = fr.wait_for_selector(sel, timeout=4000)
            if tbl and tbl.is_visible():
                return tbl
        except Exception:
            continue
    return None


def _nm_find_header_index(tbl, keywords: List[str]) -> Optional[int]:
    for r in (1, 2, 3):
        cells = (tbl.query_selector_all(f"xpath=.//tr[{r}]/*[self::th or self::td]") or [])
        for i, th in enumerate(cells, start=1):
            txt = normalize_text(th.inner_text() or "")
            if any(k in txt for k in keywords):
                return i
    return None


def _nm_parse_int(text: str) -> int:
    try:
        val = re.sub(r"\D", "", text or "")
        return int(val) if val else 0
    except Exception:
        return 0


def _nm_periodo_datas() -> Tuple[str, str]:
    p = get_periodo_dict()
    y_de = int(p.get('ano_de'))
    m_de = int(p.get('mes_de'))
    y_ate = int(p.get('ano_ate'))
    m_ate = int(p.get('mes_ate'))
    start = datetime(y_de, m_de, 1)
    last_day = calendar.monthrange(y_ate, m_ate)[1]
    end = datetime(y_ate, m_ate, last_day)
    return start.strftime("%d/%m/%Y"), end.strftime("%d/%m/%Y")


def _nm_find_date_inputs(fr):
    def _vis(sel: str):
        try:
            loc = fr.locator(sel).first
            return loc if loc.count() and loc.is_visible() else None
        except Exception:
            return None
    def _vis_any(selectors):
        for sel in selectors:
            loc = _vis(sel)
            if loc:
                return loc
        return None

    ini = _vis_any([
        "#vDTAINI", "#vDTINI", "#vDTINICIO",
        "input[name='vDTAINI']", "input[name='vDTINI']", "input[name='vDTINICIO']",
        "input[id*='DTAINI']", "input[id*='DTINI']", "input[id*='DTINICIO']",
    ])
    fim = _vis_any([
        "#vDTAFIM", "#vDTFIM", "#vDTAFINAL", "#vDTFINAL",
        "input[name='vDTAFIM']", "input[name='vDTFIM']", "input[name='vDTFINAL']",
        "input[id*='DTAFIM']", "input[id*='DTFIM']", "input[id*='DTFINAL']",
    ])

    if not ini:
        ini = _vis(
            "xpath=//*[contains(normalize-space(.),'Data InÃ­cio') or "
            "contains(normalize-space(.),'Data Inicio') or "
            "contains(normalize-space(.),'DATA INÃCIO') or "
            "contains(normalize-space(.),'DATA INICIO')]/following::input[1]"
        )
    if not fim:
        fim = _vis(
            "xpath=//*[contains(normalize-space(.),'Data Fim') or "
            "contains(normalize-space(.),'DATA FIM')]/following::input[1]"
        )
    if not (ini and fim):
        try:
            inputs = fr.locator("input[type='text'], input:not([type])")
            count = min(inputs.count(), 12)
            candidates = []
            for i in range(count):
                it = inputs.nth(i)
                if not it.is_visible():
                    continue
                ph = (it.get_attribute("placeholder") or "")
                val = (it.input_value(timeout=500) or "")
                if "/" in ph or "/" in val:
                    candidates.append(it)
            if len(candidates) >= 2:
                ini = ini or candidates[0]
                fim = fim or candidates[1]
        except Exception:
            pass
    return ini, fim


def _nm_fill_input(elem, value: str) -> bool:
    with suppress(Exception):
        if elem.get_attribute("readonly"):
            elem.evaluate("el => el.removeAttribute('readonly')")
    try:
        elem.fill(value, timeout=2000)
    except Exception:
        try:
            elem.evaluate("""
                (el, v)=>{
                    if (!el) return false;
                    el.value = v;
                    el.dispatchEvent(new Event('input', {bubbles:true}));
                    el.dispatchEvent(new Event('change', {bubbles:true}));
                    return true;
                }
            """, value)
        except Exception:
            return False
    with suppress(Exception):
        cur = elem.input_value(timeout=1000)
        if cur and cur.strip() == value:
            return True
    try:
        elem.click(timeout=1000)
        elem.press("Control+A")
        elem.type(value, delay=30)
    except Exception:
        pass
    with suppress(Exception):
        cur = elem.input_value(timeout=1000)
        if cur and cur.strip() == value:
            return True
    return True


def _nm_find_select_with_options(fr, required: List[str]):
    req_norm = [normalize_text(r) for r in required]
    try:
        sels = fr.locator("select")
        count = min(sels.count(), 30)
        for i in range(count):
            sel = sels.nth(i)
            try:
                opts = sel.evaluate("el => Array.from(el.options||[]).map(o => o.textContent || o.label || o.value || '')")
            except Exception:
                continue
            opts_norm = [normalize_text(o) for o in (opts or [])]
            if all(any(r in o for o in opts_norm) for r in req_norm):
                return sel
    except Exception:
        pass
    return None


def _nm_set_select_by_text(sel, text: str) -> bool:
    txt_norm = normalize_text(text)
    try:
        sel.select_option(label=text, timeout=2000)
    except Exception:
        try:
            sel.evaluate("""
                (el, txt)=>{
                    const norm = (s)=> (s||'')
                      .normalize('NFKD').replace(/[\u0300-\u036f]/g,'')
                      .replace(/\\s+/g,' ').trim().toLowerCase();
                    if (!el) return false;
                    const opts = Array.from(el.options||[]);
                    const tgt = opts.find(o => norm(o.textContent||o.label||'').includes(norm(txt))
                                         || norm(o.value||'').includes(norm(txt)));
                    if (!tgt) return false;
                    el.value = String(tgt.value || tgt.textContent || '');
                    el.dispatchEvent(new Event('input', {bubbles:true}));
                    el.dispatchEvent(new Event('change', {bubbles:true}));
                    return true;
                }
            """, text)
        except Exception:
            return False
    with suppress(Exception):
        cur = get_selected_text(sel)
        if txt_norm in normalize_text(cur):
            return True
    return True














def abrir_escrituracao_contabilidade(page: Page) -> None:
    if "hwmcontabilidade" not in (page.url or "").lower():
        target = _build_contabilidade_url(page)
        try:
            page.goto(target, wait_until="networkidle", timeout=15000)
        except Exception:
            pass

    data_ini, data_fim = _nm_periodo_datas()
    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
    for fr in frames:
        ini, fim = _nm_find_date_inputs(fr)
        if not (ini and fim):
            continue
        log_info(f"Periodo GUI -> Browser: Data Inicio={data_ini} | Data Fim={data_fim}")
        _nm_fill_input(ini, data_ini)
        _nm_fill_input(fim, data_fim)
        return
    log_info("Campos de Data Inicio/Fim nao encontrados.")


def _nm_set_tipo_nota(page: Page, tipo: str) -> None:
    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
    for fr in frames:
        sel = _nm_find_select_with_options(fr, ["emit", "receb"]) or _nm_find_select_with_options(fr, ["nota"]) 
        if not sel:
            continue
        if _nm_set_select_by_text(sel, tipo):
            log_info(f"Tipo Nota fixado em '{tipo}'.")
            return
    log_info("Select de Tipo Nota nao encontrado.")


def _nm_download_notas(page: Page, label: str) -> Optional[Path]:
    _nm_click_pesquisar(page, label)

    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
    for fr in frames:
        tbl = _nm_find_table(fr)
        if not tbl:
            continue
        idx_qtd = _nm_find_header_index(tbl, ["qtd notas", "qtd doc", "quantidade", "qtd"]) or None
        idx_proc = _nm_find_header_index(tbl, ["processar"]) or None
        idx_dl = _nm_find_header_index(tbl, ["download", "baixar"]) or None
        rows = (tbl.query_selector_all("xpath=.//tbody/tr[td]") or tbl.query_selector_all("xpath=.//tr[td]") or [])
        if not rows:
            continue

        def _find_clickable_in_cell(cell):
            if not cell:
                return None
            return (
                cell.query_selector("xpath=.//a[.//img]") or
                cell.query_selector("xpath=.//a") or
                cell.query_selector("xpath=.//img") or
                cell.query_selector("xpath=.//input[@type='image' or @type='button' or @type='submit']") or
                cell.query_selector("xpath=.//button")
            )

        for i, row in enumerate(rows, start=1):
            if not idx_qtd:
                qtd = 1
            else:
                cell = row.query_selector(f"xpath=./td[{idx_qtd}]")
                qtd = _nm_parse_int(cell.inner_text() if cell else "0")
            log_info(f"Qtd Notas (linha {i}) = {qtd}")
            if qtd < 1:
                continue

            # Processar
            proc = None
            if idx_proc:
                proc_cell = row.query_selector(f"xpath=./td[{idx_proc}]")
                proc = _find_clickable_in_cell(proc_cell)
            if not proc:
                proc = (row.query_selector("xpath=.//input[contains(translate(@value,'PROCESSAR','processar'),'processar')]") or
                        row.query_selector("xpath=.//button[contains(translate(normalize-space(.),'PROCESSAR','processar'),'processar')]") or
                        row.query_selector("xpath=.//a[contains(translate(normalize-space(.),'PROCESSAR','processar'),'processar')]") )
            if proc:
                log_info("Clicando 'Processar'...")
                _safe_click_with_fallback(page, proc, f"{label}:processar")
                with suppress(Exception):
                    page.wait_for_load_state("domcontentloaded", timeout=6000)
                with suppress(Exception):
                    page.wait_for_timeout(800)

            # Download
            dl_btn = None
            for _ in range(3):
                if idx_dl:
                    dl_cell = row.query_selector(f"xpath=./td[{idx_dl}]")
                    dl_btn = _find_clickable_in_cell(dl_cell)
                if not dl_btn:
                    dl_btn = (row.query_selector("xpath=.//input[contains(translate(@value,'DOWNLOAD','download'),'download')]") or
                              row.query_selector("xpath=.//button[contains(translate(normalize-space(.),'DOWNLOAD','download'),'download')]") or
                              row.query_selector("xpath=.//a[contains(translate(normalize-space(.),'DOWNLOAD','download'),'download')]") )
                if dl_btn:
                    break
                with suppress(Exception):
                    page.wait_for_timeout(700)

            if not dl_btn:
                log_info("Download nao encontrado nesta linha.")
                continue
            log_info("Clicando 'Download'... (esperando ZIP)")
            try:
                with page.expect_download(timeout=25000) as dlinfo:
                    _safe_click_with_fallback(page, dl_btn, f"{label}:download")
                dl = dlinfo.value
                fname = dl.suggested_filename or "notas.zip"
                fp = _resolve_target_filepath(_get_downloads_dir(), page, fname)
                dl.save_as(str(fp))
                log_info(f"Download salvo: {fp}")
                return fp
            except Exception as e:
                log_error(f"Falha ao baixar: {e}")
                return None
    log_info("Nenhuma linha com notas para baixar.")
    return None


def baixar_notas_emitidas(page: Page) -> Optional[Path]:
    log_info("Baixando Notas Emitidas...")
    _nm_set_tipo_nota(page, "Emitidas")
    return _nm_download_notas(page, "Notas Emitidas")


def baixar_notas_recebidas(page: Page) -> Optional[Path]:
    log_info("Baixando Notas Recebidas...")
    _nm_set_tipo_nota(page, "Recebidas")
    return _nm_download_notas(page, "Notas Recebidas")


def abrir_emissao_guias(page: Page) -> None:
    if "hwmguiarec" not in (page.url or "").lower():
        target = _build_guias_url(page)
        try:
            page.goto(target, wait_until="networkidle", timeout=15000)
        except Exception:
            pass

    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
    for fr in frames:
        sel = _nm_find_select_with_options(fr, ["todos"]) or _nm_find_select_with_options(fr, ["recolh"])
        if sel:
            if _nm_set_select_by_text(sel, "Todos"):
                log_info("Recolhimento = Todos (select).")
            break
    _nm_click_pesquisar(page, "Emissao de Guias")



# === Restored selection (auto) ===

def selecionar_empresa_por_lista(
    page: Page,
    lista_empresas: Iterable[EmpresaAlvo],
    col_nome: int = 4,
    col_cnpj: Optional[int] = None,
) -> None:
    if not lista_empresas:
        raise RuntimeError("Lista de empresas vazia.")

    alvos = []
    for item in lista_empresas:
        if isinstance(item, dict):
            nome = (item.get("empresa") or item.get("Empresa") or item.get("nome") or "").strip()
            cnpj = _only_digits(item.get("cnpj") or item.get("CNPJ") or "")
        else:
            nome = str(item or "").strip()
            cnpj = _only_digits(nome)
        alvos.append((normalize_text(nome), cnpj, nome))

    def _find_empresas_table(fr) -> Optional[object]:
        candidates = [
            "//div[@id='EmpresasContainerDiv']//table",
            "//div[contains(@id,'Empresas') or contains(@class,'Empresas')]//table",
            "//table[@id='TABLE1']",
            "//div[@id='GridContainerDiv']//table",
            "//table[contains(@class,'Grid') or contains(@class,'DataGrid')]",
        ]
        for xp in candidates:
            try:
                locs = fr.locator(f"xpath={xp}")
                count = min(locs.count(), 5)
                for i in range(count):
                    tbl = locs.nth(i)
                    try:
                        head = tbl.locator("xpath=.//tr[1]").inner_text(timeout=1500)
                    except Exception:
                        head = ""
                    head_norm = normalize_text(head)
                    if "cnpj" in head_norm or "empresa" in head_norm or "razao" in head_norm:
                        return tbl
                if count > 0:
                    return locs.nth(0)
            except Exception:
                continue
        return None

    def _find_header_indices(tbl) -> Tuple[Optional[int], Optional[int]]:
        idx_cnpj = idx_emp = None
        try:
            cells = tbl.query_selector_all("xpath=.//tr[1]/*[self::th or self::td]") or []
            for i, th in enumerate(cells, start=1):
                txt = normalize_text(th.inner_text() or "")
                if "cnpj" in txt:
                    idx_cnpj = i
                if "empresa" in txt or "razao" in txt or "razÃ£o" in txt:
                    idx_emp = i
        except Exception:
            pass
        return idx_cnpj, idx_emp

    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
    for fr in frames:
        tbl = _find_empresas_table(fr)
        if not tbl:
            continue
        idx_cnpj, idx_emp = _find_header_indices(tbl)
        rows = tbl.query_selector_all("xpath=.//tbody/tr[td]") or tbl.query_selector_all("xpath=.//tr[td]") or []
        for row in rows:
            cells = row.query_selector_all("xpath=./td") or []
            cell_texts = [(c.inner_text() or "").strip() for c in cells]
            row_text = " ".join([t for t in cell_texts if t])
            row_cnpj = ""

            if idx_cnpj and idx_cnpj <= len(cells):
                row_cnpj = _only_digits(cells[idx_cnpj - 1].inner_text())
            if not row_cnpj:
                m = re.search(r"\b(\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2})\b", row_text)
                if m:
                    row_cnpj = _only_digits(m.group(1))
            if not row_cnpj:
                row_cnpj = _only_digits(row_text)

            name_text = ""
            if idx_emp and idx_emp <= len(cells):
                name_text = (cells[idx_emp - 1].inner_text() or "").strip()
            if not name_text:
                name_text = row_text.strip()
            name_norm = normalize_text(name_text)

            for alvo_norm, alvo_cnpj, alvo_nome in alvos:
                if alvo_cnpj:
                    match = bool(row_cnpj and (alvo_cnpj == row_cnpj or alvo_cnpj in row_cnpj))
                else:
                    match = bool(alvo_norm and alvo_norm in name_norm)
                if not match:
                    continue

                link = None
                if idx_emp and idx_emp <= len(cells):
                    link = cells[idx_emp - 1].query_selector("xpath=.//a")
                if not link:
                    link = row.query_selector("xpath=.//a")
                if not link:
                    link = row

                with suppress(Exception):
                    row.scroll_into_view_if_needed(timeout=1500)
                chk = None
                with suppress(Exception):
                    chk = row.query_selector("xpath=.//input[@type='checkbox']")
                if chk:
                    with suppress(Exception):
                        if hasattr(chk, "is_checked"):
                            if not chk.is_checked():
                                _safe_click_with_fallback(page, chk, "selecionar_empresa:checkbox")
                        else:
                            _safe_click_with_fallback(page, chk, "selecionar_empresa:checkbox")

                _safe_click_with_fallback(page, link, "selecionar_empresa")
                with suppress(Exception):
                    page.wait_for_load_state("domcontentloaded", timeout=8000)

                _set_empresa_context(name_text or alvo_nome, row_cnpj or alvo_cnpj)
                log_info(f"Contexto empresa definido: nome='{name_text or alvo_nome}' | cnpj='{row_cnpj or alvo_cnpj}'")
                return

    raise RuntimeError("Empresa alvo nao encontrada na grade.")
# _browser.py overrides appended below.

def _extrair_nome_alvo(item) -> str:
    """
    Aceita:
      - string com o nome/razao social
      - dict com possiveis chaves: 'empresa', 'razao', 'razao_social', 'nome'
    Retorna string (ou "" se nada adequado).
    """
    if isinstance(item, str):
        return item.strip()
    if isinstance(item, dict):
        for k in ("empresa", "razao", "razao_social", "nome", "Nome", "Razao", "Razao Social"):
            v = item.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        partes = []
        for v in item.values():
            if isinstance(v, (str, int, float)):
                partes.append(str(v))
        return " ".join(partes).strip()
    return ""


def _wait_and_click_modal_ok(page: Page, timeout_ms: int = 8000) -> bool:
    """
    Varre pagina e iframes procurando botoes 'OK/Confirmar/Sim' (button/input/a).
    Clica no primeiro visivel/habilitado. Retorna True se conseguiu.
    """
    sel = (
        "xpath=//button[normalize-space()='OK' or normalize-space()='Ok' "
        "               or normalize-space()='Confirmar' or normalize-space()='Sim']"
        " | //input[( @type='button' or @type='submit') and "
        "           ( translate(@value,'OKCONFIRMARSIM','okconfirmarsim')='ok' "
        "          or translate(@value,'OKCONFIRMARSIM','okconfirmarsim')='confirmar' "
        "          or translate(@value,'OKCONFIRMARSIM','okconfirmarsim')='sim')]"
        " | //a[normalize-space()='OK' or normalize-space()='Ok' "
        "      or normalize-space()='Confirmar' or normalize-space()='Sim']"
        " | //div[contains(@class,'modal') or contains(@id,'dialog') or contains(@class,'dialog')]"
        "//button|//input|//a[normalize-space()='OK' or normalize-space()='Ok' "
        "                       or normalize-space()='Confirmar' or normalize-space()='Sim']"
    )
    end = time.time() + (timeout_ms / 1000.0)
    while time.time() < end:
        for ctx in [page] + list(page.frames):
            try:
                btn = ctx.locator(sel).first
                if btn and btn.count() and btn.is_visible() and btn.is_enabled():
                    try:
                        btn.scroll_into_view_if_needed(timeout=1500)
                    except Exception:
                        pass
                    btn.click(timeout=4000)
                    return True
            except Exception:
                pass
        time.sleep(0.25)
    return False


def selecionar_empresa_por_lista(
    page: Page,
    lista_empresas,
    col_nome: int = 4,
    col_cnpj: Optional[int] = None,
) -> None:
    """
    Varre a grade de Empresas Autorizadas e clica na primeira empresa cujo
    nome (normalizado) conste em `lista_empresas`.
    Trata dialogo de confirmacao da troca.
    """
    log_info("Aguardando grade de empresas...")
    page.wait_for_selector("xpath=//div[@id='EmpresasContainerDiv']//table", timeout=15_000)

    linhas = page.query_selector_all("xpath=//div[@id='EmpresasContainerDiv']//tr[position()>1]")
    log_info(f"Empresas carregadas: {len(linhas)}")

    alvos_norm = []
    for it in (lista_empresas or []):
        nome = _extrair_nome_alvo(it)
        if not nome:
            continue
        alvos_norm.append(" ".join(nome.split()).lower())

    if not alvos_norm:
        raise RuntimeError("Lista de empresas-alvo vazia (depois da normalizacao).")

    for ln in linhas:
        cel = ln.query_selector(f"xpath=./td[{col_nome}]")
        if not cel:
            continue

        texto_bruto = (cel.inner_text() or "").strip()
        texto_norm = " ".join(texto_bruto.split()).lower()

        log_info(f" - {texto_bruto}")

        for alvo in alvos_norm:
            if alvo and alvo in texto_norm:
                cnpj_row = ""
                try:
                    if col_cnpj:
                        td_cnpj = ln.query_selector(f"xpath=./td[{col_cnpj}]")
                        if td_cnpj:
                            cnpj_row = _only_digits(td_cnpj.inner_text() or "")
                    if not cnpj_row:
                        row_txt = (ln.inner_text() or "")
                        m = re.search(r"\b(\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2})\b", row_txt)
                        if m:
                            cnpj_row = _only_digits(m.group(1))
                except Exception:
                    pass

                nome_row = texto_bruto
                _set_empresa_context(nome_row, cnpj_row or None)
                log_info(f"Contexto empresa definido: nome='{nome_row}' | cnpj='{cnpj_row or '??'}'")

                link = cel.query_selector("xpath=.//a") or ln.query_selector(
                    "xpath=.//a[contains(@href,'hwcminhasempresas') or contains(@href,'empresa') or contains(@href,'hwmnu')]"
                )
                if link:
                    log_info(f"Correspondencia com '{alvo}' - clicando...")
                    link.scroll_into_view_if_needed(timeout=5_000)
                    link.focus()
                    _click_with_dialog_or_modal(page, link)
                    return

    raise RuntimeError("Nenhuma das empresas-alvo foi encontrada na grade.")


def abrir_movimentacao_mensal(page: Page) -> None:
    """
    Leva o navegador ate /nfse/servlet/hwmovmensal.
    """
    target = _build_movimentacoes_url(page)
    if "hwmovmensal" in page.url.lower():
        _apply_period_to_movimentacao(page)
        if _click_pesquisar_movimentacao(page, X_BTN_MOV_PESQUISAR):
            return
    if _try_open_movimentacao_by_url(page, target, X_BTN_MOV_PESQUISAR):
        return

    try:
        page.hover(X_MENU_MOVIMENTACOES, timeout=8_000)
        page.click(X_SUBMENU_MOV_MENSAL, timeout=8_000)
        page.wait_for_url("**/hwmovmensal*", timeout=15_000)
    except Exception as e:
        raise RuntimeError(
            "Falha ao abrir a tela 'Movimentacao M' "
            f"via URL '{target}' ou via menu. Erro: {e}"
        )

    _apply_period_to_movimentacao(page)
    if not _click_pesquisar_movimentacao(page, X_BTN_MOV_PESQUISAR):
        log_error("Nao foi possivel localizar o botao Pesquisar.")
        raise RuntimeError("Botao Pesquisar nao encontrado")


PT_ABBR = ["", "jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]
IMG_XP = "xpath=.//img[not(contains(translate(@src,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'cinza')) and not(contains(translate(@src,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'gray'))]"

_MONTH_TOKENS = [
    ("janeiro", 1), ("jan", 1),
    ("fevereiro", 2), ("fev", 2),
    ("marco", 3), ("mar", 3),
    ("abril", 4), ("abr", 4),
    ("maio", 5), ("mai", 5),
    ("junho", 6), ("jun", 6),
    ("julho", 7), ("jul", 7),
    ("agosto", 8), ("ago", 8),
    ("setembro", 9), ("set", 9),
    ("outubro", 10), ("out", 10),
    ("novembro", 11), ("nov", 11),
    ("dezembro", 12), ("dez", 12),
]

def _get_target_month_year(mes_alvo: Optional[int], ano_alvo: Optional[int]) -> Tuple[int, int]:
    if mes_alvo and ano_alvo:
        return int(mes_alvo), int(ano_alvo)
    with suppress(Exception):
        from utils.periodo import get_periodo
        per = get_periodo() or {}
        m = int(per.get("mes_de") or per.get("mes") or 0)
        a = int(per.get("ano_de") or per.get("ano") or 0)
        if 1 <= m <= 12 and a:
            return m, a
    now = datetime.now()
    return now.month, now.year

def _parse_ref_month_year(text: str) -> Tuple[Optional[int], Optional[int]]:
    norm = normalize_text(text or "").replace("\xa0", " ").strip()
    if not norm:
        return None, None
    year = None
    m_year = re.search(r"(19|20)\d{2}", norm)
    if m_year:
        year = int(m_year.group(0))
    month = None
    for token, num in _MONTH_TOKENS:
        if token in norm:
            month = num
            break
    if month is None:
        tmp = norm
        if year:
            tmp = tmp.replace(str(year), " ")
        for n in re.findall(r"\d{1,2}", tmp):
            try:
                val = int(n)
            except Exception:
                continue
            if 1 <= val <= 12:
                month = val
                break
    return month, year

def _ref_matches_target(text: str, target_m: int, target_y: int) -> bool:
    m, y = _parse_ref_month_year(text)
    if not m or m != target_m:
        return False
    if y is not None and target_y and y != target_y:
        return False
    return True

REF_HEADER_TOKENS = ["referencia", "ref.", "compet", "competencia"]
ENC_HEADER_TOKENS = ["encerrar", "enc", "enc.", "encerra", "retifica", "encerra/retifica"]
SEM_HEADER_TOKENS = ["sem mov", "sem movimenta", "sem movimento", "declar", "decl."]
MES_HEADER_TOKENS = ["mes"] + REF_HEADER_TOKENS
ENC_ACTION_KEYWORDS = ["encerr", "encerra", "retific", "retifica", "gerar", "moviment", "fechar", "final"]
SEM_ACTION_KEYWORDS = ["sem mov", "sem moviment", "declar", "decl", "zerad"]
ESTORNO_TOKENS = ["estorno", "estornar", "estorn"]
DEBUG_ICON_LOG = (os.getenv("NM_DEBUG_ICONS", "1").strip().lower() in ("1", "true", "yes", "y", "sim"))

def _header_matches(txt: str, targets: List[str]) -> bool:
    for t in targets or []:
        if not t:
            continue
        if t == "enc":
            if re.search(r"\benc\b", txt):
                return True
            continue
        if t in txt:
            return True
    return False

def _is_active_action(el) -> bool:
    if not el:
        return False
    try:
        return bool(el.evaluate(
            """
            (el) => {
              if (!el) return false;
              const hasDisabled = (n) => {
                const dis = n.getAttribute('disabled') || n.getAttribute('aria-disabled');
                if (dis && dis !== 'false') return true;
                const cls = (n.getAttribute('class') || '');
                if (/disabled|inativo|inactive|off/i.test(cls)) return true;
                return false;
              };
              const isInactiveImg = (img) => {
                if (!img) return false;
                const src = (img.getAttribute('src') || '').toLowerCase();
                const cls = (img.getAttribute('class') || '').toLowerCase();
                const alt = (img.getAttribute('alt') || '').toLowerCase();
                const title = (img.getAttribute('title') || '').toLowerCase();
                if (src.includes('desativado') || src.includes('inativ')) return true;
                if (src.includes('cinza') || src.includes('gray')) return true;
                if (cls.includes('cinza') || cls.includes('gray')) return true;
                if (alt.includes('inativo') || title.includes('inativo')) return true;
                if (cls.includes('disabled')) return true;
                const st = window.getComputedStyle ? window.getComputedStyle(img) : null;
                const filt = st && (st.filter || '');
                const op = st && parseFloat(st.opacity || '1');
                if (filt && filt.toLowerCase().includes('grayscale')) return true;
                if (!isNaN(op) && op < 0.5) return true;
                return false;
              };
              const tag = (el.tagName || '').toLowerCase();
              if (hasDisabled(el)) return false;
              const hasClick = () => {
                if (el.getAttribute('onclick')) return true;
                if (tag === 'a') return !!el.getAttribute('href');
                if (tag === 'button') return true;
                if (tag === 'input') {
                  const typ = (el.getAttribute('type') || '').toLowerCase();
                  return (typ === 'image' || typ === 'button' || typ === 'submit');
                }
                if (el.getAttribute('role') === 'button') return true;
                return false;
              };
              if (tag === 'img') return !isInactiveImg(el);
              const img = el.querySelector && el.querySelector('img');
              if (img && isInactiveImg(img)) return false;
              if (tag === 'input') {
                const typ = (el.getAttribute('type') || '').toLowerCase();
                if (typ === 'image') {
                  const src = (el.getAttribute('src') || '').toLowerCase();
                  if (src.includes('desativado') || src.includes('inativ')) return false;
                  if (src.includes('cinza') || src.includes('gray')) return false;
                }
              }
              return hasClick() || true;
            }
            """
        ))
    except Exception:
        # fallback: se for img, evita clicar em icone cinza/gray
        try:
            tag = el.evaluate("el => (el.tagName || '').toLowerCase()") or ""
        except Exception:
            return True
        if tag == "img":
            try:
                src = (el.get_attribute("src") or "").lower()
            except Exception:
                src = ""
            if "desativado" in src or "inativ" in src or "cinza" in src or "gray" in src:
                return False
        if tag == "input":
            try:
                typ = (el.get_attribute("type") or "").lower()
            except Exception:
                typ = ""
            if typ == "image":
                try:
                    src = (el.get_attribute("src") or "").lower()
                except Exception:
                    src = ""
                if "desativado" in src or "inativ" in src or "cinza" in src or "gray" in src:
                    return False
        return True


def _is_estorno_action(el) -> bool:
    if not el:
        return False
    try:
        raw = el.evaluate(
            """
            (el) => {
              if (!el) return '';
              const attrs = [
                el.getAttribute('id'),
                el.getAttribute('title'),
                el.getAttribute('alt'),
                el.getAttribute('aria-label'),
                el.getAttribute('value'),
                el.getAttribute('name'),
                el.getAttribute('onclick'),
                el.getAttribute('href'),
                el.getAttribute('src'),
                el.textContent,
              ];
              const img = (el.tagName || '').toLowerCase() === 'img' ? el :
                (el.querySelector && el.querySelector('img'));
              if (img) {
                attrs.push(img.getAttribute('alt'));
                attrs.push(img.getAttribute('title'));
                attrs.push(img.getAttribute('src'));
              }
              return attrs.filter(Boolean).join(' ');
            }
            """
        ) or ""
    except Exception:
        raw = ""
    txt = normalize_text(raw or "")
    if not txt:
        return False
    return any(tok in txt for tok in ESTORNO_TOKENS)


def _ignore_estorno(link, contexto: str = ""):
    if link and _is_estorno_action(link):
        log_info(f"Ignorando icone de estorno{f' ({contexto})' if contexto else ''}.")
        return None
    return link

def _collect_clickable_candidates(root) -> List:
    if not root:
        return []
    try:
        return root.query_selector_all("xpath=.//*[self::a or self::button or self::input or self::img]")
    except Exception:
        return []

def _get_el_info(el) -> dict:
    info: dict = {}
    if not el:
        return info
    try:
        info = el.evaluate(
            """
            (el) => ({
              tag: (el.tagName || '').toLowerCase(),
              id: el.getAttribute('id') || '',
              title: el.getAttribute('title') || '',
              alt: el.getAttribute('alt') || '',
              src: el.getAttribute('src') || '',
              href: el.getAttribute('href') || '',
              onclick: el.getAttribute('onclick') || '',
              value: (el.getAttribute('value') || ''),
              type: (el.getAttribute('type') || ''),
              role: (el.getAttribute('role') || ''),
              cls: (el.getAttribute('class') || ''),
              text: (el.textContent || '').trim(),
            })
            """
        ) or {}
    except Exception:
        info = {}
    try:
        info["visible"] = bool(el.is_visible())
    except Exception:
        info["visible"] = False
    return info

def _fmt_info(info: dict) -> str:
    def _trunc(s: str, n: int = 40) -> str:
        s = s or ""
        return (s[:n] + "...") if len(s) > n else s
    parts = [
        f"tag={info.get('tag','')}",
        f"id={_trunc(info.get('id',''))}",
        f"title={_trunc(info.get('title',''))}",
        f"alt={_trunc(info.get('alt',''))}",
        f"src={_trunc(info.get('src',''))}",
        f"href={_trunc(info.get('href',''))}",
        f"onclick={_trunc(info.get('onclick',''))}",
        f"value={_trunc(info.get('value',''))}",
        f"type={_trunc(info.get('type',''))}",
        f"role={_trunc(info.get('role',''))}",
        f"class={_trunc(info.get('cls',''))}",
        f"text={_trunc(info.get('text',''))}",
        f"visible={info.get('visible', False)}",
    ]
    return " ".join(parts)

def _debug_log_clickables(root, contexto: str, limit: int = 8) -> None:
    if not DEBUG_ICON_LOG:
        return
    cands = _collect_clickable_candidates(root)
    log_info(f"[dbg] {contexto}: candidatos={len(cands)}")
    for i, el in enumerate(cands[: max(0, int(limit))], start=1):
        info = _get_el_info(el)
        log_info(f"[dbg] {contexto}#{i}: {_fmt_info(info)}")

def _has_click_handler(el) -> bool:
    if not el:
        return False
    try:
        return bool(el.evaluate(
            """
            (el) => {
              if (!el) return false;
              const tag = (el.tagName || '').toLowerCase();
              if (el.getAttribute('disabled') || el.getAttribute('aria-disabled') === 'true') return false;
              if (el.getAttribute('onclick')) return true;
              if (tag === 'a') return !!el.getAttribute('href');
              if (tag === 'button') return true;
              if (tag === 'input') {
                const typ = (el.getAttribute('type') || '').toLowerCase();
                return (typ === 'image' || typ === 'button' || typ === 'submit');
              }
              if (el.getAttribute('role') === 'button') return true;
              const st = window.getComputedStyle ? window.getComputedStyle(el) : null;
              const cur = st && (st.cursor || '');
              if (cur && cur.toLowerCase() === 'pointer') return true;
              return false;
            }
            """
        ))
    except Exception:
        return False

def _score_clickable(el, keywords: List[str]) -> int:
    try:
        raw = el.evaluate(
            """
            (el) => {
              if (!el) return '';
              const attrs = [
                el.getAttribute('title'),
                el.getAttribute('alt'),
                el.getAttribute('aria-label'),
                el.getAttribute('value'),
                el.getAttribute('id'),
                el.getAttribute('name'),
                el.getAttribute('onclick'),
                el.getAttribute('href'),
                el.getAttribute('src'),
                el.textContent,
              ];
              return attrs.filter(Boolean).join(' ');
            }
            """
        )
    except Exception:
        raw = ""
    txt = normalize_text(raw or "")
    score = 0
    for kw in keywords or []:
        if kw and kw in txt:
            score += 1
    return score

def _find_best_clickable(
    root,
    keywords: List[str],
    contexto: str = "",
    require_keywords: bool = False,
    exclude_estorno: bool = False,
):
    cands = _collect_clickable_candidates(root)
    best = None
    best_score = -1
    for el in cands:
        if exclude_estorno and _is_estorno_action(el):
            continue
        if not _is_active_action(el):
            continue
        score = _score_clickable(el, keywords)
        if score > best_score:
            best = el
            best_score = score
    if best and (best_score > 0 or not require_keywords):
        if DEBUG_ICON_LOG:
            info = _get_el_info(best)
            log_info(f"[dbg] {contexto}: best_score={best_score} {_fmt_info(info)}")
        return best
    if require_keywords:
        if DEBUG_ICON_LOG:
            _debug_log_clickables(root, f"{contexto}:no_keyword_match")
        return None
    if DEBUG_ICON_LOG:
        _debug_log_clickables(root, f"{contexto}:no_best")
    for el in cands:
        if exclude_estorno and _is_estorno_action(el):
            continue
        if _has_click_handler(el):
            return el
    for el in cands:
        if exclude_estorno and _is_estorno_action(el):
            continue
        if _is_active_action(el):
            return el
    for el in cands:
        if exclude_estorno and _is_estorno_action(el):
            continue
        try:
            if el.is_visible():
                return el
        except Exception:
            continue
    return None

def _confirm_icon_cleared(
    page: Page,
    link,
    refresh_btn_selector: Optional[str],
    context: str,
    attempts: int = 3,
) -> bool:
    el_id = None
    with suppress(Exception):
        el_id = link.get_attribute("id")
    def _find_by_id():
        if not el_id:
            return None
        frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
        for fr in frames:
            try:
                el = fr.query_selector(f"xpath=.//*[@id='{el_id}']")
            except Exception:
                el = None
            if el:
                return el
        return None
    for _ in range(max(1, int(attempts))):
        with suppress(PWTimeout):
            page.wait_for_load_state("networkidle", timeout=2000)
        try:
            if el_id:
                cur = _find_by_id()
                if not cur or not _is_active_action(cur):
                    return True
            elif not _is_active_action(link):
                return True
        except Exception:
            return True
        if refresh_btn_selector:
            _click_pesquisar_movimentacao(page, refresh_btn_selector)
    log_error(f"Icone ainda visivel apos {attempts} tentativas ({context}).")
    return False

def _is_visible_el(el) -> bool:
    return _is_active_action(el) and (not _is_estorno_action(el))

X_ENC_FALLBACK_XP = (
    "xpath=.//*[self::a or self::button or self::input or self::img]"
    "[contains(translate(@title,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'encerr')"
    " or contains(translate(@alt,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'encerr')"
    " or contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'encerr')"
    " or contains(translate(@value,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'encerr')"
    " or contains(translate(@src,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'encerr')"
    " or contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'encerr')"
    " or contains(translate(@title,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'retific')"
    " or contains(translate(@alt,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'retific')"
    " or contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'retific')"
    " or contains(translate(@value,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'retific')"
    " or contains(translate(@src,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'retific')"
    " or contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'retific')]"
)

X_SEM_MOV_FALLBACK_XP = (
    "xpath=.//*[self::a or self::button or self::input or self::img]"
    "[contains(translate(@title,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'declar')"
    " or contains(translate(@alt,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'declar')"
    " or contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'declar')"
    " or contains(translate(@value,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'declar')"
    " or contains(translate(@src,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'declar')"
    " or contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'declar')"
    " or contains(translate(@title,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'sem mov')"
    " or contains(translate(@alt,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'sem mov')"
    " or contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'sem mov')"
    " or contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'sem mov')"
    " or contains(translate(@title,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'sem moviment')"
    " or contains(translate(@alt,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'sem moviment')"
    " or contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'sem moviment')"
    " or contains(translate(@src,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'sem mov')"
    " or contains(translate(@src,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'sem moviment')"
    " or contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'sem moviment')]"
)

def _find_enc_action_in_cell(cel, sufixo: str):
    if not cel:
        return None
    candidates = [
        ("id", cel.query_selector(f"xpath=.//*[@id='vENCERRARETIFICA_{sufixo}']")),
        ("kw", cel.query_selector(
            "xpath=.//*[self::a or self::button or self::input or self::img]"
            "[contains(translate(@onclick,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'encerr')"
            " or contains(translate(@href,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'encerr')"
            " or contains(translate(@onclick,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'retific')"
            " or contains(translate(@href,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'retific')]"
        )),
        ("kw", cel.query_selector(X_ENC_FALLBACK_XP)),
        ("generic", cel.query_selector("xpath=.//a")),
        ("generic", cel.query_selector(IMG_XP)),
    ]
    for kind, link in candidates:
        if not _is_active_action(link):
            continue
        if _is_estorno_action(link):
            continue
        if kind == "generic" and _score_clickable(link, ENC_ACTION_KEYWORDS) <= 0:
            continue
        return link
    return _find_best_clickable(
        cel,
        ENC_ACTION_KEYWORDS,
        contexto="enc:cell",
        require_keywords=True,
        exclude_estorno=True,
    )

def _find_sem_mov_in_cell(cel, sufixo: str):
    if not cel:
        return None
    candidates = [
        ("id", cel.query_selector(f"xpath=.//*[@id='vDECLARAR_{sufixo}']")),
        ("kw", cel.query_selector(
            "xpath=.//*[self::a or self::button or self::input or self::img]"
            "[contains(translate(@onclick,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'declar')"
            " or contains(translate(@href,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'declar')"
            " or contains(translate(@onclick,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'sem mov')"
            " or contains(translate(@href,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'sem mov')]"
        )),
        ("kw", cel.query_selector(X_SEM_MOV_FALLBACK_XP)),
        ("generic", cel.query_selector("xpath=.//a")),
        ("generic", cel.query_selector(IMG_XP)),
    ]
    for kind, link in candidates:
        if not _is_active_action(link):
            continue
        if _is_estorno_action(link):
            continue
        if kind == "generic" and _score_clickable(link, SEM_ACTION_KEYWORDS) <= 0:
            continue
        return link
    return _find_best_clickable(
        cel,
        SEM_ACTION_KEYWORDS,
        contexto="sem:cell",
        require_keywords=True,
        exclude_estorno=True,
    )

def _find_enc_action_in_row(ln, sufixo: str):
    if not ln:
        return None
    candidates = [
        ("id", ln.query_selector(f"xpath=.//*[@id='vENCERRARETIFICA_{sufixo}']")),
        ("kw", ln.query_selector(
            "xpath=.//*[self::a or self::button or self::input or self::img]"
            "[contains(translate(@onclick,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'encerr')"
            " or contains(translate(@href,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'encerr')"
            " or contains(translate(@onclick,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'retific')"
            " or contains(translate(@href,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'retific')]"
        )),
        ("kw", ln.query_selector(X_ENC_FALLBACK_XP)),
        ("generic", ln.query_selector(IMG_XP)),
    ]
    for kind, link in candidates:
        if not _is_active_action(link):
            continue
        if _is_estorno_action(link):
            continue
        if kind == "generic" and _score_clickable(link, ENC_ACTION_KEYWORDS) <= 0:
            continue
        return link
    return _find_best_clickable(
        ln,
        ENC_ACTION_KEYWORDS,
        contexto="enc:row",
        require_keywords=True,
        exclude_estorno=True,
    )

def _find_sem_mov_in_row(ln, sufixo: str):
    if not ln:
        return None
    candidates = [
        ("id", ln.query_selector(f"xpath=.//*[@id='vDECLARAR_{sufixo}']")),
        ("kw", ln.query_selector(
            "xpath=.//*[self::a or self::button or self::input or self::img]"
            "[contains(translate(@onclick,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'declar')"
            " or contains(translate(@href,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'declar')"
            " or contains(translate(@onclick,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'sem mov')"
            " or contains(translate(@href,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'sem mov')]"
        )),
        ("kw", ln.query_selector(X_SEM_MOV_FALLBACK_XP)),
        ("generic", ln.query_selector(IMG_XP)),
    ]
    for kind, link in candidates:
        if not _is_active_action(link):
            continue
        if _is_estorno_action(link):
            continue
        if kind == "generic" and _score_clickable(link, SEM_ACTION_KEYWORDS) <= 0:
            continue
        return link
    return _find_best_clickable(
        ln,
        SEM_ACTION_KEYWORDS,
        contexto="sem:row",
        require_keywords=True,
        exclude_estorno=True,
    )


def _get_ano_alvo_default(ano_alvo: Optional[int]) -> str:
    if ano_alvo:
        return str(int(ano_alvo))
    try:
        from utils.periodo import get_periodo
        per = get_periodo() or {}
        a = int(per.get("ano_de") or per.get("ano") or 0)
        if a:
            return str(a)
    except Exception:
        pass
    return str(datetime.now().year)


def _set_movimentacao_ano_dropdown(page: Page, ano_alvo: Optional[int], xpaths: List[str]) -> bool:
    yy = _get_ano_alvo_default(ano_alvo)
    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
    set_any = False
    for fr in frames:
        for xp in xpaths:
            try:
                locs = fr.locator(f"xpath={xp}")
                total = min(locs.count(), 6)
            except Exception:
                continue
            if total <= 0:
                continue
            for i in range(total):
                loc = locs.nth(i)
                try:
                    visible = loc.is_visible()
                except Exception:
                    visible = False
                if not visible and i < total - 1:
                    continue
                ok = False
                try:
                    loc.select_option(value=yy, timeout=2000)
                    ok = True
                except Exception:
                    try:
                        loc.select_option(label=yy, timeout=2000)
                        ok = True
                    except Exception:
                        ok = False
                if ok:
                    with suppress(Exception):
                        sel = get_selected_text(loc)
                        if sel:
                            log_info(f"Ano selecionado ({xp}): {sel}")
                    set_any = True
                    continue
                try:
                    ok_js = loc.evaluate(
                        """
                        (el, yy) => {
                          if (!el || !el.options) return false;
                          const opts = Array.from(el.options || []);
                          let idx = opts.findIndex(o => (o.value || '') === yy);
                          if (idx < 0) {
                            idx = opts.findIndex(o => (o.textContent || '').trim() === yy);
                          }
                          if (idx < 0) return false;
                          for (let i = 0; i < opts.length; i++) {
                            opts[i].selected = (i === idx);
                          }
                          el.selectedIndex = idx;
                          el.value = String(opts[idx].value || opts[idx].textContent || '');
                          if (typeof el.onfocus === 'function') el.onfocus.call(el);
                          el.dispatchEvent(new Event('input',  {bubbles:true}));
                          el.dispatchEvent(new Event('change', {bubbles:true}));
                          if (typeof el.onchange === 'function') el.onchange.call(el);
                          if (typeof el.onblur === 'function') el.onblur.call(el);
                          if (window.gx && gx.evt && el.id === 'vANOSEMMOV' && gx.evt.execEvt) {
                            gx.evt.execEvt('', false, 'EVANOSEMMOV.CLICK.', el);
                          }
                          return true;
                        }
                        """,
                        yy,
                    )
                    if ok_js:
                        with suppress(Exception):
                            sel = get_selected_text(loc)
                            if sel:
                                log_info(f"Ano selecionado ({xp}): {sel}")
                        set_any = True
                except Exception:
                    pass
    if set_any:
        with suppress(Exception):
            page.wait_for_load_state("networkidle", timeout=3000)
    return set_any


def _click_pesquisar_movimentacao(page: Page, btn_selector: str) -> bool:
    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
    x_pesq_generic = (
        "//button[normalize-space()='Pesquisar']"
        " | //input[((@type='button') or (@type='submit')) and (@value='Pesquisar' or @title='Pesquisar')]"
        " | //a[normalize-space()='Pesquisar']"
    )
    for fr in frames:
        try:
            btn = fr.locator(btn_selector).first
            if btn.count() and btn.is_visible():
                with suppress(Exception):
                    btn.scroll_into_view_if_needed(timeout=1000)
                with suppress(Exception):
                    page.wait_for_function("el => !el.disabled", arg=btn, timeout=3000)
                btn.click(timeout=8000)
                with suppress(PWTimeout):
                    page.wait_for_load_state("networkidle", timeout=8000)
                log_info("Clique em 'Pesquisar' executado.")
                return True
        except Exception:
            pass
    for fr in frames:
        try:
            btn = fr.locator(f"xpath={x_pesq_generic}").first
        except Exception:
            btn = None
        if not btn or not btn.count():
            continue
        try:
            if not btn.is_visible():
                continue
        except Exception:
            continue
        with suppress(Exception):
            btn.scroll_into_view_if_needed(timeout=1000)
        try:
            btn.click(timeout=8000)
            with suppress(PWTimeout):
                page.wait_for_load_state("networkidle", timeout=8000)
            log_info("Clique em 'Pesquisar' executado (fallback).")
            return True
        except Exception:
            pass
    log_info("Botao 'Pesquisar' nao encontrado (encerramento).")
    return False


def _try_open_movimentacao_by_url(page: Page, target: str, btn_selector: str) -> bool:
    try:
        page.goto(target, wait_until="networkidle", timeout=15_000)
    except Exception:
        return False
    _apply_period_to_movimentacao(page)
    return _click_pesquisar_movimentacao(page, btn_selector)


def _force_ano_by_xpath_gx(page: Page, ano_alvo: Optional[int], xpaths: List[str]) -> bool:
    yy = _get_ano_alvo_default(ano_alvo)
    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
    ok_any = False
    for fr in frames:
        for xp in xpaths:
            try:
                el = fr.wait_for_selector(f"xpath={xp}", state="visible", timeout=4000)
            except Exception:
                el = None
            if not el:
                continue
            with suppress(Exception):
                el.scroll_into_view_if_needed(timeout=1000)
            with suppress(Exception):
                el.click(timeout=1500, force=True)
                page.wait_for_timeout(100)
            with suppress(Exception):
                page.wait_for_function("el => !el.disabled", arg=el, timeout=2000)
            try:
                el.select_option(value=yy, timeout=2000)
            except Exception:
                with suppress(Exception):
                    el.select_option(label=yy, timeout=2000)
            try:
                res = el.evaluate(
                    """
                    (el, yy) => {
                      if (!el || !el.options) return {ok:false, sel:''};
                      const opts = Array.from(el.options || []);
                      let idx = opts.findIndex(o => (o.value || '') === yy);
                      if (idx < 0) {
                        idx = opts.findIndex(o => (o.textContent || '').trim() === yy);
                      }
                      if (idx < 0) return {ok:false, sel:''};
                      const onfocusAttr = el.getAttribute('onfocus') || '';
                      const onblurAttr = el.getAttribute('onblur') || '';
                      let focusArgs = null;
                      const fm = onfocusAttr.match(/onfocus\\(this,\\s*(\\d+)\\s*,\\s*'([^']*)'\\s*,\\s*(true|false)\\s*,\\s*'([^']*)'\\s*,\\s*(\\d+)\\s*\\)/i);
                      if (fm) {
                        focusArgs = [parseInt(fm[1],10), fm[2], fm[3] === 'true', fm[4], parseInt(fm[5],10)];
                      }
                      let blurId = null;
                      const bm = onblurAttr.match(/onblur\\(this,\\s*(\\d+)/i);
                      if (bm) blurId = parseInt(bm[1], 10);
                      if (window.gx && gx.evt) {
                        if (gx.evt.onfocus && focusArgs) gx.evt.onfocus(el, ...focusArgs);
                        if (gx.evt.jsEvent) gx.evt.jsEvent(el);
                      }
                      for (let i = 0; i < opts.length; i++) {
                        opts[i].selected = (i === idx);
                      }
                      el.selectedIndex = idx;
                      el.value = String(opts[idx].value || opts[idx].textContent || '');
                      if (window.gx && gx.evt) {
                        if (gx.evt.onchange) gx.evt.onchange(el);
                        if (gx.evt.onblur && blurId !== null) gx.evt.onblur(el, blurId);
                      }
                      el.dispatchEvent(new Event('input',  {bubbles:true}));
                      el.dispatchEvent(new Event('change', {bubbles:true}));
                      el.dispatchEvent(new Event('blur',   {bubbles:true}));
                      const opt = el.selectedOptions && el.selectedOptions[0];
                      return {ok:true, sel:(opt && (opt.textContent || opt.label) || '').trim()};
                    }
                    """,
                    yy,
                )
                if res and res.get("ok"):
                    if res.get("sel"):
                        log_info(f"Ano selecionado ({xp}): {res.get('sel')}")
                    else:
                        log_info(f"Ano selecionado ({xp}): {yy}")
                    ok_any = True
                else:
                    try:
                        opt = fr.locator(f"xpath={xp}/option[@value='{yy}' or normalize-space()='{yy}']").first
                        if opt.count():
                            opt.click(timeout=2000)
                            ok_any = True
                            with suppress(Exception):
                                sel = get_selected_text(el)
                                if sel:
                                    log_info(f"Ano selecionado ({xp}): {sel}")
                    except Exception:
                        pass
            except Exception:
                pass
    if ok_any:
        with suppress(Exception):
            page.wait_for_load_state("networkidle", timeout=3000)
    return ok_any


def encerrar_mov_mensal(
    page: Page,
    tabela_xpath: str = "//div[@id='GridContainerDiv']//table",
    tabela_xpaths: Optional[List[str]] = None,
    mes_alvo: Optional[int] = None,
    ano_alvo: Optional[int] = None,
    _sem_retry: int = 0,
    _skip_sem: bool = False,
    _skip_enc: bool = False,
) -> bool:
    def _alvo_ref() -> str:
        if mes_alvo and ano_alvo:
            return f"{PT_ABBR[int(mes_alvo)]}/{int(ano_alvo)}".lower()
        with suppress(Exception):
            from utils.periodo import get_periodo
            per = get_periodo() or {}
            m = int(per.get("mes_de") or per.get("mes") or 0)
            a = int(per.get("ano_de") or per.get("ano") or 0)
            if 1 <= m <= 12 and a:
                return f"{PT_ABBR[m]}/{a}".lower()
        now = datetime.now()
        return f"{PT_ABBR[now.month]}/{now.year}".lower()

    def _norm_ref_mmm_aaaa(s: str) -> str:
        s = (s or "").strip()
        s = re.sub(r"\s*/\s*", "/", s)
        return s.lower()

    def _get_table(fr, tries: int = 5, pause: float = 0.3):
        t = None
        ext = globals().get("_find_table")
        for _ in range(tries):
            if not t and callable(ext) and tabela_xpaths:
                with suppress(Exception):
                    t = ext(fr, tabela_xpaths)
            if not t:
                t = fr.query_selector(f"xpath={tabela_xpath}")
            if t:
                break
            time.sleep(pause)
        return t

    def _find_header_index(tbl, targets: List[str]) -> Optional[int]:
        for r in (1, 2, 3):
            cells = (tbl.query_selector_all(f"xpath=.//tr[{r}]/th") or
                     tbl.query_selector_all(f"xpath=.//tr[{r}]/td") or [])
            for i, th in enumerate(cells, start=1):
                txt = normalize_text(th.inner_text() or "").replace("\xa0", " ")
                if _header_matches(txt, targets):
                    return i
        return None

    _set_movimentacao_ano_dropdown(
        page,
        ano_alvo,
        ["//select[@id='vANOSEMMOV1']", "//select[@id='vANOSEMMOV2']"],
    )
    for _ in range(2):
        if _force_ano_by_xpath_gx(page, ano_alvo, ["//*[@id='vANOSEMMOV1']", "//*[@id='vANOSEMMOV2']"]):
            break
        time.sleep(0.3)
    _click_pesquisar_movimentacao(page, X_BTN_MOV_PESQUISAR)
    _set_movimentacao_ano_dropdown(
        page,
        ano_alvo,
        ["//select[@id='vANOSEMMOV1']", "//select[@id='vANOSEMMOV2']"],
    )
    _click_pesquisar_movimentacao(page, X_BTN_MOV_PESQUISAR)
    ok_ano = _force_ano_by_xpath_gx(
        page,
        ano_alvo,
        ["//*[@id='vANOSEMMOV1']", "//*[@id='vANOSEMMOV2']"],
    )
    if not ok_ano:
        log_info("Ano NFCE nao selecionado via XPath; seguindo mesmo assim.")
    _click_pesquisar_movimentacao(page, X_BTN_MOV_PESQUISAR)
    ok_ano_nfce = _force_ano_by_xpath_gx(
        page,
        ano_alvo,
        ["//*[@id='vANOSEMMOV1']", "//*[@id='vANOSEMMOV2']"],
    )
    if not ok_ano_nfce:
        log_info("Ano NFCE nao selecionado via XPath; seguindo mesmo assim.")
    _click_pesquisar_movimentacao(page, X_BTN_MOV_PESQUISAR)
    target_m, target_y = _get_target_month_year(mes_alvo, ano_alvo)
    alvo_label = f"{PT_ABBR[target_m]}/{target_y}"
    log_info(f"Enc.: procurando linha com Referencia '{alvo_label}'...")

    def _row_has_target_month(ln, skip_cols: Optional[set]) -> bool:
        try:
            cells = ln.query_selector_all("xpath=./td") or []
        except Exception:
            cells = []
        for j, td in enumerate(cells, start=1):
            if skip_cols and j in skip_cols:
                continue
            try:
                txt = td.inner_text() or ""
            except Exception:
                txt = ""
            if _ref_matches_target(txt, target_m, target_y):
                return True
        return False

    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
    for fr in frames:
        log_info("Verificando frame principal..." if fr is page.main_frame else f"Verificando iframe '{fr.url or ''}'...")
        tbl = _get_table(fr)
        if not tbl:
            log_info("tabela nao encontrada aqui.")
            continue

        idx_ref = _find_header_index(tbl, REF_HEADER_TOKENS)
        idx_sem = _find_header_index(tbl, SEM_HEADER_TOKENS)
        idx_enc = _find_header_index(tbl, ENC_HEADER_TOKENS)
        if not idx_enc and not idx_sem:
            log_info(f"Cabecalhos ausentes (Ref={idx_ref} Sem={idx_sem} Enc={idx_enc}).")
            continue
        if not idx_enc:
            log_warning(f"Coluna Enc./Ret. ausente (Ref={idx_ref} Sem={idx_sem}). Verificando Sem Mov.")

        rows = (tbl.query_selector_all("xpath=.//tbody/tr[td]") or
                tbl.query_selector_all("xpath=.//tr[position()>1 and td]") or [])
        if not rows:
            continue

        if not idx_ref:
            log_info("Cabecalho de referencia nao encontrado; usando fallback por conteudo da linha.")

        for i, ln in enumerate(rows, start=1):
            if idx_ref:
                td_ref = ln.query_selector(f"xpath=./td[{idx_ref}]")
                if not td_ref:
                    continue
                if not _ref_matches_target(td_ref.inner_text(), target_m, target_y):
                    continue
            else:
                skip = set(c for c in (idx_sem, idx_enc) if c)
                if not _row_has_target_month(ln, skip):
                    continue

            ref1 = (ln.query_selector("xpath=./td[1]").inner_text().strip()
                    if ln.query_selector("xpath=./td[1]") else alvo_label)

            sem_link = None
            if idx_sem:
                cel_sem = ln.query_selector(f"xpath=./td[{idx_sem}]")
                if cel_sem:
                    sufixo = f"{i:04d}"
                    sem_link = _find_sem_mov_in_cell(cel_sem, sufixo)

            cel = ln.query_selector(f"xpath=./td[{idx_enc}]")
            if not cel:
                continue

            sufixo = f"{i:04d}"
            link = _find_enc_action_in_cell(cel, sufixo)

            link = _ignore_estorno(link, "encerrar_mov_mensal:enc")
            sem_link = _ignore_estorno(sem_link, "encerrar_mov_mensal:sem_mov")

            if (not _skip_enc) and _is_active_action(link):
                with suppress(Exception):
                    link.scroll_into_view_if_needed(timeout=600)
                log_info(f"Acao pendente em '{ref1}' - clicando Enc...")
                if _safe_click_with_fallback(page, link, contexto_log="encerrar_mov_mensal:enc"):
                    log_info("Encerramento executado.")
                    if not _confirm_icon_cleared(page, link, X_BTN_MOV_PESQUISAR, "encerrar_mov_mensal:enc"):
                        log_warning("Encerrar ainda visivel apos clique; seguindo para Sem Mov.")
                    return encerrar_mov_mensal(
                        page,
                        tabela_xpath=tabela_xpath,
                        tabela_xpaths=tabela_xpaths,
                        mes_alvo=mes_alvo,
                        ano_alvo=ano_alvo,
                        _sem_retry=_sem_retry,
                        _skip_sem=_skip_sem,
                        _skip_enc=True,
                    )
                log_error("Falha ao clicar no icone Enc./Ret.")
                return False

            if (not _skip_sem) and _is_active_action(sem_link):
                with suppress(Exception):
                    sem_link.scroll_into_view_if_needed(timeout=600)
                log_info(f"Sem Mov. pendente em '{ref1}' - clicando...")
                if not _safe_click_with_fallback(page, sem_link, contexto_log="encerrar_mov_mensal:sem_mov"):
                    log_error("Falha ao clicar no icone 'Sem Mov.'")
                    return False
                if not _confirm_icon_cleared(page, sem_link, X_BTN_MOV_PESQUISAR, "encerrar_mov_mensal:sem_mov"):
                    log_warning("Sem Mov. ainda visivel apos clique; seguindo.")
                if _sem_retry < 1:
                    return encerrar_mov_mensal(
                        page,
                        tabela_xpath=tabela_xpath,
                        tabela_xpaths=tabela_xpaths,
                        mes_alvo=mes_alvo,
                        ano_alvo=ano_alvo,
                        _sem_retry=_sem_retry + 1,
                        _skip_sem=True,
                        _skip_enc=True,
                    )
                log_warning("Sem Mov. ainda pendente; seguindo.")

            log_info("Linha alvo encontrada sem icone ativo em Enc./Ret. Considerando encerrado.")
            return True

    log_info("Nenhuma linha com a referencia (mmm/aaaa) alvo encontrada.")
    return False


def abrir_movimentacao_ret_mensal(page: Page) -> None:
    target = _build_movimentacoes_ret_url(page)
    if "hwmmovmenret" in page.url.lower():
        _apply_period_to_movimentacao(page)
        if _click_pesquisar_movimentacao(page, X_BTN_MOV_RET_PESQUISAR):
            return
    if _try_open_movimentacao_by_url(page, target, X_BTN_MOV_RET_PESQUISAR):
        return

    try:
        page.hover(X_MENU_MOVIMENTACOES, timeout=8_000)
        page.click(X_SUBMENU_MOV_MENSAL, timeout=8_000)
        page.wait_for_url("**/hwmmovmenret*", timeout=15_000)
    except Exception as e:
        raise RuntimeError(
            "Falha ao abrir a tela 'Movimentacao M' "
            f"via URL '{target}' ou via menu. Erro: {e}"
        )

    _apply_period_to_movimentacao(page)
    if not _click_pesquisar_movimentacao(page, X_BTN_MOV_RET_PESQUISAR):
        log_error("Nao foi possivel localizar o botao Pesquisar.")
        raise RuntimeError("Botao Pesquisar nao encontrado")


def abrir_movimentacao_ret_mensal_nacional(page: Page) -> None:
    if "hwmmovmenretnn" in page.url.lower():
        _apply_period_to_movimentacao(page)
        if _click_pesquisar_movimentacao(page, X_BTN_MOV_RET_PESQUISAR):
            return

    target = _build_movimentacoes_ret_nacional_url(page)
    if _try_open_movimentacao_by_url(page, target, X_BTN_MOV_RET_PESQUISAR):
        return

    try:
        page.hover(X_MENU_MOVIMENTACOES, timeout=8_000)
        page.click(X_SUBMENU_MOV_MENSAL_RET_NACIONAL, timeout=8_000)
        page.wait_for_url("**/hwmmovmenretnn*", timeout=15_000)
    except Exception as e:
        raise RuntimeError(
            "Falha ao abrir a tela 'Movimentacao M - Ret. Nota Nacional' "
            f"via URL '{target}' ou via menu. Erro: {e}"
        )

    _apply_period_to_movimentacao(page)
    if not _click_pesquisar_movimentacao(page, X_BTN_MOV_RET_PESQUISAR):
        log_error("Nao foi possivel localizar o botao Pesquisar.")
        raise RuntimeError("Botao Pesquisar nao encontrado")


def encerrar_mov_ret_mensal(
    page: Page,
    tabela_xpath: str = "//div[@id='GridContainerDiv']//table",
    tabela_xpaths: Optional[List[str]] = None,
    mes_alvo: Optional[int] = None,
    timeout: float = 30.0,
    avancar_se_sem_icone: bool = True,
    strict_cols: bool = False,
    _sem_retry: int = 0,
    _skip_sem: bool = False,
    _skip_enc: bool = False,
) -> bool:
    """
    MOVIMENTACAO MENSAL DE RETENCAO
    - Encontra a linha do mes (coluna 'Mes') == mmm (ex.: 'ago')
    - Clica antes em 'Sem Mov.' (id vDECLARAR_000i, <a> ou <img> ativo), se existir
    - Em seguida clica em 'Encerra/Retifica' (id vENCERRARETIFICA_000i, <a> ou <img> ativo)
    """

    def _alvo_mmm() -> str:
        if mes_alvo:
            return PT_ABBR[int(mes_alvo)].lower()
        with suppress(Exception):
            from utils.periodo import get_periodo
            per = get_periodo() or {}
            m = int(per.get("mes_de") or per.get("mes") or 0)
            if 1 <= m <= 12:
                return PT_ABBR[m].lower()
        return PT_ABBR[datetime.now().month].lower()

    def _norm_ref_mmm(s: str) -> str:
        s = (s or "").strip().lower()
        s = re.sub(r"[^a-zÃ¡-Ãº]", "", s)
        return s[:3] if len(s) >= 3 else s

    def _get_table(fr, tries: int = 5, pause: float = 0.3):
        t = None
        ext = globals().get("_find_table")
        for _ in range(tries):
            if not t and callable(ext) and tabela_xpaths:
                with suppress(Exception):
                    t = ext(fr, tabela_xpaths)
            if not t:
                t = fr.query_selector(f"xpath={tabela_xpath}")
            if t:
                break
            time.sleep(pause)
        return t

    def _find_header_index(tbl, targets: List[str]) -> Optional[int]:
        for r in (1, 2, 3):
            cells = (tbl.query_selector_all(f"xpath=.//tr[{r}]/th") or
                     tbl.query_selector_all(f"xpath=.//tr[{r}]/td") or [])
            for i, th in enumerate(cells, start=1):
                txt = normalize_text(th.inner_text() or "").replace("\xa0", " ")
                if _header_matches(txt, targets):
                    return i
        return None

    def _row_has_target_month(ln, skip_cols: Optional[set]) -> bool:
        try:
            cells = ln.query_selector_all("xpath=./td") or []
        except Exception:
            cells = []
        for j, td in enumerate(cells, start=1):
            if skip_cols and j in skip_cols:
                continue
            try:
                txt = td.inner_text() or ""
            except Exception:
                txt = ""
            if _ref_matches_target(txt, target_m, target_y):
                return True
        return False

    def _guess_action_col(rows, kind: str) -> Optional[int]:
        for i, ln in enumerate(rows, start=1):
            try:
                cells = ln.query_selector_all("xpath=./td") or []
            except Exception:
                cells = []
            if not cells:
                continue
            sufixo = f"{i:04d}"
            for j, cel in enumerate(cells, start=1):
                link = None
                if kind == "enc":
                    link = _find_enc_action_in_cell(cel, sufixo)
                else:
                    link = _find_sem_mov_in_cell(cel, sufixo)
                if _is_visible_el(link):
                    return j
        return None

    def _row_has_target_month(ln, skip_cols: Optional[set]) -> bool:
        try:
            cells = ln.query_selector_all("xpath=./td") or []
        except Exception:
            cells = []
        for j, td in enumerate(cells, start=1):
            if skip_cols and j in skip_cols:
                continue
            try:
                txt = td.inner_text() or ""
            except Exception:
                txt = ""
            if _ref_matches_target(txt, target_m, target_y):
                return True
        return False

    def _row_has_target_month(ln, skip_cols: Optional[set]) -> bool:
        try:
            cells = ln.query_selector_all("xpath=./td") or []
        except Exception:
            cells = []
        for j, td in enumerate(cells, start=1):
            if skip_cols and j in skip_cols:
                continue
            try:
                txt = td.inner_text() or ""
            except Exception:
                txt = ""
            if _ref_matches_target(txt, target_m, target_y):
                return True
        return False

    IMG_ATIVO_XP = ("xpath=.//img[not(contains(translate(@src,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'cinza')) "
                    "and not(contains(translate(@src,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'gray'))]")

    def _click_forte(fr, el) -> bool:
        with suppress(Exception):
            el.scroll_into_view_if_needed(timeout=1000)

        auto_dialog = bool(getattr(page, "_nm_auto_dialog", False))
        if not auto_dialog:
            with suppress(Exception):
                with page.expect_event("dialog", timeout=800) as d:
                    el.click(timeout=1000)
                d.value.accept()
                return True

        with suppress(Exception):
            el.click(timeout=1200)
            return True
        with suppress(Exception):
            el.click(timeout=1200, force=True)
            return True
        with suppress(Exception):
            fr.evaluate("(e)=>e && e.click()", el)
            return True
        with suppress(Exception):
            box = el.bounding_box()
            if box:
                x = box["x"] + box["width"] / 2
                y = box["y"] + box["height"] / 2
                page.mouse.move(x, y)
                page.mouse.click(x, y)
                return True
        with suppress(Exception):
            el.focus()
            page.keyboard.press("Enter")
            return True
        with suppress(Exception):
            el.focus()
            page.keyboard.press("Space")
            return True
        return False

    _set_movimentacao_ano_dropdown(page, None, ["//select[@id='vANOSEMMOV']"])
    _click_pesquisar_movimentacao(page, X_BTN_MOV_RET_PESQUISAR)
    target_m, target_y = _get_target_month_year(mes_alvo, None)
    alvo_label = PT_ABBR[target_m]
    log_info(f"Enc.(RET): procurando linha do mes '{alvo_label}'...")

    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
    deadline = time.time() + float(timeout)

    for fr in frames:
        tbl = None
        while time.time() < deadline and not tbl:
            tbl = _get_table(fr)
            if not tbl:
                time.sleep(0.4)
        if not tbl:
            continue

        idx_mes = (_find_header_index(tbl, MES_HEADER_TOKENS) or
                   _find_header_index(tbl, REF_HEADER_TOKENS))
        idx_sem = _find_header_index(tbl, SEM_HEADER_TOKENS)
        idx_enc = _find_header_index(tbl, ENC_HEADER_TOKENS)

        if not idx_enc:
            log_info(f"Cabecalhos ausentes (Mes={idx_mes} Sem={idx_sem} Enc={idx_enc}).")
            continue

        rows = (tbl.query_selector_all("xpath=.//tbody/tr[td]") or
                tbl.query_selector_all("xpath=.//tr[position()>1 and td]") or [])
        if not rows:
            continue

        if not strict_cols:
            if not idx_enc:
                idx_enc = _guess_action_col(rows, "enc")
                if idx_enc:
                    log_info(f"Coluna Enc./Ret. inferida pela linha de dados: {idx_enc}")
            if not idx_sem:
                idx_sem = _guess_action_col(rows, "sem")
                if idx_sem:
                    log_info(f"Coluna Sem Mov. inferida pela linha de dados: {idx_sem}")

        if not idx_mes:
            log_info("Cabecalho do mes nao encontrado; usando fallback por conteudo da linha.")

        for i, ln in enumerate(rows, start=1):
            if idx_mes:
                td_mes = ln.query_selector(f"xpath=./td[{idx_mes}]")
                if not td_mes:
                    continue
                if not _ref_matches_target(td_mes.inner_text(), target_m, target_y):
                    continue
            else:
                skip = set(c for c in (idx_sem, idx_enc) if c)
                if not _row_has_target_month(ln, skip):
                    continue

            ref1 = (ln.query_selector("xpath=./td[1]").inner_text().strip()
                    if ln.query_selector("xpath=./td[1]") else alvo_label)
            sufixo = f"{i:04d}"

            sem_link = None
            if idx_sem:
                cel_sem = ln.query_selector(f"xpath=./td[{idx_sem}]")
                if cel_sem:
                    sem_link = _find_sem_mov_in_cell(cel_sem, sufixo)
                    if (not strict_cols) and not _is_visible_el(sem_link):
                        sem_link = _find_sem_mov_in_row(ln, sufixo)

            cel_enc = ln.query_selector(f"xpath=./td[{idx_enc}]")
            if not cel_enc:
                continue

            link = _find_enc_action_in_cell(cel_enc, sufixo)
            if (not strict_cols) and not _is_visible_el(link):
                link = _find_enc_action_in_row(ln, sufixo)

            link = _ignore_estorno(link, "encerrar_mov_ret_mensal:enc")
            sem_link = _ignore_estorno(sem_link, "encerrar_mov_ret_mensal:sem_mov")

            if (not _skip_enc) and _is_active_action(link):
                log_info(f"Acao pendente em '{ref1}' - clicando Enc./Ret...")
                if _click_forte(fr, link):
                    with suppress(Exception):
                        page.wait_for_load_state("networkidle", timeout=1500)
                    log_info("Encerramento executado (RET).")
                    if not _confirm_icon_cleared(page, link, X_BTN_MOV_RET_PESQUISAR, "encerrar_mov_ret_mensal:enc"):
                        log_warning("Encerrar (RET) ainda visivel apos clique; seguindo para Sem Mov.")
                    return encerrar_mov_ret_mensal(
                        page,
                        tabela_xpath=tabela_xpath,
                        tabela_xpaths=tabela_xpaths,
                        mes_alvo=mes_alvo,
                        timeout=timeout,
                        avancar_se_sem_icone=avancar_se_sem_icone,
                        strict_cols=strict_cols,
                        _sem_retry=_sem_retry,
                        _skip_sem=_skip_sem,
                        _skip_enc=True,
                    )
                log_error("Falha ao clicar no icone Enc./Ret. (RET).")
                return False

            if (not _skip_sem) and _is_active_action(sem_link):
                log_info(f"Sem Mov. pendente em '{ref1}' - clicando...")
                if not _click_forte(fr, sem_link):
                    log_error("Falha ao clicar no icone 'Sem Mov.' (RET).")
                    return False
                if not _confirm_icon_cleared(page, sem_link, X_BTN_MOV_RET_PESQUISAR, "encerrar_mov_ret_mensal:sem_mov"):
                    log_warning("Sem Mov. (RET) ainda visivel apos clique; seguindo.")
                if _sem_retry < 1:
                    return encerrar_mov_ret_mensal(
                        page,
                        tabela_xpath=tabela_xpath,
                        tabela_xpaths=tabela_xpaths,
                        mes_alvo=mes_alvo,
                        timeout=timeout,
                        avancar_se_sem_icone=avancar_se_sem_icone,
                        strict_cols=strict_cols,
                        _sem_retry=_sem_retry + 1,
                        _skip_sem=True,
                        _skip_enc=True,
                    )
                log_warning("Sem Mov. (RET) ainda pendente; seguindo.")

            if avancar_se_sem_icone:
                log_info("Linha alvo encontrada, mas sem icone ativo em Enc./Ret. Avancando...")
                return True
            log_info("Sem icone ativo em Enc./Ret. Retornando False.")
            return False

    log_info("Nenhuma linha com o mes (mmm) alvo encontrada.")
    return False


def encerrar_mov_ret_mensal_nacional(
    page: Page,
    tabela_xpath: str = "//div[@id='GridContainerDiv']//table",
    tabela_xpaths: Optional[List[str]] = None,
    mes_alvo: Optional[int] = None,
    timeout: float = 30.0,
    avancar_se_sem_icone: bool = True,
) -> bool:
    return encerrar_mov_ret_mensal(
        page,
        tabela_xpath=tabela_xpath,
        tabela_xpaths=tabela_xpaths,
        mes_alvo=mes_alvo,
        timeout=timeout,
        avancar_se_sem_icone=avancar_se_sem_icone,
        strict_cols=True,
    )


def abrir_movimentacao_mensal_nfse(page: Page) -> None:
    target = _build_movimentacoes_nfce_url(page)
    if "hwmovmensalnfce" in page.url.lower():
        _apply_period_to_movimentacao(page)
        if _click_pesquisar_movimentacao(page, X_BTN_MOV_PESQUISAR):
            return
    if _try_open_movimentacao_by_url(page, target, X_BTN_MOV_PESQUISAR):
        return

    try:
        page.hover(X_MENU_MOVIMENTACOES, timeout=8_000)
        page.click(X_SUBMENU_MOV_MENSAL, timeout=8_000)
        page.wait_for_url("**/hwmovmensalnfce*", timeout=15_000)
    except Exception as e:
        raise RuntimeError(
            "Falha ao abrir a tela 'Movimentacao M' "
            f"via URL '{target}' ou via menu. Erro: {e}"
        )

    _apply_period_to_movimentacao(page)
    if not _click_pesquisar_movimentacao(page, X_BTN_MOV_PESQUISAR):
        log_error("Nao foi possivel localizar o botao Pesquisar.")
        raise RuntimeError("Botao Pesquisar nao encontrado")


def abrir_movimentacao_mensal_nacional(page: Page) -> None:
    target = _build_movimentacoes_nacional_url(page)
    if "hwmovmensalnn" in page.url.lower():
        _apply_period_to_movimentacao(page)
        if _click_pesquisar_movimentacao(page, X_BTN_MOV_PESQUISAR):
            return
    if _try_open_movimentacao_by_url(page, target, X_BTN_MOV_PESQUISAR):
        return

    try:
        page.hover(X_MENU_MOVIMENTACOES, timeout=8_000)
        page.click(X_SUBMENU_MOV_MENSAL_NACIONAL, timeout=8_000)
        page.wait_for_url("**/hwmovmensalnn*", timeout=15_000)
    except Exception as e:
        raise RuntimeError(
            "Falha ao abrir a tela 'Movimentacao M - Nota Nacional' "
            f"via URL '{target}' ou via menu. Erro: {e}"
        )

    _apply_period_to_movimentacao(page)
    if not _click_pesquisar_movimentacao(page, X_BTN_MOV_PESQUISAR):
        log_error("Nao foi possivel localizar o botao Pesquisar.")
        raise RuntimeError("Botao Pesquisar nao encontrado")


def enncerrar_mov_mensal_nacioal(
    page: Page,
    tabela_xpath: str = "//div[@id='GridContainerDiv']//table",
    tabela_xpaths: Optional[List[str]] = None,
    mes_alvo: Optional[int] = None,
    ano_alvo: Optional[int] = None,
    _sem_retry: int = 0,
    _skip_sem: bool = False,
    _skip_enc: bool = False,
) -> bool:
    def _get_table(fr, tries: int = 5, pause: float = 0.3):
        t = None
        ext = globals().get("_find_table")
        for _ in range(tries):
            if not t and callable(ext) and tabela_xpaths:
                with suppress(Exception):
                    t = ext(fr, tabela_xpaths)
            if not t:
                t = fr.query_selector(f"xpath={tabela_xpath}")
            if t:
                break
            time.sleep(pause)
        return t

    def _find_header_index(tbl, targets: List[str]) -> Optional[int]:
        for r in (1, 2, 3):
            cells = (tbl.query_selector_all(f"xpath=.//tr[{r}]/th") or
                     tbl.query_selector_all(f"xpath=.//tr[{r}]/td") or [])
            for i, th in enumerate(cells, start=1):
                txt = normalize_text(th.inner_text() or "").replace("\xa0", " ")
                if _header_matches(txt, targets):
                    return i
        return None

    def _row_has_target_month(ln, skip_cols: Optional[set]) -> bool:
        try:
            cells = ln.query_selector_all("xpath=./td") or []
        except Exception:
            cells = []
        for j, td in enumerate(cells, start=1):
            if skip_cols and j in skip_cols:
                continue
            try:
                txt = td.inner_text() or ""
            except Exception:
                txt = ""
            if _ref_matches_target(txt, target_m, target_y):
                return True
        return False

    _set_movimentacao_ano_dropdown(
        page,
        ano_alvo,
        ["//select[@id='vANOSEMMOV1']", "//select[@id='vANOSEMMOV2']"],
    )
    for _ in range(2):
        if _force_ano_by_xpath_gx(page, ano_alvo, ["//*[@id='vANOSEMMOV1']", "//*[@id='vANOSEMMOV2']"]):
            break
        time.sleep(0.3)
    _click_pesquisar_movimentacao(page, X_BTN_MOV_PESQUISAR)
    _set_movimentacao_ano_dropdown(
        page,
        ano_alvo,
        ["//select[@id='vANOSEMMOV1']", "//select[@id='vANOSEMMOV2']"],
    )
    _click_pesquisar_movimentacao(page, X_BTN_MOV_PESQUISAR)
    ok_ano = _force_ano_by_xpath_gx(
        page,
        ano_alvo,
        ["//*[@id='vANOSEMMOV1']", "//*[@id='vANOSEMMOV2']"],
    )
    if not ok_ano:
        log_info("Ano nao selecionado via XPath; seguindo mesmo assim.")
    _click_pesquisar_movimentacao(page, X_BTN_MOV_PESQUISAR)
    ok_ano_fim = _force_ano_by_xpath_gx(
        page,
        ano_alvo,
        ["//*[@id='vANOSEMMOV1']", "//*[@id='vANOSEMMOV2']"],
    )
    if not ok_ano_fim:
        log_info("Ano nao selecionado via XPath; seguindo mesmo assim.")
    _click_pesquisar_movimentacao(page, X_BTN_MOV_PESQUISAR)
    target_m, target_y = _get_target_month_year(mes_alvo, ano_alvo)
    alvo_label = f"{PT_ABBR[target_m]}/{target_y}"
    log_info(f"Enc. (Nota Nacional): procurando linha com Referencia '{alvo_label}'...")

    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
    for fr in frames:
        log_info("Verificando frame principal..." if fr is page.main_frame else f"Verificando iframe '{fr.url or ''}'...")
        tbl = _get_table(fr)
        if not tbl:
            log_info("tabela nao encontrada aqui.")
            continue

        idx_ref = _find_header_index(tbl, REF_HEADER_TOKENS)
        idx_sem = _find_header_index(tbl, SEM_HEADER_TOKENS)
        idx_enc = _find_header_index(tbl, ENC_HEADER_TOKENS)
        if not idx_enc and not idx_sem:
            log_info(f"Cabecalhos ausentes (Ref={idx_ref} Sem={idx_sem} Enc={idx_enc}).")
            continue
        if not idx_enc:
            log_warning(f"Coluna Enc./Ret. ausente (Ref={idx_ref} Sem={idx_sem}). Verificando Sem Mov.")

        rows = (tbl.query_selector_all("xpath=.//tbody/tr[td]") or
                tbl.query_selector_all("xpath=.//tr[position()>1 and td]") or [])
        if not rows:
            continue

        if not idx_ref:
            log_info("Cabecalho de referencia nao encontrado; usando fallback por conteudo da linha.")

        for i, ln in enumerate(rows, start=1):
            if idx_ref:
                td_ref = ln.query_selector(f"xpath=./td[{idx_ref}]")
                if not td_ref:
                    continue
                if not _ref_matches_target(td_ref.inner_text(), target_m, target_y):
                    continue
            else:
                skip = set(c for c in (idx_sem, idx_enc) if c)
                if not _row_has_target_month(ln, skip):
                    continue

            ref1 = (ln.query_selector("xpath=./td[1]").inner_text().strip()
                    if ln.query_selector("xpath=./td[1]") else alvo_label)

            sem_link = None
            if idx_sem:
                cel_sem = ln.query_selector(f"xpath=./td[{idx_sem}]")
                if cel_sem:
                    sufixo = f"{i:04d}"
                    sem_link = _find_sem_mov_in_cell(cel_sem, sufixo)

            cel = ln.query_selector(f"xpath=./td[{idx_enc}]")
            if not cel:
                continue

            sufixo = f"{i:04d}"
            link = _find_enc_action_in_cell(cel, sufixo)
            link = _ignore_estorno(link, "enncerrar_mov_mensal_nacioal:enc")
            sem_link = _ignore_estorno(sem_link, "enncerrar_mov_mensal_nacioal:sem_mov")

            if (not _skip_enc) and _is_active_action(link):
                with suppress(Exception):
                    link.scroll_into_view_if_needed(timeout=600)
                log_info(f"Acao pendente em '{ref1}' - clicando Enc...")
                if _safe_click_with_fallback(page, link, contexto_log="enncerrar_mov_mensal_nacioal:enc"):
                    log_info("Encerramento executado (Nota Nacional).")
                    if not _confirm_icon_cleared(page, link, X_BTN_MOV_PESQUISAR, "enncerrar_mov_mensal_nacioal:enc"):
                        log_warning("Encerrar (Nota Nacional) ainda visivel apos clique; seguindo para Sem Mov.")
                    return enncerrar_mov_mensal_nacioal(
                        page,
                        tabela_xpath=tabela_xpath,
                        tabela_xpaths=tabela_xpaths,
                        mes_alvo=mes_alvo,
                        ano_alvo=ano_alvo,
                        _sem_retry=_sem_retry,
                        _skip_sem=_skip_sem,
                        _skip_enc=True,
                    )
                log_error("Falha ao clicar no icone Enc./Ret. (Nota Nacional).")
                return False

            if (not _skip_sem) and _is_active_action(sem_link):
                with suppress(Exception):
                    sem_link.scroll_into_view_if_needed(timeout=600)
                log_info(f"Sem Mov. pendente em '{ref1}' - clicando...")
                if not _safe_click_with_fallback(page, sem_link, contexto_log="enncerrar_mov_mensal_nacioal:sem_mov"):
                    log_error("Falha ao clicar no icone 'Sem Mov.' (Nota Nacional).")
                    return False
                if not _confirm_icon_cleared(page, sem_link, X_BTN_MOV_PESQUISAR, "enncerrar_mov_mensal_nacioal:sem_mov"):
                    log_warning("Sem Mov. (Nota Nacional) ainda visivel apos clique; seguindo.")
                if _sem_retry < 1:
                    return enncerrar_mov_mensal_nacioal(
                        page,
                        tabela_xpath=tabela_xpath,
                        tabela_xpaths=tabela_xpaths,
                        mes_alvo=mes_alvo,
                        ano_alvo=ano_alvo,
                        _sem_retry=_sem_retry + 1,
                        _skip_sem=True,
                        _skip_enc=True,
                    )
                log_warning("Sem Mov. (Nota Nacional) ainda pendente; seguindo.")

            log_info("Linha alvo encontrada sem icone ativo em Enc./Ret. Considerando encerrado (Nota Nacional).")
            return True

    log_info("Nenhuma linha com a referencia (mmm/aaaa) alvo encontrada.")
    return False


def encerrar_mov_nfse_mensal(
    page: Page,
    tabela_xpath: str = "//div[@id='GridContainerDiv']//table",
    tabela_xpaths: Optional[List[str]] = None,
    mes_alvo: Optional[int] = None,
    ano_alvo: Optional[int] = None,
    _sem_retry: int = 0,
    _skip_sem: bool = False,
    _skip_enc: bool = False,
) -> bool:
    def _alvo_ref() -> str:
        if mes_alvo and ano_alvo:
            return f"{PT_ABBR[int(mes_alvo)]}/{int(ano_alvo)}".lower()
        with suppress(Exception):
            from utils.periodo import get_periodo
            per = get_periodo() or {}
            m = int(per.get("mes_de") or per.get("mes") or 0)
            a = int(per.get("ano_de") or per.get("ano") or 0)
            if 1 <= m <= 12 and a:
                return f"{PT_ABBR[m]}/{a}".lower()
        now = datetime.now()
        return f"{PT_ABBR[now.month]}/{now.year}".lower()

    def _norm_ref_mmm_aaaa(s: str) -> str:
        s = (s or "").strip()
        s = re.sub(r"\s*/\s*", "/", s)
        return s.lower()

    def _get_table(fr, tries: int = 5, pause: float = 0.3):
        t = None
        ext = globals().get("_find_table")
        for _ in range(tries):
            if not t and callable(ext) and tabela_xpaths:
                with suppress(Exception):
                    t = ext(fr, tabela_xpaths)
            if not t:
                t = fr.query_selector(f"xpath={tabela_xpath}")
            if t:
                break
            time.sleep(pause)
        return t

    def _find_header_index(tbl, targets: List[str]) -> Optional[int]:
        for r in (1, 2, 3):
            cells = (tbl.query_selector_all(f"xpath=.//tr[{r}]/th") or
                     tbl.query_selector_all(f"xpath=.//tr[{r}]/td") or [])
            for i, th in enumerate(cells, start=1):
                txt = normalize_text(th.inner_text() or "").replace("\xa0", " ")
                if _header_matches(txt, targets):
                    return i
        return None

    target_m, target_y = _get_target_month_year(mes_alvo, ano_alvo)
    ok_ano_nfce = _force_ano_by_xpath_gx(
        page,
        ano_alvo,
        ["//*[@id='vANOSEMMOV1']", "//*[@id='vANOSEMMOV2']"],
    )
    if not ok_ano_nfce:
        log_info("Ano NFCE nao selecionado via XPath; seguindo mesmo assim.")
    _click_pesquisar_movimentacao(page, X_BTN_MOV_PESQUISAR)
    alvo_label = f"{PT_ABBR[target_m]}/{target_y}"
    log_info(f"Enc. (NFSe): procurando linha com Referencia '{alvo_label}'...")

    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
    for fr in frames:
        log_info("Verificando frame principal..." if fr is page.main_frame else f"Verificando iframe '{fr.url or ''}'...")
        tbl = _get_table(fr)
        if not tbl:
            log_info("tabela nao encontrada aqui.")
            continue

        idx_ref = _find_header_index(tbl, REF_HEADER_TOKENS)
        idx_sem = _find_header_index(tbl, SEM_HEADER_TOKENS)
        idx_enc = _find_header_index(tbl, ENC_HEADER_TOKENS)
        if not idx_enc and not idx_sem:
            log_info(f"Cabecalhos ausentes (Ref={idx_ref} Sem={idx_sem} Enc={idx_enc}).")
            continue
        if not idx_enc:
            log_warning(f"Coluna Enc./Ret. ausente (Ref={idx_ref} Sem={idx_sem}). Verificando Sem Mov.")

        rows = (tbl.query_selector_all("xpath=.//tbody/tr[td]") or
                tbl.query_selector_all("xpath=.//tr[position()>1 and td]") or [])
        if not rows:
            continue

        if not idx_ref:
            log_info("Cabecalho de referencia nao encontrado; usando fallback por conteudo da linha.")

        for i, ln in enumerate(rows, start=1):
            if idx_ref:
                td_ref = ln.query_selector(f"xpath=./td[{idx_ref}]")
                if not td_ref:
                    continue
                if not _ref_matches_target(td_ref.inner_text(), target_m, target_y):
                    continue
            else:
                try:
                    cells = ln.query_selector_all("xpath=./td") or []
                except Exception:
                    cells = []
                skip = set(c for c in (idx_sem, idx_enc) if c)
                found = False
                for j, td in enumerate(cells, start=1):
                    if skip and j in skip:
                        continue
                    try:
                        txt = td.inner_text() or ""
                    except Exception:
                        txt = ""
                    if _ref_matches_target(txt, target_m, target_y):
                        found = True
                        break
                if not found:
                    continue

            ref1 = (ln.query_selector("xpath=./td[1]").inner_text().strip()
                    if ln.query_selector("xpath=./td[1]") else alvo_label)

            sem_link = None
            if idx_sem:
                cel_sem = ln.query_selector(f"xpath=./td[{idx_sem}]")
                if cel_sem:
                    sufixo = f"{i:04d}"
                    sem_link = _find_sem_mov_in_cell(cel_sem, sufixo)
                    if not _is_visible_el(sem_link):
                        sem_link = _find_sem_mov_in_row(ln, sufixo)

            cel = ln.query_selector(f"xpath=./td[{idx_enc}]")
            if not cel:
                continue

            sufixo = f"{i:04d}"
            link = _find_enc_action_in_cell(cel, sufixo)
            if not _is_visible_el(link):
                link = _find_enc_action_in_row(ln, sufixo)

            link = _ignore_estorno(link, "encerrar_mov_nfse_mensal:enc")
            sem_link = _ignore_estorno(sem_link, "encerrar_mov_nfse_mensal:sem_mov")

            if (not _skip_enc) and _is_active_action(link):
                with suppress(Exception):
                    link.scroll_into_view_if_needed(timeout=600)
                log_info(f"Acao pendente em '{ref1}' - clicando Enc...")
                if _safe_click_with_fallback(page, link, contexto_log="encerrar_mov_nfse_mensal:enc"):
                    log_info("Encerramento executado (NFSe).")
                    if not _confirm_icon_cleared(page, link, X_BTN_MOV_PESQUISAR, "encerrar_mov_nfse_mensal:enc"):
                        log_warning("Encerrar (NFSe) ainda visivel apos clique; seguindo para Sem Mov.")
                    return encerrar_mov_nfse_mensal(
                        page,
                        tabela_xpath=tabela_xpath,
                        tabela_xpaths=tabela_xpaths,
                        mes_alvo=mes_alvo,
                        ano_alvo=ano_alvo,
                        _sem_retry=_sem_retry,
                        _skip_sem=_skip_sem,
                        _skip_enc=True,
                    )
                log_error("Falha ao clicar no icone Enc./Ret. (NFSe).")
                return False

            if (not _skip_sem) and _is_active_action(sem_link):
                with suppress(Exception):
                    sem_link.scroll_into_view_if_needed(timeout=600)
                log_info(f"Sem Mov. pendente em '{ref1}' - clicando...")
                if not _safe_click_with_fallback(page, sem_link, contexto_log="encerrar_mov_nfse_mensal:sem_mov"):
                    log_error("Falha ao clicar no icone 'Sem Mov.' (NFSe).")
                    return False
                if not _confirm_icon_cleared(page, sem_link, X_BTN_MOV_PESQUISAR, "encerrar_mov_nfse_mensal:sem_mov"):
                    log_warning("Sem Mov. (NFSe) ainda visivel apos clique; seguindo.")
                if _sem_retry < 1:
                    return encerrar_mov_nfse_mensal(
                        page,
                        tabela_xpath=tabela_xpath,
                        tabela_xpaths=tabela_xpaths,
                        mes_alvo=mes_alvo,
                        ano_alvo=ano_alvo,
                        _sem_retry=_sem_retry + 1,
                        _skip_sem=True,
                        _skip_enc=True,
                    )
                log_warning("Sem Mov. (NFSe) ainda pendente; seguindo.")

            log_info("Linha alvo encontrada sem icone ativo em Enc./Ret. Considerando encerrado (NFSe).")
            return True

    log_info("Nenhuma linha com a referencia (mmm/aaaa) alvo encontrada.")
    return False


def _click_with_dialog_or_modal(
    page: Page,
    clickable,
    wait_url_change: bool = True,
    url_timeout: int = 15000,
) -> None:
    """
    Clica em 'clickable' tratando:
      1) dialogo JS (alert/confirm/prompt) - aceita automaticamente
      2) modal HTML ('OK'/'Confirmar'/'Sim') - clica automaticamente
    Depois, espera 'networkidle' e (opcionalmente) mudanca de URL.
    """
    accepted = {"dialog": False, "html": False}
    auto_dialog = bool(getattr(page, "_nm_auto_dialog", False))

    def _on_dialog(dlg):
        try:
            log_info("dialogo JS: aceitando...")
            dlg.accept()
            accepted["dialog"] = True
        except Exception as e:
            log_error(f"Falha ao aceitar dialogo: {e}")

    if not auto_dialog:
        page.on("dialog", _on_dialog)
    old_url = page.url

    try:
        if not auto_dialog:
            try:
                with page.expect_event("dialog", timeout=2500) as ev:
                    clickable.click(timeout=6000)
                try:
                    ev.value.accept()
                    accepted["dialog"] = True
                except Exception:
                    pass
            except PWTimeout:
                try:
                    clickable.click(timeout=6000)
                except Exception:
                    try:
                        page.evaluate("(el) => el.click()", clickable)
                    except Exception:
                        pass
        else:
            try:
                clickable.click(timeout=6000)
            except Exception:
                try:
                    page.evaluate("(el) => el.click()", clickable)
                except Exception:
                    pass

        if _wait_and_click_modal_ok(page, timeout_ms=8000):
            accepted["html"] = True
        else:
            time.sleep(0.4)
            accepted["html"] = _wait_and_click_modal_ok(page, timeout_ms=4000)

        try:
            page.wait_for_load_state("networkidle", timeout=12000)
        except PWTimeout:
            try:
                page.wait_for_load_state("domcontentloaded", timeout=6000)
            except PWTimeout:
                pass

        if wait_url_change:
            try:
                page.wait_for_function("u => location.href !== u", arg=old_url, timeout=url_timeout)
            except Exception:
                pass

        if not (accepted["dialog"] or accepted["html"]):
            _wait_and_click_modal_ok(page, timeout_ms=2000)

    finally:
        try:
            page.off("dialog", _on_dialog)
        except Exception:
            try:
                page.remove_listener("dialog", _on_dialog)
            except Exception:
                pass


def _safe_click_with_fallback(page: Page, link, contexto_log: str = "") -> bool:
    el_id = None
    with suppress(Exception):
        el_id = link.get_attribute("id")
    def _click_by_id() -> bool:
        if not el_id:
            return False
        frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
        for fr in frames:
            try:
                el = fr.query_selector(f"xpath=.//*[@id='{el_id}']")
            except Exception:
                el = None
            if not el:
                continue
            with suppress(Exception):
                el.scroll_into_view_if_needed(timeout=600)
            with suppress(Exception):
                el.click(timeout=1500, force=True)
                return True
            with suppress(Exception):
                el.evaluate("el => el.click()")
                return True
        return False
    try:
        handle_dialog_and_modal(page, link)
        return True
    except Exception as e:
        log_info(f"(aviso) handler falhou em {contexto_log}: {e!r} - fallback")
        if _click_by_id():
            return True

    auto_dialog = bool(getattr(page, "_nm_auto_dialog", False))
    if not auto_dialog:
        try:
            with _expect_dialog_event(page, timeout=800) as dlg:
                with suppress(Exception):
                    link.scroll_into_view_if_needed(timeout=600)
                link.click(timeout=1500)
            with suppress(Exception):
                _safe_accept_dialog(dlg.value, "Dialog JS - OK.")
            return True
        except PWTimeout:
            pass
        except Exception as e:
            log_info(f"(aviso) expect_dialog falhou em {contexto_log}: {e!r}")

    with suppress(Exception):
        link.click(timeout=1500)
        return True
    with suppress(Exception):
        link.click(timeout=1200, force=True)
        return True
    with suppress(Exception):
        link.evaluate("el => el.click()")
        return True
    if _click_by_id():
        return True
    with suppress(Exception):
        td = link.locator("xpath=ancestor::td[1]").first
        if td and td.count():
            td.click(timeout=1200, force=True)
            return True
    return False














def abrir_escrituracao_contabilidade(page: Page) -> bool:
    target = _build_contabilidade_url(page)
    try:
        page.goto(target, wait_until="networkidle", timeout=15_000)
    except Exception:
        pass

    if "hwmcontabilidade" not in page.url.lower():
        log_info("Tentando abrir Exporta Notas Contabilidade pelo menu...")
        try:
            page.hover(X_MENU_MOVIMENTACOES, timeout=8_000)
            page.click(
                "xpath=//a[contains(@href,'hwmcontabilidade') or contains(., 'Contabilidade')]",
                timeout=8_000,
            )
            page.wait_for_url("**/hwmcontabilidade*", timeout=15_000)
        except Exception as e:
            log_error(f"Falha ao abrir a tela: {e}")
            return False

    p = get_periodo_dict()
    md, yd = int(p["mes_de"]), int(p["ano_de"])
    ma, ya = int(p["mes_ate"]), int(p["ano_ate"])

    primeiro_dia = f"01/{md:02d}/{yd}"
    ultimo_dia_mes = calendar.monthrange(ya, ma)[1]
    ultimo_dia = f"{ultimo_dia_mes:02d}/{ma:02d}/{ya}"

    log_info(f"Periodo GUI -> Browser: Data Inicio={primeiro_dia} | Data Fim={ultimo_dia}")

    def _find_date_inputs_in_frame(fr):
        U = "ABCDEFGHIJKLMNOPQRSTUVWXYZÃÃ€Ã‚ÃƒÃ‰ÃˆÃŠÃÃŒÃŽÃ“Ã’Ã”Ã•ÃšÃ™Ã›Ã‡"
        L = "abcdefghijklmnopqrstuvwxyzÃ¡Ã Ã¢Ã£Ã©Ã¨ÃªÃ­Ã¬Ã®Ã³Ã²Ã´ÃµÃºÃ¹Ã»Ã§"

        def _fmt(xp):
            return xp.replace("$U", f"'{U}'").replace("$L", f"'{L}'")

        def _first_visible(sel_list):
            for s in sel_list:
                try:
                    loc = fr.wait_for_selector(s, timeout=1500)
                    if loc and loc.is_visible():
                        return loc
                except Exception:
                    continue
            return None

        ini_sels = [
            "xpath=//label[contains(translate(normalize-space(.),$U,$L),'data inicio')]/following::input[1]",
            "xpath=//input[contains(translate(@id,$U,$L),'dtini') or contains(translate(@name,$U,$L),'dtini')]",
            "xpath=//input[contains(translate(@id,$U,$L),'dataini') or contains(translate(@name,$U,$L),'dataini')]",
            "xpath=//input[contains(translate(@aria-label,$U,$L),'data inicio')]",
        ]
        fim_sels = [
            "xpath=//label[contains(translate(normalize-space(.),$U,$L),'data fim')]/following::input[1]",
            "xpath=//input[contains(translate(@id,$U,$L),'dtfim') or contains(translate(@name,$U,$L),'dtfim')]",
            "xpath=//input[contains(translate(@id,$U,$L),'datafim') or contains(translate(@name,$U,$L),'datafim')]",
            "xpath=//input[contains(translate(@aria-label,$U,$L),'data fim')]",
        ]

        with suppress(Exception):
            fr.wait_for_load_state("domcontentloaded", timeout=5000)

        ini = _first_visible([_fmt(s) for s in ini_sels])
        fim = _first_visible([_fmt(s) for s in fim_sels])
        return ini, fim

    def _type_date(input_loc, value: str):
        if not input_loc:
            return False
        try:
            input_loc.click(timeout=4000)
            with suppress(Exception):
                input_loc.fill("")
            with suppress(Exception):
                input_loc.press("Control+A")
            input_loc.type(value, delay=40)
            with suppress(Exception):
                input_loc.blur()
            with suppress(Exception):
                input_loc.evaluate(
                    "(el, v) => { el.value=v; el.dispatchEvent(new Event('input',{bubbles:true})); el.dispatchEvent(new Event('change',{bubbles:true})); }",
                    value,
                )
            return True
        except Exception as e:
            log_error(f"Falha ao preencher data '{value}': {e}")
            return False

    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
    preencheu = False

    for fr in frames:
        where = "frame principal" if fr is page.main_frame else f"iframe '{fr.url or ''}'"
        log_info(f"Buscando campos de Data Inicio/Fim no {where}...")
        try:
            fr.wait_for_load_state("domcontentloaded", timeout=5_000)
        except PWTimeout:
            pass

        dt_ini, dt_fim = _find_date_inputs_in_frame(fr)
        achou = [n for n, el in (("Data Inicio", dt_ini), ("Data Fim", dt_fim)) if el]
        if not achou:
            log_info("campos nao encontrados aqui.")
            continue
        log_info(f"encontrados: {', '.join(achou)}")

        ok = True
        ok &= _type_date(dt_ini, primeiro_dia) if dt_ini else False
        ok &= _type_date(dt_fim, ultimo_dia) if dt_fim else False

        if ok:
            log_info(f"Datas preenchidas: Inicio [{primeiro_dia}] | Fim [{ultimo_dia}]")
            preencheu = True
            break
        else:
            log_error("Nem todos os campos confirmaram as datas.")

    if not preencheu:
        log_error("Nao consegui preencher as datas (seguirei mesmo assim).")

    X_PESQ_GENERIC = (
        "xpath=//button[normalize-space()='Pesquisar']"
        " | //input[( @type='button' or @type='submit') and "
        "           ( @value='Pesquisar' or @title='Pesquisar')]"
        " | //a[normalize-space()='Pesquisar']"
    )

    def _find_btn(fr):
        try:
            btn = fr.locator(f"xpath={X_BTN_MOV_PESQUISAR_LIVRO}").first
            if btn.count() and btn.is_visible():
                return btn
        except Exception:
            pass
        btn = fr.locator(X_PESQ_GENERIC).first
        return btn if btn.count() and btn.is_visible() else None

    for fr in frames:
        btn = _find_btn(fr)
        if not btn:
            continue
        try:
            with suppress(Exception):
                btn.scroll_into_view_if_needed(timeout=3_000)
            btn.click(timeout=8_000)
            log_info("Clique em 'Pesquisar' executado.")
            with suppress(PWTimeout):
                page.wait_for_load_state("networkidle", timeout=8_000)
            return True
        except PWTimeout:
            log_error("Timeout no clique; tentando via JS...")
            try:
                fr.evaluate("(el)=>el.click()", btn)
                log_info("Clique via JS executado.")
                return True
            except Exception as e2:
                log_error(f"Clique via JS tambem falhou: {e2}")

    log_error("Botao 'Pesquisar' nao encontrado.")
    with suppress(Exception):
        page.screenshot(path=str(Path.cwd() / "debug_pesquisar.png"))
        log_info("Screenshot salvo: debug_pesquisar.png")
    return False


def _baixar_relatorio_nota_nacional_recebidas_base(
    page: Page,
    saida_dir: Optional[Path] = None,
    *,
    intermediario: bool = False,
    target_url: Optional[str] = None,
    label: str = "Nota Nacional Recebidas",
    fname_base: str = "relatorio_nota_nacional_recebidas",
    download_timeout_ms: int = 120000,
) -> Optional[Path]:
    """
    Consulta Nota Fiscal ServiÃ§os Nota Nacional Recebidas:
      - (opcional) seleciona "IntermediÃ¡rio"
      - Preenche perÃ­odo (1Âº dia e Ãºltimo dia do mÃªs selecionado)
      - Clica em Pesquisar
      - Aguarda tabela carregar
      - Clica em Imprimir e baixa o PDF
    """
    from urllib.parse import urlparse
    download_timeout_ms = _get_download_wait_timeout_ms(download_timeout_ms, minimum_ms=60_000)
    target = target_url or _build_relatorio_nota_nacional_recebidas_url(page)
    try:
        page.goto(target, wait_until="domcontentloaded", timeout=min(download_timeout_ms, 120_000))
    except Exception:
        pass

    try:
        expected = (urlparse(target).path or "").lower().split("/")[-1] or ""
    except Exception:
        expected = ""
    if expected and expected not in (page.url or "").lower():
        log_warning(f"Nao consegui confirmar a URL da consulta de {label} (seguirei mesmo assim).")

    if saida_dir is None:
        saida_dir = _get_downloads_dir()
    saida_dir.mkdir(parents=True, exist_ok=True)

    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]

    if intermediario:
        selected = False
        for fr in frames:
            try:
                opt = fr.locator("xpath=//*[@id='vTIPOCONSULTA2']").first
                if opt and opt.count() and opt.is_visible():
                    try:
                        if opt.is_checked():
                            selected = True
                            break
                    except Exception:
                        pass
                    with suppress(Exception):
                        opt.scroll_into_view_if_needed(timeout=2000)
                    opt.click(timeout=4000)
                    selected = True
                    break
            except Exception:
                continue
        if not selected:
            log_warning("Nao consegui selecionar o tipo 'Intermediario' (seguirei mesmo assim).")

    data_ini, data_fim = _nm_periodo_datas()
    log_info(f"Periodo GUI -> Browser: Data Inicio={data_ini} | Data Fim={data_fim}")

    preencheu = False
    for fr in frames:
        try:
            fr.wait_for_load_state("domcontentloaded", timeout=min(15_000, int(download_timeout_ms * 0.10)))
        except PWTimeout:
            pass

        dt_ini, dt_fim = _nm_find_date_inputs(fr)
        if not dt_ini:
            with suppress(Exception):
                dt_ini = fr.locator("xpath=//*[@id='vDTAINI_dp_trigger']/preceding::input[1]").first
                if not (dt_ini and dt_ini.count() and dt_ini.is_visible()):
                    dt_ini = None
        if not dt_fim:
            with suppress(Exception):
                dt_fim = fr.locator("xpath=//*[@id='vDTAFIM_dp_trigger']/preceding::input[1]").first
                if not (dt_fim and dt_fim.count() and dt_fim.is_visible()):
                    dt_fim = None

        if not (dt_ini and dt_fim):
            continue

        ok = True
        ok &= _nm_fill_input(dt_ini, data_ini)
        ok &= _nm_fill_input(dt_fim, data_fim)
        if ok:
            preencheu = True
            break

    if not preencheu:
        log_error("Nao consegui preencher as datas (seguirei mesmo assim).")

    X_PESQ = "xpath=//*[@id='TABLE4']/tbody/tr[7]/td/input[1]"
    X_PESQ_ALT = "xpath=//*[@id='TABLE4']//input[@value='Pesquisar' or @title='Pesquisar' or @name='Pesquisar']"
    X_PESQ_GENERIC = (
        "xpath=//button[normalize-space()='Pesquisar']"
        " | //input[( @type='button' or @type='submit') and "
        "           ( @value='Pesquisar' or @title='Pesquisar')]"
        " | //a[normalize-space()='Pesquisar']"
    )
    clicked = False
    for fr in frames:
        btn = None
        try:
            btn = fr.locator(X_PESQ).first
            if not (btn and btn.count() and btn.is_visible()):
                btn = fr.locator(X_PESQ_ALT).first
            if not (btn and btn.count() and btn.is_visible()):
                btn = fr.locator(X_PESQ_GENERIC).first
            if btn and btn.count() and btn.is_visible():
                with suppress(Exception):
                    btn.scroll_into_view_if_needed(timeout=3_000)
                btn.click(timeout=8_000)
                clicked = True
                log_info("Clique em 'Pesquisar' executado.")
                with suppress(PWTimeout):
                    page.wait_for_load_state("networkidle", timeout=min(30_000, int(download_timeout_ms * 0.20)))
                break
        except Exception:
            if btn is not None:
                with suppress(Exception):
                    fr.evaluate("(el)=>el.click()", btn)
                    clicked = True
                    log_info("Clique em 'Pesquisar' via JS executado.")
                    break

    if not clicked:
        log_error("Botao 'Pesquisar' nao encontrado.")
        with suppress(Exception):
            dbg_dir = _get_downloads_dir()
            dbg_dir.mkdir(parents=True, exist_ok=True)
            suffix = "emitidas" if "emitidas" in (fname_base or "") else "recebidas"
            page.screenshot(path=str(dbg_dir / f"debug_relatorio_nota_nacional_{suffix}_pesquisar.png"))
            log_info(f"Screenshot salvo: {dbg_dir / f'debug_relatorio_nota_nacional_{suffix}_pesquisar.png'}")
        return None

    # aguarda tabela carregar
    table_wait_sec = max(30, min(180, int(download_timeout_ms / 1000)))
    end = time.time() + table_wait_sec
    while time.time() < end:
        found_table = False
        for fr in frames:
            try:
                tbl = _find_table(fr)
            except Exception:
                tbl = None
            if tbl:
                found_table = True
                rows = tbl.query_selector_all("xpath=.//tr[position()>1]") or []
                if rows:
                    log_info(f"Tabela carregada: {len(rows)} linha(s).")
                    end = time.time()
                    break
        if not found_table:
            time.sleep(0.5)
        else:
            break

    X_IMPR = "xpath=//*[@id='TABLE4']/tbody/tr[7]/td/input[2]"
    X_IMPR_ALT = "xpath=//*[@id='TABLE4']//input[@value='Imprimir' or @title='Imprimir' or @name='Imprimir']"
    X_IMPR_GENERIC = (
        "xpath=//button[normalize-space()='Imprimir']"
        " | //input[( @type='button' or @type='submit') and "
        "           ( @value='Imprimir' or @title='Imprimir')]"
        " | //a[normalize-space()='Imprimir']"
    )
    btn = None
    wait_imprimir_sec = max(12, min(90, int(download_timeout_ms / 2000)))
    end_impr = time.time() + wait_imprimir_sec
    while time.time() < end_impr and not btn:
        for fr in frames:
            try:
                btn = fr.locator(X_IMPR).first
                if not (btn and btn.count() and btn.is_visible()):
                    btn = fr.locator(X_IMPR_ALT).first
                if not (btn and btn.count() and btn.is_visible()):
                    btn = fr.locator(X_IMPR_GENERIC).first
                if btn and btn.count() and btn.is_visible():
                    break
            except Exception:
                btn = None
        if not btn:
            time.sleep(0.5)

    if btn and btn.count() and btn.is_visible():
        try:
            with suppress(Exception):
                btn.scroll_into_view_if_needed(timeout=3_000)

            def _click():
                with suppress(Exception):
                    btn.click(timeout=8_000)
                with suppress(Exception):
                    btn.click(timeout=4_000, force=True)

            fname = f"{fname_base}.pdf"
            if fname_base == "relatorio_nota_nacional_recebidas":
                fname = f"{fname_base}_tomador.pdf"
                if intermediario:
                    fname = f"{fname_base}_intermediario.pdf"
            elif intermediario:
                fname = f"{fname_base}_intermediario.pdf"
            start_ts = time.time()
            got = _download_pdf_from_click(
                page,
                _click,
                saida_dir,
                fname_hint=fname,
                download_timeout_ms=download_timeout_ms,
            )
            if got:
                return got
            extra = _wait_for_download_file(
                saida_dir,
                page,
                fname,
                timeout_ms=download_timeout_ms,
                start_ts=start_ts,
            )
            if extra:
                return extra
            log_error("Download nao confirmado para o relatorio nacional.")
            return None
        except Exception:
            pass

    log_error("Botao 'Imprimir' nao encontrado.")
    with suppress(Exception):
        dbg_dir = _get_downloads_dir()
        dbg_dir.mkdir(parents=True, exist_ok=True)
        suffix = "emitidas" if "emitidas" in (fname_base or "") else "recebidas"
        page.screenshot(path=str(dbg_dir / f"debug_relatorio_nota_nacional_{suffix}_imprimir.png"))
        log_info(f"Screenshot salvo: {dbg_dir / f'debug_relatorio_nota_nacional_{suffix}_imprimir.png'}")
    return None


def baixar_relatorio_nota_nacional_recebidas(
    page: Page,
    saida_dir: Optional[Path] = None,
) -> Optional[Path]:
    return _baixar_relatorio_nota_nacional_recebidas_base(page, saida_dir, intermediario=False)


def baixar_relatorio_nota_nacional_recebidas_intermediario(
    page: Page,
    saida_dir: Optional[Path] = None,
) -> Optional[Path]:
    return _baixar_relatorio_nota_nacional_recebidas_base(page, saida_dir, intermediario=True)


def baixar_relatorio_nota_nacional_emetidas(
    page: Page,
    saida_dir: Optional[Path] = None,
) -> Optional[Path]:
    target = _build_relatorio_nota_nacional_emitidas_url(page)
    return _baixar_relatorio_nota_nacional_recebidas_base(
        page,
        saida_dir,
        intermediario=False,
        target_url=target,
        label="Nota Nacional Emitidas",
        fname_base="relatorio_nota_nacional_emetidas",
    )


def baixar_extrato_detalhado_tomado_prestado(
    page: Page,
    saida_dir: Optional[Path] = None,
) -> Optional[List[Path]]:
    """
    Conta Corrente Fiscal (Extrato ISSQN detalhado):
      - Filtra por ano inicial/final
      - Pesquisa
      - Localiza a linha do mes escolhido pelo usuario nas tabelas
        de SERVICO PRESTADO e SERVICO TOMADO
      - Clica no icone vIMPRIMIR_000X da linha correspondente
      - Salva os PDFs com nomes dedicados, sem conflito
    """
    target = _build_extrato_issqn_url(page)
    download_timeout_ms = _get_download_wait_timeout_ms(120_000, minimum_ms=60_000)

    try:
        page.goto(target, wait_until="domcontentloaded", timeout=min(download_timeout_ms, 120_000))
    except Exception:
        pass
    with suppress(Exception):
        page.wait_for_load_state("domcontentloaded", timeout=8_000)

    if "hwmcontacorrente" not in (page.url or "").lower():
        log_warning("Nao consegui confirmar a URL do Extrato ISSQN detalhado (seguirei mesmo assim).")

    if saida_dir is None:
        saida_dir = _get_downloads_dir()
    saida_dir.mkdir(parents=True, exist_ok=True)

    p = get_periodo_dict()
    ano_de = str(p.get("ano_de") or "").strip()
    ano_ate = str(p.get("ano_ate") or "").strip() or ano_de
    mes_de = str(p.get("mes_de") or "").strip()

    if not ano_de:
        ano_de = str(datetime.now().year)
    if not ano_ate:
        ano_ate = ano_de
    if not mes_de:
        mes_de = f"{datetime.now().month:02d}"

    try:
        mm = max(1, min(12, int(mes_de)))
    except Exception:
        mm = datetime.now().month

    pt_abbr = {
        1: "JAN",
        2: "FEV",
        3: "MAR",
        4: "ABR",
        5: "MAI",
        6: "JUN",
        7: "JUL",
        8: "AGO",
        9: "SET",
        10: "OUT",
        11: "NOV",
        12: "DEZ",
    }
    mes_token = f"{pt_abbr.get(mm, 'JAN')}/{ano_de}".upper()
    ref_tag = f"{mm:02d}_{ano_de}"

    log_info(f"Extrato detalhado ISSQN: referencia={mes_token} | periodo anos {ano_de}-{ano_ate}")

    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]

    def _find_ano_inputs(fr):
        with suppress(PWTimeout):
            fr.wait_for_load_state("domcontentloaded", timeout=5_000)

        def _wait_first(xp: str):
            try:
                loc = fr.wait_for_selector(f"xpath={xp}", timeout=4000)
                if loc and loc.is_visible():
                    return loc
            except Exception:
                pass
            try:
                loc = fr.locator(f"xpath={xp}").first
                if loc and loc.count():
                    return loc
            except Exception:
                pass
            return None

        ini = _wait_first("//*[@id='vANOCTACORRENTE1']")
        fim = _wait_first("//*[@id='vANOCTACORRENTE2']")

        if not ini or not fim:
            ini = ini or _wait_first(
                "//label[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'ano inicial')]/following::input[1]"
                " | //label[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'ano inicial')]/following::select[1]"
            )
            fim = fim or _wait_first(
                "//label[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'ano final')]/following::input[1]"
                " | //label[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'ano final')]/following::select[1]"
            )
        return ini, fim

    def _set_year(loc, year: str) -> bool:
        if not loc:
            return False
        ok = select_year(loc, year)
        if not ok:
            ok = _nm_fill_input(loc, year)
        if not ok:
            with suppress(Exception):
                loc.evaluate(
                    """
                    (el, yy)=>{
                      if (!el) return false;
                      if (el.getAttribute('readonly')) el.removeAttribute('readonly');
                      el.value = String(yy);
                      el.dispatchEvent(new Event('input', {bubbles:true}));
                      el.dispatchEvent(new Event('change', {bubbles:true}));
                      return true;
                    }
                    """,
                    year,
                )
                ok = True
        return ok

    filled = False
    for fr in frames:
        sel_ini, sel_fim = _find_ano_inputs(fr)
        if not (sel_ini and sel_fim):
            continue
        ok = True
        ok &= _set_year(sel_ini, ano_de)
        ok &= _set_year(sel_fim, ano_ate)
        if ok:
            filled = True
            break

    if not filled:
        log_warning("Nao consegui selecionar os anos (seguirei mesmo assim).")

    X_PESQ = "xpath=//*[@id='TABLE2']/tbody/tr[4]/td/input"
    X_PESQ_GENERIC = (
        "xpath=//button[normalize-space()='Pesquisar']"
        " | //input[( @type='button' or @type='submit') and "
        "           ( @value='Pesquisar' or @title='Pesquisar')]"
        " | //a[normalize-space()='Pesquisar']"
    )

    clicked = False
    for fr in frames:
        btn = None
        try:
            btn = fr.locator(X_PESQ).first
            if not (btn and btn.count() and btn.is_visible()):
                btn = fr.locator(X_PESQ_GENERIC).first
            if btn and btn.count() and btn.is_visible():
                with suppress(Exception):
                    btn.scroll_into_view_if_needed(timeout=3_000)
                btn.click(timeout=8_000)
                clicked = True
                log_info("Clique em 'Pesquisar' executado.")
                with suppress(PWTimeout):
                    page.wait_for_load_state("networkidle", timeout=min(30_000, int(download_timeout_ms * 0.20)))
                break
        except Exception:
            if btn is not None:
                with suppress(Exception):
                    fr.evaluate("(el)=>el.click()", btn)
                    clicked = True
                    log_info("Clique em 'Pesquisar' via JS executado.")
                    break

    if not clicked:
        log_error("Botao 'Pesquisar' nao encontrado.")
        with suppress(Exception):
            dbg_dir = _get_downloads_dir()
            dbg_dir.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(dbg_dir / "debug_extrato_detalhado_pesquisar.png"))
            log_info(f"Screenshot salvo: {dbg_dir / 'debug_extrato_detalhado_pesquisar.png'}")
        return None

    # aguarda resultados basicos
    table_wait_sec = max(30, min(180, int(download_timeout_ms / 1000)))
    end = time.time() + table_wait_sec
    ready = False
    while time.time() < end:
        for fr in frames:
            try:
                tbl = _find_table(fr, [
                    "//table[contains(@class,'Grid') or contains(@class,'DataGrid')]",
                    "//table[contains(@id,'Grid')]",
                    "//table",
                ])
            except Exception:
                tbl = None
            if not tbl:
                continue
            rows = tbl.query_selector_all("xpath=.//tr[position()>1]") or []
            if rows:
                ready = True
                break
        if ready:
            break
        time.sleep(0.5)

    if not ready:
        log_warning("Nao consegui confirmar a tabela carregada; vou tentar localizar as linhas mesmo assim.")

    def _find_print_button(fr, table_hint: str):
        table_xp = (
            "//table[.//td[contains(translate(normalize-space(.),'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ'),'SERV') "
            f"and contains(translate(normalize-space(.),'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ'),'{table_hint}')]]"
        )
        try:
            tbl = fr.locator(f"xpath={table_xp}").first
            if not (tbl and tbl.count()):
                return None, None
        except Exception:
            return None, None

        tokens = [mes_token, mes_token.replace("/", " / ")]
        for token in tokens:
            row_xp = (
                "xpath=.//tr[td[1][contains(translate(normalize-space(.),'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ'),"
                f"'{token}')]]"
            )
            try:
                row = tbl.locator(row_xp).first
                if not (row and row.count()):
                    continue
                btn = row.locator("xpath=.//*[@id and starts-with(@id,'vIMPRIMIR_')]").first
                if btn and btn.count() and btn.is_visible():
                    btn_id = btn.get_attribute("id") or ""
                    return btn, btn_id
            except Exception:
                continue

        return None, None

    def _download_from_click(btn, desired_basename: str) -> Optional[Path]:
        # 1) popup + printToPDF
        try:
            with page.expect_popup(timeout=20_000) as pop_info:
                btn.click(timeout=10_000)
            pop = pop_info.value
            with suppress(Exception):
                pop.wait_for_load_state("domcontentloaded", timeout=10_000)
            fp = _print_page_to_pdf(pop, saida_dir, fname_hint=f"{desired_basename}.pdf")
            with suppress(Exception):
                pop.close()
            if fp:
                return fp
        except Exception:
            pass

        # 2) download direto
        with suppress(Exception):
            fp = _expect_and_save_download(
                page,
                lambda: btn.click(timeout=10_000),
                saida_dir,
                timeout_ms=download_timeout_ms,
                desired_basename=desired_basename,
            )
            if fp:
                return fp

        # 3) fallback: clica e imprime pagina atual
        try:
            btn.click(timeout=10_000)
            with suppress(Exception):
                page.wait_for_load_state("domcontentloaded", timeout=8_000)
            return _print_page_to_pdf(page, saida_dir, fname_hint=f"{desired_basename}.pdf")
        except Exception:
            return None

    targets = [
        ("PREST", "esxtrato_detalhado_prestado"),
        ("TOMAD", "extrato_detalhado_tomado"),
    ]

    downloaded: List[Path] = []
    for table_hint, base_name in targets:
        found_btn = None
        found_id = ""
        for fr in frames:
            btn, btn_id = _find_print_button(fr, table_hint)
            if btn is not None:
                found_btn = btn
                found_id = btn_id
                break

        if found_btn is None:
            log_warning(f"Nao encontrei icone vIMPRIMIR para {table_hint} na referencia {mes_token}.")
            continue

        log_info(f"Icone encontrado ({table_hint}) id={found_id}. Gerando PDF...")
        desired_basename = f"{base_name}_{ref_tag}"
        fp = _download_from_click(found_btn, desired_basename)
        if fp:
            downloaded.append(fp)
            log_info(f"Extrato detalhado salvo: {fp}")
        else:
            log_warning(f"Falha ao gerar extrato detalhado para {table_hint}.")

    if not downloaded:
        return None
    return downloaded


def baixar_extrato_issqn(
    page: Page,
    saida_dir: Optional[Path] = None,
) -> Optional[List[Path]]:
    """Mantido por compatibilidade: delega para o extrato detalhado (prestado/tomado)."""
    return baixar_extrato_detalhado_tomado_prestado(page, saida_dir=saida_dir)


def _norm_txt(s: str) -> str:
    import unicodedata

    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", s).strip().lower()


def _find_table(fr, tabela_xpaths: Optional[List[str]] = None):
    candidates = tabela_xpaths or [
        "//div[@id='Grid2ContainerDiv']//table",
        "//div[@id='GridContainerDiv']//table",
        "//table[contains(@class,'Grid') or contains(@class,'DataGrid')]",
        "//table[@id='TABLE1']",
    ]
    for xp in candidates:
        with suppress(PWTimeout, Exception):
            tbl = fr.wait_for_selector(f"xpath={xp}", timeout=4000)
            if tbl and tbl.is_visible():
                return tbl
    return None


def _col_idx_qtd_notas(tbl) -> Optional[int]:
    heads = (tbl.query_selector_all("xpath=.//thead/tr[1]/*[self::th or self::td]")
             or tbl.query_selector_all("xpath=.//tr[1]/*[self::th or self::td]"))
    for i, th in enumerate(heads, start=1):
        txt = _norm_txt(th.inner_text() or "")
        if "qtd" in txt and "nota" in txt:
            return i
    return None


def _get_qtd_notas_for_row(tbl, tr, idx_qtd: Optional[int] = None) -> int:
    if not idx_qtd:
        idx_qtd = _col_idx_qtd_notas(tbl)
    if not idx_qtd:
        return 0
    td = tr.query_selector(f"xpath=./td[{idx_qtd}]")
    if not td:
        return 0
    raw = (td.inner_text() or "").strip().replace("\xa0", " ")
    m = re.search(r"\d[\d\.\,]*", raw)
    if not m:
        return 0
    try:
        return int(m.group(0).replace(".", "").replace(",", ""))
    except Exception:
        return 0


def _expect_and_save_download(
    page: Page,
    click_callable,
    saida_dir: Path,
    timeout_ms: int = 60_000,
    desired_basename: Optional[str] = None,
) -> Optional[Path]:
    saida_dir.mkdir(parents=True, exist_ok=True)
    timeout_ms = _get_download_wait_timeout_ms(timeout_ms, minimum_ms=30_000)
    try:
        with page.expect_download(timeout=timeout_ms) as dlinfo:
            click_callable()
        dl = dlinfo.value

        sug = dl.suggested_filename or "arquivo"
        if desired_basename:
            ext = Path(sug).suffix
            sug = desired_basename + (ext if ext else "")

        fp = _resolve_target_filepath(saida_dir, page, sug)
        dl.save_as(str(fp))
        log_info(f"Download salvo: {fp}")
        return fp
    except Exception as e:
        log_error(f"Falha ao baixar arquivo: {e}")
        return None


def baixar_notas_emitidas(
    page: Page,
    saida_dir: Optional[Path] = None,
    tabela_xpaths: Optional[List[str]] = None,
) -> Optional[Path]:
    log_info("Baixando Notas Emitidas...")
    if saida_dir is None:
        saida_dir = _get_downloads_dir()
    download_wait_ms = _get_download_wait_timeout_ms(120_000, minimum_ms=60_000)

    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
    table_wait_sec = max(30, min(180, int(download_wait_ms / 1000)))
    deadline = time.time() + table_wait_sec
    for fr in frames:
        log_info("Verificando frame principal..." if fr is page.main_frame else f"Verificando iframe '{fr.url or ''}'...")

        tbl = None
        while time.time() < deadline and not tbl:
            tbl = _find_table(fr, tabela_xpaths)
            if not tbl:
                time.sleep(0.5)
        if not tbl:
            log_info("tabela nao encontrada aqui.")
            continue

        rows = (tbl.query_selector_all("xpath=.//tbody/tr[td]") or
                tbl.query_selector_all("xpath=.//tr[td]") or [])
        log_info(f"Linhas encontradas: {len(rows)}")
        if not rows:
            continue

        tr = rows[0]
        qtd = _get_qtd_notas_for_row(tbl, tr)
        log_info(f"Qtd Notas (linha 1) = {qtd}")
        if qtd < 1:
            log_info("Qtd Notas < 1 -> nada a baixar.")
            return None

        log_info("Clicando 'Processar'...")
        proc = fr.locator('xpath=//*[@id="vVISUALIZAR_0001"]').first
        if not proc or not proc.count() or not proc.is_visible():
            log_error("Icone 'Processar' nao encontrado/visivel.")
            return None
        with suppress(Exception):
            proc.scroll_into_view_if_needed(timeout=1500)
        proc.click(timeout=10_000)

        time.sleep(1.0)
        dlicon = fr.locator('xpath=//*[@id="vDOWNLOAD_0001"]').first
        wait_download_icon_sec = max(15, min(120, int(download_wait_ms / 1000)))
        end = time.time() + wait_download_icon_sec
        while (not dlicon or not dlicon.count() or not dlicon.is_visible()) and time.time() < end:
            time.sleep(0.5)
            dlicon = fr.locator('xpath=//*[@id="vDOWNLOAD_0001"]').first

        if not dlicon or not dlicon.count() or not dlicon.is_visible():
            log_error("Icone 'Download' nao encontrado/visivel apos processar.")
            return None

        log_info("Clicando 'Download'... (esperando ZIP)")
        return _expect_and_save_download(page, lambda: dlicon.click(timeout=10_000), saida_dir, timeout_ms=download_wait_ms)

    log_error("Nao foi possivel processar/baixar as notas (ZIP).")
    with suppress(Exception):
        dbg_dir = _get_downloads_dir()
        dbg_dir.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(dbg_dir / "debug_download_zip.png"))
        log_info(f"Screenshot salvo: {dbg_dir / 'debug_download_zip.png'}")
    return None


def baixar_notas_recebidas(
    page: Page,
    saida_dir: Optional[Path] = None,
    tabela_xpaths: Optional[List[str]] = None,
) -> Optional[Path]:
    """
    Garante Tipo Nota = 'Recebidas' e entao processa a 1a linha:
      - clica 'Processar'
      - espera 1s
      - clica 'Download' e salva o ZIP.
    """
    log_info("Baixando Notas Recebidas...")
    if saida_dir is None:
        saida_dir = _get_downloads_dir()
    download_wait_ms = _get_download_wait_timeout_ms(120_000, minimum_ms=60_000)

    frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]

    def _force_tipo_recebidas(fr) -> bool:
        try:
            sel = fr.locator(
                "xpath=//select[@id='vTIPONOTA' or @name='vTIPONOTA' "
                "or contains(@id,'TIPONOTA') or contains(@name,'TIPONOTA')]"
            ).first
            if not sel or not sel.count():
                return False

            with suppress(Exception):
                sel.select_option(label=re.compile(r"recebidas", re.I))

            txt = sel.evaluate("(el)=>el.selectedOptions?.[0]?.textContent?.trim() || ''") or ""
            if not re.search(r"recebidas", txt, re.I):
                opt = sel.evaluate(
                    """
                    (el) => {
                      const i = Array.from(el.options).findIndex(o =>
                        /recebid/i.test((o.textContent||'').trim())
                      );
                      return i >= 0 ? el.options[i].value : null;
                    }
                    """
                )
                if opt:
                    with suppress(Exception):
                        sel.select_option(value=str(opt))

            txt = sel.evaluate("(el)=>el.selectedOptions?.[0]?.textContent?.trim() || ''") or ""
            if not re.search(r"recebidas", txt, re.I):
                ok = sel.evaluate(
                    """
                    (el)=>{ 
                      const i = Array.from(el.options).findIndex(o=>/recebid/i.test((o.textContent||'').trim()));
                      if (i < 0) return false;
                      el.selectedIndex = i; el.dispatchEvent(new Event('change',{bubbles:true}));
                      return true;
                    }
                    """
                )
                if not ok:
                    return False

            txt = sel.evaluate("(el)=>el.selectedOptions?.[0]?.textContent?.trim() || ''") or ""
            if not re.search(r"recebidas", txt, re.I):
                return False

            log_info("Tipo Nota fixado em 'Recebidas'.")
            btn = fr.locator(
                "xpath=//button[normalize-space()='Pesquisar'] | "
                "//input[( @type='button' or @type='submit') and "
                "( @value='Pesquisar' or @title='Pesquisar')] | "
                "//a[normalize-space()='Pesquisar']"
            ).first
            if btn and btn.count() and btn.is_visible():
                with suppress(Exception):
                    btn.scroll_into_view_if_needed(timeout=1500)
                btn.click(timeout=8_000)
                with suppress(PWTimeout):
                    page.wait_for_load_state("networkidle", timeout=15_000)
            return True
        except Exception:
            return False

    tipo_ok = False
    for fr in frames:
        if _force_tipo_recebidas(fr):
            tipo_ok = True
            break

    if not tipo_ok:
        log_error("Nao foi possivel garantir 'Tipo Nota' = Recebidas. Abortando.")
        return None

    table_wait_sec = max(30, min(180, int(download_wait_ms / 1000)))
    deadline = time.time() + table_wait_sec
    for fr in frames:
        log_info("Verificando frame principal..." if fr is page.main_frame else f"Verificando iframe '{fr.url or ''}'...")

        tbl = None
        while time.time() < deadline and not tbl:
            tbl = _find_table(fr, tabela_xpaths)
            if not tbl:
                time.sleep(0.5)
        if not tbl:
            log_info("tabela nao encontrada aqui.")
            continue

        rows = (tbl.query_selector_all("xpath=.//tbody/tr[td]") or
                tbl.query_selector_all("xpath=.//tr[td]") or [])
        log_info(f"Linhas encontradas: {len(rows)}")
        if not rows:
            continue

        tr = rows[0]
        qtd = _get_qtd_notas_for_row(tbl, tr)
        log_info(f"Qtd Notas (linha 1) = {qtd}")
        if qtd < 1:
            log_info("Qtd Notas < 1 -> nada a baixar.")
            return None

        log_info("Clicando 'Processar'...")
        proc = fr.locator('xpath=//*[@id="vVISUALIZAR_0001"]').first
        if not proc or not proc.count() or not proc.is_visible():
            log_error("Icone 'Processar' nao encontrado/visivel.")
            return None
        with suppress(Exception):
            proc.scroll_into_view_if_needed(timeout=1500)
        proc.click(timeout=10_000)

        time.sleep(1.0)
        dlicon = fr.locator('xpath=//*[@id="vDOWNLOAD_0001"]').first
        wait_download_icon_sec = max(15, min(120, int(download_wait_ms / 1000)))
        end = time.time() + wait_download_icon_sec
        while (not dlicon or not dlicon.count() or not dlicon.is_visible()) and time.time() < end:
            time.sleep(0.5)
            dlicon = fr.locator('xpath=//*[@id="vDOWNLOAD_0001"]').first
        if not dlicon or not dlicon.count() or not dlicon.is_visible():
            log_error("Icone 'Download' nao encontrado/visivel apos processar.")
            return None

        log_info("Clicando 'Download'... (esperando ZIP)")
        return _expect_and_save_download(
            page,
            lambda: dlicon.click(timeout=10_000),
            saida_dir,
            timeout_ms=download_wait_ms,
            desired_basename="lote_nfse_recebidas",
        )

    log_error("Nao foi possivel processar/baixar as notas (ZIP).")
    with suppress(Exception):
        dbg_dir = _get_downloads_dir()
        dbg_dir.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(dbg_dir / "debug_download_zip_recebidas.png"))
        log_info(f"Screenshot salvo: {dbg_dir / 'debug_download_zip_recebidas.png'}")
    return None


def _find_mes_ano_de_in_frame(fr) -> Tuple[Optional[object], Optional[object]]:
    """
    Retorna (mes_de, ano_de) como Locators (ou (None, None)).
    Procura por IDs/NAMEs comuns. Em fallback, usa os dois primeiros <select> visiveis.
    """
    def _get(xp: str):
        try:
            loc = fr.locator(f"xpath={xp}").first
            return loc if (loc.count() and loc.is_visible()) else None
        except Exception:
            return None

    mes_de = _get('//*[@id="vMES" or @name="vMES" or contains(@id,"MES") or contains(@name,"MES")]')
    ano_de = _get('//*[@id="vANO" or @name="vANO" or contains(@id,"ANO") or contains(@name,"ANO")]')
    if mes_de or ano_de:
        return mes_de, ano_de

    try:
        selects = fr.locator("xpath=(//form//select | //div//select)").all()
        selects = [s for s in selects if s.is_visible()]
        if len(selects) >= 2:
            return selects[0], selects[1]
    except Exception:
        pass
    return None, None


def abrir_emissao_guias(page: Page) -> bool:
    """
    Emissao de Guias (Conta Corrente).
    """
    def _frames():
        return [page.main_frame] + [f for f in page.frames if f is not page.main_frame]

    def _default_download_dir() -> Path:
        d = _get_downloads_dir()
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _ensure_unique(path: Path) -> Path:
        base, suf = path.with_suffix(""), path.suffix
        i, final = 1, path
        while final.exists():
            final = Path(f"{base}_{i}{suf}")
            i += 1
        return final

    download_wait_ms = _get_download_wait_timeout_ms(120_000, minimum_ms=60_000)

    def _scaled_timeout(ratio: float, floor_ms: int, ceil_ms: Optional[int] = None) -> int:
        ceiling = int(download_wait_ms if ceil_ms is None else ceil_ms)
        if ceiling <= 0:
            ceiling = int(download_wait_ms)
        return min(ceiling, max(int(floor_ms), int(download_wait_ms * ratio)))

    def _normalize_guia_fname(fname: str, guia_tipo: Optional[str]) -> str:
        if not guia_tipo:
            return fname
        base = Path(fname).stem or "guia"
        suf = Path(fname).suffix or ".pdf"
        norm = normalize_text(base)
        if any(k in norm for k in ("emitid", "recebid", "tomad", "prestad")):
            return f"{base}{suf}"
        return f"{base}_{guia_tipo}{suf}"

    def _save_bytes_pdf(data: bytes, fname: str = "guia.pdf", guia_tipo: Optional[str] = None) -> Path:
        fname = _normalize_guia_fname(fname, guia_tipo)
        dest = _ensure_unique(_resolve_target_filepath(_default_download_dir(), page, fname))
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        log_info(f"PDF salvo: {dest}")
        return dest

    def _safe_filename_from_resp(resp) -> str:
        from urllib.parse import urlparse, unquote

        cd = (resp.headers or {}).get("content-disposition", "")
        m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd, flags=re.I)
        if m:
            return unquote(m.group(1))
        name = Path(urlparse(resp.url).path).name or "guia.pdf"
        return name if name.lower().endswith(".pdf") else (name + ".pdf")

    def _download_via_context(ctx, url: str, guia_tipo: Optional[str] = None) -> Optional[Path]:
        if not url or url.startswith(("blob:", "data:")):
            return None
        try:
            req_timeout = min(download_wait_ms, max(25_000, int(download_wait_ms * 0.60)))
            resp = ctx.request.get(url, timeout=req_timeout)
            if not resp:
                return None
            ct = (resp.headers or {}).get("content-type", "").lower()
            data = resp.body()
            if "application/pdf" in ct or "octet-stream" in ct or (data and data[:4] == b"%PDF"):
                return _save_bytes_pdf(data, _safe_filename_from_resp(resp), guia_tipo=guia_tipo)
        except Exception as e:
            log_error(f"Falha em GET direto do contexto: {e}")
        return None

    def _find_pdf_urls_in(pop) -> list[str]:
        urls = set()

        def _collect(tgt):
            try:
                lst = tgt.evaluate(
                    """
                    () => {
                      const abs = u => { try { return new URL(u, location.href).href } catch(e) { return u } };
                      const get = (sel, attr) => Array.from(document.querySelectorAll(sel))
                            .map(e => e.getAttribute(attr)).filter(Boolean);
                      const cand = [
                        ...get('embed[type="application/pdf"]','src'),
                        ...get('object[type="application/pdf"]','data'),
                        ...get('iframe','src'),
                        ...get('a[href]','href'),
                      ].map(abs);
                      return cand;
                    }
                    """
                ) or []
            except Exception:
                lst = []
            for u in lst:
                lu = (u or "").lower()
                if ".pdf" in lu or "application/pdf" in lu or "pdf=" in lu or lu.startswith("blob:") or lu.startswith("data:application/pdf"):
                    urls.add(u)

        _collect(pop)
        for fr in pop.frames:
            _collect(fr)
        return list(urls)

    def _wait_popup_navigation(pop, timeout_ms: Optional[int] = None, require_pdf_hint: bool = False) -> None:
        budget_ms = max(2_500, min(int(timeout_ms or 0), 20_000))
        end = time.time() + (budget_ms / 1000.0)
        last_url = ""
        while time.time() < end:
            cur = (pop.url or "").strip()
            cur_low = cur.lower()
            if cur and not cur_low.startswith("about:blank"):
                if cur != last_url:
                    last_url = cur
                    log_info(f"Popup URL detectada: {cur}")
                with suppress(Exception):
                    pop.wait_for_load_state("domcontentloaded", timeout=min(1_500, budget_ms))
                if (not require_pdf_hint) or ("arrelnfnaclistagem" not in cur_low):
                    return
                if _find_pdf_urls_in(pop):
                    return
            with suppress(Exception):
                pop.wait_for_timeout(150)
            with suppress(Exception):
                pop.wait_for_load_state("domcontentloaded", timeout=500)

    def _print_to_pdf_via_cdp(pop, guia_tipo: Optional[str] = None) -> Optional[Path]:
        try:
            try:
                pop.emulate_media(media="print")
            except Exception:
                pass

            client = page.context.new_cdp_session(pop)
            params = {
                "printBackground": True,
                "preferCSSPageSize": True,
                "paperWidth": 8.27,
                "paperHeight": 11.69,
                "marginTop": 0.25,
                "marginBottom": 0.25,
                "marginLeft": 0.25,
                "marginRight": 0.25,
                "scale": 1.0,
                "landscape": False,
            }
            res = client.send("Page.printToPDF", params)
            import base64

            data = base64.b64decode(res.get("data", ""))
            if data and data[:4] == b"%PDF":
                log_info("PDF gerado via DevTools Page.printToPDF.")
                return _save_bytes_pdf(data, "guia_dam.pdf", guia_tipo=guia_tipo)
        except Exception as e:
            log_error(f"printToPDF via CDP falhou: {e}")
        return None

    def _try_viewer_download(pop, guia_tipo: Optional[str] = None) -> Optional[Path]:
        try:
            dl = pop.wait_for_event("download", timeout=_scaled_timeout(0.10, 4_000, 15_000))
            sug = _normalize_guia_fname(dl.suggested_filename or "guia.pdf", guia_tipo)
            dest = _ensure_unique(_resolve_target_filepath(_default_download_dir(), page, sug))
            dl.save_as(str(dest))
            log_info(f"Download automatico do popup: {dest}")
            return dest
        except Exception:
            pass

        SELS = [
            "#download",
            "#btnDownload",
            "[download]",
            "a[download]",
            "a[href$='.pdf']",
            "button[title*='Download' i]",
            "button[title*='Baixar' i]",
            "button[title*='Salvar' i]",
            "a[title*='Download' i]",
            "a[title*='Baixar' i]",
            "a[title*='Salvar' i]",
            "[aria-label*='Download' i]",
            "[aria-label*='Baixar' i]",
            "[aria-label*='Salvar' i]",
            "xpath=//button[contains(translate(.,'downloadbaixarsalvar','DOWNLOADBAIXARSALVAR'),'DOWNLOAD')]",
            "xpath=//a[contains(translate(.,'downloadbaixarsalvar','DOWNLOADBAIXARSALVAR'),'DOWNLOAD')]",
            "xpath=//button[contains(translate(.,'downloadbaixarsalvar','DOWNLOADBAIXARSALVAR'),'BAIXAR')]",
            "xpath=//a[contains(translate(.,'downloadbaixarsalvar','DOWNLOADBAIXARSALVAR'),'BAIXAR')]",
            "xpath=//button[contains(translate(.,'downloadbaixarsalvar','DOWNLOADBAIXARSALVAR'),'SALVAR')]",
            "xpath=//a[contains(translate(.,'downloadbaixarsalvar','DOWNLOADBAIXARSALVAR'),'SALVAR')]",
        ]
        for tgt in [pop, *pop.frames]:
            for sel in SELS:
                try:
                    loc = tgt.locator(sel).first
                    if loc and loc.count() and loc.is_visible():
                        try:
                            with pop.expect_download(timeout=_scaled_timeout(0.10, 4_000, 12_000)) as dlp:
                                loc.click(timeout=_scaled_timeout(0.12, 4_000, 20_000))
                            dl = dlp.value
                            sug = _normalize_guia_fname(dl.suggested_filename or "guia.pdf", guia_tipo)
                            dest = _ensure_unique(_resolve_target_filepath(_default_download_dir(), page, sug))
                            dl.save_as(str(dest))
                            log_info(f"Download via botao do viewer: {dest}")
                            return dest
                        except Exception:
                            pass
                except Exception:
                    continue
        return None

    def _process_popup_and_save(pop, guia_tipo: Optional[str] = None) -> Optional[Path]:
        _wait_popup_navigation(pop, timeout_ms=min(download_wait_ms, 15_000), require_pdf_hint=False)
        with suppress(Exception):
            pop.wait_for_load_state("domcontentloaded", timeout=_scaled_timeout(0.12, 4_000, 15_000))

        got = _try_viewer_download(pop, guia_tipo=guia_tipo)
        if got:
            return got

        got = _print_to_pdf_via_cdp(pop, guia_tipo=guia_tipo)
        if got:
            return got

        try:
            resp = pop.wait_for_response(
                lambda r: (
                    "application/pdf" in (r.headers or {}).get("content-type", "").lower()
                    or "octet-stream" in (r.headers or {}).get("content-type", "").lower()
                    or r.url.lower().endswith(".pdf")
                ),
                timeout=_scaled_timeout(0.20, 6_000, 20_000),
            )
            if resp:
                got = _download_via_context(page.context, resp.url, guia_tipo=guia_tipo)
                if got:
                    return got
                data = resp.body()
                return _save_bytes_pdf(data, _safe_filename_from_resp(resp), guia_tipo=guia_tipo)
        except Exception:
            pass

        for u in _find_pdf_urls_in(pop):
            got = _download_via_context(page.context, u, guia_tipo=guia_tipo)
            if got:
                return got

        log_error("Nenhuma estrategia de download funcionou para este popup.")
        return None

    def _wait_results(timeout_ms: Optional[int] = None) -> str:
        timeout_ms = int(timeout_ms or _scaled_timeout(0.30, 12_000, 90_000))
        end = time.time() + timeout_ms / 1000
        EMPTY_XP = (
            "xpath=//*[contains(.,'Nenhum registro') or contains(.,'Sem registro') "
            "or contains(.,'Sem registros') or contains(.,'Nao existem registros')]"
        )
        ROWS_XP = (
            "xpath=(//div[contains(@id,'Grid') or contains(@id,'Container') or contains(@class,'gx-grid') "
            "or contains(@class,'Grid') or contains(@id,'ContainerDiv')]//table)[1]//tr[position()>1]"
        )
        IMPR_XP = "xpath=//*[@id and starts-with(@id,'vIMPRIMIR_')]"
        while time.time() < end:
            with suppress(Exception):
                page.wait_for_load_state("domcontentloaded", timeout=400)
            for fr in _frames():
                if fr.locator(IMPR_XP).count() > 0:
                    return "rows"
                if fr.locator(ROWS_XP).count() > 0:
                    return "rows"
                empty = fr.locator(EMPTY_XP).first
                if empty.count() and empty.is_visible():
                    return "empty"
            with suppress(Exception):
                page.wait_for_timeout(200)
        return "timeout"

    target = _build_guias_url(page)
    with suppress(Exception):
        page.goto(target, wait_until="domcontentloaded", timeout=_scaled_timeout(0.20, 12_000, 60_000))

    if "hwmguiarec" not in page.url.lower():
        log_info("Abrindo Emissao de Guias pelo menu...")
        try:
            page.hover(X_MENU_MOVIMENTACOES, timeout=_scaled_timeout(0.10, 5_000, 20_000))
            page.click(
                "xpath=//a[contains(@href,'hwmguiarec') or normalize-space()='Emissao de Guias' or contains(.,'Guia')]",
                timeout=_scaled_timeout(0.12, 6_000, 25_000),
            )
            page.wait_for_url("**/hwmguiarec*", timeout=_scaled_timeout(0.20, 10_000, 45_000))
        except Exception as e:
            log_error(f"Falha ao abrir Emissao de Guias: {e}")
            return False

    p = get_periodo_dict()
    preencheu = False
    for fr in _frames():
        with suppress(PWTimeout):
            fr.wait_for_load_state("domcontentloaded", timeout=_scaled_timeout(0.10, 3_000, 12_000))
        mes_de, ano_de = _find_mes_ano_de_in_frame(fr)
        ok = True
        if mes_de:
            ok &= select_month(mes_de, p["mes_de"])
        if ano_de:
            ok &= select_year(ano_de, p["ano_de"])
        if ok:
            preencheu = True
            break
    if not preencheu:
        log_error("Nao consegui preencher 'Mes De'/'Ano De' (seguirei).")

    def _set_recolhimento_todos() -> None:
        for fr in _frames():
            try:
                dd = fr.locator("#vGRCTRIBUTO").first
                if not (dd.count() and dd.is_visible()):
                    continue
                tag = None
                with suppress(Exception):
                    tag = dd.evaluate("el => el.tagName && el.tagName.toUpperCase()")
                if tag == "SELECT":
                    opt = dd.locator("option", has_text=re.compile(r"^\s*Todos\s*$", re.I)).first
                    if opt.count():
                        val = opt.get_attribute("value") or opt.inner_text().strip()
                        dd.select_option(value=val)
                        with suppress(Exception):
                            dd.evaluate("el => el.dispatchEvent(new Event('change',{bubbles:true}))")
                        log_info("Recolhimento = Todos (select).")
                        return
                with suppress(Exception):
                    dd.scroll_into_view_if_needed(timeout=1500)
                dd.click(timeout=3000)
                for xp in [
                    "xpath=//li[normalize-space()='Todos']",
                    "xpath=//*[@role='option' and normalize-space()='Todos']",
                    "xpath=//*[contains(@class,'option') and normalize-space()='Todos']",
                    "xpath=//div[contains(@class,'item') and normalize-space()='Todos']",
                    "xpath=//option[normalize-space()='Todos']",
                ]:
                    it = fr.locator(xp).first
                    if it.count() and it.is_visible():
                        it.click(timeout=3000)
                        log_info("Recolhimento = Todos (combobox).")
                        return
            except Exception:
                continue
        log_info("Nao consegui definir Recolhimento=Todos (seguindo).")

    _set_recolhimento_todos()

    X_PESQ_GENERIC = (
        "xpath=//button[normalize-space()='Pesquisar']"
        " | //input[((@type='button') or (@type='submit')) and (@value='Pesquisar' or @title='Pesquisar')]"
        " | //a[normalize-space()='Pesquisar']"
    )

    def _find_btn(fr):
        try:
            btn = fr.locator(f"xpath={X_BTN_MOV_PESQUISAR_LIVRO}").first
            if btn.count() and btn.is_visible():
                return btn
        except Exception:
            pass
        btn = fr.locator(X_PESQ_GENERIC).first
        return btn if btn.count() and btn.is_visible() else None

    def _click_pesquisar() -> bool:
        for fr in _frames():
            btn = _find_btn(fr)
            if not btn:
                continue
            try:
                with suppress(Exception):
                    btn.scroll_into_view_if_needed(timeout=1000)
                btn.click(timeout=_scaled_timeout(0.12, 5_000, 20_000))
                log_info("Pesquisar OK")
                return True
            except PWTimeout:
                try:
                    fr.evaluate("(el)=>el.click()", btn)
                    log_info("Pesquisar via JS OK")
                    return True
                except Exception:
                    pass
        return False

    if not _click_pesquisar():
        log_error("Botao 'Pesquisar' nao encontrado.")
        return False

    st = _wait_results(_scaled_timeout(0.30, 12_000, 90_000))
    if st == "timeout":
        log_info("Repetindo clique 'Pesquisar' (timeout)...")
        with suppress(Exception):
            page.wait_for_timeout(300)
        _click_pesquisar()
        st = _wait_results(_scaled_timeout(0.22, 10_000, 70_000))
    if st == "empty":
        log_info("Pesquisa sem linhas. Seguindo normalmente.")
        return True

    IMPR_XP = "xpath=//*[@id and starts-with(@id,'vIMPRIMIR_')]"

    def _page_has_pdf_hint(pg) -> bool:
        try:
            if (pg.url or "").lower().endswith(".pdf") or "pdf" in (pg.url or "").lower():
                return True
        except Exception:
            pass
        try:
            if _find_pdf_urls_in(pg):
                return True
        except Exception:
            pass
        try:
            sel = "embed[type='application/pdf'], object[type='application/pdf'], iframe[src*='.pdf'], pdf-viewer"
            if pg.locator(sel).count() > 0:
                return True
        except Exception:
            pass
        return False

    def _infer_guia_tipo(el) -> Optional[str]:
        if not el:
            return None
        try:
            raw = el.evaluate(
                """
                (el) => {
                  const parts = [];
                  const add = (v) => { if (v) parts.push(v); };
                  add(el.getAttribute('title'));
                  add(el.getAttribute('alt'));
                  add(el.getAttribute('aria-label'));
                  add(el.getAttribute('onclick'));
                  add(el.getAttribute('href'));
                  add(el.textContent);
                  const tr = el.closest('tr');
                  if (tr) add(tr.textContent);
                  const td = el.closest('td');
                  if (td) add(td.textContent);
                  const table = el.closest('table');
                  if (table && table.tHead) add(table.tHead.textContent);
                  return parts.join(' ');
                }
                """
            ) or ""
        except Exception:
            raw = ""
        txt = normalize_text(raw)
        if any(k in txt for k in ("prestado", "prestada", "emitid")):
            return "emitidas"
        if any(k in txt for k in ("tomado", "tomada", "recebid")):
            return "recebidas"
        return None

    def _list_emit_buttons() -> list[tuple[Any, str]]:
        out = []
        for fr in _frames():
            try:
                loc = fr.locator(IMPR_XP)
                count = loc.count()
            except Exception:
                continue
            for i in range(count):
                try:
                    btn_id = loc.nth(i).get_attribute("id")
                except Exception:
                    btn_id = None
                if btn_id:
                    out.append((fr, btn_id))
        return out

    baixados = 0
    processed_ids = set()
    total = 0

    while True:
        buttons = _list_emit_buttons()
        total = max(total, len(buttons))
        pendentes = [(fr, btn_id) for fr, btn_id in buttons if btn_id not in processed_ids]
        if not pendentes:
            break

        for fr, btn_id in pendentes:
            pos = len(processed_ids) + 1
            label = f"linha {pos}/{total} (id='{btn_id}')"
            try:
                el = fr.locator(f"#{btn_id}").first
                if not (el.count() and el.is_visible()):
                    continue

                with suppress(Exception):
                    el.scroll_into_view_if_needed(timeout=800)

                guia_tipo = _infer_guia_tipo(el)
                if guia_tipo:
                    log_info(f"Guia detectada: {guia_tipo} ({label})")

                old_url = page.url
                pop = None
                try:
                    with page.expect_popup(timeout=_scaled_timeout(0.80, 20_000, download_wait_ms)) as pop_info:
                        el.click(timeout=_scaled_timeout(0.15, 6_000, 25_000))
                    pop = pop_info.value
                except Exception:
                    try:
                        with page.expect_download(timeout=_scaled_timeout(0.40, 12_000, download_wait_ms)) as dlp:
                            el.click(timeout=_scaled_timeout(0.12, 4_000, 20_000))
                        dl = dlp.value
                        sug = _normalize_guia_fname(dl.suggested_filename or "guia.pdf", guia_tipo)
                        dest = _ensure_unique(_resolve_target_filepath(_default_download_dir(), page, sug))
                        dl.save_as(str(dest))
                        log_info(f"Download direto: {dest}")
                        baixados += 1
                        continue
                    except Exception:
                        # sem popup/download: tenta capturar PDF via resposta
                        try:
                            resp = page.wait_for_response(
                                lambda r: (
                                    "application/pdf" in (r.headers or {}).get("content-type", "").lower()
                                    or "octet-stream" in (r.headers or {}).get("content-type", "").lower()
                                    or r.url.lower().endswith(".pdf")
                                ),
                                timeout=_scaled_timeout(0.35, 12_000, download_wait_ms),
                            )
                        except Exception:
                            resp = None
                        if resp:
                            got = _download_via_context(page.context, resp.url, guia_tipo=guia_tipo)
                            if not got:
                                data = resp.body()
                                got = _save_bytes_pdf(data, _safe_filename_from_resp(resp), guia_tipo=guia_tipo)
                            if got:
                                log_info(f"PDF salvo via resposta: {got}")
                                baixados += 1
                                continue

                        # tenta processar a pagina atual se virou preview
                        with suppress(Exception):
                            page.wait_for_load_state("domcontentloaded", timeout=_scaled_timeout(0.18, 8_000, 40_000))
                        with suppress(Exception):
                            page.wait_for_load_state("networkidle", timeout=_scaled_timeout(0.15, 6_000, 30_000))
                        preview = _page_has_pdf_hint(page)
                        if not preview:
                            with suppress(Exception):
                                preview = (page.url or "") != (old_url or "")
                        if preview:
                            saved = _process_popup_and_save(page, guia_tipo=guia_tipo)
                            if saved:
                                log_info(f"PDF salvo a partir da pagina atual: {saved}")
                                baixados += 1
                            else:
                                log_info(f"{label} sem popup/download; preview nao gerou PDF.")
                            # tenta voltar para a grade e continuar
                            with suppress(Exception):
                                page.go_back(wait_until="domcontentloaded", timeout=_scaled_timeout(0.22, 8_000, 45_000))
                            with suppress(Exception):
                                _click_pesquisar()
                            with suppress(Exception):
                                _wait_results(_scaled_timeout(0.22, 10_000, 70_000))
                            continue
                        log_info(f"{label} clicado sem popup/download detectado.")
                        continue

                if pop:
                    saved = _process_popup_and_save(pop, guia_tipo=guia_tipo)
                    with suppress(Exception):
                        pop.close()
                    if saved:
                        baixados += 1
                    else:
                        log_info(f"Popup de {label} finalizado sem PDF.")

                with suppress(Exception):
                    page.wait_for_timeout(250)
            except Exception as e:
                log_error(f"Falha ao processar {label}: {e}")
            finally:
                processed_ids.add(btn_id)

    if baixados > 0:
        log_info(f"Downloads concluidos: {baixados} arquivo(s).")
    else:
        log_info("Nenhum PDF salvo (o portal so abriu a pre-visualizacao).")

    return baixados > 0

