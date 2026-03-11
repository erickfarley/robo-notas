# core/empresas_aut.py
from typing import List, Dict, Tuple, Optional
from unicodedata import normalize as _uninorm
import os, json, csv, tempfile, re
from pathlib import Path

from playwright.sync_api import Page
from utils.logger import log_info, log_error
from utils.paths import ensure_dir
from core.captcha import login, manual_login
from core.browser import abrir_empresas_autorizadas
from utils.empresas_json import ler_credencial

# ───────────────────────── utils de texto ─────────────────────────
def _norm(txt: str) -> str:
    if not txt:
        return ""
    t = " ".join(str(txt).split())
    t = _uninorm("NFKD", t).encode("ascii", "ignore").decode("ascii")
    return t.lower().strip()

def _has_letters(s: str) -> bool:
    return bool(re.search(r"[A-Za-zÀ-ÿ]", s or ""))

def _garantir_pasta(path: str):
    pasta = os.path.dirname(path)
    if pasta:
        os.makedirs(pasta, exist_ok=True)

def _salvar_json(empresas: List[Dict], path: str):
    _garantir_pasta(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(empresas, f, ensure_ascii=False, indent=2)

def _salvar_csv(empresas: List[Dict], path: str):
    _garantir_pasta(path)
    cols = ["empresa", "cnpj", "situacao"]
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter=";")
        w.writeheader()
        for e in empresas:
            w.writerow({c: e.get(c, "") for c in cols})

# ───────────────────────── helpers de navegação/debug ─────────────────────────
def _goto_if_blank(page: Page, url: str) -> None:
    """Se a aba estiver em about:blank ou fora do host da NFSe, força um goto."""
    try:
        cur = (page.url or "").strip().lower()
    except Exception:
        cur = ""
    if (not cur) or cur.startswith("about:blank") or "nfse-prd.manaus.am.gov.br" not in cur:
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_load_state("networkidle")

def _shot(page: Page, dbg_dir: Path, name: str) -> Path:
    p = dbg_dir / f"{name}.png"
    try:
        page.screenshot(path=str(p), full_page=True)
    except Exception:
        pass
    return p

# ------------------------------ config helpers ------------------------------
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

# ───────────────────────── helpers de encerramento ────────────────────────────
def _fechar_navegador(page: Page) -> None:
    """Fecha a aba/contexto e, se possível, o browser inteiro."""
    try:
        # fecha a aba primeiro (caso haja múltiplas)
        try:
            page.close()
        except Exception:
            pass

        ctx = None
        try:
            ctx = page.context
        except Exception:
            ctx = None

        if ctx:
            # Fecha o contexto (encerra todas as abas dele)
            try:
                ctx.close()
            except Exception:
                pass
            # Tenta fechar o browser se a API expuser
            try:
                br = getattr(ctx, "browser", None)
                if br:
                    try:
                        br.close()
                    except Exception:
                        pass
            except Exception:
                pass
    except Exception:
        pass

# ─────────── SEMPRE login + abrir Empresas Autorizadas ───────────
def _abrir_empresas_com_login(page: Page) -> None:
    """
    Fluxo robusto:
    1) Garante que não estamos em about:blank (goto base NFSe).
    2) Lê credenciais do JSON (fallback para Excel).
    3) Chama login(...) do core.captcha.
    4) Abre Empresas Autorizadas e aguarda a grade.
    5) Em falha, faz reload, tenta novamente e salva screenshot pro debug.
    """
    dbg_dir = ensure_dir(Path(tempfile.gettempdir()) / "NotaManausRPA" / "debug")
    try:
        page.set_default_navigation_timeout(60_000)
        page.set_default_timeout(60_000)
    except Exception:
        pass

    # Credenciais (JSON → fallback Excel)
    try:
        usuario, senha, _extra = ler_credencial()
    except Exception:
        try:
            from core.excel_utils import ler_excel
            creds = ler_excel()
            if isinstance(creds, (list, tuple)) and len(creds) >= 2:
                usuario, senha = creds[0], creds[1]
            else:
                raise RuntimeError("Formato inesperado retornado por ler_excel().")
        except Exception as exc:
            raise RuntimeError("Não foi possível ler as credenciais (JSON nem Excel).") from exc

    # Garante que saímos do about:blank
    _goto_if_blank(page, "https://nfse-prd.manaus.am.gov.br/nfse/")

    # Login
    log_info("Efetuando login para acessar 'Empresas Autorizadas'…")
    cfg = _read_config_json()
    try:
        if _manual_login_enabled(cfg):
            ok = manual_login(page, usuario, senha, timeout_sec=_manual_login_timeout_sec(cfg), navigate=True)
        else:
            ok = login(page, dbg_dir, usuario, senha)
        if not ok:
            raise RuntimeError("Login nao foi concluido.")
    except Exception as exc:
        snap = _shot(page, dbg_dir, "falha-login")
        raise RuntimeError(f"Falha no login. Veja screenshot: {snap}") from exc

    # Abre Empresas Autorizadas e espera a grade
    log_info("Login realizado. Abrindo 'Empresas Autorizadas'…")
    table_xpath = "//div[@id='EmpresasContainerDiv']//table"
    try:
        abrir_empresas_autorizadas(page)
        page.wait_for_selector("xpath=" + table_xpath, timeout=25_000)
        page.wait_for_selector("xpath=" + table_xpath + "//tr[1]", timeout=25_000)
        return
    except Exception:
        log_info("Recarregando e tentando novamente carregar a grade de empresas…")
        try:
            page.reload(wait_until="domcontentloaded")
        except Exception:
            pass
        try:
            abrir_empresas_autorizadas(page)
            page.wait_for_selector("xpath=" + table_xpath, timeout=35_000)
            page.wait_for_selector("xpath=" + table_xpath + "//tr[1]", timeout=35_000)
            return
        except Exception as exc_second:
            snap = _shot(page, dbg_dir, "falha-empresas-autorizadas")
            cur_url = ""
            try:
                cur_url = page.url
            except Exception:
                pass
            raise RuntimeError(
                f"Não consegui abrir/validar a grade de 'Empresas Autorizadas'. "
                f"URL atual: {cur_url or '(desconhecida)'} | Screenshot: {snap}"
            ) from exc_second

# ─────────── captura rápida do DOM (texto completo) ───────────
def _capturar_headers(page: Page, table_xpath: str) -> List[str]:
    headers = page.eval_on_selector(
        "xpath=" + table_xpath + "//tr[1]",
        r'''
        el => {
            const clean = (s) => (s || '').replace(/\u00A0/g, ' ')
                                          .replace(/\s+/g, ' ')
                                          .trim();
            const cells = Array.from(el.querySelectorAll('th,td'));
            return cells.map(e => clean(e.textContent || e.innerText));
        }
        '''
    )
    return [h if isinstance(h, str) else "" for h in (headers or [])]

def _capturar_linhas(page: Page, table_xpath: str) -> List[List[str]]:
    """
    Monta o texto FINAL de cada <td> percorrendo todos os nós na ordem do DOM
    (inclui ::before/::after). Evita cortes por ellipsis ou nós separados.
    """
    linhas: List[List[str]] = page.eval_on_selector_all(
        "xpath=" + table_xpath + "//tr[position()>1]",
        r'''
        rows => rows.map(tr => {
            const clean = (s) => (s || '').replace(/\u00A0/g,' ').replace(/\s+/g,' ').trim();
            const ELEMENT_NODE = 1, TEXT_NODE = 3;

            function cssContent(el, pseudo){
              try{
                const c = getComputedStyle(el, pseudo).getPropertyValue('content');
                if (!c || c === 'none' || c === 'normal') return '';
                return c.replace(/^["']|["']$/g,'');
              }catch(e){ return ''; }
            }

            function deepText(node){
              if (!node) return '';
              if (node.nodeType === TEXT_NODE) return clean(node.nodeValue);
              if (node.nodeType === ELEMENT_NODE){
                const el = node;
                let out = cssContent(el,'::before');
                const kids = el.childNodes || [];
                for (let i=0;i<kids.length;i++){
                  const part = deepText(kids[i]);
                  if (part) out += (out ? ' ' : '') + part;
                }
                const aft = cssContent(el,'::after');
                if (aft) out += (out ? ' ' : '') + aft;
                return clean(out);
              }
              return '';
            }

            const tds = Array.from(tr.querySelectorAll('td'));
            return tds.map(td => {
              const before = cssContent(td,'::before');
              const body   = deepText(td);
              const after  = cssContent(td,'::after');
              return clean([before, body, after].filter(Boolean).join(' '));
            });
        })
        '''
    )
    return linhas or []

# ─────────── mapeamento robusto por AMOSTRA ───────────
_CNPJ_RE = re.compile(r"\b\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}\b", re.I)
_SIT_WORDS = ("liber", "bloque", "pend", "ativo", "inativo", "susp", "regular")
_NOME_HINTS = (" ltda", " s/a", " eireli", " me", " epp", " sociedade", " serv", " com", " industria", " comércio", " comercio")

def _mapear_por_amostra(linhas: List[List[str]]) -> Dict[str, int]:
    """
    Deduz {empresa, cnpj, situacao} analisando várias linhas.
    empresa: coluna COM letras, maior média de tamanho, hífen/IM/indicadores de razão social
    cnpj: regex
    situacao: palavras-chave
    """
    colmap: Dict[str, int] = {}
    if not linhas:
        return colmap

    # Usa até 30 linhas para estatística
    sample = linhas[: min(len(linhas), 30)]
    ncols = max((len(r) for r in sample), default=0)
    if ncols == 0:
        return colmap

    # stats por coluna
    avg_len   = [0.0]*ncols
    frac_letters = [0.0]*ncols
    frac_cnpj = [0.0]*ncols
    frac_sit  = [0.0]*ncols
    frac_hifen = [0.0]*ncols
    frac_im    = [0.0]*ncols
    frac_nomehint = [0.0]*ncols

    for r in sample:
        for j in range(ncols):
            v = r[j] if j < len(r) else ""
            L = len(v or "")
            avg_len[j] += L
            if _has_letters(v): frac_letters[j] += 1
            if _CNPJ_RE.search(v or ""): frac_cnpj[j] += 1
            t = _norm(v)
            if any(w in t for w in _SIT_WORDS): frac_sit[j] += 1
            if "-" in v: frac_hifen[j] += 1
            if "im:" in t: frac_im[j] += 1
            if any(k in t for k in _NOME_HINTS): frac_nomehint[j] += 1

    m = float(len(sample)) or 1.0
    avg_len   = [x/m for x in avg_len]
    frac_letters = [x/m for x in frac_letters]
    frac_cnpj = [x/m for x in frac_cnpj]
    frac_sit  = [x/m for x in frac_sit]
    frac_hifen = [x/m for x in frac_hifen]
    frac_im    = [x/m for x in frac_im]
    frac_nomehint = [x/m for x in frac_nomehint]

    # CNPJ e Situação por maior fração
    col_cnpj = max(range(ncols), key=lambda j: frac_cnpj[j], default=-1)
    if frac_cnpj[col_cnpj] > 0: colmap["cnpj"] = col_cnpj + 1

    col_sit = max(range(ncols), key=lambda j: frac_sit[j], default=-1)
    if frac_sit[col_sit] > 0: colmap["situacao"] = col_sit + 1

    # Empresa: score ponderado
    def score_emp(j: int) -> float:
        # penaliza colunas que parecem CNPJ/Situação
        pen = 0.0
        pen += 1.0 if j == col_cnpj else 0.0
        pen += 1.0 if j == col_sit  else 0.0
        # evita coluna só numérica
        base = avg_len[j] + 30*frac_letters[j] + 10*frac_hifen[j] + 15*frac_im[j] + 15*frac_nomehint[j]
        return base - 100*pen

    col_emp = max(range(ncols), key=score_emp, default=-1)
    if col_emp >= 0: colmap["empresa"] = col_emp + 1

    log_info(f"Mapeamento por amostra (0-based): EMP={col_emp}, CNPJ={col_cnpj}, SIT={col_sit}")
    return colmap

# ─────────── composição por linha (IM + Razão) ───────────
def _compose_empresa(cols: List[str], colmap: Dict[str, int]) -> str:
    """Se a coluna empresa escolhida vier sem letras, tenta compor 'IM: 123 - RAZÃO'."""
    idx_emp = (colmap.get("empresa", 0) or 0) - 1
    idx_cnpj = (colmap.get("cnpj", 0) or 0) - 1
    idx_sit  = (colmap.get("situacao", 0) or 0) - 1

    cand = cols[idx_emp] if 0 <= idx_emp < len(cols) else ""

    if _has_letters(cand):
        return cand.strip()

    # procurar melhor "nome" na linha
    nomes = []
    ims   = []
    for j, v in enumerate(cols):
        if j in (idx_emp, idx_cnpj, idx_sit):
            continue
        t = v or ""
        tn = _norm(t)
        if _CNPJ_RE.search(t):
            continue
        if any(w in tn for w in _SIT_WORDS):
            continue
        if _has_letters(t):
            nomes.append(t.strip())
        # candidatos a IM/código
        if "im:" in tn or re.search(r"^\d{6,}$", t.strip()):
            ims.append(t.strip())

    nome = max(nomes, key=lambda s: len(s.replace("…","")), default="").strip()
    im   = ims[0] if ims else ""

    if nome and im:
        # normaliza prefixo "IM:"
        if not re.search(r"im\s*:", _norm(im)):
            im = f"IM: {im}"
        return f"{im} - {nome}".strip()
    if nome:
        return nome
    return cand.strip()

# ───────────────────── coleta da grade completa ────────────────────
def listar_empresas_autorizadas(page: Page) -> Tuple[List[Dict], Dict[str, int]]:
    table_xpath = "//div[@id='EmpresasContainerDiv']//table"

    _abrir_empresas_com_login(page)

    # Grace + tentativas (grids lentas)
    page.wait_for_timeout(600)
    tentativas = 0
    headers: List[str] = []
    linhas: List[List[str]] = []
    while tentativas < 10:
        try:
            headers = _capturar_headers(page, table_xpath)
            linhas  = _capturar_linhas(page,  table_xpath)
            if linhas:
                break
        except Exception:
            pass
        tentativas += 1
        page.wait_for_timeout(400)

    log_info(f"🏢 Linhas na grade: {len(linhas)}")

    # 1) Tenta mapear por headers
    colmap: Dict[str, int] = {}
    for idx, txt in enumerate(headers or [], start=1):
        t = _norm(txt)
        if "empresa" in t or "razao" in t or "razao social" in t or "nome" in t:
            colmap["empresa"] = idx
        if "cnpj" in t:
            colmap["cnpj"] = idx
        if "situacao" in t or "situa" in t or "status" in t:
            colmap["situacao"] = idx

    # 2) Complementa/ajusta com amostra (robusto)
    by_sample = _mapear_por_amostra(linhas)
    for k, v in by_sample.items():
        colmap[k] = v

    if not colmap.get("empresa") or not colmap.get("situacao"):
        raise RuntimeError(f"Cabeçalhos/colunas não identificados (Empresa/Situação). colmap={colmap}")

    # 3) Constrói saída com composição linha-a-linha
    out: List[Dict] = []
    total = len(linhas)
    for i, cols in enumerate(linhas, start=1):
        # Empresa com composição inteligente
        empresa = _compose_empresa(cols, colmap)

        # CNPJ/Situação “normais”
        def _val(idx1: int) -> str:
            if not idx1 or idx1 <= 0: return ""
            i0 = idx1 - 1
            return cols[i0] if i0 < len(cols) else ""

        cnpj     = _val(colmap.get("cnpj", 0)).strip()
        situacao = _val(colmap.get("situacao", 0)).strip()

        if empresa or cnpj or situacao:
            out.append({"empresa": empresa, "cnpj": cnpj, "situacao": situacao})

        if i % 50 == 0 or i == total:
            log_info(f" • Processadas {i}/{total} linhas…")

    # Amostra no log
    for e in out[:5]:
        log_info(f" • {e.get('empresa','')}  |  {e.get('cnpj','')}  |  {e.get('situacao','')}")

    return out, colmap

# ───────────────────── fluxo principal exportado ───────────────────
def carregar_empresas_liberadas(
    page: Page,
    out_json: str = "./data/empresas_liberadas.json",
    out_csv: str = "./data/empresas_liberadas.csv",
    fechar_navegador: bool = True,
) -> List[Dict]:
    """
    Carrega a grade, filtra 'Liberado', exporta JSON/CSV e, por padrão, fecha o navegador.
    """
    try:
        todas, _ = listar_empresas_autorizadas(page)
        liberadas = [e for e in todas if "liberado" in _norm(e.get("situacao", ""))]
        log_info(f"✅ Empresas com situação 'Liberado': {len(liberadas)}")
        try:
            _salvar_json(liberadas, out_json)
            _salvar_csv(liberadas, out_csv)
            log_info(f"Arquivos salvos: {out_json} e {out_csv}")
        except Exception as exc:
            log_error(f"Falha ao salvar arquivos: {exc}")
        return liberadas
    finally:
        if fechar_navegador:
            log_info("Encerrando navegador…")
            _fechar_navegador(page)
