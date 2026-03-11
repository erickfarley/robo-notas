# core/captcha.py
# Login robusto: re-tentativas de captcha + detecção correta de sucesso

import hashlib
import re
import time
import pathlib
import random
from contextlib import suppress
from typing import Tuple, Optional

from playwright.sync_api import TimeoutError as PWTimeout
from utils.logger import log_info, log_error, log_warning

from core.selectors import (
    X_LOGIN_CPF, X_LOGIN_SENHA, X_LOGIN_BTN,
    X_MENU_NOTAS, X_MENU_PERFIL, X_MENU_MOVIMENTACOES,
    CAPTCHA_IMG_SELECTORS, CAPTCHA_INPUT_SELECTORS
)
from config            import MAX_TENTATIVAS_CAPTCHA, URL
from utils.paths       import save

LOGIN_READY_TIMEOUT_MS = 8000
LOGIN_CHECK_INTERVAL = 0.2
POST_LOGIN_SELECTORS = [X_MENU_NOTAS, X_MENU_PERFIL, X_MENU_MOVIMENTACOES]
CAPTCHA_READY_TIMEOUT_MS = 6000
CAPTCHA_STABILIZE_DELAY_SEC = 0.9
CAPTCHA_PAGE_LOAD_DELAY_SEC = 1.2
CAPTCHA_PRE_SUBMIT_DELAY_SEC = 0.25
CAPTCHA_BETWEEN_ATTEMPTS_DELAY_SEC = 0.2
CAPTCHA_AFTER_RELOAD_DELAY_SEC = 0.8
CAPTCHA_PREFERRED_LENGTHS = (4,)
MAX_CODES_PER_CYCLE = 3
MAX_CODE_VARIANTS = 3
CAPTCHA_MIN_LEN = 4
CAPTCHA_MAX_LEN = 4
AMBIGUOUS_SWAP = {
    "0": "O",
    "O": "0",
    "1": "I",
    "I": "1",
    "L": "1",
    "2": "Z",
    "Z": "2",
    "5": "S",
    "S": "5",
    "6": "G",
    "G": "6",
    "7": "T",
    "T": "7",
    "8": "B",
    "B": "8",
}

def _is_logged_in(page) -> bool:
    for sel in POST_LOGIN_SELECTORS:
        try:
            if page.locator(sel).first.is_visible():
                return True
        except Exception:
            continue
    return False


# ────────────────────────── helpers ──────────────────────────
def _on_login_screen(page) -> bool:
    """
    True - ainda na tela de login (ou erro de autenticacao).
    False - login aceito (tela interna).
    """
    if _is_logged_in(page):
        return False

    try:
        url = (page.url or "").lower()
        if any(token in url for token in ("login", "logon", "autent", "auth")):
            return True
    except Exception:
        pass

    try:
        if page.locator(X_LOGIN_CPF).first.is_visible():
            return True
        if page.locator(X_LOGIN_SENHA).first.is_visible():
            return True
        if page.locator(X_LOGIN_BTN).first.is_visible():
            return True
    except Exception:
        pass

    return False

def is_on_login_screen(page) -> bool:
    """Wrapper público para detectar se a tela atual é a de login."""
    return _on_login_screen(page)


def _await_exit_login(page, timeout_ms: int = LOGIN_READY_TIMEOUT_MS) -> bool:
    """
    Aguarda sair da tela de login usando _on_login_screen (polling leve).
    """
    deadline = time.time() + timeout_ms / 1000.0
    while time.time() < deadline:
        if _is_logged_in(page):
            return True
        if _on_login_screen(page):
            time.sleep(LOGIN_CHECK_INTERVAL)
            continue
        time.sleep(LOGIN_CHECK_INTERVAL)
    return _is_logged_in(page)

def _is_server_error_page(page) -> bool:
    try:
        title = (page.title() or "").lower()
        if "error report" in title or "internal server error" in title:
            return True
    except Exception:
        pass
    try:
        loc = page.locator("text=HTTP Status 500").first
        if loc.count():
            return True
    except Exception:
        pass
    try:
        html = page.content()
        if "HTTP Status 500" in html or "Internal Server Error" in html:
            return True
    except Exception:
        pass
    return False

def _goto_login_with_backoff(page, url: str, attempts: int = 5) -> bool:
    delay = 2.0
    for i in range(1, max(1, int(attempts)) + 1):
        resp = None
        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=20_000)
        except Exception as e:
            log_error(f"Falha ao abrir login (tentativa {i}/{attempts}): {e}")
        status = None
        try:
            status = resp.status if resp else None
        except Exception:
            status = None
        if status and status >= 500:
            log_warning(f"Login retornou HTTP {status}. Aguardando {delay:.1f}s e tentando novamente...")
        elif _is_server_error_page(page):
            log_warning(f"Pagina de erro 500 no login. Aguardando {delay:.1f}s e tentando novamente...")
        else:
            return True
        time.sleep(delay + random.uniform(0.2, 1.0))
        delay = min(delay * 2.0, 20.0)
    return False

def _find_captcha(page) -> Tuple[Optional[object], Optional[object]]:
    """Retorna (img, input) do captcha ou (None, None)."""
    img = inp = None
    for sel in CAPTCHA_IMG_SELECTORS:
        loc = page.locator(sel).first
        if loc.count() and loc.is_visible():
            img = loc
            break
    for sel in CAPTCHA_INPUT_SELECTORS:
        loc = page.locator(sel).first
        if loc.count() and loc.is_visible():
            inp = loc
            break
    # Prioriza explicitamente o campo informado pelo usuario.
    try:
        vvalor = page.locator("#vVALORIMAGEM").first
        if vvalor.count() and vvalor.is_visible():
            inp = vvalor
    except Exception:
        pass
    return img, inp


def _fill_captcha_input(inp, value: str) -> bool:
    code = (value or "").strip()
    if not code:
        return False
    try:
        with suppress(Exception):
            inp.scroll_into_view_if_needed(timeout=1000)
        with suppress(Exception):
            inp.click(timeout=1500)
        with suppress(Exception):
            inp.fill("")
        with suppress(Exception):
            inp.press("Control+A")
            inp.press("Backspace")
        with suppress(Exception):
            inp.type(code, delay=35)

        typed = ""
        with suppress(Exception):
            typed = (inp.input_value() or "").strip()
        if typed and typed == code:
            return True

        # Fallback definitivo: seta valor por JS e dispara eventos.
        with suppress(Exception):
            inp.evaluate(
                """(el, v) => {
                    el.value = v;
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }""",
                code,
            )
        with suppress(Exception):
            typed = (inp.input_value() or "").strip()
        return bool(typed == code)
    except Exception:
        return False


def _ensure_login_fields(page, usuario: str, senha: str) -> None:
    """
    Garante que os campos de usuario e senha estejam preenchidos
    antes de cada envio de login/captcha.
    """
    user = (usuario or "").strip()
    pwd = str(senha or "")
    if not user or not pwd:
        return
    with suppress(Exception):
        page.fill(X_LOGIN_CPF, user)
    with suppress(Exception):
        page.fill(X_LOGIN_SENHA, pwd)


def _wait_for_captcha_ready(page, img) -> None:
    try:
        img.wait_for(state="visible", timeout=CAPTCHA_READY_TIMEOUT_MS)
    except Exception:
        pass
    try:
        page.wait_for_function(
            "img => img && img.complete && img.naturalWidth > 0",
            img,
            timeout=CAPTCHA_READY_TIMEOUT_MS
        )
    except Exception:
        pass
    time.sleep(CAPTCHA_STABILIZE_DELAY_SEC)

def _wait_for_captcha_presence(page, timeout_ms: int = 2000) -> bool:
    deadline = time.time() + timeout_ms / 1000.0
    while time.time() < deadline:
        img, inp = _find_captcha(page)
        if img and inp:
            return True
        time.sleep(0.2)
    return False

def _captcha_sig(raw: bytes) -> str:
    return hashlib.md5(raw).hexdigest()

def _normalize_captcha_code(code: Optional[str]) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", code or "").upper()


def _valid_captcha_code(code: Optional[str]) -> bool:
    if not code:
        return False
    code = _normalize_captcha_code(code)
    return CAPTCHA_MIN_LEN <= len(code) <= CAPTCHA_MAX_LEN

def _rank_codes(codes) -> list:
    cleaned = []
    counts = {}
    for code in codes or []:
        code = _normalize_captcha_code(code)
        if not _valid_captcha_code(code):
            continue
        counts[code] = counts.get(code, 0) + 1
        if code not in cleaned:
            cleaned.append(code)
    if not cleaned:
        return []

    def _len_rank(code: str) -> int:
        if len(code) in CAPTCHA_PREFERRED_LENGTHS:
            return CAPTCHA_PREFERRED_LENGTHS.index(len(code))
        return len(CAPTCHA_PREFERRED_LENGTHS) + len(code)

    return sorted(
        cleaned,
        key=lambda c: (-counts[c], _len_rank(c), cleaned.index(c)),
    )

def _expand_code_variants(code: str) -> list:
    code = _normalize_captcha_code(code)
    if not _valid_captcha_code(code):
        return []

    variants = [code]
    if any(ch.isalpha() for ch in code):
        variants.append(code.lower())
    chars = list(code)
    amb_positions = [idx for idx, ch in enumerate(chars) if ch in AMBIGUOUS_SWAP]

    # 1) troca global de ambiguos
    if amb_positions:
        swapped = chars[:]
        for idx in amb_positions:
            swapped[idx] = AMBIGUOUS_SWAP[swapped[idx]]
        swapped_code = "".join(swapped)
        if _valid_captcha_code(swapped_code):
            variants.append(swapped_code)

    # 2) trocas por posicao (gera alternativas pontuais)
    for idx in amb_positions:
        one = chars[:]
        one[idx] = AMBIGUOUS_SWAP[one[idx]]
        one_code = "".join(one)
        if _valid_captcha_code(one_code):
            variants.append(one_code)
            if any(ch.isalpha() for ch in one_code):
                variants.append(one_code.lower())
        if len(dict.fromkeys(variants)) >= MAX_CODE_VARIANTS:
            break

    return list(dict.fromkeys(variants))[:MAX_CODE_VARIANTS]


# ─────────────────────── resolver captcha ───────────────────────
def _solve_captcha(page, dbg_dir: pathlib.Path,
                   usuario: str, senha: str) -> bool:
    """
    Tenta varios ciclos de captcha. Em cada ciclo:
      - screenshot do captcha (debug)
      - tenta OCR remoto (N vezes)
      - fallback Tesseract
      - clica Entrar e verifica se saiu do login
    True = logado; False = esgotou tentativas.
    """
    last_sig = None
    for ciclo in range(1, MAX_TENTATIVAS_CAPTCHA + 1):
        tag = f"c{ciclo}_{int(time.time())}"
        log_info(f"Captcha ciclo {ciclo}/{MAX_TENTATIVAS_CAPTCHA}")

        try:
            page.wait_for_selector(X_LOGIN_CPF, timeout=LOGIN_READY_TIMEOUT_MS)
        except PWTimeout:
            pass

        time.sleep(CAPTCHA_PAGE_LOAD_DELAY_SEC)
        _ensure_login_fields(page, usuario, senha)

        img, inp = _find_captcha(page)
        if not (img and inp):
            log_info("Captcha nao visivel; recarregando para novo desafio.")
            page.reload(wait_until="domcontentloaded")
            continue

        _wait_for_captcha_ready(page, img)
        raw = img.screenshot()
        sig = _captcha_sig(raw)
        if sig == last_sig:
            log_info("Captcha repetido; recarregando para novo desafio.")
            page.reload(wait_until="domcontentloaded")
            continue
        last_sig = sig
        save(dbg_dir / f"captcha_{tag}.png", raw)

        # Tesseract primeiro (mais rapido); OpenRouter entra como reforco.
        log_info("  Tesseract local")
        try:
            from core.ocr_fallback import ocr_tesseract_quick
        except Exception:
            ocr_tesseract_quick = None
        code = ocr_tesseract_quick(raw) if ocr_tesseract_quick else ""
        if _valid_captcha_code(code):
            for attempt_code in _expand_code_variants(code):
                _ensure_login_fields(page, usuario, senha)
                if not _fill_captcha_input(inp, attempt_code):
                    log_warning(f"  Falha ao digitar captcha no campo vVALORIMAGEM ({attempt_code})")
                    img, inp = _find_captcha(page)
                    if not (img and inp):
                        break
                    continue
                time.sleep(CAPTCHA_PRE_SUBMIT_DELAY_SEC)
                page.click(X_LOGIN_BTN)
                try:
                    page.wait_for_load_state("domcontentloaded", timeout=LOGIN_READY_TIMEOUT_MS)
                except PWTimeout:
                    pass

                if _await_exit_login(page, timeout_ms=LOGIN_READY_TIMEOUT_MS):
                    log_info(f"  Captcha aceito (Tesseract {attempt_code})")
                    return True

        try:
            from core.ocr_remote import ocr_openrouter_candidates
        except Exception:
            ocr_openrouter_candidates = None
        if ocr_openrouter_candidates:
            codes = ocr_openrouter_candidates(raw, dbg_dir, tag)
        else:
            codes = []
        ranked = _rank_codes(codes)
        if ranked:
            log_info("  OpenRouter candidatos: " + ", ".join(ranked[:MAX_CODES_PER_CYCLE]))
        else:
            log_info("  OpenRouter sem candidato valido.")

        for code in ranked[:MAX_CODES_PER_CYCLE]:
            for attempt_code in _expand_code_variants(code):
                _ensure_login_fields(page, usuario, senha)
                if not _fill_captcha_input(inp, attempt_code):
                    log_warning(f"  Falha ao digitar captcha no campo vVALORIMAGEM ({attempt_code})")
                    img, inp = _find_captcha(page)
                    if not (img and inp):
                        break
                    continue
                time.sleep(CAPTCHA_PRE_SUBMIT_DELAY_SEC)
                page.click(X_LOGIN_BTN)
                try:
                    page.wait_for_load_state("domcontentloaded", timeout=LOGIN_READY_TIMEOUT_MS)
                except PWTimeout:
                    pass

                if _await_exit_login(page, timeout_ms=LOGIN_READY_TIMEOUT_MS):
                    log_info(f"  Captcha aceito ({attempt_code})")
                    return True

                log_info("  Codigo rejeitado / permaneceu na tela de login")
                img, inp = _find_captcha(page)
                if not (img and inp):
                    break
            time.sleep(CAPTCHA_BETWEEN_ATTEMPTS_DELAY_SEC)

        page.reload(wait_until="domcontentloaded")
        time.sleep(CAPTCHA_AFTER_RELOAD_DELAY_SEC)

    return False

def login(page, dbg_dir: pathlib.Path,
          usuario: str, senha: str) -> bool:
    """
    Executa o login completo:
      - Com captcha: resolve ate sair do login
      - Sem captcha: login direto e valida saida
      - Se falhar, recarrega e tenta novamente (ate MAX_TENTATIVAS_CAPTCHA)
    """
    log_info("Abrindo tela de login.")
    if not _goto_login_with_backoff(page, URL, attempts=5):
        log_error("Nao foi possivel abrir a tela de login (HTTP 500).")
        return False
    time.sleep(CAPTCHA_PAGE_LOAD_DELAY_SEC)

    for tentativa in range(1, MAX_TENTATIVAS_CAPTCHA + 1):
        try:
            page.wait_for_selector(X_LOGIN_CPF, timeout=LOGIN_READY_TIMEOUT_MS)
        except PWTimeout:
            pass
        img, inp = _find_captcha(page)

        if img and inp:
            ok = _solve_captcha(page, dbg_dir, usuario, senha)
            if ok:
                log_info("Login OK")
                return True
            log_error("Falha ao resolver captcha neste ciclo.")
        else:
            _wait_for_captcha_presence(page, timeout_ms=2000)
            img, inp = _find_captcha(page)
            if img and inp:
                ok = _solve_captcha(page, dbg_dir, usuario, senha)
                if ok:
                    log_info("Login OK")
                    return True
                log_error("Falha ao resolver captcha neste ciclo.")
                page.reload(wait_until="domcontentloaded")
                time.sleep(CAPTCHA_AFTER_RELOAD_DELAY_SEC)
                continue

            _ensure_login_fields(page, usuario, senha)
            page.click(X_LOGIN_BTN)
            try:
                page.wait_for_load_state("domcontentloaded", timeout=LOGIN_READY_TIMEOUT_MS)
            except PWTimeout:
                pass

            if _await_exit_login(page, timeout_ms=LOGIN_READY_TIMEOUT_MS):
                log_info("Login OK (sem captcha)")
                return True

            log_info("Tela de login persiste; possivelmente passou a exigir captcha.")

        page.reload(wait_until="domcontentloaded")
        time.sleep(CAPTCHA_AFTER_RELOAD_DELAY_SEC)

    log_error("Login nao foi concluido apos multiplas tentativas.")
    return False

def manual_login(page, usuario: str, senha: str, timeout_sec: int = 900, navigate: bool = True) -> bool:
    """
    Aguarda o login manual no navegador.
    - Se navigate=True, abre a URL de login antes de esperar.
    - Usuario/senha sao preenchidos automaticamente; captcha e manual.
    """
    if navigate:
        try:
            log_info("Abrindo tela de login (manual).")
            if not _goto_login_with_backoff(page, URL, attempts=5):
                log_error("Nao foi possivel abrir a tela de login (HTTP 500).")
                return False
            time.sleep(CAPTCHA_PAGE_LOAD_DELAY_SEC)
        except Exception:
            pass

    try:
        page.wait_for_selector(X_LOGIN_CPF, timeout=LOGIN_READY_TIMEOUT_MS)
    except Exception:
        pass

    try:
        cur_user = ""
        cur_pass = ""
        try:
            cur_user = page.input_value(X_LOGIN_CPF)
        except Exception:
            pass
        try:
            cur_pass = page.input_value(X_LOGIN_SENHA)
        except Exception:
            pass

        if usuario and not (cur_user or "").strip():
            page.fill(X_LOGIN_CPF, usuario)
        if senha and not (cur_pass or "").strip():
            page.fill(X_LOGIN_SENHA, senha)
    except Exception:
        pass

    log_info("Usuario/senha preenchidos. Resolva o captcha e clique em Entrar.")
    timeout_ms = max(1, int(timeout_sec)) * 1000
    if _await_exit_login(page, timeout_ms=timeout_ms):
        log_info("Login OK (manual)")
        return True

    log_error("Tempo limite aguardando login manual.")
    return False

