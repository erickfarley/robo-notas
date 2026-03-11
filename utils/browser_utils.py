"""
Helper functions for browser automation in Nota Manaus portal. (fast)
"""
from contextlib import suppress
from datetime import datetime
from unicodedata import normalize as _uninorm
import re
from pathlib import Path
from typing import List, Optional, Tuple
from playwright.sync_api import Page, TimeoutError as PWTimeout

from utils.logger import log_info, log_error
from utils.periodo import get_periodo

# -------------------- timeouts curtos para operações comuns --------------------
TINY   =  600   # ms
SHORT  = 2000   # ms
NORMAL = 5000   # ms

# Mapping month -> name (for selects by label if value doesn't match)
_MESES_PT = {
    "01":"Janeiro","02":"Fevereiro","03":"Março","04":"Abril","05":"Maio","06":"Junho",
    "07":"Julho","08":"Agosto","09":"Setembro","10":"Outubro","11":"Novembro","12":"Dezembro"
}

# regex compilada para normalização mais rápida
_WS = re.compile(r"\s+")

# ------------------------------ utils de período ------------------------------
def get_periodo_dict() -> dict:
    """Get period chosen in GUI; fallback = current month/year. (rápido)"""
    try:
        p = get_periodo()  # {"mes_de","ano_de","mes_ate","ano_ate"}
        if isinstance(p, dict) and all(k in p for k in ("mes_de","ano_de","mes_ate","ano_ate")):
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

def get_periodo_de() -> dict:
    try:
        p = get_periodo()
        return {"mm": f"{int(p['mes_de']):02d}", "yy": f"{int(p['ano_de']):04d}"}
    except Exception:
        now = datetime.now()
        return {"mm": f"{now.month:02d}", "yy": f"{now.year:04d}"}

# ------------------------------ normalização rápida ---------------------------
def normalize_text(s: str) -> str:
    """Normalize text removing accents and extra spaces (rápido)."""
    s = _uninorm("NFKD", s or "").encode("ASCII", "ignore").decode("ASCII")
    return _WS.sub(" ", s).strip().lower()

# ------------------------------ DOM helpers rápidos ---------------------------
def is_select_element(elem) -> bool:
    """Check if element is a select tag (1 evaluate)."""
    try:
        return bool(elem.evaluate("el => !!el && el.tagName?.toLowerCase()==='select'"))
    except Exception:
        return False

def get_selected_text(elem) -> str:
    """Get currently selected visible text (or value if not select), fast-path em JS."""
    try:
        return elem.evaluate("""
            (el)=>{
               if (!el) return '';
               const tag = el.tagName ? el.tagName.toLowerCase() : '';
               if (tag === 'select') {
                   const opt = el.selectedOptions && el.selectedOptions[0];
                   return (opt && (opt.textContent || opt.label) || '').trim();
               }
               const v = ('value' in el && el.value != null) ? String(el.value) : '';
               return (v || (el.textContent || '')).trim();
            }
        """)
    except Exception:
        try:
            return elem.input_value(timeout=TINY)
        except Exception:
            return ""

# ------------------------------ seleção rápida mês/ano ------------------------
def select_month(elem, mm: str) -> bool:
    """Select month in dropdown or input, minimizando round-trips."""
    mm = f"{int(mm):02d}"
    label = _MESES_PT.get(mm, mm)
    lbl_norm = normalize_text(label)
    mm_norm  = normalize_text(mm)

    # Caminho principal: tudo em um evaluate
    try:
        result = elem.evaluate("""
            (el, mm, label)=>{
              const norm = (s)=> (s||'')
                .normalize('NFKD').replace(/[\\u0300-\\u036f]/g,'')
                .replace(/\\s+/g,' ').trim().toLowerCase();

              if (!el) return {ok:false, sel:''};
              const tag = (el.tagName||'').toLowerCase();

              if (tag === 'select') {
                 const opts = Array.from(el.options||[]);
                 // 1) value exato
                 let tgt = opts.find(o => (o.value||'') === mm);
                 // 2) label/text exato
                 if (!tgt) tgt = opts.find(o => norm(o.textContent||o.label||'') === norm(label));
                 // 3) label/text contém
                 if (!tgt) tgt = opts.find(o => norm(o.textContent||o.label||'').includes(norm(label)));
                 if (!tgt) return {ok:false, sel:''};

                 const val = tgt.value || tgt.textContent || '';
                 el.value = String(val);
                 el.dispatchEvent(new Event('input',  {bubbles:true}));
                 el.dispatchEvent(new Event('change', {bubbles:true}));

                 const opt = el.selectedOptions && el.selectedOptions[0];
                 return {ok:true, sel:(opt && (opt.textContent||opt.label)||'').trim()};
              }

              // input texto
              const v = label;
              if ('value' in el) el.value = v;
              el.dispatchEvent(new Event('input',  {bubbles:true}));
              el.dispatchEvent(new Event('change', {bubbles:true}));
              return {ok:true, sel:v};
            }
        """, mm, label)

        sel = result.get("sel", "") if isinstance(result, dict) else ""
        ok = (isinstance(result, dict) and result.get("ok") and
              normalize_text(sel) in (lbl_norm, mm_norm))
        if not ok:
            log_error(f"⚠ month not confirmed: expected '{label}'/{mm}, got '{sel}'")
        return bool(ok)

    except Exception:
        # Fallbacks rápidos (timeouts curtos)
        try:
            elem.select_option(value=mm, timeout=TINY)
        except Exception:
            try:
                elem.select_option(label=label, timeout=TINY)
            except Exception:
                try:
                    elem.fill(label, timeout=TINY)
                except Exception:
                    return False

        sel = get_selected_text(elem)
        ok = normalize_text(sel) in (lbl_norm, mm_norm)
        if not ok:
            log_error(f"⚠ month not confirmed: expected '{label}'/{mm}, got '{sel}'")
        return ok

def select_year(elem, yy: str) -> bool:
    """Select year in dropdown or input, minimizando round-trips."""
    yy = f"{int(yy)}"
    yy_norm = normalize_text(yy)

    try:
        result = elem.evaluate("""
            (el, yy)=>{
              const norm = (s)=> (s||'')
                .normalize('NFKD').replace(/[\u0300-\u036f]/g,'')
                .replace(/\s+/g,' ').trim().toLowerCase();
              if (!el) return {ok:false, sel:''};
              const tag = (el.tagName||'').toLowerCase();

              if (tag === 'select') {
                 const opts = Array.from(el.options||[]);
                 let tgt = opts.find(o => (o.value||'') === yy)
                         || opts.find(o => norm(o.textContent||o.label||'') === norm(yy))
                         || opts.find(o => (o.value||'').includes(yy))
                         || opts.find(o => norm(o.textContent||o.label||'').includes(norm(yy)));
                 if (!tgt) return {ok:false, sel:''};
                 const val = tgt.value || tgt.textContent || '';
                 el.value = String(val);
                 el.dispatchEvent(new Event('input',  {bubbles:true}));
                 el.dispatchEvent(new Event('change', {bubbles:true}));

                 const opt = el.selectedOptions && el.selectedOptions[0];
                 return {ok:true, sel:(opt && (opt.textContent||opt.label)||'').trim()};
              }

              if ('value' in el) el.value = String(yy);
              el.dispatchEvent(new Event('input',  {bubbles:true}));
              el.dispatchEvent(new Event('change', {bubbles:true}));
              return {ok:true, sel:String(yy)};
            }
        """, yy)

        sel = result.get("sel", "") if isinstance(result, dict) else ""
        ok = (isinstance(result, dict) and result.get("ok") and
              (normalize_text(sel) == yy_norm or yy_norm in normalize_text(sel)))
        if not ok:
            log_error(f"?s? year not confirmed: expected '{yy}', got '{sel}'")
        return bool(ok)

    except Exception:
        # fallbacks r??pidos
        if is_select_element(elem):
            try:
                elem.select_option(value=yy, timeout=TINY)
            except Exception:
                try:
                    elem.select_option(label=yy, timeout=TINY)
                except Exception:
                    try:
                        elem.evaluate("""
                            (el, yy)=>{
                              const norm = (s)=> (s||'')
                                .normalize('NFKD').replace(/[\u0300-\u036f]/g,'')
                                .replace(/\s+/g,' ').trim().toLowerCase();
                              if (!el) return false;
                              const tag = (el.tagName||'').toLowerCase();
                              if (tag !== 'select') return false;
                              const opts = Array.from(el.options||[]);
                              const tgt = opts.find(o => (o.value||'').includes(yy) || norm(o.textContent||o.label||'').includes(norm(yy)));
                              if (!tgt) return false;
                              el.value = String(tgt.value || tgt.textContent || '');
                              el.dispatchEvent(new Event('input',  {bubbles:true}));
                              el.dispatchEvent(new Event('change', {bubbles:true}));
                              return true;
                            }
                        """, yy)
                    except Exception:
                        try:
                            elem.fill(yy, timeout=TINY)
                        except Exception:
                            return False
        else:
            try:
                elem.fill(yy, timeout=TINY)
            except Exception:
                return False

        sel = get_selected_text(elem)
        ok = normalize_text(sel) == yy_norm or yy_norm in normalize_text(sel)
        if not ok:
            log_error(f"?s? year not confirmed: expected '{yy}', got '{sel}'")
        return ok

def _fast_get_visible(fr, selector: str):
    """Retorna locator visível com timeout curto, ou None."""
    try:
        return fr.wait_for_selector(selector, state="visible", timeout=TINY)
    except Exception:
        return None

def find_period_inputs_in_frame(fr) -> Tuple[Optional[object], Optional[object], Optional[object], Optional[object]]:
    """Returns (mes_de, ano_de, mes_ate, ano_ate) as Locators (or None)."""
    # 1) CSS por id (mais rápido que XPath) — visíveis
    md = _fast_get_visible(fr, "#vMESINICIO")
    yd = _fast_get_visible(fr, "#vANOINICIO") or _fast_get_visible(fr, "#vANOSEMMOV1")
    ma = _fast_get_visible(fr, "#vMESFINAL")
    ya = _fast_get_visible(fr, "#vANOFINAL") or _fast_get_visible(fr, "#vANOSEMMOV2")
    if any((md, yd, ma, ya)):
        return md, yd, ma, ya

    # 1b) somente ano (sem mes)
    y_only = (
        _fast_get_visible(fr, "#vANOSEMMOV") or
        _fast_get_visible(fr, "#vANO") or
        _fast_get_visible(fr, "[name='vANO']")
    )
    if y_only:
        return None, y_only, None, None

    # 2) fallback por XPath (curto)
    def _get(xp: str):
        try:
            loc = fr.locator(f"xpath={xp}").first
            return loc if (loc.count() and loc.is_visible()) else None
        except Exception:
            return None

    md  = _get('//*[@id="vMESINICIO"]')
    yd  = _get('//*[@id="vANOINICIO" or @id="vANOSEMMOV1"]')
    ma  = _get('//*[@id="vMESFINAL"]')
    ya  = _get('//*[@id="vANOFINAL" or @id="vANOSEMMOV2"]')
    if any((md, yd, ma, ya)):
        return md, yd, ma, ya

    # 3) fallback opcional: primeiros 4 selects do form (sem .all() caro)
    try:
        sel = fr.locator("css=form select")
        n = sel.count()
        if n >= 4:
            return sel.nth(0), sel.nth(1), sel.nth(2), sel.nth(3)
    except Exception:
        pass
    return None, None, None, None

# --------------------------- dialogo/modal & pos-clique -----------------------
def _remove_dialog_listener(page: Page, handler) -> None:
    try:
        if hasattr(page, "off"):
            page.off("dialog", handler)
            return
    except Exception:
        pass
    with suppress(Exception):
        page.remove_listener("dialog", handler)

def handle_dialog_and_modal(page: Page, link, timeout: int = SHORT):
    """Handle JS dialog + HTML modal 'OK' após clicar. Mais rápido e não bloqueante."""
    # aceitar qualquer dialog instantaneamente
    def _on_dialog(dlg):
        try:
            log_info("↳ diálogo JS – aceitando.")
            dlg.accept()
        except Exception:
            pass

    page.on("dialog", _on_dialog)

    # clique sem esperar navegação automaticamente
    try:
        with suppress(Exception):
            link.scroll_into_view_if_needed(timeout=TINY)
        with suppress(Exception):
            link.focus()
        link.click(timeout=SHORT, no_wait_after=True)
    except Exception as e:
        _remove_dialog_listener(page, _on_dialog)
        raise e

    # tenta modal HTML 'OK' rapidamente
    ok_btn = None
    try:
        ok_btn = page.wait_for_selector(
            "css=button:has-text('OK'), button:has-text('Ok'), button:has-text('Ok!'), "
            "xpath=//button[normalize-space()='OK' or normalize-space()='Ok']",
            timeout=TINY
        )
    except Exception:
        pass

    if ok_btn:
        with suppress(Exception):
            ok_btn.click(timeout=TINY)
        log_info("↳ modal HTML 'OK' clicado.")

    # estado mínimo de carregamento (evita networkidle)
    with suppress(Exception):
        page.wait_for_load_state("domcontentloaded", timeout=SHORT)

    # remove listener
    _remove_dialog_listener(page, _on_dialog)
