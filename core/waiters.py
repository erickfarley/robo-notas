# core/waiters.py
from typing import Any, Sequence
from playwright.sync_api import Page
from playwright.sync_api import TimeoutError as PWTimeout
import time

try:
    from utils.logger import log_info, log_error
except Exception:
    # fallback simples caso logger não exista
    def log_info(msg: str): print(msg)
    def log_error(msg: str): print(msg)

# Seletores comuns de “overlay/loading” em apps web (ajuste se quiser)
_BUSY_SELECTORS: Sequence[str] = (
    "#updateProgressDiv", ".modal-backdrop", ".ui-widget-overlay",
    ".blockUI", "#loading", "div[aria-busy='true']", "div.loading",
    "#Aguarde", "#pleaseWait", ".spinner", "#spinner"
)

def _wait_overlays_to_hide(page: Page, timeout_ms: int = 10_000) -> None:
    """Espera overlays/spinners sumirem (se aparecerem)."""
    deadline = time.time() + timeout_ms / 1000
    for sel in _BUSY_SELECTORS:
        try:
            # se estiver visível, espera ficar hidden
            if page.query_selector(sel):
                page.wait_for_selector(sel, state="hidden", timeout=timeout_ms)
        except PWTimeout:
            pass
        if time.time() > deadline:
            break

def handle_dialog_and_modal(page: Page, click_target: Any,
                            wait_after_ms: int = 300,
                            nav_timeout_ms: int = 8_000) -> None:
    """
    Clica no alvo e lida automaticamente com:
      • possível diálogo JS (alert/confirm/prompt) → aceita/dispensa
      • possível navegação → espera carregar
      • possíveis overlays/spinners → espera desaparecer

    `click_target` pode ser ElementHandle ou Locator.
    """
    dialog_box = {"obj": None}

    def _on_dialog(dlg):
        dialog_box["obj"] = dlg

    # Captura UM diálogo se ocorrer logo após o clique
    page.once("dialog", _on_dialog)

    # Executa o clique
    if hasattr(click_target, "click"):
        click_target.click()
    else:
        # Último recurso: tenta clicar via JS
        page.evaluate("(el)=>el.click()", click_target)

    # Se houve diálogo, trata
    if dialog_box["obj"]:
        try:
            msg = dialog_box["obj"].message
            log_info(f"Diálogo: {msg[:120]!r} → aceitando")
            dialog_box["obj"].accept()
        except Exception:
            dialog_box["obj"].dismiss()

    # Tenta detectar navegação rápida (sem travar se for AJAX)
    try:
        page.wait_for_load_state("load", timeout=nav_timeout_ms)
    except PWTimeout:
        # sem navegação; segue o fluxo (provável postback/AJAX)
        pass

    # Aguarda pequenos efeitos e overlays sumirem
    if wait_after_ms > 0:
        time.sleep(wait_after_ms / 1000)
    _wait_overlays_to_hide(page)
