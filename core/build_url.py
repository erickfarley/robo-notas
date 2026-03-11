from urllib.parse import urlsplit
from playwright.sync_api import Page

def _build_empresas_url(page: Page) -> str:
    """
    Constrói uma URL absoluta confiável, usando o **origin** atual.
    Ex.:  https://nfse-prd.manaus.am.gov.br/nfse/servlet/hwcminhasempresas
    """
    parts = urlsplit(page.url)                      # scheme / netloc / …
    origin = f"{parts.scheme}://{parts.netloc}"
    return f"{origin}/nfse/servlet/hwcminhasempresas"


def _build_movimentacoes_url(page: Page) -> str:
    """
    Constrói uma URL absoluta confiável, usando o **origin** atual.
    Ex.:  https://nfse-prd.manaus.am.gov.br/nfse/servlet/hwmovmensal
    """
    parts = urlsplit(page.url)                      # scheme / netloc / …
    origin = f"{parts.scheme}://{parts.netloc}"
    return f"{origin}/nfse/servlet/hwmovmensal"


def _build_movimentacoes_ret_url(page: Page) -> str:
    """
    Constrói uma URL absoluta confiável, usando o **origin** atual.
    Ex.:  https://nfse-prd.manaus.am.gov.br/nfse/servlet/hwmmovmenret
    """
    parts = urlsplit(page.url)                      # scheme / netloc / …
    origin = f"{parts.scheme}://{parts.netloc}"
    return f"{origin}/nfse/servlet/hwmmovmenret"

def _build_movimentacoes_nfce_url(page: Page) -> str:
    """
    Constrói uma URL absoluta confiável, usando o **origin** atual.
    Ex.:  https://nfse-prd.manaus.am.gov.br/nfse/servlet/hwmovmensalnfce
    """
    parts = urlsplit(page.url)                      # scheme / netloc / …
    origin = f"{parts.scheme}://{parts.netloc}"
    return f"{origin}/nfse/servlet/hwmovmensalnfce"

def _build_movimentacoes_nacional_url(page: Page) -> str:
    """
    Constroi uma URL absoluta confiavel, usando o **origin** atual.
    Ex.:  https://nfse-prd.manaus.am.gov.br/nfse/servlet/hwmovmensalnn
    """
    parts = urlsplit(page.url)
    origin = f"{parts.scheme}://{parts.netloc}"
    return f"{origin}/nfse/servlet/hwmovmensalnn"

def _build_movimentacoes_ret_nacional_url(page: Page) -> str:
    """
    Constroi uma URL absoluta confiavel, usando o **origin** atual.
    Ex.:  https://nfse-prd.manaus.am.gov.br/nfse/servlet/hwmmovmenretnn
    """
    parts = urlsplit(page.url)
    origin = f"{parts.scheme}://{parts.netloc}"
    return f"{origin}/nfse/servlet/hwmmovmenretnn"

def _build_contabilidade_url(page: Page) -> str:
    """
    Constrói uma URL absoluta confiável, usando o **origin** atual.
    Ex.:  https://nfse-prd.manaus.am.gov.br/nfse/servlet/hwmcontabilidade
    """
    parts = urlsplit(page.url)                      # scheme / netloc / …
    origin = f"{parts.scheme}://{parts.netloc}"
    return f"{origin}/nfse/servlet/hwmcontabilidade"

def _build_guias_url(page: Page) -> str:
    """
    Constrói uma URL absoluta confiável, usando o **origin** atual.
    Ex.:  https://nfse-prd.manaus.am.gov.br/nfse/servlet/hwmguiarec
    """
    parts = urlsplit(page.url)                      # scheme / netloc / …
    origin = f"{parts.scheme}://{parts.netloc}"
    return f"{origin}/nfse/servlet/hwmguiarec"

def _build_extrato_issqn_url(page: Page) -> str:
    """
    Constrói uma URL absoluta confiável, usando o **origin** atual.
    Ex.: https://nfse-prd.manaus.am.gov.br/nfse/servlet/hwmcontacorrente
    """
    parts = urlsplit(page.url)
    origin = f"{parts.scheme}://{parts.netloc}"
    return f"{origin}/nfse/servlet/hwmcontacorrente"

def _build_relatorio_nota_nacional_recebidas_url(page: Page) -> str:
    """
    Constrói uma URL absoluta confiável, usando o **origin** atual.
    Ex.: https://nfse-prd.manaus.am.gov.br/nfse/servlet/hwmconnotnacrec
    """
    parts = urlsplit(page.url)
    origin = f"{parts.scheme}://{parts.netloc}"
    return f"{origin}/nfse/servlet/hwmconnotnacrec"

def _build_relatorio_nota_nacional_emitidas_url(page: Page) -> str:
    """
    Constrói uma URL absoluta confiável, usando o **origin** atual.
    Ex.: https://nfse-prd.manaus.am.gov.br/nfse/servlet/hwmconnotnacemi
    """
    parts = urlsplit(page.url)
    origin = f"{parts.scheme}://{parts.netloc}"
    return f"{origin}/nfse/servlet/hwmconnotnacemi"
