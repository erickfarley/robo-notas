# core/selectors.py
# Seletores XPath e CSS para elementos da página

# ───── Seletores de login ─────────────────────────────────────────────────
X_LOGIN_CPF = "//*[@id='vUSULOGIN']"
X_LOGIN_SENHA = "//*[@id='vSENHA']"
X_LOGIN_BTN = "//*[@id='TABLE1']/tbody/tr[6]/td/input"

# ───── Seletores de captcha ───────────────────────────────────────────────
CAPTCHA_IMG_SELECTORS = [
    "#vIMAGEM_0001",
    "img[src*='vIMAGEM_0001']",
    "img[id*='vIMAGEM_0001']",
    "img[name='vIMAGEM_0001']",
    "img[src*='Captcha']",
    "img[src*='captcha']",
    ".captcha-image"
]

CAPTCHA_INPUT_SELECTORS = [
    "#vVALORIMAGEM",
    "//*[@id='vVALORIMAGEM']",
    "input[name='vVALORIMAGEM']",
    ".captcha-input"
]

# ───── Seletores de navegação ─────────────────────────────────────────────
X_MENU_NOTAS = "//*[@id='apy0m0i0ITX']"
X_SUBMENU_CONSULTA = "//*[@id='apy0m1i2ITX']"

X_MENU_PERFIL            = "//*[@id='apy0m0i0ITD']"
X_SUBMENU_EMPRESAS_AUT   = "//*[@id='apy0m1i2ITX']"

X_MENU_MOVIMENTACOES = "//*[@id='apy0m0i2ITX']"
X_SUBMENU_MOV_MENSAL = "//*[@id='apy0m7i0ITD']"
X_SUBMENU_MOV_MENSAL_NACIONAL = (
    "//a[contains(translate(normalize-space(.),"
    " 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'MOVIMENT')"
    " and contains(translate(normalize-space(.),"
    " 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'NOTA NACIONAL')]"
    " | //span[contains(translate(normalize-space(.),"
    " 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'MOVIMENT')"
    " and contains(translate(normalize-space(.),"
    " 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'NOTA NACIONAL')]"
)
X_SUBMENU_MOV_MENSAL_RET_NACIONAL = (
    "//a[contains(translate(normalize-space(.),"
    " 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'NOTA NACIONAL')"
    " and contains(translate(normalize-space(.),"
    " 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'RET')]"
    " | //span[contains(translate(normalize-space(.),"
    " 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'NOTA NACIONAL')"
    " and contains(translate(normalize-space(.),"
    " 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'RET')]"
)

# ───── Seletores de consulta ──────────────────────────────────────────────
X_CNPJ_INPUT = "//input[@id='vCTBCPFCNPJ']"
X_DATA_DE = "//*[@id='vDTAINI_dp_trigger']"
X_DATA_ATE = "//*[@id='vDTAFIM_dp_trigger']"
X_CONSULTAR_BTN = "//*[@id='TABLE2']/tbody/tr[5]/td/input[1]"

# ───── Seletores de resultados ────────────────────────────────────────────
X_TABELA_LINHAS = "//table[contains(@class,'resultados')]//tbody/tr"

# ───── Botões ────────────────────────────────────────────
#X_BTN_MOV_PESQUISAR = "//*[@id='TABLE2']/tbody/tr[4]/td/input"
X_BTN_MOV_RET_PESQUISAR = "//*[@id='TABLE2']/tbody/tr[5]/td/input"
X_BTN_MOV_PESQUISAR = (
    "//*[@id='TABLE2']/tbody/tr[4]/td/input"
    " | //input[translate(@value,'pesquisar','Pesquisar')='PESQUISAR' "
    "        and not(contains(@style,'display:none'))]"
    " | //button[contains(translate(normalize-space(.),'pesquisar','PESQUISAR'),'PESQUISAR') "
    "          and not(contains(@style,'display:none'))]"
)
#X_BTN_MOV_PESQUISAR_LIVRO = "//*[@id='TABLE3']/tbody/tr[4]/td/input"

X_BTN_MOV_PESQUISAR_LIVRO = (
    "//input[translate(@value,'pesquisar','Pesquisar')='PESQUISAR' "
    "        and not(contains(@style,'display:none'))]"
    " | //button[contains(translate(normalize-space(.),'pesquisar','PESQUISAR'),'PESQUISAR') "
    "          and not(contains(@style,'display:none'))]"
)
