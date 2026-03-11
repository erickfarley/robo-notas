# Nota Manaus RPA

Automacao do portal NFSe de Manaus com Playwright, controle por API FastAPI e painel web.

## O que e este projeto

Este repositorio implementa um robo que:

1. Faz login no portal NFSe (automatico ou manual).
2. Carrega e filtra empresas autorizadas.
3. Executa etapas fiscais por empresa (encerramentos e downloads).
4. Atualiza status por empresa em JSON (`ok`/`erro`).
5. Gera relatorio consolidado da execucao (JSON + CSV).

O foco e operacao em lote com tolerancia a falhas (retentativas, relogin e processamento paralelo por workers).

## Como funciona (fluxo ponta a ponta)

1. Carregar empresas:
   - A rota/tela "Carregar empresas" abre "Empresas Autorizadas".
   - Captura a grade e filtra somente situacao contendo `Liberado`.
   - Salva em `data/empresas_liberadas.json` e `data/empresas_liberadas.csv`.
2. Selecionar empresas:
   - A execucao considera apenas registros com `Sel=true`.
   - A marcacao pode ser feita pela tela web (individual, marcar todas, desmarcar todas).
3. Executar robo:
   - Para cada empresa selecionada, o robo tenta manter sessao valida e processa as etapas habilitadas.
   - Falhas por etapa geram alerta e o fluxo segue para proxima etapa.
   - Falha critica na inicializacao da empresa marca a empresa com erro.
4. Consolidar resultado:
   - Cada empresa recebe `ultimo_status`, `processado_em`, `ultimo_erro` (quando houver) e `ultimo_relatorio`.
   - Ao final, e gerado relatorio consolidado em `data/reports/execucao_*.json` e `.csv`.

## Etapas disponiveis no fluxo

As etapas sao habilitadas por `flow_steps` no `config.json` ou via UI (`/api/flow`):

- `encerrar_mov_mensal`
- `encerrar_mov_ret_mensal`
- `encerrar_mov_nfse_mensal`
- `enncerrar_mov_mensal_nacioal`
- `encerrar_mov_ret_mensal_nacional`
- `baixar_notas_emitidas`
- `baixar_notas_recebidas`
- `abrir_emissao_guias`
- `baixar_relatorio_nota_nacional_recebidas`
- `baixar_relatorio_nota_nacional_recebidas_intermediario`
- `baixar_relatorio_nota_nacional_emetidas`
- `baixar_extrato_issqn`

## Arquitetura rapida

- `web_app.py`: API FastAPI, fila de tarefas, logs, agendamento mensal e endpoints de controle.
- `main.py`: orquestracao principal da execucao sequencial/paralela e consolidacao de relatorios.
- `core/browser.py`: navegacao no portal e execucao das etapas de negocio.
- `core/captcha.py`: login, captcha e modo manual.
- `core/ocr_fallback.py`: OCR local com Tesseract (fallback).
- `core/ocr_remote.py`: OCR remoto via OpenRouter (opcional).
- `core/empresas_autorizadas.py`: coleta de empresas liberadas.
- `utils/empresas_json.py`: leitura da fila (`Sel`) e marcacao de status por CNPJ.
- `tools/parallel_worker.py`: entrypoint de worker em execucao paralela.

## Requisitos

- Python 3.9+
- Google Chrome instalado (canal usado pelo Playwright)
- Dependencias Python do `requirements.txt`
- Browsers do Playwright instalados
- Tesseract instalado (recomendado para OCR local de captcha)

## Instalacao

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install
```

## Configuracao

Edite `config.json`. Exemplo minimo:

```json
{
  "downloads_dir": "C:\\Users\\SeuUsuario\\Downloads\\NotaManaus",
  "headless": false,
  "manual_login": false,
  "manual_login_timeout_sec": 900,
  "parallel_workers": 3,
  "worker_restart_limit": 2,
  "force_parallel": false,
  "credentials": {
    "username": "SEU_LOGIN",
    "password": "SUA_SENHA"
  },
  "period": {
    "mes_de": 1,
    "ano_de": 2026,
    "mes_ate": 1,
    "ano_ate": 2026
  },
  "flow_steps": {
    "encerrar_mov_mensal": true,
    "encerrar_mov_ret_mensal": true
  },
  "scheduler": {
    "enabled": false,
    "recurring": true,
    "date": "2026-04-02",
    "time": "11:00",
    "next_run": ""
  },
  "openrouter": {
    "api_key": "",
    "model": "openai/gpt-4o-mini",
    "timeout": 30,
    "models": [
      "openai/gpt-4o-mini"
    ]
  }
}
```

Observacoes importantes:

- Se `manual_login=true`, o robo preenche usuario/senha e aguarda captcha + clique manual.
- Se `manual_login=true` e `parallel_workers>1`, o sistema reduz para 1 worker, exceto com `force_parallel=true`.
- `close_browser_after` e salvo pela UI, mas a execucao principal atualmente mantem o navegador aberto ao final (com fechamento manual ou via endpoint).
- Sem `OPENROUTER_API_KEY`, o login continua usando apenas OCR local (Tesseract) e/ou modo manual.

## Execucao

Modo web (recomendado):

```bash
python web_app.py
```

Opcao equivalente:

```bash
uvicorn web_app:app --host 127.0.0.1 --port 8000
```

Depois abra no navegador:

```text
http://127.0.0.1:8000
```

Modo CLI (sem painel):

```bash
python main.py
```

No modo CLI, garanta antes:

1. `data/empresas_liberadas.json` existente.
2. Empresas com `Sel=true`.
3. `config.json` com credenciais e parametros validos.

## Operacao recomendada pelo painel

1. Abrir a aplicacao web.
2. Clicar em "Carregar empresas (Liberado)".
3. Marcar empresas que devem rodar (`Sel`).
4. Ajustar periodo, etapas e configuracoes.
5. Iniciar execucao.
6. Acompanhar logs em tempo real.
7. Exportar downloads compactados quando necessario.

## Artefatos gerados

- `data/empresas_liberadas.json`: fila de empresas e status por CNPJ.
- `data/empresas_liberadas.csv`: exportacao tabular das empresas.
- `data/reports/execucao_*.json` e `data/reports/execucao_*.csv`: consolidado final da execucao.
- `data/parallel_runs/<timestamp>/empresas_worker_*.json`: arquivos de trabalho por worker.
- Pasta de downloads configurada em `downloads_dir` (ou fallback em temp).
- Debug de captcha/login em `%TEMP%\NotaManausRPA\debug`.

## Variaveis de ambiente uteis

- `NM_MANUAL_LOGIN`
- `NM_MANUAL_LOGIN_TIMEOUT_SEC`
- `NM_DOWNLOADS_DIR`
- `NM_REPORTS_DIR`
- `NM_EMPRESAS_JSON`
- `NM_FLOW_STEPS`
- `NM_MAX_WORKERS`
- `NM_FORCE_PARALLEL`
- `NM_WORKER_RESTART_LIMIT`
- `NM_DOWNLOAD_RETRY_ATTEMPTS`
- `NM_DOWNLOAD_RETRY_WAIT_SEC`
- `OPENROUTER_API_KEY`
- `OPENROUTER_ENDPOINT`
- `OPENROUTER_MODEL`
- `OPENROUTER_TIMEOUT`
- `OPENROUTER_MODELS`

## Endpoints principais (API web)

- `GET /api/status`: estado da execucao.
- `POST /api/start`: inicia robo.
- `POST /api/stop`: solicita parada.
- `POST /api/close-browser`: fecha navegador.
- `GET/POST /api/config`: leitura/gravacao de configuracoes.
- `GET/POST /api/flow`: leitura/gravacao das etapas habilitadas.
- `GET/POST /api/period`: leitura/gravacao do periodo.
- `GET/POST /api/scheduler`: leitura/gravacao do agendador.
- `GET /api/empresas`, `POST /api/empresas/mark`, `POST /api/empresas/mark-all`, `POST /api/empresas/load`
- `GET /api/downloads/archive`: zip dos downloads.
- `GET /api/logs` e `WS /ws/logs`: stream de logs.

## Seguranca e boas praticas

- Nao comite `config.json` com credenciais reais ou chaves de API.
- Use variaveis de ambiente para segredos sempre que possivel.
- Revise `data/` e `downloads/` antes de versionar artefatos.
