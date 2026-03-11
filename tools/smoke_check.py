from __future__ import annotations

import inspect
import json
import os
import platform
import sys
from pathlib import Path
from typing import Any, Dict, Tuple

ROOT = Path(__file__).resolve().parent.parent
CONFIG_FILE = ROOT / "config.json"
EMPRESAS_FILE = ROOT / "data" / "empresas_liberadas.json"


def read_json(path: Path) -> Tuple[Any, str]:
    if not path.exists():
        return None, f"missing: {path}"
    try:
        return json.loads(path.read_text(encoding="utf-8")), ""
    except Exception as exc:
        return None, f"invalid json: {exc}"


def report(level: str, label: str, detail: str = "") -> None:
    prefix = {"ok": "[OK]", "warn": "[WARN]", "fail": "[FAIL]"}.get(level, "[INFO]")
    line = f"{prefix} {label}"
    if detail:
        line += f" - {detail}"
    print(line)


def check_playwright() -> bool:
    try:
        import playwright  # type: ignore

        version = getattr(playwright, "__version__", "unknown")
        support = "unknown"
        try:
            from playwright.sync_api._generated import Browser  # type: ignore

            sig = inspect.signature(Browser.new_context)
            support = "yes" if "downloads_path" in sig.parameters else "no"
        except Exception:
            pass
        report("ok", "playwright import", f"version={version}, downloads_path={support}")
        return True
    except Exception as exc:
        report("fail", "playwright import", str(exc))
        return False


def check_config() -> bool:
    cfg, err = read_json(CONFIG_FILE)
    if err:
        report("fail", "config.json", err)
        return False
    if not isinstance(cfg, dict):
        report("fail", "config.json", "not a dict")
        return False

    downloads_dir = str(cfg.get("downloads_dir") or "").strip()
    if downloads_dir:
        if Path(downloads_dir).exists():
            report("ok", "downloads_dir", downloads_dir)
        else:
            report("warn", "downloads_dir", f"path not found: {downloads_dir}")
    else:
        report("warn", "downloads_dir", "not set")

    creds = cfg.get("credentials") or {}
    user = str(creds.get("username") or "").strip()
    pwd = str(creds.get("password") or "")
    if user and pwd:
        report("ok", "credentials", f"user={user}")
    else:
        report("warn", "credentials", "missing username/password")

    return True


def check_empresas() -> bool:
    data, err = read_json(EMPRESAS_FILE)
    if err:
        report("warn", "empresas_liberadas.json", err)
        return False
    if not isinstance(data, list):
        report("warn", "empresas_liberadas.json", "not a list")
        return False

    total = len(data)
    selected = sum(1 for row in data if isinstance(row, dict) and bool(row.get("Sel")))
    missing_cnpj = sum(1 for row in data if isinstance(row, dict) and not str(row.get("cnpj") or "").strip())
    missing_empresa = sum(
        1
        for row in data
        if isinstance(row, dict)
        and not str(row.get("empresa") or row.get("Empresa") or "").strip()
    )
    report("ok", "empresas carregadas", f"total={total}, selected={selected}")
    if missing_cnpj:
        report("warn", "cnpj vazios", str(missing_cnpj))
    if missing_empresa:
        report("warn", "empresa vazias", str(missing_empresa))
    return True


def check_download_flow_wiring() -> bool:
    """Valida wiring e comportamento das etapas de download."""
    try:
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))
        import main as nm_main  # type: ignore
    except Exception as exc:
        report("fail", "download flow", f"import main falhou: {exc}")
        return False

    flow_steps = getattr(nm_main, "FLOW_STEPS", [])
    download_keys = [k for k, _label, group in flow_steps if group == "downloads"]
    if not download_keys:
        report("fail", "download flow", "nenhuma etapa de download encontrada em FLOW_STEPS")
        return False

    missing_callables = [key for key in download_keys if not callable(getattr(nm_main, key, None))]
    if missing_callables:
        report("fail", "download flow", f"funcoes ausentes: {', '.join(missing_callables)}")
        return False

    originals: Dict[str, Any] = {}
    called: list[str] = []

    def _patch(name: str, fn: Any) -> None:
        originals[name] = getattr(nm_main, name, None)
        setattr(nm_main, name, fn)

    def _restore() -> None:
        for name, original in originals.items():
            setattr(nm_main, name, original)

    def _track_step(name: str):
        def _fn(*_args: Any, **_kwargs: Any) -> Any:
            called.append(name)
            if name == "abrir_emissao_guias":
                return True
            if name.startswith("baixar_"):
                return str(ROOT / "downloads" / "smoke_dummy.pdf")
            return None

        return _fn

    try:
        dummy_file = ROOT / "downloads" / "smoke_dummy.pdf"
        dummy_file.parent.mkdir(parents=True, exist_ok=True)
        if not dummy_file.exists():
            dummy_file.write_bytes(b"%PDF-1.4\n% smoke check\n")

        # Evita qualquer tentativa real de navegar/logar durante o smoke.
        _patch("is_on_login_screen", lambda _page: False)
        _patch("login", lambda *_a, **_k: True)
        _patch("abrir_empresas_autorizadas", lambda *_a, **_k: None)
        _patch("selecionar_empresa_por_lista", lambda *_a, **_k: None)
        _patch("abrir_escrituracao_contabilidade", lambda *_a, **_k: None)

        for key in download_keys:
            _patch(key, _track_step(key))

        flow = {k: False for k, _label, _group in flow_steps}
        for key in download_keys:
            flow[key] = True

        ok, _err = nm_main._rotina_para_empresa(
            object(),
            {"empresa": "SMOKE", "cnpj": "00000000000000"},
            "user",
            "pass",
            ROOT,
            flow_selection=flow,
        )
        missing_calls = [key for key in download_keys if key not in called]
        if missing_calls:
            report("fail", "download flow", f"etapas nao executadas: {', '.join(missing_calls)}")
            return False
        if not ok:
            report("fail", "download flow", "rotina retornou erro com etapas de download habilitadas")
            return False

        # Cenario sem bloqueio global: a primeira etapa falha, re-tenta
        # e as demais etapas continuam executando.
        called.clear()
        first_key = download_keys[0]

        def _fail_first(*_args: Any, **_kwargs: Any) -> Any:
            called.append(first_key)
            return None

        for key in download_keys:
            _patch(key, _track_step(key))
        _patch(first_key, _fail_first)

        ok_block, _err_block = nm_main._rotina_para_empresa(
            object(),
            {"empresa": "SMOKE", "cnpj": "00000000000000"},
            "user",
            "pass",
            ROOT,
            flow_selection=flow,
        )

        downstream_missing = [k for k in download_keys[1:] if k not in called]
        if downstream_missing:
            report("fail", "download flow", f"etapas nao executadas apos falha inicial: {', '.join(downstream_missing)}")
            return False

        expected_attempts = 3
        try:
            raw_attempts = (os.getenv("NM_DOWNLOAD_RETRY_ATTEMPTS") or "").strip()
            if raw_attempts:
                expected_attempts = int(raw_attempts)
            else:
                cfg = nm_main._read_config_json()  # type: ignore[attr-defined]
                if isinstance(cfg, dict):
                    expected_attempts = int(cfg.get("download_retry_attempts", 3))
        except Exception:
            expected_attempts = 3
        expected_attempts = max(1, min(10, expected_attempts))

        fail_calls = sum(1 for k in called if k == first_key)
        if fail_calls != expected_attempts:
            report(
                "fail",
                "download flow",
                f"tentativas de retry inesperadas para {first_key}: {fail_calls} (esperado {expected_attempts})",
            )
            return False

        if not ok_block:
            report("fail", "download flow", "rotina nao deveria sinalizar erro quando uma etapa nao tem arquivo")
            return False

        report("ok", "download flow", f"{len(download_keys)} etapa(s) de download validadas")
        return True
    except Exception as exc:
        report("fail", "download flow", str(exc))
        return False
    finally:
        _restore()


def main() -> int:
    print("Smoke check - Nota Manaus RPA")
    print(f"python: {sys.version.split()[0]}")
    print(f"os: {platform.platform()}")
    print("")

    ok = True
    ok &= check_playwright()
    ok &= check_config()
    ok &= check_download_flow_wiring()
    check_empresas()

    print("")
    report("ok" if ok else "fail", "overall")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
