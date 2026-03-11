import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import main


def run() -> int:
    os.environ["NM_PARALLEL_WORKER"] = "1"
    ok, proc, errs = main.main()
    print(f"[WORKER_SUMMARY] ok={ok} processed={proc} errors={errs}", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(run())
