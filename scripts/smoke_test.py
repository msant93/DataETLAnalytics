"""
test_setup.py
-------------
Post-install smoke test. Run this immediately after setup to confirm the
environment is wired correctly BEFORE you open Jupyter. Exits non-zero on
the first failure so it can be used in CI or a Makefile.

    python test_setup.py
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# (import name, pip name, minimum version) — keep in sync with requirements.txt
REQUIRED = [
    ("dlt", "dlt", (0, 4, 0)),
    ("duckdb", "duckdb", (0, 9, 0)),
    ("pandas", "pandas", (2, 0, 0)),
    ("jupyter_core", "jupyter", (5, 0, 0)),
]

PASS, FAIL = "\033[92mPASS\033[0m", "\033[91mFAIL\033[0m"


def _version_tuple(v: str) -> tuple[int, ...]:
    parts = []
    for chunk in v.split("."):
        num = "".join(ch for ch in chunk if ch.isdigit())
        parts.append(int(num) if num else 0)
    return tuple(parts)


def check_python() -> bool:
    ok = sys.version_info >= (3, 9)
    print(f"[{PASS if ok else FAIL}] Python {sys.version.split()[0]} (need >= 3.9)")
    return ok


def check_imports() -> bool:
    all_ok = True
    for import_name, pip_name, min_ver in REQUIRED:
        try:
            mod = importlib.import_module(import_name)
            ver = getattr(mod, "__version__", "0")
            ok = _version_tuple(ver) >= min_ver
            wanted = ".".join(map(str, min_ver))
            print(f"[{PASS if ok else FAIL}] {pip_name} {ver} (need >= {wanted})")
            all_ok &= ok
        except ImportError:
            print(f"[{FAIL}] {pip_name} is not installed")
            all_ok = False
    return all_ok


def check_sample_data() -> bool:
    path = ROOT / "data" / "sample.csv"
    ok = path.exists() and path.stat().st_size > 0
    print(f"[{PASS if ok else FAIL}] sample data present at data/sample.csv")
    return ok


def check_pipeline_runs() -> bool:
    """The real test: can we actually load and query data?"""
    try:
        from demos import pipeline

        df = pipeline.load_raw_sales()
        report = pipeline.run_quality_checks(df)
        if not report.passed:
            print(f"[{FAIL}] quality gate failed on sample data")
            return False
        pipeline.load_to_duckdb(df)
        rows = pipeline.summary_statistics().iloc[0]["order_count"]
        ok = int(rows) == len(df)
        print(f"[{PASS if ok else FAIL}] end-to-end load + query ({int(rows)} rows)")
        return ok
    except Exception as exc:  # noqa: BLE001 - smoke test wants the message
        print(f"[{FAIL}] pipeline raised: {exc!r}")
        return False


def main() -> int:
    print("Validating local data engineering environment\n" + "-" * 44)
    results = [
        check_python(),
        check_imports(),
        check_sample_data(),
        check_pipeline_runs(),
    ]
    print("-" * 44)
    if all(results):
        print("All checks passed. You're ready: `jupyter notebook`")
        return 0
    print("Some checks failed. See messages above.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
