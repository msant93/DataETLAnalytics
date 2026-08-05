"""
run_local_demo.py — seed SQLite stand-ins, then run the whole ETL graph locally.

This is the DEMO wrapper around the framework's `etl.local_runner.run_all`. It
creates the stand-in sources (so the platform runs with no external servers) and
points the pipeline specs' env vars at them, then executes the real graph.

    python -m demos.run_local_demo
"""
from __future__ import annotations

import os

from etl.local_runner import run_all
from etl.settings import PROJECT_ROOT


def _seed_demo_environment() -> None:
    """Create the SQLite stand-ins + CSV and point the specs' env vars at them."""
    from demos.seeds import seed_sources
    seed_sources.main()
    src = PROJECT_ROOT / "data" / "sources"
    os.environ.setdefault("CRM_DSN", f"sqlite:///{src / 'crm.db'}")
    os.environ.setdefault("SALES_DSN", f"sqlite:///{src / 'sales.db'}")
    os.environ.setdefault("PRODUCTS_PATH", str(src / "products.csv"))


def run_seeded() -> dict[str, int]:
    """Seed the demo sources, then run the full graph. Returns rows-per-node."""
    _seed_demo_environment()
    return run_all()


if __name__ == "__main__":
    from etl.logging_setup import configure
    configure(level="INFO")
    results = run_seeded()
    print("\nPipeline results (rows written per node):")
    for name, rows in results.items():
        print(f"  {name:<16} {rows}")
