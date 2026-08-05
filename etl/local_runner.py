"""
local_runner.py — run the whole platform in dependency order.

Executes the exact same graph (etl/graph.py) that the Airflow DAG runs, so a
green run here proves the orchestration the DAG performs actually works — without
needing an Airflow scheduler. Used by the test suite and by the demo runner.

This module is pure framework: it assumes the source connections are already
configured (via env vars). The demo that seeds SQLite stand-ins lives in
`demos/run_local_demo.py`.
"""
from __future__ import annotations

import logging

from etl import engine
from etl.graph import build_graph, topological_order

logger = logging.getLogger(__name__)


def run_all() -> dict[str, int]:
    nodes = build_graph()
    order = topological_order(nodes)
    logger.info("Execution order: %s", " -> ".join(order))
    results: dict[str, int] = {}
    for name in order:
        rows = engine.run_by_name(name)
        results[name] = rows
        logger.info("[%s] done (%s rows)", name, rows)
    return results
