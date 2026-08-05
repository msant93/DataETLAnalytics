"""
engine.py — the generic runner. It executes any spec without knowing anything
table-specific; all specifics come from the YAML + the transform registry.

    run_ingest(spec):  extract -> transform -> MERGE into silver Delta
    run_model(spec):   SQL over silver Delta tables -> overwrite gold Delta
"""
from __future__ import annotations

import logging

import duckdb

from etl import delta_store, sources, transforms
from etl.registry import IngestSpec, ModelSpec, load_specs
from etl.settings import gold_path, silver_path

logger = logging.getLogger(__name__)


def run_ingest(spec: IngestSpec) -> int:
    """Source table/file -> silver Delta table. Incremental + idempotent."""
    target = silver_path(spec.target["table"])

    watermark = None
    if spec.cursor:
        watermark = delta_store.max_cursor(target, spec.cursor)

    df = sources.extract(spec.source, spec.cursor, watermark)
    if df.empty:
        logger.info("[%s] no new rows", spec.name)
        return 0

    df = transforms.apply(spec.transforms, df)
    return delta_store.write(
        target, df, mode=spec.write_mode, primary_key=spec.primary_key
    )


def run_model(spec: ModelSpec) -> int:
    """SQL over silver tables -> gold Delta table (deterministic overwrite)."""
    con = duckdb.connect()
    try:
        for name in spec.inputs:
            arrow = delta_store.read_arrow(silver_path(name))
            con.register(name, arrow)
        result = con.execute(spec.sql).df()
    finally:
        con.close()

    return delta_store.write(gold_path(spec.target["table"]), result, mode="overwrite")


def run_by_name(name: str) -> int:
    """Entry point used by the Airflow DAG / local runner: run one spec by name."""
    spec = load_specs()[name]
    if isinstance(spec, IngestSpec):
        return run_ingest(spec)
    if isinstance(spec, ModelSpec):
        return run_model(spec)
    raise TypeError(f"Unknown spec type for {name}")
