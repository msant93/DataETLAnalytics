"""
engine.py — the generic runner. It executes any spec without knowing anything
table-specific; all specifics come from the YAML + the transform registry.

    run_ingest(spec):  extract -> transform -> quality gate -> MERGE into silver
    run_model(spec):   SQL over silver Delta tables -> overwrite gold Delta

Hardening built in:
  * lag window on the incremental watermark (catch late-arriving rows)
  * CDC soft-delete handling (remove rows flagged deleted at the source)
  * a data-quality gate that runs BEFORE the load (abort / warn / quarantine)
  * money columns stored as exact decimal
"""
from __future__ import annotations

import logging

import duckdb
import pandas as pd

from etl import delta_store, quality, sources, transforms
from etl.registry import IngestSpec, ModelSpec, load_specs
from etl.settings import gold_path, silver_path

logger = logging.getLogger(__name__)


def _apply_lag(watermark, seconds: int):
    """Shift the watermark back by `seconds` so a window of recent rows is
    re-read every run. Combined with MERGE (which dedups), this recovers
    late-arriving / out-of-order rows that a strict high-water-mark would miss."""
    if seconds <= 0 or watermark is None:
        return watermark
    if isinstance(watermark, (int, float)) and not isinstance(watermark, bool):
        return watermark - seconds
    ts = pd.to_datetime(watermark, errors="coerce")
    if pd.isna(ts):
        return watermark
    return (ts - pd.Timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S")


def run_ingest(spec: IngestSpec) -> int:
    """Source table/file -> silver Delta table. Incremental, gated, idempotent."""
    target = silver_path(spec.target["table"])

    watermark = None
    if spec.cursor:
        watermark = delta_store.max_cursor(target, spec.cursor)
        watermark = _apply_lag(watermark, spec.lag_seconds)

    df = sources.extract(spec.source, spec.cursor, watermark)
    if df.empty:
        logger.info("[%s] no new rows", spec.name)
        return 0

    df = transforms.apply(spec.transforms, df)

    # --- CDC: split out soft-deleted rows before validating / merging ---------
    deletes = None
    sd = spec.soft_delete_column
    if sd and sd in df.columns:
        del_mask = df[sd].astype(bool)
        deletes = df[del_mask]
        df = df[~del_mask]

    # --- Quality gate (runs BEFORE the load) ----------------------------------
    if spec.quality and not df.empty:
        report = quality.check(df, spec.quality, spec.primary_key)
        if not report.passed:
            on_fail = spec.quality.get("on_fail", "abort")
            logger.warning("[%s] %s -> on_fail=%s", spec.name, report.summary(), on_fail)
            if on_fail == "abort":
                raise quality.QualityError(f"[{spec.name}] {report.summary()}")
            if on_fail == "quarantine":
                bad = df[report.bad_mask]
                df = df[~report.bad_mask]
                delta_store.write(
                    silver_path(f"{spec.target['table']}__rejects"), bad, mode="append"
                )
                logger.warning("[%s] quarantined %d rows", spec.name, len(bad))
            # on_fail=warn falls through and loads everything

    written = 0
    if not df.empty:
        written = delta_store.write(
            target, df,
            mode=spec.write_mode,
            primary_key=spec.primary_key,
            money_columns=spec.money_columns,
        )

    # --- apply CDC deletes to the target --------------------------------------
    if deletes is not None and len(deletes):
        delta_store.delete_keys(target, spec.primary_key, deletes[spec.primary_key].tolist())

    return written


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

    return delta_store.write(
        gold_path(spec.target["table"]), result,
        mode="overwrite", money_columns=spec.money_columns,
    )


def run_by_name(name: str) -> int:
    """Entry point used by the Airflow DAG / local runner: run one spec by name."""
    spec = load_specs()[name]
    if isinstance(spec, IngestSpec):
        return run_ingest(spec)
    if isinstance(spec, ModelSpec):
        return run_model(spec)
    raise TypeError(f"Unknown spec type for {name}")
