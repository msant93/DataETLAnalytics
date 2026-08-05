"""
sources.py — generic extraction from row-oriented sources.

Any SQL engine (Postgres, MySQL, Oracle, SQLite, ...) is reached through one
SQLAlchemy code path; only the connection string differs. CSV files are read
with pandas. Incremental extraction uses a high-water-mark: pull rows whose
cursor is >= the max cursor already in the target Delta table.
"""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text

from etl.settings import resolve_dsn, resolve_path

logger = logging.getLogger(__name__)


def extract(source: dict[str, Any], cursor: str | None, watermark: Any | None) -> pd.DataFrame:
    """Dispatch on source.type. Returns a DataFrame of candidate rows."""
    stype = source["type"]
    # Oracle rides the same SQLAlchemy path as the other SQL engines — only the
    # connection string and driver differ (oracle+oracledb://...).
    if stype in {"postgres", "mysql", "sqlite", "oracle"}:
        return _extract_sql(source, cursor, watermark)
    if stype == "csv":
        return _extract_csv(source, cursor, watermark)
    raise ValueError(f"Unsupported source.type: {stype}")


def _extract_sql(source: dict, cursor: str | None, watermark: Any | None) -> pd.DataFrame:
    dsn = resolve_dsn(source["dsn_env"])
    # Oracle (and some others) want schema-qualified, case-sensitive names; the
    # spec can supply `schema` and the exact `table` as stored in the catalog.
    table = source["table"]
    if source.get("schema"):
        table = f'{source["schema"]}.{table}'
    engine = create_engine(dsn)
    try:
        with engine.connect() as con:
            if cursor and watermark is not None:
                # Pushed-down incremental filter: only rows at/after the watermark
                # cross the wire. Closed boundary (>=) is safe because MERGE dedups.
                sql = text(f"SELECT * FROM {table} WHERE {cursor} >= :wm")  # noqa: S608
                logger.info("Extract %s WHERE %s >= %s", table, cursor, watermark)
                df = pd.read_sql(sql, con, params={"wm": watermark})
            else:
                logger.info("Extract %s (full)", table)
                df = pd.read_sql(text(f"SELECT * FROM {table}"), con)  # noqa: S608
        return df
    finally:
        engine.dispose()


def _extract_csv(source: dict, cursor: str | None, watermark: Any | None) -> pd.DataFrame:
    path = source.get("path") or resolve_path(source["path_env"])
    df = pd.read_csv(path)
    if cursor and watermark is not None and cursor in df.columns:
        df = df[df[cursor].astype(str) >= str(watermark)].copy()
    logger.info("Extract CSV %s (%d rows)", path, len(df))
    return df
