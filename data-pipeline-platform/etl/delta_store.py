"""
delta_store.py — the columnar storage layer (Delta Lake).

Delta Lake = Parquet data files + an ACID `_delta_log` transaction log. We use:
  * MERGE  -> idempotent upserts keyed on the primary key
  * overwrite -> deterministic full refresh (used for gold models)
  * max_cursor -> read the incremental high-water-mark from the target itself
                  (stateless: no external watermark store to keep in sync)

The same Delta files are readable by DuckDB, Spark, Trino, Athena and Power BI.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

logger = logging.getLogger(__name__)


def _exists(path: str) -> bool:
    try:
        DeltaTable(path)
        return True
    except TableNotFoundError:
        return False


def max_cursor(path: str, cursor: str) -> Any | None:
    """Return max(cursor) already stored, or None if the table doesn't exist."""
    if not _exists(path):
        return None
    ds = DeltaTable(path).to_pyarrow_dataset()
    con = duckdb.connect()
    con.register("t", ds)
    val = con.execute(f"SELECT max({cursor}) FROM t").fetchone()[0]  # noqa: S608
    con.close()
    return val


def write(path: str, df: pd.DataFrame, *, mode: str, primary_key: str | None = None) -> int:
    """
    Persist a DataFrame to a Delta table.
      mode="merge"     -> upsert on primary_key (idempotent)
      mode="overwrite" -> replace table contents (idempotent full refresh)
    Returns the row count of the incoming batch.
    """
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)

    if mode == "overwrite":
        write_deltalake(path, table, mode="overwrite", schema_mode="overwrite")
        logger.info("Delta overwrite %s (%d rows)", path, len(df))
        return len(df)

    if mode == "merge":
        if not primary_key:
            raise ValueError("merge mode requires a primary_key")
        if not _exists(path):
            write_deltalake(path, table)  # first load creates the table
            logger.info("Delta create %s (%d rows)", path, len(df))
            return len(df)
        dt = DeltaTable(path)
        (
            dt.merge(
                table,
                predicate=f"target.{primary_key} = source.{primary_key}",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
        logger.info("Delta merge %s (%d rows upserted)", path, len(df))
        return len(df)

    raise ValueError(f"Unknown write mode: {mode}")


def read_arrow(path: str) -> pa.Table:
    """Read a Delta table as an Arrow table (for SQL models / BI)."""
    return DeltaTable(path).to_pyarrow_table()


def row_count(path: str) -> int:
    if not _exists(path):
        return 0
    ds = DeltaTable(path).to_pyarrow_dataset()
    con = duckdb.connect()
    con.register("t", ds)
    n = con.execute("SELECT count(*) FROM t").fetchone()[0]
    con.close()
    return int(n)
