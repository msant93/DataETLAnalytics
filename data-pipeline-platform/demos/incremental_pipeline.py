"""
incremental_pipeline.py
-----------------------
Incremental (ELT) loading on top of the same DuckDB warehouse.

Where `pipeline.py` does a full `replace` load, this module loads only what has
changed since the last run and *upserts* it, using two dlt mechanisms together:

  1. Incremental cursor (high-water-mark): a `dlt.sources.incremental` on the
     `updated_at` column. dlt persists the max value it has seen in pipeline
     state, so each run only processes rows at/after that watermark.

  2. Merge write disposition + primary key: `write_disposition="merge"` with
     `primary_key="order_id"` turns the load into an UPSERT — new orders are
     inserted, and re-sent (updated) orders replace their existing row instead
     of creating a duplicate.

The "source" here is a mutable CSV (`data/orders_source.csv`) that stands in for
an operational database or API. In production you would swap the body of
`orders()` for a `dlt.sources.sql_database` / `rest_api` source; the incremental
+ merge wiring stays identical. To make that point concrete, we push the
watermark down into the read (a simulated `WHERE updated_at >= :last_value`) so
we transfer only candidate rows rather than the whole table on every run.
"""

from __future__ import annotations

from pathlib import Path

import dlt
import duckdb
import pandas as pd

# Reuse extract typing + the quality gate from the batch pipeline (DRY).
from demos.pipeline import (
    OUTPUT_DIR,
    run_quality_checks,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SOURCE_CSV = DATA_DIR / "orders_source.csv"

PIPELINE_NAME = "incremental_sales"
DATASET_NAME = "sales_incremental"
TABLE_NAME = "orders"
DUCKDB_PATH = OUTPUT_DIR / f"{PIPELINE_NAME}.duckdb"
FQ_TABLE = f"{DATASET_NAME}.{TABLE_NAME}"

# The watermark cursor column and the natural key used for the upsert.
CURSOR_COLUMN = "updated_at"
PRIMARY_KEY = "order_id"
INITIAL_WATERMARK = "2000-01-01T00:00:00"


# --------------------------------------------------------------------------- #
# dlt resource: the incremental source
# --------------------------------------------------------------------------- #

@dlt.resource(
    name=TABLE_NAME,
    write_disposition="merge",     # UPSERT semantics
    primary_key=PRIMARY_KEY,       # ...keyed on order_id
)
def orders(
    updated_at: dlt.sources.incremental[str] = dlt.sources.incremental(
        cursor_path=CURSOR_COLUMN,
        initial_value=INITIAL_WATERMARK,
        # range_start="closed" (default) => WHERE updated_at >= last_value.
        # Re-reading the boundary row is safe: merge + primary_key dedups it.
    ),
):
    """
    Yield rows whose cursor value is at/after the last watermark.

    `updated_at.last_value` is the high-water-mark dlt persisted from the
    previous run. We use it to filter *at the source* (pushdown) so we don't
    haul the entire table across the wire every run — the whole point of
    incremental loading. dlt then applies its own incremental filter as a
    safety net and advances the watermark from the rows we emit.
    """
    watermark = updated_at.last_value or INITIAL_WATERMARK

    df = pd.read_csv(SOURCE_CSV)
    df[CURSOR_COLUMN] = df[CURSOR_COLUMN].astype(str)

    # Simulated `WHERE updated_at >= :watermark` — this is the pushdown a real
    # SQL/API source would do server-side.
    candidates = df[df[CURSOR_COLUMN] >= watermark].copy()

    # Same explicit typing + derived measure as the batch path.
    candidates["order_date"] = pd.to_datetime(candidates["order_date"]).dt.strftime("%Y-%m-%d")
    candidates["quantity"] = candidates["quantity"].astype("int64")
    candidates["unit_price"] = candidates["unit_price"].astype("float64")
    candidates["revenue"] = (candidates["quantity"] * candidates["unit_price"]).round(2)

    print(f"    source pushdown: {len(candidates)}/{len(df)} rows at/after watermark {watermark!r}")

    # Quality-gate the candidate batch before emitting anything downstream.
    if len(candidates):
        report = run_quality_checks(candidates.drop(columns=[CURSOR_COLUMN, "revenue"]))
        if not report.passed:
            raise ValueError(f"Quality gate failed on incremental batch:\n{report.summary()}")

    yield from candidates.to_dict(orient="records")


# --------------------------------------------------------------------------- #
# Pipeline runner
# --------------------------------------------------------------------------- #

def _pipeline():
    return dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=dlt.destinations.duckdb(str(DUCKDB_PATH)),
        dataset_name=DATASET_NAME,
    )


def run_incremental():
    """Run one incremental cycle. Safe to call repeatedly (idempotent)."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    pipe = _pipeline()
    info = pipe.run(orders())
    return pipe, info


# --------------------------------------------------------------------------- #
# Warehouse inspection helpers
# --------------------------------------------------------------------------- #

def _connect():
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


def table_row_count() -> int:
    with _connect() as con:
        return con.execute(f"SELECT COUNT(*) FROM {FQ_TABLE}").fetchone()[0]


def load_history() -> pd.DataFrame:
    with _connect() as con:
        return con.execute(
            f"SELECT load_id, status, inserted_at FROM {DATASET_NAME}._dlt_loads ORDER BY inserted_at"
        ).df()


def current_watermark() -> str | None:
    """Read the incremental high-water-mark dlt persisted in pipeline state."""
    pipe = _pipeline()
    src_state = pipe.state.get("sources", {})
    # Walk the nested resource state to find the last cursor value.
    for _src, sdata in src_state.items():
        res = sdata.get("resources", {}).get(TABLE_NAME, {})
        inc = res.get("incremental", {}).get(CURSOR_COLUMN, {})
        if "last_value" in inc:
            return inc["last_value"]
    return None


def order_snapshot(order_id: int) -> pd.DataFrame:
    with _connect() as con:
        return con.execute(
            f"SELECT order_id, product, quantity, unit_price, revenue, updated_at "
            f"FROM {FQ_TABLE} WHERE order_id = ?", [order_id]
        ).df()
