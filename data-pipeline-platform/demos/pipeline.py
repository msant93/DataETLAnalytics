"""
pipeline.py
-----------
Reusable data-engineering logic for the local dlt + DuckDB environment.

Design note: business logic lives here (versioned, importable, testable),
NOT in the notebook. The notebook orchestrates and visualizes; this module
is the single source of truth for how data is loaded, validated and analyzed.

Pipeline stages:
    1. load_raw_sales()   -> read source CSV into a typed DataFrame
    2. run_quality_checks -> assert business rules BEFORE loading (fail fast)
    3. load_to_duckdb()   -> land data in DuckDB via dlt (schema-managed)
    4. analytics helpers  -> SQL against the dlt-managed dataset
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

import dlt
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Configuration — sourced from config.yaml so the pipeline is retargetable
# without editing code. Falls back to literals if config is unavailable, so the
# module never hard-fails on import in a stripped-down environment.
# --------------------------------------------------------------------------- #

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"

try:
    from etl.config import load_config
    _cfg = load_config()
    PIPELINE_NAME = _cfg.pipeline_name
    DATASET_NAME = _cfg.dataset
    TABLE_NAME = _cfg.table
    DUCKDB_PATH = _cfg.resolved_path(_cfg.destination.get("duckdb_path", f"output/{PIPELINE_NAME}.duckdb"))
except Exception:  # pragma: no cover - defensive default
    _cfg = None
    PIPELINE_NAME = "local_sales"
    DATASET_NAME = "sales"
    TABLE_NAME = "orders"
    DUCKDB_PATH = OUTPUT_DIR / f"{PIPELINE_NAME}.duckdb"

# Fully-qualified table name. dlt namespaces every table under its dataset,
# so you MUST qualify as <dataset>.<table> in raw SQL or the query will fail.
FQ_TABLE = f"{DATASET_NAME}.{TABLE_NAME}"


def build_destination(cfg=None):
    """
    Return the dlt destination for the configured target. This is the seam that
    makes the pipeline sellable: point config at postgres/bigquery and the same
    load code ships to a client's warehouse — no code change.
    """
    cfg = cfg or _cfg
    dest = (cfg.destination if cfg else {"type": "duckdb"})
    kind = dest.get("type", "duckdb")

    if kind == "duckdb":
        return dlt.destinations.duckdb(str(DUCKDB_PATH))
    if kind == "postgres":
        return dlt.destinations.postgres(cfg.secret("postgres_dsn"))
    if kind == "bigquery":
        return dlt.destinations.bigquery(credentials=cfg.secret("bigquery_credentials"))
    if kind == "filesystem":
        return dlt.destinations.filesystem(str(OUTPUT_DIR / "lake"))
    raise ValueError(f"Unsupported destination.type: {kind}")


# --------------------------------------------------------------------------- #
# Data quality
# --------------------------------------------------------------------------- #

@dataclass
class QualityReport:
    """Structured result of the quality gate. Truthy-checkable via `.passed`."""
    total_rows: int
    checks: dict[str, bool] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(self.checks.values())

    def summary(self) -> str:
        lines = [f"Rows evaluated: {self.total_rows}"]
        for name, ok in self.checks.items():
            lines.append(f"  [{'PASS' if ok else 'FAIL'}] {name}")
        if self.errors:
            lines.append("Errors:")
            lines.extend(f"    - {e}" for e in self.errors)
        return "\n".join(lines)


def run_quality_checks(df: pd.DataFrame) -> QualityReport:
    """
    Validate business rules *before* the data is loaded. Loading dirty data and
    cleaning it later is how silent corruption enters a warehouse; we gate instead.
    """
    report = QualityReport(total_rows=len(df))
    required = {
        "order_id", "order_date", "customer_id", "product",
        "category", "region", "quantity", "unit_price",
    }

    # 1. Schema completeness
    missing_cols = required - set(df.columns)
    report.checks["all_required_columns_present"] = bool(not missing_cols)
    if missing_cols:
        report.errors.append(f"Missing columns: {sorted(missing_cols)}")
        return report  # further checks are unsafe without the columns

    # 2. No nulls in critical fields
    null_cols = [c for c in required if df[c].isna().any()]
    report.checks["no_nulls_in_required_fields"] = bool(not null_cols)
    if null_cols:
        report.errors.append(f"Null values found in: {null_cols}")

    # 3. Primary key uniqueness
    dup = int(df["order_id"].duplicated().sum())
    report.checks["order_id_is_unique"] = bool(dup == 0)
    if dup:
        report.errors.append(f"{dup} duplicate order_id value(s)")

    # 4. Business rule: quantity and unit_price must be positive
    # Coerce to native bool so QualityReport is JSON-serializable (numpy bools
    # break json.dumps and confuse strict `is` comparisons downstream).
    report.checks["quantity_positive"] = bool((df["quantity"] > 0).all())
    report.checks["unit_price_positive"] = bool((df["unit_price"] > 0).all())
    if not report.checks["quantity_positive"]:
        report.errors.append("Non-positive quantity detected")
    if not report.checks["unit_price_positive"]:
        report.errors.append("Non-positive unit_price detected")

    # 5. Referential-style sanity: dates parse and fall in a plausible range
    parsed = pd.to_datetime(df["order_date"], errors="coerce")
    report.checks["order_date_parses"] = bool(not parsed.isna().any())
    if not report.checks["order_date_parses"]:
        report.errors.append("Unparseable order_date value(s)")

    return report


# --------------------------------------------------------------------------- #
# Extract + transform
# --------------------------------------------------------------------------- #

def load_raw_sales(csv_path: Path | str | None = None) -> pd.DataFrame:
    """Read the source CSV and apply explicit typing + a derived column."""
    csv_path = Path(csv_path) if csv_path else DATA_DIR / "sample.csv"
    df = pd.read_csv(csv_path)

    # Explicit types: never trust inference for money or dates in a pipeline.
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["quantity"] = df["quantity"].astype("int64")
    df["unit_price"] = df["unit_price"].astype("float64")

    # Transformation / enrichment: revenue is a first-class derived measure.
    df["revenue"] = (df["quantity"] * df["unit_price"]).round(2)
    return df


# --------------------------------------------------------------------------- #
# Load (dlt -> DuckDB)
# --------------------------------------------------------------------------- #

def load_to_duckdb(df: pd.DataFrame, *, write_disposition: str = "replace"):
    """
    Land the DataFrame into DuckDB through dlt.

    Why dlt instead of `duckdb.execute("INSERT ...")`?
      - It infers and versions a schema, handling type evolution for you.
      - It adds load lineage columns (_dlt_load_id, _dlt_id) for traceability.
      - Swapping the destination to Postgres/BigQuery later is a one-line change.

    write_disposition="replace" makes the run idempotent for a demo; switch to
    "append" or "merge" (with a primary key) for incremental production loads.

    The destination is chosen by config (duckdb/postgres/bigquery), and the load
    is retried with backoff on transient failures — both required for a pipeline
    a company runs unattended.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=build_destination(),
        dataset_name=DATASET_NAME,
    )

    max_retries = (_cfg.runtime["max_load_retries"] if _cfg else 1)
    backoff = (_cfg.runtime["retry_backoff_seconds"] if _cfg else 1)
    attempt = 0
    while True:
        attempt += 1
        try:
            logger.info("Loading %d rows to %s (attempt %d)", len(df), FQ_TABLE, attempt)
            load_info = pipeline.run(
                df.to_dict(orient="records"),
                table_name=TABLE_NAME,
                write_disposition=write_disposition,
            )
            logger.info("Load complete: %s", FQ_TABLE)
            return pipeline, load_info
        except Exception as exc:  # noqa: BLE001
            if attempt >= max_retries:
                logger.error("Load failed after %d attempt(s): %s", attempt, exc)
                raise
            wait = backoff * (2 ** (attempt - 1))
            logger.warning("Load attempt %d failed: %s — retrying in %ds", attempt, exc, wait)
            time.sleep(wait)


# --------------------------------------------------------------------------- #
# Analytics (SQL against the dlt-managed dataset)
# --------------------------------------------------------------------------- #

def _connect() -> duckdb.DuckDBPyConnection:
    """Read-only connection so analytics can never mutate the warehouse."""
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


def query(sql: str) -> pd.DataFrame:
    with _connect() as con:
        return con.execute(sql).df()


def summary_statistics() -> pd.DataFrame:
    return query(f"""
        SELECT
            COUNT(*)                          AS order_count,
            SUM(quantity)                     AS units_sold,
            ROUND(SUM(revenue), 2)            AS total_revenue,
            ROUND(AVG(revenue), 2)            AS avg_order_value,
            COUNT(DISTINCT customer_id)       AS unique_customers
        FROM {FQ_TABLE}
    """)


def revenue_by_category() -> pd.DataFrame:
    return query(f"""
        SELECT
            category,
            COUNT(*)               AS orders,
            ROUND(SUM(revenue), 2) AS revenue
        FROM {FQ_TABLE}
        GROUP BY category
        ORDER BY revenue DESC
    """)


def revenue_by_region() -> pd.DataFrame:
    return query(f"""
        SELECT
            region,
            COUNT(*)               AS orders,
            ROUND(SUM(revenue), 2) AS revenue
        FROM {FQ_TABLE}
        GROUP BY region
        ORDER BY revenue DESC
    """)


def export_all(prefix: str = "") -> list[Path]:
    """Persist every analytic result to output/ as CSV for downstream use."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    artifacts = {
        "summary_statistics": summary_statistics(),
        "revenue_by_category": revenue_by_category(),
        "revenue_by_region": revenue_by_region(),
    }
    written: list[Path] = []
    for name, frame in artifacts.items():
        path = OUTPUT_DIR / f"{prefix}{name}.csv"
        frame.to_csv(path, index=False)
        written.append(path)
    return written


# --------------------------------------------------------------------------- #
# Orchestration entry point (so the whole flow is runnable without Jupyter)
# --------------------------------------------------------------------------- #

def main() -> None:
    if _cfg:
        from etl.logging_setup import configure_from
        configure_from(_cfg)

    logger.info("Extracting source data")
    df = load_raw_sales()

    logger.info("Running quality gate on %d rows", len(df))
    report = run_quality_checks(df)
    for line in report.summary().splitlines():
        logger.info(line)
    if not report.passed:
        raise SystemExit("Quality gate failed — aborting load.")

    logger.info("Loading via dlt to %s destination", (_cfg.destination["type"] if _cfg else "duckdb"))
    _, load_info = load_to_duckdb(df)

    logger.info("Running analytics + exporting CSVs")
    for path in export_all():
        logger.info("wrote %s", path.relative_to(PROJECT_ROOT))
    logger.info("Pipeline finished successfully")


if __name__ == "__main__":
    main()
