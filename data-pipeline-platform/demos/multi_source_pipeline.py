"""
multi_source_pipeline.py
------------------------
Load THREE heterogeneous sources into one columnar store, build a conformed BI
model, and materialize it to Parquet (the columnar file BI tools consume).

    Postgres (customers)  ─┐
    MySQL    (transactions)├─▶ dlt (incremental + merge) ─▶ DuckDB columnar warehouse
    CSV      (products)   ─┘                                    │
                                                                ├─▶ bi.sales_mart (SQL model)
                                                                └─▶ output/bi/sales_mart.parquet

Why this shape:
  * Each source is a dlt resource with its own incremental cursor, so only
    changed rows move on each run (efficiency #1).
  * `merge` + primary key gives upserts, so re-runs don't duplicate (correctness).
  * Everything lands in ONE dataset so the BI model can join across sources.
  * DuckDB is columnar and free; swap the destination to BigQuery/Snowflake/
    ClickHouse for scale — the source + model code is unchanged.
  * The BI mart is exported to Parquet: columnar, compressed, and readable by
    Power BI / Tableau / Athena / Spark / DuckDB without a live warehouse.

SQLite stands in for Postgres/MySQL here so the demo needs no servers. For real
systems, change ONLY the connection string:

    Postgres:  postgresql+psycopg2://user:pass@host:5432/dbname
    MySQL:     mysql+pymysql://user:pass@host:3306/dbname
    (install the matching driver: psycopg2-binary / pymysql)
"""

from __future__ import annotations

import logging
from pathlib import Path

import dlt
import duckdb
import pandas as pd
from dlt.sources.sql_database import sql_table

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "data" / "sources"
OUTPUT_DIR = PROJECT_ROOT / "output"
WAREHOUSE = OUTPUT_DIR / "client_warehouse.duckdb"
PARQUET_OUT = OUTPUT_DIR / "bi" / "sales_mart.parquet"

DATASET = "client_raw"

# Real deployments read these from config/env (config.secret(...)). Hard-coded
# here to the SQLite stand-ins purely for the runnable demo.
CRM_DSN = f"sqlite:///{SRC_DIR / 'crm.db'}"
SALES_DSN = f"sqlite:///{SRC_DIR / 'sales.db'}"
PRODUCTS_CSV = SRC_DIR / "products.csv"


# --------------------------------------------------------------------------- #
# Sources — one resource per system, bundled into a single dlt source
# --------------------------------------------------------------------------- #

@dlt.source(name="client_data")
def client_data():
    """All three source systems, extracted incrementally, as one dlt source."""

    # 1) POSTGRES stand-in: customers (SQL source, incremental on updated_at)
    customers = sql_table(
        credentials=CRM_DSN,
        table="customers",
        incremental=dlt.sources.incremental("updated_at"),
    )
    customers.apply_hints(write_disposition="merge", primary_key="customer_id")

    # 2) MYSQL stand-in: transactions (SQL source, incremental on updated_at)
    transactions = sql_table(
        credentials=SALES_DSN,
        table="transactions",
        incremental=dlt.sources.incremental("updated_at"),
    )
    transactions.apply_hints(write_disposition="merge", primary_key="txn_id")

    # 3) CSV: products dimension (small; full merge on the natural key)
    @dlt.resource(name="products", write_disposition="merge", primary_key="product_sku")
    def products():
        df = pd.read_csv(PRODUCTS_CSV)
        df["unit_cost"] = df["unit_cost"].astype("float64")
        yield from df.to_dict(orient="records")

    return customers, transactions, products


# --------------------------------------------------------------------------- #
# Load
# --------------------------------------------------------------------------- #

def ingest():
    """Run all three sources into the columnar warehouse in one pipeline run."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    pipe = dlt.pipeline(
        pipeline_name="client_ingest",
        destination=dlt.destinations.duckdb(str(WAREHOUSE)),
        dataset_name=DATASET,
    )
    info = pipe.run(client_data())
    logger.info("Ingest complete: %s", [r for r in info.loads_ids])
    return pipe, info


# --------------------------------------------------------------------------- #
# BI model + columnar export
# --------------------------------------------------------------------------- #

BI_MODEL_SQL = f"""
CREATE SCHEMA IF NOT EXISTS bi;
CREATE OR REPLACE TABLE bi.sales_mart AS
SELECT
    t.txn_id,
    t.txn_ts::DATE                          AS txn_date,
    c.customer_id,
    c.name                                  AS customer_name,
    c.region,
    p.product_sku,
    p.product_name,
    p.category,
    t.quantity,
    t.amount                                AS revenue,
    ROUND(p.unit_cost * t.quantity, 2)      AS cost,
    ROUND(t.amount - p.unit_cost * t.quantity, 2) AS gross_margin
FROM {DATASET}.transactions t
JOIN {DATASET}.customers c USING (customer_id)
JOIN {DATASET}.products  p USING (product_sku);
"""


def build_bi_model():
    """Join the three sources into a single conformed mart, inside DuckDB."""
    with duckdb.connect(str(WAREHOUSE)) as con:
        con.execute(BI_MODEL_SQL)
        rows = con.execute("SELECT COUNT(*) FROM bi.sales_mart").fetchone()[0]
    logger.info("Built bi.sales_mart (%d rows)", rows)
    return rows


def export_parquet() -> Path:
    """Materialize the BI mart to a compressed, columnar Parquet file."""
    PARQUET_OUT.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(WAREHOUSE)) as con:
        con.execute(
            f"COPY bi.sales_mart TO '{PARQUET_OUT}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    logger.info("Exported columnar file: %s", PARQUET_OUT)
    return PARQUET_OUT


# --------------------------------------------------------------------------- #
# BI-style query straight off the Parquet file (no live warehouse needed)
# --------------------------------------------------------------------------- #

def bi_report_from_parquet() -> pd.DataFrame:
    """Simulate a BI tool reading the columnar file: revenue & margin by region."""
    with duckdb.connect() as con:  # in-memory; reads parquet directly
        return con.execute(
            f"""
            SELECT region,
                   category,
                   COUNT(*)                 AS orders,
                   SUM(quantity)            AS units,
                   ROUND(SUM(revenue), 2)   AS revenue,
                   ROUND(SUM(gross_margin),2) AS gross_margin
            FROM read_parquet('{PARQUET_OUT}')
            GROUP BY region, category
            ORDER BY revenue DESC
            """
        ).df()


def run_all():
    ingest()
    build_bi_model()
    export_parquet()
    return bi_report_from_parquet()


if __name__ == "__main__":
    from etl.logging_setup import configure
    configure(level="INFO")
    report = run_all()
    print("\nBI report (revenue & margin by region/category), read from Parquet:\n")
    print(report.to_string(index=False))
