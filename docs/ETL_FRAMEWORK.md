# Generic ETL Framework — Row-oriented sources → Delta Lake → BI

A declarative, config-driven platform a company points at its own Postgres,
MySQL, and CSV sources to land report-ready data in columnar storage. Adding a
table is a YAML file; adding a custom operation is a registered function. No core
code changes.

Run the demo: `python -m etl.cli run-etl` · Tests: `pytest tests/test_etl.py`

## Architecture

```
Postgres ┐
MySQL    ┼─ extract (incremental) ─ transform (pluggable) ─ MERGE ─▶ Delta "silver" tables
CSV      ┘                                                              │
                                                          SQL model ─▶ Delta "gold" mart ─▶ BI
```

Medallion layout on disk:
- `output/delta/silver/<table>` — one Delta table per source table (raw-but-typed).
- `output/delta/gold/<table>` — conformed BI models the reports read.

Each Delta table is Parquet data files plus a `_delta_log` transaction log.

## The four decisions (and why)

**Columnar storage = Delta Lake.** Parquet under the hood + an ACID log. Its
`MERGE` gives idempotent upserts; the same files are read by DuckDB, Spark,
Trino, Athena and Power BI. Iceberg would also be valid — Delta was chosen
because delta-rs performs MERGE with no Spark, and it pairs natively with Spark
when you do scale up.

**Spark = optional, not default.** The default engine (delta-rs + DuckDB) is
single-node and handles tens of GB with zero cluster to run. Forcing Spark on
GB-scale data is over-engineering. `etl/spark_engine.py` implements the *same*
interface for TB-scale, writing the *same* Delta tables — scale up without
changing the storage contract or the BI layer.

Decision rule:

| Situation | Engine |
|---|---|
| Sources up to ~tens of GB, simple/moderate transforms | default (delta-rs) |
| Hundreds of GB–TB, memory-heavy joins, existing Spark/Databricks | `spark_engine` |

**Generic + declarative.** Pipelines are YAML specs in `pipelines/`. The generic
engine (`etl/engine.py`) runs any spec; it knows nothing table-specific.

**Idempotent.** Incremental watermark (max cursor read from the target itself —
no external state store) + Delta `MERGE` on the primary key. Re-running any task
or backfilling any date reproduces the same result. Verified in tests: a re-run
leaves row counts unchanged; updating a source row upserts in place.

## Onboarding a new table (the whole workflow)

1. Drop a YAML file in `pipelines/`:

```yaml
kind: ingest
name: invoices
source:
  type: postgres            # postgres | mysql | sqlite | csv
  dsn_env: FINANCE_DSN      # connection read from env, never in the repo
  table: invoices
extract:
  incremental_cursor: modified_at
target:
  table: invoices
  primary_key: invoice_id
  write_mode: merge          # merge (upsert) | overwrite
transforms:
  - normalize_columns
  - add_ingested_at
```

2. If it needs table-specific logic, register a transform and list it:

```python
from etl.transforms import transform

@transform("mask_pii")
def _mask(df):
    df = df.copy()
    df["email"] = df["email"].str.replace(r"@.*", "@***", regex=True)
    return df
```

That's it. The Airflow DAG picks the new spec up automatically (it's built from
the registry), so it starts running on schedule with no DAG edit.

## Connecting real databases

The demo uses SQLite stand-ins so it runs serverless. For real systems, only the
connection string changes — set the env vars the specs reference:

```bash
export FINANCE_DSN="postgresql+psycopg2://user:pass@host:5432/finance"   # pip install psycopg2-binary
export SALES_DSN="mysql+pymysql://user:pass@host:3306/sales"             # pip install pymysql
export ORACLE_DSN="oracle+oracledb://user:pass@host:1521/?service_name=ORCLPDB1"  # pip install oracledb
```

## Supported source types

| `source.type` | Reader | Notes |
|---|---|---|
| `postgres` / `mysql` / `oracle` / `sqlite` | SQLAlchemy | One code path; only the DSN + driver differ. Incremental cursor pushed into the query. |
| `csv` | pandas | Flat, one table per file. |

### Oracle
Oracle rides the same SQLAlchemy path as the other SQL engines. Two Oracle-isms
to know: identifiers are stored UPPERCASE (so source column/table names are
uppercase, and `normalize_columns` lowercases them into silver), and tables are
often schema-qualified — the spec accepts an optional `schema:` key. The
`oracledb` driver's thin mode needs no Oracle Instant Client. See
`pipelines/examples/oracle_gl.yaml`. Try it: `python -m etl.cli run-oracle`.

## Scheduling with Airflow

`dags/etl_dag.py` builds the DAG from the same graph the runner uses
(`etl/graph.py`), so there's one source of truth. Ingest tasks run in parallel;
gold models wait for their inputs. It ships with retries + exponential backoff, a
nightly schedule, and `catchup`/`max_active_runs` set sanely. Because tasks are
idempotent, Airflow retries and backfills are safe.

Deploy: put the repo on the Airflow host (or bake it into the worker image), set
the source connection env vars on the workers (ideally via Airflow Connections /
a secrets backend), and the DAG appears in the UI with per-task logs, run history
and failure alerting.

> Airflow is intentionally **not** installed in this repo's environment: it
> constrains SQLAlchemy/pandas to versions older than the ones the rest of the
> repo is tested against. The DAG is standard Airflow and the orchestration logic
> it runs is proven end-to-end by `etl/local_runner.py` (same graph, same
> `run_by_name`), exercised in the test suite.

## Reading the output for BI

The gold tables are Delta (Parquet). Point BI at them:
- **DuckDB / Python:** read via `deltalake` → Arrow (as the tests do), or DuckDB's `delta_scan`.
- **Power BI / Tableau / Fabric:** native Delta/Parquet connectors, or via a query engine.
- **Athena / Trino / Spark:** register the Delta location as an external table.

## Real-data hardening (implemented)

These address the failure modes that surface on real operational data:

- **Data-quality gate (in the engine).** Declare per-pipeline rules; they run
  *before* the load. `on_fail` chooses the behaviour:
  ```yaml
  quality:
    not_null: [customer_id, updated_at]
    unique:   [customer_id]     # the primary key is always enforced too
    positive: [amount, quantity]
    on_fail:  abort             # abort | warn | quarantine
  ```
  `quarantine` routes bad rows to a `<table>__rejects` Delta table and loads the
  good ones; `abort` fails the batch; `warn` logs and loads.
- **Money as exact decimal.** List money columns and they're stored as
  `decimal(18,2)`, not float, so totals reconcile to the penny:
  ```yaml
  money_columns: [amount]
  ```
- **Late-arriving data (lag window).** `extract.lag_seconds` re-reads a window
  behind the watermark each run; MERGE dedups, so late/out-of-order rows are
  recovered instead of lost:
  ```yaml
  extract:
    incremental_cursor: updated_at
    lag_seconds: 86400          # re-read 1 day back
  ```
- **CDC soft-delete.** If the source flags deletes, name the column and the
  engine removes those rows from the target (ACID Delta `DELETE`):
  ```yaml
  target:
    soft_delete_column: is_deleted
  ```

## Honest gaps (what a real engagement still needs)

- **Hard deletes without a flag** still need a periodic full reconciliation
  (`when_not_matched_by_source_delete`) or log-based CDC (Debezium/GoldenGate);
  the lag window and soft-delete cover the common cases, not a source row that
  simply vanishes with no tombstone.
- **Schema evolution:** stable schemas are assumed. delta-rs supports
  `schema_mode="merge"`; wire it per-table where sources drift, plus a contract.
- **Large-volume extraction** still materialises each batch in memory; add
  chunked reads (`chunksize`) and read from a **replica**, not the OLTP primary.
- **Spark path is provided, not load-tested here** — validate on your cluster,
  and port the pandas transforms to Spark DataFrame ops for engine parity.
- **Observability:** logging is structured, but add a run-audit table
  (rows in/out, status, freshness) and alerting for unattended operation.

## Verified in this repo
`pytest` (30 tests) green, `ruff` clean: transforms, an acyclic graph ordering
the model last, a full 3-source run producing silver + gold Delta tables,
idempotent re-runs, incremental upsert (not append), the quality gate
(abort + quarantine), decimal money, the lag window recovering a late row, and
CDC soft-delete. Delta output is Parquet + `_delta_log`, read back through DuckDB.
