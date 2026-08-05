# Multi-Source → Columnar → BI

How a client with a **Postgres DB, a MySQL DB, and a CSV file** loads all three
into a columnar store and gets BI-ready output — efficiently.

Run it: `python -m etl.cli run-multi` (or `python -m demos.multi_source_pipeline`).

## The architecture

```
Postgres  (customers)    ─┐   incremental cursor + merge
MySQL     (transactions) ─┼─▶ dlt ─▶ DuckDB columnar warehouse  (client_raw.*)
CSV       (products)     ─┘                     │
                                                ├─▶ SQL model ─▶ bi.sales_mart
                                                └─▶ COPY TO Parquet (ZSTD)
                                                            │
                                                            ▼
                                              Power BI / Tableau / Athena / Spark
```

Each source is one dlt resource. All land in **one dataset** so the BI model can
join them. The conformed mart is built with SQL, then materialized to a
compressed columnar **Parquet** file that any BI tool or query engine reads
without a live warehouse.

## Why this is the efficient pattern

1. **Incremental extraction.** Each SQL source has a `dlt.sources.incremental`
   cursor (`updated_at`). Only rows changed since the last run are pulled —
   dlt pushes the watermark into the query (`WHERE updated_at >= :last_value`),
   so you move deltas, not whole tables.
2. **Merge (upsert).** `write_disposition="merge"` + primary key means re-runs
   update existing rows instead of duplicating. Verified idempotent: re-running
   with no new data leaves row counts unchanged.
3. **Columnar storage.** DuckDB (and the Parquet export) are columnar — BI
   queries scan only the columns they need, compressed with ZSTD. The demo mart
   is 12 columns in ~2 KB.
4. **Separation of raw and model.** Raw tables (`client_raw.*`) preserve source
   fidelity + dlt lineage; the BI mart (`bi.sales_mart`) is the conformed,
   business-facing model. BI tools point at the mart/Parquet, never raw tables.
5. **One columnar file as the hand-off.** Parquet is the universal BI/lake
   interchange format — no vendor lock-in, no always-on warehouse required.

## Connecting REAL databases

SQLite is only a stand-in so the demo runs serverless. For real systems, change
**only the connection string** (and install the driver):

```python
# multi_source_pipeline.py
CRM_DSN   = "postgresql+psycopg2://user:pass@host:5432/crm"      # pip install psycopg2-binary
SALES_DSN = "mysql+pymysql://user:pass@host:3306/sales"          # pip install pymysql
```

In production, don't hard-code these — read them from env via `config.secret(...)`:

```bash
export PIPELINE__CRM_DSN="postgresql+psycopg2://..."
export PIPELINE__SALES_DSN="mysql+pymysql://..."
```

The extraction, merge, model, and export code is identical regardless of engine.

## Choosing the columnar destination

| Destination | When | Change needed |
|---|---|---|
| **DuckDB** (default) | Small–mid data, single node, cheap/local BI | none |
| **Parquet on S3/GCS/ADLS** | Data lake, query with Athena/Trino/Spark | dlt `filesystem` destination + `loader_file_format="parquet"` |
| **BigQuery / Snowflake / Redshift** | Cloud-scale, many concurrent BI users | swap dlt destination + creds |
| **ClickHouse** | High-throughput real-time analytics | dlt `clickhouse` destination |

Because the source and model code don't change, moving from the DuckDB demo to a
cloud warehouse is a destination + credentials change, not a rewrite.

> **Note on Parquet + upserts:** plain Parquet files are append-only — you can't
> update a row in place. That's why raw upserts land in a warehouse (DuckDB here)
> and Parquet is the *output* of the modeled mart. If you need upserts directly
> on a lake, use a table format (Delta Lake / Iceberg), which dlt also supports.

## Scheduling the run

`python -m etl.cli run-multi` is one idempotent command. Wrap it in:
- **cron / systemd timer** — simplest.
- **Airflow / Dagster / Prefect** — when you need DAGs, retries, backfills.
- **Kubernetes CronJob** — run the container image on a schedule.

## Verified in this repo
Three sources (2 SQL + 1 CSV) ingest into the DuckDB warehouse with merge; the
BI mart joins all three; export produces a ZSTD Parquet file; a BI aggregate is
read straight from that Parquet. Re-running is idempotent (no duplicate rows).
