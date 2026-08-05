# Data Pipeline Platform — row-oriented sources → Delta Lake → BI

A generic, config-driven ETL platform that ingests data from a company's
operational systems — **Postgres, MySQL, Oracle, CSV** — cleans and reshapes it,
and lands it in a columnar, BI-ready store (**Delta Lake**) using pipelines that
are **incremental, idempotent, scheduled, and adaptable to any table without
writing new code**.

> **Stack:** Python 3.9+ · Delta Lake (delta-rs) · DuckDB · SQLAlchemy · pandas · Airflow · (optional) Spark

---

## What it does

```
Postgres ┐
MySQL    ┼─ extract (incremental) ─ transform (pluggable) ─ MERGE ─▶ Delta "silver" tables
Oracle   │                                                              │
CSV      ┘                                                   SQL model ─▶ Delta "gold" mart ─▶ BI
```

Each source table is declared in one small YAML file. A generic engine extracts
only changed rows (high-water-mark cursor), applies the table's transforms, and
upserts into Delta Lake via `MERGE` — so re-runs never duplicate. Silver tables
are then joined by SQL into report-ready **gold** tables that BI tools read.

---

## Project structure

The layout separates four things a senior reviewer expects to find apart: the
reusable **framework**, the **declarations** that customize it, the **earlier
demos** that show how it evolved, and the **docs**.

```
data-pipeline-platform/
│
├── etl/                       # ⭐ THE CORE FRAMEWORK (the product) — import root `etl`
│   ├── engine.py              #   generic extract → transform → load runner
│   ├── sources.py             #   extractors: Postgres/MySQL/Oracle (SQLAlchemy) + CSV (pandas)
│   ├── transforms.py          #   pluggable per-table operations (a name→function registry)
│   ├── delta_store.py         #   Delta Lake I/O: idempotent MERGE upserts, watermark reads
│   ├── registry.py            #   loads + validates the YAML pipeline specs
│   ├── graph.py               #   builds the dependency graph (single source of truth)
│   ├── local_runner.py        #   runs the graph in dependency order
│   ├── spark_engine.py        #   OPTIONAL scale-out engine (same interface, for TB-scale)
│   ├── settings.py            #   paths + connection resolution
│   ├── config.py              #   layered config loader (dev/prod profiles + env overrides)
│   ├── logging_setup.py       #   structured logging (human text / JSON)
│   └── cli.py                 #   operational entry point →  python -m etl.cli
│
├── pipelines/                 # DECLARATIONS — the customization surface (no code to add a table)
│   ├── customers.yaml         #   one spec per source table …
│   ├── transactions.yaml
│   ├── products.yaml
│   ├── sales_mart.yaml        #   … and one per gold BI model
│   └── examples/
│       └── oracle_gl.yaml     #   Oracle source example
│
├── dags/
│   └── etl_dag.py             # Airflow DAG, generated from the pipeline registry
│
├── demos/                     # EARLIER single-file pipelines — the project's evolution
│   ├── pipeline.py            #   batch pipeline (dlt → DuckDB) with quality gate + analytics
│   ├── incremental_pipeline.py#   incremental (cursor + merge) loading, dlt-native
│   ├── incremental_demo.py    #   multi-run proof of incremental behaviour
│   ├── multi_source_pipeline.py#  multi-source → Parquet demo
│   ├── run_local_demo.py      #   seeds stand-in sources, then runs the full etl/ graph
│   └── seeds/                 #   demo-data generators (SQLite stand-ins for the DBs, CSV)
│       ├── seed_sources.py
│       └── seed_oracle.py
│
├── scripts/
│   └── smoke_test.py          # post-install environment check (python -m scripts.smoke_test)
│
├── tests/                     # pytest suite (23 tests: quality, config, incremental, etl, oracle)
├── notebooks/
│   └── data_workflow.ipynb    # interactive walkthrough of the batch demo
├── data/
│   └── sample.csv             # sample dataset for the demos
├── docs/                      # deep-dive documentation (see index below)
│
├── config.yaml                # pipeline configuration (profiles, destination, quality rules)
├── requirements.txt           # pinned runtime dependencies
├── requirements-dev.txt       #   + test / lint tooling
├── Dockerfile                 # container image (non-root, multi-stage)
├── docker-compose.yml
├── Makefile                   # make setup | test | run | docker
├── setup.sh / setup.bat       # one-command environment setup
├── pytest.ini / ruff.toml     # test + lint config
├── .github/workflows/ci.yml   # CI: lint + tests + image build on push/PR
├── .dlt/config.toml           # dlt runtime settings (telemetry disabled)
└── .env.example               # environment-variable template (copy to .env)
```

**How to read it:** start in `etl/` — that's the reusable engine. `pipelines/`
is where a company adapts it to their tables (declarations, not code). `dags/`
schedules it. `demos/` are the earlier, self-contained pipelines kept to show
how the design evolved — they are *not* the product. Everything else is standard
project scaffolding.

---

## Quick start

```bash
# 1. Set up (creates ./env, installs pinned deps, runs the smoke test)
./setup.sh              # Linux / macOS      (setup.bat on Windows)
source env/bin/activate

# 2. Run the generic framework end to end (seeds stand-in sources -> Delta -> BI)
python -m etl.cli run-etl

# 3. Inspect resolved configuration
python -m etl.cli info

# 4. Run the tests
make test               # or: pytest
```

Other entry points:

```bash
python -m etl.cli run-oracle          # Oracle source example
python -m etl.cli run-multi           # multi-source -> Parquet demo
python -m etl.cli run                 # the original batch demo (dlt -> DuckDB)
python -m demos.run_local_demo        # same as run-etl, invoked directly
jupyter notebook notebooks/data_workflow.ipynb
```

### Point it at a real database (no code changes)

Set the connection string in the env var the spec names, then run:

```bash
export CRM_DSN="postgresql+psycopg2://user:pass@host:5432/crm"   # pip install psycopg2-binary
export SALES_DSN="mysql+pymysql://user:pass@host:3306/sales"     # pip install pymysql
export ORACLE_DSN="oracle+oracledb://user:pass@host:1521/?service_name=ORCLPDB1"  # pip install oracledb
python -m etl.cli run-etl
```

### Onboard a new table

Drop a YAML file in `pipelines/` (source, primary key, cursor, transforms,
target). No engine code changes — the Airflow DAG picks it up automatically.
See [docs/ETL_FRAMEWORK.md](docs/ETL_FRAMEWORK.md).

---

## Documentation

| Doc | Covers |
|---|---|
| [docs/ETL_FRAMEWORK.md](docs/ETL_FRAMEWORK.md) | The generic framework: architecture, onboarding a table, source types (incl. Oracle), the Spark decision rule, scheduling, and an honest gap analysis. |
| [docs/PRODUCTION.md](docs/PRODUCTION.md) | Deployment, retargeting to a client warehouse, and what a real engagement still requires. |
| [docs/INCREMENTAL.md](docs/INCREMENTAL.md) | The incremental (high-water-mark + merge) loading model, with proven behaviour. |
| [docs/MULTISOURCE.md](docs/MULTISOURCE.md) | Combining multiple sources into a columnar BI model. |
| [docs/PROJECT_SUMMARY.md](docs/PROJECT_SUMMARY.md) | One-page technical summary. |

---

## Key design decisions

- **Delta Lake** for storage — Parquet data files + an ACID transaction log, so
  `MERGE` gives idempotent upserts and the same files are read by DuckDB, Spark,
  Trino, Athena and Power BI.
- **Config-driven** — new tables are YAML declarations, not new code.
- **Spark is optional, not default** — the single-node engine (delta-rs + DuckDB)
  handles tens of GB with zero cluster; Spark sits behind the same interface for
  genuine TB-scale.
- **Idempotent** — incremental watermark + `MERGE` on the primary key means
  retries and re-runs reproduce identical results.

---

## What's hardened vs. what still needs work

**Implemented for real data:** a config-driven **quality gate** in the engine
(abort / warn / quarantine bad rows before load), **money stored as exact
decimal**, an incremental **lag window** that recovers late-arriving rows, and
**CDC soft-delete** handling. All are covered by the test suite (30 tests).

**Still needed for a hardened production deployment** :
hard-delete detection without a source flag (needs periodic full reconciliation
or log-based CDC); schema-drift contracts; chunked extraction reading from a
**replica** rather than the OLTP primary; a run-audit/freshness table and
alerting; and validating the Spark path on a real cluster. See the gap analyses
in [docs/ETL_FRAMEWORK.md](docs/ETL_FRAMEWORK.md) and
[docs/PRODUCTION.md](docs/PRODUCTION.md).

---

## License

MIT — use freely.
