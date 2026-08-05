# Project Summary — Local Data Engineering Environment

## One-line description
A reproducible, laptop-local ELT pipeline that ingests sales data with **dlt**,
lands it in **DuckDB** under a managed schema, gates it with data-quality checks,
and serves SQL analytics through a **Jupyter** notebook.

## Problem it solves
Getting hands-on with a modern data stack usually means standing up cloud
warehouses and paying for compute. This project delivers the same core workflow
— schema-managed ingestion, an analytical warehouse, quality gating, and
notebook-driven analysis — entirely locally and for free, so it's ideal for
learning, prototyping, and small personal datasets.

## Architecture at a glance
```
CSV source ──▶ extract + type (pandas) ──▶ QUALITY GATE ──▶ dlt load ──▶ DuckDB (sales.orders)
                                                │ (fail = abort)              │
                                                ▼                            ▼
                                          QualityReport             SQL analytics (read-only)
                                                                            │
                                                                            ▼
                                                                    output/*.csv
```

## Key engineering decisions
1. **Logic in a module, not the notebook.** `pipeline.py` holds all business
   logic so it can be diffed, imported, and tested. The notebook only orchestrates.
2. **Quality gate before load.** Bad data is rejected up front rather than
   cleaned after it has already entered the warehouse.
3. **dlt for ingestion.** Schema inference/versioning and load lineage come for
   free, and the destination is swappable to Postgres/BigQuery in one line.
4. **DuckDB for compute.** Zero-ops, in-process OLAP with full SQL — no server.
5. **Read-only analytics connections.** Exploration cannot mutate the warehouse.
6. **Exact-pinned dependencies + one-command setup.** Reproducible on any machine.
7. **A real smoke test.** `scripts/smoke_test.py` doesn't just import packages — it runs
   a full load-and-query cycle and checks the row count.

## Technologies
Python 3.9+ · dlt 1.29 (DuckDB destination) · DuckDB 1.5 · pandas 3.0 · Jupyter

## What it demonstrates to a hiring manager
- Modern ELT patterns (extract → validate → load → transform → serve)
- Schema-aware warehouse work, including dlt's namespacing and lineage columns
- Data-quality engineering with structured, testable checks
- Software-engineering discipline applied to data (modular code, pinned deps,
  smoke tests, idempotent runs, cross-platform automation)
- Awareness of the production path (incremental loads, real sources, CI)

## Verified
The pipeline, validation script, and notebook were all executed end-to-end:
10 records load into `sales.orders`, all six quality checks pass, three analytic
CSVs are produced, and the notebook runs top-to-bottom with zero errors.
