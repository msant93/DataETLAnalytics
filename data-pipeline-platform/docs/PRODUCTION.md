# Production Guide

This document is for a team evaluating whether to run this pipeline against real
data. It covers how to deploy it, how to retarget it, and — just as important —
what is **not** yet solved so there are no surprises.

## What makes this deployable (not just a demo)

| Capability | How it's implemented | Why a buyer cares |
|---|---|---|
| Retarget without code changes | `config.yaml` + `PIPELINE__*` env overrides; `build_destination()` selects duckdb/postgres/bigquery | Point at *their* warehouse in minutes, not a rewrite |
| Environment separation | `APP_ENV=dev\|staging\|prod` profiles, deep-merged | Same artifact promoted through environments |
| Secrets stay out of the repo | Read from env at load time (`secrets_env` mapping); validated at startup | Passes security review; works with Vault/CI secrets |
| Structured logging | `logging_setup.py` — text for humans, JSON for prod | Ships to Datadog/CloudWatch/ELK; alertable |
| No surprise egress | dlt telemetry disabled in `.dlt/config.toml` | Data-governance / compliance requirement |
| Fail-fast data quality | Quality gate before load; aborts on violation | Bad data never reaches the warehouse |
| Incremental + upsert | High-water-mark cursor + `merge` | Scales past full reloads; handles updates |
| Idempotent & retried | `replace`/`merge` semantics + retry-with-backoff on load | Safe to re-run; survives transient failures |
| Tested | `pytest` suite (unit + integration + incremental) | Regressions caught before deploy |
| Reproducible builds | Pinned deps, `venv`, multi-stage `Dockerfile` (non-root) | Runs the same on any host |
| CI-gated | GitHub Actions: lint + tests + image build on 3 Python versions | Every change is verified |
| One operational entry point | `etl.cli` (`info`/`validate`/`run`) | Ops schedules one command |

## Deploying

```bash
# Container (recommended)
docker build -t data-pipeline .
docker run --rm \
  -e APP_ENV=prod \
  -e PIPELINE__DESTINATION__TYPE=postgres \
  -e PIPELINE__POSTGRES_DSN="$POSTGRES_DSN" \
  -v "$PWD/output:/app/output" \
  data-pipeline run

# Or bare-metal / VM
APP_ENV=prod python -m etl.cli run
```

### Retargeting to a client's warehouse
1. Set `destination.type` (config or `PIPELINE__DESTINATION__TYPE`).
2. Provide credentials via the env var named in `secrets_env`.
3. Point `source` at their system (`csv` → `sql_database`/`rest_api`).
That's it — the load, quality, and analytics code is unchanged.

## Scheduling
The pipeline is a single idempotent command, so any scheduler works:
- **Cron / systemd timer** — simplest; fine for one pipeline.
- **Airflow / Dagster / Prefect** — recommended once you have DAGs, retries,
  backfills, and dependencies between pipelines. Wrap `etl.cli run` in a task.
- **Kubernetes CronJob** — run the container image on a schedule.

## Honest gap analysis (what a real client engagement still needs)

These are deliberately **not** built, because they depend on the client's stack
and requirements. Naming them is part of being trustworthy:

- **Orchestration** — no DAG/scheduler is included; you choose and wire one.
- **Deletes / CDC completeness** — the incremental cursor captures inserts and
  updates, not hard deletes. Needs a soft-delete flag, dlt `hard_delete` hint,
  or SCD2 depending on requirements.
- **Schema-contract enforcement** — dlt evolves schemas permissively. For strict
  contracts, pin `dlt` schema contracts (`freeze`/`evolve` modes) per table.
- **Secrets backend** — env vars are the interface; wiring to Vault/AWS Secrets
  Manager is a deployment detail.
- **Observability beyond logs** — no metrics/traces or alerting rules yet; emit
  run metrics to your monitoring system and define SLAs.
- **Data volume** — DuckDB is single-node. For large-scale or concurrent
  production loads, target a warehouse (Postgres/BigQuery/Snowflake) — the
  config seam already supports this; it just needs validation on real volumes.
- **Access control / PII** — no masking, row-level security, or audit beyond
  dlt's load lineage. Add per the client's compliance regime.

## Verified in this repo
`pytest` (23 tests) green; `ruff` clean; `etl.cli validate` passes; the notebook
executes top-to-bottom with zero errors; batch and incremental runs both produce
correct results. The Docker image and CI workflow are provided but are built by
your CI, not in this authoring environment.
