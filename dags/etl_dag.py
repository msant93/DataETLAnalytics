"""
etl_dag.py — Airflow DAG for the ETL platform.

The DAG is generated from the SAME graph the local runner uses (etl/graph.py),
so there is one source of truth for "what runs and in what order". Each pipeline
spec becomes one task; ingest tasks (independent sources) run in parallel and the
gold model tasks wait for their inputs.

Deploy: drop this repo on the Airflow host (or bake it into the worker image),
set the source connection env vars (CRM_DSN, SALES_DSN, PRODUCTS_PATH) on the
workers — ideally from Airflow Connections/Variables or a secrets backend — and
this DAG appears in the UI on the schedule below with per-task retries, logs,
run history and alerting.

Because the tasks are idempotent (incremental watermark + Delta MERGE), Airflow
retries and backfills are safe: re-running any task or date reproduces the same
result rather than duplicating data.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.engine import run_by_name
from etl.graph import build_graph

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "depends_on_past": False,
}

with DAG(
    dag_id="etl_row_to_delta",
    description="Row-oriented sources (Postgres/MySQL/CSV) -> Delta Lake -> BI",
    default_args=default_args,
    schedule="0 2 * * *",          # nightly at 02:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "delta", "bi"],
) as dag:

    tasks: dict[str, PythonOperator] = {}
    nodes = build_graph()

    # Create one task per pipeline spec.
    for node in nodes:
        tasks[node.name] = PythonOperator(
            task_id=node.name,
            python_callable=run_by_name,
            op_args=[node.name],
        )

    # Wire dependencies from the graph.
    for node in nodes:
        for upstream in node.upstream:
            tasks[upstream] >> tasks[node.name]
