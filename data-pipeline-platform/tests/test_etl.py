"""Tests for the generic ETL framework (row-oriented -> Delta Lake)."""
from __future__ import annotations

import ast
import sqlite3

import pandas as pd
import pytest

from demos.seeds import seed_sources
from etl import delta_store, settings, transforms
from etl.graph import build_graph, topological_order
from etl.local_runner import run_all


@pytest.fixture
def platform(tmp_path, monkeypatch):
    """Isolated Delta root + freshly seeded SQLite/CSV sources."""
    monkeypatch.setattr(settings, "DELTA_ROOT", tmp_path / "delta")
    seed_sources.main()
    src = settings.PROJECT_ROOT / "data" / "sources"
    monkeypatch.setenv("CRM_DSN", f"sqlite:///{src / 'crm.db'}")
    monkeypatch.setenv("SALES_DSN", f"sqlite:///{src / 'sales.db'}")
    monkeypatch.setenv("PRODUCTS_PATH", str(src / "products.csv"))
    return src


# --------------------------- transforms --------------------------- #

def test_normalize_columns():
    df = pd.DataFrame({"Order Id": [1], " Name ": ["x"]})
    out = transforms.apply(["normalize_columns"], df)
    assert list(out.columns) == ["order_id", "name"]


def test_custom_transform_derive_line_totals():
    df = pd.DataFrame({"amount": [100.0], "quantity": [5]})
    out = transforms.apply(["derive_line_totals"], df)
    assert out.loc[0, "unit_price_effective"] == 20.0
    assert bool(out.loc[0, "is_bulk"]) is True


def test_unknown_transform_raises():
    with pytest.raises(KeyError):
        transforms.apply(["does_not_exist"], pd.DataFrame())


# --------------------------- graph --------------------------- #

def test_graph_is_acyclic_and_orders_model_last():
    order = topological_order(build_graph())
    assert set(order) == {"customers", "transactions", "products", "sales_mart"}
    assert order[-1] == "sales_mart"  # model runs after its inputs


# --------------------------- end-to-end --------------------------- #

def test_full_run_and_counts(platform):
    results = run_all()
    assert results == {"customers": 4, "products": 4, "transactions": 5, "sales_mart": 5}
    # gold is a real Delta table backed by parquet
    assert delta_store.row_count(settings.gold_path("sales_mart")) == 5


def test_idempotent_rerun(platform):
    run_all()
    before = delta_store.row_count(settings.silver_path("transactions"))
    run_all()  # no source change
    after = delta_store.row_count(settings.silver_path("transactions"))
    assert before == after == 5


def test_incremental_upsert_not_append(platform):
    run_all()
    con = sqlite3.connect(platform / "sales.db")
    con.execute(
        "UPDATE transactions SET quantity=9, amount=805.5, "
        "updated_at='2024-03-01T00:00:00' WHERE txn_id=5002"
    )
    con.commit()
    con.close()

    run_all()
    # row count unchanged -> it was an upsert, not an append
    assert delta_store.row_count(settings.silver_path("transactions")) == 5
    import duckdb
    mart = delta_store.read_arrow(settings.gold_path("sales_mart"))
    con = duckdb.connect()
    con.register("mart", mart)
    qty = con.execute("SELECT quantity FROM mart WHERE txn_id = 5002").fetchone()[0]
    con.close()
    assert qty == 9


# --------------------------- Airflow DAG (structural) --------------------------- #

def test_airflow_dag_parses_and_uses_shared_graph():
    """
    Validate the DAG file without importing Airflow (which would downgrade the
    repo's pinned deps). We parse it and assert it builds tasks from the shared
    graph — the orchestration logic itself is proven by the end-to-end tests
    above, since the DAG runs the exact same run_by_name over build_graph().
    """
    dag_src = (settings.PROJECT_ROOT / "dags" / "etl_dag.py").read_text()
    tree = ast.parse(dag_src)  # raises on syntax error
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    assert "build_graph" in names
    assert "run_by_name" in names
    assert "PythonOperator" in names
