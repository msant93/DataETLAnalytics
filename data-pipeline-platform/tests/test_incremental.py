"""Isolated incremental test: proves idempotency and upsert-not-duplicate.

Isolation strategy: monkeypatch the module's paths + a unique pipeline/dataset
name so dlt state and the DuckDB file are fresh for this test only.
"""
import csv
import uuid
from pathlib import Path

import duckdb
import pytest

from demos import incremental_pipeline as inc

FIELDS = ["order_id", "order_date", "customer_id", "product",
          "category", "region", "quantity", "unit_price", "updated_at"]

SEED = [
    {"order_id": 1, "order_date": "2024-01-01", "customer_id": "C1",
     "product": "A", "category": "Cat", "region": "North",
     "quantity": 2, "unit_price": 10.0, "updated_at": "2024-01-01T00:00:00"},
    {"order_id": 2, "order_date": "2024-01-02", "customer_id": "C2",
     "product": "B", "category": "Cat", "region": "West",
     "quantity": 1, "unit_price": 20.0, "updated_at": "2024-01-02T00:00:00"},
]


def _write(path: Path, rows):
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    uniq = uuid.uuid4().hex[:8]
    source = tmp_path / "src.csv"
    ddb = tmp_path / "wh.duckdb"
    monkeypatch.setattr(inc, "SOURCE_CSV", source)
    monkeypatch.setattr(inc, "DUCKDB_PATH", ddb)
    monkeypatch.setattr(inc, "PIPELINE_NAME", f"test_inc_{uniq}")
    monkeypatch.setattr(inc, "DATASET_NAME", f"sales_{uniq}")
    monkeypatch.setattr(inc, "FQ_TABLE", f"sales_{uniq}.{inc.TABLE_NAME}")
    # fresh dlt pipeline state per test
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / ".dlt"))
    return source


def _count(ddb, fq):
    with duckdb.connect(str(ddb), read_only=True) as con:
        return con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]


def test_incremental_idempotent_and_upsert(isolated):
    source = isolated

    # Run 1: initial load
    _write(source, SEED)
    inc.run_incremental()
    assert _count(inc.DUCKDB_PATH, inc.FQ_TABLE) == 2

    # Run 2: no change -> still 2 (idempotent)
    inc.run_incremental()
    assert _count(inc.DUCKDB_PATH, inc.FQ_TABLE) == 2

    # Run 3: update order 1 (new updated_at, qty 2 -> 5) -> upsert, still 2 rows
    updated = [dict(r) for r in SEED]
    updated[0]["quantity"] = 5
    updated[0]["updated_at"] = "2024-02-01T00:00:00"
    _write(source, updated)
    inc.run_incremental()
    assert _count(inc.DUCKDB_PATH, inc.FQ_TABLE) == 2

    with duckdb.connect(str(inc.DUCKDB_PATH), read_only=True) as con:
        qty = con.execute(
            f"SELECT quantity FROM {inc.FQ_TABLE} WHERE order_id = 1"
        ).fetchone()[0]
    assert qty == 5  # value updated in place, no duplicate row
