"""Tests for the real-data hardening: quality gate, decimal money, incremental
lag window, and CDC soft-delete."""
from __future__ import annotations

import sqlite3

import pyarrow as pa
import pytest

from demos.seeds import seed_sources
from etl import delta_store, engine, quality, settings
from etl.engine import _apply_lag
from etl.registry import IngestSpec, load_specs


@pytest.fixture
def platform(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DELTA_ROOT", tmp_path / "delta")
    seed_sources.main()
    src = settings.PROJECT_ROOT / "data" / "sources"
    monkeypatch.setenv("CRM_DSN", f"sqlite:///{src / 'crm.db'}")
    monkeypatch.setenv("SALES_DSN", f"sqlite:///{src / 'sales.db'}")
    monkeypatch.setenv("PRODUCTS_PATH", str(src / "products.csv"))
    return src


# ------------------------------ quality gate ------------------------------ #

def test_quality_check_flags_bad_rows():
    import pandas as pd
    df = pd.DataFrame({"id": [1, 1, 3], "amount": [10, -5, 20]})
    rules = {"unique": ["id"], "positive": ["amount"]}
    report = quality.check(df, rules, primary_key="id")
    assert not report.passed
    assert "unique:id" in report.failures
    assert "positive:amount" in report.failures
    assert report.bad_mask.tolist() == [True, True, False]


def test_quality_abort_raises(tmp_path, monkeypatch):
    # Unconstrained temp source so we can inject a NULL required field.
    monkeypatch.setattr(settings, "DELTA_ROOT", tmp_path / "delta")
    db = tmp_path / "q.db"
    con = sqlite3.connect(db)
    con.executescript(
        "CREATE TABLE t (id INTEGER, val TEXT, updated_at TEXT);"
        "INSERT INTO t VALUES (1, 'a', '2024-01-01T00:00:00');"
        "INSERT INTO t VALUES (2, 'b', NULL);"  # bad: null required field
    )
    con.commit()
    con.close()
    monkeypatch.setenv("Q_DSN", f"sqlite:///{db}")
    spec = IngestSpec(
        name="q",
        source={"type": "sqlite", "dsn_env": "Q_DSN", "table": "t"},
        target={"table": "q_silver", "primary_key": "id"},
        quality={"not_null": ["updated_at"], "on_fail": "abort"},
    )
    with pytest.raises(quality.QualityError):
        engine.run_ingest(spec)


def test_quality_quarantine_routes_bad_rows(platform):
    # Negative amount violates positive -> transactions spec is on_fail=quarantine
    con = sqlite3.connect(platform / "sales.db")
    con.execute("UPDATE transactions SET amount = -1 WHERE txn_id = 5002")
    con.commit()
    con.close()
    engine.run_ingest(load_specs()["transactions"])
    # 4 good rows loaded, 1 bad row quarantined
    assert delta_store.row_count(settings.silver_path("transactions")) == 4
    assert delta_store.row_count(settings.silver_path("transactions__rejects")) == 1


# ------------------------------ decimal money ------------------------------ #

def test_money_stored_as_decimal_and_exact(platform):
    engine.run_ingest(load_specs()["transactions"])
    t = delta_store.read_arrow(settings.silver_path("transactions"))
    assert pa.types.is_decimal(t.schema.field("amount").type)


# ------------------------------ lag window ------------------------------ #

def test_apply_lag_shifts_timestamp_and_number():
    assert _apply_lag("2024-01-15T16:20:00", 86400) == "2024-01-14T16:20:00"
    assert _apply_lag(1000, 100) == 900
    assert _apply_lag(None, 100) is None


def test_lag_recovers_late_arriving_row(platform):
    # First run establishes the watermark at the max updated_at (2024-01-15)
    engine.run_ingest(load_specs()["transactions"])
    base = delta_store.row_count(settings.silver_path("transactions"))

    # A row that arrives LATE: its updated_at (2024-01-15T10:00) is earlier than
    # the current max (2024-01-15T16:20) but within the 1-day lag window.
    con = sqlite3.connect(platform / "sales.db")
    con.execute(
        "INSERT INTO transactions VALUES "
        "(5999, 1, 'SKU-MOUSE', 1, 24.99, '2024-01-15', '2024-01-15T10:00:00')"
    )
    con.commit()
    con.close()

    engine.run_ingest(load_specs()["transactions"])
    # With lag_seconds=86400 the late row is re-read and captured.
    assert delta_store.row_count(settings.silver_path("transactions")) == base + 1


# ------------------------------ CDC soft-delete ------------------------------ #

@pytest.fixture
def cdc_source(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DELTA_ROOT", tmp_path / "delta")
    db = tmp_path / "cdc.db"
    con = sqlite3.connect(db)
    con.executescript(
        """
        CREATE TABLE events (
            id INTEGER PRIMARY KEY, val TEXT,
            is_deleted INTEGER, updated_at TEXT
        );
        INSERT INTO events VALUES (1, 'a', 0, '2024-01-01T00:00:00');
        INSERT INTO events VALUES (2, 'b', 0, '2024-01-02T00:00:00');
        """
    )
    con.commit()
    con.close()
    monkeypatch.setenv("CDC_DSN", f"sqlite:///{db}")
    return db


def _events_spec() -> IngestSpec:
    return IngestSpec(
        name="events",
        source={"type": "sqlite", "dsn_env": "CDC_DSN", "table": "events"},
        target={"table": "events_cdc", "primary_key": "id",
                "soft_delete_column": "is_deleted"},
        extract={"incremental_cursor": "updated_at"},
    )


def test_soft_delete_removes_row(cdc_source):
    spec = _events_spec()
    engine.run_ingest(spec)
    assert delta_store.row_count(settings.silver_path("events_cdc")) == 2

    # Flag id=2 as deleted at the source (with a newer cursor so it's picked up)
    con = sqlite3.connect(cdc_source)
    con.execute("UPDATE events SET is_deleted = 1, updated_at = '2024-02-01T00:00:00' WHERE id = 2")
    con.commit()
    con.close()

    engine.run_ingest(spec)
    # Row 2 is removed; only the non-deleted row remains.
    assert delta_store.row_count(settings.silver_path("events_cdc")) == 1
    t = delta_store.read_arrow(settings.silver_path("events_cdc"))
    assert t.to_pandas()["id"].tolist() == [1]
