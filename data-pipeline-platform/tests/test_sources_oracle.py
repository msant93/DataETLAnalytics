"""Tests for the Oracle source branch (via a SQLite stand-in)."""
from __future__ import annotations

import pytest

from demos.seeds import seed_oracle
from etl import delta_store, engine, settings
from etl.registry import load_specs

EXAMPLES = seed_oracle.EXAMPLES_DIR


@pytest.fixture
def platform(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DELTA_ROOT", tmp_path / "delta")
    seed_oracle.build_oracle_standin()
    src = seed_oracle.SRC
    monkeypatch.setenv("ORACLE_DSN", f"sqlite:///{src / 'oracle_fin.db'}")
    return load_specs(EXAMPLES)


def test_oracle_ingest_normalizes_and_trims(platform):
    rows = engine.run_ingest(platform["gl_entries"])
    assert rows == 4
    tbl = delta_store.read_arrow(settings.silver_path("gl_entries"))
    cols = set(tbl.schema.names)
    # Oracle UPPERCASE columns are lowercased in silver
    assert {"entry_id", "account", "department", "amount", "last_modified"} <= cols
    df = tbl.to_pandas()
    assert df.loc[df["entry_id"] == 9001, "department"].iloc[0] == "Sales"  # trimmed


def test_oracle_ingest_is_idempotent(platform):
    engine.run_ingest(platform["gl_entries"])
    engine.run_ingest(platform["gl_entries"])  # re-run, no change
    assert delta_store.row_count(settings.silver_path("gl_entries")) == 4
