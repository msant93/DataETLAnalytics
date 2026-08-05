"""Config loading, profile merge, env override, and validation."""
import pytest

from etl import config


@pytest.fixture(autouse=True)
def _reset():
    config.reset_config()
    yield
    config.reset_config()


def test_dev_profile_defaults(monkeypatch):
    monkeypatch.delenv("APP_ENV", raising=False)
    cfg = config.load_config(force=True)
    assert cfg.profile == "dev"
    assert cfg.destination["type"] == "duckdb"
    assert cfg.runtime["log_level"] == "DEBUG"


def test_prod_profile_overrides_merge(monkeypatch):
    monkeypatch.setenv("APP_ENV", "prod")
    cfg = config.load_config(force=True)
    assert cfg.load_strategy == "incremental"
    assert cfg.runtime["log_format"] == "json"


def test_env_var_override(monkeypatch):
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("PIPELINE__PIPELINE__TABLE", "custom_orders")
    cfg = config.load_config(force=True)
    assert cfg.table == "custom_orders"


def test_postgres_without_dsn_fails(monkeypatch):
    monkeypatch.setenv("PIPELINE__DESTINATION__TYPE", "postgres")
    monkeypatch.delenv("PIPELINE__POSTGRES_DSN", raising=False)
    with pytest.raises(ValueError):
        config.load_config(force=True)
