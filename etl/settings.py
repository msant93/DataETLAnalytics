"""
settings.py — framework-wide paths and connection resolution.

Delta lake layout (medallion):
    {delta_root}/silver/<table>   raw-but-typed tables, one per source table
    {delta_root}/gold/<table>     conformed BI models the reports read

Connections are resolved from environment variables named in each pipeline spec,
so credentials never live in the repo.
"""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PIPELINES_DIR = PROJECT_ROOT / "pipelines"
DELTA_ROOT = Path(os.environ.get("DELTA_ROOT", PROJECT_ROOT / "output" / "delta"))


def silver_path(table: str) -> str:
    return str(DELTA_ROOT / "silver" / table)


def gold_path(table: str) -> str:
    return str(DELTA_ROOT / "gold" / table)


def resolve_dsn(env_name: str) -> str:
    dsn = os.environ.get(env_name)
    if not dsn:
        raise RuntimeError(f"Connection env var {env_name!r} is not set")
    return dsn


def resolve_path(env_name: str) -> str:
    p = os.environ.get(env_name)
    if not p:
        raise RuntimeError(f"Path env var {env_name!r} is not set")
    return p
