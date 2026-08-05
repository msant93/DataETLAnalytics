"""
transforms.py — pluggable per-table operations ("the operations to do in the
tables read in order to have them ready for the columnar storage").

A company customizes the platform by writing a function and registering it with
@transform("name"), then referencing that name in a pipeline's YAML `transforms:`
list. The generic engine applies them in order. This is the extension point that
keeps the core generic while allowing table-specific business logic.
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone

import pandas as pd

Transform = Callable[[pd.DataFrame], pd.DataFrame]
_REGISTRY: dict[str, Transform] = {}


def transform(name: str) -> Callable[[Transform], Transform]:
    def deco(fn: Transform) -> Transform:
        if name in _REGISTRY:
            raise ValueError(f"Transform already registered: {name}")
        _REGISTRY[name] = fn
        return fn
    return deco


def apply(names: list[str], df: pd.DataFrame) -> pd.DataFrame:
    for name in names:
        if name not in _REGISTRY:
            raise KeyError(f"Unknown transform {name!r}. Registered: {sorted(_REGISTRY)}")
        df = _REGISTRY[name](df)
    return df


def registered() -> list[str]:
    return sorted(_REGISTRY)


# --------------------------------------------------------------------------- #
# Built-in transforms (generic, reusable across any table)
# --------------------------------------------------------------------------- #

@transform("normalize_columns")
def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """snake_case + strip whitespace from column names."""
    df = df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df


@transform("trim_strings")
def _trim_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Strip surrounding whitespace from all object/string columns."""
    df = df.copy()
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].map(lambda v: v.strip() if isinstance(v, str) else v)
    return df


@transform("add_ingested_at")
def _add_ingested_at(df: pd.DataFrame) -> pd.DataFrame:
    """Add a load-time lineage column (UTC)."""
    df = df.copy()
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    return df


# --------------------------------------------------------------------------- #
# Example CUSTOM transform — how a company adds table-specific business logic.
# Referenced from pipelines/transactions.yaml as `derive_line_totals`.
# --------------------------------------------------------------------------- #

@transform("derive_line_totals")
def _derive_line_totals(df: pd.DataFrame) -> pd.DataFrame:
    """Business rule for the transactions table: net unit price + flags."""
    df = df.copy()
    if {"amount", "quantity"} <= set(df.columns):
        df["unit_price_effective"] = (df["amount"] / df["quantity"]).round(2)
        df["is_bulk"] = df["quantity"] >= 5
    return df
