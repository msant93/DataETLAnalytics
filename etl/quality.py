"""
quality.py — config-driven data-quality gate for the framework.

Unlike the batch demo (whose gate lived in demos/pipeline.py), this runs inside
the generic engine, so EVERY ingest is validated before it lands. Rules are
declared per pipeline in YAML:

    quality:
      not_null: [customer_id, updated_at]
      unique:   [customer_id]        # primary key is always enforced too
      positive: [amount]
      on_fail:  abort                # abort | warn | quarantine

`check()` returns a row-level mask of bad rows so the engine can abort, warn, or
quarantine (route bad rows to a `<table>__rejects` table and load the good ones).
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


class QualityError(Exception):
    """Raised when a quality gate with on_fail=abort finds bad rows."""


@dataclass
class QualityResult:
    total: int
    failures: dict[str, int] = field(default_factory=dict)
    bad_mask: pd.Series | None = None

    @property
    def passed(self) -> bool:
        return not self.failures

    @property
    def bad_count(self) -> int:
        return int(self.bad_mask.sum()) if self.bad_mask is not None else 0

    def summary(self) -> str:
        if self.passed:
            return f"quality OK ({self.total} rows)"
        parts = ", ".join(f"{rule}={n}" for rule, n in self.failures.items())
        return f"quality FAILED ({self.bad_count}/{self.total} bad rows): {parts}"


def check(df: pd.DataFrame, rules: dict, primary_key: str) -> QualityResult:
    """Validate df against the declared rules. Primary-key uniqueness is always
    enforced as a baseline, even if not listed."""
    result = QualityResult(total=len(df))
    bad = pd.Series(False, index=df.index)

    # not-null checks
    for col in rules.get("not_null", []):
        col_bad = df[col].isna() if col in df.columns else pd.Series(True, index=df.index)
        n = int(col_bad.sum())
        if n:
            result.failures[f"not_null:{col}"] = n
        bad |= col_bad

    # uniqueness — declared columns plus the primary key (baseline)
    unique_cols = set(rules.get("unique", [])) | {primary_key}
    for col in unique_cols:
        if col in df.columns:
            col_bad = df[col].duplicated(keep=False)
            n = int(col_bad.sum())
            if n:
                result.failures[f"unique:{col}"] = n
            bad |= col_bad

    # positivity checks
    for col in rules.get("positive", []):
        if col in df.columns:
            col_bad = ~(df[col] > 0)
            n = int(col_bad.sum())
            if n:
                result.failures[f"positive:{col}"] = n
            bad |= col_bad

    result.bad_mask = bad
    return result
