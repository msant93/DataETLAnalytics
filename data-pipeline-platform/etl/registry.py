"""
registry.py — load the declarative pipeline specs from pipelines/*.yaml.

Two spec kinds:
  * ingest : a source table/file -> a silver Delta table (incremental + merge)
  * model  : SQL over silver tables -> a gold Delta table (for BI)

Adding a new table to the platform means dropping a YAML file here. No code
changes — that is what makes the framework generic.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from etl.settings import PIPELINES_DIR


@dataclass(frozen=True)
class IngestSpec:
    name: str
    source: dict[str, Any]
    target: dict[str, Any]
    extract: dict[str, Any] = field(default_factory=dict)
    transforms: list[str] = field(default_factory=list)
    kind: str = "ingest"

    @property
    def primary_key(self) -> str:
        return self.target["primary_key"]

    @property
    def write_mode(self) -> str:
        return self.target.get("write_mode", "merge")

    @property
    def cursor(self) -> str | None:
        return self.extract.get("incremental_cursor")


@dataclass(frozen=True)
class ModelSpec:
    name: str
    inputs: list[str]
    sql: str
    target: dict[str, Any]
    kind: str = "model"


def _load_yaml(path: Path) -> dict:
    with path.open() as f:
        return yaml.safe_load(f)


def load_specs(directory: Path | None = None) -> dict[str, IngestSpec | ModelSpec]:
    directory = directory or PIPELINES_DIR
    specs: dict[str, IngestSpec | ModelSpec] = {}
    for path in sorted(directory.glob("*.yaml")):
        doc = _load_yaml(path)
        kind = doc.get("kind")
        if kind == "ingest":
            spec: IngestSpec | ModelSpec = IngestSpec(
                name=doc["name"],
                source=doc["source"],
                target=doc["target"],
                extract=doc.get("extract", {}),
                transforms=doc.get("transforms", []),
            )
        elif kind == "model":
            spec = ModelSpec(
                name=doc["name"],
                inputs=doc["inputs"],
                sql=doc["sql"],
                target=doc["target"],
            )
        else:
            raise ValueError(f"{path.name}: unknown kind {kind!r} (use ingest|model)")
        if spec.name in specs:
            raise ValueError(f"Duplicate pipeline name: {spec.name}")
        specs[spec.name] = spec
    return specs
