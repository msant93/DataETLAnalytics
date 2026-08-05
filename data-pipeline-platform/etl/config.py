"""
config.py
---------
Loads config.yaml, merges the active environment profile over the defaults, and
applies environment-variable overrides. Returns a typed, validated config object.

Design principles a buyer's team will look for:
  - Config is DATA, not code. Retargeting to a new warehouse = editing YAML/env.
  - Secrets never live in the repo. They're read from env at load time.
  - Config is loaded lazily and cached, so tests can reset it (`reset_config()`).
  - The active profile is selectable with APP_ENV (dev/staging/prod).
"""

from __future__ import annotations

import copy
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = Path(os.environ.get("APP_CONFIG", PROJECT_ROOT / "config.yaml"))


# --------------------------------------------------------------------------- #
# Typed views over the config tree
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Config:
    profile: str
    raw: dict[str, Any]

    # -- convenient typed accessors ---------------------------------------- #
    @property
    def pipeline_name(self) -> str:
        return self.raw["pipeline"]["name"]

    @property
    def dataset(self) -> str:
        return self.raw["pipeline"]["dataset"]

    @property
    def table(self) -> str:
        return self.raw["pipeline"]["table"]

    @property
    def load_strategy(self) -> str:
        return self.raw["pipeline"]["load_strategy"]

    @property
    def fq_table(self) -> str:
        return f"{self.dataset}.{self.table}"

    @property
    def source(self) -> dict[str, Any]:
        return self.raw["source"]

    @property
    def destination(self) -> dict[str, Any]:
        return self.raw["destination"]

    @property
    def quality(self) -> dict[str, Any]:
        return self.raw["quality"]

    @property
    def runtime(self) -> dict[str, Any]:
        return self.raw["runtime"]

    def secret(self, key: str) -> str | None:
        """Resolve a secret by its logical name via the configured env var."""
        env_name = self.raw.get("secrets_env", {}).get(key)
        return os.environ.get(env_name) if env_name else None

    def resolved_path(self, relative: str) -> Path:
        p = Path(relative)
        return p if p.is_absolute() else PROJECT_ROOT / p


# --------------------------------------------------------------------------- #
# Loading / merging
# --------------------------------------------------------------------------- #

def _deep_merge(base: dict, override: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def _apply_env_overrides(tree: dict) -> dict:
    """
    Environment variables of the form PIPELINE__SECTION__KEY override the tree.
    e.g. PIPELINE__DESTINATION__TYPE=postgres  ->  destination.type = postgres
    This is the standard 12-factor override pattern and is what CI/containers use.
    """
    out = copy.deepcopy(tree)
    prefix = "PIPELINE__"
    for env_key, env_val in os.environ.items():
        if not env_key.startswith(prefix):
            continue
        path = env_key[len(prefix):].lower().split("__")
        if len(path) < 2:
            continue
        node = out
        for part in path[:-1]:
            node = node.setdefault(part, {})
            if not isinstance(node, dict):
                break
        else:
            node[path[-1]] = _coerce(env_val)
    return out


def _coerce(val: str) -> Any:
    low = val.lower()
    if low in {"true", "false"}:
        return low == "true"
    if val.isdigit():
        return int(val)
    return val


_CACHE: Config | None = None


def load_config(force: bool = False) -> Config:
    global _CACHE
    if _CACHE is not None and not force:
        return _CACHE

    with CONFIG_PATH.open() as f:
        doc = yaml.safe_load(f)

    profile = os.environ.get("APP_ENV", doc.get("default_profile", "dev"))
    merged = _deep_merge(doc.get("defaults", {}), doc.get("profiles", {}).get(profile, {}))
    merged["secrets_env"] = doc.get("defaults", {}).get("secrets_env", {})
    merged = _apply_env_overrides(merged)

    cfg = Config(profile=profile, raw=merged)
    _validate(cfg)
    _CACHE = cfg
    return cfg


def reset_config() -> None:
    """Clear the cache — used by tests that set env vars between cases."""
    global _CACHE
    _CACHE = None


def _validate(cfg: Config) -> None:
    valid_dest = {"duckdb", "postgres", "bigquery", "filesystem"}
    if cfg.destination["type"] not in valid_dest:
        raise ValueError(f"destination.type must be one of {valid_dest}")
    if cfg.load_strategy not in {"full", "incremental"}:
        raise ValueError("pipeline.load_strategy must be 'full' or 'incremental'")
    if cfg.destination["type"] == "postgres" and not cfg.secret("postgres_dsn"):
        raise ValueError(
            "destination.type=postgres but PIPELINE__POSTGRES_DSN is not set"
        )


if __name__ == "__main__":
    c = load_config()
    print(f"profile={c.profile}")
    print(f"pipeline={c.pipeline_name} strategy={c.load_strategy}")
    print(f"destination={c.destination['type']} fq_table={c.fq_table}")
    print(f"log_level={c.runtime['log_level']} format={c.runtime['log_format']}")
