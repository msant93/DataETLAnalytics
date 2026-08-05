"""
logging_setup.py
----------------
Central logging configuration. Text format for humans (local/dev), JSON for
machines (prod → shipped to Datadog/CloudWatch/ELK). Call `configure()` once at
process start; every module then uses `logging.getLogger(__name__)`.

Replacing `print()` with structured logging is table stakes for anything a
company runs unattended: you need levels, timestamps, and machine-parseable
output for alerting.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Attach any structured extras passed via logger.info(..., extra={...})
        for key, val in record.__dict__.items():
            if key not in _STD_ATTRS and not key.startswith("_"):
                payload[key] = val
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


_STD_ATTRS = set(
    logging.LogRecord("", 0, "", 0, "", None, None).__dict__.keys()
) | {"message", "asctime"}


def configure(level: str = "INFO", fmt: str = "text") -> None:
    root = logging.getLogger()
    root.setLevel(level.upper())
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(sys.stdout)
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)-7s %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
    root.addHandler(handler)

    # dlt is chatty at INFO; keep it at WARNING unless we're debugging.
    logging.getLogger("dlt").setLevel("DEBUG" if level.upper() == "DEBUG" else "WARNING")
    # HTTP client internals are never useful in our logs.
    for noisy in ("urllib3", "requests", "botocore", "google"):
        logging.getLogger(noisy).setLevel("WARNING")


def configure_from(cfg) -> None:
    configure(level=cfg.runtime["log_level"], fmt=cfg.runtime["log_format"])
