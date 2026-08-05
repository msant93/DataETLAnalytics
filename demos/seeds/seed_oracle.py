"""
seed_oracle.py
--------------
Build the Oracle example source system and run its pipeline into Delta.

  * data/sources/oracle_fin.db  (SQLite)  -> stands in for the client's ORACLE
                                             GL_ENTRIES table (UPPERCASE columns,
                                             like a real Oracle catalog)

SQLite stands in for Oracle only so the demo runs without an Oracle server; the
extraction code is identical (SQLAlchemy). Real deployments set ORACLE_DSN to an
`oracle+oracledb://...` URL and install the `oracledb` driver.

    python seed_oracle.py     # seed + run the Oracle example pipeline
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC = PROJECT_ROOT / "data" / "sources"
EXAMPLES_DIR = PROJECT_ROOT / "pipelines" / "examples"


def build_oracle_standin() -> Path:
    SRC.mkdir(parents=True, exist_ok=True)
    db = SRC / "oracle_fin.db"
    db.unlink(missing_ok=True)
    con = sqlite3.connect(db)
    # UPPERCASE identifiers mimic how Oracle stores them in its catalog.
    con.executescript(
        """
        CREATE TABLE GL_ENTRIES (
            ENTRY_ID      INTEGER PRIMARY KEY,
            ACCOUNT       TEXT NOT NULL,
            DEPARTMENT    TEXT NOT NULL,
            AMOUNT        REAL NOT NULL,
            LAST_MODIFIED TEXT NOT NULL
        );
        """
    )
    con.executemany(
        "INSERT INTO GL_ENTRIES VALUES (?,?,?,?,?)",
        [
            (9001, "4000-Revenue", "Sales   ", 12500.00, "2024-01-05T00:00:00"),
            (9002, "5000-COGS", "Operations", -4200.50, "2024-01-06T00:00:00"),
            (9003, "6000-Marketing", "Marketing ", -1800.00, "2024-01-09T00:00:00"),
            (9004, "4000-Revenue", "Sales", 9800.00, "2024-01-12T00:00:00"),
        ],
    )
    con.commit()
    con.close()
    return db


def seed() -> None:
    build_oracle_standin()
    os.environ.setdefault("ORACLE_DSN", f"sqlite:///{SRC / 'oracle_fin.db'}")


def main() -> None:
    from etl import engine
    from etl.logging_setup import configure
    from etl.registry import load_specs

    configure(level="INFO")
    seed()
    specs = load_specs(EXAMPLES_DIR)
    print(f"Seeded sources in {SRC}")
    for name, spec in specs.items():
        rows = engine.run_ingest(spec)
        print(f"  ran {name:<12} -> {rows} rows")


if __name__ == "__main__":
    main()
