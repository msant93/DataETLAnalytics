"""
seed_sources.py
---------------
Builds the three heterogeneous SOURCE systems used by the multi-source demo:

  * data/sources/crm.db          (SQLite)  -> stands in for the client's POSTGRES
                                              `customers` table
  * data/sources/sales.db        (SQLite)  -> stands in for the client's MYSQL
                                              `transactions` table
  * data/sources/products.csv    (CSV)     -> a flat file export

SQLite is used ONLY so the demo runs with no external servers. The dlt
extraction code is identical for real Postgres/MySQL — only the SQLAlchemy
connection string changes (see multi_source_pipeline.py).
"""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "data" / "sources"
SRC.mkdir(parents=True, exist_ok=True)


def build_crm() -> None:
    db = SRC / "crm.db"
    db.unlink(missing_ok=True)
    con = sqlite3.connect(db)
    con.executescript(
        """
        CREATE TABLE customers (
            customer_id   INTEGER PRIMARY KEY,
            name          TEXT NOT NULL,
            region        TEXT NOT NULL,
            signup_date   TEXT NOT NULL,
            updated_at    TEXT NOT NULL      -- incremental cursor
        );
        """
    )
    rows = [
        (1, "Acme Corp",      "North", "2023-06-01", "2024-01-01T00:00:00"),
        (2, "Globex",         "West",  "2023-08-14", "2024-01-01T00:00:00"),
        (3, "Initech",        "South", "2023-11-02", "2024-01-02T00:00:00"),
        (4, "Umbrella Ltd",   "East",  "2024-01-05", "2024-01-05T00:00:00"),
    ]
    con.executemany("INSERT INTO customers VALUES (?,?,?,?,?)", rows)
    con.commit()
    con.close()


def build_sales() -> None:
    db = SRC / "sales.db"
    db.unlink(missing_ok=True)
    con = sqlite3.connect(db)
    con.executescript(
        """
        CREATE TABLE transactions (
            txn_id       INTEGER PRIMARY KEY,
            customer_id  INTEGER NOT NULL,
            product_sku  TEXT NOT NULL,
            quantity     INTEGER NOT NULL,
            amount       REAL NOT NULL,
            txn_ts       TEXT NOT NULL,
            updated_at   TEXT NOT NULL       -- incremental cursor
        );
        """
    )
    rows = [
        (5001, 1, "SKU-MOUSE", 3, 74.97,  "2024-01-05", "2024-01-05T09:00:00"),
        (5002, 2, "SKU-KEYB",  1, 89.50,  "2024-01-06", "2024-01-06T10:30:00"),
        (5003, 1, "SKU-MON27", 2, 438.00, "2024-01-08", "2024-01-08T14:15:00"),
        (5004, 3, "SKU-HUB",   5, 199.75, "2024-01-11", "2024-01-11T08:45:00"),
        (5005, 4, "SKU-KEYB",  2, 179.00, "2024-01-15", "2024-01-15T16:20:00"),
    ]
    con.executemany("INSERT INTO transactions VALUES (?,?,?,?,?,?,?)", rows)
    con.commit()
    con.close()


def build_products() -> None:
    path = SRC / "products.csv"
    rows = [
        {"product_sku": "SKU-MOUSE", "product_name": "Wireless Mouse",      "category": "Accessories", "unit_cost": 12.00},
        {"product_sku": "SKU-KEYB",  "product_name": "Mechanical Keyboard", "category": "Accessories", "unit_cost": 45.00},
        {"product_sku": "SKU-MON27", "product_name": "27-inch Monitor",     "category": "Displays",    "unit_cost": 130.00},
        {"product_sku": "SKU-HUB",   "product_name": "USB-C Hub",           "category": "Accessories", "unit_cost": 18.00},
    ]
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["product_sku", "product_name", "category", "unit_cost"])
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    build_crm()
    build_sales()
    build_products()
    print(f"Seeded sources in {SRC}:")
    for p in sorted(SRC.iterdir()):
        print(f"  {p.name}")


if __name__ == "__main__":
    main()
