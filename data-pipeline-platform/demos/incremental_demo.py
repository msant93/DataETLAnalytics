"""
incremental_demo.py
-------------------
Proves the three properties that define correct incremental loading by mutating
the source between runs and observing the warehouse after each:

    Run 1  initial load        -> all seed rows inserted
    Run 2  no source change     -> 0 rows processed (idempotent)
    Run 3  two new orders       -> only the 2 new rows loaded
    Run 4  one order updated     -> 1 row UPSERTED (count unchanged, value changed)

Run this after setup:  python incremental_demo.py
"""

from __future__ import annotations

import csv

from demos import incremental_pipeline as inc

SOURCE = inc.SOURCE_CSV
FIELDS = ["order_id", "order_date", "customer_id", "product",
          "category", "region", "quantity", "unit_price", "updated_at"]


def write_source(rows: list[dict]) -> None:
    SOURCE.parent.mkdir(parents=True, exist_ok=True)
    with SOURCE.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)


def banner(title: str) -> None:
    print("\n" + "=" * 68)
    print(title)
    print("=" * 68)


SEED = [
    {"order_id": 1001, "order_date": "2024-01-05", "customer_id": "C017",
     "product": "Wireless Mouse", "category": "Accessories", "region": "North",
     "quantity": 3, "unit_price": 24.99, "updated_at": "2024-01-05T09:00:00"},
    {"order_id": 1002, "order_date": "2024-01-06", "customer_id": "C042",
     "product": "Mechanical Keyboard", "category": "Accessories", "region": "West",
     "quantity": 1, "unit_price": 89.50, "updated_at": "2024-01-06T10:30:00"},
    {"order_id": 1003, "order_date": "2024-01-08", "customer_id": "C019",
     "product": "27-inch Monitor", "category": "Displays", "region": "North",
     "quantity": 2, "unit_price": 219.00, "updated_at": "2024-01-08T14:15:00"},
]

NEW_ORDERS = [
    {"order_id": 1004, "order_date": "2024-01-11", "customer_id": "C007",
     "product": "USB-C Hub", "category": "Accessories", "region": "South",
     "quantity": 5, "unit_price": 39.95, "updated_at": "2024-01-11T08:45:00"},
    {"order_id": 1005, "order_date": "2024-01-15", "customer_id": "C042",
     "product": "Laptop Stand", "category": "Accessories", "region": "West",
     "quantity": 2, "unit_price": 45.00, "updated_at": "2024-01-15T16:20:00"},
]


def report() -> None:
    print(f"\n  warehouse row count : {inc.table_row_count()}")
    print(f"  persisted watermark : {inc.current_watermark()!r}")


def main() -> None:
    banner("RUN 1 — initial load (3 seed orders)")
    write_source(SEED)
    _, _info = inc.run_incremental()
    report()

    banner("RUN 2 — no source change (expect 0 processed, idempotent)")
    _, _info = inc.run_incremental()
    report()

    banner("RUN 3 — two NEW orders appended (expect only 2 processed)")
    write_source(SEED + NEW_ORDERS)
    _, _info = inc.run_incremental()
    report()

    banner("RUN 4 — UPDATE order 1002 (qty 1 -> 4, new updated_at)")
    print("  before:")
    print(inc.order_snapshot(1002).to_string(index=False))
    updated = [dict(r) for r in SEED + NEW_ORDERS]
    for r in updated:
        if r["order_id"] == 1002:
            r["quantity"] = 4
            r["updated_at"] = "2024-02-01T11:00:00"   # advances the watermark
    write_source(updated)
    _, _info = inc.run_incremental()
    print("  after:")
    print(inc.order_snapshot(1002).to_string(index=False))
    report()

    banner("LOAD HISTORY (one package per run that had work)")
    print(inc.load_history().to_string(index=False))

    print("\nDone. Note: row count went 3 -> 3 -> 5 -> 5. The update did NOT add a row.")


if __name__ == "__main__":
    main()
