# Incremental Loading

The batch pipeline (`pipeline.py`) does a full `replace` on every run. That's
fine for a 10-row demo, but in production you can't re-load the whole table each
time. `incremental_pipeline.py` loads **only what changed** and **upserts** it.

## The two mechanisms

**1. High-water-mark cursor.** A `dlt.sources.incremental` on the `updated_at`
column. dlt persists the maximum `updated_at` it has seen in pipeline state.
Each run only considers rows at/after that watermark. We push the watermark into
the source read (`WHERE updated_at >= :last_value`) so we transfer only candidate
rows — that's the actual efficiency win, not just dedup.

**2. Merge + primary key.** `write_disposition="merge"` with
`primary_key="order_id"` makes each load an UPSERT: new orders insert, re-sent
(updated) orders replace their existing row. No duplicates.

An updated row must get a **new** `updated_at` so the cursor re-selects it — that
is how updates flow through. Because `range_start` is `closed` (`>=`), the exact
boundary row is re-read each run; merge + primary key dedups it, so this is safe.

## Proven behavior

`python -m demos.incremental_demo` mutates the source across four runs:

| Run | Source change        | Rows pushed down | Warehouse count | Watermark advances to |
|-----|----------------------|------------------|-----------------|-----------------------|
| 1   | seed 3 orders        | 3 / 3            | 3               | 2024-01-08T14:15:00   |
| 2   | none                 | 1 (boundary)     | 3 (unchanged)   | unchanged             |
| 3   | +2 new orders        | 3 (2 new + bdry) | 5               | 2024-01-15T16:20:00   |
| 4   | update order 1002    | 2 (updated+bdry) | 5 (unchanged)   | 2024-02-01T11:00:00   |

Run 2 produces **no load package** at all (idempotent). In run 4, order 1002's
quantity goes 1 → 4 and revenue 89.50 → 358.00 **in place** — the row count stays
5. Only three load packages exist for four runs.

## Going to a real source

Swap the body of the `orders()` resource for a real dlt source; the incremental +
merge wiring is unchanged:

```python
# SQL database source with the same cursor pushdown, handled by dlt:
from dlt.sources.sql_database import sql_table
orders = sql_table(credentials=..., table="orders", incremental=dlt.sources.incremental("updated_at"))

# or a REST API source:
from dlt.sources.rest_api import rest_api_source
```

## Limitations / next steps

- **Deletes** aren't captured (an upsert-only cursor can't see a row that
  vanished). Add a soft-delete flag, a `hard_delete` merge hint, or SCD2.
- **Late-arriving data** older than the watermark is skipped; use `lag` on the
  incremental cursor or periodic full reconciliation if the source backdates.
- **Clock/precision:** the cursor assumes `updated_at` is monotonic and reliable.
