"""Integration: run a real load and query it back. Uses `replace`, so it is
deterministic regardless of prior state."""
from demos import pipeline


def test_full_load_and_query():
    df = pipeline.load_raw_sales()
    assert pipeline.run_quality_checks(df).passed

    pipeline.load_to_duckdb(df, write_disposition="replace")

    stats = pipeline.summary_statistics().iloc[0]
    assert int(stats["order_count"]) == 10
    assert int(stats["unique_customers"]) == 7
    # 22 units across the 10 sample orders
    assert int(stats["units_sold"]) == 22


def test_analytics_partitions_cover_all_orders():
    df = pipeline.load_raw_sales()
    pipeline.load_to_duckdb(df, write_disposition="replace")
    by_cat = pipeline.revenue_by_category()
    by_region = pipeline.revenue_by_region()
    assert by_cat["orders"].sum() == 10
    assert by_region["orders"].sum() == 10
