"""Unit tests for the quality gate — pure, fast, deterministic (no I/O)."""

from demos import pipeline


def _good_df():
    return pipeline.load_raw_sales()


def test_clean_data_passes():
    report = pipeline.run_quality_checks(_good_df())
    assert report.passed
    assert report.total_rows == 10


def test_missing_column_fails_and_short_circuits():
    df = _good_df().drop(columns=["region"])
    report = pipeline.run_quality_checks(df)
    assert not report.passed
    assert report.checks["all_required_columns_present"] is False


def test_duplicate_primary_key_detected():
    df = _good_df()
    df.loc[0, "order_id"] = df.loc[1, "order_id"]
    report = pipeline.run_quality_checks(df)
    assert not report.passed
    assert report.checks["order_id_is_unique"] is False


def test_negative_price_detected():
    df = _good_df()
    df.loc[0, "unit_price"] = -1.0
    report = pipeline.run_quality_checks(df)
    assert not report.passed
    assert report.checks["unit_price_positive"] is False


def test_null_in_required_field_detected():
    df = _good_df()
    df.loc[0, "customer_id"] = None
    report = pipeline.run_quality_checks(df)
    assert not report.passed
    assert report.checks["no_nulls_in_required_fields"] is False


def test_revenue_is_derived_correctly():
    df = _good_df()
    row = df.iloc[0]
    assert row["revenue"] == round(row["quantity"] * row["unit_price"], 2)
