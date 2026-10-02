import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from pipeline import get_spark, silver, gold


@pytest.fixture(scope="module")
def spark():
    return get_spark()


def rows(spark):
    cols = ["order_id", "order_date", "region", "material", "quantity", "net_price"]
    data = [
        ("A1", "2026-01-05", "DE", "M-100", "2", "25.0"),
        ("A1", "2026-01-05", "DE", "M-100", "2", "25.0"),   # duplicate
        ("A2", "2026-01-20", "", "M-200", "1", "48.0"),     # missing region
        ("A4", "2026-02-02", None, "M-100", "1", "12.5"),   # null region (as read from CSV)
        ("A3", "2026-02-01", "DE", "M-300", "-1", "7.9"),   # invalid quantity
    ]
    return spark.createDataFrame(data, cols)


def test_silver_cleans(spark):
    s = silver(rows(spark))
    assert s.count() == 3
    assert s.filter("region = 'UNKNOWN'").count() == 2


def test_gold_aggregates(spark):
    g = gold(silver(rows(spark))).collect()
    de = [r for r in g if r.region == "DE"][0]
    assert (de.orders, de.units, de.revenue) == (1, 2, 25.0)


def test_quality_report_counts(spark):
    from pipeline import quality_report
    raw = rows(spark)
    q = {r.check: r.rows for r in quality_report(raw, silver(raw)).collect()}
    assert q["raw_rows"] == 5
    assert q["duplicate_rows_removed"] == 1
    assert q["invalid_quantity_rows_rejected"] == 1
    assert q["missing_region_rows_fixed"] == 2
    assert q["silver_rows"] == 3


def test_materials_share_sums_to_100(spark):
    from pipeline import gold_materials
    share = sum(r.revenue_share_pct for r in gold_materials(silver(rows(spark))).collect())
    assert abs(share - 100) < 0.2
