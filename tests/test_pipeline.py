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
        ("A3", "2026-02-01", "DE", "M-300", "-1", "7.9"),   # invalid quantity
    ]
    return spark.createDataFrame(data, cols)


def test_silver_cleans(spark):
    s = silver(rows(spark))
    assert s.count() == 2
    assert s.filter("region = 'UNKNOWN'").count() == 1


def test_gold_aggregates(spark):
    g = gold(silver(rows(spark))).collect()
    de = [r for r in g if r.region == "DE"][0]
    assert (de.orders, de.units, de.revenue) == (1, 2, 25.0)
