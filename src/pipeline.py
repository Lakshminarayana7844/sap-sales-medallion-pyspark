"""Bronze -> Silver -> Gold pipeline for sales orders.

Runs locally with plain PySpark (parquet). On Databricks, change FORMAT to
"delta" and the paths to Unity Catalog tables / DBFS locations.
"""
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

FORMAT = "parquet"


def get_spark() -> SparkSession:
    return (SparkSession.builder.master("local[2]").appName("sales-medallion")
            .config("spark.ui.enabled", "false").getOrCreate())


def bronze(spark: SparkSession, raw_csv: str) -> DataFrame:
    """Ingest raw data as-is, add load metadata."""
    return (spark.read.option("header", True).csv(raw_csv)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit(os.path.basename(raw_csv))))


def silver(df: DataFrame) -> DataFrame:
    """Cast types, drop duplicates, reject invalid rows, fill missing region."""
    typed = (df.withColumn("order_date", F.to_date("order_date"))
             .withColumn("quantity", F.col("quantity").cast("int"))
             .withColumn("net_price", F.col("net_price").cast("double"))
             .withColumn("region", F.when(F.col("region").isNull() | (F.trim("region") == ""), "UNKNOWN").otherwise(F.col("region"))))
    return (typed.dropDuplicates(["order_id"])
            .filter((F.col("quantity") > 0) & (F.col("net_price") > 0) & F.col("order_date").isNotNull())
            .withColumn("order_month", F.date_format("order_date", "yyyy-MM")))


def gold(df: DataFrame) -> DataFrame:
    """Monthly revenue KPIs per region."""
    return (df.groupBy("order_month", "region")
            .agg(F.count("*").alias("orders"),
                 F.sum("quantity").alias("units"),
                 F.round(F.sum("net_price"), 2).alias("revenue"))
            .orderBy("order_month", "region"))


def gold_materials(df: DataFrame) -> DataFrame:
    """Revenue and units per material with revenue share."""
    total = df.agg(F.sum("net_price")).first()[0]
    return (df.groupBy("material")
            .agg(F.count("*").alias("orders"), F.sum("quantity").alias("units"),
                 F.round(F.sum("net_price"), 2).alias("revenue"))
            .withColumn("revenue_share_pct", F.round(F.col("revenue") / F.lit(total) * 100, 1))
            .orderBy(F.desc("revenue")))


def gold_mom(df: DataFrame) -> DataFrame:
    """Total monthly revenue with month-over-month growth."""
    from pyspark.sql import Window
    m = df.groupBy("order_month").agg(F.round(F.sum("net_price"), 2).alias("revenue"))
    w = Window.orderBy("order_month")
    return (m.withColumn("prev_revenue", F.lag("revenue").over(w))
            .withColumn("mom_growth_pct",
                        F.round((F.col("revenue") - F.col("prev_revenue")) / F.col("prev_revenue") * 100, 1))
            .drop("prev_revenue").orderBy("order_month"))


def quality_report(raw: DataFrame, clean: DataFrame) -> DataFrame:
    """Row counts and the reason rows were removed or fixed in the silver step."""
    n_raw = raw.count()
    n_dupes = n_raw - raw.dropDuplicates(["order_id"]).count()
    dedup = raw.dropDuplicates(["order_id"])
    n_invalid_qty = dedup.filter(F.col("quantity").cast("int") <= 0).count()
    n_missing_region = dedup.filter(F.col("region").isNull() | (F.trim("region") == "")).count()
    rows = [("raw_rows", n_raw), ("duplicate_rows_removed", n_dupes),
            ("invalid_quantity_rows_rejected", n_invalid_qty),
            ("missing_region_rows_fixed", n_missing_region),
            ("silver_rows", clean.count())]
    return raw.sparkSession.createDataFrame(rows, ["check", "rows"])


def run(raw_csv: str, out_dir: str) -> DataFrame:
    spark = get_spark()
    b = bronze(spark, raw_csv)
    b.write.mode("overwrite").format(FORMAT).save(f"{out_dir}/bronze")
    s = silver(spark.read.format(FORMAT).load(f"{out_dir}/bronze"))
    s.write.mode("overwrite").format(FORMAT).save(f"{out_dir}/silver")
    g = gold(spark.read.format(FORMAT).load(f"{out_dir}/silver"))
    g.write.mode("overwrite").format(FORMAT).save(f"{out_dir}/gold")
    s = spark.read.format(FORMAT).load(f"{out_dir}/silver")
    gold_materials(s).write.mode("overwrite").format(FORMAT).save(f"{out_dir}/gold_materials")
    gold_mom(s).write.mode("overwrite").format(FORMAT).save(f"{out_dir}/gold_monthly_growth")
    quality_report(spark.read.format(FORMAT).load(f"{out_dir}/bronze"), s) \
        .write.mode("overwrite").format(FORMAT).save(f"{out_dir}/data_quality")
    return g


if __name__ == "__main__":
    raw = sys.argv[1] if len(sys.argv) > 1 else "data/sales_raw.csv"
    out = sys.argv[2] if len(sys.argv) > 2 else "output"
    run(raw, out).show(12, truncate=False)
