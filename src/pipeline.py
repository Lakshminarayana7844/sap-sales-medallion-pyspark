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
             .withColumn("region", F.when(F.trim("region") == "", "UNKNOWN").otherwise(F.col("region"))))
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


def run(raw_csv: str, out_dir: str) -> DataFrame:
    spark = get_spark()
    b = bronze(spark, raw_csv)
    b.write.mode("overwrite").format(FORMAT).save(f"{out_dir}/bronze")
    s = silver(spark.read.format(FORMAT).load(f"{out_dir}/bronze"))
    s.write.mode("overwrite").format(FORMAT).save(f"{out_dir}/silver")
    g = gold(spark.read.format(FORMAT).load(f"{out_dir}/silver"))
    g.write.mode("overwrite").format(FORMAT).save(f"{out_dir}/gold")
    return g


if __name__ == "__main__":
    raw = sys.argv[1] if len(sys.argv) > 1 else "data/sales_raw.csv"
    out = sys.argv[2] if len(sys.argv) > 2 else "output"
    run(raw, out).show(12, truncate=False)
