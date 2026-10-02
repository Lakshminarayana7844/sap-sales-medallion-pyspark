"""Export every layer as a single CSV so results can be browsed without running Spark."""
import os
import sys

from pipeline import get_spark

LAYERS = ["bronze", "silver", "gold", "gold_materials", "gold_monthly_growth", "data_quality"]


def export(out_dir: str, sample_dir: str) -> None:
    spark = get_spark()
    os.makedirs(sample_dir, exist_ok=True)
    for name in LAYERS:
        df = spark.read.parquet(f"{out_dir}/{name}").toPandas()
        if name in ("bronze", "silver"):
            df = df.sort_values("order_id")
        if "_ingested_at" in df:
            df = df.drop(columns=["_ingested_at"])  # keep sample files deterministic
        df.to_csv(f"{sample_dir}/{name}.csv", index=False)


if __name__ == "__main__":
    export(sys.argv[1] if len(sys.argv) > 1 else "output", sys.argv[2] if len(sys.argv) > 2 else "data/samples")
