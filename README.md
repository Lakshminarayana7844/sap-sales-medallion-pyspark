# SAP Sales Medallion Pipeline (PySpark)

A small, runnable Bronze -> Silver -> Gold data pipeline in PySpark, modelled on how a Databricks lakehouse is usually organised. The input is a messy SAP-style sales order extract (duplicates, missing regions, invalid quantities).

## Layers

| Layer | What happens |
|-------|--------------|
| Bronze | Raw CSV ingested as-is, plus load metadata (`_ingested_at`, `_source_file`) |
| Silver | Types cast, duplicates removed by `order_id`, invalid rows rejected, missing region set to `UNKNOWN` |
| Gold | Monthly orders, units and revenue per region |

## Run it

```bash
pip install -r requirements.txt
python src/generate_data.py data/sales_raw.csv
python src/pipeline.py data/sales_raw.csv output
pytest -q
```

Needs Java 8/11/17 for Spark.

## Running on Databricks

The code uses plain DataFrame APIs. To run it in a workspace, set `FORMAT = "delta"` in `src/pipeline.py`, point the paths at a volume or Unity Catalog location, and use the cluster's existing `spark` session.

## Why this project

Shows the core of a data engineering workflow that SAP BW teams are moving towards: layered data modelling, data quality rules and aggregated reporting tables, with unit tests on the transformation logic.

## Ideas for next steps

- Delta tables with schema enforcement and `MERGE` for incremental loads
- Data quality report per run (rows rejected and why)
- Orchestration with Databricks Workflows
