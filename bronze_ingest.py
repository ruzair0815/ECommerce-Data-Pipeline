# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: ingest raw CSVs
# MAGIC Load raw files as-is into Delta tables and add audit columns.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "default"
RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

FILES = {
    "bronze_orders": "olist_orders_dataset.csv",
    "bronze_order_items": "olist_order_items_dataset.csv",
    "bronze_products": "olist_products_dataset.csv",
    "bronze_customers": "olist_customers_dataset.csv",
}

# COMMAND ----------

for table, filename in FILES.items():
    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)  # keep everything as string in bronze
        .csv(f"{RAW_PATH}/{filename}")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(filename))
    )
    df.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.{table}")
    print(f"{table}: {df.count():,} rows")