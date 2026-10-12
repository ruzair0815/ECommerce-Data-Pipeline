# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: business aggregates
# MAGIC Small, query-ready tables for dashboards.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "default"
t = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

silver = spark.table(t("silver_order_lines")).filter(F.col("order_status") != "canceled")

# COMMAND ----------

daily_sales = (
    silver.groupBy("order_date")
    .agg(
        F.countDistinct("order_id").alias("orders"),
        F.round(F.sum("price"), 2).alias("revenue"),
        F.round(F.sum("freight_value"), 2).alias("freight"),
    )
    .orderBy("order_date")
)
daily_sales.write.mode("overwrite").format("delta").saveAsTable(t("gold_daily_sales"))

# COMMAND ----------

category_sales = (
    silver.groupBy("category")
    .agg(
        F.countDistinct("order_id").alias("orders"),
        F.round(F.sum("price"), 2).alias("revenue"),
        F.round(F.avg("price"), 2).alias("avg_item_price"),
    )
    .orderBy(F.desc("revenue"))
)
category_sales.write.mode("overwrite").format("delta").saveAsTable(t("gold_category_sales"))

# COMMAND ----------

delivery_by_state = (
    silver.filter(F.col("delivery_days").isNotNull())
    .groupBy("customer_state")
    .agg(
        F.countDistinct("order_id").alias("delivered_orders"),
        F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"),
        F.round(F.avg("is_late") * 100, 1).alias("pct_late"),
    )
    .orderBy(F.desc("avg_delivery_days"))
)
delivery_by_state.write.mode("overwrite").format("delta").saveAsTable(t("gold_delivery_by_state"))

# COMMAND ----------

display(spark.table(t("gold_category_sales")).limit(10))