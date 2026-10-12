# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: clean, type, join
# MAGIC Cast types, drop duplicates, join into one order-line table, run quality checks.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "default"
t = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

# COMMAND ----------

orders = (
    spark.table(t("bronze_orders"))
    .dropDuplicates(["order_id"])
    .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
    .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date"))
    .select(
        "order_id", "customer_id", "order_status",
        "order_purchase_timestamp", "order_delivered_customer_date",
        "order_estimated_delivery_date",
    )
)

items = (
    spark.table(t("bronze_order_items"))
    .withColumn("price", F.col("price").cast("double"))
    .withColumn("freight_value", F.col("freight_value").cast("double"))
    .select("order_id", "order_item_id", "product_id", "price", "freight_value")
)

products = (
    spark.table(t("bronze_products"))
    .dropDuplicates(["product_id"])
    .select("product_id", F.coalesce("product_category_name", F.lit("unknown")).alias("category"))
)

customers = (
    spark.table(t("bronze_customers"))
    .dropDuplicates(["customer_id"])
    .select("customer_id", "customer_city", "customer_state")
)

# COMMAND ----------

silver = (
    items.join(orders, "order_id", "inner")
    .join(products, "product_id", "left")
    .join(customers, "customer_id", "left")
    .withColumn("order_date", F.to_date("order_purchase_timestamp"))
    .withColumn(
        "delivery_days",
        F.datediff("order_delivered_customer_date", "order_purchase_timestamp"),
    )
    .withColumn(
        "is_late",
        F.when(
            F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"), 1
        ).otherwise(0),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality checks (the run fails if any check fails)

# COMMAND ----------

total = silver.count()
checks = {
    "no null order_id": silver.filter(F.col("order_id").isNull()).count() == 0,
    "no negative price": silver.filter(F.col("price") < 0).count() == 0,
    "unique order line key": silver.dropDuplicates(["order_id", "order_item_id"]).count() == total,
    "table not empty": total > 0,
}
for name, passed in checks.items():
    print(("PASS " if passed else "FAIL ") + name)
assert all(checks.values()), "Data quality checks failed"

# COMMAND ----------

silver.write.mode("overwrite").format("delta").saveAsTable(t("silver_order_lines"))
print(f"silver_order_lines: {total:,} rows")