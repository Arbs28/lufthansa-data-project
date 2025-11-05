
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from config.spark_config import create_spark

if "__file__" in globals():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
else:
    project_root = os.path.abspath(os.path.join(os.getcwd(), ".."))

gold_path = os.path.join(project_root, "delta", "gold") + "/"

spark = create_spark("Gold SQL Reporting")

sales_df = spark.read.format("delta").load(f"{gold_path}kpi_sales_per_category")
delivery_df = spark.read.format("delta").load(f"{gold_path}kpi_avg_delivery_per_seller")
orders_df = spark.read.format("delta").load(f"{gold_path}kpi_orders_per_state")

sales_df.createOrReplaceTempView("kpi_sales_per_category")
delivery_df.createOrReplaceTempView("kpi_avg_delivery_per_seller")
orders_df.createOrReplaceTempView("kpi_orders_per_state")

print("Total Sales per Product Category:")
spark.sql("""
    SELECT product_category_name, total_sales
    FROM kpi_sales_per_category
    ORDER BY total_sales DESC
    LIMIT 10
""").show(truncate=False)

print("Sample of Average Delivery Time per Seller:")
spark.sql("""
    SELECT seller_id, avg_delivery_time
    FROM kpi_avg_delivery_per_seller
    ORDER BY avg_delivery_time DESC
    LIMIT 10
""").show(truncate=False)

print("Number of Orders by State:")
spark.sql("""
    SELECT customer_state, order_count
    FROM kpi_orders_per_state
    ORDER BY order_count DESC
""").show(truncate=False)

spark.stop()
print("Gold SQL reporting completed successfully.")


