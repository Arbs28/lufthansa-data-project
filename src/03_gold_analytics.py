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

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))


if "__file__" in globals():
    # Running from a script (e.g., src/01_bronze_ingestion.py)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
else:
    # Running from a notebook inside /notebooks
    project_root = os.path.abspath(os.path.join(os.getcwd(), ".."))


from config.spark_config import create_spark
from pyspark.sql import functions as F, Window

silver_path = os.path.join(project_root, "delta", "silver") + "/"
gold_path = os.path.join(project_root, "delta", "gold") + "/"

def cumulative_sales_per_customer(orders, order_items):
    """Compute running total of total_price partitioned by customer_id."""
    window_spec = (
        Window.partitionBy("customer_id")
        .orderBy("order_purchase_timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    result = (
        order_items
        .join(orders.select("order_id", "customer_id", "order_purchase_timestamp"), "order_id")
        .withColumn("cumulative_sales", F.sum("total_price").over(window_spec))
        .select("customer_id", "order_id", "order_purchase_timestamp", "total_price", "cumulative_sales")
    )

    result.write.format("delta").mode("overwrite").save(f"{gold_path}cumulative_sales_per_customer")
    print(" Created cumulative_sales_per_customer table.")
    return result

def rolling_avg_delivery_per_category(orders, order_items):
    """Compute rolling average delivery time per product category."""
    window_spec = (
        Window.partitionBy("product_category_name")
        .orderBy("order_purchase_timestamp")
        .rowsBetween(-2, 0)  # last 3 rows including current
    )

    result = (
        order_items
        .join(orders.select("order_id", "delivery_time_days", "order_purchase_timestamp"), "order_id")
        .withColumn("rolling_avg_delivery", F.avg("delivery_time_days").over(window_spec))
        .select("product_category_name", "order_id", "delivery_time_days", "rolling_avg_delivery")
    )

    result.write.format("delta").mode("overwrite").save(f"{gold_path}rolling_avg_delivery_per_category")
    print(" Created rolling_avg_delivery_per_category table.")
    return result

def kpi_summary_tables(orders, order_items):
    """Generate KPI summary tables: total sales, avg delivery time, order counts."""
    # Total sales per product category
    sales_per_category = (
        order_items
        .groupBy("product_category_name")
        .agg(F.sum("total_price").alias("total_sales"))
    )
    sales_per_category.write.format("delta").mode("overwrite").save(f"{gold_path}kpi_sales_per_category")
    print(" Created kpi_sales_per_category table.")

    # Average delivery time per seller
    delivery_per_seller = (
        order_items
        .join(orders.select("order_id", "delivery_time_days"), "order_id")
        .groupBy("seller_id")
        .agg(F.avg("delivery_time_days").alias("avg_delivery_time"))
    )
    delivery_per_seller.write.format("delta").mode("overwrite").save(f"{gold_path}kpi_avg_delivery_per_seller")
    print(" Created kpi_avg_delivery_per_seller table.")

    # Order counts per customer state
    orders_per_state = (
        orders
        .groupBy("customer_state")
        .agg(F.countDistinct("order_id").alias("order_count"))
    )
    orders_per_state.write.format("delta").mode("overwrite").save(f"{gold_path}kpi_orders_per_state")
    print(" Created kpi_orders_per_state table.")

    return sales_per_category, delivery_per_seller, orders_per_state

def main():
    """Orchestrate Gold layer analytics creation."""
    spark = create_spark("Gold Analytics")

    # Load Silver tables
    orders = spark.read.format("delta").load(f"{silver_path}orders_enriched")
    order_items = spark.read.format("delta").load(f"{silver_path}order_items_enriched")

    # Analytical views
    cumulative_sales_per_customer(orders, order_items)
    rolling_avg_delivery_per_category(orders, order_items)
    kpi_summary_tables(orders, order_items)

    # Verify written tables
    print("\n Gold tables created successfully:")
    for tbl in [
        "cumulative_sales_per_customer",
        "rolling_avg_delivery_per_category",
        "kpi_sales_per_category",
        "kpi_avg_delivery_per_seller",
        "kpi_orders_per_state",
    ]:
        df = spark.read.format("delta").load(f"{gold_path}{tbl}")
        print(f" - {tbl}: {df.count()} rows")

    spark.stop()
    print("\n Gold analytics finished successfully.")

if __name__ == "__main__":
    main()
