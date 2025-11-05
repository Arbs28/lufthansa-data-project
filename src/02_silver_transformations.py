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

if "__file__" in globals():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
else:
    project_root = os.path.abspath(os.path.join(os.getcwd(), ".."))

from config.spark_config import create_spark
from pyspark.sql import functions as F, DataFrame

bronze_path = os.path.join(project_root, "delta", "bronze") + "/"
silver_path = os.path.join(project_root, "delta", "silver") + "/"


def read_bronze_tables(spark):
    """Load all Bronze Delta tables into a dictionary."""
    return {
        "orders": spark.read.format("delta").load(f"{bronze_path}olist_orders_dataset"),
        "order_items": spark.read.format("delta").load(f"{bronze_path}olist_order_items_dataset"),
        "payments": spark.read.format("delta").load(f"{bronze_path}olist_order_payments_dataset"),
        "customers": spark.read.format("delta").load(f"{bronze_path}olist_customers_dataset"),
        "products": spark.read.format("delta").load(f"{bronze_path}olist_products_dataset"),
        "sellers": spark.read.format("delta").load(f"{bronze_path}olist_sellers_dataset"),
        "reviews": spark.read.format("delta").load(f"{bronze_path}olist_order_reviews_dataset"),
        "translations": spark.read.format("delta").load(f"{bronze_path}product_category_name_translation")
    }


def clean_dataframe(df: DataFrame, subset_cols: list[str]) -> DataFrame:
    """Remove duplicates and null values based on subset columns."""
    return df.dropDuplicates().dropna(subset=subset_cols)


def add_derived_columns(orders: DataFrame, order_items: DataFrame, payments: DataFrame):
    """Add calculated columns: total_price, profit_margin, delivery_time_days, payment_count."""
    order_items = (
        order_items
        .withColumn("total_price", F.col("price") + F.col("freight_value"))
        .withColumn("profit_margin", F.col("price") - F.col("freight_value"))
    )

    orders = (
        orders
        .withColumn(
            "delivery_time_days",
            F.when(
                F.col("order_delivered_customer_date").isNotNull(),
                F.datediff(F.col("order_delivered_customer_date"), F.col("order_purchase_timestamp"))
            )
        )
        .filter(F.col("delivery_time_days").isNotNull())  # remove undelivered
        .filter(F.col("delivery_time_days") > 0)          # remove invalid/zero
    )

    payments_agg = (
        payments
        .groupBy("order_id")
        .agg(F.sum("payment_installments").alias("payment_count"))
    )

    return orders, order_items, payments_agg


def create_silver_tables(orders, customers, order_items, payments_agg, reviews, products, sellers, translations):
    """Join datasets to create enriched Silver tables."""
    silver_orders = (
        orders
        .join(customers, "customer_id", "left")
        .join(payments_agg, "order_id", "left")
        .join(reviews.select("order_id", "review_score"), "order_id", "left")
    )

    silver_order_items = (
        order_items
        .join(products, "product_id", "left")
        .join(sellers, "seller_id", "left")
        .join(translations, "product_category_name", "left")
    )

    return silver_orders, silver_order_items


def write_delta(df: DataFrame, name: str):
    """Write DataFrame as a Delta table to the Silver path."""
    path = f"{silver_path}{name}"
    df.write.format("delta").mode("overwrite").save(path)
    print(f"Written {name} to {path}")


def main():
    """Pipeline entrypoint for Silver transformation."""
    spark = create_spark("Silver transformations")
    bronze = read_bronze_tables(spark)

    # Clean each dataset
    orders = clean_dataframe(bronze["orders"], ["order_id"])
    order_items = clean_dataframe(bronze["order_items"], ["order_id", "price"])
    payments = clean_dataframe(bronze["payments"], ["order_id"])
    customers = clean_dataframe(bronze["customers"], ["customer_id"])
    reviews = clean_dataframe(bronze["reviews"], ["order_id"])
    products = clean_dataframe(bronze["products"], ["product_id"])
    sellers = clean_dataframe(bronze["sellers"], ["seller_id"])
    translations = clean_dataframe(bronze["translations"], ["product_category_name"])

    # Add derived and aggregated columns
    orders, order_items, payments_agg = add_derived_columns(orders, order_items, payments)

    # Create enriched Silver datasets
    silver_orders, silver_order_items = create_silver_tables(
        orders, customers, order_items, payments_agg, reviews, products, sellers, translations
    )

    # Write to Delta
    write_delta(silver_orders, "orders_enriched")
    write_delta(silver_order_items, "order_items_enriched")


    spark.stop()
    print("\nSilver transformation completed successfully.")


if __name__ == "__main__":
    main()
