from config.spark_config import create_spark
from bonus_challenge.bonus import DataPipeline
from pyspark.sql import functions as F

spark = create_spark("ETL Pipeline")

pipeline = DataPipeline(
    spark,
    raw_path="data/raw_csvs",
    delta_path="delta"
)

orders_df = pipeline.load_csv("olist_orders_dataset.csv")

def add_delivery_time(df):
    return df.withColumn(
        "delivery_time_days",
        F.datediff(F.col("order_delivered_customer_date"), F.col("order_purchase_timestamp"))
    )

transformations = [
    ("Add delivery time column", add_delivery_time)
]

orders_transformed = pipeline.apply_transformations(orders_df, transformations)
pipeline.save_to_delta(orders_transformed, layer="silver", table_name="orders_enriched")

spark.stop()
