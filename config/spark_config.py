
from delta import *
from pyspark.sql import SparkSession

def create_spark(app_name: str):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "4g")          # optional tuning
        .config("spark.executor.memory", "4g")        # optional tuning
        .config("spark.sql.shuffle.partitions", "8")  # local optimization
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
