# ---
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

# +

from config.spark_config import create_spark

helper_path = "../data/raw_csvs/"
delta_path = "../delta/bronze/"

data_paths = {
        f"{helper_path}olist_customers_dataset.csv",
        f"{helper_path}olist_geolocation_dataset.csv",
        f"{helper_path}olist_order_items_dataset.csv",
        f"{helper_path}olist_order_payments_dataset.csv",
        f"{helper_path}olist_order_reviews_dataset.csv",
        f"{helper_path}olist_orders_dataset.csv",
        f"{helper_path}olist_products_dataset.csv",
        f"{helper_path}olist_sellers_dataset.csv",
        f"{helper_path}product_category_name_translation.csv"
}



def ingest_csv(spark, path: str):
    print(f"Ingestion started for {path}")
    df = spark.read.csv(path, header=True, inferSchema=True)
    print(f"Loaded {df.count()} records of {path}.")
    return df

def create_delta_table(df, path: str):
    print("Writing as Delta table...")
    table_path = delta_path + path.removeprefix(helper_path).replace(".csv", "")
    df.write.format("delta").mode("overwrite").save(table_path)
    print(f"Delta table written to {table_path}")

def data_ingestion(spark):
    for path in data_paths:
        df = ingest_csv(spark, path)
        create_delta_table(df, path)

def main():
    spark = create_spark("Bronze Ingestion")
    data_ingestion(spark)
    spark.stop()
    print("Ingestion complete ✅")

if __name__ == "__main__":
    main()






# -


