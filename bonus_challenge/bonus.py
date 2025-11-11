
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
from typing import Callable, List, Tuple
import os


class DataPipeline:
    """
    A reusable PySpark pipeline for:
    - Loading CSV files
    - Applying transformations
    - Saving DataFrames as Delta tables
    """

    def __init__(self, spark: SparkSession, raw_path: str, delta_path: str):
        """
        Initialize the pipeline.

        Args:
            spark (SparkSession): Active Spark session.
            raw_path (str): Directory containing raw CSV files.
            delta_path (str): Base directory for Delta layers (e.g., 'delta/').
        """
        self.spark = spark
        self.raw_path = raw_path
        self.delta_path = delta_path

    def load_csv(self, file_name: str, infer_schema: bool = True, header: bool = True) -> DataFrame:
        """
        Load a CSV file into a Spark DataFrame.

        Args:
            file_name (str): CSV file name (with extension).
            infer_schema (bool): Whether to infer schema automatically.
            header (bool): Whether the CSV includes headers.

        Returns:
            DataFrame: Loaded Spark DataFrame.
        """
        path = os.path.join(self.raw_path, file_name)
        print(f"Loading CSV: {path}")

        if not os.path.exists(path):
            raise FileNotFoundError(f"File not found at: {path}")

        try:
            df = (
                self.spark.read
                .option("header", header)
                .option("inferSchema", infer_schema)
                .csv(path)
            )
            print(f"Loaded {df.count()} rows from {file_name}")
            return df
        except AnalysisException as e:
            print(f"Spark failed to read {path}: {e}")
            raise e

    def apply_transformations(self, df: DataFrame, transformations: List[Tuple[str, Callable]]) -> DataFrame:
        """
        Apply a list of transformations sequentially to a DataFrame.

        Args:
            df (DataFrame): Input DataFrame.
            transformations (List[Tuple[str, Callable]]): 
                A list of (name, function) tuples for transformations.

        Returns:
            DataFrame: Transformed DataFrame.
        """
        print(f"Starting transformations on DataFrame with {df.count()} records...")

        for name, func in transformations:
            print(f"Applying: {name}")
            df = func(df)
            print(f"Completed: {name} — Current rows: {df.count()}")

        print("All transformations completed successfully.")
        return df

    def save_to_delta(self, df: DataFrame, layer: str, table_name: str, mode: str = "overwrite"):
        """
        Save the DataFrame as a Delta table.

        Args:
            df (DataFrame): DataFrame to save.
            layer (str): Data layer name ('bronze', 'silver', 'gold', etc.).
            table_name (str): Table name or subdirectory.
            mode (str): Write mode ('overwrite', 'append', etc.).
        """
        layer_path = os.path.join(self.delta_path, layer)
        os.makedirs(layer_path, exist_ok=True)
        delta_table_path = os.path.join(layer_path, table_name)

        print(f"Writing Delta table: {delta_table_path}")

        (
            df.write
            .format("delta")
            .mode(mode)
            .save(delta_table_path)
        )

        print(f"Delta table '{table_name}' written to {layer.upper()} layer successfully.")
