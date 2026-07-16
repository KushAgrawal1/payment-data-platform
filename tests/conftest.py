import sys
import os
import pytest
from pyspark.sql import SparkSession

# Insert the dags folder directly into Python's path so we can import modules directly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../airflow/dags')))

@pytest.fixture(scope="session")
def spark_session():
    """Provides a local, in-memory Spark Session for testing transformations."""
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("pdp-unit-tests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.default.parallelism", "1") \
        .getOrCreate()
    yield spark
    spark.stop()