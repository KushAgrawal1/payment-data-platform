import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """
    Creates a Spark Session for the duration of the test session.
    Configured to run locally with minimal resources.
    """
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-pyspark-local-testing")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    
    # Lower the log level to avoid noise in the terminal during testing
    spark.sparkContext.setLogLevel("WARN")
    
    yield spark
    
    # Teardown: Stop the session after all tests are finished
    spark.stop()