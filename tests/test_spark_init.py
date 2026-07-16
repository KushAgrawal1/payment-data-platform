from pyspark.sql import SparkSession

def test_spark_session_creation():
    # Attempt to create a Spark Session
    spark = SparkSession.builder \
        .appName("SmokeTest") \
        .master("local[1]") \
        .getOrCreate()
    
    # Check if the session is active
    assert spark is not None
    assert spark.version is not None
    
    # Close the session
    spark.stop()