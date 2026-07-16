from itertools import chain
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, create_map, current_timestamp, dayofmonth, lit, month, 
    round as sql_round, year, when, to_timestamp,  # <-- Added to_timestamp here
)
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, IntegerType
)

# Constants
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("country", StringType(), True),
])

DEFAULT_FX_TO_GBP = {
    "GBP": 1.0000, "USD": 0.8100, "EUR": 0.8800,
    "CAD": 0.6000, "AUD": 0.5400, "JPY": 0.0052,
}

def to_bronze(df: DataFrame) -> DataFrame:
    """Bronze: raw records + ingestion metadata + partition columns."""
    ts = to_timestamp(col("timestamp")) # Using to_timestamp
    return (
        df.withColumn("ingestion_time", current_timestamp())
          .withColumn("year", year(ts).cast(IntegerType()))
          .withColumn("month", month(ts).cast(IntegerType()))
          .withColumn("day", dayofmonth(ts).cast(IntegerType()))
    )

def to_silver(df: DataFrame, fx_rates: dict | None = None) -> DataFrame:
    """Silver: cleaned, deduplicated, and currency-normalised."""
    rates = fx_rates or DEFAULT_FX_TO_GBP
    rate_map = create_map(*chain.from_iterable(
        (lit(code), lit(rate)) for code, rate in rates.items()
    ))
    ts = to_timestamp(col("timestamp")) # Using to_timestamp
    
    return (
        df.filter(col("transaction_id").isNotNull())
          .filter(col("amount").isNotNull() & (col("amount") > 0))
          .dropDuplicates(["transaction_id"])
          .withColumn("timestamp_parsed", ts)
          .filter(col("timestamp_parsed").isNotNull()) # Records with 'junk' are dropped here
          .withColumn("fx_rate_gbp", rate_map[col("currency")])
          .withColumn("amount_gbp", 
                      when(col("fx_rate_gbp").isNotNull(), sql_round(col("amount") * col("fx_rate_gbp"), 2))
                      .otherwise(None))
          .withColumn("year", year(col("timestamp_parsed")).cast(IntegerType()))
          .withColumn("month", month(col("timestamp_parsed")).cast(IntegerType()))
          .withColumn("day", dayofmonth(col("timestamp_parsed")).cast(IntegerType()))
    )