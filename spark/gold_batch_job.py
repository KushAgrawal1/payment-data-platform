from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg

# Initialize Batch Spark Session
spark = SparkSession.builder \
    .appName("GoldAggregationsBatch") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

print("🏁 Reading Silver Cleaned Layer...")
# Read the cleaned parquet stream data directly from S3
silver_df = spark.read.parquet("s3a://payment-data-lake/silver/transactions")

print("📊 Generating Gold KPI Aggregations (Daily Metrics)...")
gold_daily_metrics = silver_df.groupBy("year", "month", "day", "country", "status") \
    .agg(
        count("*").alias("transaction_count"),
        sum("amount_gbp").alias("total_amount_gbp"),
        avg("amount_gbp").alias("avg_amount_gbp")
    )

gold_daily_metrics.show(truncate=False)

print("💾 Writing Gold Aggregations to MinIO...")
# Write Gold dataset back to S3
gold_daily_metrics.write \
    .mode("overwrite") \
    .parquet("s3a://payment-data-lake/gold/daily_metrics")

print("🎉 Gold metrics updated successfully!")