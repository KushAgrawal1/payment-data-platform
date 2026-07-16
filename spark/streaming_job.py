import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, expr, year, month, dayofmonth

# --- Configuration Constants ---
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
POSTGRES_URL = "jdbc:postgresql://127.0.0.1:5432/payment_dw"
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_secure_password")

# --- MinIO (S3A) Configuration Constants ---
S3_ENDPOINT = "http://127.0.0.1:9000"
AWS_ACCESS_KEY = "minioadmin"
AWS_SECRET_KEY = "minioadmin"
S3_BUCKET = "s3a://payment-data-lake"

# 1. Initialize Spark Session with S3A/Hadoop Configs
spark = SparkSession.builder \
    .appName("PaymentMedallionStreamingProcessor") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Read Stream from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "payments_transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Bronze Layer Processing: Immutable Source-of-Truth
# Parse json payload, extract values, but keep raw structures
raw_parsed_df = kafka_stream.selectExpr("CAST(value AS STRING) as json_payload") \
    .select(expr("from_json(json_payload, 'transaction_id STRING, customer_id STRING, merchant_id STRING, amount DOUBLE, currency STRING, status STRING, payment_method STRING, timestamp STRING, country STRING')").alias("data")) \
    .select("data.*")

# 4. Define Multi-Write Target (Postgres + Bronze/Silver Data Lake)
def process_medallion_microbatch(batch_df, batch_id):
    print("\n" + "="*50)
    print(f"🌲 PROCESS BATCH {batch_id} - MEDALLION WRITE STARTED 🌲")
    print(f"Record count to ingest: {batch_df.count()}")
    print("="*50 + "\n")

    if batch_df.isEmpty():
        print("⚠️ Batch is empty. Skipping processing step.")
        return
    
    # Cache batch since we are writing to multiple locations
    batch_df.cache()

    try:
        # --- A. BRONZE LAYER WRITE (S3) ---
        # Add ingestion timestamps and partitioning columns
        bronze_df = batch_df \
            .withColumn("ingestion_time", current_timestamp()) \
            .withColumn("year", year(col("timestamp").cast("timestamp"))) \
            .withColumn("month", month(col("timestamp").cast("timestamp"))) \
            .withColumn("day", dayofmonth(col("timestamp").cast("timestamp")))
        
        print("💾 Saving Bronze Parquet to MinIO...")
        # Write to Bronze partitioning path: year/month/day/country
        bronze_df.write \
            .format("parquet") \
            .mode("append") \
            .partitionBy("year", "month", "day", "country") \
            .save(f"{S3_BUCKET}/bronze/transactions")

        # --- B. SILVER LAYER WRITE (S3) ---
        # Data Cleansing: Deduplicate, standardize amount currency, filter out corrupted records
        print("🧼 Cleaning & Saving Silver Parquet to MinIO...")
        silver_df = batch_df \
            .filter(col("transaction_id").isNotNull()) \
            .dropDuplicates(["transaction_id"]) \
            .withColumn("amount_gbp", col("amount")) \
            .withColumn("timestamp_parsed", col("timestamp").cast("timestamp")) \
            .withColumn("year", year(col("timestamp").cast("timestamp"))) \
            .withColumn("month", month(col("timestamp").cast("timestamp"))) \
            .withColumn("day", dayofmonth(col("timestamp").cast("timestamp")))
        
        silver_df.write \
            .format("parquet") \
            .mode("append") \
            .partitionBy("year", "month") \
            .save(f"{S3_BUCKET}/silver/transactions")

        # --- C. POSTGRES DATA WAREHOUSE SYNC ---
        print("⏳ Writing to Postgres DW...")
        
        # Dim Customer Write
        unique_customers = batch_df.select(col("customer_id"), col("country")).distinct() \
            .withColumn("customer_name", col("customer_id")) \
            .withColumn("join_date", current_timestamp().cast("date")) \
            .withColumn("status", lit("ACTIVE")) \
            .withColumn("created_at", current_timestamp())

        unique_customers.write.format("jdbc") \
            .option("url", POSTGRES_URL).option("dbtable", "dim_customer") \
            .option("user", POSTGRES_USER).option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver").mode("append").save()

        # Dim Merchant Write
        unique_merchants = batch_df.select(col("merchant_id"), col("country")).distinct() \
            .withColumn("merchant_name", col("merchant_id")) \
            .withColumn("category", lit("General")) \
            .withColumn("status", lit("ACTIVE")) \
            .withColumn("created_at", current_timestamp())

        unique_merchants.write.format("jdbc") \
            .option("url", POSTGRES_URL).option("dbtable", "dim_merchant") \
            .option("user", POSTGRES_USER).option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver").mode("append").save()

        # Fact Transactions Write
        facts_payload = silver_df.select(
            col("transaction_id"),
            col("currency").alias("currency_code"),
            col("amount_gbp"),
            col("amount").alias("original_amount"),
            col("payment_method"),
            col("status"),
            lit(False).alias("fraud_flag"),
            col("timestamp_parsed").alias("created_at")
        ).withColumn("date_key", expr("cast(date_format(created_at, 'yyyyMMdd') as int)")) \
         .withColumn("ingested_at", current_timestamp())

        facts_payload.write.format("jdbc") \
            .option("url", POSTGRES_URL).option("dbtable", "fact_transactions") \
            .option("user", POSTGRES_USER).option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver").mode("append").save()

        print("✨ SUCCESS: Micro-batch sync completed for MinIO & Postgres!")

    except Exception as e:
        print("\n🚨🚨🚨 MEDALLION WRITE ERROR DETECTED 🚨🚨🚨")
        print(str(e))
        print("🚨🚨🚨" + "="*15 + "🚨🚨🚨\n")
    finally:
        batch_df.unpersist()

# 5. Kick off Streaming Query
medallion_query = raw_parsed_df.writeStream \
    .foreachBatch(process_medallion_microbatch) \
    .option("checkpointLocation", "checkpoints/medallion_stream") \
    .start()

medallion_query.awaitTermination()