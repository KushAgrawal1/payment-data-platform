import logging
import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from transformations import TRANSACTION_SCHEMA, to_bronze, to_silver

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("streaming_job")

# Configuration
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "payments_transactions")
PG_JDBC_URL = f"jdbc:postgresql://{os.environ.get('POSTGRES_HOST', 'localhost')}:{os.environ.get('POSTGRES_PORT', '5432')}/{os.environ.get('POSTGRES_DB', 'payment_dw')}"

JDBC_OPTS = {
    "url": PG_JDBC_URL,
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver",
    "batchsize": "1000",
    "rewriteBatchedStatements": "true"
}

def execute_merge(temp_table: str) -> None:
    conn = psycopg2.connect(dbname=os.environ["POSTGRES_DB"], user=os.environ["POSTGRES_USER"], 
                            password=os.environ["POSTGRES_PASSWORD"], host=os.environ["POSTGRES_HOST"])
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO dim_customer (customer_id, customer_name, country, join_date, status)
                SELECT DISTINCT customer_id, customer_id, country, CURRENT_DATE, 'ACTIVE'
                FROM {temp_table} ON CONFLICT (customer_id) DO NOTHING;

                INSERT INTO dim_merchant (merchant_id, merchant_name, category, country, status)
                SELECT DISTINCT merchant_id, merchant_id, 'General', country, 'ACTIVE'
                FROM {temp_table} ON CONFLICT (merchant_id) DO NOTHING;

                INSERT INTO fact_transactions (
                    transaction_id, customer_sk, merchant_sk, date_key, currency_code,
                    amount_gbp, original_amount, payment_method, status, fraud_flag, created_at
                )
                SELECT s.transaction_id, c.customer_sk, m.merchant_sk, CAST(to_char(s.event_time, 'YYYYMMDD') AS INT),
                       s.currency, ROUND(s.amount * cur.exchange_rate_to_gbp, 2), s.amount,
                       s.payment_method, s.status, FALSE, s.event_time
                FROM {temp_table} s
                JOIN dim_customer c ON c.customer_id = s.customer_id
                JOIN dim_merchant m ON m.merchant_id = s.merchant_id
                JOIN dim_currency cur ON cur.currency_code = s.currency
                ON CONFLICT (transaction_id) DO NOTHING;
                
                DROP TABLE {temp_table};
            """)
        conn.commit()
    finally:
        conn.close()

def process_medallion_microbatch(batch_df, batch_id: int) -> None:
    if batch_df.isEmpty(): return
    batch_df.cache()

    # 1. Bronze/Silver S3 Writes
    to_bronze(batch_df).write.format("parquet").mode("append").partitionBy("year", "month", "day", "country").save(f"{os.environ.get('S3_BUCKET')}/bronze/transactions")
    silver_df = to_silver(batch_df)
    silver_df.write.format("parquet").mode("append").partitionBy("year", "month").save(f"{os.environ.get('S3_BUCKET')}/silver/transactions")

    # 2. Warehouse Write using dynamic temp table
    temp_table = f"stg_trans_{batch_id}_{os.urandom(4).hex()}"
    silver_df.select("transaction_id", "customer_id", "merchant_id", "amount", "currency", 
                     "country", "payment_method", "status", "timestamp_parsed").withColumnRenamed("timestamp_parsed", "event_time") \
        .write.format("jdbc").options(**JDBC_OPTS, dbtable=temp_table).mode("overwrite").save()

    execute_merge(temp_table)
    batch_df.unpersist()

def main() -> None:
    spark = SparkSession.builder.appName("PaymentMedallionStreamingProcessor").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    parsed = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC).load().selectExpr("CAST(value AS STRING) AS json_payload") \
        .select(from_json(col("json_payload"), TRANSACTION_SCHEMA).alias("data")).select("data.*")

    parsed.writeStream.foreachBatch(process_medallion_microbatch) \
        .option("checkpointLocation", os.environ.get("CHECKPOINT_DIR", "/tmp/checkpoints")).start().awaitTermination()

if __name__ == "__main__":
    main()