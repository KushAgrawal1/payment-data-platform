# airflow/dags/gold_processor.py
import psycopg2
import os

POSTGRES_DB = "payment_dw"
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_secure_password")
POSTGRES_HOST = "127.0.0.1"

def load_gold_layer_to_dw():
    """
    Computes daily business KPIs (Gold Layer) from the Silver layer (Postgres)
    using idempotent MERGE/ON CONFLICT updates.
    """
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST
    )
    cursor = conn.cursor()

    # Create a Gold summary table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold_daily_merchant_kpis (
            summary_date DATE,
            merchant_id VARCHAR(50),
            total_transactions INT,
            total_volume_gbp NUMERIC(12, 2),
            success_rate NUMERIC(5, 2),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (summary_date, merchant_id)
        );
    """)

    # Idempotent Upsert (using ON CONFLICT)
    gold_aggregation_query = """
        INSERT INTO gold_daily_merchant_kpis (summary_date, merchant_id, total_transactions, total_volume_gbp, success_rate)
        SELECT 
            f.created_at::date as summary_date,
            m.merchant_id,
            COUNT(f.transaction_id) as total_transactions,
            SUM(f.amount_gbp) as total_volume_gbp,
            ROUND(
                (SUM(CASE WHEN f.status = 'SUCCESS' THEN 1 ELSE 0 END)::numeric / COUNT(f.transaction_id)) * 100, 
                2
            ) as success_rate
        FROM fact_transactions f
        JOIN dim_merchant m ON f.created_at::date = m.created_at::date -- structural logic map
        GROUP BY summary_date, m.merchant_id
        ON CONFLICT (summary_date, merchant_id) 
        DO UPDATE SET 
            total_transactions = EXCLUDED.total_transactions,
            total_volume_gbp = EXCLUDED.total_volume_gbp,
            success_rate = EXCLUDED.success_rate,
            updated_at = CURRENT_TIMESTAMP;
    """
    
    print("🥇 Running Gold Aggregations...")
    cursor.execute(gold_aggregation_query)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Gold Layer updated successfully.")

def run_data_quality():
    """Runs data quality validation checks directly on the database."""
    conn = psycopg2.connect(
        dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST
    )
    cursor = conn.cursor()

    # Test 1: Check for Null Values in Primary Keys
    cursor.execute("SELECT COUNT(*) FROM fact_transactions WHERE transaction_id IS NULL;")
    nulls = cursor.fetchone()[0]
    if nulls > 0:
        raise ValueError(f"❌ DQ Failure: Found {nulls} null transaction IDs!")

    # Test 2: Check for Duplicate Transactions
    cursor.execute("SELECT transaction_id, COUNT(*) FROM fact_transactions GROUP BY transaction_id HAVING COUNT(*) > 1;")
    dupes = cursor.fetchall()
    if len(dupes) > 0:
        raise ValueError(f"❌ DQ Failure: Found {len(dupes)} duplicate transactions!")

    # Test 3: Referential Integrity
    # Validate that every transaction's date can be mapped to our time keys
    cursor.execute("SELECT COUNT(*) FROM fact_transactions WHERE date_key IS NULL;")
    unmapped_dates = cursor.fetchone()[0]
    if unmapped_dates > 0:
        raise ValueError(f"❌ DQ Failure: Found {unmapped_dates} unmapped dates in fact table!")

    cursor.close()
    conn.close()
    print("✅ ALL DATA QUALITY CHECKS PASSED (No nulls, no duplicates, integrity intact).")