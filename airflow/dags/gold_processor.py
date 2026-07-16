
import logging
import os

import psycopg2

log = logging.getLogger(__name__)

POSTGRES_DB = os.environ.get("POSTGRES_DB", "payment_dw")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]


def _connect():
    return psycopg2.connect(
        dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST, port=POSTGRES_PORT,
    )


GOLD_UPSERT = """
INSERT INTO gold_daily_merchant_kpis
    (summary_date, merchant_id, total_transactions, total_volume_gbp, success_rate)
SELECT
    f.created_at::date                          AS summary_date,
    m.merchant_id,
    COUNT(f.transaction_id)                     AS total_transactions,
    SUM(f.amount_gbp)                           AS total_volume_gbp,
    ROUND(
        SUM(CASE WHEN f.status = 'SUCCESS' THEN 1 ELSE 0 END)::numeric
        / COUNT(f.transaction_id) * 100, 2
    )                                           AS success_rate
FROM fact_transactions f
JOIN dim_merchant m ON m.merchant_sk = f.merchant_sk   -- FIXED: join on the key
GROUP BY f.created_at::date, m.merchant_id
ON CONFLICT (summary_date, merchant_id) DO UPDATE SET
    total_transactions = EXCLUDED.total_transactions,
    total_volume_gbp   = EXCLUDED.total_volume_gbp,
    success_rate       = EXCLUDED.success_rate,
    updated_at         = CURRENT_TIMESTAMP;
"""


def load_gold_layer_to_dw() -> None:
    """Computes daily per-merchant KPIs from facts, idempotently upserted."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("""
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
        cur.execute(GOLD_UPSERT)
    log.info("Gold layer updated.")


def run_data_quality() -> None:
    """Hard-fails the DAG on any integrity violation."""
    checks = [
        ("null transaction IDs",
         "SELECT COUNT(*) FROM fact_transactions WHERE transaction_id IS NULL"),
        ("duplicate transaction IDs",
         """SELECT COUNT(*) FROM (
                SELECT transaction_id FROM fact_transactions
                GROUP BY transaction_id HAVING COUNT(*) > 1) d"""),
        ("facts with unresolved merchant_sk",
         "SELECT COUNT(*) FROM fact_transactions WHERE merchant_sk IS NULL"),
        ("facts with unresolved customer_sk",
         "SELECT COUNT(*) FROM fact_transactions WHERE customer_sk IS NULL"),
        ("facts with null date_key",
         "SELECT COUNT(*) FROM fact_transactions WHERE date_key IS NULL"),
        ("negative or zero amounts",
         "SELECT COUNT(*) FROM fact_transactions WHERE amount_gbp <= 0"),
    ]
    with _connect() as conn, conn.cursor() as cur:
        failures = []
        for name, sql in checks:
            cur.execute(sql)
            n = cur.fetchone()[0]
            if n > 0:
                failures.append(f"{name}: {n}")
        if failures:
            raise ValueError("DQ failures -> " + "; ".join(failures))
    log.info("All data quality checks passed.")