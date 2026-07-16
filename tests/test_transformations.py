import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../spark")))

from pyspark.sql import Row
from transformations import to_silver, to_bronze


def _tx(**overrides):
    base = dict(
        transaction_id="TXN-1", customer_id="CUST-1", merchant_id="MERCH-1",
        amount=100.0, currency="USD", status="SUCCESS",
        payment_method="credit_card", timestamp="2026-07-15T10:00:00+00:00",
        country="US",
    )
    base.update(overrides)
    return Row(**base)


def test_silver_deduplicates_by_transaction_id(spark_session):
    df = spark_session.createDataFrame([_tx(), _tx(), _tx(transaction_id="TXN-2")])
    assert to_silver(df).count() == 2


def test_silver_drops_invalid_records(spark_session):
    df = spark_session.createDataFrame([
        _tx(transaction_id=None),                       # null PK
        _tx(transaction_id="TXN-3", amount=-5.0),       # negative amount
        _tx(transaction_id="TXN-4", timestamp="junk"),  # unparseable ts
        _tx(transaction_id="TXN-5"),                    # valid
    ])
    result = to_silver(df).collect()
    assert len(result) == 1
    assert result[0].transaction_id == "TXN-5"


def test_silver_converts_currency_to_gbp(spark_session):
    df = spark_session.createDataFrame([_tx(amount=100.0, currency="USD")])
    row = to_silver(df, fx_rates={"USD": 0.81}).collect()[0]
    assert row.amount_gbp == 81.0
    assert row.fx_rate_gbp == 0.81


def test_bronze_adds_partition_columns(spark_session):
    df = spark_session.createDataFrame([_tx(timestamp="2026-07-15T10:00:00+00:00")])
    row = to_bronze(df).collect()[0]
    assert (row.year, row.month, row.day) == (2026, 7, 15)
    assert row.ingestion_time is not None