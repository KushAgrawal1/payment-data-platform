import pytest
from pyspark.sql import Row

def test_deduplication(spark_session):
    """Ensure that duplicate transaction IDs are cleanly dropped."""
    # Create mock rows with duplicate transaction IDs
    data = [
        Row(transaction_id="tx_101", amount=50.0, status="SUCCESS"),
        Row(transaction_id="tx_101", amount=50.0, status="SUCCESS"),  # Duplicate
        Row(transaction_id="tx_102", amount=120.0, status="FAILED")
    ]
    
    df = spark_session.createDataFrame(data)
    
    # Deduplication step
    deduped_df = df.dropDuplicates(["transaction_id"])
    result = deduped_df.collect()
    
    assert deduped_df.count() == 2
    # Convert back to list of transaction IDs to verify
    ids = [row.transaction_id for row in result]
    assert "tx_101" in ids
    assert "tx_102" in ids