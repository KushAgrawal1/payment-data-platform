import pytest
# Assuming your generator file inside airflow/dags or ingestion is importable
from data_generator import generate_payments

def test_generate_payments_format():
    """Verify that generate_payments produces a valid list of records."""
    records = generate_payments(num_records=5, write_to_kafka=False)
    
    assert isinstance(records, list)
    assert len(records) == 5
    
    for tx in records:
        assert "transaction_id" in tx
        assert "amount" in tx
        assert "status" in tx
        assert isinstance(tx["amount"], (int, float))
        assert tx["amount"] > 0
        assert tx["status"] in ["SUCCESS", "FAILED", "REFUND", "CHARGEBACK"]