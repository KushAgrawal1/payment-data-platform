import pytest
from ingestion.payment_generator import PaymentGenerator

def test_generate_transaction():
    gen = PaymentGenerator()
    tx = gen.generate_transaction()
    
    # 1. Assert required schema elements are present
    assert "transaction_id" in tx
    assert "customer_id" in tx
    assert "merchant_id" in tx
    assert "amount" in tx
    assert "currency" in tx
    assert "country" in tx
    assert "payment_method" in tx
    assert "status" in tx
    assert "timestamp" in tx

    # 2. Assert data values conform to domain limits
    assert tx["amount"] > 0
    assert tx["status"] in ["SUCCESS", "FAILED", "REFUND", "CHARGEBACK"]
    assert tx["country"] in gen.countries
    
    # 3. Assert currency maps correctly to country choice
    expected_currency = gen.country_currencies[tx["country"]]
    assert tx["currency"] == expected_currency