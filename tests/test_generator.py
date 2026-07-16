import sys
import os
from unittest.mock import MagicMock

# 1. Inject 'airflow/dags' into the system path so Python can find 'data_generator'
dags_path = os.path.join(os.getcwd(), "airflow", "dags")
if dags_path not in sys.path:
    sys.path.insert(0, dags_path)

# 2. Mock 'airflow' dependencies so the real library doesn't load/initialize
sys.modules["airflow"] = MagicMock()
sys.modules["airflow.models"] = MagicMock()
sys.modules["airflow.utils"] = MagicMock()
sys.modules["airflow.utils.session"] = MagicMock()

# 3. Now perform the import
from data_generator import generate_payments

# --- Test Functions ---
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