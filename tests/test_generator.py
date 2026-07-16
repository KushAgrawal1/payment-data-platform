import sys
import os
from unittest.mock import MagicMock

# Inject path
dags_path = os.path.join(os.getcwd(), "airflow", "dags")
if dags_path not in sys.path:
    sys.path.insert(0, dags_path)

# Mock Airflow
sys.modules["airflow"] = MagicMock()
sys.modules["airflow.models"] = MagicMock()
sys.modules["airflow.utils"] = MagicMock()
sys.modules["airflow.utils.session"] = MagicMock()

# Now the import, with an ignore comment for the linter
from data_generator import generate_payments  # noqa: E402

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