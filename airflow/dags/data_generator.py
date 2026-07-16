# airflow/dags/data_generator.py
import json
import random
import time

def generate_payments(num_records=100, write_to_kafka=True):
    """Generates mock payment transactions."""
    records = []
    currencies = ['USD', 'GBP', 'EUR', 'CAD']
    statuses = ['SUCCESS', 'SUCCESS', 'SUCCESS', 'FAILED', 'REFUND', 'CHARGEBACK']
    methods = ['CREDIT_CARD', 'BANK_TRANSFER', 'APPLE_PAY', 'PAYPAL']
    countries = ['US', 'GB', 'DE', 'CA', 'FR']

    for _ in range(num_records):
        tx_id = f"tx_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        payload = {
            "transaction_id": tx_id,
            "customer_id": f"cust_{random.randint(1, 500)}",
            "merchant_id": f"merch_{random.randint(1, 50)}",
            "amount": round(random.uniform(5.0, 1500.0), 2),
            "currency": random.choice(currencies),
            "status": random.choice(statuses),
            "payment_method": random.choice(methods),
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "country": random.choice(countries)
        }
        records.append(payload)

    if write_to_kafka:
        from kafka import KafkaProducer  # Lazy import to prevent local unit-test crashes!
        try:
            producer = KafkaProducer(
                bootstrap_servers=['127.0.0.1:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"🚀 Sending {num_records} transaction records to Kafka...")
            for rec in records:
                producer.send('payments_transactions', value=rec)
            producer.flush()
            print("✅ Successfully generated and sent records to Kafka.")
        except Exception as e:
            print(f"⚠️ Kafka write skipped or failed (likely offline): {e}")

    return records