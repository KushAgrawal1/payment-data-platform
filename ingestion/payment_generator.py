import os
import json
import random
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("PaymentGenerator")

load_dotenv()

class PaymentGenerator:
    def __init__(self):
        self.countries = ["US", "GB", "DE", "CA", "AU", "FR", "JP"]
        self.payment_methods = ["credit_card", "bank_transfer", "digital_wallet", "crypto"]
        self.statuses = (
            ["SUCCESS"] * 85 + 
            ["FAILED"] * 10 + 
            ["REFUND"] * 3 + 
            ["CHARGEBACK"] * 2
        )
        self.country_currencies = {
            "US": "USD", "GB": "GBP", "DE": "EUR", "FR": "EUR",
            "CA": "CAD", "AU": "AUD", "JP": "JPY"
        }

    def generate_transaction(self) -> dict:
        tx_id = f"TXN-{random.randint(10000000, 99999999)}"
        customer_id = f"CUST-{random.randint(10000, 99999)}"
        merchant_id = f"MERCH-{random.randint(1000, 9999)}"
        
        country = random.choice(self.countries)
        currency = self.country_currencies.get(country, "USD")
        amount = round(random.uniform(5.00, 1500.00), 2)
        payment_method = random.choice(self.payment_methods)
        status = random.choice(self.statuses)
        
        return {
            "transaction_id": tx_id,
            "customer_id": customer_id,
            "merchant_id": merchant_id,
            "amount": amount,
            "currency": currency,
            "country": country,
            "payment_method": payment_method,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Delivered to {msg.topic()} [Partition: {msg.partition()}]")

def run_producer():
    kafka_broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    rate_delay = float(os.getenv("GENERATOR_RATE_DELAY", "1.0"))

    logger.info(f"Initializing Kafka Producer targeting {kafka_broker}...")
    
    conf = {
        'bootstrap.servers': kafka_broker,
        'client.id': 'payment-generator-producer',
        'acks': 'all'
    }
    
    try:
        producer = Producer(conf)
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return

    generator = PaymentGenerator()
    logger.info("Starting multi-topic message stream... Press Ctrl+C to stop.")

    try:
        while True:
            tx_data = generator.generate_transaction()
            payload = json.dumps(tx_data)
            
            # ROUTING LOGIC: Determine target topic by transaction status
            if tx_data["status"] in ["REFUND", "CHARGEBACK"]:
                target_topic = "payments_refunds"
            else:
                target_topic = "payments_transactions"
            
            # Send message asynchronously
            producer.produce(
                topic=target_topic,
                key=tx_data["transaction_id"],
                value=payload,
                callback=delivery_report
            )
            
            producer.poll(0)
            logger.info(f"Sent to {target_topic}: {payload}")
            time.sleep(rate_delay)
            
    except KeyboardInterrupt:
        logger.info("Stopping generator...")
    finally:
        logger.info("Flushing final records...")
        producer.flush()

if __name__ == "__main__":
    run_producer()