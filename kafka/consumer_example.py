import os
import json
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

def run_consumer():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'local-test-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    
    # Let's subscribe to both topics at once to monitor all traffic!
    target_topics = ['payments_transactions', 'payments_refunds']
    consumer.subscribe(target_topics)
    
    print(f"Listening for messages on {target_topics}... Press Ctrl+C to exit.")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"⚠️ Consumer error: {msg.error()}")
                continue
                
            payload = json.loads(msg.value().decode('utf-8'))
            print(f"📥 Received from [{msg.topic()}] (Part: {msg.partition()}): {payload}")
            
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    run_consumer()