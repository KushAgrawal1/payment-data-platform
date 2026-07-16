import os
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()

def create_topics():
    # Use localhost:9092 for running locally on your Mac host machine
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    print(f"Connecting to Kafka Admin Client at: {bootstrap_servers}")
    
    admin = AdminClient({'bootstrap.servers': bootstrap_servers})
    
    # Define our production-grade topology
    topics = [
        NewTopic("payments_transactions", num_partitions=6, replication_factor=1),
        NewTopic("payments_refunds", num_partitions=3, replication_factor=1),
        NewTopic("payments_fraud_events", num_partitions=3, replication_factor=1)
    ]
    
    # Execute topic creation
    fs = admin.create_topics(topics)
    
    for topic, f in fs.items():
        try:
            f.result() # Blocks until topic is created or fails
            print(f"✅ Topic '{topic}' created successfully.")
        except Exception as e:
            print(f"⚠️ Topic '{topic}' already exists or skipped: {e}")

if __name__ == "__main__":
    create_topics()