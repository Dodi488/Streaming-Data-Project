import json
import random
import time
import uuid
import os
from datetime import datetime, timedelta

from faker import Faker
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

# --- Kafka Configuration ---
# Reads from the environment variable set in docker-compose.yml
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9022')
TOPICS = ['dirty_events', 'dirty_metadata', 'dirty_reference']

# Initialize Faker for synthetic data
fake = Faker()

# --- Topic Creation ---
def create_topics():
    """Creates the necessary Kafka topics if they don't exist."""
    print(f"Connecting to Kafka admin at {KAFKA_BROKER}...")
    admin_client = None
    retries = 10
    while retries > 0:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BROKER,
                client_id='dirty_data_producer'
            )
            existing_topics = admin_client.list_topics()
            print("Successfully connected to Kafka admin.")
            break # Exit loop on successful connection
        except Exception as e:
            print(f"Kafka not ready yet, retrying... ({retries} left). Error: {e}")
            retries -= 1
            time.sleep(5)
            
    if not admin_client:
        print("Could not connect to Kafka after 10 retries. Exiting.")
        exit(1)

    try:
        existing_topics = admin_client.list_topics()
        topic_list = []
        for topic in TOPICS:
            if topic not in existing_topics:
                print(f"Topic '{topic}' not found. Creating it.")
                topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
        
        if topic_list:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print("Topics created successfully.")
        else:
            print("All topics already exist.")
            
    except Exception as e:
        print(f"Could not create topics: {e}")
    finally:
        admin_client.close()


# --- Producer Initialization ---
def get_producer():
    """Initializes and returns a Kafka producer."""
    retries = 10
    producer = None
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all'
            )
            print("Successfully connected Kafka producer.")
            return producer
        except Exception as e:
            print(f"Kafka producer not ready yet, retrying... ({retries} left). Error: {e}")
            retries -= 1
            time.sleep(5)
    
    if not producer:
        print("Could not connect Kafka producer after 10 retries. Exiting.")
        exit(1)

# --- Data Generation Functions with Intentional Flaws ---

def generate_dirty_event(entity_id, last_event_id=None):
    """Generates a single dirty event record."""
    # 10% chance to be a duplicate
    event_id = last_event_id if random.random() < 0.1 else str(uuid.uuid4())
    
    # Mix timestamp formats
    ts_format_choice = random.choice(['iso', 'unix', 'string', 'missing'])
    if ts_format_choice == 'iso':
        timestamp = datetime.now().isoformat()
    elif ts_format_choice == 'unix':
        timestamp = time.time()
    elif ts_format_choice == 'string':
        timestamp = datetime.now().strftime('%d-%b-%Y %H:%M:%S')
    else:
        timestamp = None # Missing timestamp
    
    # 15% chance of corrupted numeric type
    value = str(random.randint(0, 100)) if random.random() < 0.15 else round(random.uniform(10, 500), 2)

    return {
        "event_id": event_id,
        "entity_id": entity_id,
        "timestamp": timestamp,
        "value": value
    }

def generate_dirty_metadata(entity_id):
    """Generates a dirty metadata record."""
    # Inconsistent casing and nulls
    platform = random.choice(['android', 'IOS', 'web', 'Web', None])
    category = random.choice(['TYPE_A', 'type_b', 'Type_C', 'type_a'])
    
    return {
        "entity_id": entity_id,
        "category": category,
        "geo": fake.country_code(),
        "platform": platform,
        "event_id": str(uuid.uuid4())
    }

def generate_dirty_reference(entity_id):
    """Generates a dirty reference data record."""
    # 10% chance of "N/A"
    # mean = "N/A" if random.random() < 0.1 else round(random.uniform(200, 300), 2)
    # 10% chance of being null
    # std = str(round(random.uniform(20, 40), 2)) if random.random() > 0.1 else None
    mean = round(random.uniform(200, 300), 2)
    std = str(round(random.uniform(20, 40), 2))
    
    return {
        "entity_id": entity_id,
        "baseline_mean": mean,
        "baseline_std": std,
        "label": f"label_{random.randint(1,5)}",
        "event_id": str(uuid.uuid4())
    }

# --- Main Production Loop ---
def main():
    """Main function to run the producer."""
    print("Waiting for Kafka service to be ready...")
    # This sleep is less critical now thanks to retry logic, but good for docker-compose startup
    time.sleep(10) 
    
    create_topics()
    producer = get_producer()
    
    record_count = 0
    last_event_id = None
    entity_ids = [str(uuid.uuid4()) for _ in range(20)] # Simulate 20 distinct entities

    # --- THIS IS THE "PRIMING" LOOP ---
    print(f"Priming the system with metadata and reference data for {len(entity_ids)} entities...")
    for entity_id in entity_ids:
        # Send one metadata doc for this entity
        metadata = generate_dirty_metadata(entity_id)
        producer.send(TOPICS[1], value=metadata)
        
        # Send one reference doc for this entity
        reference = generate_dirty_reference(entity_id)
        producer.send(TOPICS[2], value=reference)
    
    producer.flush()
    print("Priming complete. Starting real-time event stream...")
    # --- END OF PRIMING LOOP ---

    try:
        while True:
            entity_id = random.choice(entity_ids)
            
            # Send to dirty_events
            event_data = generate_dirty_event(entity_id, last_event_id)
            producer.send(TOPICS[0], value=event_data)
            last_event_id = event_data['event_id'] # Keep track for potential duplication

            # Occasionally send updated metadata
            if random.random() < 0.05: # 5% chance
                metadata = generate_dirty_metadata(entity_id)
                producer.send(TOPICS[1], value=metadata)
            
            # Occasionally send updated reference data
            if random.random() < 0.02: # 2% chance
                reference = generate_dirty_reference(entity_id)
                producer.send(TOPICS[2], value=reference)
                
            producer.flush()
            record_count += 1
            print(f"Sent record #{record_count}: event_id={event_data['event_id']}")
            
            time.sleep(random.uniform(1, 4)) # Generate data every 1-4 seconds
    except KeyboardInterrupt:
        print("Stopping producer.")
    finally:
        producer.close()
        print("Producer closed.")

if __name__ == "__main__":
    main()


