import json
import re
from datetime import datetime
import time
import os

from kafka import KafkaConsumer
from pymongo import MongoClient

# --- Configuration ---
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9022')
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB = 'DataMiningProject'
TOPICS = ['dirty_events', 'dirty_metadata', 'dirty_reference']

# --- MongoDB Connection ---
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
raw_collection = db['raw_dirty']
cleaned_collection = db['cleaned.events']
cleaned_collection.create_index("event_id", unique=True)


# --- Cleaning and Standardization Functions ---

def standardize_timestamp(ts):
    """Converts various timestamp formats to ISO 8601."""
    if ts is None:
        return None
    if isinstance(ts, (int, float)): # Unix timestamp
        try:
            return datetime.fromtimestamp(ts).isoformat()
        except OSError:
            return None # Handle out-of-range timestamps
    try: # ISO format
        datetime.fromisoformat(ts)
        return ts
    except (ValueError, TypeError):
        pass
    try: # String format like 'DD-Mon-YYYY HH:MM:SS'
        return datetime.strptime(ts, '%d-%b-%Y %H:%M:%S').isoformat()
    except (ValueError, TypeError):
        return None # Unparseable format

def clean_numeric(value, default=None):
    """Converts a value to a float, handling non-numeric strings."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned_str = re.sub(r"[^0-9.-]", "", value)
        try:
            return float(cleaned_str)
        except (ValueError, TypeError):
            return default
    return default

def clean_record(msg):
    """Applies all cleaning rules to a raw message."""
    data = msg.value
    cleaned_data = {}
    
    # 1. Standardize Timestamps
    cleaned_data['timestamp'] = standardize_timestamp(data.get('timestamp'))
    
    # 2. Enforce Consistent Data Types
    cleaned_data['event_id'] = str(data.get('event_id', ''))
    cleaned_data['entity_id'] = str(data.get('entity_id', ''))
    cleaned_data['value'] = clean_numeric(data.get('value'))
    
    # 3. Normalize Categorical Fields
    category = data.get('category')
    if category:
        cleaned_data['category'] = category.lower().replace('_', ' ')
        
    platform = data.get('platform')
    if platform:
        cleaned_data['platform'] = platform.lower()
        
    # 4. Impute or Flag Missing Values
    if cleaned_data['value'] is None:
        cleaned_data['data_quality_issue'] = 'missing_value'
        
    # --- FIX for null z_score ---
    # Clean the reference data fields as well
    cleaned_data['baseline_mean'] = clean_numeric(data.get('baseline_mean'))
    cleaned_data['baseline_std'] = clean_numeric(data.get('baseline_std'))
    
    # Copy over other fields that don't need cleaning
    for key in ['geo', 'label']: # Removed baseline fields from this list
        if key in data:
            cleaned_data[key] = data[key]

    return cleaned_data

# --- Main Consumer Loop ---
def main():
    """Main function to run the cleaning consumer."""
    print("Waiting for Kafka to be ready...")
    # FIX for NoBrokersAvailable error
    time.sleep(25) 
    
    print("Starting cleaning consumer...")
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        group_id='cleaning-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    cleaned_count = 0
    dropped_count = 0
    
    for message in consumer:
        raw_collection.insert_one(message.value)
        cleaned_data = clean_record(message)
        
        if 'event_id' in cleaned_data and cleaned_data['event_id']:
            try:
                cleaned_collection.insert_one(cleaned_data)
                cleaned_count += 1
                print(f"Cleaned and stored record: {cleaned_data['event_id']}")
            except Exception as e: # pymongo.errors.DuplicateKeyError
                dropped_count += 1
                print(f"Dropped duplicate record: {cleaned_data['event_id']}.")
        else:
            dropped_count += 1
            print(f"Dropped record with missing event_id.")
            
        if (cleaned_count + dropped_count) % 100 == 0 and (cleaned_count + dropped_count) > 0:
            print(f"--- STATS ---")
            print(f"Total processed: {cleaned_count + dropped_count}")
            print(f"Cleaned and stored: {cleaned_count}")
            print(f"Dropped (duplicates/invalid): {dropped_count}")
            print(f"---------------")

if __name__ == "__main__":
    main()


