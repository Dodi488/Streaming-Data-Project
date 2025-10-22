import os
import time
from pymongo import MongoClient
from datetime import datetime

# --- Configuration ---
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB = 'DataMiningProject'

# --- MongoDB Connection ---
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

cleaned_collection = db['cleaned.events']
curated_collection = db['curated.events']

# Create indexes for better performance
curated_collection.create_index("event_id", unique=True)
curated_collection.create_index("entity_id")
curated_collection.create_index("timestamp")


def get_latest_metadata(entity_id):
    """Get the most recent metadata for an entity."""
    metadata = cleaned_collection.find_one(
        {
            'entity_id': entity_id,
            'category': {'$ne': None}
        },
        sort=[('_id', -1)]
    )
    return metadata


def get_latest_reference(entity_id):
    """Get the most recent reference data for an entity."""
    reference = cleaned_collection.find_one(
        {
            'entity_id': entity_id,
            'baseline_mean': {'$ne': None}
        },
        sort=[('_id', -1)]
    )
    return reference


def enrich_event(event):
    """Enrich an event with metadata and reference data."""
    entity_id = event.get('entity_id')
    
    # Start with the event data
    enriched = {
        'event_id': event.get('event_id'),
        'entity_id': entity_id,
        'timestamp': event.get('timestamp'),
        'value': event.get('value'),
        'data_quality_issue': event.get('data_quality_issue')
    }
    
    # Get and merge metadata
    metadata = get_latest_metadata(entity_id)
    if metadata:
        enriched['category'] = metadata.get('category')
        enriched['platform'] = metadata.get('platform')
        enriched['geo'] = metadata.get('geo')
    
    # Get and merge reference data
    reference = get_latest_reference(entity_id)
    if reference:
        enriched['baseline_mean'] = reference.get('baseline_mean')
        enriched['baseline_std'] = reference.get('baseline_std')
        enriched['label'] = reference.get('label')
    
    # Calculate derived fields
    if enriched.get('value') is not None and enriched.get('baseline_mean') is not None:
        enriched['value_delta'] = enriched['value'] - enriched['baseline_mean']
        
        # Calculate z-score
        if enriched.get('baseline_std') is not None and enriched['baseline_std'] != 0:
            enriched['z_score'] = enriched['value_delta'] / enriched['baseline_std']
            
            # Flag outliers (|z-score| > 2)
            enriched['is_outlier'] = abs(enriched['z_score']) > 2
        else:
            enriched['z_score'] = None
            enriched['is_outlier'] = None
    else:
        enriched['value_delta'] = None
        enriched['z_score'] = None
        enriched['is_outlier'] = None
    
    return enriched


def transform_pipeline():
    """Main transformation pipeline."""
    print("Starting transformation pipeline...")
    
    # Track statistics
    processed = 0
    enriched = 0
    skipped = 0
    
    # Process all event records (those with actual values)
    events = cleaned_collection.find({
        'value': {'$ne': None},
        'timestamp': {'$ne': None}
    })
    
    for event in events:
        try:
            enriched_event = enrich_event(event)
            
            # Only store if we successfully enriched it
            if enriched_event.get('event_id'):
                try:
                    curated_collection.insert_one(enriched_event)
                    enriched += 1
                    
                    if enriched % 10 == 0:
                        print(f"Enriched {enriched} events...")
                        
                except Exception as e:
                    # Handle duplicates gracefully
                    if 'duplicate key' in str(e).lower():
                        skipped += 1
                    else:
                        print(f"Error inserting event {enriched_event['event_id']}: {e}")
            
            processed += 1
            
        except Exception as e:
            print(f"Error processing event: {e}")
            skipped += 1
    
    print("\n" + "="*50)
    print("TRANSFORMATION COMPLETE")
    print("="*50)
    print(f"Total events processed: {processed}")
    print(f"Successfully enriched: {enriched}")
    print(f"Skipped (duplicates/errors): {skipped}")
    print("="*50)
    
    # Print sample enriched record
    sample = curated_collection.find_one({})
    if sample:
        print("\nSample enriched record:")
        for key, value in sample.items():
            if key != '_id':
                print(f"  {key}: {value}")


def continuous_transform():
    """Continuously transform new events."""
    print("Starting continuous transformation mode...")
    print("Waiting for MongoDB to be ready...")
    time.sleep(10)
    
    # Do an initial full transformation
    transform_pipeline()
    
    # Then run periodically to catch new events
    print("\nEntering polling mode (checking every 30 seconds)...")
    
    while True:
        time.sleep(30)
        
        # Count how many cleaned events haven't been transformed yet
        cleaned_count = cleaned_collection.count_documents({
            'value': {'$ne': None},
            'timestamp': {'$ne': None}
        })
        curated_count = curated_collection.count_documents({})
        
        if cleaned_count > curated_count:
            print(f"\nNew events detected ({cleaned_count - curated_count} new). Transforming...")
            transform_pipeline()
        else:
            print(".", end="", flush=True)


if __name__ == "__main__":
    continuous_transform()

