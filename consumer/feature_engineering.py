import os
import time
from datetime import datetime, timedelta
from pymongo import MongoClient
from collections import defaultdict

# --- Configuration ---
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB = 'DataMiningProject'

# --- MongoDB Connection ---
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

curated_collection = db['curated.events']
features_collection = db['curated.features']

# Create indexes
features_collection.create_index("event_id", unique=True)
features_collection.create_index("entity_id")
features_collection.create_index("timestamp")


def calculate_rolling_statistics(entity_id, current_timestamp, window_minutes=15):
    """
    Calculate rolling statistics for an entity within a time window.
    Returns: rolling_mean, rolling_std, rolling_min, rolling_max, event_count
    """
    if not current_timestamp:
        return None, None, None, None, 0
    
    try:
        # Parse timestamp
        current_dt = datetime.fromisoformat(current_timestamp)
        window_start = current_dt - timedelta(minutes=window_minutes)
        
        # Get events in the window
        events = list(curated_collection.find({
            'entity_id': entity_id,
            'timestamp': {
                '$gte': window_start.isoformat(),
                '$lte': current_timestamp
            },
            'value': {'$ne': None}
        }).sort('timestamp', 1))
        
        if not events:
            return None, None, None, None, 0
        
        values = [e['value'] for e in events if e.get('value') is not None]
        
        if not values:
            return None, None, None, None, 0
        
        # Calculate statistics
        rolling_mean = sum(values) / len(values)
        rolling_min = min(values)
        rolling_max = max(values)
        
        # Calculate standard deviation
        if len(values) > 1:
            variance = sum((x - rolling_mean) ** 2 for x in values) / len(values)
            rolling_std = variance ** 0.5
        else:
            rolling_std = 0.0
        
        return rolling_mean, rolling_std, rolling_min, rolling_max, len(values)
        
    except Exception as e:
        print(f"Error calculating rolling stats: {e}")
        return None, None, None, None, 0


def calculate_inter_arrival_time(entity_id, current_timestamp):
    """
    Calculate time since last event for this entity (in seconds).
    Returns: inter_arrival_time_seconds
    """
    if not current_timestamp:
        return None
    
    try:
        current_dt = datetime.fromisoformat(current_timestamp)
        
        # Find the previous event
        previous_event = curated_collection.find_one({
            'entity_id': entity_id,
            'timestamp': {'$lt': current_timestamp},
            'value': {'$ne': None}
        }, sort=[('timestamp', -1)])
        
        if not previous_event or not previous_event.get('timestamp'):
            return None
        
        previous_dt = datetime.fromisoformat(previous_event['timestamp'])
        delta = current_dt - previous_dt
        
        return delta.total_seconds()
        
    except Exception as e:
        print(f"Error calculating inter-arrival time: {e}")
        return None


def calculate_rate_of_change(entity_id, current_timestamp, lookback_events=3):
    """
    Calculate rate of change over last N events.
    Returns: rate_of_change (value change per second)
    """
    if not current_timestamp:
        return None
    
    try:
        # Get last N events including current
        events = list(curated_collection.find({
            'entity_id': entity_id,
            'timestamp': {'$lte': current_timestamp},
            'value': {'$ne': None}
        }).sort('timestamp', -1).limit(lookback_events))
        
        if len(events) < 2:
            return None
        
        # Calculate change between oldest and newest
        oldest = events[-1]
        newest = events[0]
        
        value_change = newest['value'] - oldest['value']
        
        oldest_dt = datetime.fromisoformat(oldest['timestamp'])
        newest_dt = datetime.fromisoformat(newest['timestamp'])
        time_delta = (newest_dt - oldest_dt).total_seconds()
        
        if time_delta == 0:
            return None
        
        return value_change / time_delta
        
    except Exception as e:
        print(f"Error calculating rate of change: {e}")
        return None


def calculate_event_frequency(entity_id, current_timestamp, window_minutes=15):
    """
    Calculate event frequency (events per minute) within window.
    Returns: events_per_minute
    """
    if not current_timestamp:
        return None
    
    try:
        current_dt = datetime.fromisoformat(current_timestamp)
        window_start = current_dt - timedelta(minutes=window_minutes)
        
        event_count = curated_collection.count_documents({
            'entity_id': entity_id,
            'timestamp': {
                '$gte': window_start.isoformat(),
                '$lte': current_timestamp
            },
            'value': {'$ne': None}
        })
        
        return event_count / window_minutes if window_minutes > 0 else 0
        
    except Exception as e:
        print(f"Error calculating event frequency: {e}")
        return None


def engineer_features(event):
    """
    Generate all engineered features for an event.
    """
    entity_id = event.get('entity_id')
    timestamp = event.get('timestamp')
    value = event.get('value')
    
    # Start with base event data
    features = {
        'event_id': event.get('event_id'),
        'entity_id': entity_id,
        'timestamp': timestamp,
        'value': value,
        'category': event.get('category'),
        'platform': event.get('platform'),
        'geo': event.get('geo'),
        'baseline_mean': event.get('baseline_mean'),
        'baseline_std': event.get('baseline_std'),
        'label': event.get('label'),
        'value_delta': event.get('value_delta'),
        'z_score': event.get('z_score'),
        'is_outlier': event.get('is_outlier'),
        'data_quality_issue': event.get('data_quality_issue')
    }
    
    # Feature 1-4: Rolling statistics (15-minute window)
    rolling_mean, rolling_std, rolling_min, rolling_max, event_count = \
        calculate_rolling_statistics(entity_id, timestamp, window_minutes=15)
    
    features['rolling_mean_15min'] = rolling_mean
    features['rolling_std_15min'] = rolling_std
    features['rolling_min_15min'] = rolling_min
    features['rolling_max_15min'] = rolling_max
    features['event_count_15min'] = event_count
    
    # Feature 5: Inter-arrival time
    features['inter_arrival_time_sec'] = calculate_inter_arrival_time(entity_id, timestamp)
    
    # Feature 6: Rate of change
    features['rate_of_change'] = calculate_rate_of_change(entity_id, timestamp, lookback_events=3)
    
    # Feature 7: Event frequency
    features['event_frequency_per_min'] = calculate_event_frequency(entity_id, timestamp, window_minutes=15)
    
    # Feature 8: High activity indicator (>2 events per minute)
    if features['event_frequency_per_min'] is not None:
        features['is_high_activity'] = features['event_frequency_per_min'] > 2.0
    else:
        features['is_high_activity'] = False
    
    # Feature 9: Deviation from rolling mean
    if rolling_mean is not None and value is not None:
        features['deviation_from_rolling_mean'] = value - rolling_mean
        
        # Feature 10: Relative position in rolling range (0-1 scale)
        if rolling_max is not None and rolling_min is not None and rolling_max != rolling_min:
            features['relative_position_in_range'] = \
                (value - rolling_min) / (rolling_max - rolling_min)
        else:
            features['relative_position_in_range'] = None
    else:
        features['deviation_from_rolling_mean'] = None
        features['relative_position_in_range'] = None
    
    # Feature 11: Is anomaly (combines outlier and high deviation)
    if features.get('is_outlier') and features.get('deviation_from_rolling_mean') is not None:
        features['is_anomaly'] = features['is_outlier'] and \
            abs(features['deviation_from_rolling_mean']) > 50
    else:
        features['is_anomaly'] = False
    
    return features


def feature_engineering_pipeline():
    """
    Main feature engineering pipeline.
    """
    print("Starting feature engineering pipeline...")
    
    processed = 0
    features_created = 0
    skipped = 0
    
    # Process all curated events
    events = curated_collection.find({
        'value': {'$ne': None},
        'timestamp': {'$ne': None}
    }).sort('timestamp', 1)
    
    for event in events:
        try:
            features = engineer_features(event)
            
            if features.get('event_id'):
                try:
                    features_collection.insert_one(features)
                    features_created += 1
                    
                    if features_created % 10 == 0:
                        print(f"Engineered features for {features_created} events...")
                        
                except Exception as e:
                    if 'duplicate key' in str(e).lower():
                        skipped += 1
                    else:
                        print(f"Error inserting features for event {features['event_id']}: {e}")
            
            processed += 1
            
        except Exception as e:
            print(f"Error engineering features: {e}")
            skipped += 1
    
    print("\n" + "="*50)
    print("FEATURE ENGINEERING COMPLETE")
    print("="*50)
    print(f"Total events processed: {processed}")
    print(f"Features created: {features_created}")
    print(f"Skipped (duplicates/errors): {skipped}")
    print("="*50)
    
    # Print sample feature record
    sample = features_collection.find_one({'rolling_mean_15min': {'$ne': None}})
    if sample:
        print("\nSample feature record:")
        for key, value in sample.items():
            if key != '_id':
                print(f"  {key}: {value}")


def continuous_feature_engineering():
    """
    Continuously engineer features for new events.
    """
    print("Starting continuous feature engineering mode...")
    print("Waiting for MongoDB and transform pipeline to be ready...")
    time.sleep(15)
    
    # Do initial feature engineering
    feature_engineering_pipeline()
    
    # Then run periodically
    print("\nEntering polling mode (checking every 30 seconds)...")
    
    while True:
        time.sleep(30)
        
        # Check for new curated events
        curated_count = curated_collection.count_documents({
            'value': {'$ne': None},
            'timestamp': {'$ne': None}
        })
        features_count = features_collection.count_documents({})
        
        if curated_count > features_count:
            print(f"\nNew curated events detected ({curated_count - features_count} new). Engineering features...")
            feature_engineering_pipeline()
        else:
            print(".", end="", flush=True)


if __name__ == "__main__":
    continuous_feature_engineering()

