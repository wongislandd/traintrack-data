import time
from collections import Counter
from typing import Dict, Optional, List, Tuple
from pathlib import Path

import requests
from google.transit import gtfs_realtime_pb2

from gtfs_utils import create_supabase_client
from load_gtfs_cache import load_cache

# Define all GTFS realtime feeds
GTFS_FEEDS = [
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si"
]

def get_cache_dir() -> Path:
    """
    Get the path to the cache directory.
    
    Returns:
        Path: Path to the cache directory
    """
    script_dir = Path(__file__).parent
    cache_dir = script_dir / "cache"
    cache_dir.mkdir(exist_ok=True)
    return cache_dir

def get_full_trip_id(trip_ids: List[str], partial_trip_id: str) -> Optional[str]:
    """Get the full trip ID that contains the partial trip ID."""
    for trip_id in trip_ids:
        if partial_trip_id in trip_id:
            return trip_id
    return None

def get_database_stop_id(stop_ids: Dict[str, str], realtime_stop_id: str) -> Optional[str]:
    """Get the database stop ID for a realtime stop ID."""
    return stop_ids.get(realtime_stop_id)

def upload_batch(supabase, table_name: str, data: List[Dict], batch_size: int = 5000):
    """Upload data in batches to Supabase. If batch upload fails, attempts individual uploads."""
    if not data:
        return 0
    
    total_uploaded = 0
    failed_batches = 0
    failed_records = 0
    
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        try:
            response = supabase.table(table_name).upsert(batch).execute()
            total_uploaded += len(batch)
            print(f"Uploaded {total_uploaded}/{len(data)} records to {table_name}")
        except Exception as e:
            print(f"Error uploading batch to {table_name}: {e}")
            print("Attempting individual record uploads for this batch...")
            failed_batches += 1
            
            # Try uploading each record individually
            for record in batch:
                try:
                    response = supabase.table(table_name).upsert([record]).execute()
                    total_uploaded += 1
                except Exception as individual_error:
                    print(f"Failed to upload individual record: {individual_error}")
                    failed_records += 1
                    continue
    
    if failed_batches > 0:
        print(f"\nUpload Summary for {table_name}:")
        print(f"Total records: {len(data)}")
        print(f"Successfully uploaded: {total_uploaded}")
        print(f"Failed batches: {failed_batches}")
        print(f"Failed individual records: {failed_records}")
    
    return total_uploaded

def process_feed(feed_url: str, trip_ids: List[str], stop_ids: Dict[str, str], current_timestamp: int) -> Tuple[List[Dict], List[Dict], Dict]:
    """Process a single GTFS realtime feed and return the updates and statistics."""
    print(f"\nProcessing feed: {feed_url}")
    
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        response = requests.get(feed_url)
        feed.ParseFromString(response.content)
    except Exception as e:
        print(f"Error fetching feed {feed_url}: {e}")
        return [], [], {
            'total_entities': 0,
            'processed_trips': 0,
            'skipped_trips': 0,
            'skipped_stops': 0,
            'processed_stops': 0,
            'skipped_trip_ids': Counter(),
            'skipped_stop_ids': Counter()
        }
    
    print(f"Number of entities in feed: {len(feed.entity)}")
    
    # Statistics tracking
    stats = {
        'total_entities': len(feed.entity),
        'processed_trips': 0,
        'skipped_trips': 0,
        'skipped_stops': 0,
        'processed_stops': 0,
        'skipped_trip_ids': Counter(),
        'skipped_stop_ids': Counter()
    }
    
    trip_updates = []
    stop_updates = []
    
    for i, entity in enumerate(feed.entity):
        if entity.HasField('trip_update'):
            partial_trip_id = entity.trip_update.trip.trip_id
            full_trip_id = get_full_trip_id(trip_ids, partial_trip_id)
            
            if full_trip_id:
                stats['processed_trips'] += 1
                trip_update_data = {
                    'trip_id': full_trip_id,
                    'route_id': entity.trip_update.trip.route_id,
                    'direction_id': entity.trip_update.trip.direction_id,
                    'timestamp': current_timestamp
                }
                trip_updates.append(trip_update_data)
                
                for update in entity.trip_update.stop_time_update:
                    database_stop_id = get_database_stop_id(stop_ids, update.stop_id)
                    if database_stop_id:
                        stats['processed_stops'] += 1
                        stop_update = {
                            'trip_id': full_trip_id,
                            'stop_id': database_stop_id,
                            'arrival_time': update.arrival.time if update.HasField('arrival') else None,
                            'departure_time': update.departure.time if update.HasField('departure') else None
                        }
                        stop_updates.append(stop_update)
                    else:
                        stats['skipped_stops'] += 1
                        stats['skipped_stop_ids'][update.stop_id] += 1
            else:
                stats['skipped_trips'] += 1
                stats['skipped_trip_ids'][partial_trip_id] += 1
            
            if (i + 1) % 100 == 0:
                print(f"Processed {i + 1}/{len(feed.entity)} entities")
    
    return trip_updates, stop_updates, stats

def parse_gtfs_realtime():
    # Initialize Supabase client
    supabase = create_supabase_client()
    
    # Get cache directory and load cache data
    cache_dir = get_cache_dir()
    print("Loading cache data...")
    cache_data = load_cache(cache_dir)
    
    # Extract trip and stop IDs from cache
    trip_ids = [trip['id'] for trip in cache_data.get('trips', [])]
    stop_ids = {stop['id']: stop['id'] for stop in cache_data.get('stops', [])}
    
    print(f"Loaded {len(trip_ids)} trip IDs and {len(stop_ids)} stop IDs from cache")
    
    current_timestamp = int(time.time())
    
    # Process all feeds
    all_trip_updates = []
    all_stop_updates = []
    all_stats = {
        'total_entities': 0,
        'processed_trips': 0,
        'skipped_trips': 0,
        'skipped_stops': 0,
        'processed_stops': 0,
        'skipped_trip_ids': Counter(),
        'skipped_stop_ids': Counter()
    }
    
    for feed_url in GTFS_FEEDS:
        trip_updates, stop_updates, stats = process_feed(feed_url, trip_ids, stop_ids, current_timestamp)
        
        all_trip_updates.extend(trip_updates)
        all_stop_updates.extend(stop_updates)
        
        # Aggregate statistics
        for key in ['total_entities', 'processed_trips', 'skipped_trips', 'skipped_stops', 'processed_stops']:
            all_stats[key] += stats[key]
        all_stats['skipped_trip_ids'].update(stats['skipped_trip_ids'])
        all_stats['skipped_stop_ids'].update(stats['skipped_stop_ids'])
    
    # Upload all trip updates
    print("\nUploading trip updates...")
    trip_upload_result = upload_batch(supabase, 'trip_updates', all_trip_updates)
    
    if trip_upload_result is None or trip_upload_result != len(all_trip_updates):
        print("Error: Failed to upload all trip updates. Aborting stop updates to maintain data consistency.")
        return
    
    # Upload all stop updates
    print("\nUploading stop updates...")
    stop_upload_result = upload_batch(supabase, 'stop_updates', all_stop_updates)
    
    if stop_upload_result is None or stop_upload_result != len(all_stop_updates):
        print("Warning: Not all stop updates were uploaded successfully.")
    
    # Print final statistics
    print("\n=== Processing Statistics ===")
    print(f"Total entities across all feeds: {all_stats['total_entities']}")
    print(f"Processed trips: {all_stats['processed_trips']}")
    print(f"Skipped trips: {all_stats['skipped_trips']}")
    print(f"Processed stops: {all_stats['processed_stops']}")
    print(f"Skipped stops: {all_stats['skipped_stops']}")
    
    if all_stats['skipped_trip_ids']:
        print("\nTop 5 most frequently skipped trip IDs:")
        for trip_id, count in all_stats['skipped_trip_ids'].most_common(5):
            print(f"  {trip_id}: {count} times")
    
    if all_stats['skipped_stop_ids']:
        print("\nTop 5 most frequently skipped stop IDs:")
        for stop_id, count in all_stats['skipped_stop_ids'].most_common(5):
            print(f"  {stop_id}: {count} times")

if __name__ == "__main__":
    parse_gtfs_realtime()
