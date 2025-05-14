import os
import zipfile
import csv
import time
from typing import List, Dict, Any, Set
from supabase import create_client, Client

# Supabase configuration from environment variables
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables must be set")

# Constants
UPLOAD_BATCH_SIZE = 10000  # Smaller batch size for uploads

# Mapping from GTFS file names to table names
GTFS_TO_TABLE = {
    "agency.txt": "agencies",
    "stops.txt": "stops",
    "routes.txt": "routes",
    "trips.txt": "trips",
    "stop_times.txt": "stop_times",
    "calendar.txt": "calendars",
    "calendar_dates.txt": "calendar_dates",
    "shapes.txt": "shapes",
    "transfers.txt": "transfers"
}

# Primary keys for each GTFS file type
PRIMARY_KEYS = {
    "stops.txt": ["stop_id"],
    "trips.txt": ["trip_id"],
    "routes.txt": ["route_id"],
    "stop_times.txt": ["trip_id", "stop_sequence"],
    "calendar.txt": ["service_id"],
    "calendar_dates.txt": ["service_id", "date"],
    "shapes.txt": ["shape_id", "shape_pt_sequence"],
    "transfers.txt": ["from_stop_id", "to_stop_id"],
}

# Mapping from GTFS primary key names to database column names
PRIMARY_KEY_TO_DB_COLUMN = {
    "stop_id": "id",
    "trip_id": "id",
    "route_id": "id",
    "service_id": "id",
    "shape_id": "id",
    "from_stop_id": "from_stop_id",
    "to_stop_id": "to_stop_id",
    "stop_sequence": "stop_sequence",
    "date": "date",
    "shape_pt_sequence": "shape_pt_sequence"
}

# Special handling for tables with composite keys
COMPOSITE_KEY_TABLES = {
    "stop_times": ["trip_id", "stop_sequence"],
    "shapes": ["shape_id", "shape_pt_sequence"],
    "calendar_dates": ["service_id", "date"],
    "transfers": ["from_stop_id", "to_stop_id"]
}

def create_supabase_client() -> Client:
    """Create and return a Supabase client."""
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def process_batch(supabase: Client, table_name: str, items: List[Dict[str, Any]]) -> None:
    """
    Process a batch of items and upload them to Supabase.
    
    Args:
        supabase: Supabase client instance
        table_name: Name of the table to upload to
        items: List of items to upload
    """
    if not items:
        return
    
    total_items = len(items)
    start_time = time.time()
    
    # Process items in batches
    for i in range(0, total_items, UPLOAD_BATCH_SIZE):
        batch = items[i:i + UPLOAD_BATCH_SIZE]
        batch_num = (i // UPLOAD_BATCH_SIZE) + 1
        total_batches = (total_items + UPLOAD_BATCH_SIZE - 1) // UPLOAD_BATCH_SIZE
        
        batch_start_time = time.time()
        print(f"Processing batch {batch_num}/{total_batches} for {table_name}...")
        
        try:
            supabase.table(table_name).upsert(batch).execute()
            batch_end_time = time.time()
            print(f"Uploaded {len(batch)} items in {(batch_end_time - batch_start_time):.2f}s")
        except Exception as e:
            print(f"Error processing batch for {table_name}: {str(e)}")
            # If batch processing fails, try processing items one by one
            for item in batch:
                try:
                    supabase.table(table_name).upsert([item]).execute()
                except Exception as e:
                    print(f"Error processing item in {table_name}: {str(e)}")
                    continue
    
    end_time = time.time()
    print(f"Completed processing {table_name}:")
    print(f"  Total items: {total_items}")
    print(f"  Total time: {(end_time - start_time):.2f}s")

def delete_records(keys: List[List[str]], table_name: str, supabase: Client) -> None:
    """
    Delete records from a table based on their primary keys.
    
    Args:
        keys: List of primary key values to delete
        table_name: Name of the table to delete from
        supabase: Supabase client instance
    """
    if not keys:
        return
    
    # Get the GTFS file name for this table
    gtfs_file = next((f for f, t in GTFS_TO_TABLE.items() if t == table_name), None)
    if not gtfs_file:
        print(f"Error: No GTFS file mapping found for table {table_name}")
        return
    
    # Get primary keys for this table
    primary_keys = PRIMARY_KEYS.get(gtfs_file)
    if not primary_keys:
        print(f"Error: No primary keys defined for table {table_name}")
        return
    
    print(f"Processing deletion batch 1/1 for {table_name}...")
    
    # Process deletions in smaller batches to avoid URL length limits
    BATCH_SIZE = 100  # Smaller batch size for deletions
    total_batches = (len(keys) + BATCH_SIZE - 1) // BATCH_SIZE
    total_deleted = 0
    
    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min((batch_num + 1) * BATCH_SIZE, len(keys))
        batch_keys = keys[start_idx:end_idx]
        
        print(f"Processing deletion batch {batch_num + 1}/{total_batches} for {table_name}...")
        
        try:
            # For each key in the batch, delete individually
            for key in batch_keys:
                try:
                    query = supabase.table(table_name).delete()
                    
                    if table_name in COMPOSITE_KEY_TABLES:
                        # Handle composite keys
                        for i, pk_name in enumerate(primary_keys):
                            query = query.eq(pk_name, key[i])
                    else:
                        # Handle single primary key
                        db_column = PRIMARY_KEY_TO_DB_COLUMN.get(primary_keys[0])
                        if not db_column:
                            print(f"Error: No database column mapping for {primary_keys[0]}")
                            continue
                        query = query.eq(db_column, key[0])
                    
                    result = query.execute()
                    
                    if not (hasattr(result, 'error') and result.error):
                        total_deleted += 1
                        
                except Exception as e:
                    print(f"Error deleting record in {table_name}: {str(e)}")
                    continue
            
            print(f"Deleted {total_deleted} records from {table_name} in current batch")
                
        except Exception as e:
            print(f"Error processing deletion batch for {table_name}: {str(e)}")
            continue
    
    print(f"Total records deleted from {table_name}: {total_deleted}")

def parse_csv_from_zip(zip_file: zipfile.ZipFile, filename: str) -> List[Dict[str, Any]]:
    """Parse a CSV file from the GTFS zip file."""
    try:
        with zip_file.open(filename) as f:
            reader = csv.DictReader(f.read().decode('utf-8').splitlines())
            return list(reader)
    except KeyError:
        print(f"Warning: {filename} not found in GTFS zip")
        return []

def parse_agency(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse agency data."""
    return [{
        'id': row['agency_id'],
        'agency_name': row['agency_name'],
        'agency_url': row['agency_url'],
        'agency_timezone': row['agency_timezone'],
        'agency_lang': row.get('agency_lang'),
        'agency_phone': row.get('agency_phone'),
        'agency_fare_url': row.get('agency_fare_url')
    } for row in data]

def parse_stops(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse stops data."""
    return [{
        'id': row['stop_id'],
        'stop_code': row.get('stop_code'),
        'stop_name': row['stop_name'],
        'stop_desc': row.get('stop_desc'),
        'stop_lat': float(row['stop_lat']) if row['stop_lat'] else 0.0,
        'stop_lon': float(row['stop_lon']) if row['stop_lon'] else 0.0,
        'zone_id': int(row['zone_id']) if row.get('zone_id') else None,
        'stop_url': row.get('stop_url'),
        'location_type': int(row['location_type']) if row.get('location_type') else None,
        'parent_station_id': row.get('parent_station')
    } for row in data]

def parse_calendar(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse calendar data."""
    return [{
        'service_id': row['service_id'],
        'monday': row['monday'] == '1',
        'tuesday': row['tuesday'] == '1',
        'wednesday': row['wednesday'] == '1',
        'thursday': row['thursday'] == '1',
        'friday': row['friday'] == '1',
        'saturday': row['saturday'] == '1',
        'sunday': row['sunday'] == '1',
        'start_date': row['start_date'],
        'end_date': row['end_date']
    } for row in data]

def parse_routes(data: List[Dict[str, Any]], agencies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse routes data."""
    agency_map = {a['id']: a for a in agencies}
    valid_routes = []
    skipped_routes = 0
    
    for row in data:
        if row['agency_id'] not in agency_map:
            skipped_routes += 1
            continue
        valid_routes.append({
            'id': row['route_id'],
            'agency_id': row['agency_id'],
            'route_short_name': row['route_short_name'],
            'route_long_name': row['route_long_name'],
            'route_desc': row.get('route_desc'),
            'route_type': int(row['route_type']) if row['route_type'] else 0,
            'route_url': row.get('route_url'),
            'route_color': row.get('route_color'),
            'route_text_color': row.get('route_text_color')
        })
    
    if skipped_routes > 0:
        print(f"Skipped {skipped_routes} routes due to missing agency references")
    return valid_routes

def parse_trips(data: List[Dict[str, Any]], routes: List[Dict[str, Any]], service_ids: Set[str]) -> List[Dict[str, Any]]:
    """Parse trips data."""
    route_map = {r['id']: r for r in routes}
    valid_trips = []
    skipped_trips = 0
    
    for row in data:
        if row['route_id'] not in route_map:
            skipped_trips += 1
            continue
        if row['service_id'] not in service_ids:
            skipped_trips += 1
            continue
        valid_trips.append({
            'id': row['trip_id'],
            'route_id': row['route_id'],
            'service_id': row['service_id'],
            'trip_headsign': row.get('trip_headsign'),
            'trip_short_name': row.get('trip_short_name'),
            'direction_id': int(row['direction_id']) if row.get('direction_id') else None,
            'block_id': int(row['block_id']) if row.get('block_id') else None,
            'shape_id': row.get('shape_id'),
            'wheelchair_accessible': int(row['wheelchair_accessible']) if row.get('wheelchair_accessible') else None,
            'bikes_allowed': int(row['bikes_allowed']) if row.get('bikes_allowed') else None
        })
    
    if skipped_trips > 0:
        print(f"Skipped {skipped_trips} trips due to missing route or service references")
    return valid_trips

def parse_stop_times(data: List[Dict[str, Any]], trips: List[Dict[str, Any]], stops: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse stop times data."""
    trip_map = {t['id']: t for t in trips}
    stop_map = {s['id']: s for s in stops}
    valid_stop_times = []
    skipped_stop_times = 0
    
    for row in data:
        if row['trip_id'] not in trip_map:
            skipped_stop_times += 1
            continue
        if row['stop_id'] not in stop_map:
            skipped_stop_times += 1
            continue
        
        valid_stop_times.append({
            'trip_id': row['trip_id'],
            'arrival_time': row['arrival_time'],
            'departure_time': row['departure_time'],
            'stop_id': row['stop_id'],
            'stop_sequence': int(row['stop_sequence']) if row['stop_sequence'] else 0,
            'stop_headsign': row.get('stop_headsign'),
            'pickup_type': int(row['pickup_type']) if row.get('pickup_type') else None,
            'drop_off_type': int(row['drop_off_type']) if row.get('drop_off_type') else None,
            'shape_dist_traveled': float(row['shape_dist_traveled']) if row.get('shape_dist_traveled') else None,
            'timepoint': int(row['timepoint']) if row.get('timepoint') else None
        })
    
    if skipped_stop_times > 0:
        print(f"Skipped {skipped_stop_times} stop times due to missing trip or stop references")
    return valid_stop_times

def parse_calendar_dates(data: List[Dict[str, Any]], calendars: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse calendar dates data."""
    calendar_map = {c['service_id']: c for c in calendars}
    valid_calendar_dates = []
    skipped_calendar_dates = 0
    
    for row in data:
        if row['service_id'] not in calendar_map:
            skipped_calendar_dates += 1
            continue
        
        valid_calendar_dates.append({
            'service_id': row['service_id'],
            'date': row['date'],
            'exception_type': int(row['exception_type']) if row['exception_type'] else 0
        })
    
    if skipped_calendar_dates > 0:
        print(f"Skipped {skipped_calendar_dates} calendar dates due to missing calendar references")
    return valid_calendar_dates

def parse_transfers(data: List[Dict[str, Any]], stops: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse transfers data."""
    stop_map = {s['id']: s for s in stops}
    valid_transfers = []
    skipped_transfers = 0
    
    for row in data:
        if row['from_stop_id'] not in stop_map:
            skipped_transfers += 1
            continue
        if row['to_stop_id'] not in stop_map:
            skipped_transfers += 1
            continue
        
        valid_transfers.append({
            'from_stop_id': row['from_stop_id'],
            'to_stop_id': row['to_stop_id'],
            'transfer_type': int(row['transfer_type']) if row.get('transfer_type') else 0,
            'min_transfer_time': int(row['min_transfer_time']) if row.get('min_transfer_time') else None
        })
    
    if skipped_transfers > 0:
        print(f"Skipped {skipped_transfers} transfers due to missing stop references")
    return valid_transfers

def parse_shapes(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse shapes data."""
    return [{
        'shape_id': row['shape_id'],
        'shape_pt_lat': float(row['shape_pt_lat']),
        'shape_pt_lon': float(row['shape_pt_lon']),
        'shape_pt_sequence': int(row['shape_pt_sequence']),
        'shape_dist_traveled': float(row['shape_dist_traveled']) if row.get('shape_dist_traveled') else None
    } for row in data] 