"""
GTFS Cache Saver

This script loads GTFS data from the database and saves it to cache files.
Each table's data is saved in a separate JSON file with a timestamp.

Usage:
    python save_gtfs_cache.py

The script will:
1. Load data from each GTFS table in the database
2. Save the data to JSON files in the cache directory
3. Include a timestamp with each cache file
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Any

from gtfs_utils import create_supabase_client, GTFS_TO_TABLE


def load_table_data(supabase, table_name: str, select_fields: str = '*') -> List[Dict[str, Any]]:
    """Load all records from a table in batches."""
    records = []
    # Get total count first
    count_response = supabase.table(table_name).select(select_fields, count='exact').execute()
    total_count = count_response.count
    
    # Fetch all records in batches
    batch_size = 1000
    for start in range(0, total_count, batch_size):
        end = min(start + batch_size, total_count) - 1  # -1 because range is inclusive
        response = supabase.table(table_name).select(select_fields).range(start, end).execute()
        if response.data:
            records.extend(response.data)
            print(f"Loaded {table_name} {start} to {end} of {total_count} (batch size: {len(response.data)})")
        else:
            print(f"Warning: No data received for {table_name} batch {start} to {end}")
    
    # Verify we got all records
    if len(records) != total_count:
        print(f"Warning: Expected {total_count} records for {table_name}, but got {len(records)}")
    
    return records

def save_cache(data: Dict[str, List[Dict[str, Any]]], cache_dir: str = "cache"):
    """Save data to cache files."""
    # Create cache directory if it doesn't exist
    os.makedirs(cache_dir, exist_ok=True)
    
    # Save each table's data
    for table_name, records in data.items():
        with open(os.path.join(cache_dir, f"{table_name}.json"), "w") as f:
            json.dump({
                "timestamp": datetime.utcnow().isoformat(),
                "data": records
            }, f)

def main():
    # Initialize Supabase client
    supabase = create_supabase_client()
    
    # Load data for each table
    cached_data = {}
    for table_name in GTFS_TO_TABLE.values():
        print(f"\nLoading {table_name}...")
        records = load_table_data(supabase, table_name)
        cached_data[table_name] = records
        print(f"Loaded {len(records)} {table_name} records")
    
    # Save to cache
    print("\nSaving to cache...")
    save_cache(cached_data)
    print("Cache saved successfully")

if __name__ == "__main__":
    main()