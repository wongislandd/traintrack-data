"""
GTFS Cache Loader

This script loads GTFS data from cache files.
It can load all tables or specific tables as requested.

Usage:
    python load_gtfs_cache.py [table1 table2 ...]

If no tables are specified, it will load all available cache files.
Each cache file should be a JSON file containing a timestamp and data array.

Example:
    python load_gtfs_cache.py agencies stops routes
"""

import json
import os
from typing import Dict, List, Any, Optional

def load_cache(cache_dir: str = "cache", tables: Optional[List[str]] = None) -> Dict[str, List[Dict[str, Any]]]:
    """
    Load GTFS data from cache files.
    
    Args:
        cache_dir: Directory containing cache files
        tables: Optional list of table names to load. If None, loads all available tables.
    
    Returns:
        Dictionary mapping table names to their cached data
    """
    cached_data = {}
    
    # If no specific tables requested, try to load all available cache files
    if tables is None:
        tables = [f.replace('.json', '') for f in os.listdir(cache_dir) if f.endswith('.json')]
    
    for table_name in tables:
        cache_file = os.path.join(cache_dir, f"{table_name}.json")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r') as f:
                    cache_data = json.load(f)
                    cached_data[table_name] = cache_data['data']
                print(f"Loaded {len(cached_data[table_name])} records from {table_name} cache")
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error loading cache for {table_name}: {e}")
        else:
            print(f"No cache file found for {table_name}")
    
    return cached_data

def main():
    import sys
    
    # Get tables to load from command line arguments
    tables = sys.argv[1:] if len(sys.argv) > 1 else None
    
    # Load from cache
    print("Loading from cache...")
    loaded_data = load_cache(tables=tables)
    print(f"Loaded {len(loaded_data)} tables from cache")
    
    # Print summary of loaded data
    print("\nSummary of loaded data:")
    for table_name, records in loaded_data.items():
        print(f"{table_name}: {len(records)} records")

if __name__ == "__main__":
    main() 