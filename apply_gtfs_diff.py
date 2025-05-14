"""
GTFS Diff Applicator

This script applies changes from a GTFS diff zip file to the database.
It handles both updates to existing records and deletions.

The script processes files in the following order:
1. Independent entities (agencies, stops)
2. Routes (depends on agencies)
3. Dependent entities (trips, stop times, calendar, etc.)

For each file type:
1. Reads the .changes.csv file to get modified/added records
2. Reads the .deletions.csv file to get records to delete
3. Applies changes in batches to the database

Usage:
    python apply_gtfs_diff.py diff.zip

The script will:
1. Load the diff zip file
2. Process each file type in the correct order
3. Apply changes in batches
4. Report progress and any errors

Dependencies:
- supabase: For database operations
- gtfs_utils: For shared GTFS processing functions
"""

import csv
import io
import json
import zipfile
from pathlib import Path
from typing import Dict, List, Any

from supabase import Client

from gtfs_utils import (
    create_supabase_client,
    process_batch,
    delete_records,
    parse_agency,
    parse_stops,
    parse_calendar,
    parse_routes,
    parse_trips,
    parse_stop_times,
    parse_calendar_dates,
    parse_transfers,
    parse_shapes,
    GTFS_TO_TABLE
)
from load_gtfs_cache import load_cache


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


def process_diff_zip(diff_zip_path: str) -> None:
    """
    Process a GTFS diff zip file and apply changes to the database.
    
    Args:
        diff_zip_path: Path to the diff zip file
        
    The function:
    1. Loads the diff zip file
    2. Reads the summary to understand what changes exist
    3. Processes each file type in the correct order
    4. Applies changes in batches
    5. Reports progress and any errors
    """
    print(f"Processing diff zip: {diff_zip_path}")
    
    # Create Supabase client
    supabase = create_supabase_client()
    
    # Load summary
    with zipfile.ZipFile(diff_zip_path) as diff_zip:
        summary = json.loads(diff_zip.read("summary.json").decode('utf-8'))
    
    if not summary.get("has_changes", False):
        print("No changes found in diff zip")
        return
    
    # Get cache directory
    cache_dir = get_cache_dir()
    
    # Load cache data for dependencies
    print("Loading cache data for dependencies...")
    cache_data = load_cache(cache_dir)
    
    # Process files in order of dependencies
    process_independent_entities(diff_zip_path, supabase, cache_data)
    process_routes(diff_zip_path, supabase, cache_data)
    process_dependent_entities(diff_zip_path, supabase, cache_data)
    
    print("Diff processing completed successfully")

def process_independent_entities(diff_zip_path: str, supabase: Client, cache_data: Dict[str, List[Dict[str, Any]]]) -> None:
    """
    Process independent entities (agencies, stops) from the diff zip.
    
    Args:
        diff_zip_path: Path to the diff zip file
        supabase: Supabase client instance
        cache_data: Dictionary of cached data
    """
    print("\nProcessing independent entities...")
    
    # Process agencies
    if "agency.txt" in GTFS_TO_TABLE:
        process_file_changes(
            diff_zip_path,
            "agency.txt",
            parse_agency,
            supabase,
            cache_data
        )
    
    # Process stops
    if "stops.txt" in GTFS_TO_TABLE:
        process_file_changes(
            diff_zip_path,
            "stops.txt",
            parse_stops,
            supabase,
            cache_data
        )

def process_routes(diff_zip_path: str, supabase: Client, cache_data: Dict[str, List[Dict[str, Any]]]) -> None:
    """
    Process routes from the diff zip.
    
    Args:
        diff_zip_path: Path to the diff zip file
        supabase: Supabase client instance
        cache_data: Dictionary of cached data
    """
    print("\nProcessing routes...")
    
    if "routes.txt" in GTFS_TO_TABLE:
        process_file_changes(
            diff_zip_path,
            "routes.txt",
            parse_routes,
            supabase,
            cache_data
        )

def process_dependent_entities(diff_zip_path: str, supabase: Client, cache_data: Dict[str, List[Dict[str, Any]]]) -> None:
    """
    Process dependent entities (trips, stop times, etc.) from the diff zip.
    
    Args:
        diff_zip_path: Path to the diff zip file
        supabase: Supabase client instance
        cache_data: Dictionary of cached data
    """
    print("\nProcessing dependent entities...")
    
    # Process trips
    if "trips.txt" in GTFS_TO_TABLE:
        process_file_changes(
            diff_zip_path,
            "trips.txt",
            parse_trips,
            supabase,
            cache_data
        )
    
    # Process stop times
    if "stop_times.txt" in GTFS_TO_TABLE:
        process_file_changes(
            diff_zip_path,
            "stop_times.txt",
            parse_stop_times,
            supabase,
            cache_data
        )
    
    # Process calendar
    if "calendar.txt" in GTFS_TO_TABLE:
        process_file_changes(
            diff_zip_path,
            "calendar.txt",
            parse_calendar,
            supabase,
            cache_data
        )
    
    # Process calendar dates
    if "calendar_dates.txt" in GTFS_TO_TABLE:
        process_file_changes(
            diff_zip_path,
            "calendar_dates.txt",
            parse_calendar_dates,
            supabase,
            cache_data
        )
    
    # Process transfers
    if "transfers.txt" in GTFS_TO_TABLE:
        process_file_changes(
            diff_zip_path,
            "transfers.txt",
            parse_transfers,
            supabase,
            cache_data
        )
    
    # Process shapes
    if "shapes.txt" in GTFS_TO_TABLE:
        process_file_changes(
            diff_zip_path,
            "shapes.txt",
            parse_shapes,
            supabase,
            cache_data
        )

def process_file_changes(
    diff_zip_path: str,
    filename: str,
    parse_func,
    supabase: Client,
    cache_data: Dict[str, List[Dict[str, Any]]]
) -> None:
    """
    Process changes for a specific file type.
    
    Args:
        diff_zip_path: Path to the diff zip file
        filename: Name of the file to process
        parse_func: Function to parse records from the file
        supabase: Supabase client instance
        cache_data: Dictionary of cached data
    """
    print(f"\nProcessing {filename}...")
    
    with zipfile.ZipFile(diff_zip_path) as diff_zip:
        # Process changed records
        changes_filename = f"{filename}.changes.csv"
        if changes_filename in diff_zip.namelist():
            print(f"Processing changes in {changes_filename}")
            with diff_zip.open(changes_filename) as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding='utf-8'))
                header = next(reader)
                rows = list(reader)
                
                if rows:
                    # Convert rows to dictionaries
                    data = [dict(zip(header, row)) for row in rows]
                    records = parse_func(data, **get_cache_params(parse_func, cache_data))
                    if records:
                        process_batch(supabase, GTFS_TO_TABLE[filename], records)
        
        # Process deleted records
        deletions_filename = f"{filename}.deletions.csv"
        if deletions_filename in diff_zip.namelist():
            print(f"Processing deletions in {deletions_filename}")
            with diff_zip.open(deletions_filename) as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding='utf-8'))
                header = next(reader)  # Skip header
                keys = list(reader)
                if keys:
                    delete_records(keys, GTFS_TO_TABLE[filename], supabase)

def get_cache_params(parse_func, cache_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    """
    Get the appropriate cache parameters for a parse function.
    
    Args:
        parse_func: The parse function to get parameters for
        cache_data: Dictionary of cached data
        
    Returns:
        Dictionary of parameters to pass to the parse function
    """
    params = {}
    
    if parse_func == parse_routes:
        params["agencies"] = cache_data.get("agencies", [])
    elif parse_func == parse_trips:
        params["routes"] = cache_data.get("routes", [])
        params["service_ids"] = {c["service_id"] for c in cache_data.get("calendars", [])}
    elif parse_func == parse_stop_times:
        params["trips"] = cache_data.get("trips", [])
        params["stops"] = cache_data.get("stops", [])
    elif parse_func == parse_calendar_dates:
        params["calendars"] = cache_data.get("calendars", [])
    elif parse_func == parse_transfers:
        params["stops"] = cache_data.get("stops", [])
    
    return params

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python apply_gtfs_diff.py diff.zip")
        sys.exit(1)
    
    diff_zip_path = sys.argv[1]
    
    try:
        process_diff_zip(diff_zip_path)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1) 