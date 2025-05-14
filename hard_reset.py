"""
GTFS Hard Reset

This script performs a complete reset of the GTFS data:
1. Downloads the latest GTFS data from the network
2. Processes all GTFS files in the correct order to maintain referential integrity
3. Updates the cache with the new data
4. Processes realtime updates

Usage:
    python hard_reset.py

The script will:
1. Download the latest GTFS data
2. Process all GTFS files
3. Update the cache with the new data
4. Start processing realtime updates

Dependencies:
- requests: For downloading GTFS data
- supabase: For database operations
- gtfs_utils: For shared GTFS processing functions
"""

import csv
import io
import sys
import time
import requests
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, List, Optional
import shutil

from gtfs_utils import (
    create_supabase_client,
    process_batch,
    GTFS_TO_TABLE,
    parse_agency,
    parse_stops,
    parse_routes,
    parse_calendar,
    parse_calendar_dates,
    parse_trips,
    parse_stop_times,
    parse_shapes
)
from save_gtfs_cache import save_cache
from gtfs_realtime_parser import parse_gtfs_realtime

def download_gtfs_zip(url: str, output_path: str) -> None:
    """
    Download a GTFS zip file from a URL.
    
    Args:
        url: The URL to download from
        output_path: Where to save the downloaded file
        
    Raises:
        requests.exceptions.RequestException: If download fails
    """
    print(f"Downloading GTFS data from {url}...")
    start_time = time.time()
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        block_size = 8192  # 8 KB chunks
        downloaded = 0
        
        with open(output_path, 'wb') as f:
            for data in response.iter_content(block_size):
                downloaded += len(data)
                f.write(data)
                
                if total_size > 0:
                    percent = (downloaded / total_size) * 100
                    print(f"\rDownload progress: {percent:.1f}%", end='')
        
        print(f"\nDownload completed in {(time.time() - start_time):.2f}s")
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading GTFS data: {str(e)}")
        raise

def parse_csv_from_zip(zip_file: zipfile.ZipFile, filename: str) -> List[Dict]:
    """
    Parse a CSV file from a GTFS zip file.
    
    Args:
        zip_file: The GTFS zip file to read from
        filename: The name of the CSV file within the zip
        
    Returns:
        List of dictionaries containing the CSV data
    """
    try:
        with zip_file.open(filename) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'))
            return list(reader)
    except KeyError:
        return []

def process_gtfs_zip(zip_path: str) -> Dict[str, List[Dict]]:
    """
    Process a GTFS zip file and upload its contents to Supabase.
    
    Args:
        zip_path: Path to the GTFS zip file
        
    Returns:
        Dictionary containing all processed data for caching
    """
    supabase = create_supabase_client()
    processed_data = {}
    
    with zipfile.ZipFile(zip_path) as zip_file:
        # Process independent entities first
        agency_data = parse_csv_from_zip(zip_file, 'agency.txt')
        if agency_data:
            agencies = parse_agency(agency_data)
            process_batch(supabase, GTFS_TO_TABLE['agency.txt'], agencies)
            processed_data['agencies'] = agencies
        
        stop_data = parse_csv_from_zip(zip_file, 'stops.txt')
        if stop_data:
            stops = parse_stops(stop_data)
            process_batch(supabase, GTFS_TO_TABLE['stops.txt'], stops)
            processed_data['stops'] = stops
        
        # Process routes (depends on agencies)
        route_data = parse_csv_from_zip(zip_file, 'routes.txt')
        if route_data:
            routes = parse_routes(route_data, agencies if agency_data else [])
            process_batch(supabase, GTFS_TO_TABLE['routes.txt'], routes)
            processed_data['routes'] = routes
        
        # Process dependent entities
        calendar_data = parse_csv_from_zip(zip_file, 'calendar.txt')
        if calendar_data:
            calendars = parse_calendar(calendar_data)
            process_batch(supabase, GTFS_TO_TABLE['calendar.txt'], calendars)
            processed_data['calendars'] = calendars
        
        calendar_dates_data = parse_csv_from_zip(zip_file, 'calendar_dates.txt')
        if calendar_dates_data:
            calendar_dates = parse_calendar_dates(calendar_dates_data, calendars if calendar_data else [])
            process_batch(supabase, GTFS_TO_TABLE['calendar_dates.txt'], calendar_dates)
            processed_data['calendar_dates'] = calendar_dates
        
        trip_data = parse_csv_from_zip(zip_file, 'trips.txt')
        if trip_data:
            service_ids = {c['service_id'] for c in calendars} if calendar_data else set()
            trips = parse_trips(trip_data, routes if route_data else [], service_ids)
            process_batch(supabase, GTFS_TO_TABLE['trips.txt'], trips)
            processed_data['trips'] = trips
        
        stop_times_data = parse_csv_from_zip(zip_file, 'stop_times.txt')
        if stop_times_data:
            stop_times = parse_stop_times(stop_times_data, trips if trip_data else [], stops if stop_data else [])
            process_batch(supabase, GTFS_TO_TABLE['stop_times.txt'], stop_times)
            processed_data['stop_times'] = stop_times
        
        shape_data = parse_csv_from_zip(zip_file, 'shapes.txt')
        if shape_data:
            shapes = parse_shapes(shape_data)
            process_batch(supabase, GTFS_TO_TABLE['shapes.txt'], shapes)
            processed_data['shapes'] = shapes
    
    return processed_data

def ensure_cache_dir() -> Path:
    """
    Ensure the project's cache directory exists.
    
    Returns:
        Path: Path to the cache directory
    """
    script_dir = Path(__file__).parent
    cache_dir = script_dir / "cache"
    cache_dir.mkdir(exist_ok=True)
    return cache_dir

def main():
    # GTFS data URL
    gtfs_url = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip"
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        gtfs_zip_path = temp_dir_path / "gtfs-data-latest.zip"
        
        try:
            # Download latest GTFS data
            download_gtfs_zip(gtfs_url, str(gtfs_zip_path))
            
            # Save downloaded GTFS data to cache
            print("\nSaving GTFS data to cache...")
            cache_dir = ensure_cache_dir()
            cache_zip_path = cache_dir / "gtfs-data-latest.zip"
            
            # Copy the downloaded file to cache
            shutil.copy2(gtfs_zip_path, cache_zip_path)
            print(f"GTFS data saved to cache: {cache_zip_path}")
            
            # Step 1: Flash GTFS data to database
            print("\nStep 1: Flashing GTFS data to database...")
            try:
                processed_data = process_gtfs_zip(str(gtfs_zip_path))
            except Exception as e:
                print(f"Failed to flash GTFS data: {str(e)}")
                sys.exit(1)
            
            # Step 2: Save cache
            print("\nStep 2: Saving cache...")
            try:
                save_cache(processed_data)
                print("Cache saved successfully")
            except Exception as e:
                print(f"Failed to save cache: {str(e)}")
                sys.exit(1)
            
            # Step 3: Process realtime updates
            print("\nStep 3: Processing realtime updates...")
            try:
                parse_gtfs_realtime()
            except Exception as e:
                print(f"Failed to process realtime updates: {str(e)}")
                sys.exit(1)
            
            print("\n=== GTFS Update Process Completed Successfully ===")
            
        except Exception as e:
            print(f"Error during GTFS update: {str(e)}")
            sys.exit(1)

if __name__ == "__main__":
    main() 