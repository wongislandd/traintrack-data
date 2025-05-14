"""
GTFS Zip Diff Creator

This script creates a diff between two GTFS (General Transit Feed Specification) zip files.
It identifies changes, additions, and deletions in the GTFS data and creates a diff zip file
containing only the modified records.

The diff zip contains:
- For each modified file:
  - {filename}.changes.csv: Records that were modified or added
  - {filename}.deletions.csv: Primary keys of records that were deleted
- summary.json: Overview of all changes found

Primary Keys:
- stops.txt: stop_id
- trips.txt: trip_id
- routes.txt: route_id
- stop_times.txt: trip_id, stop_sequence
- calendar.txt: service_id
- calendar_dates.txt: service_id, date
- shapes.txt: shape_id, shape_pt_sequence
- transfers.txt: from_stop_id, to_stop_id

Usage:
    python create_gtfs_diff.py previous.zip current.zip [output.zip]

The script will:
1. Load and parse both GTFS zip files
2. Compare records using primary keys and content hashes
3. Create a diff zip containing only the changes
4. Generate a summary of all changes found

Dependencies:
- csv: For parsing GTFS CSV files
- hashlib: For generating content hashes
- zipfile: For handling zip files
"""

import csv
import hashlib
import io
import json
import sys
import time
import zipfile
from typing import Dict, List, Tuple
from pathlib import Path

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

def hash_row(row: List[str]) -> str:
    """Generate an MD5 hash of a row's contents."""
    return hashlib.md5(','.join(row).encode('utf-8')).hexdigest()

def load_csv_from_zip(zip_file: zipfile.ZipFile, filename: str) -> Tuple[List[str], Dict[Tuple, Tuple[List[str], str]]]:
    """
    Load and parse a CSV file from a GTFS zip file.
    
    Args:
        zip_file: The GTFS zip file to read from
        filename: The name of the CSV file within the zip
        
    Returns:
        Tuple containing:
        - List of header names
        - Dictionary mapping primary key tuples to (row data, hash) tuples
    """
    with zip_file.open(filename) as f:
        # Read the file content and decode it
        content = f.read().decode('utf-8')
        # Split into lines and create a CSV reader
        reader = csv.reader(content.splitlines())
        # Get the header
        header = next(reader)
        
        # Get the indices of primary key columns in the header
        pk_indices = [header.index(k) for k in PRIMARY_KEYS.get(filename, [])]
        
        # Create dictionary mapping primary key tuples to (row data, hash) tuples
        data = {}
        for row in reader:
            if not row:  # Skip empty rows
                continue
                
            # Extract primary key values using the correct indices
            pk_values = tuple(str(row[i]) for i in pk_indices)
            data[pk_values] = (row, hash_row(row))
    
    return header, data

def create_diff_zip(old_zip_path: str, new_zip_path: str, output_path: str) -> bool:
    """
    Create a diff zip file containing only the changes between old and new GTFS data.
    
    Args:
        old_zip_path: Path to the old GTFS zip file
        new_zip_path: Path to the new GTFS zip file
        output_path: Where to save the diff zip file
        
    Returns:
        bool: True if there are changes, False if no changes found
        
    The diff zip will contain:
    - For each modified file:
      - {filename}.changes.csv: Records that were modified or added
      - {filename}.deletions.csv: Primary keys of records that were deleted
    - summary.json: Overview of all changes found
    """
    print(f"Creating diff zip from:")
    print(f"  Old: {old_zip_path}")
    print(f"  New: {new_zip_path}")
    start_time = time.time()
    
    # Load data from both zip files
    old_data = {}
    new_data = {}
    
    with zipfile.ZipFile(old_zip_path) as old_zip:
        for filename in old_zip.namelist():
            if filename.endswith('.txt'):
                old_data[filename] = load_csv_from_zip(old_zip, filename)
    
    with zipfile.ZipFile(new_zip_path) as new_zip:
        for filename in new_zip.namelist():
            if filename.endswith('.txt'):
                new_data[filename] = load_csv_from_zip(new_zip, filename)
    
    # Track if any changes were found
    has_changes = False
    summary = {}
    
    # Create diff zip
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as diff_zip:
        # Process each file type
        for filename in set(old_data.keys()) | set(new_data.keys()):
            if filename not in old_data:
                # New file
                header, data = new_data[filename]
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerow(header)
                writer.writerows([row for row, _ in data.values()])
                diff_zip.writestr(f"{filename}.changes.csv", output.getvalue())
                has_changes = True
                summary[filename] = {
                    "status": "new_file",
                    "records": len(data)
                }
                continue
                
            if filename not in new_data:
                # Deleted file
                has_changes = True
                summary[filename] = {
                    "status": "deleted_file",
                    "records": len(old_data[filename][1])
                }
                continue
                
            # Compare files
            old_header, old_data_dict = old_data[filename]
            new_header, new_data_dict = new_data[filename]
            
            if not old_data_dict or not new_data_dict:
                continue
                
            # Get primary key field
            primary_keys = PRIMARY_KEYS.get(filename)
            if not primary_keys:
                continue
                
            # Find changed and deleted records
            changed_rows = []
            deleted_keys = []
            
            # Process new and modified records
            for pk_tuple, (new_row, new_hash) in new_data_dict.items():
                if pk_tuple not in old_data_dict:
                    # New record
                    changed_rows.append(new_row)
                else:
                    old_row, old_hash = old_data_dict[pk_tuple]
                    if old_hash != new_hash:
                        # Changed record
                        changed_rows.append(new_row)
            
            # Process deleted records
            for pk_tuple in old_data_dict:
                if pk_tuple not in new_data_dict:
                    # Deleted record - use the actual values from the old data
                    old_row, _ = old_data_dict[pk_tuple]
                    pk_indices = [old_header.index(k) for k in primary_keys]
                    deleted_keys.append([old_row[i] for i in pk_indices])
            
            # Update summary
            if changed_rows or deleted_keys:
                has_changes = True
                summary[filename] = {
                    "status": "modified",
                    "changed_records": len(changed_rows),
                    "deleted_records": len(deleted_keys)
                }
            
            # Write changed records
            if changed_rows:
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerow(new_header)
                writer.writerows(changed_rows)
                diff_zip.writestr(f"{filename}.changes.csv", output.getvalue())
            
            # Write deleted keys
            if deleted_keys:
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerow(primary_keys)
                writer.writerows(deleted_keys)
                diff_zip.writestr(f"{filename}.deletions.csv", output.getvalue())
        
        # Write summary file
        summary_json = json.dumps({
            "has_changes": has_changes,
            "files": summary
        }, indent=2)
        diff_zip.writestr("summary.json", summary_json)
    
    end_time = time.time()
    print(f"\nDiff zip created in {(end_time - start_time):.2f}s")
    print(f"Temp directory: {Path(output_path).parent}")
    print(f"Diff file: {Path(output_path).name}")
    print(f"Changes found: {has_changes}")
    
    return has_changes

if __name__ == "__main__":
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Usage: python create_gtfs_diff.py previous.zip current.zip [output.zip]")
        sys.exit(1)

    old_path, new_path = sys.argv[1], sys.argv[2]
    output_path = sys.argv[3] if len(sys.argv) == 4 else "delta.zip"
    
    try:
        has_changes = create_diff_zip(old_path, new_path, output_path)
        if not has_changes:
            print("No changes found in GTFS data.")
            sys.exit(0)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)