import csv
import hashlib
import zipfile
import io
import sys
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

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
        reader = csv.reader(io.TextIOWrapper(f, encoding='utf-8'))
        header = next(reader)
        rows = list(reader)
    
    pk_indexes = [header.index(k) for k in PRIMARY_KEYS.get(filename, [])]
    data = {
        tuple(row[i] for i in pk_indexes): (row, hash_row(row))
        for row in rows
    }
    return header, data

def diff_file(name: str, zip_old: zipfile.ZipFile, zip_new: zipfile.ZipFile) -> Tuple[Optional[str], Optional[str], Dict]:
    """
    Generate diff for a single GTFS file.
    
    Args:
        name: Name of the GTFS file
        zip_old: Old GTFS zip file
        zip_new: New GTFS zip file
        
    Returns:
        Tuple containing:
        - Changed records as CSV string
        - Deleted records as CSV string
        - Summary statistics dictionary
    """
    if name not in PRIMARY_KEYS:
        return None, None, {}

    try:
        old_header, old_data = load_csv_from_zip(zip_old, name)
    except KeyError:
        old_data = {}

    try:
        new_header, new_data = load_csv_from_zip(zip_new, name)
    except KeyError:
        new_data = {}

    if old_data and new_data and old_header != new_header:
        raise Exception(f"CSV headers for {name} do not match")

    # Find changed and deleted records
    changed_rows = []
    for key, (row, hash_val) in new_data.items():
        if key not in old_data or old_data[key][1] != hash_val:
            changed_rows.append(row)

    deleted_keys = [
        list(key) for key in old_data
        if key not in new_data
    ]

    # Generate changed records CSV
    changed_csv = None
    if changed_rows:
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(new_header)
        writer.writerows(changed_rows)
        changed_csv = output.getvalue()

    # Generate deleted records CSV
    deleted_csv = None
    if deleted_keys:
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(PRIMARY_KEYS[name])
        writer.writerows(deleted_keys)
        deleted_csv = output.getvalue()

    # Generate summary statistics
    summary = {
        "total_old": len(old_data),
        "total_new": len(new_data),
        "changed": len(changed_rows),
        "deleted": len(deleted_keys),
        "added": len(new_data) - (len(old_data) - len(deleted_keys))
    }

    return changed_csv, deleted_csv, summary

def validate_gtfs_zip(zip_path: str) -> bool:
    """
    Validate that a zip file contains valid GTFS data.
    
    Args:
        zip_path: Path to the GTFS zip file
        
    Returns:
        True if valid, False otherwise
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            # Check for at least one GTFS file
            return any(f.endswith('.txt') for f in z.namelist())
    except Exception as e:
        print(f"Error reading zip file {zip_path}: {str(e)}")
        return False

def create_diff_zip(old_zip_path: str, new_zip_path: str) -> Path:
    """
    Create a diff zip file between two GTFS zip files.
    
    Args:
        old_zip_path: Path to the old GTFS zip file
        new_zip_path: Path to the new GTFS zip file
        
    Returns:
        Path to the created diff zip file
    """
    # Validate input files
    if not validate_gtfs_zip(old_zip_path):
        raise ValueError(f"Invalid GTFS zip file: {old_zip_path}")
    if not validate_gtfs_zip(new_zip_path):
        raise ValueError(f"Invalid GTFS zip file: {new_zip_path}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_zip_path = Path(f"delta_{timestamp}.zip")
    summary = {}

    with zipfile.ZipFile(old_zip_path, 'r') as zip_old, \
         zipfile.ZipFile(new_zip_path, 'r') as zip_new, \
         zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_out:

        # Get list of files from both zips
        old_files = {f for f in zip_old.namelist() if f.endswith('.txt')}
        new_files = {f for f in zip_new.namelist() if f.endswith('.txt')}
        all_files = old_files | new_files
        
        for name in sorted(all_files):
            if name not in PRIMARY_KEYS:
                continue
            changed_csv, deleted_csv, file_summary = diff_file(name, zip_old, zip_new)
            summary[name] = file_summary

            if changed_csv:
                zip_out.writestr(name, changed_csv)

            if deleted_csv:
                deleted_name = name.replace(".txt", "_deleted.txt")
                zip_out.writestr(deleted_name, deleted_csv)

        # Add summary file
        summary_json = json.dumps(summary, indent=2)
        zip_out.writestr("summary.json", summary_json)

    print(f"\nWrote diff output to {output_zip_path}")
    print("\nSummary of changes:")
    for file_name, stats in summary.items():
        print(f"\n{file_name}:")
        print(f"  Total records (old): {stats['total_old']}")
        print(f"  Total records (new): {stats['total_new']}")
        print(f"  Changed records: {stats['changed']}")
        print(f"  Deleted records: {stats['deleted']}")
        print(f"  Added records: {stats['added']}")

    return output_zip_path

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python gtfs_zip_diff.py previous.zip current.zip")
        sys.exit(1)

    old_path, new_path = sys.argv[1], sys.argv[2]
    try:
        create_diff_zip(old_path, new_path)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)