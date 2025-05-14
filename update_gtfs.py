"""
GTFS Incremental Update

This script performs an incremental update of GTFS data:
1. Downloads the latest GTFS data from the network
2. Compares it with the cached version
3. Applies any changes found

Usage:
    python update_gtfs.py

The script will:
1. Download the latest GTFS data
2. Compare it with the cached version
3. Apply any changes found
4. Update the cache with the new data
5. Start processing realtime updates

Dependencies:
- requests: For downloading GTFS data
- gtfs_zip_diff: For creating diffs
- apply_gtfs_diff: For applying diffs
"""

import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Optional

import requests
from create_gtfs_diff import create_diff_zip


def ensure_temp_dir() -> Path:
    """
    Ensure the project's temp directory exists.
    
    Returns:
        Path: Path to the temp directory
    """
    script_dir = Path(__file__).parent
    temp_dir = script_dir / "temp"
    temp_dir.mkdir(exist_ok=True)
    return temp_dir


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
        print(f"Total file size: {total_size} bytes")
        
        block_size = 8192  # 8 KB chunks
        downloaded = 0

        with open(output_path, 'wb') as f:
            for data in response.iter_content(block_size):
                downloaded += len(data)
                f.write(data)

                if total_size > 0:
                    percent = (downloaded / total_size) * 100
                    print(f"\rDownload progress: {percent:.1f}% ({downloaded}/{total_size} bytes)", end='')

        print(f"\nDownload completed in {(time.time() - start_time):.2f}s")
        print(f"File saved to: {output_path}")
        print(f"File size: {Path(output_path).stat().st_size} bytes")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading GTFS data: {str(e)}")
        raise


def get_cached_gtfs_path() -> Optional[str]:
    """
    Get the path to the cached GTFS zip file if it exists.

    Returns:
        Path to the cached GTFS zip file, or None if it doesn't exist
    """
    script_dir = Path(__file__).parent
    cache_dir = script_dir / "cache"
    cached_gtfs_path = cache_dir / "gtfs-data-latest.zip"

    if cached_gtfs_path.exists():
        return str(cached_gtfs_path)

    return None


def run_script(script_name: str, args: Optional[list] = None) -> bool:
    """
    Run a Python script and return whether it was successful.

    Args:
        script_name: Name of the script to run
        args: Optional list of arguments to pass to the script

    Returns:
        bool: True if the script ran successfully, False otherwise
    """
    script_path = Path(__file__).parent / script_name
    cmd = [sys.executable, str(script_path)]
    if args:
        cmd.extend(args)

    print(f"\n=== Running {script_name} ===")
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}:")
        print(e.stderr)
        return False


def is_valid_zip(file_path: str) -> bool:
    """
    Check if a file is a valid ZIP file.
    
    Args:
        file_path: Path to the file to check
        
    Returns:
        bool: True if the file is a valid ZIP file, False otherwise
    """
    try:
        with zipfile.ZipFile(file_path) as zf:
            # Try to read the file list to verify it's a valid ZIP
            zf.namelist()
            return True
    except zipfile.BadZipFile:
        return False
    except Exception as e:
        print(f"Error checking ZIP file {file_path}: {str(e)}")
        return False


def main():
    # GTFS data URL
    gtfs_url = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip"

    # Ensure temp directory exists
    temp_dir = ensure_temp_dir()
    latest_gtfs_path = temp_dir / "gtfs-data-latest.zip"
    diff_zip_path = temp_dir / "gtfs_diff.zip"

    try:
        # Download latest GTFS data
        download_gtfs_zip(gtfs_url, str(latest_gtfs_path))

        # Validate downloaded file
        if not is_valid_zip(str(latest_gtfs_path)):
            print(f"Error: Downloaded file is not a valid ZIP file: {latest_gtfs_path}")
            sys.exit(1)

        # Get path to cached GTFS data
        cached_gtfs_path = get_cached_gtfs_path()

        if cached_gtfs_path is None:
            print("No cached GTFS data found. Performing full reset...")
            if not run_script("hard_reset.py", [str(latest_gtfs_path)]):
                print("Failed to perform full reset.")
                sys.exit(1)
        else:
            print("Cached GTFS data found. Creating and applying diffs...")

            # Validate cached file
            if not is_valid_zip(cached_gtfs_path):
                print(f"Error: Cached file is not a valid ZIP file: {cached_gtfs_path}")
                sys.exit(1)

            # Create diff zip directly
            try:
                has_changes = create_diff_zip(cached_gtfs_path, str(latest_gtfs_path), str(diff_zip_path))
                if not has_changes:
                    print("No changes found in GTFS data.")
                    sys.exit(0)
            except Exception as e:
                print(f"Failed to create diff zip: {str(e)}")
                sys.exit(1)

            # Apply diff using the script
            print("\nApplying diff to database...")
            if not run_script("apply_gtfs_diff.py", [str(diff_zip_path)]):
                print("Failed to apply diff.")
                sys.exit(1)

            # Update cache
            print("\nUpdating cache...")
            if not run_script("load_gtfs_cache.py"):
                print("Failed to update cache.")
                sys.exit(1)

            # Process realtime updates
            print("\nProcessing realtime updates...")
            if not run_script("gtfs_realtime_parser.py"):
                print("Failed to process realtime updates.")
                sys.exit(1)

        print("\n=== GTFS Update Process Completed Successfully ===")

    except Exception as e:
        print(f"Error during GTFS update: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()