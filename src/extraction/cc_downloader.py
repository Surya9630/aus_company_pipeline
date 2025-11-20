#!/usr/bin/env python3
"""
cc_downloader.py

Download Common Crawl WET files from the list in config/cc_shards.txt.
Supports parallel downloading, resume capability, and progress tracking.

Usage:
    python src/extraction/cc_downloader.py --max 10  # Download first 10 files
    python src/extraction/cc_downloader.py --all     # Download all files
"""

import argparse
import os
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import urlretrieve
from tqdm import tqdm
import time

# Configuration
CC_BASE_URL = "https://data.commoncrawl.org/"
DEFAULT_SHARDS_FILE = "config/cc_shards.txt"
DEFAULT_OUTPUT_DIR = "cc_data"
MAX_WORKERS = 4  # Parallel downloads
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5  # seconds


def read_shard_paths(shards_file):
    """Read WET file paths from config file."""
    with open(shards_file, 'r') as f:
        paths = [line.strip() for line in f if line.strip()]
    return paths


def download_file(url, output_path, desc=None):
    """
    Download a single file with retry logic.
    Returns True if successful, False otherwise.
    """
    for attempt in range(RETRY_ATTEMPTS):
        try:
            # Check if file already exists
            if os.path.exists(output_path):
                file_size = os.path.getsize(output_path)
                if file_size > 1000000:  # > 1MB, assume valid
                    if desc:
                        tqdm.write(f"[SKIP] {desc} (already exists)")
                    return True
            
            # Create parent directory if needed
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Download with progress
            if desc:
                tqdm.write(f"[DOWNLOAD] {desc}")
            
            urlretrieve(url, output_path)
            
            # Verify downloaded file
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                return True
            else:
                if desc:
                    tqdm.write(f"[ERROR] Downloaded file is empty: {desc}")
                return False
                
        except Exception as e:
            if attempt < RETRY_ATTEMPTS - 1:
                if desc:
                    tqdm.write(f"[RETRY] {desc} - Attempt {attempt + 1}/{RETRY_ATTEMPTS}: {e}")
                time.sleep(RETRY_DELAY)
            else:
                if desc:
                    tqdm.write(f"[FAILED] {desc}: {e}")
                return False
    
    return False


def download_worker(shard_path, output_dir, index):
    """Worker function for parallel downloading."""
    url = CC_BASE_URL + shard_path
    
    # Preserve directory structure
    output_path = os.path.join(output_dir, shard_path)
    
    desc = f"[{index}] {os.path.basename(shard_path)}"
    success = download_file(url, output_path, desc)
    
    return {
        'shard_path': shard_path,
        'output_path': output_path,
        'success': success
    }


def download_shards(shards_file, output_dir, max_files=None, workers=MAX_WORKERS):
    """
    Download WET files from Common Crawl.
    
    Args:
        shards_file: Path to file containing list of WET paths
        output_dir: Directory to save downloaded files
        max_files: Maximum number of files to download (None = all)
        workers: Number of parallel download threads
    """
    print(f"Reading shard paths from: {shards_file}")
    shard_paths = read_shard_paths(shards_file)
    
    if max_files:
        shard_paths = shard_paths[:max_files]
    
    print(f"Total files to download: {len(shard_paths)}")
    print(f"Output directory: {output_dir}")
    print(f"Parallel workers: {workers}")
    print(f"Base URL: {CC_BASE_URL}")
    print()
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Track results
    successful = 0
    failed = 0
    skipped = 0
    
    # Download files in parallel
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit all download tasks
        futures = {
            executor.submit(download_worker, shard_path, output_dir, i + 1): shard_path 
            for i, shard_path in enumerate(shard_paths)
        }
        
        # Process completed downloads with progress bar
        with tqdm(total=len(shard_paths), desc="Downloading WET files", unit="file") as pbar:
            for future in as_completed(futures):
                result = future.result()
                
                if os.path.exists(result['output_path']) and os.path.getsize(result['output_path']) > 1000000:
                    if result['success']:
                        successful += 1
                    else:
                        skipped += 1
                else:
                    if result['success']:
                        successful += 1
                    else:
                        failed += 1
                
                pbar.update(1)
    
    print("\n" + "="*60)
    print("Download Summary:")
    print(f"  Successful: {successful}")
    print(f"  Skipped (already exist): {skipped}")
    print(f"  Failed: {failed}")
    print(f"  Total: {len(shard_paths)}")
    print("="*60)
    
    if failed > 0:
        print("\nℹ️  Some downloads failed. You can re-run this script to retry.")
    
    return successful, skipped, failed


def main():
    parser = argparse.ArgumentParser(
        description="Download Common Crawl WET files for Australian company extraction"
    )
    parser.add_argument(
        "--shards-file",
        default=DEFAULT_SHARDS_FILE,
        help=f"Path to file containing WET paths (default: {DEFAULT_SHARDS_FILE})"
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory for downloaded files (default: {DEFAULT_OUTPUT_DIR})"
    )
    parser.add_argument(
        "--max",
        type=int,
        default=None,
        dest="max_files",
        help="Maximum number of files to download (default: all)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS,
        help=f"Number of parallel download threads (default: {MAX_WORKERS})"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Download all files (overrides --max)"
    )
    
    args = parser.parse_args()
    
    # Validate shards file exists
    if not os.path.exists(args.shards_file):
        print(f"Error: Shards file not found: {args.shards_file}")
        sys.exit(1)
    
    max_files = None if args.all else args.max_files
    
    print("="*60)
    print("Common Crawl WET File Downloader")
    print("="*60)
    
    download_shards(
        shards_file=args.shards_file,
        output_dir=args.output_dir,
        max_files=max_files,
        workers=args.workers
    )


if __name__ == "__main__":
    main()
