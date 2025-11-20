#!/usr/bin/env python3
"""
cc_loader.py

Load Common Crawl extracted JSONL data into PostgreSQL stg_common_crawl table.

Usage:
    python src/loading/cc_loader.py --input outputs/cc_extracted.jsonl
"""

import argparse
import json
import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import create_engine, Table, MetaData, func
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))


def get_db_engine():
    """Create database engine from environment variables."""
    if not all([DB_USER, DB_HOST, DB_NAME]):
        print("Error: Database environment variables are not fully set. Check .env.")
        sys.exit(1)

    try:
        db_url = URL.create(
            drivername="postgresql+psycopg2",
            username=DB_USER,
            password=DB_PASSWORD or None,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
        )
        engine = create_engine(db_url)
        return engine
    except Exception as e:
        print(f"Error creating database engine: {e}")
        sys.exit(1)


def load_jsonl_to_postgres(jsonl_path, engine, stg_cc_table):
    """
    Load JSONL data into stg_common_crawl table.
    
    Expected JSONL fields:
    - domain
    - url (website_url)
    - company_name (scraped_company_name)
    - industry (scraped_industry)
    - scraped_abn
    - title, meta_description, h1, schema_org_name (combined as html_content_snippet)
    """
    print(f"Loading data from: {jsonl_path}")
    
    if not os.path.exists(jsonl_path):
        print(f"Error: File not found: {jsonl_path}")
        sys.exit(1)
    
    # Count total lines for progress bar
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f)
    
    print(f"Total records to load: {total_lines}")
    
    batch = []
    inserted = 0
    errors = 0
    
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        with tqdm(total=total_lines, desc="Loading to database", unit=" records") as pbar:
            for line in f:
                try:
                    data = json.loads(line.strip())
                    
                    # Create HTML snippet for context
                    html_snippet_parts = []
                    if data.get('title'):
                        html_snippet_parts.append(f"Title: {data['title']}")
                    if data.get('h1'):
                        html_snippet_parts.append(f"H1: {data['h1']}")
                    if data.get('meta_description'):
                        html_snippet_parts.append(f"Description: {data['meta_description']}")
                    html_content_snippet = " | ".join(html_snippet_parts) if html_snippet_parts else None
                    
                    # Map JSONL fields to database columns
                    record = {
                        'website_url': data.get('url'),
                        'scraped_company_name': data.get('company_name'),
                        'scraped_industry': data.get('industry'),
                        'scraped_abn': data.get('scraped_abn'),
                        'html_content_snippet': html_content_snippet,
                        'warc_file_path': None,  # Can add if tracking source files
                    }
                    
                    # Only add if we have essential data
                    if record['website_url']:
                        batch.append(record)
                    
                    # Insert batch when ready
                    if len(batch) >= BATCH_SIZE:
                        insert_batch(engine, batch, stg_cc_table)
                        inserted += len(batch)
                        pbar.update(len(batch))
                        batch = []
                        
                except json.JSONDecodeError as e:
                    errors += 1
                    if errors < 10:  # Only show first few errors
                        print(f"\nError parsing JSON: {e}")
                except Exception as e:
                    errors += 1
                    if errors < 10:
                        print(f"\nError processing record: {e}")
            
            # Insert remaining batch
            if batch:
                insert_batch(engine, batch, stg_cc_table)
                inserted += len(batch)
                pbar.update(len(batch))
    
    print(f"\n--- Load Complete ---")
    print(f"Successfully inserted: {inserted} records")
    print(f"Errors encountered: {errors} records")
    
    return inserted, errors


def insert_batch(engine, batch, stg_cc_table):
    """Insert a batch of records using upsert logic."""
    if not batch:
        return
    
    try:
        with engine.begin() as connection:
            stmt = insert(stg_cc_table)
            
            # Upsert: update if URL already exists
            stmt = stmt.on_conflict_do_update(
                index_elements=["website_url"],
                set_={
                    "scraped_company_name": stmt.excluded.scraped_company_name,
                    "scraped_industry": stmt.excluded.scraped_industry,
                    "scraped_abn": stmt.excluded.scraped_abn,
                    "html_content_snippet": stmt.excluded.html_content_snippet,
                    "loaded_at": func.now(),
                },
            )
            
            connection.execute(stmt, batch)
            
    except SQLAlchemyError as e:
        print(f"\n[ERROR] Database error during batch insert: {e}")
        if batch:
            print(f"Sample failed record: {batch[0]}")
    except Exception as e:
        print(f"\n[ERROR] Unexpected error during insert: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Load Common Crawl JSONL data into PostgreSQL"
    )
    parser.add_argument(
        "-i", "--input",
        dest="input_path",
        required=True,
        help="Path to JSONL file from Common Crawl extraction"
    )
    
    args = parser.parse_args()
    
    print("="*60)
    print("Common Crawl Database Loader")
    print("="*60)
    
    # Connect to database
    engine = get_db_engine()
    
    # Reflect table
    try:
        metadata = MetaData()
        stg_cc_table = Table("stg_common_crawl", metadata, autoload_with=engine)
        print("Successfully reflected 'stg_common_crawl' table from database.")
    except SQLAlchemyError as e:
        print("FATAL: Could not reflect 'stg_common_crawl' table. Did you run schema.sql?")
        print(f"Error: {e}")
        sys.exit(1)
    
    # Load data
    load_jsonl_to_postgres(args.input_path, engine, stg_cc_table)


if __name__ == "__main__":
    main()
