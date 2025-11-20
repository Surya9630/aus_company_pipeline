#!/usr/bin/env python3
"""
abr_extractor.py

Parse ABR XML files from ABR_DATA_DIRECTORY and load into PostgreSQL stg_abr table.

Features:
 - Memory-efficient parsing with lxml.etree.iterparse
 - Robust start_date extraction from ABN attributes
 - Address fallback: BusinessAddress -> LegalAddress
 - Two insertion strategies:
    * SQLAlchemy executemany (default; safe and portable)
    * psycopg2.extras.execute_values (faster; toggle with USE_PSYCOPG2_BULK)
"""
from __future__ import annotations

import os
import sys
import time
import glob
from datetime import datetime, date
from typing import Optional

from lxml import etree
from dotenv import load_dotenv
from tqdm import tqdm

# SQLAlchemy imports
from sqlalchemy import create_engine, Table, MetaData, func
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert

# Optional psycopg2 bulk import
try:
    import psycopg2
    from psycopg2.extras import execute_values
    _HAVE_PSYCOPG2 = True
except Exception:
    _HAVE_PSYCOPG2 = False

# --- Configuration ---

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")  # can be empty
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

ABR_DATA_DIRECTORY = os.getenv("ABR_DATA_DIRECTORY", "abr_data")

# Safe default: tune to environment (500 - 2000 usually sensible)
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "2000"))

# Toggle bulk psycopg2 method (if psycopg2 installed). False uses SQLAlchemy executemany.
USE_PSYCOPG2_BULK = os.getenv("USE_PSYCOPG2_BULK", "false").lower() in ("1", "true", "yes")


# --- Utility: parse date strings into date objects ---

def _parse_date_str_to_date(s: Optional[str]) -> Optional[date]:
    """
    Convert a date/time string into a datetime.date object.
    Returns None on failure.
    Tries multiple common formats and datetime.fromisoformat as fallback.
    """
    if not s:
        return None
    s = s.strip()
    # Try a few common formats
    fmts = (
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S.%f",
    )
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    # try fromisoformat fallback (handles a few other ISO variants)
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        pass
    # last-resort: extract leading YYYY-MM-DD if present
    try:
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        pass
    return None


# --- DB Engine ---

def get_db_engine():
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


# --- Insert functions ---

def insert_batch_sqlalchemy(engine, batch, stg_abr_table):
    """
    Use SQLAlchemy statement + connection.execute(stmt, batch) to avoid huge compiled param expansion.
    This results in DBAPI executemany behavior for most drivers.
    """
    if not batch:
        return

    try:
        with engine.begin() as connection:
            stmt = insert(stg_abr_table)

            stmt = stmt.on_conflict_do_update(
                index_elements=["abn"],
                set_={
                    "entity_name": stmt.excluded.entity_name,
                    "entity_type": stmt.excluded.entity_type,
                    "entity_status": stmt.excluded.entity_status,
                    "address_line_1": stmt.excluded.address_line_1,
                    "address_line_2": stmt.excluded.address_line_2,
                    "postcode": stmt.excluded.postcode,
                    "state": stmt.excluded.state,
                    "start_date": stmt.excluded.start_date,
                    "full_address": stmt.excluded.full_address,
                    "loaded_at": func.now(),
                },
            )

            # Pass the list-of-dicts as the second param -> driver executemany
            connection.execute(stmt, batch)

    except SQLAlchemyError as e:
        print(f"[SQLAlchemy] Database error during batch insert: {e}")
        if batch:
            print("Sample failed record:", batch[0])
    except Exception as e:
        print(f"[SQLAlchemy] Unexpected error during insert: {e}")


def insert_batch_psycopg2(engine, batch, stg_abr_table_name="stg_abr"):
    """
    Very fast bulk insert using psycopg2.extras.execute_values.
    This bypasses SQLAlchemy ORM/compilation and uses a raw connection.
    Requires psycopg2 to be installed.
    """
    if not _HAVE_PSYCOPG2:
        raise RuntimeError("psycopg2 is not installed - cannot use psycopg2 bulk insert.")

    if not batch:
        return

    # Define column list in consistent order
    columns = [
        "abn",
        "entity_name",
        "entity_type",
        "entity_status",
        "address_line_1",
        "address_line_2",
        "postcode",
        "state",
        "start_date",
        "full_address",
    ]

    # Convert to tuples in column order
    values = [tuple(rec.get(col) for col in columns) for rec in batch]

    insert_sql = f"""
    INSERT INTO {stg_abr_table_name} ({', '.join(columns)})
    VALUES %s
    ON CONFLICT (abn) DO UPDATE SET
      entity_name = EXCLUDED.entity_name,
      entity_type = EXCLUDED.entity_type,
      entity_status = EXCLUDED.entity_status,
      address_line_1 = EXCLUDED.address_line_1,
      address_line_2 = EXCLUDED.address_line_2,
      postcode = EXCLUDED.postcode,
      state = EXCLUDED.state,
      start_date = EXCLUDED.start_date,
      full_address = EXCLUDED.full_address,
      loaded_at = CURRENT_TIMESTAMP
    """

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            execute_values(cur, insert_sql, values, page_size=1000)
        raw_conn.commit()
    except Exception as e:
        raw_conn.rollback()
        print("[psycopg2] Bulk insert error:", e)
        if batch:
            print("Sample failed record:", batch[0])
    finally:
        raw_conn.close()


def insert_batch(engine, batch, stg_abr_table):
    """
    Wrapper that picks the configured implementation.
    """
    if USE_PSYCOPG2_BULK:
        if not _HAVE_PSYCOPG2:
            print("USE_PSYCOPG2_BULK is True but psycopg2 isn't available. Falling back to SQLAlchemy.")
            insert_batch_sqlalchemy(engine, batch, stg_abr_table)
        else:
            insert_batch_psycopg2(engine, batch, stg_abr_table.name)
    else:
        insert_batch_sqlalchemy(engine, batch, stg_abr_table)


# --- XML Parsing ---

def parse_abr_xml(file_path, engine, stg_abr_table):
    """
    Parse a single XML ABR file and insert into stg_abr table in batches.
    Expects top-level tag 'ABR' for each record.
    """
    print(f"\nStarting to parse XML file: {file_path}")

    data_batch = []
    records_in_this_file = 0
    addresses_found_in_file = 0

    try:
        context = etree.iterparse(file_path, events=("end",), tag="ABR")
    except FileNotFoundError:
        print(f"ERROR: The file '{file_path}' was not found.")
        return 0
    except etree.XMLSyntaxError as e:
        print(f"ERROR: The XML file '{file_path}' is corrupt: {e}")
        return 0

    with tqdm(desc=f"Processing {os.path.basename(file_path)}", unit=" records") as pbar:
        for event, elem in context:
            # --- ABN and its attributes (status, start date, etc.) ---
            abn_tag = elem.find("ABN")
            if abn_tag is not None:
                abn_value = abn_tag.text
                abn_status = abn_tag.get("status") or abn_tag.get("ABNStatus", None)
                # Try several possible attribute names for the start date
                raw_start_date = (
                    abn_tag.get("ABNStatusFromDate")
                    or abn_tag.get("ABNStatusFrom")
                    or abn_tag.get("ABNFromDate")
                    or abn_tag.get("FromDate")
                )
                parsed_start_date = _parse_date_str_to_date(raw_start_date)
            else:
                abn_value = None
                abn_status = None
                parsed_start_date = None

            # Entity name
            entity_name = elem.findtext("MainEntity/NonIndividualName/NonIndividualNameText")

            # --- Address extraction (BusinessAddress first, then LegalAddress) ---
            address = elem.find("MainEntity/BusinessAddress/AddressDetails")
            if address is None:
                address = elem.find("MainEntity/LegalAddress/AddressDetails")

            if address is not None:
                state = address.findtext("State")
                postcode = address.findtext("Postcode")
                address_line_1 = address.findtext("AddressLine1")
                address_line_2 = address.findtext("AddressLine2")
                # build a trimmed full_address fallback
                parts = []
                if address_line_1 and address_line_1.strip():
                    parts.append(address_line_1.strip())
                if address_line_2 and address_line_2.strip():
                    parts.append(address_line_2.strip())
                full_address = " ".join(parts) if parts else None
                addresses_found_in_file += 1
            else:
                state = postcode = address_line_1 = address_line_2 = full_address = None

            record = {
                "abn": abn_value,
                "entity_name": entity_name,
                "entity_type": elem.findtext("EntityType/EntityTypeInd"),
                "entity_status": abn_status,
                "address_line_1": address_line_1,
                "address_line_2": address_line_2,
                "postcode": postcode,
                "state": state,
                "start_date": parsed_start_date,
                "full_address": full_address,
            }

            if record["abn"]:
                data_batch.append(record)
                records_in_this_file += 1

            if len(data_batch) >= BATCH_SIZE:
                insert_batch(engine, data_batch, stg_abr_table)
                pbar.update(len(data_batch))
                data_batch = []

            # memory cleanup for iterparse
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

    # Insert any leftover records
    if data_batch:
        insert_batch(engine, data_batch, stg_abr_table)
        pbar.update(len(data_batch))

    del context
    print(f"Finished processing {file_path}. Processed {records_in_this_file} records.")
    print(f"Addresses found in this file: {addresses_found_in_file}")
    return records_in_this_file


# --- Main ---

def main():
    print("--- Starting ABR Extraction Process ---")
    start_time = time.time()

    engine = get_db_engine()

    # Reflect table
    try:
        metadata = MetaData()
        stg_abr_table = Table("stg_abr", metadata, autoload_with=engine)
        print("Successfully reflected 'stg_abr' table from database.")
    except SQLAlchemyError as e:
        print("FATAL: Could not reflect 'stg_abr' table. Did you run schema.sql and provide correct DB creds?")
        print("Error:", e)
        sys.exit(1)

    # Find XML files
    xml_files = glob.glob(os.path.join(ABR_DATA_DIRECTORY, "*.xml"))
    if not xml_files:
        print(f"FATAL: No .xml files found in directory: '{ABR_DATA_DIRECTORY}'")
        sys.exit(1)

    xml_files.sort()
    print(f"Found {len(xml_files)} XML files to process.")

    total_records_processed = 0
    total_addresses_found = 0
    for xml_file_path in xml_files:
        file_records = parse_abr_xml(xml_file_path, engine, stg_abr_table)
        total_records_processed += file_records
        # Note: per-file address count printed inside parse function

    end_time = time.time()
    print("\n--- ABR Extraction Complete ---")
    print(f"Successfully processed {total_records_processed} records from {len(xml_files)} files.")
    print(f"Total time: {end_time - start_time:.2f} seconds.")


if __name__ == "__main__":
    main()
