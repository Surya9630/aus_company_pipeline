#!/usr/bin/env python3
"""
entity_matcher.py

Multi-strategy entity matching between Common Crawl and ABR data.

Implements three matching strategies:
1. Direct ABN Match (highest confidence: 0.95)
2. Fuzzy Name + State Match (medium confidence: 0.70-0.90)
3. LLM-Based Match (variable confidence: 0.60-0.95)

Usage:
    python src/transformation/entity_matcher.py --strategy all
    python src/transformation/entity_matcher.py --strategy direct --limit 1000
"""

import argparse
import os
import sys
from pathlib import Path
from difflib import SequenceMatcher
from typing import Optional, Tuple, List, Dict

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from tqdm import tqdm

# Import LLM matcher
from src.transformation.llm_matcher import match_with_llm

# Load environment
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

# Matching thresholds
FUZZY_THRESHOLD = 0.85  # Minimum similarity score (0-1)
DIRECT_ABN_CONFIDENCE = 0.95
FUZZY_BASE_CONFIDENCE = 0.75
STATE_MATCH_BONUS = 0.10


def get_db_engine():
    """Create database engine from environment variables."""
    if not all([DB_USER, DB_HOST, DB_NAME]):
        print("Error: Database environment variables not set. Check .env")
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
        return create_engine(db_url)
    except Exception as e:
        print(f"Error creating database engine: {e}")
        sys.exit(1)


def normalize_company_name(name: Optional[str]) -> str:
    """
    Normalize company name for comparison.
    - Convert to uppercase
    - Remove common suffixes (PTY LTD, LIMITED, INC, etc.)
    - Remove punctuation
    - Strip whitespace
    """
    if not name:
        return ""
    
    name = name.upper().strip()
    
    # Remove common company suffixes
    suffixes = [
        "PTY LTD", "PTY. LTD.", "PTY LIMITED",
        "LIMITED", "LTD", "LTD.",
        "INCORPORATED", "INC", "INC.",
        "PROPRIETARY", "PROPRIETARY LIMITED",
        "CORPORATION", "CORP", "CORP.",
        "COMPANY", "CO", "CO.",
        "& CO", "&CO",
    ]
    
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
    
    # Remove punctuation
    name = "".join(c if c.isalnum() or c.isspace() else " " for c in name)
    
    # Collapse multiple spaces
    name = " ".join(name.split())
    
    return name


def calculate_similarity(name1: str, name2: str) -> float:
    """
    Calculate similarity score between two company names.
    Uses SequenceMatcher ratio (0.0 to 1.0).
    """
    if not name1 or not name2:
        return 0.0
    
    norm1 = normalize_company_name(name1)
    norm2 = normalize_company_name(name2)
    
    if not norm1 or not norm2:
        return 0.0
    
    return SequenceMatcher(None, norm1, norm2).ratio()


def strategy_direct_abn(engine, limit=None) -> Tuple[int, int]:
    """
    Strategy 1: Direct ABN Match
    Match CC records that have a scraped_abn directly to ABR records.
    
    Returns: (matched_count, total_candidates)
    """
    print("\n" + "="*60)
    print("STRATEGY 1: Direct ABN Matching")
    print("="*60)
    
    query = text("""
        SELECT 
            cc.id as cc_id,
            cc.scraped_abn,
            cc.scraped_company_name,
            abr.abn,
            abr.entity_name
        FROM stg_common_crawl cc
        INNER JOIN stg_abr abr ON cc.scraped_abn = abr.abn
        WHERE cc.scraped_abn IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM stg_matched_companies m 
            WHERE m.cc_id = cc.id
        )
        LIMIT :limit
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"limit": limit if limit else 1000000})
        candidates = result.fetchall()
    
    print(f"Found {len(candidates)} direct ABN matches")
    
    if not candidates:
        return 0, 0
    
    matches = []
    for row in tqdm(candidates, desc="Processing direct ABN matches"):
        match = {
            'cc_id': row.cc_id,
            'abn': row.abn,
            'match_method': 'direct_abn',
            'match_confidence': DIRECT_ABN_CONFIDENCE,
            'match_reasoning': f"Direct ABN match: {row.scraped_abn} found on website matches ABR record"
        }
        matches.append(match)
    
    # Bulk insert matches
    insert_matches(engine, matches)
    
    return len(matches), len(candidates)


def strategy_fuzzy_name(engine, limit=None) -> Tuple[int, int]:
    """
    Strategy 2: Fuzzy Name Matching
    Match CC records to ABR using company name similarity.
    Optionally boost confidence if state matches.
    
    Returns: (matched_count, total_candidates)
    """
    print("\n" + "="*60)
    print("STRATEGY 2: Fuzzy Name Matching")
    print("="*60)
    
    # Get unmatched CC records
    query = text("""
        SELECT 
            cc.id as cc_id,
            cc.scraped_company_name,
            cc.website_url
        FROM stg_common_crawl cc
        WHERE cc.scraped_company_name IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM stg_matched_companies m 
            WHERE m.cc_id = cc.id
        )
        LIMIT :limit
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"limit": limit if limit else 10000})
        cc_records = result.fetchall()
    
    print(f"Processing {len(cc_records)} unmatched CC records")
    
    if not cc_records:
        return 0, 0
    
    # Get all ABR records (cache in memory for faster matching)
    abr_query = text("""
        SELECT abn, entity_name, state
        FROM stg_abr
        WHERE entity_status = 'Active'
        AND entity_name IS NOT NULL
    """)
    
    with engine.connect() as conn:
        result = conn.execute(abr_query)
        abr_records = result.fetchall()
    
    print(f"Comparing against {len(abr_records)} active ABR records")
    
    matches = []
    for cc_record in tqdm(cc_records, desc="Fuzzy name matching"):
        best_match = find_best_fuzzy_match(
            cc_record.scraped_company_name,
            abr_records,
            FUZZY_THRESHOLD
        )
        
        if best_match:
            confidence = best_match['confidence']
            match = {
                'cc_id': cc_record.cc_id,
                'abn': best_match['abn'],
                'match_method': 'fuzzy_name',
                'match_confidence': round(confidence, 2),
                'match_reasoning': f"Fuzzy name match (similarity: {best_match['similarity']:.2f}) between '{cc_record.scraped_company_name}' and '{best_match['entity_name']}'"
            }
            matches.append(match)
    
    print(f"Found {len(matches)} fuzzy matches above threshold {FUZZY_THRESHOLD}")
    
    # Insert matches
    if matches:
        insert_matches(engine, matches)
    
    return len(matches), len(cc_records)


def find_best_fuzzy_match(cc_name: str, abr_records: List, threshold: float) -> Optional[Dict]:
    """
    Find the best matching ABR record for a CC company name.
    Returns None if no match above threshold.
    """
    best_match = None
    best_similarity = 0.0
    
    for abr_record in abr_records:
        similarity = calculate_similarity(cc_name, abr_record.entity_name)
        
        if similarity > best_similarity and similarity >= threshold:
            best_similarity = similarity
            confidence = FUZZY_BASE_CONFIDENCE + (similarity - threshold) * 0.5
            
            best_match = {
                'abn': abr_record.abn,
                'entity_name': abr_record.entity_name,
                'similarity': similarity,
                'confidence': min(confidence, 0.95)  # Cap at 0.95
            }
    
    return best_match


def strategy_llm_matching(engine, limit=None) -> Tuple[int, int]:
    """
    Strategy 3: LLM-Based Matching
    Use LLM to match difficult cases by providing context.
    
    Returns: (matched_count, total_candidates)
    """
    print("\n" + "="*60)
    print("STRATEGY 3: LLM-Based Matching")
    print("="*60)
    
    # Get unmatched CC records with good context
    query = text("""
        SELECT 
            cc.id as cc_id,
            cc.scraped_company_name,
            cc.scraped_industry,
            cc.website_url,
            cc.html_content_snippet
        FROM stg_common_crawl cc
        WHERE cc.scraped_company_name IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM stg_matched_companies m 
            WHERE m.cc_id = cc.id
        )
        LIMIT :limit
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"limit": limit if limit else 100})
        cc_records = result.fetchall()
    
    print(f"Processing {len(cc_records)} unmatched CC records with LLM")
    
    if not cc_records:
        return 0, 0
    
    matches = []
    for cc_record in tqdm(cc_records, desc="LLM matching"):
        # Get top 5 ABR candidates based on fuzzy matching
        candidates = get_top_abr_candidates(engine, cc_record.scraped_company_name, top_n=5)
        
        if not candidates:
            continue
        
        # Use LLM to decide best match
        llm_result = match_with_llm(cc_record, candidates)
        
        if llm_result and llm_result.get('matched_abn'):
            match = {
                'cc_id': cc_record.cc_id,
                'abn': llm_result['matched_abn'],
                'match_method': 'llm',
                'match_confidence': llm_result.get('confidence', 0.70),
                'match_reasoning': f"LLM match: {llm_result.get('reasoning', 'AI-determined best match')}"
            }
            matches.append(match)
    
    print(f"Found {len(matches)} LLM matches")
    
    if matches:
        insert_matches(engine, matches)
    
    return len(matches), len(cc_records)


def get_top_abr_candidates(engine, cc_name: str, top_n: int = 5) -> List[Dict]:
    """Get top N ABR candidates based on fuzzy similarity."""
    query = text("""
        SELECT abn, entity_name, entity_type, state, full_address
        FROM stg_abr
        WHERE entity_status = 'Active'
        AND entity_name IS NOT NULL
        LIMIT 1000
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query)
        abr_records = result.fetchall()
    
    # Calculate similarities
    scored = []
    for record in abr_records:
        similarity = calculate_similarity(cc_name, record.entity_name)
        scored.append({
            'abn': record.abn,
            'entity_name': record.entity_name,
            'entity_type': record.entity_type,
            'state': record.state,
            'full_address': record.full_address,
            'similarity': similarity
        })
    
    # Sort by similarity and return top N
    scored.sort(key=lambda x: x['similarity'], reverse=True)
    return scored[:top_n]


def insert_matches(engine, matches: List[Dict]):
    """Bulk insert matches into stg_matched_companies."""
    if not matches:
        return
    
    try:
        metadata = MetaData()
        match_table = Table("stg_matched_companies", metadata, autoload_with=engine)
        
        with engine.begin() as conn:
            conn.execute(match_table.insert(), matches)
        
        print(f"✓ Inserted {len(matches)} matches into database")
    except SQLAlchemyError as e:
        print(f"Error inserting matches: {e}")


def main():
    parser = argparse.ArgumentParser(description="Entity Matching Pipeline")
    parser.add_argument(
        "--strategy",
        choices=["all", "direct", "fuzzy", "llm"],
        default="all",
        help="Matching strategy to run"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of records to process (for testing)"
    )
    
    args = parser.parse_args()
    
    print("="*60)
    print("ENTITY MATCHING PIPELINE")
    print("="*60)
    
    engine = get_db_engine()
    
    total_matched = 0
    total_candidates = 0
    
    # Run matching strategies
    if args.strategy in ["all", "direct"]:
        matched, candidates = strategy_direct_abn(engine, args.limit)
        total_matched += matched
        total_candidates += candidates
    
    if args.strategy in ["all", "fuzzy"]:
        matched, candidates = strategy_fuzzy_name(engine, args.limit)
        total_matched += matched
        total_candidates += candidates
    
    if args.strategy in ["all", "llm"]:
        matched, candidates = strategy_llm_matching(engine, args.limit)
        total_matched += matched
        total_candidates += candidates
    
    # Summary
    print("\n" + "="*60)
    print("MATCHING SUMMARY")
    print("="*60)
    print(f"Total matched: {total_matched}")
    print(f"Total candidates processed: {total_candidates}")
    if total_candidates > 0:
        print(f"Match rate: {total_matched/total_candidates*100:.1f}%")
    
    # Statistics by method
    stats_query = text("""
        SELECT 
            match_method,
            COUNT(*) as count,
            AVG(match_confidence) as avg_confidence,
            MIN(match_confidence) as min_confidence,
            MAX(match_confidence) as max_confidence
        FROM stg_matched_companies
        GROUP BY match_method
        ORDER BY count DESC
    """)
    
    with engine.connect() as conn:
        result = conn.execute(stats_query)
        stats = result.fetchall()
    
    if stats:
        print("\nMatches by method:")
        for row in stats:
            print(f"  {row.match_method:15s}: {row.count:6d} matches "
                  f"(avg confidence: {row.avg_confidence:.2f})")


if __name__ == "__main__":
    main()
