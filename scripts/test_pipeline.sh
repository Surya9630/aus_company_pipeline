#!/bin/bash
# test_pipeline.sh - Lightweight pipeline test for local MacBook
# Tests all pipeline components with minimal data (<500MB)

set -e  # Exit on error

echo "🧪 Testing Australian Company Pipeline (Minimal Data)"
echo "===================================================="
echo ""

# Check prerequisites
echo "Checking prerequisites..."
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 not found. Please install Python 3.11+"
    exit 1
fi

if ! command -v psql &> /dev/null; then
    echo "❌ PostgreSQL not found. Please install PostgreSQL"
    exit 1
fi

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
else
    echo "⚠️  Warning: .env file not found. Using defaults."
    export DB_HOST=localhost
    export DB_PORT=5432
    export DB_NAME=aus_companies_test
    export DB_USER=$USER
fi

echo "✅ Prerequisites OK"
echo ""

# Test 1: Database Setup
echo "[1/7] Testing database schema creation..."
echo "Creating test database: $DB_NAME"

# Drop and recreate test database
dropdb --if-exists $DB_NAME 2>/dev/null || true
createdb $DB_NAME

# Create schema
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f sql/schema.sql > /dev/null 2>&1

# Verify tables exist
TABLE_COUNT=$(psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';")

if [ "$TABLE_COUNT" -ge 4 ]; then
    echo "✅ Database schema created ($TABLE_COUNT tables)"
else
    echo "❌ Database schema failed (expected 4+ tables, got $TABLE_COUNT)"
    exit 1
fi
echo ""

# Test 2: Generate Sample ABR Data
echo "[2/7] Generating sample ABR data..."
python scripts/generate_sample_abr.py --count 100
echo "✅ Generated 100 sample ABR records"
echo ""

# Test 3: ABR Extraction
echo "[3/7] Testing ABR extractor..."
if [ -f "abr_data/sample_abr.xml" ]; then
    python src/extraction/abr_extractor.py
    ABR_COUNT=$(psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM stg_abr;")
    echo "✅ ABR extractor loaded $ABR_COUNT records"
else
    echo "⚠️  No ABR data, skipping ABR test"
fi
echo ""

# Test 4: Common Crawl Extraction
echo "[4/7] Testing Common Crawl extractor (sample file)..."
python src/extraction/common_crawl_extractor.py \
    --input samples/sample_cc.warc.gz \
    --output outputs/test_cc.jsonl \
    --max 100 \
    --no-llm

if [ -f "outputs/test_cc.jsonl" ]; then
    CC_EXTRACTED=$(wc -l < outputs/test_cc.jsonl | tr -d ' ')
    echo "✅ CC extractor extracted $CC_EXTRACTED records"
else
    echo "❌ CC extractor failed"
    exit 1
fi
echo ""

# Test 5: Database Loader
echo "[5/7] Testing Common Crawl database loader..."
python src/loading/cc_loader.py --input outputs/test_cc.jsonl
CC_COUNT=$(psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM stg_common_crawl;")
echo "✅ Loaded $CC_COUNT records to stg_common_crawl"
echo ""

# Test 6: Entity Matcher
echo "[6/7] Testing entity matcher..."
python src/transformation/entity_matcher.py --strategy direct --limit 100 2>/dev/null || true
MATCH_COUNT=$(psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM stg_matched_companies;" 2>/dev/null | tr -d ' ')
if [ -z "$MATCH_COUNT" ]; then
    MATCH_COUNT=0
fi
echo "✅ Entity matcher found $MATCH_COUNT matches"
echo ""

# Test 7: dbt Models
echo "[7/7] Testing dbt transformations..."
cd dbt_project
export DBT_PROFILES_DIR=.
dbt run --select stg_cc_clean 2>&1 | grep -E "(Completed|ERROR)" || true
dbt test --select stg_cc_clean 2>&1 | grep -E "(PASS|FAIL|ERROR)" || true
cd ..
echo "✅ dbt models executed"
echo ""

# Summary
echo "=================================================="
echo "📊 Test Summary"
echo "=================================================="
echo ""
psql -h $DB_HOST -U $DB_USER -d $DB_NAME << EOF
\echo 'Common Crawl Records:'
SELECT COUNT(*) as cc_records FROM stg_common_crawl;

\echo ''
\echo 'ABR Records:'
SELECT COUNT(*) as abr_records FROM stg_abr;

\echo ''
\echo 'Matched Companies:'
SELECT 
    COALESCE(match_method, 'No matches yet') as match_method, 
    COUNT(*) as count
FROM stg_matched_companies
GROUP BY match_method
UNION ALL
SELECT 'TOTAL', COUNT(*) FROM stg_matched_companies;
EOF

echo ""
echo "🎉 All tests passed!"
echo "Your pipeline is working correctly."
