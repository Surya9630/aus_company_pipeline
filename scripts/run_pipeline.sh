#!/bin/bash
# run_pipeline.sh
# Master script to run the complete Australian Company Data Pipeline
#
# This script orchestrates:
# 1. Database setup
# 2. Common Crawl data extraction
# 3. ABR data extraction
# 4. Entity matching
# 5. dbt transformations
# 6. Statistics generation

set -e  # Exit on error

echo "=========================================="
echo "Australian Company Data Pipeline"
echo "=========================================="
echo ""

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | xargs)
else
    echo "Error: .env file not found!"
    exit 1
fi

# Step 1: Database Setup
echo "[1/6] Setting up database schema..."
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f sql/schema.sql
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f sql/indexes.sql
echo "✓ Database schema created"
echo ""

# Step 2: Common Crawl Extraction
echo "[2/6] Processing Common Crawl data..."
echo "Downloading WET files (this may take a while)..."
python src/extraction/cc_downloader.py --max 10 --output-dir cc_data

echo "Extracting Australian company data from WET files..."
python src/extraction/common_crawl_extractor.py \
    --input-dir cc_data/crawl-data \
    --output outputs/cc_extracted.jsonl \
    --max 100000 \
    --no-llm

echo "Loading Common Crawl data to database..."
python src/loading/cc_loader.py --input outputs/cc_extracted.jsonl
echo "✓ Common Crawl data processed"
echo ""

# Step 3: ABR Extraction
echo "[3/6] Processing ABR data..."
if [ -d "abr_data" ] && [ "$(ls -A abr_data/*.xml 2>/dev/null)" ]; then
    python src/extraction/abr_extractor.py
    echo "✓ ABR data processed"
else
    echo "⚠ Warning: No ABR XML files found in abr_data/ directory"
    echo "Please download ABR bulk files from https://data.gov.au/"
fi
echo ""

# Step 4: Entity Matching
echo "[4/6] Running entity matching..."
echo "  - Direct ABN matching..."
python src/transformation/entity_matcher.py --strategy direct --limit 50000

echo "  - Fuzzy name matching..."
python src/transformation/entity_matcher.py --strategy fuzzy --limit 10000

echo "  - LLM matching (limited for cost)..."
python src/transformation/entity_matcher.py --strategy llm --limit 100

echo "✓ Entity matching complete"
echo ""

# Step 5: dbt Transformations
echo "[5/6] Running dbt transformations..."
cd dbt_project
dbt deps
dbt run
dbt test
cd ..
echo "✓ dbt transformations complete"
echo ""

# Step 6: Generate Statistics
echo "[6/6] Generating statistics..."
psql -h $DB_HOST -U $DB_USER -d $DB_NAME << EOF
\echo 'Pipeline Statistics:'
\echo '===================='
\echo ''
\echo 'Common Crawl Records:'
SELECT COUNT(*) FROM stg_common_crawl;

\echo ''
\echo 'ABR Records:'
SELECT COUNT(*) FROM stg_abr WHERE entity_status = 'Active';

\echo ''
\echo 'Matched Companies (by method):'
SELECT match_method, COUNT(*), AVG(match_confidence) as avg_confidence
FROM stg_matched_companies
GROUP BY match_method
ORDER BY COUNT(*) DESC;

\echo ''
\echo 'Final Company Dimension (by source):'
SELECT data_source, COUNT(*)
FROM dim_companies
GROUP BY data_source
ORDER BY data_source;
EOF

echo ""
echo "=========================================="
echo "Pipeline Complete!"
echo "=========================================="
echo "Check outputs/ directory for extracted files"
echo "Query dim_companies table for final results"
