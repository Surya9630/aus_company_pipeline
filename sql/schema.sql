-- sql/schema.sql
-- This script defines the staging and final tables for our data pipeline.

-- Drop tables if they exist to ensure a fresh start (for development)
DROP TABLE IF EXISTS stg_matched_companies CASCADE;
DROP TABLE IF EXISTS dim_companies CASCADE;
DROP TABLE IF EXISTS stg_abr CASCADE;
DROP TABLE IF EXISTS stg_common_crawl CASCADE;

-- -----------------------------------------------------
-- Table 1: stg_abr
-- Purpose: Holds raw data extracted from the ABR bulk XML file.
-- -----------------------------------------------------
CREATE TABLE stg_abr (
    -- Core ABR Fields
    abn VARCHAR(20) PRIMARY KEY,
    entity_name TEXT,
    entity_type VARCHAR(100),
    entity_status VARCHAR(50),

    -- Address Fields
    address_line_1 TEXT,
    address_line_2 TEXT,
    postcode TEXT,
    state TEXT,

    -- Computed Address
    full_address TEXT,  -- NEW FIELD for combined/cleaned address

    -- Metadata
    start_date DATE,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------
-- Table 2: stg_common_crawl
-- Purpose: Holds raw data scraped from Common Crawl.
-- -----------------------------------------------------
CREATE TABLE stg_common_crawl (
    id SERIAL PRIMARY KEY,
    website_url TEXT NOT NULL UNIQUE,  -- Added UNIQUE constraint for upsert
    scraped_company_name TEXT,
    scraped_industry TEXT,

    -- This is the most critical field for matching.
    -- We will try to find an ABN on the website.
    scraped_abn VARCHAR(20),

    -- This is for the LLM to provide context for matching
    html_content_snippet TEXT,

    -- Metadata
    warc_file_path TEXT, -- Good to track provenance
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Add a comment to the scraped_abn column for clarity
COMMENT ON COLUMN stg_common_crawl.scraped_abn IS
'The ABN found by scraping the website (e.g., in the footer). This is our primary key for matching.';

-- -----------------------------------------------------
-- Table 3: stg_matched_companies
-- Purpose: Stores entity matching results between CC and ABR
-- -----------------------------------------------------
CREATE TABLE stg_matched_companies (
    match_id SERIAL PRIMARY KEY,
    cc_id INTEGER REFERENCES stg_common_crawl(id) ON DELETE CASCADE,
    abn VARCHAR(20) REFERENCES stg_abr(abn) ON DELETE CASCADE,
    match_method VARCHAR(50) NOT NULL, -- 'direct_abn', 'fuzzy_name', 'llm', 'manual'
    match_confidence NUMERIC(3,2) CHECK (match_confidence >= 0 AND match_confidence <= 1), -- 0.00 to 1.00
    match_reasoning TEXT, -- Explanation of why this match was made
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(cc_id, abn) -- Prevent duplicate matches
);

COMMENT ON TABLE stg_matched_companies IS
'Entity matching results from multiple strategies (ABN matching, fuzzy name matching, LLM matching)';

COMMENT ON COLUMN stg_matched_companies.match_method IS
'Matching strategy used: direct_abn (0.95), fuzzy_name (0.70-0.90), llm (0.60-0.95), manual (1.00)';

COMMENT ON COLUMN stg_matched_companies.match_confidence IS
'Confidence score from 0.00 to 1.00, with higher scores indicating more reliable matches';

-- -----------------------------------------------------
-- Table 4: dim_companies
-- Purpose: Final unified company dimension for analytics
-- -----------------------------------------------------
CREATE TABLE dim_companies (
    company_id SERIAL PRIMARY KEY,
    abn VARCHAR(20) UNIQUE,
    company_name TEXT NOT NULL,
    website_url TEXT,
    industry TEXT,
    address TEXT,
    state TEXT,
    postcode TEXT,
    entity_type VARCHAR(100),
    entity_status VARCHAR(50),
    start_date DATE,
    data_source VARCHAR(50) NOT NULL CHECK (data_source IN ('ABR', 'CC', 'BOTH')),
    match_confidence NUMERIC(3,2), -- Highest match confidence if from matching
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_companies IS
'Final unified company dimension combining ABR and Common Crawl data through entity matching';

COMMENT ON COLUMN dim_companies.data_source IS
'Source of company data: ABR (only in business register), CC (only in Common Crawl), BOTH (matched from both sources)';

-- Create indexes for performance (detailed indexes in indexes.sql)
CREATE INDEX idx_stg_cc_website_url ON stg_common_crawl(website_url);
CREATE INDEX idx_stg_cc_scraped_abn ON stg_common_crawl(scraped_abn) WHERE scraped_abn IS NOT NULL;
CREATE INDEX idx_stg_abr_entity_name ON stg_abr(entity_name);
CREATE INDEX idx_dim_companies_abn ON dim_companies(abn) WHERE abn IS NOT NULL;
